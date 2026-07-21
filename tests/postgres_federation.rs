//! Integration test for the federated Parquet ⋈ PostgreSQL join path.
//!
//! Requires a live PostgreSQL the test may freely create tables in. Set
//! `IGLOO_TEST_POSTGRES_URI` to run it, e.g.:
//!
//! ```sh
//! IGLOO_TEST_POSTGRES_URI=postgres://postgres:postgres@localhost:5432/igloo_test \
//!     cargo test --test postgres_federation
//! ```
//!
//! Without the variable the test skips (and reports so), keeping plain
//! `cargo test` hermetic. CI provides a Postgres service container.

use std::sync::Arc;

use datafusion::arrow::array::{Array, Int64Array, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::parquet::arrow::ArrowWriter;
use tokio_postgres::NoTls;

use igloo::datafusion_engine::DataFusionEngine;

fn write_parquet_fixture(dir: &std::path::Path) {
    let schema = Arc::new(ArrowSchema::new(vec![
        Field::new("user_id", DataType::Int64, false),
        Field::new("data", DataType::Utf8, true),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![42, 7])),
            Arc::new(StringArray::from(vec![Some("hello"), Some("world")])),
        ],
    )
    .unwrap();

    let file = std::fs::File::create(dir.join("data.parquet")).unwrap();
    let mut writer = ArrowWriter::try_new(file, schema, None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
}

async fn seed_postgres(uri: &str) {
    let (client, connection) = tokio_postgres::connect(uri, NoTls)
        .await
        .expect("failed to connect to IGLOO_TEST_POSTGRES_URI");
    tokio::spawn(connection);

    client
        .batch_execute(
            "DROP TABLE IF EXISTS my_pg_table;
             CREATE TABLE my_pg_table (user_id BIGINT NOT NULL, extra_info TEXT);
             INSERT INTO my_pg_table (user_id, extra_info)
             VALUES (42, 'vip'), (7, 'basic'), (99, NULL);",
        )
        .await
        .expect("failed to seed my_pg_table");
}

fn string_column(batch: &RecordBatch, index: usize) -> &StringArray {
    batch
        .column(index)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap()
}

#[tokio::test]
async fn federated_join_returns_matching_row_from_both_sources() {
    let Ok(uri) = std::env::var("IGLOO_TEST_POSTGRES_URI") else {
        eprintln!("skipping postgres_federation: IGLOO_TEST_POSTGRES_URI is not set");
        return;
    };

    let dir = std::env::temp_dir().join(format!("igloo_federation_test_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    write_parquet_fixture(&dir);
    seed_postgres(&uri).await;

    let engine = DataFusionEngine::new(dir.to_str().unwrap(), &uri, &["public".to_string()])
        .await
        .expect("engine initialization against live Postgres failed");

    // The exact query main.rs runs today: freezes the demo behavior.
    let batches = engine
        .query(
            "SELECT i.user_id, i.data, p.extra_info \
             FROM iceberg i JOIN pg_table p ON i.user_id = p.user_id \
             WHERE i.user_id = 42",
        )
        .await
        .expect("federated join query failed");

    let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(rows, 1, "expected exactly one joined row for user_id 42");

    let batch = batches.iter().find(|b| b.num_rows() > 0).unwrap();
    let user_id = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(user_id.value(0), 42);
    assert_eq!(string_column(batch, 1).value(0), "hello");
    assert_eq!(string_column(batch, 2).value(0), "vip");

    // NULL handling: user 99 exists only in Postgres with a NULL column and
    // must not join, and a projection touching it must not error.
    let null_batches = engine
        .query("SELECT extra_info FROM pg_table WHERE user_id = 99")
        .await
        .expect("projection over NULL column failed");
    let null_rows: usize = null_batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(null_rows, 1);
    let batch = null_batches.iter().find(|b| b.num_rows() > 0).unwrap();
    assert!(string_column(batch, 0).is_null(0));

    std::fs::remove_dir_all(&dir).unwrap();
}
