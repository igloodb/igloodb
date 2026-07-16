//! Integration test for the pgwire server: a real PostgreSQL client
//! (tokio-postgres) connects to Igloo over TCP and runs queries.
//!
//! Requires a live PostgreSQL for the engine's `pg_table` registration.
//! Set `IGLOO_TEST_POSTGRES_URI` to run; skips otherwise (CI provides a
//! service container).

use std::sync::Arc;

use datafusion::arrow::array::{Int64Array, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::parquet::arrow::ArrowWriter;
use tokio_postgres::{NoTls, SimpleQueryMessage};

use igloo::datafusion_engine::DataFusionEngine;
use igloo::server::serve_with_listener;

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

fn data_rows(messages: &[SimpleQueryMessage]) -> Vec<&tokio_postgres::SimpleQueryRow> {
    messages
        .iter()
        .filter_map(|m| match m {
            SimpleQueryMessage::Row(row) => Some(row),
            _ => None,
        })
        .collect()
}

#[tokio::test]
async fn pgwire_client_queries_and_survives_errors() {
    let Ok(uri) = std::env::var("IGLOO_TEST_POSTGRES_URI") else {
        eprintln!("skipping pgwire_server: IGLOO_TEST_POSTGRES_URI is not set");
        return;
    };

    let dir = std::env::temp_dir().join(format!("igloo_pgwire_test_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    write_parquet_fixture(&dir);

    // The engine needs my_pg_table to exist for registration.
    let (setup, connection) = tokio_postgres::connect(&uri, NoTls).await.unwrap();
    tokio::spawn(connection);
    setup
        .batch_execute(
            "CREATE TABLE IF NOT EXISTS my_pg_table (user_id BIGINT NOT NULL, extra_info TEXT);",
        )
        .await
        .unwrap();

    let engine = Arc::new(
        DataFusionEngine::new(dir.to_str().unwrap(), &uri)
            .await
            .expect("engine init failed"),
    );

    // Bind port 0 so parallel test runs never collide, then serve.
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(serve_with_listener(engine, listener));

    let (client, connection) = tokio_postgres::connect(
        &format!("host={} port={} user=igloo", addr.ip(), addr.port()),
        NoTls,
    )
    .await
    .expect("client failed to connect to igloo pgwire server");
    let client_conn = tokio::spawn(connection);

    // 1. A valid query over the parquet-backed table returns correct rows.
    let messages = client
        .simple_query("SELECT user_id, data FROM iceberg ORDER BY user_id")
        .await
        .expect("valid query failed");
    let rows = data_rows(&messages);
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].get(0), Some("7"));
    assert_eq!(rows[0].get(1), Some("world"));
    assert_eq!(rows[1].get(0), Some("42"));
    assert_eq!(rows[1].get(1), Some("hello"));

    // 2. An invalid query returns an error...
    let err = client
        .simple_query("SELECT definitely not valid sql !!!")
        .await
        .expect_err("invalid SQL should produce an error response");
    assert!(err.as_db_error().is_some(), "expected a database error");

    // 3. ...and the SAME connection still works afterwards.
    let messages = client
        .simple_query("SELECT data FROM iceberg WHERE user_id = 42")
        .await
        .expect("connection should survive a failed query");
    let rows = data_rows(&messages);
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get(0), Some("hello"));

    drop(client);
    client_conn.abort();
    server.abort();
    std::fs::remove_dir_all(&dir).unwrap();
}
