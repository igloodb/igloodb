//! Integration tests for dynamic catalog introspection.
//!
//! These exercise [`igloo::datafusion_engine::DataFusionEngine`] against a
//! live PostgreSQL: tables are introspected from `information_schema`, one
//! DataFusion table is registered per base table, unsupported column types
//! degrade to a supported subset, tables in non-default schemas resolve, and
//! `SHOW TABLES` lists the catalog.
//!
//! Requires a live PostgreSQL the test may freely create objects in. Set
//! `IGLOO_TEST_POSTGRES_URI` to run, e.g.:
//!
//! ```sh
//! IGLOO_TEST_POSTGRES_URI=postgres://postgres@127.0.0.1:5543/igloo_test \
//!     cargo test --test catalog
//! ```
//!
//! Without the variable every test skips (and says so), keeping plain
//! `cargo test` hermetic. CI provides a Postgres service container.
//!
//! Tests within this file share the database, so they serialize on a single
//! async mutex (mirroring `tests/pgwire_server.rs`). Assertions are
//! "contains"-style so pre-existing tables (e.g. `my_pg_table` created by
//! sibling test binaries) don't break them.

use std::sync::Arc;

use datafusion::arrow::array::{
    Array, BinaryArray, BooleanArray, Date32Array, Float32Array, Float64Array, Int16Array,
    Int32Array, Int64Array, StringArray, TimestampNanosecondArray,
};
use datafusion::arrow::datatypes::{DataType, Field, Schema as ArrowSchema, TimeUnit};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::parquet::arrow::ArrowWriter;
use tokio_postgres::{Client, NoTls};

use igloo::datafusion_engine::DataFusionEngine;

/// Tests here mutate shared catalog state; serialize them within this binary.
static DB_LOCK: std::sync::OnceLock<tokio::sync::Mutex<()>> = std::sync::OnceLock::new();

async fn db_guard() -> tokio::sync::MutexGuard<'static, ()> {
    DB_LOCK
        .get_or_init(|| tokio::sync::Mutex::new(()))
        .lock()
        .await
}

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

fn temp_parquet_dir(tag: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!("igloo_catalog_{}_{}", tag, std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    write_parquet_fixture(&dir);
    dir
}

async fn connect(uri: &str) -> Client {
    let (client, connection) = tokio_postgres::connect(uri, NoTls)
        .await
        .expect("failed to connect to IGLOO_TEST_POSTGRES_URI");
    tokio::spawn(connection);
    client
}

#[tokio::test]
async fn all_supported_types_round_trip_including_nulls() {
    let Ok(uri) = std::env::var("IGLOO_TEST_POSTGRES_URI") else {
        eprintln!("skipping catalog test: IGLOO_TEST_POSTGRES_URI is not set");
        return;
    };
    let _guard = db_guard().await;
    let client = connect(&uri).await;

    client
        .batch_execute(
            "DROP TABLE IF EXISTS cat_all_types;
             CREATE TABLE cat_all_types (
                 c_smallint  smallint,
                 c_integer   integer,
                 c_bigint    bigint NOT NULL,
                 c_real      real,
                 c_double    double precision,
                 c_text      text,
                 c_varchar   varchar(32),
                 c_char      character(4),
                 c_bool      boolean,
                 c_bytea     bytea,
                 c_date      date,
                 c_ts        timestamp without time zone
             );
             INSERT INTO cat_all_types VALUES
                 (1, 2, 3, 4.5, 6.25, 'hi', 'vv', 'abcd', true,
                  '\\xdeadbeef'::bytea, '2021-01-02', '2021-01-02 03:04:05'),
                 (NULL, NULL, 9, NULL, NULL, NULL, NULL, NULL, NULL,
                  NULL, NULL, NULL);",
        )
        .await
        .expect("failed to create cat_all_types");

    let dir = temp_parquet_dir("alltypes");
    let engine = DataFusionEngine::new(dir.to_str().unwrap(), &uri, &["public".to_string()])
        .await
        .expect("engine init failed");

    let batches = engine
        .query("SELECT * FROM cat_all_types ORDER BY c_bigint")
        .await
        .expect("query over introspected table failed");
    let batch = batches.iter().find(|b| b.num_rows() > 0).unwrap();
    assert_eq!(batch.num_rows(), 2);

    // Schema was introspected to the exact supported Arrow types.
    let schema = batch.schema();
    let ty = |name: &str| schema.field_with_name(name).unwrap().data_type().clone();
    assert_eq!(ty("c_smallint"), DataType::Int16);
    assert_eq!(ty("c_integer"), DataType::Int32);
    assert_eq!(ty("c_bigint"), DataType::Int64);
    assert_eq!(ty("c_real"), DataType::Float32);
    assert_eq!(ty("c_double"), DataType::Float64);
    assert_eq!(ty("c_text"), DataType::Utf8);
    assert_eq!(ty("c_varchar"), DataType::Utf8);
    assert_eq!(ty("c_char"), DataType::Utf8);
    assert_eq!(ty("c_bool"), DataType::Boolean);
    assert_eq!(ty("c_bytea"), DataType::Binary);
    assert_eq!(ty("c_date"), DataType::Date32);
    assert_eq!(ty("c_ts"), DataType::Timestamp(TimeUnit::Nanosecond, None));

    // Row 0 (c_bigint = 3): real values.
    let col = |name: &str| batch.column(schema.index_of(name).unwrap());
    assert_eq!(
        col("c_smallint")
            .as_any()
            .downcast_ref::<Int16Array>()
            .unwrap()
            .value(0),
        1
    );
    assert_eq!(
        col("c_integer")
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .value(0),
        2
    );
    assert_eq!(
        col("c_bigint")
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        3
    );
    assert_eq!(
        col("c_real")
            .as_any()
            .downcast_ref::<Float32Array>()
            .unwrap()
            .value(0),
        4.5
    );
    assert_eq!(
        col("c_double")
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .value(0),
        6.25
    );
    assert_eq!(
        col("c_text")
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0),
        "hi"
    );
    assert_eq!(
        col("c_char")
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0),
        "abcd"
    );
    assert!(col("c_bool")
        .as_any()
        .downcast_ref::<BooleanArray>()
        .unwrap()
        .value(0));
    assert_eq!(
        col("c_bytea")
            .as_any()
            .downcast_ref::<BinaryArray>()
            .unwrap()
            .value(0),
        &[0xde, 0xad, 0xbe, 0xef]
    );
    assert!(
        col("c_date")
            .as_any()
            .downcast_ref::<Date32Array>()
            .unwrap()
            .value(0)
            > 0
    );
    assert!(
        col("c_ts")
            .as_any()
            .downcast_ref::<TimestampNanosecondArray>()
            .unwrap()
            .value(0)
            > 0
    );

    // Row 1 (c_bigint = 9): every nullable column is NULL.
    for name in [
        "c_smallint",
        "c_integer",
        "c_real",
        "c_double",
        "c_text",
        "c_varchar",
        "c_char",
        "c_bool",
        "c_bytea",
        "c_date",
        "c_ts",
    ] {
        assert!(col(name).is_null(1), "{} should be NULL in row 1", name);
    }

    client
        .batch_execute("DROP TABLE IF EXISTS cat_all_types;")
        .await
        .unwrap();
    std::fs::remove_dir_all(&dir).unwrap();
}

#[tokio::test]
async fn unsupported_column_is_dropped_but_table_queryable() {
    let Ok(uri) = std::env::var("IGLOO_TEST_POSTGRES_URI") else {
        eprintln!("skipping catalog test: IGLOO_TEST_POSTGRES_URI is not set");
        return;
    };
    let _guard = db_guard().await;
    let client = connect(&uri).await;

    // uuid and jsonb have no Arrow mapping; id and label do.
    client
        .batch_execute(
            "DROP TABLE IF EXISTS cat_mixed;
             CREATE TABLE cat_mixed (
                 id     bigint NOT NULL,
                 uid    uuid,
                 blob   jsonb,
                 label  text
             );
             INSERT INTO cat_mixed (id, uid, blob, label)
             VALUES (1, gen_random_uuid(), '{\"a\":1}'::jsonb, 'keep');",
        )
        .await
        .expect("failed to create cat_mixed");

    let dir = temp_parquet_dir("mixed");
    let engine = DataFusionEngine::new(dir.to_str().unwrap(), &uri, &["public".to_string()])
        .await
        .expect("engine init failed");

    let batches = engine
        .query("SELECT * FROM cat_mixed")
        .await
        .expect("query over supported subset failed");
    let batch = batches.iter().find(|b| b.num_rows() > 0).unwrap();
    let schema = batch.schema();
    let names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
    assert_eq!(names, vec!["id", "label"], "uuid + jsonb columns dropped");
    assert_eq!(
        batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0),
        "keep"
    );

    // Referencing the dropped column is a plan-time error, not a panic.
    let err = engine.query("SELECT uid FROM cat_mixed").await;
    assert!(err.is_err(), "unsupported column must not be selectable");

    client
        .batch_execute("DROP TABLE IF EXISTS cat_mixed;")
        .await
        .unwrap();
    std::fs::remove_dir_all(&dir).unwrap();
}

#[tokio::test]
async fn table_in_second_schema_is_registered_and_queryable() {
    let Ok(uri) = std::env::var("IGLOO_TEST_POSTGRES_URI") else {
        eprintln!("skipping catalog test: IGLOO_TEST_POSTGRES_URI is not set");
        return;
    };
    let _guard = db_guard().await;
    let client = connect(&uri).await;

    client
        .batch_execute(
            "CREATE SCHEMA IF NOT EXISTS cat_other;
             DROP TABLE IF EXISTS cat_other.widgets;
             CREATE TABLE cat_other.widgets (id integer NOT NULL, name text);
             INSERT INTO cat_other.widgets (id, name) VALUES (10, 'sprocket');",
        )
        .await
        .expect("failed to create table in second schema");

    let dir = temp_parquet_dir("secondschema");
    let engine = DataFusionEngine::new(
        dir.to_str().unwrap(),
        &uri,
        &["public".to_string(), "cat_other".to_string()],
    )
    .await
    .expect("engine init failed");

    // The table in the non-default schema is registered under its bare name
    // and its scan resolves (schema-qualified SQL).
    let batches = engine
        .query("SELECT id, name FROM widgets WHERE id = 10")
        .await
        .expect("query over second-schema table failed");
    let batch = batches.iter().find(|b| b.num_rows() > 0).unwrap();
    assert_eq!(
        batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .value(0),
        10
    );
    assert_eq!(
        batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0),
        "sprocket"
    );

    client
        .batch_execute("DROP TABLE IF EXISTS cat_other.widgets;")
        .await
        .unwrap();
    std::fs::remove_dir_all(&dir).unwrap();
}

#[tokio::test]
async fn show_tables_lists_registered_catalog_and_legacy_alias_works() {
    let Ok(uri) = std::env::var("IGLOO_TEST_POSTGRES_URI") else {
        eprintln!("skipping catalog test: IGLOO_TEST_POSTGRES_URI is not set");
        return;
    };
    let _guard = db_guard().await;
    let client = connect(&uri).await;

    client
        .batch_execute(
            "DROP TABLE IF EXISTS my_pg_table;
             CREATE TABLE my_pg_table (user_id bigint NOT NULL, extra_info text);
             INSERT INTO my_pg_table (user_id, extra_info) VALUES (42, 'vip');
             DROP TABLE IF EXISTS cat_show;
             CREATE TABLE cat_show (id integer NOT NULL);
             INSERT INTO cat_show (id) VALUES (1);",
        )
        .await
        .expect("failed to seed tables");

    let dir = temp_parquet_dir("show");
    let engine = DataFusionEngine::new(dir.to_str().unwrap(), &uri, &["public".to_string()])
        .await
        .expect("engine init failed");

    // SHOW TABLES works (information_schema enabled) and lists our tables.
    let batches = engine
        .query("SHOW TABLES")
        .await
        .expect("SHOW TABLES failed");
    let mut listed: Vec<String> = Vec::new();
    for batch in &batches {
        // The table_name column is present regardless of column order.
        let idx = batch.schema().index_of("table_name").unwrap();
        let names = batch
            .column(idx)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        for i in 0..names.len() {
            listed.push(names.value(i).to_string());
        }
    }
    // "contains", not exact — other tables may exist in the database.
    for expected in ["iceberg", "my_pg_table", "cat_show", "pg_table"] {
        assert!(
            listed.iter().any(|n| n == expected),
            "SHOW TABLES missing {:?}; got {:?}",
            expected,
            listed
        );
    }

    // The bare name and the legacy alias resolve to the same upstream table.
    let via_bare = engine
        .query("SELECT extra_info FROM my_pg_table WHERE user_id = 42")
        .await
        .expect("bare-name query failed");
    let via_alias = engine
        .query("SELECT extra_info FROM pg_table WHERE user_id = 42")
        .await
        .expect("legacy alias query failed");
    let value = |batches: &[RecordBatch]| {
        let b = batches.iter().find(|b| b.num_rows() > 0).unwrap();
        b.column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0)
            .to_string()
    };
    assert_eq!(value(&via_bare), "vip");
    assert_eq!(value(&via_alias), "vip");

    client
        .batch_execute("DROP TABLE IF EXISTS cat_show;")
        .await
        .unwrap();
    std::fs::remove_dir_all(&dir).unwrap();
}
