//! Integration test for the pgwire server: a real PostgreSQL client
//! (tokio-postgres) connects to Igloo over TCP and runs queries.
//!
//! Requires a live PostgreSQL for the engine's `pg_table` registration.
//! Set `IGLOO_TEST_POSTGRES_URI` to run; skips otherwise (CI provides a
//! service container).

use std::sync::Arc;
use std::time::Duration;

use datafusion::arrow::array::{Int64Array, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::parquet::arrow::ArrowWriter;
use tokio_postgres::{NoTls, SimpleQueryMessage};

use igloo::cache_layer::Cache;
use igloo::cdc_sync::CdcListener;
use igloo::datafusion_engine::DataFusionEngine;
use igloo::server::serve_with_listener;

/// Both tests manipulate the shared `my_pg_table`; serialize them so DDL
/// from one can't race the other within this test binary.
static PG_TABLE_LOCK: std::sync::OnceLock<tokio::sync::Mutex<()>> = std::sync::OnceLock::new();

async fn pg_table_guard() -> tokio::sync::MutexGuard<'static, ()> {
    PG_TABLE_LOCK
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
    let _guard = pg_table_guard().await;

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
    let cache = Arc::new(Cache::new(64, Duration::from_secs(300)));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(serve_with_listener(engine, cache, listener));

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

/// End-to-end freshness: a cached result served over pgwire is refreshed
/// after an upstream change signalled by a CDC event.
#[tokio::test]
async fn cdc_event_refreshes_cached_pgwire_results() {
    let Ok(uri) = std::env::var("IGLOO_TEST_POSTGRES_URI") else {
        eprintln!("skipping cdc freshness test: IGLOO_TEST_POSTGRES_URI is not set");
        return;
    };
    let _guard = pg_table_guard().await;

    let base = std::env::temp_dir().join(format!("igloo_cdc_fresh_{}", std::process::id()));
    let parquet_dir = base.join("parquet");
    let cdc_dir = base.join("cdc");
    let _ = std::fs::remove_dir_all(&base);
    std::fs::create_dir_all(&parquet_dir).unwrap();
    std::fs::create_dir_all(&cdc_dir).unwrap();
    write_parquet_fixture(&parquet_dir);

    let (setup, connection) = tokio_postgres::connect(&uri, NoTls).await.unwrap();
    tokio::spawn(connection);
    setup
        .batch_execute(
            "DROP TABLE IF EXISTS my_pg_table;
             CREATE TABLE my_pg_table (user_id BIGINT NOT NULL, extra_info TEXT);
             INSERT INTO my_pg_table (user_id, extra_info) VALUES (42, 'vip');",
        )
        .await
        .unwrap();

    let engine = Arc::new(
        DataFusionEngine::new(parquet_dir.to_str().unwrap(), &uri)
            .await
            .unwrap(),
    );
    let cache = Arc::new(Cache::new(64, Duration::from_secs(300)));
    let cdc = Arc::new(CdcListener::new(cdc_dir.to_str().unwrap()));
    cdc.spawn_polling(cache.clone(), Duration::from_millis(200));

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(serve_with_listener(engine, cache.clone(), listener));

    let (client, connection) = tokio_postgres::connect(
        &format!("host={} port={} user=igloo", addr.ip(), addr.port()),
        NoTls,
    )
    .await
    .unwrap();
    let client_conn = tokio::spawn(connection);

    let query = "SELECT extra_info FROM pg_table WHERE user_id = 42";
    let value = |messages: &[SimpleQueryMessage]| {
        data_rows(messages)[0].get(0).map(str::to_string).unwrap()
    };

    // Populate the cache, then change the upstream value WITHOUT a CDC
    // event: the cached (stale) value keeps being served.
    assert_eq!(value(&client.simple_query(query).await.unwrap()), "vip");
    setup
        .batch_execute("UPDATE my_pg_table SET extra_info = 'gold' WHERE user_id = 42")
        .await
        .unwrap();
    assert_eq!(
        value(&client.simple_query(query).await.unwrap()),
        "vip",
        "without a CDC event the cached result is served"
    );

    // A CDC event lands: within a few poll intervals the cache is
    // invalidated and the fresh value is served.
    std::fs::write(
        cdc_dir.join("event_update.json"),
        r#"{"table": "my_pg_table", "op": "update"}"#,
    )
    .unwrap();
    let mut fresh = String::new();
    for _ in 0..50 {
        tokio::time::sleep(Duration::from_millis(200)).await;
        fresh = value(&client.simple_query(query).await.unwrap());
        if fresh == "gold" {
            break;
        }
    }
    assert_eq!(fresh, "gold", "CDC event must refresh the served result");

    drop(client);
    client_conn.abort();
    server.abort();
    std::fs::remove_dir_all(&base).unwrap();
}
