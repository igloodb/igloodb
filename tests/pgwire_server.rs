//! Integration test for the pgwire server: a real PostgreSQL client
//! (tokio-postgres) connects to Igloo over TCP and runs queries.
//!
//! Requires a live PostgreSQL for the engine's catalog registration.
//! Set `IGLOO_TEST_POSTGRES_URI` to run; skips otherwise (CI provides a
//! service container). Every test creates and drops its **own**
//! uniquely-named fixture table, so tests never collide — including across
//! test binaries, which `cargo test` runs in parallel.

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

    // A uniquely-named table exercises catalog registration; queries below
    // only touch the Parquet-backed `iceberg`.
    let (setup, connection) = tokio_postgres::connect(&uri, NoTls).await.unwrap();
    tokio::spawn(connection);
    let table = format!("igloo_pw_{}", std::process::id());
    setup
        .batch_execute(&format!(
            "DROP TABLE IF EXISTS {table};
             CREATE TABLE {table} (user_id BIGINT NOT NULL, extra_info TEXT);"
        ))
        .await
        .unwrap();

    let engine = Arc::new(
        DataFusionEngine::new(dir.to_str().unwrap(), &uri, &["public".to_string()])
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
    let _ = setup
        .batch_execute(&format!("DROP TABLE IF EXISTS {table};"))
        .await;
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

    let base = std::env::temp_dir().join(format!("igloo_cdc_fresh_{}", std::process::id()));
    let parquet_dir = base.join("parquet");
    let cdc_dir = base.join("cdc");
    let _ = std::fs::remove_dir_all(&base);
    std::fs::create_dir_all(&parquet_dir).unwrap();
    std::fs::create_dir_all(&cdc_dir).unwrap();
    write_parquet_fixture(&parquet_dir);

    let (setup, connection) = tokio_postgres::connect(&uri, NoTls).await.unwrap();
    tokio::spawn(connection);
    let table = format!("igloo_cdc_{}", std::process::id());
    setup
        .batch_execute(&format!(
            "DROP TABLE IF EXISTS {table};
             CREATE TABLE {table} (user_id BIGINT NOT NULL, extra_info TEXT);
             INSERT INTO {table} (user_id, extra_info) VALUES (42, 'vip');"
        ))
        .await
        .unwrap();

    let engine = Arc::new(
        DataFusionEngine::new(parquet_dir.to_str().unwrap(), &uri, &["public".to_string()])
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

    let query = format!("SELECT extra_info FROM {table} WHERE user_id = 42");
    let query = query.as_str();
    let value = |messages: &[SimpleQueryMessage]| {
        data_rows(messages)[0].get(0).map(str::to_string).unwrap()
    };

    // Populate the cache, then change the upstream value WITHOUT a CDC
    // event: the cached (stale) value keeps being served.
    assert_eq!(value(&client.simple_query(query).await.unwrap()), "vip");
    setup
        .batch_execute(&format!(
            "UPDATE {table} SET extra_info = 'gold' WHERE user_id = 42"
        ))
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
        format!(r#"{{"table": "{table}", "op": "update"}}"#),
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

/// Extended query protocol (roadmap F1.1): a real Postgres client parses,
/// binds and executes prepared statements with parameters. tokio-postgres
/// uses the extended protocol for `query`/`prepare` and leaves parameter
/// type OIDs unspecified, so this exercises the server's inferred-type
/// binding end to end (binary-encoded values).
#[tokio::test]
async fn extended_protocol_prepared_statements_and_parameters() {
    let Ok(uri) = std::env::var("IGLOO_TEST_POSTGRES_URI") else {
        eprintln!("skipping extended protocol test: IGLOO_TEST_POSTGRES_URI is not set");
        return;
    };

    let base = std::env::temp_dir().join(format!("igloo_extq_{}", std::process::id()));
    let parquet_dir = base.join("parquet");
    let _ = std::fs::remove_dir_all(&base);
    std::fs::create_dir_all(&parquet_dir).unwrap();
    write_parquet_fixture(&parquet_dir);

    let (setup, connection) = tokio_postgres::connect(&uri, NoTls).await.unwrap();
    tokio::spawn(connection);
    let table = format!("igloo_extq_{}", std::process::id());
    setup
        .batch_execute(&format!(
            "DROP TABLE IF EXISTS {table};
             CREATE TABLE {table} (user_id BIGINT NOT NULL, extra_info TEXT);
             INSERT INTO {table} (user_id, extra_info) VALUES
               (42, 'vip'), (7, 'lucky'), (100, NULL);"
        ))
        .await
        .unwrap();

    let engine = Arc::new(
        DataFusionEngine::new(parquet_dir.to_str().unwrap(), &uri, &["public".to_string()])
            .await
            .unwrap(),
    );
    let cache = Arc::new(Cache::new(64, Duration::from_secs(300)));
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

    // 1. Unnamed statement, untyped binary i64 parameter: the server must
    //    infer $1 as BIGINT from the comparison against user_id.
    let rows = client
        .query(
            &format!("SELECT extra_info FROM {table} WHERE user_id = $1"),
            &[&42i64],
        )
        .await
        .expect("parameterized query");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<_, Option<&str>>(0), Some("vip"));

    // 2. A named prepared statement reused with different bindings.
    let stmt = client
        .prepare(&format!(
            "SELECT user_id FROM {table} WHERE extra_info = $1"
        ))
        .await
        .expect("prepare");
    for (needle, expected) in [("vip", 42i64), ("lucky", 7)] {
        let rows = client.query(&stmt, &[&needle]).await.unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].get::<_, i64>(0), expected);
    }
    // A binding with no match returns zero rows, not an error.
    let rows = client.query(&stmt, &[&"absent"]).await.unwrap();
    assert!(rows.is_empty());

    // 3. Explicitly typed prepare (declared OID) also works.
    let typed = client
        .prepare_typed(
            &format!("SELECT extra_info FROM {table} WHERE user_id >= $1"),
            &[tokio_postgres::types::Type::INT8],
        )
        .await
        .expect("typed prepare");
    let rows = client.query(&typed, &[&42i64]).await.unwrap();
    // user_ids 42 ('vip') and 100 (NULL) are both >= 42.
    assert_eq!(rows.len(), 2);
    let infos: Vec<Option<&str>> = rows.iter().map(|r| r.get(0)).collect();
    assert!(infos.contains(&Some("vip")), "got {infos:?}");
    assert!(infos.contains(&None), "NULL column decodes; got {infos:?}");

    // 4. Zero-parameter statements share the cache with the simple path.
    let shared: &str = &format!("SELECT COUNT(*) FROM {table}");
    let before = cache.stats().hits;
    let messages = client.simple_query(shared).await.unwrap(); // populates
    let _ = data_rows(&messages);
    let rows = client.query(shared, &[]).await.unwrap(); // extended hits it
    assert_eq!(rows[0].get::<_, i64>(0), 3);
    assert!(
        cache.stats().hits > before,
        "zero-param extended query must hit the cache populated via simple query"
    );

    // 5. A failed extended query is a clean database error...
    let err = client
        .query("SELECT totally not valid sql !!!", &[&1i64])
        .await
        .expect_err("invalid SQL should error");
    assert!(err.as_db_error().is_some());
    // ...and the connection keeps working afterwards.
    let rows = client
        .query(
            &format!("SELECT user_id FROM {table} WHERE user_id = $1"),
            &[&100i64],
        )
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);

    drop(client);
    client_conn.abort();
    server.abort();
    let _ = setup
        .batch_execute(&format!("DROP TABLE IF EXISTS {table};"))
        .await;
    std::fs::remove_dir_all(&base).unwrap();
}

/// Roadmap F1.1: at least 50 simultaneous connections issuing mixed queries
/// (cache-miss scans, cache-hit repeats, parameterized prepares) all complete
/// with correct results and no deadlocks or errors.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_load_50_connections_mixed_queries() {
    let Ok(uri) = std::env::var("IGLOO_TEST_POSTGRES_URI") else {
        eprintln!("skipping concurrent load test: IGLOO_TEST_POSTGRES_URI is not set");
        return;
    };

    let base = std::env::temp_dir().join(format!("igloo_load_{}", std::process::id()));
    let parquet_dir = base.join("parquet");
    let _ = std::fs::remove_dir_all(&base);
    std::fs::create_dir_all(&parquet_dir).unwrap();
    write_parquet_fixture(&parquet_dir);

    let (setup, connection) = tokio_postgres::connect(&uri, NoTls).await.unwrap();
    tokio::spawn(connection);
    let table = format!("igloo_load_{}", std::process::id());
    setup
        .batch_execute(&format!(
            "DROP TABLE IF EXISTS {table};
             CREATE TABLE {table} (user_id BIGINT NOT NULL, extra_info TEXT);
             INSERT INTO {table} (user_id, extra_info)
             SELECT g, CASE WHEN g % 2 = 0 THEN 'even' ELSE 'odd' END
             FROM generate_series(1, 200) AS g;"
        ))
        .await
        .unwrap();

    let engine = Arc::new(
        DataFusionEngine::new(parquet_dir.to_str().unwrap(), &uri, &["public".to_string()])
            .await
            .unwrap(),
    );
    let cache = Arc::new(Cache::new(256, Duration::from_secs(300)));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(serve_with_listener(engine, cache.clone(), listener));
    let addr = format!("host={} port={}", addr.ip(), addr.port());

    const CONNECTIONS: usize = 50;
    let barrier = Arc::new(tokio::sync::Barrier::new(CONNECTIONS));
    let mut handles = Vec::with_capacity(CONNECTIONS);
    for conn_idx in 0..CONNECTIONS {
        let addr = addr.clone();
        let barrier = barrier.clone();
        let table = table.clone();
        handles.push(tokio::spawn(async move {
            let (client, connection) = tokio_postgres::connect(&addr, NoTls)
                .await
                .expect("connect to igloo");
            let conn_task = tokio::spawn(connection);

            // Fire every connection at once so the server sees real overlap.
            barrier.wait().await;

            for round in 0..10u32 {
                match (conn_idx + round as usize) % 3 {
                    // Parameterized scan through a prepared statement.
                    0 => {
                        let stmt = client
                            .prepare(&format!("SELECT user_id FROM {table} WHERE user_id = $1"))
                            .await
                            .expect("prepare");
                        let id = ((conn_idx * round as usize) % 200 + 1) as i64;
                        let rows = client.query(&stmt, &[&id]).await.expect("param query");
                        assert_eq!(rows.len(), 1);
                        assert_eq!(rows[0].get::<_, i64>(0), id);
                    }
                    // Repeated aggregate — exercises the cache-hit path.
                    1 => {
                        let rows = client
                            .query(
                                &format!("SELECT COUNT(*) FROM {table} WHERE user_id <= 100"),
                                &[],
                            )
                            .await
                            .expect("count query");
                        assert_eq!(rows[0].get::<_, i64>(0), 100);
                    }
                    // Federated join over Parquet ⋈ Postgres.
                    _ => {
                        let rows = client
                            .query(
                                &format!(
                                    "SELECT iceberg.data FROM iceberg \
                                     JOIN {table} p ON iceberg.user_id = p.user_id \
                                     WHERE iceberg.user_id = $1"
                                ),
                                &[&42i64],
                            )
                            .await
                            .expect("join query");
                        assert_eq!(rows.len(), 1);
                        assert_eq!(rows[0].get::<_, &str>(0), "hello");
                    }
                }
            }

            drop(client);
            conn_task.abort();
        }));
    }

    for handle in handles {
        handle.await.expect("connection task panicked");
    }

    server.abort();
    std::fs::remove_dir_all(&base).unwrap();
}
