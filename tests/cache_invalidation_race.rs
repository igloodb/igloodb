//! Wire-level regression for cache invalidation during an upstream scan.
//!
//! Requires the disposable PostgreSQL configured by IGLOO_TEST_POSTGRES_URI;
//! the test only creates and removes a uniquely named allowlisted table.

use std::sync::Arc;
use std::time::Duration;

use igloo::cache_layer::Cache;
use igloo::config::PostgresSource;
use igloo::datafusion_engine::DataFusionEngine;
use igloo::server::serve_with_listener;
use tokio::net::TcpListener;
use tokio_postgres::{Client, NoTls, SimpleQueryMessage};
use uuid::Uuid;

const BLOCK_TIMEOUT: Duration = Duration::from_secs(15);

async fn connect(uri: &str) -> Client {
    let (client, connection) = tokio_postgres::connect(uri, NoTls)
        .await
        .expect("connect to test PostgreSQL");
    tokio::spawn(async move {
        let _ = connection.await;
    });
    client
}

async fn wait_for_blocked_scan(observer: &Client, table: &str) -> Result<(), String> {
    let pattern = format!("%{table}%");
    tokio::time::timeout(BLOCK_TIMEOUT, async {
        loop {
            let blocked: bool = observer
                .query_one(
                    "SELECT EXISTS (
                       SELECT 1 FROM pg_stat_activity
                       WHERE pid <> pg_backend_pid()
                         AND state = 'active'
                         AND wait_event_type = 'Lock'
                         AND query LIKE $1
                     )",
                    &[&pattern],
                )
                .await
                .map_err(|error| error.to_string())?
                .get(0);
            if blocked {
                return Ok(());
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .map_err(|_| format!("PostgreSQL scan of {table} did not block on the held table lock"))?
}

fn simple_value(messages: &[SimpleQueryMessage]) -> Result<i64, String> {
    let row = messages
        .iter()
        .find_map(|message| match message {
            SimpleQueryMessage::Row(row) => Some(row),
            _ => None,
        })
        .ok_or_else(|| "simple query returned no row".to_string())?;
    row.get(0)
        .ok_or_else(|| "simple query returned a NULL value".to_string())?
        .parse::<i64>()
        .map_err(|error| error.to_string())
}

async fn race_simple_query(
    client: Arc<tokio_postgres::Client>,
    observer: &Client,
    lock_holder: &Client,
    cache: &Cache,
    table: &str,
) -> Result<(), String> {
    let query = format!("SELECT id FROM {table} WHERE id = 41");
    lock_holder
        .batch_execute(&format!(
            "BEGIN; LOCK TABLE {table} IN ACCESS EXCLUSIVE MODE"
        ))
        .await
        .map_err(|error| error.to_string())?;

    let query_client = client.clone();
    let query_sql = query.clone();
    let query_task = tokio::spawn(async move { query_client.simple_query(&query_sql).await });
    let result = async {
        wait_for_blocked_scan(observer, table).await?;
        cache.clear();
        lock_holder
            .batch_execute("COMMIT")
            .await
            .map_err(|error| error.to_string())?;
        let messages = tokio::time::timeout(BLOCK_TIMEOUT, query_task)
            .await
            .map_err(|_| {
                "simple pgwire query timed out after releasing the table lock".to_string()
            })?
            .map_err(|error| error.to_string())?
            .map_err(|error| error.to_string())?;
        let value = simple_value(&messages)?;
        if value != 41 {
            return Err(format!("simple pgwire query returned {value}, expected 41"));
        }
        if !cache.is_empty() {
            return Err("late simple-query fill repopulated the cleared cache".to_string());
        }

        let refreshed = client
            .simple_query(&query)
            .await
            .map_err(|error| error.to_string())?;
        if simple_value(&refreshed)? != 41 || cache.is_empty() {
            return Err("simple-query retry did not return and cache a fresh result".to_string());
        }
        Ok(())
    }
    .await;

    if result.is_err() {
        let _ = lock_holder.batch_execute("ROLLBACK").await;
    }
    result
}

async fn race_zero_parameter_extended_query(
    client: Arc<tokio_postgres::Client>,
    observer: &Client,
    lock_holder: &Client,
    cache: &Cache,
    table: &str,
) -> Result<(), String> {
    let query = format!("SELECT id FROM {table} WHERE id = 42");
    lock_holder
        .batch_execute(&format!(
            "BEGIN; LOCK TABLE {table} IN ACCESS EXCLUSIVE MODE"
        ))
        .await
        .map_err(|error| error.to_string())?;

    let query_client = client.clone();
    let query_sql = query.clone();
    let query_task = tokio::spawn(async move { query_client.query(&query_sql, &[]).await });
    let result = async {
        wait_for_blocked_scan(observer, table).await?;
        cache.clear();
        lock_holder
            .batch_execute("COMMIT")
            .await
            .map_err(|error| error.to_string())?;
        let rows = tokio::time::timeout(BLOCK_TIMEOUT, query_task)
            .await
            .map_err(|_| {
                "extended pgwire query timed out after releasing the table lock".to_string()
            })?
            .map_err(|error| error.to_string())?
            .map_err(|error| error.to_string())?;
        if rows.len() != 1 || rows[0].get::<_, i64>(0) != 42 {
            return Err("zero-parameter extended query returned an incorrect result".to_string());
        }
        if !cache.is_empty() {
            return Err("late extended-query fill repopulated the cleared cache".to_string());
        }

        let refreshed = client
            .query(&query, &[])
            .await
            .map_err(|error| error.to_string())?;
        if refreshed.len() != 1 || refreshed[0].get::<_, i64>(0) != 42 || cache.is_empty() {
            return Err("extended-query retry did not return and cache a fresh result".to_string());
        }
        Ok(())
    }
    .await;

    if result.is_err() {
        let _ = lock_holder.batch_execute("ROLLBACK").await;
    }
    result
}

#[tokio::test]
async fn pgwire_inflight_scans_cannot_refill_cache_after_clear() {
    let Ok(uri) = std::env::var("IGLOO_TEST_POSTGRES_URI") else {
        eprintln!("skipping cache invalidation race: IGLOO_TEST_POSTGRES_URI is not set");
        return;
    };

    let suffix = Uuid::new_v4().simple().to_string();
    let table = format!("igloo_cache_race_{suffix}");
    let parquet_dir = std::env::temp_dir().join(format!("igloo_cache_race_{suffix}"));
    std::fs::create_dir_all(&parquet_dir).expect("create empty parquet directory");

    let setup = connect(&uri).await;
    let observer = connect(&uri).await;
    setup
        .batch_execute(&format!(
            "CREATE TABLE {table} (id BIGINT PRIMARY KEY);
             INSERT INTO {table} (id) VALUES (41), (42);"
        ))
        .await
        .expect("create unique cache-race table");

    let source = PostgresSource::new("cache_race", uri.clone(), vec!["public".to_string()])
        .with_tables(vec![table.clone()]);
    let engine = Arc::new(
        DataFusionEngine::new(parquet_dir.to_str().expect("UTF-8 temp path"), &[source])
            .await
            .expect("initialize engine and introspect allowlisted table"),
    );
    let cache = Arc::new(Cache::new(16, Duration::from_secs(300)));
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind pgwire");
    let addr = listener.local_addr().expect("pgwire address");
    let server = tokio::spawn(serve_with_listener(engine, cache.clone(), listener));
    let (client, client_connection) = tokio_postgres::connect(
        &format!("host={} port={} user=igloo", addr.ip(), addr.port()),
        NoTls,
    )
    .await
    .expect("connect pgwire client");
    let client = Arc::new(client);
    let client_task = tokio::spawn(client_connection);

    let simple_result = race_simple_query(client.clone(), &observer, &setup, &cache, &table).await;
    let mut result = simple_result;
    if result.is_ok() {
        cache.clear();
        result =
            race_zero_parameter_extended_query(client.clone(), &observer, &setup, &cache, &table)
                .await;
    }

    server.abort();
    client_task.abort();
    let _ = setup.batch_execute("ROLLBACK").await;
    let cleanup = setup
        .batch_execute(&format!("DROP TABLE IF EXISTS {table};"))
        .await;
    let _ = std::fs::remove_dir_all(&parquet_dir);

    assert!(result.is_ok(), "cache invalidation race failed: {result:?}");
    cleanup.expect("drop unique cache-race table");
}
