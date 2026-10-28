use std::process::Command;
use std::time::Duration;

use igloo::postgres_cdc::{CdcHealth, PgCdcConfig, PgCdcError, PgCdcSource, SlotMode};
use pgwire_replication::config::{ReplicationConfig, TlsConfig};
use tokio_postgres::config::Host;
use tokio_postgres::{Client, NoTls};
use uuid::Uuid;

fn replication_config(uri: &str, slot: &str, publication: &str) -> ReplicationConfig {
    let parsed = uri
        .parse::<tokio_postgres::Config>()
        .expect("test PostgreSQL URI must parse");
    let host = match parsed
        .get_hosts()
        .first()
        .expect("test PostgreSQL URI must provide a host")
    {
        Host::Tcp(host) => host.clone(),
        Host::Unix(_) => panic!("test PostgreSQL URI must use a TCP host"),
    };
    let user = parsed.get_user().unwrap_or("postgres");
    let password = std::str::from_utf8(parsed.get_password().unwrap_or_default())
        .expect("test PostgreSQL password must be UTF-8");
    let database = parsed.get_dbname().unwrap_or("postgres");
    let port = parsed.get_ports().first().copied().unwrap_or(5432);
    ReplicationConfig::new(host, user, password, database, slot, publication)
        .with_port(port)
        .with_tls(TlsConfig::disabled())
        .with_application_name(format!("igloo_cdc_test_{}", std::process::id()))
        .with_wakeup_interval(Duration::from_millis(20))
}

async fn connect(uri: &str) -> Client {
    let (client, connection) = tokio_postgres::connect(uri, NoTls).await.unwrap();
    tokio::spawn(async move {
        let _ = connection.await;
    });
    client
}

#[tokio::test]
async fn temporary_pgoutput_slot_tracks_committed_dml_markers_and_disconnect() {
    let Ok(uri) = std::env::var("IGLOO_TEST_POSTGRES_URI") else {
        eprintln!("skipping PostgreSQL CDC integration: IGLOO_TEST_POSTGRES_URI is unset");
        return;
    };
    let client = connect(&uri).await;
    let wal_level: String = client
        .query_one("SHOW wal_level", &[])
        .await
        .unwrap()
        .get(0);
    assert_eq!(wal_level, "logical", "CDC integration requires logical WAL");

    let suffix = Uuid::new_v4().simple().to_string();
    let table = format!("igloo_cdc_fixture_{suffix}");
    let publication = format!("igloo_cdc_pub_{suffix}");
    let slot = format!("igloo_cdc_slot_{suffix}");
    client
        .batch_execute(&format!(
            "CREATE TABLE {table} (id BIGINT PRIMARY KEY, value TEXT);
             CREATE PUBLICATION {publication} FOR TABLE {table};"
        ))
        .await
        .unwrap();

    let replication = replication_config(&uri, &slot, &publication);
    let source = match PgCdcSource::start(PgCdcConfig {
        replication,
        slot_mode: SlotMode::Temporary,
        barrier_timeout: Duration::from_secs(2),
    })
    .await
    {
        Ok(source) => source,
        Err(error) => {
            let _ = client
                .batch_execute(&format!(
                    "DROP PUBLICATION IF EXISTS {publication}; DROP TABLE IF EXISTS {table};"
                ))
                .await;
            panic!("PostgreSQL CDC integration setup failed: {error}");
        }
    };
    let handle = source.handle();
    let initial = handle.epoch();
    let mut marker_only = handle.barrier(&client).await.unwrap();
    assert_eq!(marker_only.committed_epoch, initial.committed_epoch);

    for sql in [
        format!("INSERT INTO {table} VALUES (1, 'one')"),
        format!("UPDATE {table} SET value = 'two' WHERE id = 1"),
        format!("DELETE FROM {table} WHERE id = 1"),
        format!("INSERT INTO {table} VALUES (2, 'two')"),
        format!("TRUNCATE {table}"),
    ] {
        client.batch_execute(&sql).await.unwrap();
        let after = handle.barrier(&client).await.unwrap();
        assert_eq!(after.committed_epoch, marker_only.committed_epoch + 1);
        marker_only = after;
    }

    let before_rollback = handle.epoch();
    client
        .batch_execute(&format!(
            "BEGIN; INSERT INTO {table} VALUES (3, 'rolled back'); ROLLBACK;"
        ))
        .await
        .unwrap();
    let after_rollback = handle.barrier(&client).await.unwrap();
    assert_eq!(
        after_rollback.committed_epoch,
        before_rollback.committed_epoch
    );

    // An uncommitted caller transaction keeps the marker uncommitted. The
    // bounded barrier must fail closed and invalidate this generation.
    client.batch_execute("BEGIN").await.unwrap();
    let timeout_error = handle.barrier(&client).await.unwrap_err();
    assert_eq!(timeout_error, PgCdcError::BarrierTimeout);
    client.batch_execute("ROLLBACK").await.unwrap();
    assert_eq!(handle.health(), CdcHealth::Unhealthy);

    let generation_before_shutdown = after_rollback.generation;
    drop(source);
    assert_eq!(handle.health(), CdcHealth::Unhealthy);
    assert_ne!(handle.epoch().generation, generation_before_shutdown);

    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            let remaining: i64 = client
                .query_one(
                    "SELECT count(*) FROM pg_replication_slots WHERE slot_name = $1 AND temporary",
                    &[&slot],
                )
                .await
                .unwrap()
                .get(0);
            if remaining == 0 {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("dropped source must release its temporary slot");

    client
        .batch_execute(&format!(
            "DROP PUBLICATION IF EXISTS {publication}; DROP TABLE IF EXISTS {table};"
        ))
        .await
        .unwrap();
}

#[tokio::test]
async fn cdc_check_cli_uses_source_env_and_cleans_up_temporary_slot() {
    let Ok(uri) = std::env::var("IGLOO_TEST_POSTGRES_URI") else {
        eprintln!("skipping cdc-check process integration: IGLOO_TEST_POSTGRES_URI is unset");
        return;
    };
    let _ = replication_config(&uri, "placeholder", "placeholder");
    let client = connect(&uri).await;
    let wal_level: String = client
        .query_one("SHOW wal_level", &[])
        .await
        .unwrap()
        .get(0);
    assert_eq!(
        wal_level, "logical",
        "cdc-check integration requires logical WAL"
    );

    let suffix = Uuid::new_v4().simple().to_string();
    let table = format!("igloo_cdc_cli_fixture_{suffix}");
    let publication = format!("igloo_cdc_cli_pub_{suffix}");
    client
        .batch_execute(&format!(
            "CREATE TABLE {table} (id BIGINT PRIMARY KEY);
             CREATE PUBLICATION {publication} FOR TABLE {table};"
        ))
        .await
        .unwrap();

    let config_path = std::env::temp_dir().join(format!("igloo_cdc_cli_{suffix}.toml"));
    std::fs::write(
        &config_path,
        "[[sources]]\nname = 'app'\nschemas = ['public']\n",
    )
    .unwrap();
    let slots_before: i64 = client
        .query_one(
            "SELECT count(*) FROM pg_replication_slots WHERE temporary AND slot_name LIKE 'igloo_probe_%'",
            &[],
        )
        .await
        .unwrap()
        .get(0);
    let binary = env!("CARGO_BIN_EXE_igloo").to_owned();
    let config_for_child = config_path.clone();
    let uri_for_child = uri.clone();
    let publication_for_child = publication.clone();
    let output = tokio::task::spawn_blocking(move || {
        Command::new(binary)
            .args([
                "cdc-check",
                "--source",
                "app",
                "--publication",
                &publication_for_child,
                "--timeout-ms",
                "30000",
            ])
            .env_clear()
            .env("IGLOO_CONFIG", config_for_child)
            .env("IGLOO_SOURCE_APP_URI", uri_for_child)
            .env("PATH", std::env::var_os("PATH").unwrap_or_default())
            .output()
    })
    .await
    .unwrap()
    .unwrap();
    let slots_after: i64 = client
        .query_one(
            "SELECT count(*) FROM pg_replication_slots WHERE temporary AND slot_name LIKE 'igloo_probe_%'",
            &[],
        )
        .await
        .unwrap()
        .get(0);
    let cleanup = client
        .batch_execute(&format!(
            "DROP PUBLICATION IF EXISTS {publication}; DROP TABLE IF EXISTS {table};"
        ))
        .await;
    let _ = std::fs::remove_file(config_path);

    assert!(cleanup.is_ok(), "fixture cleanup failed");
    assert!(
        output.status.success(),
        "cdc-check failed with status {:?}",
        output.status.code()
    );
    let response: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(response["ok"], true);
    assert_eq!(response["source"], "app");
    assert_eq!(response["barrier"], "committed");
    assert_eq!(response["slot"], "temporary");
    assert_eq!(response["query_cache_integration"], false);
    assert_eq!(response["cross_source_snapshot"], false);
    assert_eq!(
        slots_after, slots_before,
        "temporary slot must be cleaned up"
    );
}

#[test]
fn config_debug_and_errors_do_not_expose_password() {
    let secret = "test-password-that-must-not-leak";
    let config = PgCdcConfig {
        replication: ReplicationConfig::new(
            "db.example.test",
            "replicator",
            secret,
            "app",
            "slot",
            "publication",
        ),
        slot_mode: SlotMode::Existing,
        barrier_timeout: Duration::from_secs(1),
    };
    assert!(!format!("{config:?}").contains(secret));
    assert!(!PgCdcError::ReplicationConnection
        .to_string()
        .contains(secret));
}
