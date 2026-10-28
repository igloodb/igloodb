use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};

use adbc_core::driver_manager::ManagedDriver;
use adbc_core::options::{AdbcVersion, OptionDatabase};
use adbc_core::{Connection, Database, Driver, Statement};
use igloo::agent::{AgentSession, QueryRequest, SchemaRequest};
use igloo::config::Config;
use igloo::datafusion_engine::DataFusionEngine;
use serde_json::json;

static NEXT: AtomicUsize = AtomicUsize::new(0);

fn temp(name: &str) -> PathBuf {
    std::env::temp_dir().join(format!(
        "igloo_connectors_{name}_{}_{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ))
}

fn sqlite_driver() -> Option<String> {
    let value = std::env::var("IGLOO_TEST_ADBC_SQLITE_DRIVER").ok();
    if value.is_none() {
        eprintln!("skipping SQLite driver integration: set IGLOO_TEST_ADBC_SQLITE_DRIVER");
    }
    value
}

fn sqlite_config(driver: &str, database: &std::path::Path, extra: &str) -> String {
    format!(
        "[[adbc_sources]]\nname = 'local_db'\ndriver = {}\nuri = {}\n{extra}\n",
        json!(driver),
        json!(database.to_str().unwrap())
    )
}

fn create_sqlite(driver: &str, database: &std::path::Path) {
    let mut driver =
        ManagedDriver::load_dynamic_from_filename(driver, None, AdbcVersion::V110).unwrap();
    let mut db = driver
        .new_database_with_opts([(OptionDatabase::Uri, database.to_str().unwrap().into())])
        .unwrap();
    let mut connection = db.new_connection().unwrap();
    let mut statement = connection.new_statement().unwrap();
    for sql in [
        "CREATE TABLE customers (id INTEGER, name TEXT, amount REAL)",
        "INSERT INTO customers VALUES (1, 'Alex', 12.5), (2, NULL, 20), (3, 'Sam', NULL)",
        "CREATE TABLE hidden_table (secret TEXT)",
        "CREATE TABLE \"odd\"\"table\" (\"select\" INTEGER)",
        "INSERT INTO \"odd\"\"table\" VALUES (42)",
    ] {
        statement.set_sql_query(sql).unwrap();
        statement.execute_update().unwrap();
    }
}

fn query(sql: &str, max_rows: usize) -> QueryRequest {
    QueryRequest {
        sql: sql.into(),
        max_rows: Some(max_rows),
        timeout_ms: None,
    }
}

#[test]
fn config_supports_non_postgres_sources_and_redacts_secrets() {
    let config = Config::from_toml(
        r#"
        [[adbc_sources]]
        name = "warehouse"
        driver = "/trusted/driver.so"
        [adbc_sources.options_env]
        password = "WAREHOUSE_PASSWORD"
    "#,
        |key| match key {
            "WAREHOUSE_PASSWORD" => Some("never-print-me".into()),
            "DATABASE_URL" => Some("postgres://must-not-connect/unrelated".into()),
            _ => None,
        },
    )
    .unwrap();
    assert!(config.postgres_sources.is_empty());
    assert!(config.primary_source().is_none());
    assert!(config.parquet_path.is_none());
    assert!(config.cdc_path.is_none());
    assert_eq!(
        config.adbc_sources[0].options["password"].expose(),
        "never-print-me"
    );
    assert!(!format!("{config:?}").contains("never-print-me"));
    let error = Config::from_toml("password = 'never-print-me'", |_| None)
        .unwrap_err()
        .to_string();
    assert!(!error.contains("never-print-me"));
    let config_path = temp("bad_driver_config").with_extension("toml");
    std::fs::write(&config_path, "[[adbc_sources]]\nname='broken'\ndriver='/no-such-driver.so'\n[adbc_sources.options]\npassword='never-print-me'").unwrap();
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_igloo"))
        .args(["schema"])
        .env("IGLOO_CONFIG", &config_path)
        .output()
        .unwrap();
    assert_eq!(output.status.code(), Some(2));
    let response: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert!(response["error"]["message"]
        .as_str()
        .unwrap()
        .contains("driver loading"));
    assert!(!response.to_string().contains("never-print-me"));
    std::fs::remove_file(config_path).unwrap();
    for extra in ["tables = []", "schemas = ['']", "entrypoint = ''"] {
        assert!(Config::from_toml(
            &sqlite_config("/driver.so", std::path::Path::new("/db.sqlite"), extra),
            |_| None
        )
        .is_err());
    }
    let duplicates = "[[adbc_sources]]\nname='same'\ndriver='/a.so'\n[[parquet_sources]]\nname='same'\ntables=[{name='t',path='/x.parquet'}]";
    assert!(Config::from_toml(duplicates, |_| None)
        .unwrap_err()
        .to_string()
        .contains("duplicate"));
    assert!(Config::from_toml(
        "[[adbc_sources]]\nname='datafusion'\ndriver='/driver.so'",
        |_| None
    )
    .is_err());
}

#[test]
fn identifiers_and_inventory_do_not_overstate_coverage() {
    use igloo::connectors::IdentifierDialect;
    assert_eq!(IdentifierDialect::Ansi.quote("a\"b").unwrap(), "\"a\"\"b\"");
    assert_eq!(IdentifierDialect::Mysql.quote("a`b").unwrap(), "`a``b`");
    assert_eq!(IdentifierDialect::Mssql.quote("a]b").unwrap(), "[a]]b]");
    assert!(IdentifierDialect::Ansi.quote("bad\0name").is_err());
    let inventory = igloo::connectors::inventory();
    assert_eq!(inventory["routes"][1]["status"], "experimental");
    assert_eq!(inventory["routes"][4]["status"], "planned");
    assert!(inventory["adbc_candidates"]["compatibility"]
        .as_str()
        .unwrap()
        .contains("unverified"));
}

#[tokio::test]
async fn sqlite_metadata_projection_nulls_filters_count_and_hostile_identifiers() {
    let Some(driver) = sqlite_driver() else {
        return;
    };
    let path = temp("sqlite");
    create_sqlite(&driver, &path);
    let config = Config::from_toml(
        &sqlite_config(&driver, &path, "tables = ['customers', 'odd\"table']"),
        |_| None,
    )
    .unwrap();
    let session = AgentSession::new(DataFusionEngine::from_config(&config).await.unwrap(), false);
    let schema = session.schema(SchemaRequest::default()).await.unwrap();
    assert_eq!(schema["tables"].as_array().unwrap().len(), 2);
    assert!(schema["tables"]
        .as_array()
        .unwrap()
        .iter()
        .all(|table| table["connector"] == "adbc"));
    let result = session
        .query(
            query(
                "SELECT id, name FROM local_db.public.customers ORDER BY id",
                2,
            ),
            false,
        )
        .await
        .unwrap();
    assert_eq!(
        result["rows"],
        json!([{"id":"1","name":"Alex"},{"id":"2","name":null}])
    );
    assert_eq!(result["truncated"], true);
    let count = session
        .query(
            query("SELECT count(*) AS n FROM local_db.public.customers", 10),
            false,
        )
        .await
        .unwrap();
    assert_eq!(count["rows"][0]["n"], "3");
    let filtered = session
        .query(
            query("SELECT name FROM local_db.public.customers WHERE id = 3", 1),
            false,
        )
        .await
        .unwrap();
    assert_eq!(filtered["rows"][0]["name"], "Sam");
    let hostile = session
        .query(
            query("SELECT \"select\" FROM local_db.public.\"odd\"\"table\"", 1),
            false,
        )
        .await
        .unwrap();
    assert_eq!(hostile["rows"][0]["select"], "42");
    assert!(session
        .query(query("DROP TABLE local_db.public.customers", 10), false)
        .await
        .is_err());
    let explain = session
        .query(query("SELECT id FROM local_db.public.customers", 10), true)
        .await
        .unwrap();
    assert_eq!(explain["executed"], false);
    drop(session);
    std::fs::remove_file(path).unwrap();
}

#[tokio::test]
async fn parquet_infers_general_schemas_and_joins_with_sqlite() {
    use arrow::array::{Float64Array, Int64Array};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use datafusion::parquet::arrow::ArrowWriter;
    use std::sync::Arc;
    let Some(driver) = sqlite_driver() else {
        return;
    };
    let database = temp("join");
    create_sqlite(&driver, &database);
    let parquet = temp("snapshot").with_extension("parquet");
    let schema = Arc::new(Schema::new(vec![
        Field::new("customer_id", DataType::Int64, false),
        Field::new("total", DataType::Float64, false),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![1, 2])),
            Arc::new(Float64Array::from(vec![125.0, 75.0])),
        ],
    )
    .unwrap();
    let mut writer =
        ArrowWriter::try_new(std::fs::File::create(&parquet).unwrap(), schema, None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    let text = format!(
        "{}\n[[parquet_sources]]\nname='lake'\ntables=[{{name='orders',path={}}}]",
        sqlite_config(&driver, &database, "tables=['customers']"),
        json!(parquet.to_str().unwrap())
    );
    let config = Config::from_toml(&text, |_| None).unwrap();
    let session = AgentSession::new(DataFusionEngine::from_config(&config).await.unwrap(), false);
    let result = session.query(query("SELECT c.name, o.total FROM local_db.public.customers c JOIN lake.public.orders o ON c.id = o.customer_id ORDER BY o.total DESC",10),false).await.unwrap();
    assert_eq!(
        result["rows"],
        json!([{"name":"Alex","total":125.0},{"name":null,"total":75.0}])
    );
    drop(session);
    std::fs::remove_file(database).unwrap();
    std::fs::remove_file(parquet).unwrap();
}

#[tokio::test]
async fn postgres_adbc_and_native_connectors_join() {
    let (Ok(uri), Ok(driver)) = (
        std::env::var("IGLOO_TEST_POSTGRES_URI"),
        std::env::var("IGLOO_TEST_ADBC_POSTGRES_DRIVER"),
    ) else {
        eprintln!("skipping PostgreSQL ADBC test: set IGLOO_TEST_POSTGRES_URI and IGLOO_TEST_ADBC_POSTGRES_DRIVER");
        return;
    };
    let (client, connection) = tokio_postgres::connect(&uri, tokio_postgres::NoTls)
        .await
        .unwrap();
    tokio::spawn(async move {
        connection.await.unwrap();
    });
    let table = format!("igloo_adbc_fixture_{}", std::process::id());
    client.batch_execute(&format!("CREATE TABLE {table} (id BIGINT, label TEXT); INSERT INTO {table} VALUES (42, 'cross-connector')")).await.unwrap();
    let database: String = client
        .query_one("SELECT current_database()", &[])
        .await
        .unwrap()
        .get(0);
    let text=format!("[[sources]]\nname='native'\nuri={}\ntables=[{}]\n[[adbc_sources]]\nname='arrow_db'\ndriver={}\nuri={}\ncatalog={}\nschemas=['public']\ntables=[{}]",json!(uri),json!(table),json!(driver),json!(uri),json!(database),json!(table));
    let config = Config::from_toml(&text, |_| None).unwrap();
    let session = AgentSession::new(DataFusionEngine::from_config(&config).await.unwrap(), false);
    let sql=format!("SELECT a.id, n.label FROM arrow_db.public.{table} a JOIN native.public.{table} n ON a.id = n.id");
    let result = session.query(query(&sql, 10), false).await.unwrap();
    assert_eq!(
        result["rows"][0],
        json!({"id":"42","label":"cross-connector"})
    );
    client
        .batch_execute(&format!("DROP TABLE {table}"))
        .await
        .unwrap();
}

#[tokio::test]
async fn missing_driver_and_missing_tables_fail_without_leaking_options() {
    let config=Config::from_toml("[[adbc_sources]]\nname='broken'\ndriver='/no-such-driver.so'\n[adbc_sources.options]\npassword='never-print-me'", |_| None).unwrap();
    let error = DataFusionEngine::from_config(&config)
        .await
        .err()
        .expect("driver must fail")
        .to_string();
    assert!(error.contains("driver loading"));
    assert!(!error.contains("never-print-me"));
    let Some(driver) = sqlite_driver() else {
        return;
    };
    let path = temp("missing");
    create_sqlite(&driver, &path);
    let config = Config::from_toml(
        &sqlite_config(&driver, &path, "tables=['not_present']"),
        |_| None,
    )
    .unwrap();
    assert!(DataFusionEngine::from_config(&config).await.is_err());
    std::fs::remove_file(path).unwrap();
}
