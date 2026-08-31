//! Integration tests for multi-source federation (roadmap F1.3).
//!
//! Two PostgreSQL **databases** are configured as two Igloo sources and
//! queried through one engine: every table is reachable by its canonical
//! `<source>.<schema>.<table>` name, cross-source joins work, `SHOW TABLES`
//! lists both catalogs so BI tools can browse them, a per-source table
//! allowlist restricts what is registered, and same-named tables in
//! different sources never shadow each other.
//!
//! Requires a live PostgreSQL whose user may `CREATE DATABASE` (the second
//! source is created by the test). Set `IGLOO_TEST_POSTGRES_URI` to run:
//!
//! ```sh
//! IGLOO_TEST_POSTGRES_URI=postgres://postgres@127.0.0.1:5432/igloo_test \
//!     cargo test --test multi_source
//! ```
//!
//! Without the variable every test skips (and says so), keeping plain
//! `cargo test` hermetic. CI provides a Postgres service container.
//!
//! Each test creates its own uniquely-named database and tables, so tests
//! never collide — including across test binaries, which `cargo test` runs
//! in parallel.

use datafusion::arrow::array::{Array, Int64Array, StringArray};
use datafusion::arrow::record_batch::RecordBatch;
use tokio_postgres::{Client, NoTls};

use igloo::config::PostgresSource;
use igloo::datafusion_engine::DataFusionEngine;

/// Rewrites the database of a PostgreSQL connection string, so the test can
/// address a second database on the same server as `IGLOO_TEST_POSTGRES_URI`
/// without needing a second variable. Handles both the URI form
/// (`postgres://user@host:port/db?params`) and the key-value form
/// (`host=... dbname=...`).
fn with_database(uri: &str, database: &str) -> String {
    for prefix in ["postgres://", "postgresql://"] {
        if let Some(rest) = uri.strip_prefix(prefix) {
            // Split the query string off first: it must survive the rewrite.
            let (location, query) = match rest.split_once('?') {
                Some((location, query)) => (location, Some(query)),
                None => (rest, None),
            };
            // The authority runs up to the first '/', which starts the
            // database path (absent when no database is named).
            let authority = location.split('/').next().unwrap_or(location);
            let mut out = format!("{prefix}{authority}/{database}");
            if let Some(query) = query {
                out.push('?');
                out.push_str(query);
            }
            return out;
        }
    }

    // Key-value form: replace an existing dbname=..., else append one.
    let mut replaced = false;
    let mut parts: Vec<String> = uri
        .split_whitespace()
        .map(|part| match part.split_once('=') {
            Some((key, _)) if key.eq_ignore_ascii_case("dbname") => {
                replaced = true;
                format!("dbname={database}")
            }
            _ => part.to_string(),
        })
        .collect();
    if !replaced {
        parts.push(format!("dbname={database}"));
    }
    parts.join(" ")
}

async fn connect(uri: &str) -> Client {
    let (client, connection) = tokio_postgres::connect(uri, NoTls)
        .await
        .expect("connect to PostgreSQL");
    tokio::spawn(async move {
        let _ = connection.await;
    });
    client
}

/// (Re)creates `database` on the same server as `admin_uri` and returns its
/// connection string. `CREATE DATABASE` cannot run inside a transaction, so
/// each statement is sent on its own.
async fn create_database(admin: &Client, admin_uri: &str, database: &str) -> String {
    drop_database(admin, database).await;
    admin
        .batch_execute(&format!("CREATE DATABASE {database}"))
        .await
        .unwrap_or_else(|e| panic!("CREATE DATABASE {database} failed: {e}"));
    with_database(admin_uri, database)
}

/// Drops `database`, forcing open connections closed (the engine keeps its
/// per-source connection open for the process lifetime). Tolerates absence.
async fn drop_database(admin: &Client, database: &str) {
    let _ = admin
        .batch_execute(&format!("DROP DATABASE IF EXISTS {database} WITH (FORCE)"))
        .await;
}

/// An empty directory for the Parquet-backed `iceberg` table: these tests
/// only query PostgreSQL sources, and an empty listing scans to zero rows.
fn temp_parquet_dir(tag: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!("igloo_ms_{}_{}", tag, std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

/// Flattens a Utf8 column across batches.
fn strings(batches: &[RecordBatch], col: usize) -> Vec<String> {
    let mut out = Vec::new();
    for batch in batches {
        let array = batch
            .column(col)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("Utf8 column");
        for i in 0..array.len() {
            if !array.is_null(i) {
                out.push(array.value(i).to_string());
            }
        }
    }
    out
}

/// Flattens an Int64 column across batches.
fn i64s(batches: &[RecordBatch], col: usize) -> Vec<i64> {
    let mut out = Vec::new();
    for batch in batches {
        let array = batch
            .column(col)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("Int64 column");
        for i in 0..array.len() {
            if !array.is_null(i) {
                out.push(array.value(i));
            }
        }
    }
    out
}

/// Reads the integration-test URI, or returns `None` so the caller skips.
macro_rules! uri_or_skip {
    ($name:literal) => {
        match std::env::var("IGLOO_TEST_POSTGRES_URI") {
            Ok(uri) => uri,
            Err(_) => {
                eprintln!("skipping {}: IGLOO_TEST_POSTGRES_URI is not set", $name);
                return;
            }
        }
    };
}

#[test]
fn with_database_rewrites_uri_and_key_value_forms() {
    assert_eq!(
        with_database("postgres://postgres:pw@localhost:5432/igloo_test", "other"),
        "postgres://postgres:pw@localhost:5432/other"
    );
    assert_eq!(
        with_database("postgresql://localhost/igloo_test?sslmode=disable", "other"),
        "postgresql://localhost/other?sslmode=disable"
    );
    assert_eq!(
        with_database("postgres://localhost:5432", "other"),
        "postgres://localhost:5432/other"
    );
    assert_eq!(
        with_database("host=localhost dbname=igloo_test user=postgres", "other"),
        "host=localhost dbname=other user=postgres"
    );
    assert_eq!(
        with_database("host=localhost user=postgres", "other"),
        "host=localhost user=postgres dbname=other"
    );
}

/// The headline F1.3 criterion: declaring two PostgreSQL sources (here two
/// separate databases) makes every table queryable as
/// `<source>.<schema>.<table>` with no Rust code changes, including joins
/// that span sources.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn two_sources_are_independently_queryable_and_joinable() {
    let uri = uri_or_skip!("two_sources_are_independently_queryable_and_joinable");
    let admin = connect(&uri).await;
    let beta_db = "igloo_ms_join_b";
    let beta_uri = create_database(&admin, &uri, beta_db).await;

    admin
        .batch_execute(
            "DROP TABLE IF EXISTS ms_left;
             CREATE TABLE ms_left (id bigint NOT NULL, label text);
             INSERT INTO ms_left (id, label) VALUES (1, 'left-one'), (2, 'left-two');",
        )
        .await
        .expect("seed source alpha");

    let beta = connect(&beta_uri).await;
    beta.batch_execute(
        "CREATE TABLE ms_right (id bigint NOT NULL, note text);
         INSERT INTO ms_right (id, note) VALUES (1, 'right-one'), (3, 'right-three');",
    )
    .await
    .expect("seed source beta");

    let dir = temp_parquet_dir("join");
    let engine = DataFusionEngine::new(
        dir.to_str().unwrap(),
        &[
            PostgresSource::new("alpha", &uri, vec!["public".to_string()])
                .with_tables(vec!["ms_left".to_string()]),
            PostgresSource::new("beta", &beta_uri, vec!["public".to_string()])
                .with_tables(vec!["ms_right".to_string()]),
        ],
    )
    .await
    .expect("engine init failed");

    // Each source's table resolves by its canonical three-part name, and
    // reads from its own database.
    let left = engine
        .query("SELECT label FROM alpha.public.ms_left WHERE id = 1")
        .await
        .expect("query against source alpha failed");
    assert_eq!(strings(&left, 0), vec!["left-one".to_string()]);

    let right = engine
        .query("SELECT note FROM beta.public.ms_right WHERE id = 1")
        .await
        .expect("query against source beta failed");
    assert_eq!(strings(&right, 0), vec!["right-one".to_string()]);

    // A join across two separate PostgreSQL databases, in one query.
    let joined = engine
        .query(
            "SELECT l.label, r.note \
             FROM alpha.public.ms_left l \
             JOIN beta.public.ms_right r ON l.id = r.id \
             ORDER BY l.id",
        )
        .await
        .expect("cross-source join failed");
    assert_eq!(strings(&joined, 0), vec!["left-one".to_string()]);
    assert_eq!(strings(&joined, 1), vec!["right-one".to_string()]);

    // Unqualified aliases still work: both bare names are free here.
    let via_alias = engine
        .query("SELECT id FROM ms_right ORDER BY id")
        .await
        .expect("alias query failed");
    assert_eq!(i64s(&via_alias, 0), vec![1, 3]);

    admin
        .batch_execute("DROP TABLE IF EXISTS ms_left;")
        .await
        .unwrap();
    drop_database(&admin, beta_db).await;
    std::fs::remove_dir_all(&dir).unwrap();
}

/// `SHOW TABLES` (and hence `information_schema`) lists every source's
/// catalog, which is how a BI tool browses a multi-source deployment.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn show_tables_lists_every_source_catalog() {
    let uri = uri_or_skip!("show_tables_lists_every_source_catalog");
    let admin = connect(&uri).await;
    let beta_db = "igloo_ms_show_b";
    let beta_uri = create_database(&admin, &uri, beta_db).await;

    admin
        .batch_execute(
            "DROP TABLE IF EXISTS ms_show_a;
             CREATE TABLE ms_show_a (id bigint NOT NULL);",
        )
        .await
        .expect("seed source alpha");
    let beta = connect(&beta_uri).await;
    beta.batch_execute("CREATE TABLE ms_show_b (id bigint NOT NULL);")
        .await
        .expect("seed source beta");

    let dir = temp_parquet_dir("show");
    let engine = DataFusionEngine::new(
        dir.to_str().unwrap(),
        &[
            PostgresSource::new("alpha", &uri, vec!["public".to_string()])
                .with_tables(vec!["ms_show_a".to_string()]),
            PostgresSource::new("beta", &beta_uri, vec!["public".to_string()])
                .with_tables(vec!["ms_show_b".to_string()]),
        ],
    )
    .await
    .expect("engine init failed");

    let batches = engine
        .query("SHOW TABLES")
        .await
        .expect("SHOW TABLES failed");
    let mut listed: Vec<(String, String, String)> = Vec::new();
    for batch in &batches {
        let catalog = batch.schema().index_of("table_catalog").unwrap();
        let schema = batch.schema().index_of("table_schema").unwrap();
        let name = batch.schema().index_of("table_name").unwrap();
        let col = |idx: usize| {
            batch
                .column(idx)
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("Utf8 column")
                .clone()
        };
        let (catalogs, schemas, names) = (col(catalog), col(schema), col(name));
        for i in 0..names.len() {
            listed.push((
                catalogs.value(i).to_string(),
                schemas.value(i).to_string(),
                names.value(i).to_string(),
            ));
        }
    }

    // "contains", not exact: the default catalog and information_schema are
    // listed too.
    for expected in [
        ("alpha", "public", "ms_show_a"),
        ("beta", "public", "ms_show_b"),
    ] {
        assert!(
            listed
                .iter()
                .any(|(c, s, n)| (c.as_str(), s.as_str(), n.as_str()) == expected),
            "SHOW TABLES missing {:?}; got {:?}",
            expected,
            listed
        );
    }

    admin
        .batch_execute("DROP TABLE IF EXISTS ms_show_a;")
        .await
        .unwrap();
    drop_database(&admin, beta_db).await;
    std::fs::remove_dir_all(&dir).unwrap();
}

/// A per-source `tables` allowlist registers exactly those tables: anything
/// else in the schema stays invisible instead of bloating the catalog.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn table_allowlist_restricts_registration() {
    let uri = uri_or_skip!("table_allowlist_restricts_registration");
    let admin = connect(&uri).await;

    admin
        .batch_execute(
            "DROP TABLE IF EXISTS ms_allowed;
             DROP TABLE IF EXISTS ms_excluded;
             CREATE TABLE ms_allowed (id bigint NOT NULL);
             INSERT INTO ms_allowed (id) VALUES (1);
             CREATE TABLE ms_excluded (id bigint NOT NULL);
             INSERT INTO ms_excluded (id) VALUES (2);",
        )
        .await
        .expect("seed tables");

    let dir = temp_parquet_dir("allowlist");
    let engine = DataFusionEngine::new(
        dir.to_str().unwrap(),
        &[
            PostgresSource::new("alpha", &uri, vec!["public".to_string()]).with_tables(vec![
                "ms_allowed".to_string(),
                // A name that matches nothing must not break startup.
                "ms_typo_never_exists".to_string(),
            ]),
        ],
    )
    .await
    .expect("engine init failed");

    let allowed = engine
        .query("SELECT id FROM alpha.public.ms_allowed")
        .await
        .expect("allowlisted table must be queryable");
    assert_eq!(i64s(&allowed, 0), vec![1]);

    assert!(
        engine
            .query("SELECT id FROM alpha.public.ms_excluded")
            .await
            .is_err(),
        "a table outside the allowlist must not be registered"
    );
    assert!(
        engine.query("SELECT id FROM ms_excluded").await.is_err(),
        "the excluded table must not get an unqualified alias either"
    );

    admin
        .batch_execute("DROP TABLE IF EXISTS ms_allowed; DROP TABLE IF EXISTS ms_excluded;")
        .await
        .unwrap();
    std::fs::remove_dir_all(&dir).unwrap();
}

/// Same-named tables in two sources stay distinct: the first source keeps
/// the unqualified alias, the second is reachable by its qualified alias,
/// and each canonical name reads its own database's data.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn same_named_tables_in_two_sources_do_not_shadow_each_other() {
    let uri = uri_or_skip!("same_named_tables_in_two_sources_do_not_shadow_each_other");
    let admin = connect(&uri).await;
    let beta_db = "igloo_ms_shadow_b";
    let beta_uri = create_database(&admin, &uri, beta_db).await;

    admin
        .batch_execute(
            "DROP TABLE IF EXISTS ms_shared;
             CREATE TABLE ms_shared (id bigint NOT NULL, origin text);
             INSERT INTO ms_shared (id, origin) VALUES (1, 'from-alpha');",
        )
        .await
        .expect("seed source alpha");
    let beta = connect(&beta_uri).await;
    beta.batch_execute(
        "CREATE TABLE ms_shared (id bigint NOT NULL, origin text);
         INSERT INTO ms_shared (id, origin) VALUES (1, 'from-beta');",
    )
    .await
    .expect("seed source beta");

    let dir = temp_parquet_dir("shadow");
    let engine = DataFusionEngine::new(
        dir.to_str().unwrap(),
        &[
            PostgresSource::new("alpha", &uri, vec!["public".to_string()])
                .with_tables(vec!["ms_shared".to_string()]),
            PostgresSource::new("beta", &beta_uri, vec!["public".to_string()])
                .with_tables(vec!["ms_shared".to_string()]),
        ],
    )
    .await
    .expect("engine init failed");

    // Canonical names always disambiguate.
    let from_alpha = engine
        .query("SELECT origin FROM alpha.public.ms_shared")
        .await
        .expect("alpha query failed");
    assert_eq!(strings(&from_alpha, 0), vec!["from-alpha".to_string()]);
    let from_beta = engine
        .query("SELECT origin FROM beta.public.ms_shared")
        .await
        .expect("beta query failed");
    assert_eq!(strings(&from_beta, 0), vec!["from-beta".to_string()]);

    // The primary source keeps the unqualified alias; the second source's
    // table falls back to the schema-qualified alias rather than being lost.
    let via_bare = engine
        .query("SELECT origin FROM ms_shared")
        .await
        .expect("unqualified alias query failed");
    assert_eq!(
        strings(&via_bare, 0),
        vec!["from-alpha".to_string()],
        "the first configured source owns the bare name"
    );
    let via_qualified = engine
        .query("SELECT origin FROM public__ms_shared")
        .await
        .expect("qualified alias query failed");
    assert_eq!(strings(&via_qualified, 0), vec!["from-beta".to_string()]);

    admin
        .batch_execute("DROP TABLE IF EXISTS ms_shared;")
        .await
        .unwrap();
    drop_database(&admin, beta_db).await;
    std::fs::remove_dir_all(&dir).unwrap();
}

/// Every configured schema of a source becomes a schema of that source's
/// catalog, so `<source>.<schema>.<table>` addresses non-default namespaces.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn source_schemas_become_catalog_schemas() {
    let uri = uri_or_skip!("source_schemas_become_catalog_schemas");
    let admin = connect(&uri).await;

    admin
        .batch_execute(
            "CREATE SCHEMA IF NOT EXISTS ms_reporting;
             DROP TABLE IF EXISTS ms_reporting.ms_daily;
             CREATE TABLE ms_reporting.ms_daily (id bigint NOT NULL, metric text);
             INSERT INTO ms_reporting.ms_daily (id, metric) VALUES (1, 'dau');",
        )
        .await
        .expect("seed reporting schema");

    let dir = temp_parquet_dir("schemas");
    let engine = DataFusionEngine::new(
        dir.to_str().unwrap(),
        &[PostgresSource::new(
            "alpha",
            &uri,
            vec!["public".to_string(), "ms_reporting".to_string()],
        )
        .with_tables(vec!["ms_daily".to_string()])],
    )
    .await
    .expect("engine init failed");

    let batches = engine
        .query("SELECT metric FROM alpha.ms_reporting.ms_daily WHERE id = 1")
        .await
        .expect("query against non-default schema failed");
    assert_eq!(strings(&batches, 0), vec!["dau".to_string()]);

    admin
        .batch_execute("DROP TABLE IF EXISTS ms_reporting.ms_daily;")
        .await
        .unwrap();
    std::fs::remove_dir_all(&dir).unwrap();
}
