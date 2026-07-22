//! Integration tests for filter pushdown to PostgreSQL (ROADMAP F3.1).
//!
//! These prove three things against a live database:
//!
//! 1. **Reduction** — a selective query fetches far fewer rows from PostgreSQL
//!    than the table holds, read from the provider's `rows_fetched` counter.
//! 2. **Correctness (differential)** — a corpus of pushable and non-pushable
//!    predicates returns identical results whether pushdown is on or off.
//! 3. **Injection safety** — a hostile predicate value (`'; DROP TABLE ...`)
//!    returns correct results and leaves the table intact.
//!
//! Requires a live PostgreSQL the test may freely create tables in. Set
//! `IGLOO_TEST_POSTGRES_URI` to run, e.g.:
//!
//! ```sh
//! IGLOO_TEST_POSTGRES_URI=postgres://postgres@127.0.0.1:5544/igloo_test \
//!     cargo test --test pushdown
//! ```
//!
//! Without the variable every test skips (and says so), keeping plain
//! `cargo test` hermetic. Tests share the database, so they serialize on a
//! single async mutex (mirroring `tests/catalog.rs`).

use std::sync::Arc;

use datafusion::arrow::array::{Array, Int64Array, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::parquet::arrow::ArrowWriter;
use tokio_postgres::{Client, NoTls};

use igloo::datafusion_engine::DataFusionEngine;
use igloo::postgres_table::PostgresTable;

/// Tests here mutate shared catalog state; serialize them within this binary.
static DB_LOCK: std::sync::OnceLock<tokio::sync::Mutex<()>> = std::sync::OnceLock::new();

async fn db_guard() -> tokio::sync::MutexGuard<'static, ()> {
    DB_LOCK
        .get_or_init(|| tokio::sync::Mutex::new(()))
        .lock()
        .await
}

const ROW_COUNT: i64 = 1000;

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
    let dir = std::env::temp_dir().join(format!("igloo_pushdown_{}_{}", tag, std::process::id()));
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

/// Seed `pushdown_items` with ~1000 rows. Columns exercise every literal type
/// the translator supports (int, double precision, bool, text) plus NULLs.
///
/// Row `i` in `1..=1000`:
/// - `id`       = i
/// - `category` = 'cat' + (i % 5)              (text; 5 buckets)
/// - `score`    = i as double precision        (float8 → exact round-trip)
/// - `active`   = (i % 2 == 0)
/// - `note`     = NULL when i % 10 == 0, else 'note-' + i
///
/// One special row (id = 500) has `category = 'quo''te'` to exercise a value
/// with a single quote flowing through a pushed predicate.
async fn seed(client: &Client) {
    client
        .batch_execute(
            "DROP TABLE IF EXISTS pushdown_items;
             CREATE TABLE pushdown_items (
                 id       bigint NOT NULL,
                 category text,
                 score    double precision,
                 active   boolean,
                 note     text
             );",
        )
        .await
        .expect("failed to create pushdown_items");

    // Insert in one statement for speed.
    let mut values = Vec::with_capacity(ROW_COUNT as usize);
    for i in 1..=ROW_COUNT {
        let category = if i == 500 {
            "'quo''te'".to_string() // literal: 'quo''te' -> value quo'te
        } else {
            format!("'cat{}'", i % 5)
        };
        let active = if i % 2 == 0 { "true" } else { "false" };
        let note = if i % 10 == 0 {
            "NULL".to_string()
        } else {
            format!("'note-{}'", i)
        };
        values.push(format!(
            "({}, {}, {}::double precision, {}, {})",
            i, category, i, active, note
        ));
    }
    let sql = format!(
        "INSERT INTO pushdown_items (id, category, score, active, note) VALUES {}",
        values.join(", ")
    );
    client
        .batch_execute(&sql)
        .await
        .expect("failed to seed rows");
}

/// Flatten a result set into sorted, stringified rows so two runs can be
/// compared regardless of row order.
fn normalize(batches: &[RecordBatch]) -> Vec<String> {
    let mut out = Vec::new();
    for batch in batches {
        for r in 0..batch.num_rows() {
            let mut cells = Vec::with_capacity(batch.num_columns());
            for c in 0..batch.num_columns() {
                let col = batch.column(c);
                if col.is_null(r) {
                    cells.push("NULL".to_string());
                } else if let Some(a) = col.as_any().downcast_ref::<Int64Array>() {
                    cells.push(a.value(r).to_string());
                } else if let Some(a) = col.as_any().downcast_ref::<StringArray>() {
                    cells.push(format!("'{}'", a.value(r)));
                } else {
                    // Fall back to Arrow's debug formatting for other types.
                    cells.push(format!("{:?}", arrow_cell_debug(col, r)));
                }
            }
            out.push(cells.join("|"));
        }
    }
    out.sort();
    out
}

fn arrow_cell_debug(col: &Arc<dyn Array>, r: usize) -> String {
    use datafusion::arrow::util::display::{ArrayFormatter, FormatOptions};
    let fmt = ArrayFormatter::try_new(col.as_ref(), &FormatOptions::default()).unwrap();
    fmt.value(r).to_string()
}

fn total_rows(batches: &[RecordBatch]) -> usize {
    batches.iter().map(|b| b.num_rows()).sum()
}

#[tokio::test]
async fn selective_query_reduces_rows_fetched_from_postgres() {
    let Ok(uri) = std::env::var("IGLOO_TEST_POSTGRES_URI") else {
        eprintln!("skipping pushdown test: IGLOO_TEST_POSTGRES_URI is not set");
        return;
    };
    let _guard = db_guard().await;
    let client = connect(&uri).await;
    seed(&client).await;

    let dir = temp_parquet_dir("reduce");
    let engine = DataFusionEngine::new(dir.to_str().unwrap(), &uri, &["public".to_string()])
        .await
        .expect("engine init failed");

    // Selective: only a handful of the 1000 rows match.
    let batches = engine
        .query("SELECT id, category FROM pushdown_items WHERE id > 995 ORDER BY id")
        .await
        .expect("selective query failed");
    assert_eq!(total_rows(&batches), 5, "ids 996..=1000 match");

    // Read the provider's rows_fetched: pushdown must have transferred far
    // fewer than the full table.
    let fetched = provider_rows_fetched(&engine, "pushdown_items").await;
    assert!(
        fetched < 50,
        "expected << {} rows fetched with pushdown, got {}",
        ROW_COUNT,
        fetched
    );
    eprintln!(
        "rows_fetched with pushdown for `id > 995`: {} (table has {})",
        fetched, ROW_COUNT
    );

    client
        .batch_execute("DROP TABLE IF EXISTS pushdown_items;")
        .await
        .unwrap();
    std::fs::remove_dir_all(&dir).unwrap();
}

/// Contrast: with pushdown disabled the same selective query pulls the whole
/// table. Proves `rows_fetched` reflects real transfer, not a constant.
#[tokio::test]
async fn without_pushdown_fetches_the_whole_table() {
    let Ok(uri) = std::env::var("IGLOO_TEST_POSTGRES_URI") else {
        eprintln!("skipping pushdown test: IGLOO_TEST_POSTGRES_URI is not set");
        return;
    };
    let _guard = db_guard().await;
    let client = connect(&uri).await;
    seed(&client).await;

    let dir = temp_parquet_dir("nopush");
    let engine = DataFusionEngine::new_with_pushdown(
        dir.to_str().unwrap(),
        &uri,
        &["public".to_string()],
        false,
    )
    .await
    .expect("engine init failed");

    let batches = engine
        .query("SELECT id, category FROM pushdown_items WHERE id > 995")
        .await
        .expect("selective query failed");
    assert_eq!(total_rows(&batches), 5);

    let fetched = provider_rows_fetched(&engine, "pushdown_items").await;
    assert_eq!(
        fetched, ROW_COUNT as u64,
        "without pushdown the whole table is fetched"
    );

    client
        .batch_execute("DROP TABLE IF EXISTS pushdown_items;")
        .await
        .unwrap();
    std::fs::remove_dir_all(&dir).unwrap();
}

/// Differential corpus: every query returns identical results with pushdown on
/// and off. Mixes pushable predicates (comparisons, IS NULL, IN, AND, string
/// with a quote, bool, float) and non-pushable ones (OR, LIKE, arithmetic),
/// combined with projections and LIMITs.
#[tokio::test]
async fn pushed_and_unpushed_results_are_identical() {
    let Ok(uri) = std::env::var("IGLOO_TEST_POSTGRES_URI") else {
        eprintln!("skipping pushdown test: IGLOO_TEST_POSTGRES_URI is not set");
        return;
    };
    let _guard = db_guard().await;
    let client = connect(&uri).await;
    seed(&client).await;

    let dir_on = temp_parquet_dir("diff_on");
    let dir_off = temp_parquet_dir("diff_off");
    let engine_on = DataFusionEngine::new(dir_on.to_str().unwrap(), &uri, &["public".to_string()])
        .await
        .expect("engine (pushdown on) init failed");
    let engine_off = DataFusionEngine::new_with_pushdown(
        dir_off.to_str().unwrap(),
        &uri,
        &["public".to_string()],
        false,
    )
    .await
    .expect("engine (pushdown off) init failed");

    let queries = [
        // pushable comparisons
        "SELECT id FROM pushdown_items WHERE id = 42",
        "SELECT id, category FROM pushdown_items WHERE id > 995",
        "SELECT id FROM pushdown_items WHERE id <= 3",
        // AND of pushables + projection
        "SELECT id, category FROM pushdown_items WHERE id >= 100 AND category = 'cat2'",
        // bool + float (double precision → exact)
        "SELECT id FROM pushdown_items WHERE active = true AND score > 990.0",
        // IS NULL / IS NOT NULL
        "SELECT id FROM pushdown_items WHERE note IS NULL",
        "SELECT id FROM pushdown_items WHERE note IS NOT NULL AND id < 25",
        // IN / NOT IN
        "SELECT id FROM pushdown_items WHERE id IN (1, 2, 3, 999, 1000)",
        "SELECT id FROM pushdown_items WHERE category NOT IN ('cat0','cat1','cat2','cat3')",
        // string predicate containing a quote (special row id = 500)
        "SELECT id FROM pushdown_items WHERE category = 'quo''te'",
        // non-pushable: OR, LIKE, arithmetic — must degrade, still correct
        "SELECT id FROM pushdown_items WHERE id = 1 OR id = 1000",
        "SELECT id FROM pushdown_items WHERE category LIKE 'cat%' AND id < 12",
        "SELECT id FROM pushdown_items WHERE id + 1 = 43",
        // combined pushable predicate + LIMIT (order to make LIMIT deterministic)
        "SELECT id FROM pushdown_items WHERE id > 990 ORDER BY id LIMIT 3",
        // mixed pushable + non-pushable in one query
        "SELECT id FROM pushdown_items WHERE id > 990 AND (id = 995 OR id = 999)",
    ];

    for q in queries {
        let on = engine_on
            .query(q)
            .await
            .unwrap_or_else(|e| panic!("pushdown-on query failed: {q}: {e}"));
        let off = engine_off
            .query(q)
            .await
            .unwrap_or_else(|e| panic!("pushdown-off query failed: {q}: {e}"));
        assert_eq!(
            normalize(&on),
            normalize(&off),
            "results differ with/without pushdown for query: {q}"
        );
    }

    client
        .batch_execute("DROP TABLE IF EXISTS pushdown_items;")
        .await
        .unwrap();
    std::fs::remove_dir_all(&dir_on).unwrap();
    std::fs::remove_dir_all(&dir_off).unwrap();
}

/// A hostile predicate value returns correct results (matching nothing) and
/// leaves the table intact — proving the value is confined to a quoted literal.
#[tokio::test]
async fn hostile_predicate_value_is_neutralized() {
    let Ok(uri) = std::env::var("IGLOO_TEST_POSTGRES_URI") else {
        eprintln!("skipping pushdown test: IGLOO_TEST_POSTGRES_URI is not set");
        return;
    };
    let _guard = db_guard().await;
    let client = connect(&uri).await;
    seed(&client).await;

    let dir = temp_parquet_dir("hostile");
    let engine = DataFusionEngine::new(dir.to_str().unwrap(), &uri, &["public".to_string()])
        .await
        .expect("engine init failed");

    // This value, if not escaped when translated to a pushed-down WHERE, would
    // drop the table. Because it is a well-formed SQL string literal in the
    // user's query, DataFusion parses it as a single string value, our
    // translator re-escapes it into a single quoted literal for PostgreSQL, and
    // it simply matches no row.
    let query =
        "SELECT id FROM pushdown_items WHERE category = '''; DROP TABLE pushdown_items; --'";
    let batches = engine
        .query(query)
        .await
        .expect("hostile-value query failed");
    assert_eq!(
        total_rows(&batches),
        0,
        "no category equals the hostile string"
    );

    // The table must still exist with all its rows.
    let still_there = client
        .query_one("SELECT COUNT(*) FROM pushdown_items", &[])
        .await
        .expect("table should still exist after hostile predicate");
    let count: i64 = still_there.get(0);
    assert_eq!(count, ROW_COUNT, "table must be intact");

    client
        .batch_execute("DROP TABLE IF EXISTS pushdown_items;")
        .await
        .unwrap();
    std::fs::remove_dir_all(&dir).unwrap();
}

/// Look up the registered provider and read its `rows_fetched` counter.
async fn provider_rows_fetched(engine: &DataFusionEngine, name: &str) -> u64 {
    let provider = engine
        .ctx
        .table_provider(name)
        .await
        .expect("provider registered");
    provider
        .as_any()
        .downcast_ref::<PostgresTable>()
        .expect("provider is a PostgresTable")
        .rows_fetched()
}
