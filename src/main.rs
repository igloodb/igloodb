// src/main.rs
use datafusion::arrow::util::pretty::pretty_format_batches;

use igloo::cache_layer::Cache;
use igloo::cdc_sync::CdcListener;
use igloo::config::Config;
use igloo::datafusion_engine::DataFusionEngine;
use igloo::errors::Result;
use igloo::{adbc_postgres, config};

#[tokio::main]
async fn main() -> Result<()> {
    // Logs go to stderr. Defaults to `info` but respects RUST_LOG if set.
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();

    let config = load_config_or_exit();

    log::info!("Initializing Igloo components...");
    let mut cache = Cache::new();
    let cdc = CdcListener::new(&config.cdc_path);

    log::info!("Initializing DataFusionEngine...");
    let engine = DataFusionEngine::new(&config.parquet_path, config.postgres_uri.expose()).await?;
    log::info!("DataFusionEngine initialized successfully.");

    let query = "SELECT i.user_id, i.data, p.extra_info FROM iceberg i JOIN pg_table p ON i.user_id = p.user_id WHERE i.user_id = 42";

    if let Some(cached_result) = cache.get(query) {
        log::info!(target: "igloo_main", "Cache hit for query: {}", query);
        println!("Cached result:\n{}", cached_result);
    } else {
        log::info!(target: "igloo_main", "Cache miss for query: {}. Executing with DataFusion.", query);
        let record_batches = engine.query(query).await?;
        let result_str = pretty_format_batches(&record_batches)?.to_string();
        cache.set(query, &result_str);
        println!("Cache miss. Executed with DataFusion:\n{}", result_str);
    }

    // Connect to Postgres using ADBC and run a test query
    let sql_adbc_test = "SELECT 1 AS test_col";
    adbc_postgres::adbc_postgres_query_example(config.postgres_uri.expose(), sql_adbc_test).await?;
    log::info!(target: "igloo_main", "ADBC test query succeeded! sql: {}", sql_adbc_test);

    log::info!("Starting CDC sync...");
    cdc.sync(&mut cache);
    log::info!("CDC sync completed.");

    log::info!("Igloo application finished successfully.");
    Ok(())
}

/// Loads configuration, exiting with a clear message on failure so a
/// misconfigured deployment fails fast instead of falling back to defaults.
fn load_config_or_exit() -> config::Config {
    match Config::load() {
        Ok(config) => config,
        Err(e) => {
            // IglooError::Config's Display already carries the prefix.
            eprintln!("{}", e);
            eprintln!("See igloo.example.toml for all options.");
            std::process::exit(2);
        }
    }
}
