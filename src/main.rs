// src/main.rs
mod adbc_postgres;
mod cache_layer;
mod cdc_sync;
mod datafusion_engine;
mod errors;
pub mod postgres_table;

use cache_layer::Cache;
use cdc_sync::CdcListener;
use datafusion::arrow::util::pretty::pretty_format_batches;
use datafusion_engine::DataFusionEngine;
use errors::Result;
use std::env;

#[tokio::main]
async fn main() -> Result<()> {
    // Logs go to stderr. Defaults to `info` but respects RUST_LOG if set.
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();

    log::info!("Initializing Igloo components...");
    let mut cache = Cache::new();
    let cdc_path = env::var("IGLOO_CDC_PATH").unwrap_or_else(|_| "./dummy_iceberg_cdc".to_string());
    let cdc = CdcListener::new(&cdc_path);

    log::info!("Initializing DataFusionEngine...");
    let parquet_path =
        env::var("IGLOO_PARQUET_PATH").unwrap_or_else(|_| "./dummy_iceberg_cdc/".to_string());
    let postgres_conn_str = env::var("DATABASE_URL")
        .or_else(|_| env::var("IGLOO_POSTGRES_URI"))
        .unwrap_or_else(|_| "postgres://postgres:postgres@localhost:5432/mydb".to_string());

    let engine = DataFusionEngine::new(&parquet_path, &postgres_conn_str).await?;
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
    adbc_postgres::adbc_postgres_query_example(&postgres_conn_str, sql_adbc_test).await?;
    log::info!(target: "igloo_main", "ADBC test query succeeded! sql: {}", sql_adbc_test);

    log::info!("Starting CDC sync...");
    cdc.sync(&mut cache);
    log::info!("CDC sync completed.");

    log::info!("Igloo application finished successfully.");
    Ok(())
}
