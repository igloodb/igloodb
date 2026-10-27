// src/main.rs
use datafusion::arrow::util::pretty::pretty_format_batches;

use igloo::cache_layer::Cache;
use igloo::cdc_sync::CdcListener;
use igloo::config::Config;
use igloo::datafusion_engine::DataFusionEngine;
use igloo::errors::Result;
use igloo::{adbc_postgres, config, server};
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<()> {
    // Logs go to stderr. Defaults to `info` but respects RUST_LOG if set.
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();

    let args: Vec<String> = std::env::args().skip(1).collect();
    if let Some(command) = args.first() {
        if matches!(command.as_str(), "--help" | "-h" | "help") {
            print!("{}", igloo::agent_cli::HELP);
            return Ok(());
        }
        if matches!(command.as_str(), "--version" | "-V") {
            println!("igloo {}", env!("CARGO_PKG_VERSION"));
            return Ok(());
        }
        if matches!(
            command.as_str(),
            "query" | "schema" | "explain" | "mcp" | "capabilities" | "connectors"
        ) {
            if let Err(error) = igloo::agent_cli::run(command, args[1..].to_vec()).await {
                // Never write non-protocol output into an MCP transport.
                if command == "mcp" {
                    eprintln!("{}", error.envelope());
                } else {
                    println!("{}", error.envelope());
                }
                std::process::exit(2);
            }
            return Ok(());
        }
        if !matches!(command.as_str(), "serve" | "crypto-demo") || args.len() > 1 {
            eprintln!("Unknown command or argument. Run igloo --help.");
            std::process::exit(2);
        }
    }

    // `igloo crypto-demo` is self-contained (Parquet-only): it synthesizes
    // deterministic sample OHLCV data if the directory is empty and prints a
    // suite of crypto market metrics computed through DataFusion. It needs no
    // Postgres and deliberately skips config loading, which would otherwise
    // fail fast on settings the demo doesn't use.
    if std::env::args().nth(1).as_deref() == Some("crypto-demo") {
        let dir = std::env::var("IGLOO_CRYPTO_PARQUET_PATH")
            .unwrap_or_else(|_| "./crypto_ohlcv_data".to_string());
        return igloo::crypto_metrics::run_crypto_demo(&dir).await;
    }

    let config = load_config_or_exit();

    // `igloo serve` runs the long-lived pgwire server; plain `igloo` runs
    // the single-query demo below.
    if std::env::args().nth(1).as_deref() == Some("serve") {
        return serve(config).await;
    }

    let primary = config.primary_source().ok_or_else(|| igloo::errors::IglooError::Config(
        "the legacy demo needs a native PostgreSQL source; use query, schema, mcp or serve for other connectors".into()))?;
    let cdc_path = config.cdc_path.as_deref().ok_or_else(|| {
        igloo::errors::IglooError::Config("the legacy demo needs cdc_path".into())
    })?;
    if config.parquet_path.is_none() {
        return Err(igloo::errors::IglooError::Config(
            "the legacy demo needs parquet_path".into(),
        ));
    }

    log::info!("Initializing Igloo components...");
    let cache = Cache::new(config.cache_max_entries, config.cache_ttl);
    let cdc = CdcListener::new(cdc_path);

    log::info!("Initializing DataFusionEngine...");
    let engine = DataFusionEngine::from_config(&config).await?;
    log::info!("DataFusionEngine initialized successfully.");

    let query = "SELECT i.user_id, i.data, p.extra_info FROM iceberg i JOIN pg_table p ON i.user_id = p.user_id WHERE i.user_id = 42";

    if let Some(cached_batches) = cache.get(query) {
        log::info!(target: "igloo_main", "Cache hit for query: {query}");
        println!(
            "Cached result:\n{}",
            pretty_format_batches(&cached_batches)?
        );
    } else {
        log::info!(target: "igloo_main", "Cache miss for query: {query}. Executing with DataFusion.");
        let record_batches = engine.query(query).await?;
        let result_str = pretty_format_batches(&record_batches)?.to_string();
        cache.set(query, record_batches);
        println!("Cache miss. Executed with DataFusion:\n{result_str}");
    }

    // Connect to Postgres using ADBC and run a test query against the
    // primary (first configured) source.
    let sql_adbc_test = "SELECT 1 AS test_col";
    adbc_postgres::adbc_postgres_query_example(primary.uri.expose(), sql_adbc_test).await?;
    log::info!(target: "igloo_main", "ADBC test query succeeded! sql: {sql_adbc_test}");

    log::info!("Starting CDC sync...");
    cdc.sync(&cache);
    log::info!("CDC sync completed.");

    log::info!("Igloo application finished successfully.");
    Ok(())
}

/// Runs the pgwire server until killed, with the query cache kept fresh
/// by a background CDC polling task.
async fn serve(config: config::Config) -> Result<()> {
    let listen_addr = match config.require_listen_addr() {
        Ok(addr) => addr.to_string(),
        Err(e) => {
            eprintln!("{e}");
            eprintln!("See igloo.example.toml for all options.");
            std::process::exit(2);
        }
    };
    log::info!("Initializing DataFusionEngine for serve mode...");
    let engine = Arc::new(DataFusionEngine::from_config(&config).await?);

    let cache = Arc::new(Cache::new(config.cache_max_entries, config.cache_ttl));
    if let Some(path) = &config.cdc_path {
        let cdc = Arc::new(CdcListener::new(path));
        cdc.spawn_polling(cache.clone(), config.cdc_poll_interval);
    }

    server::serve(engine, cache, &listen_addr).await
}

/// Loads configuration, exiting with a clear message on failure so a
/// misconfigured deployment fails fast instead of falling back to defaults.
fn load_config_or_exit() -> config::Config {
    match Config::load() {
        Ok(config) => config,
        Err(e) => {
            // IglooError::Config's Display already carries the prefix.
            eprintln!("{e}");
            eprintln!("See igloo.example.toml for all options.");
            std::process::exit(2);
        }
    }
}
