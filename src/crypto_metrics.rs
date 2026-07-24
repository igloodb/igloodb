// src/crypto_metrics.rs
//! Crypto market metrics computed through the DataFusion engine.
//!
//! Registers an OHLCV candle table (`crypto_ohlcv`, Parquet-backed like the
//! `iceberg` table) and provides a small library of market-metric queries —
//! latest close, daily volume, daily VWAP, simple moving average, rolling
//! log-return volatility, and maximum drawdown — as pure SQL builders so the
//! generated SQL is unit-testable. `igloo crypto-demo` runs the whole suite
//! over deterministic synthetic data with no external services.

use std::sync::Arc;

use datafusion::arrow::array::{Float64Array, StringArray, TimestampNanosecondArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema as ArrowSchema, SchemaRef, TimeUnit};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::arrow::util::pretty::pretty_format_batches;
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::listing::{
    ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl,
};
use datafusion::parquet::arrow::ArrowWriter;
use datafusion::prelude::SessionContext;

use crate::errors::Result;

/// Name the OHLCV table is registered under.
pub const CRYPTO_TABLE: &str = "crypto_ohlcv";

/// Assets the synthetic sample data covers; the `crypto_assets` reference
/// table seeded by `scripts/seed_test_db.sql` uses the same symbols.
pub const SAMPLE_ASSETS: [&str; 3] = ["BTC", "ETH", "SOL"];

/// Arrow schema of the `crypto_ohlcv` table: hourly candles per asset.
pub fn crypto_ohlcv_schema() -> SchemaRef {
    Arc::new(ArrowSchema::new(vec![
        Field::new("ts", DataType::Timestamp(TimeUnit::Nanosecond, None), false),
        Field::new("asset", DataType::Utf8, false),
        Field::new("open", DataType::Float64, false),
        Field::new("high", DataType::Float64, false),
        Field::new("low", DataType::Float64, false),
        Field::new("close", DataType::Float64, false),
        Field::new("volume", DataType::Float64, false),
    ]))
}

/// Registers the Parquet files in `dir` as the `crypto_ohlcv` table.
pub fn register_crypto_table(ctx: &SessionContext, dir: &str) -> Result<()> {
    let listing_options = ListingOptions::new(Arc::new(ParquetFormat::default()))
        .with_file_extension(".parquet")
        .with_target_partitions(num_cpus::get());

    let table_url = ListingTableUrl::parse(dir)?;
    let config = ListingTableConfig::new(table_url)
        .with_listing_options(listing_options)
        .with_schema(crypto_ohlcv_schema());

    ctx.register_table(CRYPTO_TABLE, Arc::new(ListingTable::try_new(config)?))?;
    Ok(())
}

/// Latest close per asset (most recent candle wins).
pub fn sql_latest_close() -> String {
    format!(
        "SELECT asset, ts, close FROM (\
         SELECT asset, ts, close, \
         ROW_NUMBER() OVER (PARTITION BY asset ORDER BY ts DESC) AS rn \
         FROM {CRYPTO_TABLE}) t WHERE rn = 1 ORDER BY asset"
    )
}

/// Total traded volume per asset per day.
pub fn sql_daily_volume() -> String {
    format!(
        "SELECT asset, date_trunc('day', ts) AS day, SUM(volume) AS volume \
         FROM {CRYPTO_TABLE} GROUP BY asset, date_trunc('day', ts) \
         ORDER BY asset, day"
    )
}

/// Volume-weighted average price per asset per day.
pub fn sql_daily_vwap() -> String {
    format!(
        "SELECT asset, date_trunc('day', ts) AS day, \
         SUM(close * volume) / SUM(volume) AS vwap \
         FROM {CRYPTO_TABLE} GROUP BY asset, date_trunc('day', ts) \
         ORDER BY asset, day"
    )
}

/// Simple moving average of the close over the trailing `window` candles.
pub fn sql_sma(window: usize) -> String {
    let preceding = window.saturating_sub(1);
    format!(
        "SELECT asset, ts, close, \
         AVG(close) OVER (PARTITION BY asset ORDER BY ts \
         ROWS BETWEEN {preceding} PRECEDING AND CURRENT ROW) AS sma_{window} \
         FROM {CRYPTO_TABLE} ORDER BY asset, ts"
    )
}

/// Rolling sample standard deviation of hourly log returns over the trailing
/// `window` candles — the usual "realized volatility" building block. The
/// first candle of each asset has no previous close, so its log return is
/// NULL and is ignored by the aggregate.
pub fn sql_rolling_volatility(window: usize) -> String {
    let preceding = window.saturating_sub(1);
    format!(
        "SELECT asset, ts, \
         STDDEV_SAMP(log_return) OVER (PARTITION BY asset ORDER BY ts \
         ROWS BETWEEN {preceding} PRECEDING AND CURRENT ROW) AS volatility_{window} \
         FROM (SELECT asset, ts, \
         LN(close / LAG(close) OVER (PARTITION BY asset ORDER BY ts)) AS log_return \
         FROM {CRYPTO_TABLE}) r ORDER BY asset, ts"
    )
}

/// Maximum drawdown per asset: the worst peak-to-trough decline of the close,
/// as a negative fraction (e.g. -0.25 = a 25% drop from the running peak).
pub fn sql_max_drawdown() -> String {
    format!(
        "SELECT asset, MIN(close / running_max - 1.0) AS max_drawdown \
         FROM (SELECT asset, close, \
         MAX(close) OVER (PARTITION BY asset ORDER BY ts \
         ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_max \
         FROM {CRYPTO_TABLE}) t GROUP BY asset ORDER BY asset"
    )
}

/// The demo's metric suite: display name + SQL.
pub fn metrics_suite() -> Vec<(&'static str, String)> {
    vec![
        ("Latest close", sql_latest_close()),
        ("Daily volume", sql_daily_volume()),
        ("Daily VWAP", sql_daily_vwap()),
        ("SMA(24) of close", sql_sma(24)),
        (
            "Rolling 24h log-return volatility",
            sql_rolling_volatility(24),
        ),
        ("Maximum drawdown", sql_max_drawdown()),
    ]
}

/// Writes `hours` of deterministic synthetic hourly candles per sample asset
/// into `dir/sample_ohlcv.parquet`.
///
/// The series is a sine wave with LCG-derived noise so runs are reproducible
/// with no extra dependencies. The LCG is for synthetic fixture data ONLY —
/// it is not a cryptographic or statistical RNG and must not be used as one.
pub fn write_sample_ohlcv(dir: &std::path::Path, hours: usize) -> Result<()> {
    std::fs::create_dir_all(dir)?;

    let base_ns: i64 = 1_700_000_000_000_000_000; // 2023-11-14T22:13:20Z
    let hour_ns: i64 = 3_600 * 1_000_000_000;

    let mut ts = Vec::new();
    let mut asset = Vec::new();
    let mut open = Vec::new();
    let mut high = Vec::new();
    let mut low = Vec::new();
    let mut close = Vec::new();
    let mut volume = Vec::new();

    for (asset_idx, symbol) in SAMPLE_ASSETS.iter().enumerate() {
        let base_price = [40_000.0, 2_500.0, 90.0][asset_idx];
        let mut lcg: u64 = 0x9E37_79B9_7F4A_7C15 ^ (asset_idx as u64 + 1);
        let mut noise = || {
            lcg = lcg
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            // Top 53 bits -> [0, 1).
            (lcg >> 11) as f64 / (1u64 << 53) as f64
        };

        let mut prev_close = base_price;
        for i in 0..hours {
            let drift = (i as f64 / 12.0).sin() * 0.02;
            let shock = (noise() - 0.5) * 0.02;
            let c = (prev_close * (1.0 + drift / 24.0 + shock)).max(base_price * 0.5);
            let o = prev_close;
            let h = o.max(c) * (1.0 + noise() * 0.005);
            let l = o.min(c) * (1.0 - noise() * 0.005);
            let v = base_price * (50.0 + noise() * 25.0);

            ts.push(base_ns + (i as i64) * hour_ns);
            asset.push(*symbol);
            open.push(o);
            high.push(h);
            low.push(l);
            close.push(c);
            volume.push(v);
            prev_close = c;
        }
    }

    let schema = crypto_ohlcv_schema();
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(TimestampNanosecondArray::from(ts)),
            Arc::new(StringArray::from(asset)),
            Arc::new(Float64Array::from(open)),
            Arc::new(Float64Array::from(high)),
            Arc::new(Float64Array::from(low)),
            Arc::new(Float64Array::from(close)),
            Arc::new(Float64Array::from(volume)),
        ],
    )?;

    let file = std::fs::File::create(dir.join("sample_ohlcv.parquet"))?;
    let mut writer = ArrowWriter::try_new(file, schema, None)?;
    writer.write(&batch)?;
    writer.close()?;
    Ok(())
}

/// Runs the whole metric suite over the Parquet data in `dir`, generating
/// deterministic sample data first if the directory holds no `.parquet`
/// file. Self-contained: needs no Postgres and no configuration.
pub async fn run_crypto_demo(dir: &str) -> Result<()> {
    let path = std::path::Path::new(dir);
    let has_parquet = path.is_dir()
        && std::fs::read_dir(path)?
            .flatten()
            .any(|e| e.path().extension().is_some_and(|ext| ext == "parquet"));
    if !has_parquet {
        log::info!("No parquet data in {dir:?}; writing deterministic sample OHLCV data.");
        write_sample_ohlcv(path, 24 * 7)?;
    }

    let ctx = SessionContext::new();
    register_crypto_table(&ctx, dir)?;

    for (name, sql) in metrics_suite() {
        let batches = ctx.sql(&sql).await?.collect().await?;
        println!("\n== {name} ==");
        println!("{}", pretty_format_batches(&batches)?);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{Array, Float64Array, StringArray};

    fn temp_dir(name: &str) -> std::path::PathBuf {
        let dir =
            std::env::temp_dir().join(format!("igloo_crypto_{}_{}", name, std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    /// Four BTC candles with hand-computable metrics.
    fn write_hand_fixture(dir: &std::path::Path) {
        let hour_ns: i64 = 3_600 * 1_000_000_000;
        let closes = [100.0, 110.0, 99.0, 121.0];
        let volumes = [10.0, 20.0, 10.0, 10.0];

        let schema = crypto_ohlcv_schema();
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(TimestampNanosecondArray::from(
                    (0..4).map(|i| i * hour_ns).collect::<Vec<_>>(),
                )),
                Arc::new(StringArray::from(vec!["BTC"; 4])),
                Arc::new(Float64Array::from(closes.to_vec())), // open (unused by metrics)
                Arc::new(Float64Array::from(closes.to_vec())), // high
                Arc::new(Float64Array::from(closes.to_vec())), // low
                Arc::new(Float64Array::from(closes.to_vec())),
                Arc::new(Float64Array::from(volumes.to_vec())),
            ],
        )
        .unwrap();

        let file = std::fs::File::create(dir.join("fixture.parquet")).unwrap();
        let mut writer = ArrowWriter::try_new(file, schema, None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
    }

    async fn ctx_over(dir: &std::path::Path) -> SessionContext {
        let ctx = SessionContext::new();
        register_crypto_table(&ctx, dir.to_str().unwrap()).unwrap();
        ctx
    }

    fn f64_col(batches: &[RecordBatch], idx: usize) -> Vec<f64> {
        batches
            .iter()
            .flat_map(|b| {
                let col = b
                    .column(idx)
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .unwrap();
                (0..col.len())
                    .filter(|&i| !col.is_null(i))
                    .map(|i| col.value(i))
                    .collect::<Vec<_>>()
            })
            .collect()
    }

    #[test]
    fn sql_builders_pin_expected_strings() {
        assert_eq!(
            sql_max_drawdown(),
            "SELECT asset, MIN(close / running_max - 1.0) AS max_drawdown \
             FROM (SELECT asset, close, \
             MAX(close) OVER (PARTITION BY asset ORDER BY ts \
             ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_max \
             FROM crypto_ohlcv) t GROUP BY asset ORDER BY asset"
        );
        assert_eq!(
            sql_sma(24),
            "SELECT asset, ts, close, \
             AVG(close) OVER (PARTITION BY asset ORDER BY ts \
             ROWS BETWEEN 23 PRECEDING AND CURRENT ROW) AS sma_24 \
             FROM crypto_ohlcv ORDER BY asset, ts"
        );
        assert!(sql_daily_vwap().contains("SUM(close * volume) / SUM(volume)"));
    }

    #[tokio::test]
    async fn hand_fixture_metrics_are_exact() {
        let dir = temp_dir("hand");
        write_hand_fixture(&dir);
        let ctx = ctx_over(&dir).await;

        // Latest close: last candle (ts = 3h) closed at 121.
        let batches = ctx
            .sql(&sql_latest_close())
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        assert_eq!(f64_col(&batches, 2), vec![121.0]);

        // Daily VWAP, single day: (100*10 + 110*20 + 99*10 + 121*10) / 50 = 108.
        let batches = ctx
            .sql(&sql_daily_vwap())
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        assert_eq!(f64_col(&batches, 2), vec![108.0]);

        // Max drawdown: running max [100,110,110,121] vs close -> worst is
        // 99/110 - 1 = -0.0909...
        let batches = ctx
            .sql(&sql_max_drawdown())
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let dd = f64_col(&batches, 1);
        assert_eq!(dd.len(), 1);
        assert!(
            (dd[0] - (99.0 / 110.0 - 1.0)).abs() < 1e-12,
            "got {}",
            dd[0]
        );

        // SMA(2): [100, 105, 104.5, 110].
        let batches = ctx.sql(&sql_sma(2)).await.unwrap().collect().await.unwrap();
        assert_eq!(f64_col(&batches, 3), vec![100.0, 105.0, 104.5, 110.0]);

        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[tokio::test]
    async fn sample_data_supports_full_suite() {
        let dir = temp_dir("suite");
        write_sample_ohlcv(&dir, 48).unwrap();
        let ctx = ctx_over(&dir).await;

        for (name, sql) in metrics_suite() {
            let batches = ctx.sql(&sql).await.unwrap().collect().await.unwrap();
            let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
            assert!(rows > 0, "metric {name:?} returned no rows");
        }

        // Volatility over the sample data must produce finite values.
        let batches = ctx
            .sql(&sql_rolling_volatility(24))
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        assert!(f64_col(&batches, 2).iter().all(|v| v.is_finite()));

        std::fs::remove_dir_all(&dir).unwrap();
    }

    /// Federated metric: OHLCV Parquet joined with the `crypto_assets`
    /// reference table in PostgreSQL (seeded by scripts/seed_test_db.sql).
    /// Gated like the other integration tests: skips unless
    /// IGLOO_TEST_POSTGRES_URI is set.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn integration_volume_by_asset_name_via_postgres() {
        let uri = match std::env::var("IGLOO_TEST_POSTGRES_URI") {
            Ok(uri) => uri,
            Err(_) => {
                eprintln!(
                    "skipping integration_volume_by_asset_name_via_postgres: \
                     IGLOO_TEST_POSTGRES_URI not set"
                );
                return;
            }
        };

        let dir = temp_dir("federated");
        write_sample_ohlcv(&dir, 24).unwrap();
        let ctx = ctx_over(&dir).await;

        let pg_schema = Arc::new(ArrowSchema::new(vec![
            Field::new("asset", DataType::Utf8, false),
            Field::new("name", DataType::Utf8, false),
        ]));
        let assets = crate::postgres_table::PostgresTable::try_new(
            &uri,
            "public",
            "crypto_assets",
            pg_schema,
        )
        .await
        .unwrap();
        ctx.register_table("crypto_assets", Arc::new(assets))
            .unwrap();

        let batches = ctx
            .sql(
                "SELECT a.name, SUM(o.volume) AS total_volume \
                 FROM crypto_ohlcv o JOIN crypto_assets a ON o.asset = a.asset \
                 GROUP BY a.name ORDER BY a.name",
            )
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();

        let names: Vec<String> = batches
            .iter()
            .flat_map(|b| {
                let col = b.column(0).as_any().downcast_ref::<StringArray>().unwrap();
                (0..col.len())
                    .map(|i| col.value(i).to_string())
                    .collect::<Vec<_>>()
            })
            .collect();
        assert_eq!(names, vec!["Bitcoin", "Ethereum", "Solana"]);
        assert!(f64_col(&batches, 1).iter().all(|v| *v > 0.0));

        std::fs::remove_dir_all(&dir).unwrap();
    }
}
