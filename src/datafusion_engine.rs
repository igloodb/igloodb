// src/datafusion_engine.rs
use datafusion::arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::listing::{
    ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl,
};
use datafusion::prelude::*;

use std::sync::Arc;

use tokio_postgres::NoTls;

use crate::catalog;
use crate::errors::{IglooError, Result};
use crate::postgres_table::PostgresTable;

/// The legacy table name the demo binary and pre-F1.3 integration tests
/// query. When a `my_pg_table` is discovered it is additionally registered
/// under this alias for backward compatibility.
const LEGACY_ALIAS: &str = "pg_table";
/// The upstream table name the legacy alias points at.
const LEGACY_SOURCE_TABLE: &str = "my_pg_table";

pub struct DataFusionEngine {
    pub ctx: SessionContext,
}

impl DataFusionEngine {
    /// Builds the engine, registering the Parquet-backed `iceberg` table and
    /// every PostgreSQL base table discovered in `postgres_schemas` (default
    /// `["public"]`). Filter pushdown to PostgreSQL is enabled.
    pub async fn new(
        parquet_path: &str,
        postgres_conn_str: &str,
        postgres_schemas: &[String],
    ) -> Result<Self> {
        Self::new_with_pushdown(parquet_path, postgres_conn_str, postgres_schemas, true).await
    }

    /// Like [`Self::new`] but lets the caller disable filter pushdown on the
    /// registered PostgreSQL tables. With `filter_pushdown = false`, every
    /// predicate is applied by DataFusion locally rather than translated to a
    /// SQL `WHERE`. This exists so tests can differentially compare pushed vs.
    /// unpushed execution and assert identical results.
    pub async fn new_with_pushdown(
        parquet_path: &str,
        postgres_conn_str: &str,
        postgres_schemas: &[String],
        filter_pushdown: bool,
    ) -> Result<Self> {
        // Enable DataFusion's information_schema so BI tools (and tests) can
        // run `SHOW TABLES` / query `information_schema` against Igloo.
        let ctx =
            SessionContext::new_with_config(SessionConfig::new().with_information_schema(true));
        Self::register_iceberg_table(&ctx, parquet_path)?;
        Self::register_postgres_tables(&ctx, postgres_conn_str, postgres_schemas, filter_pushdown)
            .await?;
        log::info!("DataFusion context initialized with Iceberg and Postgres tables.");
        Ok(Self { ctx })
    }

    /// Registers the Parquet files backing the Iceberg table as `iceberg`.
    fn register_iceberg_table(ctx: &SessionContext, parquet_path: &str) -> Result<()> {
        // This schema must match the actual schema of the Parquet files.
        let iceberg_schema = Arc::new(ArrowSchema::new(vec![
            Field::new("user_id", DataType::Int64, false),
            Field::new("data", DataType::Utf8, true),
        ]));

        let listing_options = ListingOptions::new(Arc::new(ParquetFormat::default()))
            .with_file_extension(".parquet")
            .with_target_partitions(num_cpus::get());

        let table_url = ListingTableUrl::parse(parquet_path)?;

        let listing_table_config = ListingTableConfig::new(table_url)
            .with_listing_options(listing_options)
            .with_schema(iceberg_schema);

        let iceberg_table = Arc::new(ListingTable::try_new(listing_table_config)?);
        ctx.register_table("iceberg", iceberg_table)?;
        Ok(())
    }

    /// Introspects PostgreSQL and registers one DataFusion table per
    /// discovered base table in `schemas`.
    ///
    /// Each table is registered under its bare name; on a name collision
    /// across schemas the first (in schema-priority order) keeps the bare
    /// name and the later one is registered as `schema__table` (see
    /// [`catalog::resolve_registration_names`]). For backward compatibility a
    /// discovered `my_pg_table` is additionally registered under the legacy
    /// alias `pg_table`. If no tables are found the engine still starts (with
    /// a warning) — the Parquet source may be all that's needed.
    async fn register_postgres_tables(
        ctx: &SessionContext,
        postgres_conn_str: &str,
        schemas: &[String],
        filter_pushdown: bool,
    ) -> Result<()> {
        // One shared connection drives introspection and every table's scans.
        let (client, connection) = tokio_postgres::connect(postgres_conn_str, NoTls)
            .await
            .map_err(IglooError::Postgres)?;
        tokio::spawn(async move {
            if let Err(e) = connection.await {
                log::error!("PostgreSQL connection error: {}", e);
            }
        });
        let client = Arc::new(client);

        let tables = catalog::introspect_tables(&client, schemas).await?;
        if tables.is_empty() {
            log::warn!(
                "no PostgreSQL base tables found in schemas {:?}; \
                 starting with no Postgres tables registered",
                schemas
            );
            return Ok(());
        }

        let mut legacy_registered = false;
        for (reg_name, table) in catalog::resolve_registration_names(&tables) {
            let arrow_schema = Arc::new(ArrowSchema::new(table.fields.clone()));
            let provider = Arc::new(
                PostgresTable::from_client(
                    client.clone(),
                    &table.schema,
                    &table.name,
                    arrow_schema.clone(),
                )
                .with_filter_pushdown(filter_pushdown),
            );
            ctx.register_table(reg_name.as_str(), provider)?;
            log::info!(
                "registered Postgres table {}.{} as {:?} ({} column(s))",
                table.schema,
                table.name,
                reg_name,
                table.fields.len()
            );

            // Backward compatibility: expose my_pg_table under `pg_table` too.
            if !legacy_registered && table.name == LEGACY_SOURCE_TABLE {
                let alias_provider = Arc::new(
                    PostgresTable::from_client(
                        client.clone(),
                        &table.schema,
                        &table.name,
                        arrow_schema,
                    )
                    .with_filter_pushdown(filter_pushdown),
                );
                ctx.register_table(LEGACY_ALIAS, alias_provider)?;
                legacy_registered = true;
                log::info!(
                    "registered legacy alias {:?} -> {}.{} (deprecated; \
                     prefer the bare table name)",
                    LEGACY_ALIAS,
                    table.schema,
                    table.name
                );
            }
        }
        Ok(())
    }

    pub async fn query(&self, sql: &str) -> Result<Vec<RecordBatch>> {
        log::debug!("Executing SQL query in DataFusion: {}", sql);
        let df = self.ctx.sql(sql).await?;
        let results = df.collect().await?;
        log::debug!(
            "Query executed successfully. Number of batches: {}",
            results.len()
        );
        Ok(results)
    }
}

#[cfg(test)]
mod tests {
    use super::DataFusionEngine;
    use datafusion::arrow::array::{Int64Array, StringArray};
    use datafusion::arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::parquet::arrow::ArrowWriter;
    use datafusion::prelude::SessionContext;
    use std::sync::Arc;

    fn write_test_parquet(dir: &std::path::Path) {
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

    #[tokio::test]
    async fn iceberg_table_is_queryable_from_parquet() {
        let dir = std::env::temp_dir().join(format!("igloo_df_test_{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        write_test_parquet(&dir);

        let ctx = SessionContext::new();
        DataFusionEngine::register_iceberg_table(&ctx, dir.to_str().unwrap()).unwrap();

        let batches = ctx
            .sql("SELECT data FROM iceberg WHERE user_id = 42")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();

        let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(rows, 1);
        let col = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(col.value(0), "hello");

        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[tokio::test]
    async fn iceberg_scan_empty_dir_returns_zero_rows() {
        // Mirrors the shipped demo condition where dummy_iceberg_cdc/ contains
        // no .parquet files: DataFusion returns zero rows rather than erroring.
        let dir = std::env::temp_dir().join(format!("igloo_df_test_empty_{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();

        let ctx = SessionContext::new();
        DataFusionEngine::register_iceberg_table(&ctx, dir.to_str().unwrap()).unwrap();

        let batches = ctx
            .sql("SELECT * FROM iceberg")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();

        let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(rows, 0);

        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[tokio::test]
    async fn iceberg_projection_selects_second_row() {
        let dir = std::env::temp_dir().join(format!("igloo_df_test_proj_{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        write_test_parquet(&dir);

        let ctx = SessionContext::new();
        DataFusionEngine::register_iceberg_table(&ctx, dir.to_str().unwrap()).unwrap();

        let batches = ctx
            .sql("SELECT data FROM iceberg WHERE user_id = 7")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();

        let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(rows, 1);
        let col = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(col.value(0), "world");

        std::fs::remove_dir_all(&dir).unwrap();
    }
}
