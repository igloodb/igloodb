// src/datafusion_engine.rs
use datafusion::arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::listing::{
    ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl,
};
use datafusion::prelude::*;

use std::sync::Arc;

use crate::errors::Result;
use crate::postgres_table::PostgresTable;

pub struct DataFusionEngine {
    pub ctx: SessionContext,
}

impl DataFusionEngine {
    pub async fn new(parquet_path: &str, postgres_conn_str: &str) -> Result<Self> {
        let ctx = SessionContext::new();
        Self::register_iceberg_table(&ctx, parquet_path)?;
        Self::register_postgres_table(&ctx, postgres_conn_str).await?;
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

    /// Registers the PostgreSQL-backed table as `pg_table`.
    async fn register_postgres_table(ctx: &SessionContext, postgres_conn_str: &str) -> Result<()> {
        let pg_schema = Arc::new(ArrowSchema::new(vec![
            Field::new("user_id", DataType::Int64, false),
            Field::new("extra_info", DataType::Utf8, true),
        ]));
        let pg_provider =
            Arc::new(PostgresTable::try_new(postgres_conn_str, "my_pg_table", pg_schema).await?);
        ctx.register_table("pg_table", pg_provider)?;
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
}
