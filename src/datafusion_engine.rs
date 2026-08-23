// src/datafusion_engine.rs
use datafusion::arrow::datatypes::{DataType, Field, Schema as ArrowSchema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::listing::{
    ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl,
};
use datafusion::prelude::*;
use datafusion::scalar::ScalarValue;

use std::collections::HashMap;
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

    /// Like [`Self::query`] but binds prepared-statement parameters first.
    ///
    /// `params[i]` replaces placeholder `$i + 1`. DataFusion infers each
    /// placeholder's type from the query context during planning (e.g.
    /// `WHERE id = $1` against an Int64 column infers Int64); the bound
    /// values are type-checked when substituted, so a mismatch surfaces as
    /// a planning error rather than wrong data. An empty `params` is
    /// equivalent to [`Self::query`].
    pub async fn query_with_params(
        &self,
        sql: &str,
        params: Vec<ScalarValue>,
    ) -> Result<Vec<RecordBatch>> {
        log::debug!(
            "Executing SQL query in DataFusion with {} parameter(s): {}",
            params.len(),
            sql
        );
        let df = self.ctx.sql(sql).await?;
        let df = if params.is_empty() {
            df
        } else {
            df.with_param_values(params)?
        };
        let results = df.collect().await?;
        log::debug!(
            "Query executed successfully. Number of batches: {}",
            results.len()
        );
        Ok(results)
    }

    /// Plans a statement without executing it and reports the metadata a
    /// client needs to prepare it: the output schema and the inferred type
    /// of every numbered placeholder (`$1`, `$2`, ...) in ordinal order.
    ///
    /// Types that DataFusion could not infer from context are `None`; the
    /// caller may then treat the parameter as text. Planning errors (bad
    /// SQL, unknown tables) propagate so callers can reject statements at
    /// prepare time.
    pub async fn describe_query(&self, sql: &str) -> Result<QueryDescription> {
        log::debug!("Planning SQL query for describe: {}", sql);
        let df = self.ctx.sql(sql).await?;
        let schema: SchemaRef = Arc::new(ArrowSchema::from(df.schema()));
        let plan = df.logical_plan();
        // Typed entries only appear in get_parameter_types once inference
        // assigned one; get_parameter_names lists every placeholder, so the
        // parameter count stays correct even when nothing could be inferred.
        let placeholder_ids = plan.get_parameter_names()?;
        let inferred_types = plan.get_parameter_types()?;
        let param_types = ordered_param_types(placeholder_ids, inferred_types);
        Ok(QueryDescription {
            schema,
            param_types,
        })
    }
}

/// Metadata for a prepared statement, as returned by
/// [`DataFusionEngine::describe_query`].
#[derive(Debug, Clone)]
pub struct QueryDescription {
    /// The statement's output schema (column names, types, nullability).
    pub schema: SchemaRef,
    /// Inferred Arrow type of each numbered placeholder, ordinal order:
    /// entry `i` describes `$i + 1`. `None` where inference had no context
    /// (the caller may then decode the value as text).
    pub param_types: Vec<Option<DataType>>,
}

/// Flattens DataFusion's placeholder information (`"$1"`, `"$2"`, ...) into an
/// ordinal vector of inferred types, sized to the highest-numbered
/// placeholder. Placeholders DataFusion could not type (or missing indices)
/// become `None`. Keys that are not plain numbers — named parameters never
/// sent over the pgwire protocol — are ignored.
fn ordered_param_types(
    placeholder_ids: std::collections::HashSet<String>,
    param_types: HashMap<String, Option<DataType>>,
) -> Vec<Option<DataType>> {
    let index_of = |id: &str| {
        id.strip_prefix('$')
            .and_then(|num| num.parse::<usize>().ok())
    };
    let count = placeholder_ids
        .iter()
        .chain(param_types.keys())
        .filter_map(|id| index_of(id))
        .max()
        .unwrap_or(0);
    let mut out: Vec<Option<DataType>> = vec![None; count];
    for (id, dt) in param_types {
        if let Some(idx) = index_of(&id) {
            if idx >= 1 && idx <= count {
                out[idx - 1] = dt;
            }
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::{ordered_param_types, DataFusionEngine};
    use datafusion::arrow::array::{Int64Array, StringArray};
    use datafusion::arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::parquet::arrow::ArrowWriter;
    use datafusion::prelude::SessionContext;
    use datafusion::scalar::ScalarValue;
    use std::collections::HashMap;
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

    #[tokio::test]
    async fn parameterized_query_binds_and_infers_types() {
        let dir = std::env::temp_dir().join(format!("igloo_df_test_param_{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        write_test_parquet(&dir);

        let ctx = SessionContext::new();
        DataFusionEngine::register_iceberg_table(&ctx, dir.to_str().unwrap()).unwrap();
        let engine = DataFusionEngine { ctx };

        // Describe infers the placeholder type from the comparison context.
        let described = engine
            .describe_query("SELECT data FROM iceberg WHERE user_id = $1")
            .await
            .unwrap();
        assert_eq!(described.param_types, vec![Some(DataType::Int64)]);
        assert_eq!(described.schema.fields().len(), 1);
        assert_eq!(described.schema.field(0).name(), "data");

        // Binding the matching value executes correctly.
        let batches = engine
            .query_with_params(
                "SELECT data FROM iceberg WHERE user_id = $1",
                vec![ScalarValue::from(7i64)],
            )
            .await
            .unwrap();
        let col = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(col.value(0), "world");

        // A second parameter position is bound independently.
        let batches = engine
            .query_with_params(
                "SELECT data FROM iceberg WHERE user_id >= $1 AND user_id < $2",
                vec![ScalarValue::from(7i64), ScalarValue::from(43i64)],
            )
            .await
            .unwrap();
        let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(rows, 2);

        // Missing parameter values fail with a planning error, not silently.
        let err = engine
            .query_with_params(
                "SELECT data FROM iceberg WHERE user_id = $1 AND data = $2",
                vec![ScalarValue::from(7i64)],
            )
            .await;
        assert!(err.is_err(), "missing $2 must be rejected");

        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[tokio::test]
    async fn describe_reports_untyped_placeholder_as_none() {
        // `SELECT $1` has no context to infer from: the parameter stays
        // untyped and the output column carries whatever was bound.
        let ctx = SessionContext::new();
        let engine = DataFusionEngine { ctx };
        let described = engine.describe_query("SELECT $1").await.unwrap();
        assert_eq!(described.param_types.len(), 1);
        assert_eq!(described.param_types[0], None);

        // Any scalar binds fine when no type was inferred.
        let batches = engine
            .query_with_params("SELECT $1", vec![ScalarValue::from("anything")])
            .await
            .unwrap();
        let col = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(col.value(0), "anything");
    }

    #[test]
    fn ordered_param_types_sorts_and_fills_gaps() {
        let mut map = HashMap::new();
        map.insert("$1".to_string(), Some(DataType::Int64));
        map.insert("$3".to_string(), Some(DataType::Utf8));
        map.insert("$10".to_string(), Some(DataType::Boolean));
        map.insert("named".to_string(), Some(DataType::Int32)); // ignored
        let ids: std::collections::HashSet<String> = ["$1", "$2", "$3", "$10"]
            .iter()
            .map(|s| s.to_string())
            .collect();

        let out = ordered_param_types(ids, map);
        assert_eq!(out.len(), 10);
        assert_eq!(out[0], Some(DataType::Int64));
        assert_eq!(out[1], None, "gap becomes None");
        assert_eq!(out[2], Some(DataType::Utf8));
        for slot in &out[3..9] {
            assert_eq!(*slot, None);
        }
        assert_eq!(out[9], Some(DataType::Boolean));

        // A placeholder with no inferred type is still counted (None).
        let out = ordered_param_types(
            ["$1"].iter().map(|s| s.to_string()).collect(),
            HashMap::new(),
        );
        assert_eq!(out, vec![None]);

        assert!(ordered_param_types(std::collections::HashSet::new(), HashMap::new()).is_empty());
    }
}
