// src/datafusion_engine.rs
use datafusion::arrow::datatypes::{DataType, Field, Schema as ArrowSchema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::catalog::{CatalogProvider, SchemaProvider, TableProvider};
use datafusion::catalog_common::memory::{MemoryCatalogProvider, MemorySchemaProvider};
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::listing::{
    ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl,
};
use datafusion::prelude::*;
use datafusion::scalar::ScalarValue;

use std::collections::HashMap;
use std::sync::Arc;

use tokio_postgres::{Client, NoTls};

use crate::catalog::{self, SourceTables, TableSchema};
use crate::config::PostgresSource;
use crate::errors::{IglooError, Result};
use crate::postgres_table::PostgresTable;

/// Name of the Parquet-backed table in the default catalog.
const ICEBERG_TABLE: &str = "iceberg";
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
    /// every PostgreSQL base table discovered in each configured source.
    /// Filter pushdown to PostgreSQL is enabled.
    pub async fn new(parquet_path: &str, sources: &[PostgresSource]) -> Result<Self> {
        Self::new_with_pushdown(parquet_path, sources, true).await
    }

    /// Like [`Self::new`] but lets the caller disable filter pushdown on the
    /// registered PostgreSQL tables. With `filter_pushdown = false`, every
    /// predicate is applied by DataFusion locally rather than translated to a
    /// SQL `WHERE`. This exists so tests can differentially compare pushed vs.
    /// unpushed execution and assert identical results.
    pub async fn new_with_pushdown(
        parquet_path: &str,
        sources: &[PostgresSource],
        filter_pushdown: bool,
    ) -> Result<Self> {
        // Enable DataFusion's information_schema so BI tools (and tests) can
        // run `SHOW TABLES` / query `information_schema` against Igloo.
        let ctx =
            SessionContext::new_with_config(SessionConfig::new().with_information_schema(true));
        Self::register_iceberg_table(&ctx, parquet_path)?;
        Self::register_postgres_sources(&ctx, sources, filter_pushdown).await?;
        log::info!(
            "DataFusion context initialized with the Iceberg table and {} PostgreSQL source(s).",
            sources.len()
        );
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
        ctx.register_table(ICEBERG_TABLE, iceberg_table)?;
        Ok(())
    }

    /// Introspects every configured PostgreSQL source and registers its
    /// tables twice over:
    ///
    /// * canonically, in a DataFusion catalog named after the source, so
    ///   `<source>.<schema>.<table>` always resolves — this is the name BI
    ///   tools see and the only one guaranteed unique; and
    /// * as an unqualified alias in the default catalog (`orders`), so short
    ///   names stay ergonomic. Collisions across sources and schemas are
    ///   broken deterministically by [`catalog::resolve_aliases`].
    ///
    /// A discovered `my_pg_table` in the primary source additionally keeps
    /// the legacy `pg_table` alias. If a source yields no tables the engine
    /// still starts (with a warning) — the Parquet source may be all that's
    /// needed.
    async fn register_postgres_sources(
        ctx: &SessionContext,
        sources: &[PostgresSource],
        filter_pushdown: bool,
    ) -> Result<()> {
        let mut discovered: Vec<SourceTables> = Vec::with_capacity(sources.len());
        // One shared connection per source drives its introspection and all
        // of its scans.
        let mut clients: HashMap<&str, Arc<Client>> = HashMap::new();

        for source in sources {
            let client = Self::connect(source).await?;
            let tables =
                catalog::introspect_tables(&client, &source.schemas, source.tables.as_deref())
                    .await?;
            if tables.is_empty() {
                log::warn!(
                    "source {:?}: no PostgreSQL base tables found in schemas {:?}; \
                     registering none",
                    source.name,
                    source.schemas
                );
            }
            clients.insert(source.name.as_str(), client);
            discovered.push(SourceTables {
                source: source.name.clone(),
                tables,
            });
        }

        // Canonical <source>.<schema>.<table> registration, one catalog per
        // source. Providers are shared (Arc) with the alias registration
        // below so both names hit the same connection and counters.
        let mut providers: HashMap<(&str, &str, &str), Arc<dyn TableProvider>> = HashMap::new();
        for source in &discovered {
            let client = clients
                .get(source.source.as_str())
                .expect("client registered above");
            let catalog_provider = Arc::new(MemoryCatalogProvider::new());
            for table in &source.tables {
                let provider = Self::table_provider(client.clone(), table, filter_pushdown);
                Self::schema_provider(&catalog_provider, &table.schema)?
                    .register_table(table.name.clone(), provider.clone())?;
                providers.insert(
                    (&source.source, &table.schema, &table.name),
                    provider.clone(),
                );
                log::info!(
                    "registered PostgreSQL table {}.{}.{} ({} column(s))",
                    source.source,
                    table.schema,
                    table.name,
                    table.fields.len()
                );
            }
            ctx.register_catalog(source.source.clone(), catalog_provider);
        }

        // Unqualified aliases in the default catalog. `iceberg` and the
        // legacy alias are reserved so an upstream table of the same name
        // cannot clash with them.
        let mut legacy_registered = false;
        for alias in catalog::resolve_aliases(&discovered, &[ICEBERG_TABLE, LEGACY_ALIAS]) {
            let provider = providers
                .get(&(
                    alias.source,
                    alias.table.schema.as_str(),
                    alias.table.name.as_str(),
                ))
                .expect("provider built above")
                .clone();
            ctx.register_table(alias.alias.as_str(), provider.clone())?;
            log::debug!(
                "aliased {}.{}.{} as {:?}",
                alias.source,
                alias.table.schema,
                alias.table.name,
                alias.alias
            );

            // Backward compatibility: expose my_pg_table under `pg_table` too.
            if !legacy_registered && alias.table.name == LEGACY_SOURCE_TABLE {
                ctx.register_table(LEGACY_ALIAS, provider)?;
                legacy_registered = true;
                log::info!(
                    "registered legacy alias {:?} -> {}.{}.{} (deprecated; \
                     prefer the qualified table name)",
                    LEGACY_ALIAS,
                    alias.source,
                    alias.table.schema,
                    alias.table.name
                );
            }
        }
        Ok(())
    }

    /// Opens the connection for one source, driving its connection task in
    /// the background for the client's lifetime.
    async fn connect(source: &PostgresSource) -> Result<Arc<Client>> {
        let (client, connection) = tokio_postgres::connect(source.uri.expose(), NoTls)
            .await
            .map_err(IglooError::Postgres)?;
        let name = source.name.clone();
        tokio::spawn(async move {
            if let Err(e) = connection.await {
                // The URI (and any credentials in it) is never logged.
                log::error!("PostgreSQL connection error on source {:?}: {}", name, e);
            }
        });
        Ok(Arc::new(client))
    }

    /// Builds the table provider for one introspected table.
    fn table_provider(
        client: Arc<Client>,
        table: &TableSchema,
        filter_pushdown: bool,
    ) -> Arc<dyn TableProvider> {
        let arrow_schema = Arc::new(ArrowSchema::new(table.fields.clone()));
        Arc::new(
            PostgresTable::from_client(client, &table.schema, &table.name, arrow_schema)
                .with_filter_pushdown(filter_pushdown),
        )
    }

    /// Returns the catalog's schema provider for `name`, creating it on first
    /// use so each PostgreSQL schema becomes a DataFusion schema.
    fn schema_provider(
        catalog: &MemoryCatalogProvider,
        name: &str,
    ) -> Result<Arc<dyn SchemaProvider>> {
        if let Some(existing) = catalog.schema(name) {
            return Ok(existing);
        }
        catalog.register_schema(name, Arc::new(MemorySchemaProvider::new()))?;
        Ok(catalog
            .schema(name)
            .expect("schema registered on the line above"))
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
