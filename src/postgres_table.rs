// src/postgres_table.rs
use arrow::array::{
    ArrayRef, BinaryBuilder, BooleanBuilder, Date32Builder, Float32Builder, Float64Builder,
    Int16Builder, Int32Builder, Int64Builder, StringBuilder, TimestampNanosecondBuilder,
};
use arrow::datatypes::{DataType, Date32Type, Fields, SchemaRef, TimeUnit};
use arrow::record_batch::{RecordBatch, RecordBatchOptions};
use async_trait::async_trait;
use datafusion::catalog::Session;
use datafusion::datasource::TableProvider;
use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown, TableType};
use datafusion::physical_plan::memory::MemoryExec;
use datafusion::physical_plan::ExecutionPlan;
use std::any::Any;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio_postgres::{Client, NoTls};

use crate::errors::{IglooError, Result as IglooResult};
use crate::pushdown::translate_filter;

/// Quote a SQL identifier for PostgreSQL, escaping embedded double quotes.
///
/// Shared with [`crate::pushdown`] so column references in pushed-down
/// `WHERE` clauses are escaped identically to those in the projection.
pub(crate) fn quote_ident(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

/// Quote a (schema, table) pair into a schema-qualified relation reference,
/// e.g. `"public"."my_table"`. Qualifying is what lets scans resolve tables
/// that live outside the connection's default `search_path`. Each identifier
/// is quoted and escaped independently.
fn quote_relation(schema_name: &str, table_name: &str) -> String {
    format!("{}.{}", quote_ident(schema_name), quote_ident(table_name))
}

/// Build the SQL that [`PostgresTable::scan`] runs against PostgreSQL for a
/// given projection, pushed-down `WHERE` conjuncts, and `LIMIT`.
///
/// The table is referenced schema-qualified (`"schema"."table"`) so it
/// resolves regardless of the connection's `search_path`.
///
/// `where_conjuncts` are already-translated boolean SQL fragments (see
/// [`crate::pushdown::translate_filter`]); they are `AND`-ed together into the
/// `WHERE` clause. When empty, no `WHERE` is emitted.
///
/// When `projected_fields` is empty (e.g. `SELECT COUNT(*)`), this produces a
/// row-count query with the same `WHERE`/`LIMIT` applied inside the counted
/// subquery; otherwise it selects the projected columns explicitly, in
/// projected-schema order, so result column positions line up with the Arrow
/// schema. This is pure string construction with no client/session
/// dependency, kept as the single source of truth for the scan SQL.
fn build_scan_sql(
    schema_name: &str,
    table_name: &str,
    projected_fields: &Fields,
    where_conjuncts: &[String],
    limit: Option<usize>,
) -> String {
    let where_clause = if where_conjuncts.is_empty() {
        String::new()
    } else {
        format!(" WHERE {}", where_conjuncts.join(" AND "))
    };
    let limit_clause = limit.map(|n| format!(" LIMIT {}", n)).unwrap_or_default();
    let relation = quote_relation(schema_name, table_name);

    // A projection can be empty (e.g. `SELECT COUNT(*)`), in which case
    // DataFusion only needs the row count, not any column data.
    if projected_fields.is_empty() {
        format!(
            "SELECT COUNT(*) FROM (SELECT 1 FROM {}{}{}) AS t",
            relation, where_clause, limit_clause
        )
    } else {
        // Select the projected columns explicitly, in projected-schema order,
        // so result column positions line up with the Arrow schema.
        let sql_select_cols = projected_fields
            .iter()
            .map(|f| quote_ident(f.name()))
            .collect::<Vec<_>>()
            .join(", ");

        format!(
            "SELECT {} FROM {}{}{}",
            sql_select_cols, relation, where_clause, limit_clause
        )
    }
}

/// Decide the `LIMIT` to push to PostgreSQL given how many of the filters
/// DataFusion handed to `scan` were actually translated into the `WHERE`.
///
/// # Why this rule is correct
///
/// All pushed filters are classified `Inexact`, so DataFusion re-applies every
/// filter locally on top of the scan. An upstream `LIMIT` is applied *after*
/// the upstream `WHERE` but *before* DataFusion's local re-filter. If any
/// filter DataFusion gave us is **not** captured in the pushed `WHERE`, that
/// filter is only applied locally, and it may discard rows that the upstream
/// `LIMIT` already counted — so the upstream `LIMIT` could cut rows the query
/// still needs. That is unsafe.
///
/// It is only safe to push the `LIMIT` when the pushed `WHERE` captures
/// *every* filter (`translated == total`), because then the local re-filter
/// removes nothing (our translations are semantically faithful), so
/// `WHERE ... LIMIT n` upstream yields exactly what a local `LIMIT n` would.
/// When `total == 0` this trivially holds and the bare `LIMIT` is pushed as
/// before. Otherwise we conservatively drop the `LIMIT` and let DataFusion
/// apply it locally.
///
/// In practice DataFusion never even offers a `LIMIT` to `scan` while an
/// `Inexact` filter sits above the scan (the limit stays above the local
/// `Filter`), so this is a belt-and-braces guarantee at the SQL layer.
fn pushed_limit(limit: Option<usize>, translated: usize, total: usize) -> Option<usize> {
    if translated == total {
        limit
    } else {
        None
    }
}

// Represents a table physically stored in PostgreSQL
#[derive(Debug, Clone)]
pub struct PostgresTable {
    client: Arc<Client>,
    schema_name: String,
    table_name: String,
    schema: SchemaRef,
    /// When `false`, filter pushdown is disabled: `supports_filters_pushdown`
    /// reports every filter `Unsupported` and `scan` emits no `WHERE`, so
    /// DataFusion does all filtering locally. Defaults to `true`; the disabled
    /// mode exists so tests can differentially compare pushed vs. unpushed
    /// results for identical answers.
    filter_pushdown: bool,
    /// Count of data rows read from PostgreSQL by column-projecting scans.
    /// Shared across clones (an `Arc`) so callers holding any handle to this
    /// provider observe the same total. Lets tests prove pushdown actually
    /// reduced the rows transferred. The empty-projection `COUNT(*)` path does
    /// not fetch column data and does not increment this.
    rows_fetched: Arc<AtomicU64>,
}

impl PostgresTable {
    // Connects to PostgreSQL and keeps the client for later scans. The
    // connection task is driven in the background for the client's lifetime.
    // `schema_name` is the PostgreSQL schema (namespace) the table lives in,
    // e.g. `public`; scans are schema-qualified with it so tables outside the
    // connection's default `search_path` still resolve.
    pub async fn try_new(
        conn_str: &str,
        schema_name: &str,
        table_name: &str,
        schema: SchemaRef,
    ) -> IglooResult<Self> {
        let (client, connection) = tokio_postgres::connect(conn_str, NoTls)
            .await
            .map_err(IglooError::Postgres)?;

        tokio::spawn(async move {
            if let Err(e) = connection.await {
                log::error!("PostgreSQL connection error: {}", e);
            }
        });

        Ok(Self::from_client(
            Arc::new(client),
            schema_name,
            table_name,
            schema,
        ))
    }

    /// Builds a [`PostgresTable`] from an already-connected client. Lets
    /// callers (e.g. catalog registration) share one connection across many
    /// tables instead of opening one per table. Filter pushdown is enabled by
    /// default; disable it with [`Self::with_filter_pushdown`].
    pub fn from_client(
        client: Arc<Client>,
        schema_name: &str,
        table_name: &str,
        schema: SchemaRef,
    ) -> Self {
        Self {
            client,
            schema_name: schema_name.to_string(),
            table_name: table_name.to_string(),
            schema,
            filter_pushdown: true,
            rows_fetched: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Enable or disable filter pushdown on this provider (default enabled).
    /// With pushdown off, predicates are never translated to SQL and every
    /// filter is applied by DataFusion locally — used to differential-test
    /// that pushed and unpushed queries return identical results.
    pub fn with_filter_pushdown(mut self, enabled: bool) -> Self {
        self.filter_pushdown = enabled;
        self
    }

    /// Total number of data rows this provider has read from PostgreSQL across
    /// all column-projecting scans. Monotonic; shared across clones.
    pub fn rows_fetched(&self) -> u64 {
        self.rows_fetched.load(Ordering::Relaxed)
    }
}

#[async_trait]
impl TableProvider for PostgresTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    /// Classify each filter for pushdown. Everything translatable is reported
    /// [`TableProviderFilterPushDown::Inexact`] (never `Exact`): DataFusion
    /// re-applies the filter locally, so a query's correctness never depends on
    /// the fidelity of our SQL translation — pushdown is purely a row-reduction
    /// optimization. Anything outside the supported grammar (or when pushdown
    /// is disabled) is `Unsupported` and applied entirely by DataFusion.
    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> DFResult<Vec<TableProviderFilterPushDown>> {
        Ok(filters
            .iter()
            .map(|f| {
                if self.filter_pushdown && translate_filter(f, self.schema.as_ref()).is_some() {
                    TableProviderFilterPushDown::Inexact
                } else {
                    TableProviderFilterPushDown::Unsupported
                }
            })
            .collect())
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        let projected_schema = match projection {
            Some(p) => Arc::new(self.schema.project(p)?),
            None => self.schema.clone(),
        };

        // Translate the filters DataFusion pushed to us into WHERE conjuncts.
        // When pushdown is disabled, or a filter is outside the supported
        // grammar, it is left for DataFusion to apply locally (Inexact
        // semantics), so skipping it here is always safe.
        let where_conjuncts: Vec<String> = if self.filter_pushdown {
            filters
                .iter()
                .filter_map(|f| translate_filter(f, self.schema.as_ref()))
                .collect()
        } else {
            Vec::new()
        };

        // Only push LIMIT when every filter DataFusion handed us made it into
        // the WHERE (see `pushed_limit` for the correctness argument).
        let effective_limit = pushed_limit(limit, where_conjuncts.len(), filters.len());

        let query = build_scan_sql(
            &self.schema_name,
            &self.table_name,
            projected_schema.fields(),
            &where_conjuncts,
            effective_limit,
        );

        // Log the generated SQL (structure only) at debug level. Predicate
        // *values* live in the SQL string, so this stays at debug and never
        // info, to avoid logging data at normal verbosity.
        log::debug!("Executing scan query on Postgres: {}", query);

        // A projection can be empty (e.g. `SELECT COUNT(*)`), in which case
        // DataFusion only needs the row count, not any column data.
        if projected_schema.fields().is_empty() {
            let row = self
                .client
                .query_one(&query, &[])
                .await
                .map_err(|e| DataFusionError::External(Box::new(IglooError::Postgres(e))))?;
            let row_count: i64 = row.get(0);
            let options = RecordBatchOptions::new().with_row_count(Some(row_count as usize));
            let batch =
                RecordBatch::try_new_with_options(projected_schema.clone(), vec![], &options)
                    .map_err(|e| DataFusionError::ArrowError(e, None))?;
            return Ok(Arc::new(MemoryExec::try_new(
                &[vec![batch]],
                projected_schema,
                None,
            )?));
        }

        let rows =
            self.client.query(&query, &[]).await.map_err(|pg_err| {
                DataFusionError::External(Box::new(IglooError::Postgres(pg_err)))
            })?;

        // Record how many rows PostgreSQL actually returned for this scan, so
        // callers can prove filter pushdown reduced the transfer.
        self.rows_fetched
            .fetch_add(rows.len() as u64, Ordering::Relaxed);

        let mut arrow_columns: Vec<ArrayRef> = Vec::with_capacity(projected_schema.fields().len());

        for (col_idx, field) in projected_schema.fields().iter().enumerate() {
            // Builds one Arrow array from column `col_idx` of every row.
            // `$convert` maps the decoded Postgres value to the value the
            // Arrow builder accepts, and may fail (e.g. timestamp overflow).
            macro_rules! build_array {
                ($pg_ty:ty, $builder:expr, $convert:expr) => {{
                    let mut builder = $builder;
                    for row in &rows {
                        match row.try_get::<usize, Option<$pg_ty>>(col_idx) {
                            Ok(Some(val)) => builder.append_value($convert(val)?),
                            Ok(None) => builder.append_null(),
                            Err(e) => {
                                return Err(DataFusionError::External(Box::new(
                                    IglooError::Postgres(e),
                                )))
                            }
                        }
                    }
                    Arc::new(builder.finish()) as ArrayRef
                }};
            }

            let array_ref: ArrayRef = match field.data_type() {
                DataType::Int16 => build_array!(
                    i16,
                    Int16Builder::with_capacity(rows.len()),
                    |v: i16| -> DFResult<i16> { Ok(v) }
                ),
                DataType::Int32 => build_array!(
                    i32,
                    Int32Builder::with_capacity(rows.len()),
                    |v: i32| -> DFResult<i32> { Ok(v) }
                ),
                DataType::Int64 => build_array!(
                    i64,
                    Int64Builder::with_capacity(rows.len()),
                    |v: i64| -> DFResult<i64> { Ok(v) }
                ),
                DataType::Float32 => build_array!(
                    f32,
                    Float32Builder::with_capacity(rows.len()),
                    |v: f32| -> DFResult<f32> { Ok(v) }
                ),
                DataType::Float64 => build_array!(
                    f64,
                    Float64Builder::with_capacity(rows.len()),
                    |v: f64| -> DFResult<f64> { Ok(v) }
                ),
                DataType::Boolean => build_array!(
                    bool,
                    BooleanBuilder::with_capacity(rows.len()),
                    |v: bool| -> DFResult<bool> { Ok(v) }
                ),
                DataType::Utf8 => build_array!(
                    String,
                    StringBuilder::with_capacity(rows.len(), 0),
                    |v: String| -> DFResult<String> { Ok(v) }
                ),
                DataType::Binary => build_array!(
                    Vec<u8>,
                    BinaryBuilder::with_capacity(rows.len(), 0),
                    |v: Vec<u8>| -> DFResult<Vec<u8>> { Ok(v) }
                ),
                DataType::Timestamp(TimeUnit::Nanosecond, None) => build_array!(
                    chrono::NaiveDateTime,
                    TimestampNanosecondBuilder::with_capacity(rows.len()),
                    |v: chrono::NaiveDateTime| {
                        v.and_utc().timestamp_nanos_opt().ok_or_else(|| {
                            DataFusionError::Execution(format!(
                                "timestamp {} is out of range for nanosecond precision",
                                v
                            ))
                        })
                    }
                ),
                DataType::Date32 => build_array!(
                    chrono::NaiveDate,
                    Date32Builder::with_capacity(rows.len()),
                    |v: chrono::NaiveDate| -> DFResult<i32> { Ok(Date32Type::from_naive_date(v)) }
                ),
                dt => {
                    return Err(DataFusionError::External(Box::new(
                        IglooError::UnsupportedArrowType(dt.clone()),
                    )));
                }
            };
            arrow_columns.push(array_ref);
        }

        let batch = RecordBatch::try_new(projected_schema.clone(), arrow_columns)
            .map_err(|e| DataFusionError::ArrowError(e, None))?;

        // The batch is already projected, so no further projection is applied.
        Ok(Arc::new(MemoryExec::try_new(
            &[vec![batch]],
            projected_schema,
            None,
        )?))
    }
}

#[cfg(test)]
mod tests {
    use super::{build_scan_sql, pushed_limit, quote_ident};
    use arrow::datatypes::{DataType, Field, Fields};

    /// Build an Arrow `Fields` from column names. The concrete data type is
    /// irrelevant to `build_scan_sql`, which only reads field names.
    fn fields_from(names: &[&str]) -> Fields {
        names
            .iter()
            .map(|n| Field::new(*n, DataType::Int64, true))
            .collect()
    }

    /// No WHERE conjuncts — the common no-pushdown shorthand for tests.
    const NO_WHERE: &[String] = &[];

    #[test]
    fn quote_ident_wraps_in_double_quotes() {
        assert_eq!(quote_ident("my_table"), "\"my_table\"");
    }

    #[test]
    fn quote_ident_escapes_embedded_quotes() {
        assert_eq!(quote_ident("evil\"name"), "\"evil\"\"name\"");
    }

    #[test]
    fn build_scan_sql_projection_no_limit() {
        let sql = build_scan_sql(
            "public",
            "my_pg_table",
            &fields_from(&["user_id", "extra_info"]),
            NO_WHERE,
            None,
        );
        assert_eq!(
            sql,
            "SELECT \"user_id\", \"extra_info\" FROM \"public\".\"my_pg_table\""
        );
    }

    #[test]
    fn build_scan_sql_projection_with_limit() {
        let sql = build_scan_sql(
            "public",
            "my_pg_table",
            &fields_from(&["user_id", "extra_info"]),
            NO_WHERE,
            Some(5),
        );
        assert_eq!(
            sql,
            "SELECT \"user_id\", \"extra_info\" FROM \"public\".\"my_pg_table\" LIMIT 5"
        );
    }

    #[test]
    fn build_scan_sql_empty_projection_with_limit() {
        let sql = build_scan_sql(
            "public",
            "my_pg_table",
            &fields_from(&[]),
            NO_WHERE,
            Some(5),
        );
        assert_eq!(
            sql,
            "SELECT COUNT(*) FROM (SELECT 1 FROM \"public\".\"my_pg_table\" LIMIT 5) AS t"
        );
    }

    #[test]
    fn build_scan_sql_empty_projection_no_limit() {
        let sql = build_scan_sql("public", "my_pg_table", &fields_from(&[]), NO_WHERE, None);
        assert_eq!(
            sql,
            "SELECT COUNT(*) FROM (SELECT 1 FROM \"public\".\"my_pg_table\") AS t"
        );
    }

    #[test]
    fn build_scan_sql_qualifies_non_default_schema() {
        // A table in a non-default schema resolves because the relation is
        // schema-qualified.
        let sql = build_scan_sql("analytics", "events", &fields_from(&["id"]), NO_WHERE, None);
        assert_eq!(sql, "SELECT \"id\" FROM \"analytics\".\"events\"");
    }

    #[test]
    fn build_scan_sql_escapes_embedded_quotes() {
        // The schema, table and column names each contain a double quote,
        // which `quote_ident` doubles when escaping — independently per part.
        let sql = build_scan_sql(
            "sch\"ema",
            "we\"ird",
            &fields_from(&["c\"ol"]),
            NO_WHERE,
            None,
        );
        assert_eq!(sql, "SELECT \"c\"\"ol\" FROM \"sch\"\"ema\".\"we\"\"ird\"");
    }

    #[test]
    fn build_scan_sql_single_where_conjunct() {
        let sql = build_scan_sql(
            "public",
            "my_pg_table",
            &fields_from(&["user_id"]),
            &["\"user_id\" > 5".to_string()],
            None,
        );
        assert_eq!(
            sql,
            "SELECT \"user_id\" FROM \"public\".\"my_pg_table\" WHERE \"user_id\" > 5"
        );
    }

    #[test]
    fn build_scan_sql_projection_where_and_limit_combined() {
        // Projection + multiple WHERE conjuncts (AND-joined) + LIMIT together.
        let sql = build_scan_sql(
            "public",
            "my_pg_table",
            &fields_from(&["user_id", "extra_info"]),
            &[
                "\"user_id\" >= 10".to_string(),
                "\"extra_info\" = 'vip'".to_string(),
            ],
            Some(3),
        );
        assert_eq!(
            sql,
            "SELECT \"user_id\", \"extra_info\" FROM \"public\".\"my_pg_table\" \
             WHERE \"user_id\" >= 10 AND \"extra_info\" = 'vip' LIMIT 3"
        );
    }

    #[test]
    fn build_scan_sql_empty_projection_with_where() {
        // WHERE is applied inside the counted subquery.
        let sql = build_scan_sql(
            "public",
            "my_pg_table",
            &fields_from(&[]),
            &["\"user_id\" = 42".to_string()],
            None,
        );
        assert_eq!(
            sql,
            "SELECT COUNT(*) FROM (SELECT 1 FROM \"public\".\"my_pg_table\" \
             WHERE \"user_id\" = 42) AS t"
        );
    }

    // --- WHERE + LIMIT interaction rule (`pushed_limit`) ---------------------

    #[test]
    fn pushed_limit_no_filters_keeps_limit() {
        // 0 of 0 filters translated → safe to push the bare LIMIT.
        assert_eq!(pushed_limit(Some(10), 0, 0), Some(10));
        assert_eq!(pushed_limit(None, 0, 0), None);
    }

    #[test]
    fn pushed_limit_all_filters_translated_keeps_limit() {
        // Every filter DataFusion gave us made it into the WHERE → safe.
        assert_eq!(pushed_limit(Some(10), 2, 2), Some(10));
    }

    #[test]
    fn pushed_limit_partial_translation_drops_limit() {
        // A filter is only applied locally → pushing LIMIT upstream is unsafe.
        assert_eq!(pushed_limit(Some(10), 1, 2), None);
        assert_eq!(pushed_limit(Some(10), 0, 1), None);
    }
}
