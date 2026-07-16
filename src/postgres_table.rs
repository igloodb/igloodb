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
use datafusion::logical_expr::{Expr, Operator, TableProviderFilterPushDown, TableType};
use datafusion::physical_plan::memory::MemoryExec;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::scalar::ScalarValue;
use std::any::Any;
use std::sync::Arc;
use tokio_postgres::{Client, NoTls};

use crate::errors::{IglooError, Result as IglooResult};

/// Quote a SQL identifier for PostgreSQL, escaping embedded double quotes.
fn quote_ident(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

/// Quote a (schema, table) pair into a schema-qualified relation reference,
/// e.g. `"public"."my_table"`. Qualifying is what lets scans resolve tables
/// that live outside the connection's default `search_path`. Each identifier
/// is quoted and escaped independently.
fn quote_relation(schema_name: &str, table_name: &str) -> String {
    format!("{}.{}", quote_ident(schema_name), quote_ident(table_name))
}

/// The category of a translated literal, which determines the set of
/// comparison operators that translate to PostgreSQL with identical
/// semantics.
#[derive(Clone, Copy)]
enum LiteralKind {
    /// Signed integer or boolean operand. Every comparison operator orders
    /// these values identically in Arrow/DataFusion and PostgreSQL.
    IntOrBool,
    /// UTF-8 string operand. Only equality/inequality are exactly equivalent;
    /// ordering depends on collation (see [`comparison_to_sql`]).
    Str,
}

/// Translate a literal [`Expr`] into a PostgreSQL literal, returning the SQL
/// text together with its [`LiteralKind`], or `None` for any value outside the
/// exactly-translatable whitelist.
///
/// Only non-NULL `Int8`/`Int16`/`Int32`/`Int64`, `Boolean`, and `Utf8` are
/// accepted. NULL literals, floats, decimals, dates, timestamps, unsigned and
/// large integers, and every other `ScalarValue` return `None`, because their
/// PostgreSQL text representation and/or comparison semantics could diverge
/// from DataFusion's (e.g. float text round-trip, or `col = NULL` which is
/// never true and must not be pushed as a comparison).
fn literal_to_sql(expr: &Expr) -> Option<(String, LiteralKind)> {
    let value = match expr {
        Expr::Literal(value) => value,
        _ => return None,
    };
    match value {
        ScalarValue::Int8(Some(v)) => Some((v.to_string(), LiteralKind::IntOrBool)),
        ScalarValue::Int16(Some(v)) => Some((v.to_string(), LiteralKind::IntOrBool)),
        ScalarValue::Int32(Some(v)) => Some((v.to_string(), LiteralKind::IntOrBool)),
        ScalarValue::Int64(Some(v)) => Some((v.to_string(), LiteralKind::IntOrBool)),
        ScalarValue::Boolean(Some(v)) => Some((
            (if *v { "TRUE" } else { "FALSE" }).to_string(),
            LiteralKind::IntOrBool,
        )),
        ScalarValue::Utf8(Some(s)) => {
            // PostgreSQL text cannot contain a NUL byte; reject rather than
            // emit a literal that would not round-trip.
            if s.contains('\0') {
                return None;
            }
            // Escape by doubling single quotes. `standard_conforming_strings`
            // is on by default in PostgreSQL, so backslashes are literal and
            // need no extra escaping.
            Some((format!("'{}'", s.replace('\'', "''")), LiteralKind::Str))
        }
        _ => None,
    }
}

/// If `expr` references a column present in `schema` by exact name, return its
/// quoted PostgreSQL identifier; otherwise `None`. This is what rejects
/// column-vs-column comparisons, casts, and references to columns the provider
/// does not expose.
fn column_to_sql(expr: &Expr, schema: &SchemaRef) -> Option<String> {
    match expr {
        Expr::Column(column) if schema.fields().iter().any(|f| f.name() == &column.name) => {
            Some(quote_ident(&column.name))
        }
        _ => None,
    }
}

/// Translate a binary comparison between a schema column and a whitelisted
/// literal (in either operand order) into a PostgreSQL boolean snippet, or
/// `None` if the shape or operator is not exactly translatable.
///
/// The operands are rendered in their original positions, so no operator
/// flipping is needed and the comparison stays exact. Only `=`, `<>`, `<`,
/// `<=`, `>`, `>=` are handled.
///
/// Ordering operators (`<`, `<=`, `>`, `>=`) are rejected for string operands:
/// PostgreSQL orders text by the column's collation, which can diverge from
/// Arrow's byte ordering, so pushing them could drop or keep the wrong rows.
/// String equality/inequality is pushed on the assumption of a *deterministic*
/// collation (the PostgreSQL default), under which text equality matches
/// Arrow's byte-wise equality exactly.
fn comparison_to_sql(
    left: &Expr,
    op: Operator,
    right: &Expr,
    schema: &SchemaRef,
) -> Option<String> {
    let op_sql = match op {
        Operator::Eq => "=",
        Operator::NotEq => "<>",
        Operator::Lt => "<",
        Operator::LtEq => "<=",
        Operator::Gt => ">",
        Operator::GtEq => ">=",
        _ => return None,
    };

    // Exactly one operand must be a schema column and the other a whitelisted
    // literal. Both-columns, both-literals, casts, and unknown columns fall
    // through to `None`.
    let (left_sql, right_sql, kind) = match (column_to_sql(left, schema), literal_to_sql(right)) {
        (Some(column), Some((literal, kind))) => (column, literal, kind),
        _ => match (literal_to_sql(left), column_to_sql(right, schema)) {
            (Some((literal, kind)), Some(column)) => (literal, column, kind),
            _ => return None,
        },
    };

    // String ordering diverges from Arrow byte ordering; only `=`/`<>` are
    // exact for strings (see the doc comment above).
    if matches!(kind, LiteralKind::Str) && !matches!(op, Operator::Eq | Operator::NotEq) {
        return None;
    }

    Some(format!("({} {} {})", left_sql, op_sql, right_sql))
}

/// Translate a DataFusion filter [`Expr`] into an *exactly* equivalent
/// PostgreSQL boolean SQL snippet, or `None` when exact equivalence cannot be
/// guaranteed.
///
/// "Exactly equivalent" means the returned snippet keeps and rejects precisely
/// the same rows in PostgreSQL as `expr` does in DataFusion, including
/// identical three-valued logic over NULLs. This is the single source of truth
/// shared by [`PostgresTable::supports_filters_pushdown`] and
/// [`PostgresTable::scan`]. Because the contract is exact (never merely
/// "close"), pushing a translated predicate stays sound even when composed
/// with a pushed-down `LIMIT`: there is never an under-fetch that the
/// re-applied filter above the scan could not recover.
///
/// Whitelisted constructs: column-vs-literal comparisons (see
/// [`comparison_to_sql`]), `IS NULL`/`IS NOT NULL` on schema columns, and
/// `AND`/`OR`/`NOT` over translatable subexpressions. Everything else (LIKE,
/// IN, BETWEEN, casts, functions, column-vs-column, subqueries, ...) returns
/// `None`. `AND`/`OR`/`NOT` only translate when *all* their operands do, so a
/// partially translatable predicate is never pushed as a weaker one.
///
/// The result is always fully parenthesized, so callers can join several
/// snippets with ` AND ` without any operator-precedence ambiguity.
fn try_expr_to_sql(expr: &Expr, schema: &SchemaRef) -> Option<String> {
    match expr {
        Expr::BinaryExpr(binary) => match binary.op {
            Operator::And => {
                let left = try_expr_to_sql(&binary.left, schema)?;
                let right = try_expr_to_sql(&binary.right, schema)?;
                Some(format!("({} AND {})", left, right))
            }
            Operator::Or => {
                let left = try_expr_to_sql(&binary.left, schema)?;
                let right = try_expr_to_sql(&binary.right, schema)?;
                Some(format!("({} OR {})", left, right))
            }
            op => comparison_to_sql(&binary.left, op, &binary.right, schema),
        },
        Expr::Not(inner) => {
            let inner = try_expr_to_sql(inner, schema)?;
            Some(format!("(NOT {})", inner))
        }
        Expr::IsNull(inner) => Some(format!("({} IS NULL)", column_to_sql(inner, schema)?)),
        Expr::IsNotNull(inner) => Some(format!("({} IS NOT NULL)", column_to_sql(inner, schema)?)),
        _ => None,
    }
}

/// Build the SQL that [`PostgresTable::scan`] runs against PostgreSQL for a
/// given projection.
///
/// The table is referenced schema-qualified (`"schema"."table"`) so it
/// resolves regardless of the connection's `search_path`.
///
/// When `projected_fields` is empty (e.g. `SELECT COUNT(*)`), this produces a
/// row-count query; otherwise it selects the projected columns explicitly, in
/// projected-schema order, so result column positions line up with the Arrow
/// schema. This is pure string construction with no client/session
/// dependency, kept as the single source of truth for the scan SQL.
///
/// `where_clauses` holds already-parenthesized boolean snippets produced by
/// [`try_expr_to_sql`]. They are joined with ` AND ` and emitted between the
/// `FROM` and any `LIMIT`, in both the row-count and normal paths. When
/// `where_clauses` is empty the output is byte-identical to the no-filter
/// query.
fn build_scan_sql(
    schema_name: &str,
    table_name: &str,
    projected_fields: &Fields,
    where_clauses: &[String],
    limit: Option<usize>,
) -> String {
    let where_clause = if where_clauses.is_empty() {
        String::new()
    } else {
        format!(" WHERE {}", where_clauses.join(" AND "))
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

// Represents a table physically stored in PostgreSQL
#[derive(Debug, Clone)]
pub struct PostgresTable {
    client: Arc<Client>,
    schema_name: String,
    table_name: String,
    schema: SchemaRef,
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

        Ok(Self {
            client: Arc::new(client),
            schema_name: schema_name.to_string(),
            table_name: table_name.to_string(),
            schema,
        })
    }

    /// Builds a [`PostgresTable`] from an already-connected client. Lets
    /// callers (e.g. catalog registration) share one connection across many
    /// tables instead of opening one per table.
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
        }
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

    /// Report which filters can be pushed into the generated SQL.
    ///
    /// A filter is `Inexact` when [`try_expr_to_sql`] can translate it (the
    /// single source of truth also used by [`Self::scan`]), otherwise
    /// `Unsupported`. `Inexact` is deliberate belt-and-braces: DataFusion
    /// re-applies every pushed predicate above the scan, so even a translation
    /// bug cannot silently corrupt results — at worst it over- or under-fetches
    /// rows the re-applied filter then corrects. Combined with the exactness
    /// contract of [`try_expr_to_sql`] (which never under-fetches), this keeps
    /// pushdown sound even when composed with a pushed-down `LIMIT`.
    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> DFResult<Vec<TableProviderFilterPushDown>> {
        Ok(filters
            .iter()
            .map(|filter| {
                if try_expr_to_sql(filter, &self.schema).is_some() {
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

        // Translate the pushed-down filters with the same single source of
        // truth used by `supports_filters_pushdown`. Only exactly-equivalent
        // snippets are emitted; anything else is skipped here and (having been
        // reported `Inexact`/`Unsupported`) is re-applied by DataFusion above
        // the scan.
        let where_clauses: Vec<String> = filters
            .iter()
            .filter_map(|filter| try_expr_to_sql(filter, &self.schema))
            .collect();

        let query = build_scan_sql(
            &self.schema_name,
            &self.table_name,
            projected_schema.fields(),
            &where_clauses,
            limit,
        );

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

        log::debug!("Executing scan query on Postgres: {}", query);

        let rows =
            self.client.query(&query, &[]).await.map_err(|pg_err| {
                DataFusionError::External(Box::new(IglooError::Postgres(pg_err)))
            })?;

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
    use super::{build_scan_sql, quote_ident, try_expr_to_sql, PostgresTable};
    use arrow::array::{Array, Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Fields, Schema, SchemaRef};
    use arrow::record_batch::RecordBatch;
    use datafusion::logical_expr::{col, lit, not};
    use datafusion::prelude::SessionContext;
    use datafusion::scalar::ScalarValue;
    use std::sync::Arc;

    /// Build an Arrow `Fields` from column names. The concrete data type is
    /// irrelevant to `build_scan_sql`, which only reads field names.
    fn fields_from(names: &[&str]) -> Fields {
        names
            .iter()
            .map(|n| Field::new(*n, DataType::Int64, true))
            .collect()
    }

    /// The `my_pg_table` schema: `user_id BIGINT NOT NULL`, `extra_info TEXT`.
    /// Shared by the expression-translation unit tests and the live-database
    /// integration tests.
    fn test_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("user_id", DataType::Int64, false),
            Field::new("extra_info", DataType::Utf8, true),
        ]))
    }

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
            &[],
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
            &[],
            Some(5),
        );
        assert_eq!(
            sql,
            "SELECT \"user_id\", \"extra_info\" FROM \"public\".\"my_pg_table\" LIMIT 5"
        );
    }

    #[test]
    fn build_scan_sql_empty_projection_with_limit() {
        let sql = build_scan_sql("public", "my_pg_table", &fields_from(&[]), &[], Some(5));
        assert_eq!(
            sql,
            "SELECT COUNT(*) FROM (SELECT 1 FROM \"public\".\"my_pg_table\" LIMIT 5) AS t"
        );
    }

    #[test]
    fn build_scan_sql_empty_projection_no_limit() {
        let sql = build_scan_sql("public", "my_pg_table", &fields_from(&[]), &[], None);
        assert_eq!(
            sql,
            "SELECT COUNT(*) FROM (SELECT 1 FROM \"public\".\"my_pg_table\") AS t"
        );
    }

    #[test]
    fn build_scan_sql_qualifies_non_default_schema() {
        // A table in a non-default schema resolves because the relation is
        // schema-qualified.
        let sql = build_scan_sql("analytics", "events", &fields_from(&["id"]), &[], None);
        assert_eq!(sql, "SELECT \"id\" FROM \"analytics\".\"events\"");
    }

    #[test]
    fn build_scan_sql_escapes_embedded_quotes() {
        // The schema, table and column names each contain a double quote,
        // which `quote_ident` doubles when escaping — independently per part.
        let sql = build_scan_sql("sch\"ema", "we\"ird", &fields_from(&["c\"ol"]), &[], None);
        assert_eq!(sql, "SELECT \"c\"\"ol\" FROM \"sch\"\"ema\".\"we\"\"ird\"");
    }

    // --- build_scan_sql with WHERE clauses ---------------------------------

    #[test]
    fn build_scan_sql_single_where_with_limit() {
        let sql = build_scan_sql(
            "public",
            "my_pg_table",
            &fields_from(&["user_id", "extra_info"]),
            &["(\"user_id\" = 42)".to_string()],
            Some(5),
        );
        assert_eq!(
            sql,
            "SELECT \"user_id\", \"extra_info\" FROM \"public\".\"my_pg_table\" WHERE (\"user_id\" = 42) LIMIT 5"
        );
    }

    #[test]
    fn build_scan_sql_two_where_clauses() {
        let sql = build_scan_sql(
            "public",
            "my_pg_table",
            &fields_from(&["user_id"]),
            &[
                "(\"user_id\" > 7)".to_string(),
                "(\"extra_info\" IS NULL)".to_string(),
            ],
            None,
        );
        assert_eq!(
            sql,
            "SELECT \"user_id\" FROM \"public\".\"my_pg_table\" WHERE (\"user_id\" > 7) AND (\"extra_info\" IS NULL)"
        );
    }

    #[test]
    fn build_scan_sql_count_path_with_where() {
        let sql = build_scan_sql(
            "public",
            "my_pg_table",
            &fields_from(&[]),
            &["(\"user_id\" > 7)".to_string()],
            None,
        );
        assert_eq!(
            sql,
            "SELECT COUNT(*) FROM (SELECT 1 FROM \"public\".\"my_pg_table\" WHERE (\"user_id\" > 7)) AS t"
        );
    }

    // --- try_expr_to_sql translation ---------------------------------------

    #[test]
    fn translate_int_comparisons_all_operators() {
        let schema = test_schema();
        assert_eq!(
            try_expr_to_sql(&col("user_id").eq(lit(42_i64)), &schema).unwrap(),
            "(\"user_id\" = 42)"
        );
        assert_eq!(
            try_expr_to_sql(&col("user_id").not_eq(lit(42_i64)), &schema).unwrap(),
            "(\"user_id\" <> 42)"
        );
        assert_eq!(
            try_expr_to_sql(&col("user_id").lt(lit(42_i64)), &schema).unwrap(),
            "(\"user_id\" < 42)"
        );
        assert_eq!(
            try_expr_to_sql(&col("user_id").lt_eq(lit(42_i64)), &schema).unwrap(),
            "(\"user_id\" <= 42)"
        );
        assert_eq!(
            try_expr_to_sql(&col("user_id").gt(lit(42_i64)), &schema).unwrap(),
            "(\"user_id\" > 42)"
        );
        assert_eq!(
            try_expr_to_sql(&col("user_id").gt_eq(lit(42_i64)), &schema).unwrap(),
            "(\"user_id\" >= 42)"
        );
    }

    #[test]
    fn translate_literal_on_left_preserves_order() {
        // Column-vs-literal is accepted in either operand order, and operands
        // are rendered in their original positions.
        let schema = test_schema();
        assert_eq!(
            try_expr_to_sql(&lit(42_i64).eq(col("user_id")), &schema).unwrap(),
            "(42 = \"user_id\")"
        );
        assert_eq!(
            try_expr_to_sql(&lit(7_i64).lt(col("user_id")), &schema).unwrap(),
            "(7 < \"user_id\")"
        );
    }

    #[test]
    fn translate_string_equality_escapes_quotes() {
        let schema = test_schema();
        assert_eq!(
            try_expr_to_sql(&col("extra_info").eq(lit("it's")), &schema).unwrap(),
            "(\"extra_info\" = 'it''s')"
        );
        assert_eq!(
            try_expr_to_sql(&col("extra_info").not_eq(lit("x")), &schema).unwrap(),
            "(\"extra_info\" <> 'x')"
        );
    }

    #[test]
    fn translate_boolean_literal() {
        let schema: SchemaRef = Arc::new(Schema::new(vec![Field::new(
            "flag",
            DataType::Boolean,
            true,
        )]));
        assert_eq!(
            try_expr_to_sql(&col("flag").eq(lit(true)), &schema).unwrap(),
            "(\"flag\" = TRUE)"
        );
        assert_eq!(
            try_expr_to_sql(&col("flag").not_eq(lit(false)), &schema).unwrap(),
            "(\"flag\" <> FALSE)"
        );
    }

    #[test]
    fn translate_is_null_and_is_not_null() {
        let schema = test_schema();
        assert_eq!(
            try_expr_to_sql(&col("extra_info").is_null(), &schema).unwrap(),
            "(\"extra_info\" IS NULL)"
        );
        assert_eq!(
            try_expr_to_sql(&col("user_id").is_not_null(), &schema).unwrap(),
            "(\"user_id\" IS NOT NULL)"
        );
    }

    #[test]
    fn translate_and_or_not_parenthesizes() {
        let schema = test_schema();
        let and = col("user_id")
            .gt(lit(7_i64))
            .and(col("extra_info").is_null());
        assert_eq!(
            try_expr_to_sql(&and, &schema).unwrap(),
            "((\"user_id\" > 7) AND (\"extra_info\" IS NULL))"
        );
        let or = col("user_id")
            .eq(lit(42_i64))
            .or(col("user_id").eq(lit(7_i64)));
        assert_eq!(
            try_expr_to_sql(&or, &schema).unwrap(),
            "((\"user_id\" = 42) OR (\"user_id\" = 7))"
        );
        let negated = not(col("user_id").eq(lit(42_i64)));
        assert_eq!(
            try_expr_to_sql(&negated, &schema).unwrap(),
            "(NOT (\"user_id\" = 42))"
        );
    }

    #[test]
    fn reject_string_ordering() {
        // PG collation order for text diverges from Arrow byte order, so `<`,
        // `<=`, `>`, `>=` on strings must not be pushed.
        let schema = test_schema();
        assert!(try_expr_to_sql(&col("extra_info").lt(lit("a")), &schema).is_none());
        assert!(try_expr_to_sql(&col("extra_info").lt_eq(lit("a")), &schema).is_none());
        assert!(try_expr_to_sql(&col("extra_info").gt(lit("a")), &schema).is_none());
        assert!(try_expr_to_sql(&col("extra_info").gt_eq(lit("a")), &schema).is_none());
    }

    #[test]
    fn reject_float_literal() {
        let schema = test_schema();
        assert!(try_expr_to_sql(&col("user_id").eq(lit(1.5_f64)), &schema).is_none());
    }

    #[test]
    fn reject_unknown_column() {
        let schema = test_schema();
        assert!(try_expr_to_sql(&col("does_not_exist").eq(lit(1_i64)), &schema).is_none());
    }

    #[test]
    fn reject_like() {
        let schema = test_schema();
        assert!(try_expr_to_sql(&col("extra_info").like(lit("%x%")), &schema).is_none());
    }

    #[test]
    fn reject_null_literal_comparison() {
        // `col = NULL` is never true and must not be pushed as a comparison.
        let schema = test_schema();
        let expr = col("user_id").eq(lit(ScalarValue::Int64(None)));
        assert!(try_expr_to_sql(&expr, &schema).is_none());
    }

    #[test]
    fn reject_column_vs_column() {
        let schema = test_schema();
        assert!(try_expr_to_sql(&col("user_id").eq(col("user_id")), &schema).is_none());
    }

    #[test]
    fn reject_and_with_untranslatable_child() {
        // A conjunction is only pushed when *every* operand translates; an
        // untranslatable child (here, string ordering) rejects the whole AND
        // rather than pushing a weaker predicate.
        let schema = test_schema();
        let expr = col("user_id")
            .eq(lit(42_i64))
            .and(col("extra_info").gt(lit("a")));
        assert!(try_expr_to_sql(&expr, &schema).is_none());
    }

    // --- Integration tests (live PostgreSQL) -------------------------------
    //
    // Each reads `IGLOO_TEST_POSTGRES_URI` at runtime and skips (printing a
    // note to stderr) when it is unset, so a plain `cargo test` stays green on
    // machines without a database. They exercise the full DataFusion ->
    // PostgresTable pushdown path against the seeded `my_pg_table`:
    //   (42, 'answer to everything'), (7, 'lucky number'), (100, NULL).

    /// Read the integration-test database URI, or skip the calling test.
    macro_rules! pg_uri_or_skip {
        ($name:literal) => {
            match std::env::var("IGLOO_TEST_POSTGRES_URI") {
                Ok(uri) => uri,
                Err(_) => {
                    eprintln!("skipping {}: IGLOO_TEST_POSTGRES_URI not set", $name);
                    return;
                }
            }
        };
    }

    /// Register `my_pg_table` as `pg_table` in a fresh `SessionContext` and run
    /// `sql`, returning the collected result batches.
    async fn scan_via_datafusion(uri: &str, sql: &str) -> Vec<RecordBatch> {
        let provider = PostgresTable::try_new(uri, "public", "my_pg_table", test_schema())
            .await
            .expect("connect to PostgreSQL");
        let ctx = SessionContext::new();
        ctx.register_table("pg_table", Arc::new(provider))
            .expect("register pg_table");
        ctx.sql(sql)
            .await
            .expect("plan sql")
            .collect()
            .await
            .expect("collect results")
    }

    /// Flatten an `Int64` column across all batches into nullable values.
    fn collect_i64(batches: &[RecordBatch], col_idx: usize) -> Vec<Option<i64>> {
        let mut out = Vec::new();
        for batch in batches {
            let array = batch
                .column(col_idx)
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("Int64 column");
            for i in 0..array.len() {
                if array.is_null(i) {
                    out.push(None);
                } else {
                    out.push(Some(array.value(i)));
                }
            }
        }
        out
    }

    /// Flatten a `Utf8` column across all batches into nullable owned strings.
    fn collect_str(batches: &[RecordBatch], col_idx: usize) -> Vec<Option<String>> {
        let mut out = Vec::new();
        for batch in batches {
            let array = batch
                .column(col_idx)
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("Utf8 column");
            for i in 0..array.len() {
                if array.is_null(i) {
                    out.push(None);
                } else {
                    out.push(Some(array.value(i).to_string()));
                }
            }
        }
        out
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn integration_filter_user_id_eq() {
        let uri = pg_uri_or_skip!("integration_filter_user_id_eq");
        // `user_id = 42` is pushed down as an exact SQL predicate.
        let batches =
            scan_via_datafusion(&uri, "SELECT extra_info FROM pg_table WHERE user_id = 42").await;
        assert_eq!(
            collect_str(&batches, 0),
            vec![Some("answer to everything".to_string())]
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn integration_filter_is_null() {
        let uri = pg_uri_or_skip!("integration_filter_is_null");
        // `extra_info IS NULL` is pushed down; only user_id 100 has a NULL.
        let batches = scan_via_datafusion(
            &uri,
            "SELECT user_id FROM pg_table WHERE extra_info IS NULL",
        )
        .await;
        assert_eq!(collect_i64(&batches, 0), vec![Some(100)]);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn integration_text_ordering_refiltered_above_scan() {
        let uri = pg_uri_or_skip!("integration_text_ordering_refiltered_above_scan");
        // `extra_info > 'a'` is NOT pushed down (string ordering is unsupported),
        // so DataFusion re-applies it above the scan. Results must still be
        // correct: 'answer to everything' and 'lucky number' match, NULL does
        // not, so user_ids 7 and 42 remain (100 is excluded).
        let batches = scan_via_datafusion(
            &uri,
            "SELECT user_id FROM pg_table WHERE extra_info > 'a' ORDER BY user_id",
        )
        .await;
        assert_eq!(collect_i64(&batches, 0), vec![Some(7), Some(42)]);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn integration_count_with_filter() {
        let uri = pg_uri_or_skip!("integration_count_with_filter");
        // `user_id > 7` matches 42 and 100 -> COUNT(*) = 2.
        let batches =
            scan_via_datafusion(&uri, "SELECT COUNT(*) FROM pg_table WHERE user_id > 7").await;
        assert_eq!(collect_i64(&batches, 0), vec![Some(2)]);
    }
}
