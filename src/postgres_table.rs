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
use datafusion::logical_expr::{Expr, TableType};
use datafusion::physical_plan::memory::MemoryExec;
use datafusion::physical_plan::ExecutionPlan;
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
fn build_scan_sql(
    schema_name: &str,
    table_name: &str,
    projected_fields: &Fields,
    limit: Option<usize>,
) -> String {
    let limit_clause = limit.map(|n| format!(" LIMIT {}", n)).unwrap_or_default();
    let relation = quote_relation(schema_name, table_name);

    // A projection can be empty (e.g. `SELECT COUNT(*)`), in which case
    // DataFusion only needs the row count, not any column data.
    if projected_fields.is_empty() {
        format!(
            "SELECT COUNT(*) FROM (SELECT 1 FROM {}{}) AS t",
            relation, limit_clause
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
            "SELECT {} FROM {}{}",
            sql_select_cols, relation, limit_clause
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

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr], // Filters are not pushed down; DataFusion applies them on top.
        limit: Option<usize>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        let projected_schema = match projection {
            Some(p) => Arc::new(self.schema.project(p)?),
            None => self.schema.clone(),
        };

        let query = build_scan_sql(
            &self.schema_name,
            &self.table_name,
            projected_schema.fields(),
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
    use super::{build_scan_sql, quote_ident};
    use arrow::datatypes::{DataType, Field, Fields};

    /// Build an Arrow `Fields` from column names. The concrete data type is
    /// irrelevant to `build_scan_sql`, which only reads field names.
    fn fields_from(names: &[&str]) -> Fields {
        names
            .iter()
            .map(|n| Field::new(*n, DataType::Int64, true))
            .collect()
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
            Some(5),
        );
        assert_eq!(
            sql,
            "SELECT \"user_id\", \"extra_info\" FROM \"public\".\"my_pg_table\" LIMIT 5"
        );
    }

    #[test]
    fn build_scan_sql_empty_projection_with_limit() {
        let sql = build_scan_sql("public", "my_pg_table", &fields_from(&[]), Some(5));
        assert_eq!(
            sql,
            "SELECT COUNT(*) FROM (SELECT 1 FROM \"public\".\"my_pg_table\" LIMIT 5) AS t"
        );
    }

    #[test]
    fn build_scan_sql_empty_projection_no_limit() {
        let sql = build_scan_sql("public", "my_pg_table", &fields_from(&[]), None);
        assert_eq!(
            sql,
            "SELECT COUNT(*) FROM (SELECT 1 FROM \"public\".\"my_pg_table\") AS t"
        );
    }

    #[test]
    fn build_scan_sql_qualifies_non_default_schema() {
        // A table in a non-default schema resolves because the relation is
        // schema-qualified.
        let sql = build_scan_sql("analytics", "events", &fields_from(&["id"]), None);
        assert_eq!(sql, "SELECT \"id\" FROM \"analytics\".\"events\"");
    }

    #[test]
    fn build_scan_sql_escapes_embedded_quotes() {
        // The schema, table and column names each contain a double quote,
        // which `quote_ident` doubles when escaping — independently per part.
        let sql = build_scan_sql("sch\"ema", "we\"ird", &fields_from(&["c\"ol"]), None);
        assert_eq!(sql, "SELECT \"c\"\"ol\" FROM \"sch\"\"ema\".\"we\"\"ird\"");
    }
}
