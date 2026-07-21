// src/catalog.rs
//! Dynamic catalog: PostgreSQL schema introspection.
//!
//! Rather than hardcoding table schemas, Igloo asks PostgreSQL's
//! `information_schema` which base tables exist in a configured set of
//! schemas and what their columns are, mapping each PostgreSQL column type
//! to an Arrow [`DataType`]. The result feeds
//! [`crate::datafusion_engine::DataFusionEngine`], which registers one
//! DataFusion table per discovered PostgreSQL table.
//!
//! Type mapping is a pure function ([`pg_type_to_arrow`]) so it can be unit
//! tested exhaustively. Columns whose type has no Arrow mapping are dropped
//! from the registered schema with a per-column warning; a table with no
//! mappable columns at all is skipped entirely (also with a warning).

use arrow::datatypes::{DataType, Field, TimeUnit};
use tokio_postgres::Client;

use crate::errors::Result;

/// The introspected shape of one PostgreSQL base table: where it lives and
/// the subset of its columns that map to Arrow types.
#[derive(Debug, Clone, PartialEq)]
pub struct TableSchema {
    /// PostgreSQL schema (namespace) the table lives in, e.g. `public`.
    pub schema: String,
    /// Bare table name.
    pub name: String,
    /// The Arrow fields for the columns Igloo can read, in ordinal order.
    /// Columns with unsupported types are absent (see module docs).
    pub fields: Vec<Field>,
}

/// One column as reported by `information_schema.columns`.
#[derive(Debug, Clone)]
struct ColumnInfo {
    schema: String,
    table: String,
    column: String,
    data_type: String,
    udt_name: String,
    is_nullable: bool,
}

/// Maps a PostgreSQL type (as reported by `information_schema.columns`) to
/// an Arrow [`DataType`], returning `None` for types Igloo cannot yet read.
///
/// `data_type` is the SQL standard name (e.g. `double precision`,
/// `timestamp without time zone`); `udt_name` is the underlying PostgreSQL
/// type name (e.g. `float8`, `timestamp`) and is used as a fallback for
/// spellings that vary by server version. The match is case-insensitive.
pub fn pg_type_to_arrow(data_type: &str, udt_name: &str) -> Option<DataType> {
    let dt = data_type.to_ascii_lowercase();
    let udt = udt_name.to_ascii_lowercase();
    match dt.as_str() {
        "smallint" => Some(DataType::Int16),
        "integer" => Some(DataType::Int32),
        "bigint" => Some(DataType::Int64),
        "real" => Some(DataType::Float32),
        "double precision" => Some(DataType::Float64),
        "text" | "character varying" | "character" => Some(DataType::Utf8),
        "boolean" => Some(DataType::Boolean),
        "bytea" => Some(DataType::Binary),
        "date" => Some(DataType::Date32),
        "timestamp without time zone" => Some(DataType::Timestamp(TimeUnit::Nanosecond, None)),
        _ => match udt.as_str() {
            // Fall back to the underlying type name for the common cases,
            // covering any server-specific `data_type` spelling drift.
            "int2" => Some(DataType::Int16),
            "int4" => Some(DataType::Int32),
            "int8" => Some(DataType::Int64),
            "float4" => Some(DataType::Float32),
            "float8" => Some(DataType::Float64),
            "text" | "varchar" | "bpchar" => Some(DataType::Utf8),
            "bool" => Some(DataType::Boolean),
            "bytea" => Some(DataType::Binary),
            "date" => Some(DataType::Date32),
            "timestamp" => Some(DataType::Timestamp(TimeUnit::Nanosecond, None)),
            _ => None,
        },
    }
}

/// The introspection query. Joins `information_schema.columns` to
/// `information_schema.tables` so views are excluded (`BASE TABLE` only),
/// restricted to the requested schemas, ordered so columns arrive in
/// ordinal position within each table.
const INTROSPECT_SQL: &str = "\
SELECT c.table_schema, c.table_name, c.column_name, \
       c.data_type, c.udt_name, c.is_nullable \
FROM information_schema.columns c \
JOIN information_schema.tables t \
  ON c.table_schema = t.table_schema AND c.table_name = t.table_name \
WHERE t.table_type = 'BASE TABLE' \
  AND c.table_schema = ANY($1) \
ORDER BY c.table_schema, c.table_name, c.ordinal_position";

/// Introspects `information_schema` for base tables in `schemas`, returning
/// one [`TableSchema`] per table that has at least one mappable column.
///
/// Ordering follows `schemas`: tables in an earlier-listed schema come
/// first (and alphabetically by name within a schema), which lets callers
/// give earlier schemas priority for bare-name registration.
///
/// Columns whose PostgreSQL type has no Arrow mapping are dropped with a
/// per-column warning naming the table, column and type. A table left with
/// zero mappable columns is omitted entirely, also with a warning.
pub async fn introspect_tables(client: &Client, schemas: &[String]) -> Result<Vec<TableSchema>> {
    let rows = client.query(INTROSPECT_SQL, &[&schemas]).await?;

    let columns: Vec<ColumnInfo> = rows
        .iter()
        .map(|row| ColumnInfo {
            schema: row.get("table_schema"),
            table: row.get("table_name"),
            column: row.get("column_name"),
            data_type: row.get("data_type"),
            udt_name: row.get("udt_name"),
            is_nullable: row
                .get::<_, String>("is_nullable")
                .eq_ignore_ascii_case("YES"),
        })
        .collect();

    let mut tables = group_columns_into_tables(columns);
    order_by_schema_priority(&mut tables, schemas);
    Ok(tables)
}

/// Groups a flat, table-ordered column list into [`TableSchema`]s, applying
/// the type mapping and the unsupported-column / empty-table rules. Pure so
/// the degradation behaviour can be unit tested without a database.
fn group_columns_into_tables(columns: Vec<ColumnInfo>) -> Vec<TableSchema> {
    let mut tables: Vec<TableSchema> = Vec::new();

    for col in columns {
        match pg_type_to_arrow(&col.data_type, &col.udt_name) {
            Some(dt) => {
                let field = Field::new(&col.column, dt, col.is_nullable);
                match tables
                    .last_mut()
                    .filter(|t| t.schema == col.schema && t.name == col.table)
                {
                    Some(t) => t.fields.push(field),
                    None => tables.push(TableSchema {
                        schema: col.schema.clone(),
                        name: col.table.clone(),
                        fields: vec![field],
                    }),
                }
            }
            None => {
                log::warn!(
                    "skipping unsupported column {}.{}.{} of type {:?} (udt {:?}); \
                     the table will be registered without it",
                    col.schema,
                    col.table,
                    col.column,
                    col.data_type,
                    col.udt_name
                );
                // Ensure a placeholder entry exists so an all-unsupported
                // table can be detected and warned about below.
                if !tables
                    .last()
                    .is_some_and(|t| t.schema == col.schema && t.name == col.table)
                {
                    tables.push(TableSchema {
                        schema: col.schema.clone(),
                        name: col.table.clone(),
                        fields: Vec::new(),
                    });
                }
            }
        }
    }

    tables
        .into_iter()
        .filter(|t| {
            if t.fields.is_empty() {
                log::warn!(
                    "skipping table {}.{}: no columns with a supported Arrow type",
                    t.schema,
                    t.name
                );
                false
            } else {
                true
            }
        })
        .collect()
}

/// Reorders tables so that those in an earlier-listed schema come first,
/// preserving alphabetical table-name order within each schema. Tables in a
/// schema not present in `schemas` (should not happen) sort last.
fn order_by_schema_priority(tables: &mut [TableSchema], schemas: &[String]) {
    let rank = |schema: &str| {
        schemas
            .iter()
            .position(|s| s == schema)
            .unwrap_or(usize::MAX)
    };
    tables.sort_by(|a, b| {
        rank(&a.schema)
            .cmp(&rank(&b.schema))
            .then_with(|| a.name.cmp(&b.name))
    });
}

/// Given introspected tables (already in priority order), decides the
/// DataFusion registration name for each: the bare table name if free,
/// otherwise a `schema__table` qualified name. Returns names paired with
/// their table, in the input order. Pure and deterministic so collision
/// handling can be unit tested.
pub fn resolve_registration_names(tables: &[TableSchema]) -> Vec<(String, &TableSchema)> {
    let mut used: std::collections::HashSet<String> = std::collections::HashSet::new();
    let mut out = Vec::with_capacity(tables.len());
    for t in tables {
        let name = if used.contains(&t.name) {
            let qualified = format!("{}__{}", t.schema, t.name);
            log::warn!(
                "table name {:?} already registered; registering {}.{} as {:?} instead",
                t.name,
                t.schema,
                t.name,
                qualified
            );
            qualified
        } else {
            t.name.clone()
        };
        used.insert(name.clone());
        out.push((name, t));
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn type_mapping_covers_all_supported_types() {
        // (data_type, udt_name, expected)
        let cases: &[(&str, &str, DataType)] = &[
            ("smallint", "int2", DataType::Int16),
            ("integer", "int4", DataType::Int32),
            ("bigint", "int8", DataType::Int64),
            ("real", "float4", DataType::Float32),
            ("double precision", "float8", DataType::Float64),
            ("text", "text", DataType::Utf8),
            ("character varying", "varchar", DataType::Utf8),
            ("character", "bpchar", DataType::Utf8),
            ("boolean", "bool", DataType::Boolean),
            ("bytea", "bytea", DataType::Binary),
            ("date", "date", DataType::Date32),
            (
                "timestamp without time zone",
                "timestamp",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
            ),
        ];
        for (data_type, udt, expected) in cases {
            assert_eq!(
                pg_type_to_arrow(data_type, udt).as_ref(),
                Some(expected),
                "mapping for {:?}/{:?}",
                data_type,
                udt
            );
        }
    }

    #[test]
    fn type_mapping_is_case_insensitive() {
        assert_eq!(pg_type_to_arrow("BIGINT", "INT8"), Some(DataType::Int64));
    }

    #[test]
    fn unsupported_types_map_to_none() {
        assert_eq!(pg_type_to_arrow("uuid", "uuid"), None);
        assert_eq!(pg_type_to_arrow("jsonb", "jsonb"), None);
        assert_eq!(pg_type_to_arrow("numeric", "numeric"), None);
        assert_eq!(
            pg_type_to_arrow("timestamp with time zone", "timestamptz"),
            None
        );
    }

    fn col(schema: &str, table: &str, column: &str, data_type: &str, udt: &str) -> ColumnInfo {
        ColumnInfo {
            schema: schema.into(),
            table: table.into(),
            column: column.into(),
            data_type: data_type.into(),
            udt_name: udt.into(),
            is_nullable: true,
        }
    }

    #[test]
    fn table_with_unsupported_column_keeps_supported_subset() {
        let cols = vec![
            col("public", "t", "id", "bigint", "int8"),
            col("public", "t", "tags", "jsonb", "jsonb"),
            col("public", "t", "name", "text", "text"),
        ];
        let tables = group_columns_into_tables(cols);
        assert_eq!(tables.len(), 1);
        let names: Vec<&str> = tables[0].fields.iter().map(|f| f.name().as_str()).collect();
        assert_eq!(names, vec!["id", "name"], "jsonb column dropped");
    }

    #[test]
    fn table_with_zero_supported_columns_is_skipped() {
        let cols = vec![
            col("public", "only_uuid", "id", "uuid", "uuid"),
            col("public", "keep", "n", "integer", "int4"),
        ];
        let tables = group_columns_into_tables(cols);
        assert_eq!(tables.len(), 1);
        assert_eq!(tables[0].name, "keep");
    }

    #[test]
    fn nullability_is_carried_from_is_nullable() {
        let mut not_null = col("public", "t", "id", "bigint", "int8");
        not_null.is_nullable = false;
        let tables = group_columns_into_tables(vec![not_null]);
        assert!(!tables[0].fields[0].is_nullable());
    }

    fn tbl(schema: &str, name: &str) -> TableSchema {
        TableSchema {
            schema: schema.into(),
            name: name.into(),
            fields: vec![Field::new("id", DataType::Int64, true)],
        }
    }

    #[test]
    fn registration_uses_bare_name_when_free() {
        let tables = vec![tbl("public", "orders"), tbl("public", "users")];
        let resolved = resolve_registration_names(&tables);
        let names: Vec<&str> = resolved.iter().map(|(n, _)| n.as_str()).collect();
        assert_eq!(names, vec!["orders", "users"]);
    }

    #[test]
    fn registration_qualifies_on_collision() {
        // Same bare name in two schemas: first (priority-ordered) keeps the
        // bare name, the second is qualified as schema__table.
        let tables = vec![tbl("public", "widget"), tbl("analytics", "widget")];
        let resolved = resolve_registration_names(&tables);
        let names: Vec<&str> = resolved.iter().map(|(n, _)| n.as_str()).collect();
        assert_eq!(names, vec!["widget", "analytics__widget"]);
    }

    #[test]
    fn order_by_schema_priority_prefers_earlier_schema() {
        let mut tables = vec![
            tbl("analytics", "b"),
            tbl("public", "z"),
            tbl("public", "a"),
        ];
        let schemas = vec!["public".to_string(), "analytics".to_string()];
        order_by_schema_priority(&mut tables, &schemas);
        let ordered: Vec<(&str, &str)> = tables
            .iter()
            .map(|t| (t.schema.as_str(), t.name.as_str()))
            .collect();
        assert_eq!(
            ordered,
            vec![("public", "a"), ("public", "z"), ("analytics", "b")]
        );
    }
}
