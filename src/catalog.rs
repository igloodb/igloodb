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

/// Arrow field metadata key carrying the source PostgreSQL type for columns
/// whose Arrow representation is ambiguous — several PG types map to the
/// same [`DataType`] (e.g. `uuid`/`json`/`jsonb` all become `Utf8`). The
/// scan decoder reads this to pick the right wire-format decoding.
pub const PG_TYPE_META_KEY: &str = "igloo.pg_type";

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
///
/// Types whose Arrow representation is ambiguous (`uuid`, `json`, `jsonb`
/// all map to [`DataType::Utf8`]) must also be recorded in the field's
/// metadata under [`PG_TYPE_META_KEY`] — see
/// [`ambiguous_pg_type`]. `timestamptz` maps to a UTC-typed
/// timestamp so it stays distinguishable from naive timestamps.
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
        // Read as an instant and normalized to UTC; the tz-aware Arrow type
        // keeps these distinct from naive timestamps (which decode with a
        // different PostgreSQL wire representation).
        "timestamp with time zone" => Some(DataType::Timestamp(
            TimeUnit::Nanosecond,
            Some("+00:00".into()),
        )),
        "json" | "jsonb" | "uuid" => Some(DataType::Utf8),
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
            "timestamptz" => Some(DataType::Timestamp(
                TimeUnit::Nanosecond,
                Some("+00:00".into()),
            )),
            "json" | "jsonb" | "uuid" => Some(DataType::Utf8),
            _ => None,
        },
    }
}

/// The [`PG_TYPE_META_KEY`] metadata value for a column, populated only
/// where the Arrow type alone cannot tell the scan decoder which PostgreSQL
/// wire representation to read (`uuid`/`json`/`jsonb` all look like Utf8).
pub fn ambiguous_pg_type(data_type: &str, udt_name: &str) -> Option<String> {
    let dt = data_type.to_ascii_lowercase();
    let udt = udt_name.to_ascii_lowercase();
    if matches!(dt.as_str(), "uuid" | "json" | "jsonb") {
        return Some(dt);
    }
    if matches!(udt.as_str(), "uuid" | "json" | "jsonb") {
        return Some(udt);
    }
    None
}

/// The introspection query. Joins `information_schema.columns` to
/// `information_schema.tables` so views are excluded (`BASE TABLE` only),
/// restricted to the requested schemas and (optionally) an allowlist of
/// table names, ordered so columns arrive in ordinal position within each
/// table.
///
/// `$1` is the schema list; `$2` is the table allowlist or SQL `NULL` for
/// "every table". Both are bound as parameters — never interpolated — so a
/// hostile schema or table name cannot alter the statement.
const INTROSPECT_SQL: &str = "\
SELECT c.table_schema, c.table_name, c.column_name, \
       c.data_type, c.udt_name, c.is_nullable \
FROM information_schema.columns c \
JOIN information_schema.tables t \
  ON c.table_schema = t.table_schema AND c.table_name = t.table_name \
WHERE t.table_type = 'BASE TABLE' \
  AND c.table_schema = ANY($1) \
  AND ($2::text[] IS NULL OR c.table_name::text = ANY($2::text[])) \
ORDER BY c.table_schema, c.table_name, c.ordinal_position";

/// Introspects `information_schema` for base tables in `schemas`, returning
/// one [`TableSchema`] per table that has at least one mappable column.
///
/// `tables`, when given, is an allowlist: only tables whose name appears in
/// it are introspected. Names in the allowlist that match nothing are
/// reported with a warning, since a typo there silently hides data.
///
/// Ordering follows `schemas`: tables in an earlier-listed schema come
/// first (and alphabetically by name within a schema), which lets callers
/// give earlier schemas priority for bare-name registration.
///
/// Columns whose PostgreSQL type has no Arrow mapping are dropped with a
/// per-column warning naming the table, column and type. A table left with
/// zero mappable columns is omitted entirely, also with a warning.
pub async fn introspect_tables(
    client: &Client,
    schemas: &[String],
    tables: Option<&[String]>,
) -> Result<Vec<TableSchema>> {
    // `Option<&[String]>` binds as text[] or SQL NULL, which the query reads
    // as "no allowlist".
    let rows = client.query(INTROSPECT_SQL, &[&schemas, &tables]).await?;

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

    let mut discovered = group_columns_into_tables(columns);
    order_by_schema_priority(&mut discovered, schemas);
    if let Some(allowlist) = tables {
        warn_unmatched_allowlist(allowlist, &discovered, schemas);
    }
    Ok(discovered)
}

/// Warns about allowlisted table names that no schema actually provided, so
/// a typo in `tables = [...]` surfaces instead of silently hiding data.
fn warn_unmatched_allowlist(allowlist: &[String], found: &[TableSchema], schemas: &[String]) {
    for wanted in allowlist {
        if !found.iter().any(|t| &t.name == wanted) {
            log::warn!(
                "configured table {:?} was not found as a base table in schemas {:?}; \
                 it will not be registered",
                wanted,
                schemas
            );
        }
    }
}

/// Groups a flat, table-ordered column list into [`TableSchema`]s, applying
/// the type mapping and the unsupported-column / empty-table rules. Pure so
/// the degradation behaviour can be unit tested without a database.
fn group_columns_into_tables(columns: Vec<ColumnInfo>) -> Vec<TableSchema> {
    let mut tables: Vec<TableSchema> = Vec::new();

    for col in columns {
        match pg_type_to_arrow(&col.data_type, &col.udt_name) {
            Some(dt) => {
                let mut field = Field::new(&col.column, dt, col.is_nullable);
                // Stamp the source type where the Arrow type is ambiguous,
                // so the scan decoder reads the right wire format.
                if let Some(kind) = ambiguous_pg_type(&col.data_type, &col.udt_name) {
                    field = field.with_metadata(
                        [(PG_TYPE_META_KEY.to_string(), kind)].into_iter().collect(),
                    );
                }
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

/// The tables discovered in one configured source.
#[derive(Debug, Clone, PartialEq)]
pub struct SourceTables {
    /// The source's catalog name (see [`crate::config::PostgresSource`]).
    pub source: String,
    /// Its introspected tables, in registration-priority order.
    pub tables: Vec<TableSchema>,
}

/// A convenience alias in the default catalog: `alias` resolves to
/// `source.schema.table`.
#[derive(Debug, Clone, PartialEq)]
pub struct Alias<'a> {
    /// The unqualified name to register in the default catalog.
    pub alias: String,
    /// The source (catalog) the aliased table belongs to.
    pub source: &'a str,
    /// The aliased table.
    pub table: &'a TableSchema,
}

/// Decides the unqualified alias each discovered table gets in the default
/// catalog, so short names like `orders` keep working alongside the always-
/// available canonical name `<source>.<schema>.<table>`.
///
/// Candidates are tried in order and the first free one wins:
///
/// 1. `table` — the bare name,
/// 2. `schema__table` — when another source or schema already took the bare
///    name,
/// 3. `source__schema__table` — when that is taken too.
///
/// `reserved` names are treated as already taken, which is how names the
/// engine registers itself (`iceberg`, the legacy `pg_table` alias) stay
/// unshadowable by an upstream table that happens to share them.
///
/// Sources are processed in configured order, so the primary (first) source
/// keeps the bare names. A table whose every candidate is taken gets no
/// alias (with a warning); it is still queryable by its canonical
/// three-part name. Pure and deterministic so collision handling can be unit
/// tested.
pub fn resolve_aliases<'a>(sources: &'a [SourceTables], reserved: &[&str]) -> Vec<Alias<'a>> {
    let mut used: std::collections::HashSet<String> =
        reserved.iter().map(|r| r.to_string()).collect();
    let mut out = Vec::new();
    for source in sources {
        for table in &source.tables {
            let candidates = [
                table.name.clone(),
                format!("{}__{}", table.schema, table.name),
                format!("{}__{}__{}", source.source, table.schema, table.name),
            ];
            match candidates.iter().find(|c| !used.contains(*c)) {
                Some(alias) => {
                    if alias != &table.name {
                        log::warn!(
                            "unqualified name {:?} is already registered; aliasing \
                             {}.{}.{} as {:?} instead (its {}.{}.{} name always works)",
                            table.name,
                            source.source,
                            table.schema,
                            table.name,
                            alias,
                            source.source,
                            table.schema,
                            table.name
                        );
                    }
                    used.insert(alias.clone());
                    out.push(Alias {
                        alias: alias.clone(),
                        source: &source.source,
                        table,
                    });
                }
                None => log::warn!(
                    "no unqualified alias left for {}.{}.{}; query it as {}.{}.{}",
                    source.source,
                    table.schema,
                    table.name,
                    source.source,
                    table.schema,
                    table.name
                ),
            }
        }
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
            // timestamptz is a UTC-typed timestamp: same precision, distinct
            // Arrow type so decoding knows which wire representation to read.
            (
                "timestamp with time zone",
                "timestamptz",
                DataType::Timestamp(TimeUnit::Nanosecond, Some("+00:00".into())),
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
    fn text_shaped_types_map_to_utf8() {
        // json/jsonb/uuid all surface as Utf8; their source kind is carried
        // in field metadata (see ambiguous_pg_type) so scans decode right.
        for pg in ["json", "jsonb", "uuid"] {
            assert_eq!(pg_type_to_arrow(pg, pg), Some(DataType::Utf8), "{pg}");
        }
    }

    #[test]
    fn ambiguous_types_are_stamped_into_metadata() {
        for pg in ["uuid", "json", "jsonb"] {
            let kind = ambiguous_pg_type(pg, pg).expect("stamped");
            assert_eq!(kind, pg);
        }
        // Spelling-drift fallback via udt_name.
        assert_eq!(
            ambiguous_pg_type("weird spelling", "jsonb").as_deref(),
            Some("jsonb")
        );
        // Unambiguous types carry no metadata.
        assert_eq!(ambiguous_pg_type("text", "text"), None);
        assert_eq!(
            ambiguous_pg_type("timestamp without time zone", "timestamp"),
            None
        );
        assert_eq!(ambiguous_pg_type("integer", "int4"), None);
    }

    #[test]
    fn type_mapping_is_case_insensitive() {
        assert_eq!(pg_type_to_arrow("BIGINT", "INT8"), Some(DataType::Int64));
        assert_eq!(pg_type_to_arrow("UUID", "UUID"), Some(DataType::Utf8));
    }

    #[test]
    fn unsupported_types_map_to_none() {
        assert_eq!(pg_type_to_arrow("numeric", "numeric"), None);
        assert_eq!(pg_type_to_arrow("inet", "inet"), None);
        assert_eq!(pg_type_to_arrow("ARRAY", "_text"), None);
        assert_eq!(pg_type_to_arrow("time without time zone", "time"), None);
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
        // text[] (udt _text) has no mapping; id and name do.
        let cols = vec![
            col("public", "t", "id", "bigint", "int8"),
            col("public", "t", "tags", "ARRAY", "_text"),
            col("public", "t", "name", "text", "text"),
        ];
        let tables = group_columns_into_tables(cols);
        assert_eq!(tables.len(), 1);
        let names: Vec<&str> = tables[0].fields.iter().map(|f| f.name().as_str()).collect();
        assert_eq!(names, vec!["id", "name"], "array column dropped");
    }

    #[test]
    fn ambiguous_columns_carry_source_type_metadata() {
        let cols = vec![
            col("public", "t", "id", "uuid", "uuid"),
            col("public", "t", "doc", "jsonb", "jsonb"),
            col("public", "t", "label", "text", "text"),
        ];
        let tables = group_columns_into_tables(cols);
        assert_eq!(tables.len(), 1);
        let f = |name: &str| {
            tables[0]
                .fields
                .iter()
                .find(|f| f.name() == name)
                .unwrap()
                .clone()
        };
        assert_eq!(
            f("id").metadata().get(PG_TYPE_META_KEY).map(String::as_str),
            Some("uuid")
        );
        assert_eq!(
            f("doc")
                .metadata()
                .get(PG_TYPE_META_KEY)
                .map(String::as_str),
            Some("jsonb")
        );
        assert!(
            !f("label").metadata().contains_key(PG_TYPE_META_KEY),
            "plain text needs no source-type stamp"
        );
    }

    #[test]
    fn table_with_zero_supported_columns_is_skipped() {
        let cols = vec![
            col("public", "only_numeric", "id", "numeric", "numeric"),
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

    fn src(source: &str, tables: Vec<TableSchema>) -> SourceTables {
        SourceTables {
            source: source.into(),
            tables,
        }
    }

    /// The resolved alias names, in resolution order, with nothing reserved.
    fn alias_names(sources: &[SourceTables]) -> Vec<String> {
        resolve_aliases(sources, &[])
            .into_iter()
            .map(|a| a.alias)
            .collect()
    }

    #[test]
    fn registration_uses_bare_name_when_free() {
        let sources = vec![src(
            "pg",
            vec![tbl("public", "orders"), tbl("public", "users")],
        )];
        assert_eq!(alias_names(&sources), vec!["orders", "users"]);
    }

    #[test]
    fn registration_qualifies_on_collision() {
        // Same bare name in two schemas of one source: the first
        // (priority-ordered) keeps the bare name, the second becomes
        // schema__table.
        let sources = vec![src(
            "pg",
            vec![tbl("public", "widget"), tbl("analytics", "widget")],
        )];
        assert_eq!(alias_names(&sources), vec!["widget", "analytics__widget"]);
    }

    #[test]
    fn primary_source_keeps_bare_names_across_sources() {
        // Both sources expose public.widget: the first source configured wins
        // the bare alias, the second falls back to schema__table.
        let sources = vec![
            src("alpha", vec![tbl("public", "widget")]),
            src("beta", vec![tbl("public", "widget")]),
        ];
        let resolved = resolve_aliases(&sources, &[]);
        assert_eq!(
            resolved
                .iter()
                .map(|a| (a.alias.as_str(), a.source))
                .collect::<Vec<_>>(),
            vec![("widget", "alpha"), ("public__widget", "beta")]
        );
    }

    #[test]
    fn third_candidate_includes_the_source_name() {
        // widget, public__widget and then gamma__public__widget: the source
        // name disambiguates once both shorter candidates are taken.
        let sources = vec![
            src("alpha", vec![tbl("public", "widget")]),
            src("beta", vec![tbl("public", "widget")]),
            src("gamma", vec![tbl("public", "widget")]),
        ];
        assert_eq!(
            alias_names(&sources),
            vec!["widget", "public__widget", "gamma__public__widget"]
        );
    }

    #[test]
    fn table_without_any_free_alias_is_skipped_not_overwritten() {
        // Exhausting all three candidates takes contrived naming: `widget` and
        // `public__widget` are taken by the first two sources, and a table
        // literally named `s__public__widget` occupies the third candidate of
        // source `s`. That table must be skipped rather than shadow an
        // existing alias — it stays reachable as s.public.widget.
        let sources = vec![
            src("alpha", vec![tbl("public", "widget")]),
            src("beta", vec![tbl("public", "widget")]),
            src("x", vec![tbl("public", "s__public__widget")]),
            src("s", vec![tbl("public", "widget")]),
        ];
        let resolved = resolve_aliases(&sources, &[]);
        assert_eq!(
            alias_names(&sources),
            vec!["widget", "public__widget", "s__public__widget"],
            "the fourth table gets no alias"
        );
        let sources_with_alias: Vec<&str> = resolved.iter().map(|a| a.source).collect();
        assert_eq!(sources_with_alias, vec!["alpha", "beta", "x"]);
    }

    #[test]
    fn aliases_point_at_their_own_source_and_table() {
        let sources = vec![
            src("alpha", vec![tbl("public", "a")]),
            src("beta", vec![tbl("sales", "b")]),
        ];
        let resolved = resolve_aliases(&sources, &[]);
        assert_eq!(resolved[0].source, "alpha");
        assert_eq!(resolved[0].table.schema, "public");
        assert_eq!(resolved[1].source, "beta");
        assert_eq!(resolved[1].table.name, "b");
    }

    #[test]
    fn reserved_names_are_never_shadowed() {
        // The engine registers `iceberg` itself; an upstream table of that
        // name must fall back to a qualified alias rather than clash.
        let sources = vec![src("pg", vec![tbl("public", "iceberg")])];
        let resolved = resolve_aliases(&sources, &["iceberg", "pg_table"]);
        assert_eq!(resolved.len(), 1);
        assert_eq!(resolved[0].alias, "public__iceberg");
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
