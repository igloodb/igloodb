//! Connector configuration and machine-readable coverage. Driver availability
//! is deliberately separate from verified Igloo compatibility.
use std::collections::{BTreeMap, HashSet};
use std::path::Path;

use serde::Deserialize;
use serde_json::{json, Value};

use crate::config::Secret;
use crate::errors::{IglooError, Result};

#[derive(Debug, Clone, Copy, Default, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IdentifierDialect {
    #[default]
    Ansi,
    Mysql,
    Mssql,
}

impl IdentifierDialect {
    pub fn quote(self, name: &str) -> Result<String> {
        if name.contains('\0') {
            return Err(IglooError::Config(
                "SQL identifiers must not contain NUL".into(),
            ));
        }
        Ok(match self {
            Self::Ansi => format!("\"{}\"", name.replace('"', "\"\"")),
            Self::Mysql => format!("`{}`", name.replace('`', "``")),
            Self::Mssql => format!("[{}]", name.replace(']', "]]")),
        })
    }
}

/// A trusted administrator-supplied ADBC C-ABI driver, one remote database per
/// Igloo catalog. Options are strings; all option values are redacted in Debug.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AdbcSource {
    pub name: String,
    #[serde(default)]
    pub driver: String,
    pub entrypoint: Option<String>,
    pub uri: Option<Secret>,
    pub catalog: Option<String>,
    pub schemas: Option<Vec<String>>,
    pub tables: Option<Vec<String>>,
    #[serde(default)]
    pub dialect: IdentifierDialect,
    #[serde(default)]
    pub options: BTreeMap<String, Secret>,
    #[serde(default)]
    pub options_env: BTreeMap<String, String>,
    #[serde(default)]
    pub connection_options: BTreeMap<String, Secret>,
    #[serde(default)]
    pub connection_options_env: BTreeMap<String, String>,
}

impl AdbcSource {
    pub(crate) fn resolve(&mut self, env: &impl Fn(&str) -> Option<String>) -> Result<()> {
        crate::config::validate_source_name(&self.name)?;
        let prefix = format!("IGLOO_SOURCE_{}", self.name.to_ascii_uppercase());
        if let Some(driver) = env(&format!("{prefix}_DRIVER")) {
            self.driver = driver;
        }
        if let Some(uri) = env(&format!("{prefix}_URI")) {
            self.uri = Some(Secret::new(uri));
        }
        for (values, bindings) in [
            (&mut self.options, &self.options_env),
            (&mut self.connection_options, &self.connection_options_env),
        ] {
            for (key, variable) in bindings {
                if variable.is_empty() {
                    return Err(config_error(
                        &self.name,
                        "option environment names must not be empty",
                    ));
                }
                let value = env(variable).ok_or_else(|| {
                    config_error(
                        &self.name,
                        "a configured option environment variable is missing",
                    )
                })?;
                values.insert(key.clone(), Secret::new(value));
            }
        }
        if !Path::new(&self.driver).is_absolute() || self.driver.contains('\0') {
            return Err(config_error(&self.name, "driver must be an absolute path to a trusted ADBC shared library (or set IGLOO_SOURCE_<NAME>_DRIVER)"));
        }
        if self
            .entrypoint
            .as_ref()
            .is_some_and(|name| name.is_empty() || name.contains('\0'))
        {
            return Err(config_error(
                &self.name,
                "entrypoint must be a non-empty C symbol without NUL",
            ));
        }
        for list in [&self.schemas, &self.tables].into_iter().flatten() {
            if list.is_empty()
                || list
                    .iter()
                    .any(|name| name.trim().is_empty() || name.contains('\0'))
            {
                return Err(config_error(&self.name, "schema/table allowlists must be non-empty and contain non-empty names without NUL"));
            }
        }
        if self
            .options
            .keys()
            .chain(self.connection_options.keys())
            .any(|key| key.is_empty() || key.contains('\0'))
        {
            return Err(config_error(
                &self.name,
                "option keys must be non-empty and contain no NUL",
            ));
        }
        if self.options.contains_key("uri") {
            return Err(config_error(
                &self.name,
                "use uri or IGLOO_SOURCE_<NAME>_URI rather than an options.uri entry",
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ParquetSource {
    pub name: String,
    pub tables: Vec<ParquetTable>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ParquetTable {
    pub name: String,
    pub path: String,
}

impl ParquetSource {
    pub(crate) fn validate(&self) -> Result<()> {
        crate::config::validate_source_name(&self.name)?;
        let mut names = HashSet::new();
        if self.tables.is_empty() {
            return Err(config_error(&self.name, "tables must not be empty"));
        }
        for table in &self.tables {
            if table.name.is_empty() || table.name.contains('\0') || !names.insert(&table.name) {
                return Err(config_error(
                    &self.name,
                    "table names must be non-empty, unique and contain no NUL",
                ));
            }
            if table.path.trim().is_empty()
                || table.path.contains('\0')
                || table.path.contains("://")
            {
                return Err(config_error(
                    &self.name,
                    "Parquet paths must be local file/directory paths",
                ));
            }
        }
        Ok(())
    }
}

fn config_error(name: &str, reason: &str) -> IglooError {
    IglooError::Config(format!("source {name:?}: {reason}"))
}

pub fn inventory() -> Value {
    json!({"ok":true,"coverage_policy":"Only tested integrations are called verified; installing a driver is not certification.",
        "routes":[
            {"id":"postgres","status":"tested","configuration":"sources","mode":"live_query","discovery":true,"cdc":false},
            {"id":"adbc","status":"experimental","configuration":"adbc_sources","mode":"live_query","discovery":"ADBC GetObjects + GetTableSchema","tested_drivers":["postgresql 1.7.0","sqlite 1.7.0"],"cdc":false},
            {"id":"parquet","status":"tested","configuration":"parquet_sources","mode":"snapshot_query","schema":"inferred","cdc":false},
            {"id":"iceberg","status":"planned","mode":"snapshot_query","requires":["catalog","snapshots","delete files","schema evolution"]},
            {"id":"cdc_replication","status":"planned","requires":["source connector","checkpointing","deletes","schema changes","recovery"]}
        ],
        "adbc_candidates":{"compatibility":"unverified in Igloo; requires a compatible installed driver and per-database validation","databases":["MySQL","SQL Server","Oracle","Snowflake","BigQuery","DuckDB","Databricks","ClickHouse","Redshift","Trino","Presto","Cassandra","Teradata","SingleStore","Flight SQL services"]},
        "other_database_routes":{"MongoDB":"planned native adapter or replication into a correctly read lakehouse table","databases_without_adbc":"planned JDBC/ODBC bridge, native adapter or replication"},
        "limitations":["ADBC is not a CDC protocol","Drivers are trusted native code","Remote driver options, TLS and privileges are deployment-specific","No cross-source snapshot consistency","A Parquet directory is not an Iceberg table"]})
}
