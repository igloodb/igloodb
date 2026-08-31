// src/config.rs
//! Typed, fail-fast configuration.
//!
//! Values are read from an optional TOML file (`IGLOO_CONFIG`, falling back
//! to `./igloo.toml` when present) with environment variables taking
//! precedence. Missing required values abort startup with an error naming
//! the value and how to set it — there are no silent localhost defaults.

use std::fmt;
use std::path::Path;

use serde::Deserialize;

use crate::errors::{IglooError, Result};

/// A configuration value that must never appear in logs or debug output
/// (connection strings can embed credentials).
#[derive(Clone, Deserialize)]
#[serde(transparent)]
pub struct Secret(String);

impl Secret {
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    /// Returns the underlying value. Call sites should pass it straight to
    /// the consumer (driver, client) and never format it into messages.
    pub fn expose(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for Secret {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Secret(***)")
    }
}

impl fmt::Display for Secret {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("***")
    }
}

/// Shape of the optional `igloo.toml` file. Unknown keys are rejected so a
/// typo fails loudly instead of being ignored.
#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct FileConfig {
    parquet_path: Option<String>,
    cdc_path: Option<String>,
    postgres_uri: Option<String>,
    postgres_schemas: Option<Vec<String>>,
    /// Multi-source form: one `[[sources]]` entry per PostgreSQL database.
    /// Mutually exclusive with the `postgres_uri`/`postgres_schemas` pair.
    sources: Option<Vec<FileSource>>,
    listen_addr: Option<String>,
    cache_max_entries: Option<u64>,
    cache_ttl_seconds: Option<u64>,
    cdc_poll_seconds: Option<u64>,
}

/// One `[[sources]]` entry in the config file.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct FileSource {
    name: String,
    /// Connection string. Optional in the file so credentials can live only
    /// in `IGLOO_SOURCE_<NAME>_URI`.
    uri: Option<String>,
    schemas: Option<Vec<String>>,
    tables: Option<Vec<String>>,
}

/// Default cache capacity when not configured.
const DEFAULT_CACHE_MAX_ENTRIES: u64 = 1024;
/// Default cache entry TTL when not configured.
const DEFAULT_CACHE_TTL_SECONDS: u64 = 300;
/// Default CDC polling interval in serve mode.
const DEFAULT_CDC_POLL_SECONDS: u64 = 10;
/// Default PostgreSQL schema to introspect when none is configured.
const DEFAULT_POSTGRES_SCHEMA: &str = "public";
/// Name given to the source declared through the single-source
/// `postgres_uri`/`postgres_schemas` keys, so its tables are still reachable
/// as `postgres.<schema>.<table>` like any named source.
pub const DEFAULT_SOURCE_NAME: &str = "postgres";
/// Catalog names DataFusion owns; a source may not claim them because doing
/// so would shadow the default catalog or the information schema.
const RESERVED_SOURCE_NAMES: [&str; 2] = ["datafusion", "information_schema"];

/// One PostgreSQL database Igloo federates over.
///
/// Each source becomes a DataFusion catalog named [`Self::name`], so its
/// tables are queryable as `<source>.<schema>.<table>` (see
/// [`crate::datafusion_engine::DataFusionEngine`]).
#[derive(Debug, Clone)]
pub struct PostgresSource {
    /// Catalog name for this source: lowercase so it can be written
    /// unquoted in SQL (DataFusion folds unquoted identifiers to lowercase).
    pub name: String,
    /// Connection string (URI or key-value form).
    pub uri: Secret,
    /// Schemas (namespaces) to introspect; never empty after validation.
    pub schemas: Vec<String>,
    /// Optional allowlist of table names to register. `None` registers
    /// every base table found in [`Self::schemas`]; `Some` restricts
    /// registration to exactly those names.
    pub tables: Option<Vec<String>>,
}

impl PostgresSource {
    /// Builds a source descriptor directly, for callers that construct the
    /// engine without a config file (tests, embedding).
    pub fn new(name: impl Into<String>, uri: impl Into<String>, schemas: Vec<String>) -> Self {
        Self {
            name: name.into(),
            uri: Secret::new(uri),
            schemas,
            tables: None,
        }
    }

    /// Restricts this source to the named tables (see [`Self::tables`]).
    pub fn with_tables(mut self, tables: Vec<String>) -> Self {
        self.tables = Some(tables);
        self
    }
}

/// Fully-resolved application configuration.
#[derive(Debug)]
pub struct Config {
    /// Directory of Parquet files registered as the `iceberg` table.
    pub parquet_path: String,
    /// Directory watched for CDC event files.
    pub cdc_path: String,
    /// PostgreSQL sources to federate over, in configured order. Never
    /// empty after validation; the first is the primary source (it owns the
    /// unqualified table names — see
    /// [`crate::datafusion_engine::DataFusionEngine`]).
    pub postgres_sources: Vec<PostgresSource>,
    /// Address for the pgwire server (`igloo serve`). Optional because the
    /// demo mode doesn't need it; serve mode fails fast when it is absent.
    pub listen_addr: Option<String>,
    /// Maximum cached query results before LRU eviction (default 1024).
    pub cache_max_entries: usize,
    /// How long a cached result stays valid (default 300s).
    pub cache_ttl: std::time::Duration,
    /// How often serve mode polls the CDC location for new events
    /// (default 10s).
    pub cdc_poll_interval: std::time::Duration,
}

impl Config {
    /// Loads configuration from the config file (if any) and the process
    /// environment, environment taking precedence.
    pub fn load() -> Result<Self> {
        let file = Self::load_file()?;
        Self::from_sources(file, |key| std::env::var(key).ok())
    }

    fn load_file() -> Result<FileConfig> {
        let (path, explicit) = match std::env::var("IGLOO_CONFIG") {
            Ok(p) => (p, true),
            Err(_) => ("igloo.toml".to_string(), false),
        };
        if !Path::new(&path).exists() {
            if explicit {
                return Err(IglooError::Config(format!(
                    "config file {:?} (from IGLOO_CONFIG) does not exist",
                    path
                )));
            }
            return Ok(FileConfig::default());
        }
        let raw = std::fs::read_to_string(&path)?;
        toml::from_str(&raw)
            .map_err(|e| IglooError::Config(format!("invalid config file {:?}: {}", path, e)))
    }

    /// Resolves the final configuration from a parsed file and an
    /// environment lookup. Separated from `load` so it is unit-testable
    /// without touching the process environment.
    fn from_sources(file: FileConfig, env: impl Fn(&str) -> Option<String>) -> Result<Self> {
        let FileConfig {
            parquet_path: file_parquet_path,
            cdc_path: file_cdc_path,
            postgres_uri: file_postgres_uri,
            postgres_schemas: file_postgres_schemas,
            sources: file_sources,
            listen_addr: file_listen_addr,
            cache_max_entries: file_cache_max_entries,
            cache_ttl_seconds: file_cache_ttl_seconds,
            cdc_poll_seconds: file_cdc_poll_seconds,
        } = file;

        let parquet_path = env("IGLOO_PARQUET_PATH")
            .or(file_parquet_path)
            .ok_or_else(|| missing("parquet_path", "IGLOO_PARQUET_PATH"))?;
        let cdc_path = env("IGLOO_CDC_PATH")
            .or(file_cdc_path)
            .ok_or_else(|| missing("cdc_path", "IGLOO_CDC_PATH"))?;
        let postgres_sources =
            resolve_postgres_sources(file_sources, file_postgres_uri, file_postgres_schemas, &env)?;
        let listen_addr = env("IGLOO_LISTEN_ADDR").or(file_listen_addr);
        let cache_max_entries = env_u64(&env, "IGLOO_CACHE_MAX_ENTRIES")?
            .or(file_cache_max_entries)
            .unwrap_or(DEFAULT_CACHE_MAX_ENTRIES);
        let cache_ttl_seconds = env_u64(&env, "IGLOO_CACHE_TTL_SECONDS")?
            .or(file_cache_ttl_seconds)
            .unwrap_or(DEFAULT_CACHE_TTL_SECONDS);
        let cdc_poll_seconds = env_u64(&env, "IGLOO_CDC_POLL_SECONDS")?
            .or(file_cdc_poll_seconds)
            .unwrap_or(DEFAULT_CDC_POLL_SECONDS);

        let config = Self {
            parquet_path,
            cdc_path,
            postgres_sources,
            listen_addr,
            cache_max_entries: cache_max_entries as usize,
            cache_ttl: std::time::Duration::from_secs(cache_ttl_seconds),
            cdc_poll_interval: std::time::Duration::from_secs(cdc_poll_seconds),
        };
        config.validate()?;
        Ok(config)
    }

    fn validate(&self) -> Result<()> {
        if self.parquet_path.trim().is_empty() {
            return Err(IglooError::Config("parquet_path must not be empty".into()));
        }
        if self.cdc_path.trim().is_empty() {
            return Err(IglooError::Config("cdc_path must not be empty".into()));
        }
        self.validate_sources()?;
        if let Some(addr) = &self.listen_addr {
            addr.parse::<std::net::SocketAddr>().map_err(|e| {
                IglooError::Config(format!(
                    "listen_addr {:?} is not a valid socket address (host:port): {}",
                    addr, e
                ))
            })?;
        }
        if self.cache_max_entries == 0 {
            return Err(IglooError::Config(
                "cache_max_entries must be positive".into(),
            ));
        }
        if self.cache_ttl.is_zero() {
            return Err(IglooError::Config(
                "cache_ttl_seconds must be positive".into(),
            ));
        }
        if self.cdc_poll_interval.is_zero() {
            return Err(IglooError::Config(
                "cdc_poll_seconds must be positive".into(),
            ));
        }
        Ok(())
    }

    /// Validates every source: usable catalog name, unique across sources,
    /// plausible connection string, and non-empty schema/table lists. Each
    /// message names the offending source so a multi-source file is
    /// debuggable.
    fn validate_sources(&self) -> Result<()> {
        if self.postgres_sources.is_empty() {
            return Err(IglooError::Config(
                "at least one PostgreSQL source must be configured \
                 (postgres_uri, or a [[sources]] entry)"
                    .into(),
            ));
        }
        let mut seen: std::collections::HashSet<&str> = std::collections::HashSet::new();
        for source in &self.postgres_sources {
            validate_source_name(&source.name)?;
            if !seen.insert(source.name.as_str()) {
                return Err(IglooError::Config(format!(
                    "duplicate source name {:?}: source names must be unique",
                    source.name
                )));
            }
            let uri = source.uri.expose();
            let looks_like_uri = uri.starts_with("postgres://") || uri.starts_with("postgresql://");
            let looks_like_kv = uri.contains('=');
            if !(looks_like_uri || looks_like_kv) {
                return Err(IglooError::Config(format!(
                    "uri for source {:?} must be a postgres:// URI or key-value \
                     connection string",
                    source.name
                )));
            }
            if source.schemas.is_empty() {
                return Err(IglooError::Config(format!(
                    "schemas for source {:?} must list at least one schema",
                    source.name
                )));
            }
            if source.schemas.iter().any(|s| s.trim().is_empty()) {
                return Err(IglooError::Config(format!(
                    "schemas for source {:?} must not contain empty schema names",
                    source.name
                )));
            }
            match &source.tables {
                Some(tables) if tables.is_empty() => {
                    return Err(IglooError::Config(format!(
                        "tables for source {:?} is an empty allowlist, which would register \
                         nothing; omit the key to register every table",
                        source.name
                    )));
                }
                Some(tables) if tables.iter().any(|t| t.trim().is_empty()) => {
                    return Err(IglooError::Config(format!(
                        "tables for source {:?} must not contain empty table names",
                        source.name
                    )));
                }
                _ => {}
            }
        }
        Ok(())
    }

    /// The primary source: the first configured one. It owns the unqualified
    /// table names in the default catalog, and single-source callers (the
    /// demo binary, the ADBC example) use it.
    pub fn primary_source(&self) -> &PostgresSource {
        // Validation guarantees a non-empty list.
        &self.postgres_sources[0]
    }

    /// The listen address, required in serve mode.
    pub fn require_listen_addr(&self) -> Result<&str> {
        self.listen_addr.as_deref().ok_or_else(|| {
            IglooError::Config(
                "missing required configuration for serve mode: listen_addr \
                 (set IGLOO_LISTEN_ADDR, or listen_addr in igloo.toml)"
                    .into(),
            )
        })
    }
}

/// Resolves the configured PostgreSQL sources from the two mutually
/// exclusive file forms plus environment overrides.
///
/// * `[[sources]]` present → one source per entry, each URI/schema list
///   overridable by `IGLOO_SOURCE_<NAME>_URI` / `IGLOO_SOURCE_<NAME>_SCHEMAS`
///   so credentials need never be written to a file.
/// * otherwise → the single-source `postgres_uri`/`postgres_schemas` keys
///   (with the historical `DATABASE_URL` / `IGLOO_POSTGRES_URI` /
///   `IGLOO_POSTGRES_SCHEMAS` overrides), registered under the name
///   [`DEFAULT_SOURCE_NAME`].
///
/// Mixing the two forms *in the file* is an error rather than a guess. Flat
/// environment variables alongside `[[sources]]` are ignored with a warning:
/// erroring there would break every shell that happens to export
/// `DATABASE_URL`.
fn resolve_postgres_sources(
    file_sources: Option<Vec<FileSource>>,
    file_postgres_uri: Option<String>,
    file_postgres_schemas: Option<Vec<String>>,
    env: &impl Fn(&str) -> Option<String>,
) -> Result<Vec<PostgresSource>> {
    let env_uri = env("DATABASE_URL").or_else(|| env("IGLOO_POSTGRES_URI"));
    let env_schemas = env("IGLOO_POSTGRES_SCHEMAS").map(|raw| split_csv(&raw));

    let Some(entries) = file_sources else {
        let uri = env_uri
            .or(file_postgres_uri)
            .ok_or_else(|| missing("postgres_uri", "IGLOO_POSTGRES_URI or DATABASE_URL"))?;
        let schemas = env_schemas
            .or(file_postgres_schemas)
            .unwrap_or_else(|| vec![DEFAULT_POSTGRES_SCHEMA.to_string()]);
        return Ok(vec![PostgresSource {
            name: DEFAULT_SOURCE_NAME.to_string(),
            uri: Secret::new(uri),
            schemas,
            tables: None,
        }]);
    };

    if file_postgres_uri.is_some() || file_postgres_schemas.is_some() {
        return Err(IglooError::Config(
            "config file sets both [[sources]] and the single-source keys \
             postgres_uri/postgres_schemas: pick one form (move the \
             single-source settings into a [[sources]] entry)"
                .into(),
        ));
    }
    if env_uri.is_some() || env_schemas.is_some() {
        log::warn!(
            "[[sources]] is configured, so DATABASE_URL / IGLOO_POSTGRES_URI / \
             IGLOO_POSTGRES_SCHEMAS are ignored; override a named source with \
             IGLOO_SOURCE_<NAME>_URI or IGLOO_SOURCE_<NAME>_SCHEMAS instead"
        );
    }
    if entries.is_empty() {
        return Err(IglooError::Config(
            "sources is empty: list at least one [[sources]] entry".into(),
        ));
    }

    entries
        .into_iter()
        .map(|entry| resolve_named_source(entry, env))
        .collect()
}

/// Resolves one `[[sources]]` entry, applying its per-source environment
/// overrides. The name is validated first because it determines the
/// environment keys.
fn resolve_named_source(
    entry: FileSource,
    env: &impl Fn(&str) -> Option<String>,
) -> Result<PostgresSource> {
    let name = entry.name.trim().to_string();
    validate_source_name(&name)?;
    let uri = source_env(env, &name, "URI").or(entry.uri).ok_or_else(|| {
        IglooError::Config(format!(
            "missing required configuration: uri for source {:?} \
             (set {}, or uri in its [[sources]] entry)",
            name,
            source_env_key(&name, "URI")
        ))
    })?;
    let schemas = source_env(env, &name, "SCHEMAS")
        .map(|raw| split_csv(&raw))
        .or(entry.schemas)
        .unwrap_or_else(|| vec![DEFAULT_POSTGRES_SCHEMA.to_string()]);
    Ok(PostgresSource {
        name,
        uri: Secret::new(uri),
        schemas,
        tables: entry.tables,
    })
}

/// The environment variable that overrides `suffix` for a named source, e.g.
/// source `orders_db` → `IGLOO_SOURCE_ORDERS_DB_URI`.
fn source_env_key(name: &str, suffix: &str) -> String {
    format!("IGLOO_SOURCE_{}_{}", name.to_ascii_uppercase(), suffix)
}

fn source_env(env: &impl Fn(&str) -> Option<String>, name: &str, suffix: &str) -> Option<String> {
    env(&source_env_key(name, suffix))
}

/// A source name must be usable as an unquoted SQL catalog name: DataFusion
/// folds unquoted identifiers to lowercase, so an uppercase name would be
/// unreachable as `<source>.<schema>.<table>`.
fn validate_source_name(name: &str) -> Result<()> {
    let invalid = |reason: &str| {
        IglooError::Config(format!(
            "invalid source name {:?}: {} (use lowercase letters, digits and \
             underscores, starting with a letter)",
            name, reason
        ))
    };
    let mut chars = name.chars();
    match chars.next() {
        None => return Err(invalid("it is empty")),
        Some(c) if !c.is_ascii_lowercase() => {
            return Err(invalid("it must start with a lowercase ASCII letter"))
        }
        Some(_) => {}
    }
    if let Some(bad) = chars.find(|c| !(c.is_ascii_lowercase() || c.is_ascii_digit() || *c == '_'))
    {
        return Err(invalid(&format!("{:?} is not allowed", bad)));
    }
    if RESERVED_SOURCE_NAMES.contains(&name) {
        return Err(IglooError::Config(format!(
            "invalid source name {:?}: it is reserved by the query engine",
            name
        )));
    }
    Ok(())
}

/// Splits a comma-separated environment value, trimming entries and dropping
/// empty ones (so " , " collapses to an empty list and fails validation
/// rather than silently defaulting).
fn split_csv(raw: &str) -> Vec<String> {
    raw.split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect()
}

/// Reads an optional non-negative integer from the environment, failing
/// loudly on unparseable values instead of ignoring them.
fn env_u64(env: &impl Fn(&str) -> Option<String>, key: &str) -> Result<Option<u64>> {
    env(key)
        .map(|raw| {
            raw.parse::<u64>().map_err(|_| {
                IglooError::Config(format!(
                    "{} must be a non-negative integer, got {:?}",
                    key, raw
                ))
            })
        })
        .transpose()
}

fn missing(key: &str, how: &str) -> IglooError {
    IglooError::Config(format!(
        "missing required configuration: {} (set {}, or {} in igloo.toml)",
        key, how, key
    ))
}

#[cfg(test)]
mod tests {
    use super::{Config, FileConfig, FileSource, PostgresSource, Secret, DEFAULT_SOURCE_NAME};

    fn no_env(_: &str) -> Option<String> {
        None
    }

    fn full_file() -> FileConfig {
        FileConfig {
            parquet_path: Some("/data/parquet".into()),
            cdc_path: Some("/data/cdc".into()),
            postgres_uri: Some("postgres://u:p@db:5432/mydb".into()),
            postgres_schemas: None,
            sources: None,
            listen_addr: None,
            cache_max_entries: None,
            cache_ttl_seconds: None,
            cdc_poll_seconds: None,
        }
    }

    /// A file that uses the multi-source form: no flat postgres keys.
    fn sources_file(sources: Vec<FileSource>) -> FileConfig {
        FileConfig {
            postgres_uri: None,
            postgres_schemas: None,
            sources: Some(sources),
            ..full_file()
        }
    }

    fn source_entry(name: &str, uri: Option<&str>) -> FileSource {
        FileSource {
            name: name.into(),
            uri: uri.map(str::to_string),
            schemas: None,
            tables: None,
        }
    }

    /// The primary source's connection string, for brevity in assertions.
    fn primary_uri(config: &Config) -> &str {
        config.primary_source().uri.expose()
    }

    #[test]
    fn missing_postgres_uri_names_the_key_and_env_vars() {
        let file = FileConfig {
            postgres_uri: None,
            ..full_file()
        };
        let err = Config::from_sources(file, no_env).unwrap_err().to_string();
        assert!(err.contains("postgres_uri"), "got: {}", err);
        assert!(err.contains("IGLOO_POSTGRES_URI"), "got: {}", err);
        assert!(err.contains("DATABASE_URL"), "got: {}", err);
    }

    #[test]
    fn env_overrides_file() {
        let config = Config::from_sources(full_file(), |key| {
            (key == "IGLOO_PARQUET_PATH").then(|| "/env/parquet".to_string())
        })
        .unwrap();
        assert_eq!(config.parquet_path, "/env/parquet");
        assert_eq!(config.cdc_path, "/data/cdc");
    }

    #[test]
    fn database_url_takes_precedence_over_igloo_postgres_uri() {
        let config = Config::from_sources(full_file(), |key| match key {
            "DATABASE_URL" => Some("postgres://from-database-url/db".to_string()),
            "IGLOO_POSTGRES_URI" => Some("postgres://from-igloo-uri/db".to_string()),
            _ => None,
        })
        .unwrap();
        assert_eq!(primary_uri(&config), "postgres://from-database-url/db");
    }

    #[test]
    fn file_alone_is_sufficient() {
        let config = Config::from_sources(full_file(), no_env).unwrap();
        assert_eq!(config.parquet_path, "/data/parquet");
        assert_eq!(primary_uri(&config), "postgres://u:p@db:5432/mydb");
    }

    #[test]
    fn invalid_postgres_uri_is_rejected() {
        let file = FileConfig {
            postgres_uri: Some("not-a-connection-string".into()),
            ..full_file()
        };
        let err = Config::from_sources(file, no_env).unwrap_err().to_string();
        assert!(err.contains("uri"), "got: {}", err);
        assert!(err.contains(DEFAULT_SOURCE_NAME), "got: {}", err);
    }

    #[test]
    fn listen_addr_is_optional_but_validated() {
        let absent = Config::from_sources(full_file(), no_env).unwrap();
        assert!(absent.listen_addr.is_none());
        let err = absent.require_listen_addr().unwrap_err().to_string();
        assert!(err.contains("IGLOO_LISTEN_ADDR"), "got: {}", err);

        let valid = Config::from_sources(full_file(), |key| {
            (key == "IGLOO_LISTEN_ADDR").then(|| "127.0.0.1:5442".to_string())
        })
        .unwrap();
        assert_eq!(valid.require_listen_addr().unwrap(), "127.0.0.1:5442");

        let invalid = Config::from_sources(full_file(), |key| {
            (key == "IGLOO_LISTEN_ADDR").then(|| "not-an-address".to_string())
        });
        let err = invalid.unwrap_err().to_string();
        assert!(err.contains("listen_addr"), "got: {}", err);
    }

    #[test]
    fn cache_settings_default_and_override() {
        let defaults = Config::from_sources(full_file(), no_env).unwrap();
        assert_eq!(defaults.cache_max_entries, 1024);
        assert_eq!(defaults.cache_ttl.as_secs(), 300);
        assert_eq!(defaults.cdc_poll_interval.as_secs(), 10);

        let file = FileConfig {
            cache_max_entries: Some(8),
            cache_ttl_seconds: Some(30),
            ..full_file()
        };
        let overridden = Config::from_sources(file, |key| {
            (key == "IGLOO_CACHE_MAX_ENTRIES").then(|| "16".to_string())
        })
        .unwrap();
        assert_eq!(overridden.cache_max_entries, 16, "env beats file");
        assert_eq!(overridden.cache_ttl.as_secs(), 30, "file beats default");
    }

    #[test]
    fn invalid_cache_settings_are_rejected() {
        let zero_capacity = FileConfig {
            cache_max_entries: Some(0),
            ..full_file()
        };
        let err = Config::from_sources(zero_capacity, no_env)
            .unwrap_err()
            .to_string();
        assert!(err.contains("cache_max_entries"), "got: {}", err);

        let bad_env = Config::from_sources(full_file(), |key| {
            (key == "IGLOO_CACHE_TTL_SECONDS").then(|| "soon".to_string())
        });
        let err = bad_env.unwrap_err().to_string();
        assert!(err.contains("IGLOO_CACHE_TTL_SECONDS"), "got: {}", err);
    }

    // --- single-source (flat) form ------------------------------------------

    #[test]
    fn flat_keys_become_one_source_named_postgres() {
        let config = Config::from_sources(full_file(), no_env).unwrap();
        assert_eq!(config.postgres_sources.len(), 1);
        let source = config.primary_source();
        assert_eq!(source.name, DEFAULT_SOURCE_NAME);
        assert_eq!(source.schemas, vec!["public".to_string()]);
        assert!(source.tables.is_none(), "no allowlist by default");
    }

    #[test]
    fn postgres_schemas_defaults_to_public() {
        let config = Config::from_sources(full_file(), no_env).unwrap();
        assert_eq!(config.primary_source().schemas, vec!["public".to_string()]);
    }

    #[test]
    fn postgres_schemas_from_file() {
        let file = FileConfig {
            postgres_schemas: Some(vec!["public".into(), "analytics".into()]),
            ..full_file()
        };
        let config = Config::from_sources(file, no_env).unwrap();
        assert_eq!(config.primary_source().schemas, vec!["public", "analytics"]);
    }

    #[test]
    fn postgres_schemas_env_overrides_file_and_splits_csv() {
        let file = FileConfig {
            postgres_schemas: Some(vec!["fromfile".into()]),
            ..full_file()
        };
        let config = Config::from_sources(file, |key| {
            (key == "IGLOO_POSTGRES_SCHEMAS").then(|| " public , analytics ,reporting".to_string())
        })
        .unwrap();
        assert_eq!(
            config.primary_source().schemas,
            vec!["public", "analytics", "reporting"],
            "env wins, is trimmed and split on commas"
        );
    }

    #[test]
    fn empty_postgres_schemas_list_is_rejected() {
        let file = FileConfig {
            postgres_schemas: Some(vec![]),
            ..full_file()
        };
        let err = Config::from_sources(file, no_env).unwrap_err().to_string();
        assert!(err.contains("schemas"), "got: {}", err);

        // An env value of only commas/whitespace collapses to empty and is
        // rejected too, rather than silently falling back to a default.
        let err = Config::from_sources(full_file(), |key| {
            (key == "IGLOO_POSTGRES_SCHEMAS").then(|| " , ".to_string())
        })
        .unwrap_err()
        .to_string();
        assert!(err.contains("schemas"), "got: {}", err);
    }

    // --- multi-source ([[sources]]) form -------------------------------------

    #[test]
    fn sources_are_resolved_in_order_with_defaults() {
        let file = sources_file(vec![
            FileSource {
                schemas: Some(vec!["public".into(), "billing".into()]),
                tables: Some(vec!["orders".into()]),
                ..source_entry("orders_db", Some("postgres://o@db/orders"))
            },
            source_entry("crm", Some("postgres://c@db/crm")),
        ]);
        let config = Config::from_sources(file, no_env).unwrap();

        let names: Vec<&str> = config
            .postgres_sources
            .iter()
            .map(|s| s.name.as_str())
            .collect();
        assert_eq!(names, vec!["orders_db", "crm"], "configured order kept");
        assert_eq!(config.primary_source().name, "orders_db");
        assert_eq!(
            config.postgres_sources[0].schemas,
            vec!["public", "billing"]
        );
        assert_eq!(
            config.postgres_sources[0].tables.as_deref(),
            Some(["orders".to_string()].as_slice())
        );
        assert_eq!(
            config.postgres_sources[1].schemas,
            vec!["public".to_string()],
            "schemas default to public per source"
        );
        assert_eq!(primary_uri(&config), "postgres://o@db/orders");
    }

    #[test]
    fn per_source_env_supplies_uri_and_schemas() {
        // The file names the source but holds no credentials: the URI comes
        // from IGLOO_SOURCE_<NAME>_URI, so secrets stay out of the file.
        let file = sources_file(vec![source_entry("orders_db", None)]);
        let config = Config::from_sources(file, |key| match key {
            "IGLOO_SOURCE_ORDERS_DB_URI" => Some("postgres://from-env/orders".to_string()),
            "IGLOO_SOURCE_ORDERS_DB_SCHEMAS" => Some("public, billing".to_string()),
            _ => None,
        })
        .unwrap();
        assert_eq!(primary_uri(&config), "postgres://from-env/orders");
        assert_eq!(config.primary_source().schemas, vec!["public", "billing"]);
    }

    #[test]
    fn per_source_env_overrides_the_file_uri() {
        let file = sources_file(vec![source_entry("crm", Some("postgres://in-file/crm"))]);
        let config = Config::from_sources(file, |key| {
            (key == "IGLOO_SOURCE_CRM_URI").then(|| "postgres://from-env/crm".to_string())
        })
        .unwrap();
        assert_eq!(primary_uri(&config), "postgres://from-env/crm");
    }

    #[test]
    fn source_without_uri_anywhere_names_the_env_key() {
        let file = sources_file(vec![source_entry("orders_db", None)]);
        let err = Config::from_sources(file, no_env).unwrap_err().to_string();
        assert!(err.contains("orders_db"), "got: {}", err);
        assert!(err.contains("IGLOO_SOURCE_ORDERS_DB_URI"), "got: {}", err);
    }

    #[test]
    fn mixing_sources_with_flat_keys_in_the_file_is_rejected() {
        let file = FileConfig {
            sources: Some(vec![source_entry("crm", Some("postgres://c@db/crm"))]),
            ..full_file() // still carries postgres_uri
        };
        let err = Config::from_sources(file, no_env).unwrap_err().to_string();
        assert!(err.contains("[[sources]]"), "got: {}", err);
        assert!(err.contains("postgres_uri"), "got: {}", err);
    }

    #[test]
    fn flat_env_vars_do_not_override_named_sources() {
        // An inherited DATABASE_URL must not silently redirect a multi-source
        // deployment (it is ignored, with a warning).
        let file = sources_file(vec![source_entry("crm", Some("postgres://in-file/crm"))]);
        let config = Config::from_sources(file, |key| {
            (key == "DATABASE_URL").then(|| "postgres://inherited/other".to_string())
        })
        .unwrap();
        assert_eq!(config.postgres_sources.len(), 1);
        assert_eq!(primary_uri(&config), "postgres://in-file/crm");
    }

    #[test]
    fn empty_sources_list_is_rejected() {
        let err = Config::from_sources(sources_file(vec![]), no_env)
            .unwrap_err()
            .to_string();
        assert!(err.contains("sources"), "got: {}", err);
    }

    #[test]
    fn duplicate_source_names_are_rejected() {
        let file = sources_file(vec![
            source_entry("crm", Some("postgres://a@db/one")),
            source_entry("crm", Some("postgres://b@db/two")),
        ]);
        let err = Config::from_sources(file, no_env).unwrap_err().to_string();
        assert!(err.contains("duplicate source name"), "got: {}", err);
        assert!(err.contains("crm"), "got: {}", err);
    }

    #[test]
    fn source_names_must_be_usable_as_unquoted_sql_identifiers() {
        for bad in ["Orders", "9lives", "with-dash", "with space", ""] {
            let file = sources_file(vec![source_entry(bad, Some("postgres://a@db/one"))]);
            let err = Config::from_sources(file, no_env)
                .unwrap_err()
                .to_string()
                .to_lowercase();
            assert!(
                err.contains("source name"),
                "name {:?} should be rejected; got: {}",
                bad,
                err
            );
        }
        // Legal names round-trip.
        for good in ["crm", "orders_db", "pg2"] {
            let file = sources_file(vec![source_entry(good, Some("postgres://a@db/one"))]);
            let config = Config::from_sources(file, no_env)
                .unwrap_or_else(|e| panic!("name {:?} should be accepted: {}", good, e));
            assert_eq!(config.primary_source().name, good);
        }
    }

    #[test]
    fn reserved_source_names_are_rejected() {
        for reserved in ["datafusion", "information_schema"] {
            let file = sources_file(vec![source_entry(reserved, Some("postgres://a@db/one"))]);
            let err = Config::from_sources(file, no_env).unwrap_err().to_string();
            assert!(err.contains("reserved"), "got: {}", err);
        }
    }

    #[test]
    fn source_names_are_trimmed_before_validation() {
        let file = sources_file(vec![source_entry("  crm  ", Some("postgres://a@db/one"))]);
        let config = Config::from_sources(file, no_env).unwrap();
        assert_eq!(config.primary_source().name, "crm");
    }

    #[test]
    fn empty_table_allowlist_is_rejected() {
        let file = sources_file(vec![FileSource {
            tables: Some(vec![]),
            ..source_entry("crm", Some("postgres://a@db/one"))
        }]);
        let err = Config::from_sources(file, no_env).unwrap_err().to_string();
        assert!(err.contains("tables"), "got: {}", err);

        let file = sources_file(vec![FileSource {
            tables: Some(vec!["  ".into()]),
            ..source_entry("crm", Some("postgres://a@db/one"))
        }]);
        let err = Config::from_sources(file, no_env).unwrap_err().to_string();
        assert!(err.contains("tables"), "got: {}", err);
    }

    #[test]
    fn invalid_source_uri_names_the_source() {
        let file = sources_file(vec![source_entry("crm", Some("nonsense"))]);
        let err = Config::from_sources(file, no_env).unwrap_err().to_string();
        assert!(err.contains("crm"), "got: {}", err);
        assert!(err.contains("uri"), "got: {}", err);
    }

    #[test]
    fn multi_source_toml_round_trips() {
        // The documented file shape parses, including per-source keys.
        let toml_text = r#"
            parquet_path = "/data/parquet"
            cdc_path = "/data/cdc"

            [[sources]]
            name = "orders_db"
            uri = "postgres://u@db:5432/orders"
            schemas = ["public", "billing"]
            tables = ["orders"]

            [[sources]]
            name = "crm"
            uri = "postgres://u@db:5432/crm"
        "#;
        let file: FileConfig = toml::from_str(toml_text).expect("parses");
        let config = Config::from_sources(file, no_env).unwrap();
        assert_eq!(config.postgres_sources.len(), 2);
        assert_eq!(config.postgres_sources[1].name, "crm");
    }

    #[test]
    fn unknown_source_keys_are_rejected() {
        let toml_text = r#"
            parquet_path = "/data/parquet"
            cdc_path = "/data/cdc"

            [[sources]]
            name = "crm"
            uri = "postgres://u@db/crm"
            schema = "public"
        "#;
        assert!(
            toml::from_str::<FileConfig>(toml_text).is_err(),
            "a typo inside [[sources]] must fail loudly"
        );
    }

    // --- misc ---------------------------------------------------------------

    #[test]
    fn empty_paths_are_rejected() {
        let file = FileConfig {
            parquet_path: Some("   ".into()),
            ..full_file()
        };
        assert!(Config::from_sources(file, no_env).is_err());
    }

    #[test]
    fn secret_never_leaks_in_debug_display_or_config_debug() {
        let secret = Secret::new("postgres://user:hunter2@db/igloo");
        assert_eq!(format!("{:?}", secret), "Secret(***)");
        assert_eq!(format!("{}", secret), "***");

        let config = Config::from_sources(full_file(), no_env).unwrap();
        let debug = format!("{:?}", config);
        assert!(!debug.contains("u:p@"), "credentials leaked: {}", debug);

        // Per-source URIs are redacted too, not just a top-level field.
        let file = sources_file(vec![source_entry(
            "crm",
            Some("postgres://user:hunter2@db/crm"),
        )]);
        let config = Config::from_sources(file, no_env).unwrap();
        let debug = format!("{:?}", config);
        assert!(!debug.contains("hunter2"), "credentials leaked: {}", debug);
        // The source itself must be safe to log on its own.
        let source_debug = format!("{:?}", config.primary_source());
        assert!(
            !source_debug.contains("hunter2"),
            "credentials leaked: {}",
            source_debug
        );
    }

    #[test]
    fn source_descriptors_are_constructible_without_a_file() {
        let source = PostgresSource::new("crm", "postgres://u@db/crm", vec!["public".into()])
            .with_tables(vec!["customers".into()]);
        assert_eq!(source.name, "crm");
        assert_eq!(source.uri.expose(), "postgres://u@db/crm");
        assert_eq!(
            source.tables.as_deref(),
            Some(["customers".to_string()].as_slice())
        );
    }

    #[test]
    fn unknown_file_keys_are_rejected() {
        let err = toml::from_str::<FileConfig>("postgress_uri = \"typo\"");
        assert!(err.is_err(), "unknown keys must fail loudly");
    }
}
