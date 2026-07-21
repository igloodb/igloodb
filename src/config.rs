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
    listen_addr: Option<String>,
    cache_max_entries: Option<u64>,
    cache_ttl_seconds: Option<u64>,
    cdc_poll_seconds: Option<u64>,
}

/// Default cache capacity when not configured.
const DEFAULT_CACHE_MAX_ENTRIES: u64 = 1024;
/// Default cache entry TTL when not configured.
const DEFAULT_CACHE_TTL_SECONDS: u64 = 300;
/// Default CDC polling interval in serve mode.
const DEFAULT_CDC_POLL_SECONDS: u64 = 10;
/// Default PostgreSQL schema to introspect when none is configured.
const DEFAULT_POSTGRES_SCHEMA: &str = "public";

/// Fully-resolved application configuration.
#[derive(Debug)]
pub struct Config {
    /// Directory of Parquet files registered as the `iceberg` table.
    pub parquet_path: String,
    /// Directory watched for CDC event files.
    pub cdc_path: String,
    /// PostgreSQL connection string (URI or key-value form).
    pub postgres_uri: Secret,
    /// PostgreSQL schemas (namespaces) to introspect for tables to register
    /// (default `["public"]`). Never empty after validation.
    pub postgres_schemas: Vec<String>,
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
        let parquet_path = env("IGLOO_PARQUET_PATH")
            .or(file.parquet_path)
            .ok_or_else(|| missing("parquet_path", "IGLOO_PARQUET_PATH"))?;
        let cdc_path = env("IGLOO_CDC_PATH")
            .or(file.cdc_path)
            .ok_or_else(|| missing("cdc_path", "IGLOO_CDC_PATH"))?;
        let postgres_uri = env("DATABASE_URL")
            .or_else(|| env("IGLOO_POSTGRES_URI"))
            .or(file.postgres_uri)
            .ok_or_else(|| missing("postgres_uri", "IGLOO_POSTGRES_URI or DATABASE_URL"))?;
        // Env is a comma-separated list; file is a TOML array. Env wins when
        // set. Absent everywhere → the documented default of ["public"].
        let postgres_schemas = env("IGLOO_POSTGRES_SCHEMAS")
            .map(|raw| {
                raw.split(',')
                    .map(|s| s.trim().to_string())
                    .filter(|s| !s.is_empty())
                    .collect::<Vec<_>>()
            })
            .or(file.postgres_schemas)
            .unwrap_or_else(|| vec![DEFAULT_POSTGRES_SCHEMA.to_string()]);
        let listen_addr = env("IGLOO_LISTEN_ADDR").or(file.listen_addr);
        let cache_max_entries = env_u64(&env, "IGLOO_CACHE_MAX_ENTRIES")?
            .or(file.cache_max_entries)
            .unwrap_or(DEFAULT_CACHE_MAX_ENTRIES);
        let cache_ttl_seconds = env_u64(&env, "IGLOO_CACHE_TTL_SECONDS")?
            .or(file.cache_ttl_seconds)
            .unwrap_or(DEFAULT_CACHE_TTL_SECONDS);
        let cdc_poll_seconds = env_u64(&env, "IGLOO_CDC_POLL_SECONDS")?
            .or(file.cdc_poll_seconds)
            .unwrap_or(DEFAULT_CDC_POLL_SECONDS);

        let config = Self {
            parquet_path,
            cdc_path,
            postgres_uri: Secret::new(postgres_uri),
            postgres_schemas,
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
        let uri = self.postgres_uri.expose();
        let looks_like_uri = uri.starts_with("postgres://") || uri.starts_with("postgresql://");
        let looks_like_kv = uri.contains('=');
        if !(looks_like_uri || looks_like_kv) {
            return Err(IglooError::Config(
                "postgres_uri must be a postgres:// URI or key-value connection string".into(),
            ));
        }
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
        if self.postgres_schemas.is_empty() {
            return Err(IglooError::Config(
                "postgres_schemas must list at least one schema".into(),
            ));
        }
        if self.postgres_schemas.iter().any(|s| s.trim().is_empty()) {
            return Err(IglooError::Config(
                "postgres_schemas must not contain empty schema names".into(),
            ));
        }
        Ok(())
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
    use super::{Config, FileConfig, Secret};

    fn no_env(_: &str) -> Option<String> {
        None
    }

    fn full_file() -> FileConfig {
        FileConfig {
            parquet_path: Some("/data/parquet".into()),
            cdc_path: Some("/data/cdc".into()),
            postgres_uri: Some("postgres://u:p@db:5432/mydb".into()),
            postgres_schemas: None,
            listen_addr: None,
            cache_max_entries: None,
            cache_ttl_seconds: None,
            cdc_poll_seconds: None,
        }
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
        assert_eq!(
            config.postgres_uri.expose(),
            "postgres://from-database-url/db"
        );
    }

    #[test]
    fn file_alone_is_sufficient() {
        let config = Config::from_sources(full_file(), no_env).unwrap();
        assert_eq!(config.parquet_path, "/data/parquet");
        assert_eq!(config.postgres_uri.expose(), "postgres://u:p@db:5432/mydb");
    }

    #[test]
    fn invalid_postgres_uri_is_rejected() {
        let file = FileConfig {
            postgres_uri: Some("not-a-connection-string".into()),
            ..full_file()
        };
        let err = Config::from_sources(file, no_env).unwrap_err().to_string();
        assert!(err.contains("postgres_uri"), "got: {}", err);
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

    #[test]
    fn postgres_schemas_defaults_to_public() {
        let config = Config::from_sources(full_file(), no_env).unwrap();
        assert_eq!(config.postgres_schemas, vec!["public".to_string()]);
    }

    #[test]
    fn postgres_schemas_from_file() {
        let file = FileConfig {
            postgres_schemas: Some(vec!["public".into(), "analytics".into()]),
            ..full_file()
        };
        let config = Config::from_sources(file, no_env).unwrap();
        assert_eq!(config.postgres_schemas, vec!["public", "analytics"]);
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
            config.postgres_schemas,
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
        assert!(err.contains("postgres_schemas"), "got: {}", err);

        // An env value of only commas/whitespace collapses to empty and is
        // rejected too, rather than silently falling back to a default.
        let err = Config::from_sources(full_file(), |key| {
            (key == "IGLOO_POSTGRES_SCHEMAS").then(|| " , ".to_string())
        })
        .unwrap_err()
        .to_string();
        assert!(err.contains("postgres_schemas"), "got: {}", err);
    }

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
    }

    #[test]
    fn unknown_file_keys_are_rejected() {
        let err = toml::from_str::<FileConfig>("postgress_uri = \"typo\"");
        assert!(err.is_err(), "unknown keys must fail loudly");
    }
}
