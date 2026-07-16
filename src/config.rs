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
}

/// Fully-resolved application configuration.
#[derive(Debug)]
pub struct Config {
    /// Directory of Parquet files registered as the `iceberg` table.
    pub parquet_path: String,
    /// Directory watched for CDC event files.
    pub cdc_path: String,
    /// PostgreSQL connection string (URI or key-value form).
    pub postgres_uri: Secret,
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

        let config = Self {
            parquet_path,
            cdc_path,
            postgres_uri: Secret::new(postgres_uri),
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
        Ok(())
    }
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
