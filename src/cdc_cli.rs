//! Operator-only probe for the native PostgreSQL CDC foundation.
//!
//! This command verifies a committed logical marker and removes its temporary
//! slot before returning. It does not enable CDC-backed query caching.

use std::time::Duration;

use serde_json::{json, Value};
use tokio::task::JoinHandle;
use tokio_postgres::NoTls;

use crate::agent::{AgentError, MAX_TIMEOUT_MS};
use crate::config::Config;
use crate::postgres_cdc::{PgCdcConfig, PgCdcSource};

struct ProbeArgs {
    source: Option<String>,
    publication: String,
    timeout: Duration,
}

fn invalid(message: &str) -> AgentError {
    AgentError::new(
        "invalid_request",
        message,
        "Use cdc-check [--source NAME] --publication NAME [--timeout-ms 10000].",
    )
}

fn parse(args: Vec<String>) -> Result<ProbeArgs, AgentError> {
    let mut source = None;
    let mut publication = None;
    let mut timeout = None;
    let mut args = args.into_iter();
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--source" if source.is_none() => {
                source = Some(
                    args.next()
                        .ok_or_else(|| invalid("--source requires a name."))?,
                );
            }
            "--publication" if publication.is_none() => {
                let value = args
                    .next()
                    .ok_or_else(|| invalid("--publication requires a name."))?;
                if value.is_empty()
                    || value.len() > 63
                    || !value.bytes().all(|byte| {
                        byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_'
                    })
                {
                    return Err(invalid("Use a publication name containing lowercase letters, digits and underscores."));
                }
                publication = Some(value);
            }
            "--timeout-ms" if timeout.is_none() => {
                let value = args
                    .next()
                    .ok_or_else(|| invalid("--timeout-ms requires an integer."))?
                    .parse::<u64>()
                    .map_err(|_| invalid("--timeout-ms requires an integer."))?;
                if !(1..=MAX_TIMEOUT_MS).contains(&value) {
                    return Err(invalid("--timeout-ms must be between 1 and 30000."));
                }
                timeout = Some(Duration::from_millis(value));
            }
            _ => return Err(invalid("Unknown or duplicate cdc-check argument.")),
        }
    }
    Ok(ProbeArgs {
        source,
        publication: publication.ok_or_else(|| invalid("--publication is required."))?,
        timeout: timeout.unwrap_or(Duration::from_secs(10)),
    })
}

struct ConnectionTask(JoinHandle<()>);

impl Drop for ConnectionTask {
    fn drop(&mut self) {
        self.0.abort();
    }
}

/// Verifies native logical replication against one explicitly configured source.
/// Input is validated before opening connections. Error messages never include
/// connection strings, vendor errors, or credential values.
pub async fn run(args: Vec<String>) -> Result<Value, AgentError> {
    let args = parse(args)?;
    let config = Config::load().map_err(|_| {
        AgentError::new(
            "configuration_error",
            "Could not load Igloo source configuration.",
            "Set IGLOO_CONFIG to an existing TOML file with a native PostgreSQL source.",
        )
    })?;
    let source = match args.source {
        Some(name) => config
            .postgres_sources
            .iter()
            .find(|source| source.name == name),
        None if config.postgres_sources.len() == 1 => config.postgres_sources.first(),
        None => None,
    }
    .ok_or_else(|| invalid("Select an existing native PostgreSQL source with --source."))?;
    let connection: tokio_postgres::Config = source.uri.expose().parse().map_err(|_| {
        AgentError::new(
            "configuration_error",
            "Invalid PostgreSQL source connection settings.",
            "Check the configured native source without including credentials in diagnostics.",
        )
    })?;
    let slot = format!("igloo_probe_{}", uuid::Uuid::new_v4().simple());
    let cdc =
        PgCdcConfig::from_postgres_config(&connection, &slot, &args.publication, args.timeout)
            .map_err(|_| {
                invalid(
                    "This probe requires one explicitly named PostgreSQL host, user and database.",
                )
            })?;

    tokio::time::timeout(args.timeout, async {
        // Match the existing native control client's transport. NoTls rejects
        // sslmode=require; required encryption is never silently downgraded.
        let (client, connection) = connection.connect(NoTls).await.map_err(|_| {
            AgentError::new("connection_error", "Could not connect the PostgreSQL control client.",
                "Use a permitted local development source. This CLI control connection does not yet support TLS.")
        })?;
        let _connection = ConnectionTask(tokio::spawn(async move { let _ = connection.await; }));
        let cdc = PgCdcSource::start(cdc).await.map_err(|error| AgentError::new(
            "cdc_setup_error", error.to_string(),
            "Use PostgreSQL 15+ with wal_level=logical, an existing publication, replication privileges and an available slot.",
        ))?;
        let result = cdc.handle().barrier(&client).await;
        // Always close the temporary-slot owner before reporting success/error.
        cdc.shutdown().await;
        let position = result.map_err(|error| AgentError::new(
            "cdc_barrier_error", error.to_string(),
            "Verify logical message output, marker privileges, source identity and stream health.",
        ))?;
        Ok(json!({
            "ok": true,
            "source": source.name,
            "barrier": "committed",
            "generation": position.generation.to_string(),
            "committed_epoch": position.committed_epoch.to_string(),
            "slot": "temporary",
            "query_cache_integration": false,
            "cross_source_snapshot": false
        }))
    }).await.map_err(|_| AgentError::new(
        "timeout", "CDC probe exceeded its total time budget.",
        "Check source availability and publication setup before retrying.",
    ))?
}

#[cfg(test)]
mod tests {
    use super::parse;

    #[test]
    fn rejects_ambiguous_or_invalid_probe_options_before_connecting() {
        for args in [
            vec![],
            vec!["--publication", "pub", "--publication", "other"],
            vec!["--publication", "pub; DROP TABLE orders"],
            vec!["--publication", "pub", "--timeout-ms", "0"],
            vec!["--publication", "pub", "--timeout-ms", "30001"],
            vec!["--publication", "pub", "--demo"],
        ] {
            assert!(parse(args.into_iter().map(str::to_owned).collect()).is_err());
        }
    }

    #[test]
    fn parses_explicit_source_and_bounded_deadline() {
        let args = parse(
            vec![
                "--source",
                "app",
                "--publication",
                "igloo_changes",
                "--timeout-ms",
                "1000",
            ]
            .into_iter()
            .map(str::to_owned)
            .collect(),
        )
        .unwrap();
        assert_eq!(args.source.as_deref(), Some("app"));
        assert_eq!(args.publication, "igloo_changes");
        assert_eq!(args.timeout.as_millis(), 1000);
    }
}
