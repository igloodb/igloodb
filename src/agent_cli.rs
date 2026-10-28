//! Noninteractive JSON commands. Stdout is exclusively machine-readable.
use std::io::Read;

use serde_json::Value;

use crate::agent::{AgentError, AgentSession, QueryRequest, SchemaRequest, MAX_SQL_BYTES};

pub const HELP: &str = "Igloo — experimental SQL federation for developers and AI agents

Usage:
  igloo mcp [--demo]                          Native MCP server over stdio
  igloo mcp-http [--demo] [--listen IP:PORT]  Authenticated Streamable HTTP MCP
      [--behind-tls-proxy] [--allowed-host HOST]...
  igloo query [--demo] --sql 'SELECT ...'      One bounded, read-only JSON result
  igloo explain [--demo] --sql 'SELECT ...'    Logical plan; no query execution
  igloo query|explain|schema [--demo] --json   Read a JSON request from stdin
  igloo schema [--demo]                       Discover tables and typed columns
  igloo capabilities                         JSON limits and contract information
  igloo connectors                           Connector routes and verified coverage
  igloo cdc-check [--source NAME] --publication NAME [--timeout-ms N]
                                            PostgreSQL logical replication probe
  igloo serve                                PostgreSQL wire server (configured)
  igloo crypto-demo                          Deterministic market analytics demo

query/explain JSON: {\"sql\":\"SELECT 1\",\"max_rows\":100,\"timeout_ms\":10000}
schema JSON: {\"catalog\":\"app\",\"after\":null,\"max_tables\":50}

--demo uses an in-memory sample_events(id, event, account_id) table.
Configured agent commands read IGLOO_CONFIG / igloo.toml and source environment
variables. Native PostgreSQL and --demo need no ADBC driver; adbc_sources do.
No model API key is required. Agent queries bypass cache.
JSON commands exit 0 on success or 2 with an error object; diagnostics use stderr.
See docs/AGENTS.md for setup, exact encodings, limits, and MCP client examples.
HTTP MCP binds to loopback by default and requires IGLOO_MCP_HTTP_TOKEN or
IGLOO_MCP_HTTP_TOKEN_FILE. Igloo serves plaintext HTTP; non-loopback binding
requires --behind-tls-proxy and an explicit --allowed-host for the TLS proxy.
";

fn invalid(message: &str) -> AgentError {
    AgentError::new(
        "invalid_request",
        message,
        "Run igloo --help for command usage.",
    )
}

pub async fn run(command: &str, args: Vec<String>) -> Result<(), AgentError> {
    if command == "mcp-http" {
        return run_http(args).await;
    }
    let mut demo = false;
    let mut json_input = false;
    let mut sql = None;
    let mut args = args.into_iter();
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--demo" if !demo => demo = true,
            "--json" if !json_input => json_input = true,
            "--sql" if sql.is_none() => {
                sql = Some(
                    args.next()
                        .ok_or_else(|| invalid("--sql requires a value."))?,
                )
            }
            _ => return Err(invalid("Unknown or duplicate argument.")),
        }
    }
    let is_query = matches!(command, "query" | "explain");
    if (sql.is_some() && (!is_query || json_input))
        || (json_input && matches!(command, "mcp" | "capabilities" | "connectors"))
    {
        return Err(invalid(
            "--sql is for query/explain; --json and --sql are mutually exclusive.",
        ));
    }
    // Validate all input before opening database connections.
    let input: Option<Value> = if json_input {
        let mut bytes = Vec::new();
        std::io::stdin()
            .take((MAX_SQL_BYTES * 2 + 1) as u64)
            .read_to_end(&mut bytes)
            .map_err(|_| invalid("Could not read stdin."))?;
        if bytes.len() > MAX_SQL_BYTES * 2 {
            return Err(invalid("JSON input exceeds 128 KiB."));
        }
        Some(
            serde_json::from_slice(&bytes)
                .map_err(|_| invalid("stdin must contain one valid JSON object."))?,
        )
    } else {
        None
    };
    let query = if is_query {
        Some(match input.clone() {
            Some(value) => serde_json::from_value::<QueryRequest>(value).map_err(|_| {
                invalid("Invalid query JSON. Expected sql, optional max_rows and timeout_ms.")
            })?,
            None => QueryRequest {
                sql: sql.ok_or_else(|| invalid("Provide --sql or --json."))?,
                max_rows: None,
                timeout_ms: None,
            },
        })
    } else {
        None
    };
    let schema = if command == "schema" {
        Some(match input {
            Some(value) => serde_json::from_value::<SchemaRequest>(value).map_err(|_| {
                invalid("Invalid schema JSON. Expected optional catalog, after and max_tables.")
            })?,
            None => SchemaRequest::default(),
        })
    } else {
        None
    };
    if command == "connectors" {
        println!("{}", crate::connectors::inventory());
        return Ok(());
    }
    let session = if demo || command == "capabilities" {
        AgentSession::demo()?
    } else {
        AgentSession::configured().await?
    };
    if command == "mcp" {
        return crate::mcp::serve(session).await;
    }
    let value = match command {
        "query" | "explain" => {
            session
                .query(query.expect("validated above"), command == "explain")
                .await?
        }
        "schema" => session.schema(schema.expect("validated above")).await?,
        "capabilities" => session.capabilities(),
        _ => return Err(invalid("Unknown command.")),
    };
    println!("{value}");
    Ok(())
}

const DEFAULT_HTTP_LISTEN: &str = "127.0.0.1:8000";

#[derive(Debug, PartialEq, Eq)]
struct HttpArgs {
    demo: bool,
    listen: std::net::SocketAddr,
    behind_tls_proxy: bool,
    allowed_hosts: Vec<String>,
}

fn parse_http_args(args: Vec<String>) -> Result<HttpArgs, AgentError> {
    let mut demo = false;
    let mut listen = None;
    let mut behind_tls_proxy = false;
    let mut allowed_hosts = Vec::new();
    let mut args = args.into_iter();
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--demo" if !demo => demo = true,
            "--listen" if listen.is_none() => {
                let value = args
                    .next()
                    .ok_or_else(|| invalid("--listen requires an IP:PORT value."))?;
                listen = Some(value.parse().map_err(|_| {
                    invalid(
                        "--listen must be a numeric IP address and port, such as 127.0.0.1:8000.",
                    )
                })?);
            }
            "--behind-tls-proxy" if !behind_tls_proxy => behind_tls_proxy = true,
            "--allowed-host" => {
                let value = args
                    .next()
                    .ok_or_else(|| invalid("--allowed-host requires a host or host:port value."))?;
                if value.is_empty() || value.bytes().any(|byte| byte.is_ascii_whitespace()) {
                    return Err(invalid(
                        "--allowed-host must be a non-empty host or host:port value.",
                    ));
                }
                if !allowed_hosts.contains(&value) {
                    allowed_hosts.push(value);
                }
            }
            _ => return Err(invalid("Unknown or duplicate mcp-http argument.")),
        }
    }
    let listen =
        listen.unwrap_or_else(|| DEFAULT_HTTP_LISTEN.parse().expect("static socket address"));
    crate::mcp::validate_http_bind(listen, behind_tls_proxy)?;
    if !listen.ip().is_loopback() && allowed_hosts.is_empty() {
        return Err(AgentError::new(
            "invalid_configuration",
            "A non-loopback listener requires an explicit allowed host.",
            "Add --allowed-host with the host name used by the TLS reverse proxy.",
        ));
    }
    Ok(HttpArgs {
        demo,
        listen,
        behind_tls_proxy,
        allowed_hosts,
    })
}

pub async fn run_http(args: Vec<String>) -> Result<(), AgentError> {
    let args = parse_http_args(args)?;
    // Resolve credentials before opening any configured source connections.
    let auth = crate::mcp::HttpAuth::from_environment()?;
    let session = if args.demo {
        AgentSession::demo()?
    } else {
        AgentSession::configured().await?
    };
    crate::mcp::serve_http(
        session,
        args.listen,
        auth,
        args.behind_tls_proxy,
        args.allowed_hosts,
    )
    .await
}

#[cfg(test)]
mod http_cli_tests {
    use super::{parse_http_args, DEFAULT_HTTP_LISTEN};
    use std::net::SocketAddr;

    #[test]
    fn http_cli_defaults_to_loopback_and_rejects_non_loopback_without_proxy() {
        let args = parse_http_args(Vec::new()).unwrap();
        assert_eq!(
            args.listen,
            DEFAULT_HTTP_LISTEN.parse::<SocketAddr>().unwrap()
        );
        assert!(!args.behind_tls_proxy);
        assert!(parse_http_args(vec!["--listen".into(), "0.0.0.0:8000".into()]).is_err());
    }

    #[test]
    fn remote_listener_requires_explicit_proxy_and_allowed_host() {
        assert!(parse_http_args(vec![
            "--listen".into(),
            "0.0.0.0:8000".into(),
            "--behind-tls-proxy".into(),
        ])
        .is_err());
        let args = parse_http_args(vec![
            "--listen".into(),
            "10.0.0.5:8000".into(),
            "--behind-tls-proxy".into(),
            "--allowed-host".into(),
            "mcp.internal:8000".into(),
        ])
        .unwrap();
        assert_eq!(args.allowed_hosts, ["mcp.internal:8000"]);
    }

    #[test]
    fn conflicting_http_flags_are_rejected() {
        assert!(parse_http_args(vec!["--demo".into(), "--demo".into()]).is_err());
        assert!(parse_http_args(vec![
            "--listen".into(),
            "127.0.0.1:8000".into(),
            "--listen".into(),
            "127.0.0.1:8001".into(),
        ])
        .is_err());
        assert!(parse_http_args(vec![
            "--listen".into(),
            "127.0.0.1:8000".into(),
            "--behind-tls-proxy".into(),
        ])
        .is_ok());
    }
}
