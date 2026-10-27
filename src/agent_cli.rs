//! Noninteractive JSON commands. Stdout is exclusively machine-readable.
use std::io::Read;

use serde_json::Value;

use crate::agent::{AgentError, AgentSession, QueryRequest, SchemaRequest, MAX_SQL_BYTES};

pub const HELP: &str = "Igloo — experimental SQL federation for developers and AI agents

Usage:
  igloo mcp [--demo]                          Native MCP server over stdio
  igloo query [--demo] --sql 'SELECT ...'      One bounded, read-only JSON result
  igloo explain [--demo] --sql 'SELECT ...'    Logical plan; no query execution
  igloo query|explain|schema [--demo] --json   Read a JSON request from stdin
  igloo schema [--demo]                       Discover tables and typed columns
  igloo capabilities                         JSON limits and contract information
  igloo serve                                PostgreSQL wire server (configured)
  igloo crypto-demo                          Deterministic market analytics demo

query/explain JSON: {\"sql\":\"SELECT 1\",\"max_rows\":100,\"timeout_ms\":10000}
schema JSON: {\"catalog\":\"app\",\"after\":null,\"max_tables\":50}

--demo uses an in-memory sample_events(id, event, account_id) table.
Configured agent commands read IGLOO_CONFIG / igloo.toml and source environment
variables. No ADBC driver or model API key is required. Agent queries bypass cache.
JSON commands exit 0 on success or 2 with an error object; diagnostics use stderr.
See docs/AGENTS.md for setup, exact encodings, limits, and MCP client examples.
";

fn invalid(message: &str) -> AgentError {
    AgentError::new(
        "invalid_request",
        message,
        "Run igloo --help for command usage.",
    )
}

pub async fn run(command: &str, args: Vec<String>) -> Result<(), AgentError> {
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
        || (json_input && matches!(command, "mcp" | "capabilities"))
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
