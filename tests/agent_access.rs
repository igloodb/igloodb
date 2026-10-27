use igloo::agent::{AgentSession, QueryRequest, SchemaRequest};
use igloo::mcp::IglooMcp;
use rmcp::{model::CallToolRequestParams, ServiceExt};
use serde_json::{json, Value};
use std::io::Write;
use std::process::{Command, Stdio};

fn query(sql: &str, max_rows: usize) -> QueryRequest {
    QueryRequest {
        sql: sql.into(),
        max_rows: Some(max_rows),
        timeout_ms: None,
    }
}

#[tokio::test]
async fn discovery_query_plan_and_exact_encoding() {
    let session = AgentSession::demo().unwrap();
    let schema = session.schema(SchemaRequest::default()).await.unwrap();
    assert_eq!(schema["tables"][0]["name"], "sample_events");
    let name = schema["tables"][0]["qualified_name"].as_str().unwrap();
    let result = session
        .query(
            query(&format!("SELECT * FROM {name} ORDER BY id"), 2),
            false,
        )
        .await
        .unwrap();
    assert_eq!(result["rows"][0]["id"], "1");
    assert_eq!(result["row_count"], 2);
    assert_eq!(result["truncated"], true);
    let plan = session
        .query(query("SELECT count(*) AS n FROM sample_events", 10), true)
        .await
        .unwrap();
    assert_eq!(plan["executed"], false);
    assert!(plan["plan"].as_str().unwrap().contains("Aggregate"));
    let result = session.query(query("SELECT 9223372036854775807 AS n, CAST(1.23 AS DECIMAL(12,2)) AS d, CAST(NULL AS VARCHAR) AS missing", 10), false).await.unwrap();
    assert_eq!(
        result["rows"][0],
        json!({"n":"9223372036854775807", "d":"1.23", "missing":null})
    );
    let result = session
        .query(query("SELECT * FROM sample_events WHERE id < 0", 10), false)
        .await
        .unwrap();
    assert_eq!(result["rows"], json!([]));
    assert_eq!(result["columns"].as_array().unwrap().len(), 3);
    assert_eq!(result["truncated"], false);
}

#[tokio::test]
async fn policy_rejects_writes_exports_and_stacked_statements() {
    let session = AgentSession::demo().unwrap();
    for sql in [
        "CREATE TABLE stolen AS SELECT 1",
        "DROP TABLE sample_events",
        "INSERT INTO sample_events VALUES (5, 'bad', 1)",
        "DELETE FROM sample_events",
        "SET datafusion.execution.batch_size = 1",
        "COPY (SELECT 1) TO '/tmp/igloo-agent-must-not-write.csv'",
        "SELECT 1; SELECT 2",
        "EXPLAIN ANALYZE SELECT * FROM sample_events",
        "WITH deleted AS (DELETE FROM sample_events RETURNING *) SELECT * FROM deleted",
        "SELECT * INTO stolen FROM sample_events",
    ] {
        assert!(
            session.query(query(sql, 10), false).await.is_err(),
            "accepted {sql}"
        );
        assert!(
            session.query(query(sql, 10), true).await.is_err(),
            "explain accepted {sql}"
        );
    }
    assert!(!session.engine.ctx.table_exist("stolen").unwrap());
    let result = session.query(query("/* DELETE is only a comment */ WITH events AS (SELECT * FROM sample_events) SELECT count(*) AS n FROM events", 10), false).await.unwrap();
    assert_eq!(result["rows"][0]["n"], "4");
}

#[tokio::test]
async fn budgets_pagination_and_actionable_errors() {
    let session = AgentSession::demo().unwrap();
    for max_rows in [0, 1001] {
        assert_eq!(
            session
                .query(query("SELECT 1", max_rows), false)
                .await
                .unwrap_err()
                .code,
            "invalid_request"
        );
    }
    let mut request = query("SELECT 1", 1);
    request.timeout_ms = Some(30_001);
    assert_eq!(
        session.query(request, false).await.unwrap_err().code,
        "invalid_request"
    );
    assert_eq!(
        session
            .query(query("SELECT repeat('x', 1048577) AS big", 1), false)
            .await
            .unwrap_err()
            .code,
        "response_too_large"
    );
    assert!(session
        .query(
            query("SELECT id, account_id AS id FROM sample_events", 1),
            false
        )
        .await
        .is_err());
    assert!(session
        .query(query("SELECT * FROM unknown_table", 1), false)
        .await
        .is_err());
    let result = session
        .query(query("SELECT id FROM sample_events LIMIT 2", 2), false)
        .await
        .unwrap();
    assert_eq!(result["truncated"], false);
    session
        .engine
        .ctx
        .register_batch(
            "another",
            session
                .engine
                .ctx
                .table("sample_events")
                .await
                .unwrap()
                .collect()
                .await
                .unwrap()
                .remove(0),
        )
        .unwrap();
    let first = session
        .schema(SchemaRequest {
            max_tables: Some(1),
            ..Default::default()
        })
        .await
        .unwrap();
    let cursor = first["next_cursor"].as_str().unwrap();
    let next = session
        .schema(SchemaRequest {
            max_tables: Some(1),
            after: Some(cursor.into()),
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(next["tables"][0]["name"], "sample_events");
    assert_eq!(next["next_cursor"], Value::Null);
}

#[tokio::test]
async fn mcp_sdk_client_discovers_and_calls_native_tools() {
    let (server_io, client_io) = tokio::io::duplex(16_384);
    let server = tokio::spawn(async move {
        IglooMcp::new(AgentSession::demo().unwrap())
            .serve(server_io)
            .await
            .unwrap()
            .waiting()
            .await
            .unwrap();
    });
    let client = ().serve(client_io).await.unwrap();
    let tools = client.list_all_tools().await.unwrap();
    assert_eq!(tools.len(), 4);
    assert!(tools
        .iter()
        .all(|tool| tool.annotations.as_ref().unwrap().read_only_hint == Some(true)));
    let result = client
        .call_tool(
            CallToolRequestParams::new("igloo_query").with_arguments(
                json!({"sql":"SELECT count(*) AS n FROM sample_events"})
                    .as_object()
                    .unwrap()
                    .clone(),
            ),
        )
        .await
        .unwrap();
    assert_ne!(result.is_error, Some(true));
    assert_eq!(result.structured_content.unwrap()["rows"][0]["n"], "4");
    let result = client
        .call_tool(
            CallToolRequestParams::new("igloo_query").with_arguments(
                json!({"sql":"DROP TABLE sample_events"})
                    .as_object()
                    .unwrap()
                    .clone(),
            ),
        )
        .await
        .unwrap();
    assert_eq!(result.is_error, Some(true));
    assert_eq!(
        result.structured_content.unwrap()["error"]["code"],
        "read_only_violation"
    );
    client.cancel().await.unwrap();
    server.await.unwrap();
}

#[test]
fn cli_json_is_clean_and_errors_have_nonzero_exit_status() {
    let mut child = Command::new(env!("CARGO_BIN_EXE_igloo"))
        .args(["query", "--demo", "--json"])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .unwrap();
    child
        .stdin
        .take()
        .unwrap()
        .write_all(br#"{"sql":"SELECT count(*) AS n FROM sample_events"}"#)
        .unwrap();
    let output = child.wait_with_output().unwrap();
    assert!(output.status.success());
    let body: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(body["rows"][0]["n"], "4");
    for args in [
        vec!["query", "--demo", "--sql", "DROP TABLE sample_events"],
        vec!["query", "--demo", "--sql"],
        vec!["schema", "--demo", "--sql", "SELECT 1"],
    ] {
        let output = Command::new(env!("CARGO_BIN_EXE_igloo"))
            .args(args)
            .output()
            .unwrap();
        assert_eq!(output.status.code(), Some(2));
        let body: Value = serde_json::from_slice(&output.stdout).unwrap();
        assert_eq!(body["ok"], false);
        assert!(body["error"]["hint"].is_string());
    }
}

#[tokio::test]
async fn live_postgres_agent_queries_discover_allowlist_and_bypass_cache() {
    let Ok(uri) = std::env::var("IGLOO_TEST_POSTGRES_URI") else {
        eprintln!("skipping live agent test: IGLOO_TEST_POSTGRES_URI is unset");
        return;
    };
    let (client, connection) = tokio_postgres::connect(&uri, tokio_postgres::NoTls)
        .await
        .unwrap();
    tokio::spawn(async move {
        connection.await.unwrap();
    });
    let table = format!("igloo_agent_fixture_{}", std::process::id());
    client.batch_execute(&format!("CREATE TABLE {table} (id BIGINT, label TEXT); INSERT INTO {table} VALUES (42, 'initial')")).await.unwrap();
    let directory = std::env::temp_dir().join(&table);
    std::fs::create_dir_all(&directory).unwrap();
    let source = igloo::config::PostgresSource::new("agent_source", uri, vec!["public".into()])
        .with_tables(vec![table.clone()]);
    let engine =
        igloo::datafusion_engine::DataFusionEngine::new(directory.to_str().unwrap(), &[source])
            .await
            .unwrap();
    let session = AgentSession::new(engine, false);
    let schema = session
        .schema(SchemaRequest {
            catalog: Some("agent_source".into()),
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(schema["tables"].as_array().unwrap().len(), 1);
    assert_eq!(schema["tables"][0]["name"], table);
    let sql = format!("SELECT id, label FROM agent_source.public.{table} WHERE id = 42");
    let first = session.query(query(&sql, 10), false).await.unwrap();
    assert_eq!(first["rows"][0], json!({"id":"42", "label":"initial"}));
    client
        .batch_execute(&format!("UPDATE {table} SET label = 'changed'"))
        .await
        .unwrap();
    let second = session.query(query(&sql, 10), false).await.unwrap();
    assert_eq!(second["rows"][0]["label"], "changed");
    assert_eq!(second["cache"], "bypass");
    assert_eq!(second["demo"], false);
    client
        .batch_execute(&format!("DROP TABLE {table}"))
        .await
        .unwrap();
    std::fs::remove_dir_all(directory).unwrap();
}
