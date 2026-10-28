use igloo::agent::AgentSession;
use igloo::mcp::{http_router, HttpAuth};
use rmcp::model::CallToolRequestParams;
use rmcp::transport::streamable_http_client::StreamableHttpClientTransportConfig;
use rmcp::transport::StreamableHttpClientTransport;
use rmcp::ServiceExt;
use serde_json::json;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

static TEST_ID: AtomicU64 = AtomicU64::new(0);

fn token_file() -> PathBuf {
    std::env::temp_dir().join(format!(
        "igloo_http_mcp_token_{}_{}",
        std::process::id(),
        TEST_ID.fetch_add(1, Ordering::Relaxed)
    ))
}

async fn raw_request(addr: SocketAddr, authorization: Option<&str>, body: &str) -> String {
    raw_request_with_headers(addr, authorization, &addr.to_string(), None, body).await
}

async fn raw_request_with_headers(
    addr: SocketAddr,
    authorization: Option<&str>,
    host: &str,
    origin: Option<&str>,
    body: &str,
) -> String {
    let mut stream = TcpStream::connect(addr).await.unwrap();
    let authorization = authorization
        .map(|value| format!("Authorization: {value}\r\n"))
        .unwrap_or_default();
    let origin = origin
        .map(|value| format!("Origin: {value}\r\n"))
        .unwrap_or_default();
    let request = format!(
        "POST /mcp HTTP/1.1\r\nHost: {host}\r\nConnection: close\r\nContent-Type: application/json\r\nAccept: application/json, text/event-stream\r\n{authorization}{origin}Content-Length: {}\r\n\r\n{body}",
        body.len()
    );
    stream.write_all(request.as_bytes()).await.unwrap();
    let mut response = Vec::new();
    stream.read_to_end(&mut response).await.unwrap();
    String::from_utf8(response).unwrap()
}

async fn mcp_client(
    addr: SocketAddr,
    token: &str,
) -> rmcp::service::RunningService<rmcp::RoleClient, ()> {
    let transport = StreamableHttpClientTransport::from_config(
        StreamableHttpClientTransportConfig::with_uri(format!("http://{addr}/mcp"))
            .auth_header(token),
    );
    ().serve(transport).await.unwrap()
}

#[tokio::test]
async fn streamable_http_requires_bearer_auth_and_supports_token_revocation() {
    let token_path = token_file();
    let first_token = "first-test-token-that-is-long-enough-0123456789";
    let second_token = "rotated-test-token-that-is-long-enough-0123456789";
    std::fs::write(&token_path, format!("{first_token}\n")).unwrap();

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let app = http_router(
        AgentSession::demo().unwrap(),
        HttpAuth::SecretFile(token_path.clone()),
        Vec::new(),
    );
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

    let initialize = r#"{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-11-25","capabilities":{},"clientInfo":{"name":"test","version":"1"}}}"#;
    let missing = raw_request(addr, None, initialize).await;
    assert!(missing.starts_with("HTTP/1.1 401"), "{missing}");
    assert!(!missing.contains("initialize"));
    let malformed = raw_request(addr, Some("Basic c2VjcmV0"), initialize).await;
    assert!(malformed.starts_with("HTTP/1.1 401"), "{malformed}");
    let wrong = raw_request(addr, Some("Bearer wrong-token"), initialize).await;
    assert!(wrong.starts_with("HTTP/1.1 401"), "{wrong}");
    let hostile_host = raw_request_with_headers(
        addr,
        Some(&format!("Bearer {first_token}")),
        "attacker.example",
        None,
        initialize,
    )
    .await;
    assert!(hostile_host.starts_with("HTTP/1.1 403"), "{hostile_host}");
    let hostile_origin = raw_request_with_headers(
        addr,
        Some(&format!("Bearer {first_token}")),
        &addr.to_string(),
        Some("https://attacker.example"),
        initialize,
    )
    .await;
    assert!(
        hostile_origin.starts_with("HTTP/1.1 403"),
        "{hostile_origin}"
    );
    let oversized_body = "x".repeat(128 * 1024 + 1);
    let oversized = raw_request(
        addr,
        Some(&format!("Bearer {first_token}")),
        &oversized_body,
    )
    .await;
    assert!(
        oversized.starts_with("HTTP/1.1 413"),
        "{}",
        &oversized[..80]
    );

    // Older protocol clients can use the stateless endpoint too. Initialize
    // does not allocate a retained MCP session, and subsequent calls need no
    // session identifier.
    let initialized = raw_request(addr, Some(&format!("Bearer {first_token}")), initialize).await;
    assert!(initialized.starts_with("HTTP/1.1 200"), "{initialized}");
    assert!(!initialized
        .split("\r\n\r\n")
        .next()
        .unwrap()
        .to_ascii_lowercase()
        .contains("mcp-session-id"));
    let legacy_tools = raw_request(
        addr,
        Some(&format!("Bearer {first_token}")),
        r#"{"jsonrpc":"2.0","id":2,"method":"tools/list"}"#,
    )
    .await;
    assert!(legacy_tools.starts_with("HTTP/1.1 200"), "{legacy_tools}");
    assert!(legacy_tools.contains("igloo_query"), "{legacy_tools}");

    // Exercise discovery and query execution through rmcp's actual HTTP client.
    let client = mcp_client(addr, first_token).await;
    let tools = client.list_all_tools().await.unwrap();
    assert_eq!(tools.len(), 5);
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

    // Replacing the secret file revokes the old token without restarting the
    // process. The old value is rejected before MCP dispatch/discovery.
    std::fs::write(&token_path, format!("{second_token}\n")).unwrap();
    let revoked = raw_request(addr, Some(&format!("Bearer {first_token}")), initialize).await;
    assert!(revoked.starts_with("HTTP/1.1 401"), "{revoked}");
    let rotated = mcp_client(addr, second_token).await;
    assert_eq!(rotated.list_all_tools().await.unwrap().len(), 5);
    rotated.cancel().await.unwrap();
    client.cancel().await.unwrap();
    server.abort();
    let _ = std::fs::remove_file(token_path);
}
