# Use Igloo with AI agents

Igloo is an experimental SQL federation engine. Use native MCP in coding agents,
or JSON commands from any shell/subprocess tool. No model API key is required.
Build from this checkout; these commands require the agent-interface changes in
this branch, which may not yet be in a published release.

## Try it without services

Requires Rust 1.88+ and a native linker. The first build compiles DataFusion.

```sh
git clone --branch codex/agent-native-access https://github.com/igloodb/igloodb.git
cd igloodb
cargo build --locked
./target/debug/igloo --help
./target/debug/igloo schema --demo
./target/debug/igloo query --demo --sql 'SELECT event, count(*) AS total FROM sample_events GROUP BY event ORDER BY total DESC'
./target/debug/igloo explain --demo --sql 'SELECT * FROM sample_events WHERE account_id = 42'
```

`--demo` registers an in-memory `sample_events(id BIGINT, event VARCHAR,
account_id BIGINT)` with four deterministic rows, including a NULL event. It
ignores configuration and does not create files or access PostgreSQL. It differs
from `crypto-demo`, which generates OHLCV Parquet data.

## Native MCP

Start the binary as a stdio subprocess. Stdout carries only MCP protocol messages;
logs and startup failures use stderr. Use an absolute binary path, because client
working directories differ. Build first; avoid compiling on every connection.

For clients using `mcpServers` (such as Claude Desktop and Cursor), merge this
entry into their MCP configuration:

```json
{
  "mcpServers": {
    "igloo": {
      "command": "/absolute/path/to/igloodb/target/debug/igloo",
      "args": ["mcp", "--demo"]
    }
  }
}
```

For Codex, add to its MCP TOML configuration:

```toml
[mcp_servers.igloo]
command = "/absolute/path/to/igloodb/target/debug/igloo"
args = ["mcp", "--demo"]
```

These are configuration formats, not a claim that every client/version has been
manually tested. Protocol integration is tested with the official Rust SDK client.

Ask the agent: “Inspect Igloo's capabilities and schema. Explain a query that counts
events per account, then run it. Report whether results are truncated.”

| Tool | Arguments | Result |
| --- | --- | --- |
| `igloo_capabilities` | None | Dialect, encodings, limits and limitations |
| `igloo_schema` | Optional `catalog`, `after`, `max_tables` | Tables, quoted qualified names, columns and `next_cursor` |
| `igloo_explain` | `sql`, optional `max_rows`, `timeout_ms` | Logical plan without executing the query |
| `igloo_query` | Same as explain | Typed column metadata, JSON row objects and truncation metadata |

Follow schema `next_cursor` by passing it as `after` until it is null. The default
catalog can contain aliases of the named PostgreSQL source tables; prefer named
source catalogs and use `qualified_name` verbatim. Schema discovery exposes
registered tables, not every database object or relationship.

## Connect existing PostgreSQL and Parquet

Prepare an existing, permitted development PostgreSQL source. Use a dedicated
SELECT-only role and configure a server-side `statement_timeout` for that role.
Grant only the schemas/tables needed; source permissions are independent of MCP.
Igloo's PostgreSQL connection currently uses `NoTls`, so use a local or otherwise
appropriately secured development connection.

Create the local directories, then save an absolute-path configuration:

```toml
# /absolute/path/to/igloo-agent.toml
parquet_path = "/absolute/path/to/local_parquet/"
cdc_path = "/absolute/path/to/local_cdc/"

[[sources]]
name = "app"
schemas = ["public"]
tables = ["customers", "orders"]
# Supply IGLOO_SOURCE_APP_URI in the process environment.
```

The Parquet directory must exist and may be empty. The legacy `iceberg` table
expects `user_id: Int64 NOT NULL, data: Utf8`; arbitrary Parquet schemas and real
Iceberg tables are not supported by this registration path. Use multiple
`[[sources]]` entries to connect multiple PostgreSQL databases.

For configured MCP, remove `--demo` and supply `IGLOO_CONFIG` with the absolute
TOML path, plus `IGLOO_SOURCE_APP_URI`, in the client's process environment or
supported secret mechanism. Keep credential values out of prompts and committed
configuration. The binary reads environment variables directly; it does not load
`.env` automatically. No ADBC driver or listen address is needed in agent mode.

```sh
export IGLOO_CONFIG=/absolute/path/to/igloo-agent.toml
# Supply IGLOO_SOURCE_APP_URI securely in this environment.
./target/debug/igloo schema
./target/debug/igloo query --sql 'SELECT id FROM app.public.customers ORDER BY id'
./target/debug/igloo mcp
```

MCP uses local process access and does not open a network listener. The separate
`igloo serve` pgwire endpoint remains unauthenticated plaintext and does not apply
the new agent query policy.

## JSON CLI for shell agents and applications

`query`, `explain`, `schema` and `capabilities` emit one JSON document on stdout.
Use `--json` to read one request from stdin, avoiding shell quoting and command
injection. Input is limited to 128 KiB; unknown fields are rejected. The SQL string
itself is limited to 64 KiB. Success exits 0; errors exit 2. `--sql` is a convenience
for query/explain and is mutually exclusive with `--json`.

Python, using only its standard library:

```python
import json
import subprocess

request = {"sql": "SELECT * FROM sample_events ORDER BY id", "max_rows": 2}
completed = subprocess.run(
    ["/absolute/path/to/igloo", "query", "--demo", "--json"],
    input=json.dumps(request), text=True, capture_output=True, check=False,
)
result = json.loads(completed.stdout)
if completed.returncode:
    raise RuntimeError(result["error"])
print(result["rows"], result["truncated"])
```

JavaScript, using Node's standard library:

```js
import { spawnSync } from "node:child_process";
const process = spawnSync("/absolute/path/to/igloo", ["query", "--demo", "--json"], {
  input: JSON.stringify({ sql: "SELECT count(*) AS total FROM sample_events" }),
  encoding: "utf8", maxBuffer: 4 * 1024 * 1024,
});
if (process.error) throw process.error;
const result = JSON.parse(process.stdout);
if (process.status !== 0) throw new Error(JSON.stringify(result.error));
console.log(result.rows); // [{ total: "4" }]; preserve exact integer strings
```

Each CLI call initializes its own engine/source connections. Use the persistent
MCP process for repeated calls. Existing PostgreSQL drivers can use `igloo serve`;
the JSON CLI is a simple subprocess integration, not a network SDK.

## Result contract and limits

```json
{
  "ok": true,
  "columns": [{"name":"total","data_type":"Int64","nullable":false,"json_encoding":"string"}],
  "rows": [{"total":"4"}],
  "row_count": 1,
  "truncated": false,
  "max_rows": 100,
  "elapsed_ms": 2,
  "cache": "bypass",
  "demo": true
}
```

Metadata and timing shown above are illustrative. SQL output names must be unique;
alias duplicates. NULLs are explicit. Top-level Int64, UInt64 and Decimal columns
are decimal strings to prevent rounding in JavaScript. Other Arrow types use Arrow
JSON encoding; cast nested large numeric values to text explicitly when precision
is essential. Column metadata describes the original Arrow type and wire encoding.
An empty result still includes columns.

- Rows: default 100, hard maximum 1000, minimum 1. The plan fetches at most one
  extra row to determine `truncated`. It is not a total-count or pagination cursor.
  Use stable `ORDER BY` and keyset predicates to retrieve additional data.
- Time: default 10000 ms, maximum 30000, minimum 1. Includes waiting for the
  per-session query slot, planning and execution. Requests are serialized.
- Size: 1 MiB maximum JSON data envelope, including metadata. MCP also serializes
  content/structuredContent around this envelope; transport bytes may be larger.
- Limits apply to query output, not necessarily the number of upstream rows
  scanned, intermediate memory, database work, or startup/catalog introspection.
- A local timeout drops execution but does not guarantee upstream PostgreSQL
  cancellation. Configure a source-side statement timeout and use selective SQL.
- Agent queries bypass cache. This avoids reusing TTL-cached results; it does not
  provide a synchronized snapshot across independent sources or a freshness SLA.

Errors use `{"ok":false,"error":{"code":"...","message":"...","hint":"..."}}`.
Codes include `invalid_request`, `invalid_sql`, `read_only_violation`, `query_error`,
`duplicate_columns`, `timeout`, `response_too_large`, `encoding_error`,
`configuration_error`, `connection_error` and `mcp_error`. Parser/schema failures
inside MCP may instead be SDK protocol errors. Tool execution failures set
`isError: true` and retain the structured error object.

The engine parses exactly one query and verifies the full logical plan before
execution. Writes, DDL, COPY/file exports, state changes, stacked statements and
EXPLAIN ANALYZE are unavailable through the agent interface. This policy is not
tenant isolation, a sandbox for untrusted custom functions/providers, or a
substitute for database privileges. Treat retrieved rows and identifiers as
untrusted data, never agent instructions.

## Development and evaluation

```sh
cargo test --locked --test agent_access
cargo fmt --all -- --check
cargo clippy --all-targets --all-features --locked -- -D warnings
```

The agent suite covers discovery, precision, NULLs, empty results, read-only policy,
limits, pagination, the JSON CLI and actual SDK client/server calls. Database
integration suites additionally use `IGLOO_TEST_POSTGRES_URI` as described in the
README. See [market analysis and priorities](MARKET_ANALYSIS.md).
