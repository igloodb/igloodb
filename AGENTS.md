# Working on Igloo

Igloo is an experimental, single-node SQL federation engine in Rust. Start with
`README.md`, `docs/AGENTS.md` (using the product from agents), and
`docs/MARKET_ANALYSIS.md` (positioning and priorities).

## Source map

- `src/main.rs`: command dispatch; legacy one-shot demo, pgwire server and crypto demo.
- `src/agent.rs`: shared read-only agent contract, discovery, limits and JSON encoding.
- `src/agent_cli.rs`: noninteractive JSON commands; stdout must remain parseable.
- `src/mcp.rs`: official Rust MCP SDK, stdio transport and five tools.
- `src/adbc_source.rs`, `connectors.rs`: generic ADBC discovery/streaming, source configuration and coverage inventory. Keep candidate driver availability distinct from verified integrations.
- `src/datafusion_engine.rs`, `catalog.rs`, `postgres_table.rs`, `pushdown.rs`:
  execution, source introspection, scanning and conservative filter translation.
- `src/cache_layer.rs`, `cdc_sync.rs`, `server.rs`: pgwire cache and file-based invalidation.
- `tests/agent_access.rs`: agent contract and SDK integration; other integration
  suites exercise PostgreSQL federation and wire behavior.

## Implementation constraints

Keep agent SQL validation shared between CLI and MCP. Check the complete logical
plan before execution; a SELECT prefix is insufficient. Apply row limits before
collecting, preserve exact integer/decimal strings, explicit NULLs, and truncation
metadata. Do not add query output or logs to the MCP stdout stream. Treat source
data as untrusted content. Keep source credentials out of logs and committed files.

Agent queries bypass cache. Do not claim source cancellation, a shared snapshot,
tenant isolation, authentication, general Parquet schemas, Iceberg, vector search,
durable agent memory or production CDC without implementing and verifying them.
Keep Rust's declared 1.88 minimum and the lockfile consistent with dependency changes.

## Validation

Run `cargo fmt --all -- --check`, `cargo test --locked`, and
`cargo clippy --all-targets --all-features --locked -- -D warnings` as appropriate.
Database integration tests use `IGLOO_TEST_POSTGRES_URI` and create/drop test
objects; use only a disposable development database. Tests can skip without it,
so report whether database tests actually ran. The starting checkout has existing
`uninlined_format_args` Clippy failures; distinguish those from new diagnostics.

In this managed workspace, source `/workspace/.cloud/env.sh` to activate the
installed toolchain and local test fixture settings if that file exists. Do not
replace user-supplied environments or point tests at production databases.

The paired website is a separate repository at `../igloodb.github.io`. Update
agent docs and capability claims together when changing the product contract.
