# First implementation increment

Verified 28 October 2026 on Rust 1.88.0 / Linux x86_64. The local working branch
was fast-forwarded to agent preview commit `567b2c1` before these additions.
This is an unreleased source preview, not the complete production program.
The changes are published through [draft PR #32](https://github.com/igloodb/igloodb/pull/32);
publication of source does not clear the release gate below.
Implementation and review used four Luna subagents with root integration.

## Implemented

- `mcp-http`: authenticated Streamable HTTP MCP sharing the existing read-only
  agent kernel. Supports environment or rotating secret-file tokens, Host/Origin
  validation, 128 KiB requests and 32 active response streams. Stateless HTTP
  avoids retained per-client MCP sessions. Shared use requires reverse-proxy TLS
  and one trusted-team source scope; stdio MCP remains available.
- Native PostgreSQL `pgoutput` library and `cdc-check` operator command. Tracks
  committed change epochs and marker barriers, invalidates generation on failure,
  bounds waits and pending markers, and cleans up temporary slots. Startup/config
  errors are credential-redacted. Dropping a source invalidates handles immediately.
- Result-cache byte budgets (64 MiB default, configurable), byte counters and
  oversized-result rejection. Generation-fenced fills protect both pgwire query
  protocols from a query repopulating the cache after invalidation.
- Compatible vulnerable dependency updates, direct constant-time token comparison
  dependency, and CI coverage for logical replication and the wire-level race.
- Agreed project plan, refreshed market research, current roadmap and operator docs.

## Verification

The complete suite passed **198 tests**: 160 library tests and 38 integration
tests. PostgreSQL and both ADBC driver paths were enabled, not skipped. Fixtures:
disposable PostgreSQL 15 with `wal_level=logical`, SQLite/PostgreSQL ADBC drivers
1.7.0 and ADBC driver manager 1.13.0.

Commands run with the managed toolchain and disposable fixture settings:

```sh
cargo check --locked --all-targets
cargo test --locked --all-features -- --test-threads=2
cargo clippy --all-targets --all-features --locked -- -D warnings
RUSTDOCFLAGS='-D warnings' cargo doc --no-deps --locked
cargo fmt --all -- --check
git diff --check
```

Checks cover actual HTTP MCP SDK discovery/query, older stateless protocol calls,
missing/incorrect/revoked tokens, hostile Host/Origin, oversized HTTP bodies,
source commits/rollback/truncate, marker ordering and timeout, slot cleanup,
CLI JSON output and credential redaction. The wire race blocks upstream table
scans, invalidates during execution and verifies rejected late fills for simple
and zero-parameter extended queries, followed by successful fresh fills.

## Release gate still failing

`cargo deny --color never --all-features check` passes licenses, bans and sources,
but fails on these existing compression dependencies:

| Locked package | Advisory | Required fixed line |
| --- | --- | --- |
| zstd 0.13.3 | RUSTSEC-2026-0391 | >= 0.14.3 |
| zstd-safe 7.2.1 | RUSTSEC-2026-0403 | >= 8.2.0 |
| zstd-sys 2.0.12 | RUSTSEC-2026-0410 | >= 2.1.2 |

DataFusion 44 / Arrow-Parquet 53 require incompatible older ranges. Inspected
newer published DataFusion graphs did not provide the fixed compression line
while preserving the project's Rust 1.88 minimum. No additional advisory ignores,
codec removal or ad hoc dependency forks were introduced. Keep this increment
unreleased until the dependency gate can be satisfied with a maintained graph.

## Next implementation work

The native CDC foundation is **not connected to query-cache validation**. Agent
queries still bypass cache; pgwire retains its existing TTL/file-event freshness.
Incremental materializations are not implemented. Follow the dependency-ordered
[project plan](PROJECT_PLAN.md) to add publication/source/schema/privilege validation,
query-scoped source snapshots, strict per-source federated cache barriers and
admission, then incremental operators and operational hardening.

The CDC probe's control connection uses `NoTls`, matching the current native
adapter. Encrypted control connections, production source certification,
independent permission domains and complete source cancellation are unfinished.
No production-readiness or comparative performance claims follow from these tests.
