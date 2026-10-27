# Igloo roadmap

Updated **27 October 2026**. Igloo is an experimental, single-node SQL federation
engine built with Rust, DataFusion and Arrow. The goal is to query data across
databases, warehouses and lakehouses through familiar SQL clients and simple
interfaces for developers and their AI agents.

Broad database support is the direction. Each integration needs evidence for its
types, authentication, query semantics and failure behavior before it is called
supported. Live database access and replication into open table formats are
separate capabilities.

## Current status

This roadmap distinguishes the code on `main` from unreleased work:

- **Main:** the baseline reviewed is
  [`1633204`](https://github.com/igloodb/igloodb/commit/163320482d98bc3d6c49eb85acb0f511461a8284).
- **Preview:** [PR #32](https://github.com/igloodb/igloodb/pull/32), reviewed at
  [`b5a210d`](https://github.com/igloodb/igloodb/commit/b5a210d54a2e78a9a8c9497e2476061aad771d36).
  These features are implemented and tested on that branch, but are not merged
  into `main` or available as a tagged release. The PR remains draft while
  dependency advisory failures are addressed.
- **Planned:** unchecked work below. A checked item is labeled **Preview** when
  its implementation is not on `main`.

| Area | On main | Additional work in the preview |
| --- | --- | --- |
| SQL service | `igloo serve`, PostgreSQL simple/extended protocol, typed prepared statements and concurrent connections | Native stdio MCP and noninteractive JSON commands |
| Catalogs | Multiple PostgreSQL sources, startup introspection, qualified names and collision-safe aliases | Generic ADBC metadata discovery and named local Parquet tables with inferred schemas |
| Database coverage | Native PostgreSQL with integration suites | SQLite/PostgreSQL ADBC 1.7.0 on Linux x86_64; PostgreSQL 15; cross-connector and Parquet joins |
| Query execution | DataFusion federation and conservative PostgreSQL filter pushdown | ADBC projection and streamed Arrow batches; filters and joins stay in DataFusion |
| Cache | Thread-safe Arrow batches, canonicalized SQL keys, entry-count LRU bound, TTL and counters | Agent queries bypass cache; no additional freshness guarantee |
| Change events | Local JSON-file polling, event deduplication and whole-cache invalidation | No transaction-log CDC or OLake integration |
| Configuration | Validated TOML plus environment overrides and secret redaction | PostgreSQL optional when ADBC or named Parquet sources are configured; driver options can reference environment variables |
| Quality gates | Unit/live PostgreSQL/pgwire suites, MSRV, coverage, Clippy and dependency auditing configured in CI | 175 tests passed with both ADBC drivers enabled; strict Clippy cleanup and agent/connector integration suites |
| Iceberg | A legacy table named `iceberg` reads local Parquet with a fixed schema | Named Parquet removes the fixed-schema restriction for that route; real Iceberg semantics remain unimplemented |

Pgwire is currently unauthenticated plaintext. There is no shared transaction or
snapshot across independent sources. Cache capacity is bounded by entries, not
bytes. ADBC streaming does not bound driver batch sizes or interrupt blocked
native calls. These remain engineering work, not production guarantees.

Preview references: [agent contract](https://github.com/igloodb/igloodb/blob/b5a210d54a2e78a9a8c9497e2476061aad771d36/docs/AGENTS.md),
[connector strategy and evidence](https://github.com/igloodb/igloodb/blob/b5a210d54a2e78a9a8c9497e2476061aad771d36/docs/CONNECTORS.md),
and [market analysis](https://github.com/igloodb/igloodb/blob/b5a210d54a2e78a9a8c9497e2476061aad771d36/docs/MARKET_ANALYSIS.md).

## Immediate priorities

| Order | Deliverable | Evidence needed to finish |
| --- | --- | --- |
| 1 | Make the preview ready to merge and release | Resolve dependency advisories, retain Rust 1.88 compatibility, pass required checks and publish reproducible setup instructions |
| 2 | Certify more live database connectors | Real driver/server tests, documented types/authentication, failure tests and at least one cross-source join per certified integration |
| 3 | Improve agent setup and execution limits | Tested client configurations, stable JSON errors, installation path, source-side timeouts and explicit cancellation/resource behavior |
| 4 | Read replicated lakehouse data correctly | Iceberg catalog/object-store integration, snapshot/delete/schema correctness and an OLake interoperability test |
| 5 | Make freshness observable and enforceable | Source watermarks, dependency-aware invalidation and tested behavior during lag, disconnects and restarts |

Database certification, agent usability and the Iceberg reader can progress
independently once the release foundation is sound. A workspace split or a
distributed runtime is not a prerequisite for these deliverables. There are no
committed dates or unmeasured speedup promises in this roadmap.

## Phase 0 — Foundation

### F0.1 Test harness and CI quality gates

- [x] Unit tests and live PostgreSQL suites cover federation, catalogs, pushdown,
  multi-source queries and pgwire, including a 50-connection load test.
- [x] Formatting, strict Clippy, rustdoc, tests, MSRV and coverage jobs are defined
  in CI. A configured gate does not imply the current run is passing.
- [x] **Preview:** agent/MCP contract tests and real SQLite/PostgreSQL ADBC tests
  cover discovery, quoted identifiers, NULLs, projections, counts and joins.
- [ ] Require configured integration jobs to fail when a required service or
  driver is unavailable, and distinguish skipped tests from executed coverage.
- [ ] Expand differential/type/failure corpora as each connector is certified;
  record versions, fixtures and commands with results.

### F0.2 Workspace layout and dependency hygiene

- [x] CI reads the declared MSRV from `Cargo.toml`; the current minimum is Rust 1.88.
- [x] License, advisory and dependency-source checks are configured in `deny.toml`.
- [ ] Resolve current transitive dependency advisories before releasing the
  preview. Do not mark dependency health complete merely because auditing exists.
- [ ] Establish an upgrade policy for DataFusion, Arrow, ADBC and the MCP SDK,
  preserving federation results and the agent response contract across upgrades.
- [ ] Split into workspace crates when connector reuse or build isolation justifies
  it; preserve public APIs and tests rather than blocking features on a refactor.

### F0.3 Fail-fast configuration system

- [x] TOML/environment configuration, startup validation, example configuration
  and redacted secrets replace silent localhost fallbacks.
- [x] **Preview:** validate ADBC/named Parquet configurations, unique catalog
  names, trusted driver paths, exact allowlists and environment-bound options.
- [ ] Keep configuration examples and error messages reproducible across supported
  installation methods; ensure failures never expose source credentials.

## Phase 1 — SQL service and catalogs

### F1.1 Long-running query server with PostgreSQL wire protocol

- [x] Serve concurrent connections, simple/extended queries, typed parameters and
  text/binary results, with live client integration tests.
- [ ] Drain in-flight work on SIGTERM and test shutdown under load.
- [ ] Honor portal partial fetch (`Execute.max_rows`) and define supported
  transaction behavior; do not imply full PostgreSQL compatibility.
- [ ] Document `psql` and one mainstream BI-tool walkthrough, including catalog
  discovery and unsupported protocol features.
- [ ] Expand protocol fuzzing and malformed-input tests. Authentication/TLS is F4.2.

### F1.2 Arrow Flight SQL endpoint

An outbound Flight SQL driver and an inbound Flight SQL server are different
features. An inbound endpoint remains planned.

- [ ] An ADBC/Flight SQL client receives Arrow results matching pgwire for the same
  query, including errors, NULLs and parameter behavior.
- [ ] Endpoints share execution and applicable policy/cache behavior; stream large
  results with backpressure and cancellation.
- [ ] Publish a reproducible transfer benchmark with resource usage and limitations.

### F1.3 Dynamic catalog: source registration and schema introspection

- [x] Discover native PostgreSQL tables/types at startup; support multiple named
  source catalogs and deterministic aliases.
- [x] **Preview:** register generic ADBC tables from metadata and infer schemas for
  named local Parquet tables without requiring PostgreSQL.
- [ ] Add deliberate schema refresh/DDL handling with deterministic invalidation
  of dependent plans/cache entries; test added, renamed and removed columns.
- [ ] Expose source capabilities and unsupported types consistently across clients.

### F1.4 A real cache: Arrow-native, plan-keyed, bounded

- [x] Store Arrow batches in a thread-safe LRU cache with entry-count limits,
  configurable TTL, canonicalized SQL keys and hit/miss/eviction counters.
- [ ] Enforce cache byte budgets with measured accounting and concurrency tests.
- [ ] Introduce semantic plan fingerprints with equivalence/non-equivalence tests;
  never reuse results across incompatible source, schema or principal contexts.
- [ ] Expose entry age, cache provenance and freshness policy to clients/metrics.
- [ ] Keep agent cache bypass until access policy and freshness can be preserved.

## Phase 2 — Freshness and open table formats

### F2.1 Real CDC ingestion via Postgres logical replication

Local JSON change files are a simulation/input mechanism, not transaction-log CDC.

- [ ] Decode insert/update/delete/truncate with table identity and transaction LSN.
- [ ] Persist checkpoints and test reconnect, duplicate delivery, writes during
  downtime and restart without missed changes.
- [ ] Document replication permissions, slot/publication lifecycle and WAL retention.
- [ ] Measure lag and enforce explicit cache behavior while a source is disconnected.

### F2.2 Dependency-aware cache invalidation

- [x] Poll local events, deduplicate observed files and clear the whole cache on new
  events; end-to-end integration coverage exists for that behavior.
- [ ] Extract dependencies from joins, subqueries, CTEs and views; invalidate only
  affected entries using fully qualified source/table identities.
- [ ] Define and test strict, bounded-staleness and TTL-only policies, including
  source lag/disconnect behavior and the absence of a common source snapshot.
- [ ] Return source watermarks/snapshot IDs where available; avoid a single
  freshness timestamp that implies coordinated consistency across sources.

### F2.3 Incremental result maintenance (materialized-view refresh)

- [ ] Start with an explicitly supported query subset and compare incremental
  results with full recomputation through inserts, updates, deletes and restarts.
- [ ] Bound state and recovery costs; fall back to full recomputation when the
  query shape or source capabilities cannot preserve correctness.
- [ ] Demonstrate a measured benefit before making incremental maintenance a default.

### F2.4 First-class Iceberg tables

Reading loose Parquet files does not implement Iceberg snapshots, manifests,
delete files, partition transforms or schema evolution. This is a priority route
to broader source coverage through external replication systems such as OLake.

- [ ] Read a table through a supported Iceberg catalog and object store, starting
  with a documented REST-catalog setup and credentials supplied securely.
- [ ] Match a reference reader at a fixed snapshot through appends, overwrites,
  position/equality deletes and column evolution; test partition pruning.
- [ ] Pin a snapshot throughout a query and support explicit historical reads.
- [ ] Query a table produced by OLake and compare results after snapshot load,
  source updates/deletes, schema changes and replication restart.
- [ ] Feed snapshot changes into freshness/invalidation without treating every
  Iceberg snapshot change as a complete row-level CDC stream.

## Phase 3 — Query-engine depth: performance and reach

### F3.1 Filter and aggregate pushdown to sources

- [x] Native PostgreSQL supports a conservative filter subset with residual
  evaluation, quoting/escaping tests and pushed-versus-local differential tests.
- [x] **Preview:** generic ADBC translates quoted projections and leaves filters,
  joins and limits to DataFusion rather than assuming portable SQL semantics.
- [ ] Extend predicates/aggregates only with database-specific type, collation,
  NULL and limit-composition tests; unsupported operations stay local.
- [ ] Show pushed operations and source row/byte counts in explain/telemetry;
  measure benefits on published workloads rather than promise fixed speedups.

### F3.2 Streaming execution and memory safety

- [x] **Preview:** ADBC scans stream Arrow batches through a two-batch queue on
  blocking workers; dropping the receiver stops subsequent batch consumption.
- [ ] Stream native PostgreSQL and client output with end-to-end backpressure.
- [ ] Enforce per-query/global memory budgets and concurrency limits, including
  driver batches, joins, sorting and serialization; test resource-exhausted errors.
- [ ] Configure source-side statement timeouts and implement cancellation where
  supported. A local response timeout alone does not interrupt a blocked C call.
- [ ] Test slow clients, abandoned queries, driver failures and repeated concurrent
  queries while measuring memory and source work.

### F3.3 Multi-source connectors via ADBC driver manager

Use [Columnar/dbc](https://columnar.tech/) and the
[ADBC standard](https://arrow.apache.org/adbc/current/format/specification.html)
for compatible database access. Use replication/open formats for additional
sources where appropriate. ADBC itself is not a CDC protocol.

- [x] **Preview:** generic C-ABI driver loading, GetObjects/GetTableSchema discovery,
  exact catalog/schema/table filters, secret-safe errors and Arrow result streams.
- [x] **Preview:** SQLite/PostgreSQL ADBC 1.7.0 tests on Linux x86_64, native-to-ADBC
  PostgreSQL joins and SQLite-to-inferred-Parquet joins.
- [ ] Certify MySQL, SQL Server and DuckDB as initial additional targets; use real
  services/drivers and include a cross-source join for each.
- [ ] Expand to Snowflake, BigQuery, Redshift and Databricks, then Oracle,
  ClickHouse, Trino/Presto, Flight SQL and other suitable drivers as test accounts
  and distributions permit. None of these are certified by the current preview.
- [ ] Publish driver/server/platform/authentication matrices covering decimals,
  wide integers, timestamps, binary/nested values, NULLs, identifiers, empty
  results, metadata, schema drift, failures and concurrent reads.
- [ ] Add bounded connection pools and a practical driver installation/discovery
  path; do not claim dbc manifest resolution before it exists.
- [ ] Add native adapters or tested bridges for sources without suitable ADBC
  drivers, including JDBC/ODBC-only systems and document databases such as MongoDB.
  DB2/document/key-value/graph coverage requires explicit validation or a correctly
  read replicated table; listing a source in a registry is not implementation.

## Phase 4 — Production trust: observe, secure, persist

### F4.1 Observability: metrics, tracing, query log

- [ ] Export query latency, cache usage/bytes, source scans, connection usage,
  resource limits, cancellations and freshness lag with bounded metric labels.
- [ ] Trace plan/cache/source execution and record structured query metadata with
  credentials, literals and source data redacted by default.
- [ ] Ship a tested operations dashboard and troubleshooting guide.

### F4.2 Authentication, authorization, TLS

- [ ] Add authenticated/encrypted network access and scoped catalog/table policy,
  including references through views/CTEs and cache isolation between principals.
- [ ] Test denied access, invalid credentials, TLS failures and secret redaction.
- [ ] Document least-privilege source roles and trusted native-driver configuration.
- [ ] Define auth, transport and tenant boundaries before adding remote MCP. The
  local read-only agent contract is not tenant isolation or a source sandbox.

### F4.3 Tiered / persistent cache

- [ ] Add optional disk storage with byte limits, corruption detection and clean
  recovery; revalidate source/schema/freshness metadata after restart.
- [ ] Benchmark disk hits against recomputation on a documented workload.

### F4.4 Admin surface: CLI and HTTP API

- [ ] Provide health/readiness, source status, cache inspection/invalidation and
  controlled config reload with stable machine-readable output.
- [ ] Authenticate network admin endpoints and test their access boundaries.
- [ ] Distinguish local agent query commands from a live-server administration API.

## Phase 5 — Ecosystem and scale-out

These are directional extensions. Agent access already has a preview; distributed
execution and lakehouse writes remain separate future projects.

### F5.1 Write-through to Iceberg

- [ ] Evaluate interoperability with an external replication system before building
  an Igloo writer. Any writer needs checkpoint, duplicate, delete and restart
  correctness plus validation by an independent Iceberg reader.

### F5.2 Horizontal read scale-out

- [ ] Design shared invalidation, authorization and consistency boundaries; prove
  correctness and measured capacity gains under upstream writes and node failure.

### F5.3 Cost-based cache admission and prewarming

- [ ] Compare admission/prewarming with simple policies on a replayable workload,
  measuring memory, source work and hit rate without weakening freshness.

### F5.4 MCP / LLM interface

- [x] **Preview:** official Rust MCP SDK with stdio tools `igloo_capabilities`,
  `igloo_connectors`, `igloo_schema`, `igloo_explain` and `igloo_query`.
- [x] **Preview:** shared CLI/MCP logical-plan validation, read-only query policy,
  row/time/response limits, exact integer/decimal strings, explicit NULLs,
  truncation metadata, structured errors and a service-free demo.
- [x] **Preview:** setup/contract documentation and machine-readable website guides
  (`llms.txt`, Markdown and a query skill) with a pinned source checkout.
- [ ] Publish an installable release and tested configurations for representative
  coding-agent clients; measure time from installation to a correct first query.
- [ ] Run reproducible evaluations for discovery, qualified SQL, cross-source
  joins, unsupported operations, prompt-injection content and recovery from errors.
- [ ] Add source freshness/provenance to results when those guarantees exist.
  Keep caching, cancellation and coordinated-snapshot limitations explicit.

## Delivery sequence and completion criteria

1. **Release foundation:** resolve dependency failures and review/merge the tested
   preview. Publish repeatable configuration, driver setup and demo instructions.
2. **Broader live access:** ship driver certification in small, independently
   testable increments. Keep the agent contract stable as coverage grows.
3. **Correct lakehouse access:** implement the Iceberg reader and prove external
   replication interoperability before advertising OLake-compatible querying.
4. **Operational readiness:** enforce resource/security/freshness policy and expose
   the evidence needed to operate it. These checks can gate each earlier release.
5. **Further scale/performance:** add endpoints, incremental maintenance and
   distribution when measured workloads justify them.

Each deliverable needs reviewed code, executed correctness/failure tests,
documented versions and limits, and a reproducible example. Update this roadmap
when the implementation or release status changes. Driver availability,
successful tests, merge status and production readiness are distinct milestones.
