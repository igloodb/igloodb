# Igloo delivery plan

Agreed 28 October 2026. This document records the project decisions, execution
sequence and release gates. Dates are planning ranges; completed code, tests and
pilot evidence determine readiness. See [market research](MARKET_ANALYSIS.md),
[current roadmap](../ROADMAP.md) and [architecture](ARCHITECTURE.md).

## Goal, audience and resources

Build an MIT-licensed SQL access and acceleration layer for developers and their
agents. Start with recurring investigations across PostgreSQL databases and
versioned Parquet extracts. Expand from developers with coding agents to agent
application builders, then data platform engineers.

- One full-time maintainer, 30–40 hours/week; working allocation 35 hours/week.
- $100–300/month project cash, excluding personal runway and adopter infrastructure.
- First public experimental release within six months; production target 18–24 months.
- Support local use and authenticated shared deployment for one trusted team.
- Make CDC-backed caching central: invalidation/recomputation and incremental
  maintenance coexist, with explicit coverage and fallback.
- Require PostgreSQL reads to reflect commits completed before each request
  began. Transactions racing a request may be included or excluded.
- Validate each federated PostgreSQL source independently. A vector of source
  positions does not constitute a shared cross-database snapshot.

## Market hypothesis and initial workflow

Agent ubiquity increases automated data access, but does not itself establish a
market for another SQL engine. Direct SQL, DuckDB, database MCP tools, Spice,
ReadySet, PgCache, Materialize, RisingWave, Feldera and existing warehouse tools
are credible alternatives. No unique moat or market size is established.

The initial user has heterogeneous data, recurring costly reads, permission for
logical replication and a preference for self-hosted, inspectable behavior.
Use a single reference workflow: investigate an account/billing discrepancy by
joining orders, account data in another PostgreSQL database and a versioned
Parquet extract; inspect SQL and freshness evidence; repeat after source changes.

Test whether users choose Igloo repeatedly and whether avoided computation
exceeds replication, barrier, maintenance, memory and operational costs. Strict
cache hits still contact the primary. Highly variable SQL and high write rates
can make caching unattractive. Named queries and declared materializations are
important evaluation cases.

## Implementation contract

### Shared execution and interfaces

Keep Rust, DataFusion and Arrow. CLI, MCP and supported pgwire reads must share
planning, policy, freshness and cache execution. Keep exact numeric encodings,
explicit NULLs, typed parameters, output budgets and truncation indicators.
Expose execution strategy, cache age, request ID and independently validated
source positions in a versioned contract. Never invent a global freshness time.

Keep stdio MCP for local use and Streamable HTTP MCP for shared use. Authenticate
every shared request, terminate TLS at a configured reverse proxy, separate
administration credentials, and preserve one deployment-wide read-only source
scope. Distinct permission domains use separate deployments. Pgwire remains
local until it implements the same policy plus authentication/TLS.

### Strict result-cache path

1. Plan a deterministic read and resolve every source/table dependency.
2. Verify source identity, schema, effective privileges, publication coverage
   and unsupported state-dependent/RLS behavior.
3. Emit a transactional WAL marker on every dependent PostgreSQL primary and
   wait until each containing Commit has been processed.
4. Return only an entry matching source identity, generation, dependency
   epochs, schema, policy, typed parameters and result budget.
5. Otherwise execute using query-scoped read-only repeatable-read snapshots per
   native PostgreSQL source. All scans of one source share that query snapshot.
6. Before admitting a fill, process another committed marker after execution
   and reject admission if dependent epochs changed during population.

On unhealthy CDC or unsupported eligibility, perform a fresh source read within
the remaining request deadline. If that cannot succeed, return a structured
error. Never fall back to an old cached answer. TTL controls eviction, not proof
of freshness. Immutable Parquet snapshots carry explicit version identities.
Uncertified ADBC sources bypass this cache.

Use direct pgoutput rather than requiring an external CDC deployment. Temporary
slots and exported snapshots bootstrap derived state. Apply whole transactions
atomically. A logical Message frame or transport heartbeat alone is not a
committed read barrier. Reject incomplete publication coverage; detect schema
and permission changes separately from ordinary row CDC.

### Incremental maintenance

Start with explicit single-table COUNT/SUM materializations. Expand to projections,
deterministic filters, GROUP BY, inner equijoins and COUNT/SUM/AVG/MIN/MAX, including
cross-source maintained joins with separate source frontiers. Use
`differential-dataflow` behind a restricted DataFusion-plan adapter. Do not build
a second general SQL compiler or claim unsupported operators are incremental.

Require primary keys and retained projected row state to reconstruct updates,
deletes and unchanged values. Test NULL, precision, grouping and multiset
semantics. Keep original SQL for equivalent source fallback while materializations
warm, exceed state budgets, or recover. Arbitrary unsupported SQL uses ordinary
execution or invalidation/recomputation.

Derived state is volatile and rebuildable through version 1.0. On stream loss,
slot loss, source identity change or restart, discard affected generations and
bootstrap again; query service uses source execution meanwhile. Durable derived
storage and automatic cluster failover are separate later programs.

### Operational bounds

Enforce source-side deadlines and cancellation, streaming scans, bounded active
queries and queues, byte-accounted cache entries, incremental-state budgets and
spill limits. On exhaustion, use controlled failure/fallback without serving
partially maintained results. Publish the measured workload envelope.

Expose CDC/barrier progress, invalidation, cache reuse, source work, fallback,
memory and recovery metrics. Logs omit credentials, row values and raw SQL by
default. Ship versioned binaries/images with checksums. Certify Linux x86_64,
then macOS ARM64 and Linux ARM64; use WSL2 initially on Windows.

## Milestones and dependencies

| Period | Deliverable | Exit gate |
| --- | --- | --- |
| Weeks 1–2 | Interviews and technical proofs | Ten developer interviews, three real workloads; commit-marker/snapshot/population-race proofs |
| Weeks 3–6 | Release foundation | Preview reconciled, dependency gate green, Rust minimum tested, clean-install demo |
| Months 2–3 | Strict CDC result caching | Native PostgreSQL federation passes freshness, invalidation, race and fallback tests |
| Months 4–6 | Public experimental release | Local/shared MCP, simple incremental aggregates, three pilots, benchmarks and explicit limits |
| Months 7–9 | Incremental execution foundation | Snapshot-to-stream correctness, mutation reconstruction, safe budget fallback |
| Months 10–12 | Wider incremental SQL | Grouped aggregates and inner joins match full recomputation |
| Months 13–15 | Operational hardening | Permissions/schema changes, cancellation, overload, restart and upgrade tests; secure pgwire |
| Months 16–18 | Production candidate | Certified matrix, recovery runbooks, independent review, sustained pilots |
| Months 19–24 | Production qualification | Resolve residual failures and satisfy all production gates before 1.0 |

Use roughly 21 engineering/testing hours, seven user/evaluation hours, four
documentation/example hours and three maintenance/release hours each week.
Working monthly cash allocation: $80 evaluation APIs, $60 test infrastructure,
$20 distribution services and $40 reserve. Adjust within the $100–300 ceiling.
Retain MIT for core capabilities; consider sponsorship/grants after repeated use.

## Tests, adoption and decision gates

Every behavior change receives regression coverage. Preserve formatting, strict
Clippy, rustdoc, MSRV, dependency auditing and real PostgreSQL/ADBC integration
gates. Missing required fixtures must fail configured integration jobs; report
skips separately from executed tests.

Required correctness cases include commits before/during requests, marker and
commit ordering, aborted/long transactions, delayed CDC, cache-fill races,
concurrent misses, randomized row mutations, NULL/decimal boundaries, source
disconnect/restart, slot loss, schema changes, privilege/token revocation, RLS
exclusion, incomplete publications, cancellation, memory/spill exhaustion and
oversized HTTP/data responses. Test actual MCP SDK calls and CLI parity.

Compare results with ordinary DataFusion and native PostgreSQL references when
their semantics match. Benchmark direct SQL, DuckDB and Igloo on identical
datasets/traces. Include cold starts, barriers, WAL, mutations, maintenance,
recovery, source CPU, retries and manual intervention. Before expanding IVM,
require two real workloads to show a twofold latency improvement or 30% source
CPU reduction after accounting for all overhead. These are targets, not claims.

Adoption goals (not forecasts): three workload providers by month two; three
independent pilots repeating use for four weeks by month six; ten repeat-use
teams and two outside contributors by month twelve. Keep telemetry off by
default and obtain feedback from consenting pilots.

Revisit the product direction if replication permissions are unavailable,
queries rarely repeat, barriers erase acceleration benefits, existing tools
solve the workflow more easily, or support exceeds solo capacity.

Production gate: no known supported-subset correctness bugs; automated
concurrency/failure coverage; three independent deployments completing a
30-day candidate period; measured limits; exercised recovery, upgrade, rollback
and credential-rotation runbooks; and independent shared-service review.

After production qualification, certify more connectors and a real Iceberg
reader (snapshot, deletes and schema evolution) before advertising lakehouse
support. OLake remains a complementary ingestion path. Vector storage, durable
agent memory, transactional writes, multi-tenant isolation, high availability
and distributed execution are outside this program.

## Current execution state

The first source-preview increment reconciles the existing agent preview, adds
authenticated HTTP MCP, protects cache admission and byte budgets, and implements
the native CDC library/operator probe. Strict query-cache integration and IVM
are **not implemented**. The upstream compression advisory conflict remains a
release blocker; HTTP/CDC prototypes must not be presented as production-ready.
