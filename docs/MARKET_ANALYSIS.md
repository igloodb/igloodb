# Igloo: market analysis and agent product strategy

Research date: **27 October 2026**. Primary product documentation and the local
engine source were reviewed. Vendor capabilities are vendor statements, not
independently benchmarked results. Recommendations and commercial hypotheses are
identified below. This is a product and developer-experience assessment; it does
not estimate market size, adoption, revenue, or comparative performance.

## Recommendation

Position Igloo as **a common data access and SQL federation layer for developers
and AI agents across databases, warehouses and lakehouses**. Broad connector
coverage is a product goal. Use native adapters and compatible ADBC drivers for
live access, open table formats for replicated/snapshot access, and additional
bridges or native adapters for systems those routes cannot reach. Preserve one
SQL, catalog, type and agent contract across these routes.

Do not initially expand into vector storage, persistent agent memory, a managed
transactional database, or an LLM platform. Those jobs have strong alternatives
and require capabilities Igloo does not possess. MCP is an integration requirement,
not a durable competitive advantage. Federation itself is also established.
Igloo needs to earn preference through measurable setup simplicity, correct
answers, predictable access, and transparent execution. See the
[connector strategy and implemented foundation](CONNECTORS.md) for the
OLake/Columnar comparison, coverage evidence and expansion sequence. “All
databases” is the intended direction, not a certification claim.

## Demand and market structure

The 2025 Stack Overflow survey reports that 46% of respondents distrust AI tool
accuracy versus 33% who trust it; 69% of agent users report improved productivity.
These are self-reported survey results, not database purchasing intent or evidence
of demand for Igloo. They support testing a workflow where developers can inspect
schemas, SQL, plans, and results. [Survey: AI](https://survey.stackoverflow.co/2025/ai).

There are four distinct buying jobs behind the broad label “AI database”:

| Job | Typical data and action | Relevant alternatives | Igloo fit |
| --- | --- | --- | --- |
| Build and change applications | Transactional state, migrations, branches | Supabase, Neon, Turso | Low: Igloo is not the system of record |
| Retrieve context and memories | Embeddings, documents, similarity retrieval | LanceDB, Chroma, Qdrant | Low today: no native retrieval or memory lifecycle |
| Answer operational questions | SQL joins and aggregates over business sources | Direct PostgreSQL, DuckDB, Spice, Tinybird | Best initial hypothesis |
| Enterprise federation | Many connectors, distributed execution, governance | Trino, broader data platforms | Architectural overlap; maturity gap |
| Standard database access | Arrow-native connections and metadata across database families | Columnar / ADBC / dbc | Reuse compatible drivers as the main coverage multiplier |
| Replication into open formats | Snapshots, source changes, deletes and recovery | OLake and other ingestion systems | Separate integration path; querying data does not implement CDC |

The meaningful substitute is often a developer giving an agent `psql`, Python,
DuckDB, and two connection strings. Any proposed Igloo workflow must beat that
baseline in integration effort or repeatability, rather than compare only with
large commercial platforms.

## Competitive landscape

| Alternative | Verified offering | Reason to choose it | Implication for Igloo |
| --- | --- | --- | --- |
| **Spice.ai** | SQL federation/acceleration positioning and configured tools served over Streamable HTTP MCP | Broad data and AI runtime requirements | Closest strategic overlap. A Rust/Arrow/MCP story alone is not differentiation. Win a deliberately smaller workflow or acknowledge the better fit. |
| **Columnar / dbc** | Arrow/ADBC access and driver installation across database families | Applications needing a standard connectivity interface | Build on compatible drivers; validate real database semantics instead of equating installation with support. |
| **OLake** | Database replication into Iceberg and Parquet, plus table maintenance products | Teams moving operational data into a lakehouse | Complement live federation with correctly read replicated tables; do not treat raw Iceberg files as a complete table. |
| **DuckDB + community MCP** | Local analytical SQL with a community extension exposing tables, queries, and tools through MCP | File analytics and existing DuckDB projects | Strong default baseline. The MCP extension is community maintained; do not present it as an official DuckDB server. Demonstrate easier PostgreSQL source setup and reproducible cross-source workflows. |
| **Tinybird** | Remote MCP for Workspace SQL and published API Endpoints, with token-scoped access | Operational analytics APIs and deployed analytics tools | Validates reusable query tools and scoped access. Igloo's local process avoids requiring a hosted Workspace but lacks equivalent operational maturity. |
| **Supabase** | MCP for project interaction, including read-only and project/feature scoping options | Teams already building on Supabase | Direct platform tools can make an intermediate engine unnecessary. Integrate existing Postgres rather than claim to replace their application stack. |
| **Neon** | MCP and typed agent tools for SQL, migrations, diagnostics and platform management | Agent-assisted Postgres development | Schema/SQL tooling is becoming standard. Igloo needs to justify a cross-source requirement, not merely expose a SQL tool. |
| **Turso** | Persistent, isolated databases per agent, branching and vector capabilities | Per-agent writable state and memory | A different problem. Do not claim Igloo supports per-agent persistence, rollback or database isolation. |
| **LanceDB** | Multimodal tables for curation, search, retrieval, features and training, with agent tooling documented | Multimodal AI and retrieval workflows | An adjacent data layer, not the initial replacement target. Avoid adding embeddings solely to match AI branding. |
| **Chroma / Qdrant** | Chroma documents embedding/document retrieval; Qdrant maintains an official MCP server | Semantic retrieval and agent memory implementations | Retrieval products compete on different semantics. Igloo can supply structured SQL context alongside them. |
| **Trino** | Distributed SQL execution across catalog-configured sources | Broad enterprise federation and distributed workloads | Igloo's single process may simplify local deployment, but offers neither equivalent connector coverage nor distributed execution. |

Primary sources supporting the matrix, checked on the research date:

- [Spice MCP API](https://spice.ai/docs/cloud/api/mcp) and [Spice API overview](https://spiceai.org/docs/api/overview).
- [DuckDB community MCP extension](https://duckdb.org/community_extensions/extensions/duckdb_mcp).
- [Tinybird MCP](https://www.tinybird.co/docs/forward/query-data/mcp).
- [Supabase MCP](https://supabase.com/docs/guides/ai-tools/mcp).
- [Neon typed agent tools](https://neon.com/blog/give-your-agent-neon-tools).
- [Turso for AI agents](https://turso.tech/solutions/ai-agents).
- [LanceDB documentation](https://docs.lancedb.com/).
- [Chroma documentation](https://docs.trychroma.com/) and [Qdrant MCP source](https://github.com/qdrant/mcp-server-qdrant).
- [Trino architecture and catalogs](https://trino.io/docs/current/overview/concepts.html).
- [Columnar](https://columnar.tech/), [dbc documentation](https://docs.columnar.tech/dbc/), and [OLake](https://olake.io/).

No inference about competitor cost, latency, security quality, market share or
adoption follows from this table. Hosted and open-source offerings differ; validate
the particular deployment and current pricing when evaluating a purchase.

## Initial users and concrete workflows

**Primary hypothesis: developers building agents over a heterogeneous data stack.**
The user has two or more relevant databases, warehouses or snapshots, permission to read selected
tables, and a coding agent that can run local tools. The buyer, if one emerges,
is likely the engineering lead responsible for integration and operation. This
buyer description needs customer validation.

Three useful pilot tasks:

1. Join application orders and CRM accounts to explain a change in customer spend.
2. Combine a local Parquet extract with PostgreSQL reference data for an incident
   investigation. Today the legacy Parquet table has a fixed `user_id, data`
   schema; this is a constraint, not general arbitrary-file support.
3. Let a coding agent inspect schemas, propose SQL, inspect the logical plan, and
   return a bounded answer with enough detail for a developer to reproduce it.

Avoid selling to teams that need hosted public APIs, audited tenant isolation,
distributed execution, arbitrary lakehouse connectivity, transactional writes, or
embeddings as a prerequisite. Igloo is experimental and lacks those capabilities.

## What “AI native” must mean in this product

It should describe executable contracts rather than a chat box:

| Need | Product response | Evidence to collect |
| --- | --- | --- |
| Discover before generating SQL | Typed tables/columns and correctly quoted qualified names | Fewer nonexistent-column/table retries |
| Work with coding agents and scripts | Native MCP plus JSON stdin/stdout CLI | Successful first call from a fresh checkout and an SDK client |
| Inspect execution | Logical-plan tool separate from query execution | Developer can explain where a join/filter runs |
| Bound response size and time | Row limit in the plan, response byte cap, timeout, truncation flag | No oversized context dumps; correct truncation behavior |
| Repair failures | Stable error codes and concrete hints | Agent recovers from a bad identifier without a human edit |
| Preserve data meaning | Column types/nullability; exact integer/decimal strings | No identifier rounding in JavaScript |
| Restrict operations | One query, AST check and complete logical-plan policy | Writes, file exports and state changes fail before execution |
| Know freshness semantics | Cache bypass explicit in agent results | No implication of CDC consistency or synchronized source snapshots |
| Learn without rendering the site | `/llms.txt`, Markdown agent guide, copyable setup | Agent can integrate using only text documentation |

The [MCP tool specification](https://modelcontextprotocol.io/specification/2026-07-28/server/tools)
defines discoverable tool schemas and structured results. The official
[Rust SDK](https://github.com/modelcontextprotocol/rust-sdk) supplies lifecycle and
transport behavior. Tool annotations describe intent; they do not enforce SQL
permissions. Igloo must enforce its own query policy and use restricted source
roles. `llms.txt` is a discovery convention, not access control or proof of agent
compatibility.

## Audit of the starting product

Useful foundations verified in source: DataFusion planning/execution, Arrow
results, PostgreSQL catalog introspection, qualified multi-source names, filter
pushdown, pgwire simple/extended queries, in-memory LRU/TTL caching, JSON-file
cache invalidation, and an isolated crypto analytics demo.

The main developer-experience gaps were a hard-coded default query, no arbitrary
JSON query command, no native agent protocol, no typed agent discovery contract,
and onboarding oriented around database/ADBC configuration. PostgreSQL-specific
registration and the fixed legacy Parquet schema also constrained coverage. The website accurately
described many limitations but offered no agent-specific path.

Do not inherit unsupported claims from early project descriptions: the `iceberg`
table is a local directory of Parquet files, ADBC is a separate demonstration
path, MySQL/SQL Server are not working connectors, and JSON-file invalidation is
not a production CDC integration. Pgwire remains unauthenticated plaintext.

## Changes delivered in this iteration

- Native stdio MCP through the official Rust SDK: capabilities, paginated schema,
  logical explain and bounded query tools. No hosted endpoint or LLM dependency.
- The same SQL policy, limits, encodings and errors exposed as JSON CLI commands.
- An in-memory `--demo` path with no configuration, files or database services.
- Single-query AST validation plus DataFusion's full-plan DDL/DML/state checks;
  limits before result collection; exact top-level Int64/UInt64/decimal strings.
- Agent setup documentation, client config examples, a copyable data-access skill,
  Python/JavaScript subprocess examples, and website discovery assets.
- Tests for policy, serialization, budgets, pagination, CLI behavior and MCP calls.
- Generic ADBC catalog discovery and streaming DataFusion providers, with trusted
  driver configuration and environment-bound secret options.
- Named Parquet sources with inferred schemas, plus joins with SQLite; live ADBC
  SQLite and PostgreSQL validation and a native/ADBC PostgreSQL join.
- Machine-readable connector coverage through CLI and MCP, distinguishing tested
  routes, candidate drivers and planned lakehouse/replication/bridge support.

Agent tools intentionally bypass the existing cache. This trades cached response
reuse for clear source-read behavior until dependencies, invalidation and freshness
metadata are mature. Cross-source queries still have no shared snapshot. The
fixed schema remains only in the optional legacy Parquet path. Additional driver
certification, upstream scan/memory behavior, authentication/TLS, real Iceberg
reads and CDC connectors remain unresolved. The implementation is a foundation for pilots,
not a claim of production readiness or proven market fit.

## Priorities after the foundation

| Priority | Deliverable | Acceptance criterion | Why this order |
| --- | --- | --- | --- |
| Done in preview | Generic ADBC and named Parquet sources | Real driver metadata/queries and a distinct Parquet schema joined without engine edits | Establishes the broader connector foundation |
| Next | Certify priority database/warehouse drivers | Recorded driver/server/auth/type matrix with exact-result tests | Converts an available driver route into evidence-backed support |
| Next | Real Iceberg catalog/snapshot reads | Correct manifests, snapshots, deletes and schema evolution; OLake interoperability tests | Makes replicated-source coverage reliable |
| Next | Source statement timeouts, cancellation and memory accounting | Timed-out queries stop upstream work within a measured budget; memory stress case has a controlled failure | Response limits alone do not control source load |
| Next | Repeatable evaluation harness | Fixed question set, expected SQL/results and schema/error tasks; compare direct Postgres, DuckDB and Igloo | Tests whether the product actually saves effort |
| Then | Parameterized, named query tools | Declared input/output schemas and scoped source access, with no SQL string interpolation | Reduces unnecessary SQL generation and narrows exposed operations |
| Then | Real freshness and provenance metadata | Source dependencies, cache age/invalidation and documented consistency for each result | Needed before claiming trustworthy cached operational answers |
| Then | Easier installation | Versioned binaries with checksums and tested client setup | A large first Rust build is an onboarding disadvantage |
| Conditional | Remote MCP/HTTP | Authenticated scoped access, TLS, rate limits, auditability and tested isolation | Build only when remote/shared deployment demand is demonstrated |
| Defer | Vector storage, memory lifecycle, distributed execution | Evidence that the primary users require it and will adopt it | Broad database connectivity does not require rebuilding every storage product |

These are priorities, not promised dates. Keep the core roadmap's existing
technical work visible and adjust it using pilot evidence.

## Distribution and commercial hypotheses

Lead with one reproducible task and a no-services demo. Publish exact MCP setup
for local coding agents, retain a shell path for agents without MCP, and make text
documentation discoverable. Show an actual source-to-answer trace, not an invented
benchmark or a simulated UI presented as a live engine. Make limitations visible
near the setup instructions.

Potential distribution paths: open-source examples, DataFusion/PostgreSQL developer
communities, reproducible evaluations, and narrowly focused integration guides.
Do not market a generic “AI database.” A useful message is: **Give your agent one
SQL interface to existing PostgreSQL and Parquet data.**

Keep the local open-source runtime useful. A future paid offer could cover shared
deployment, access policy, operation and auditing, but willingness to pay is
unvalidated. Do not invent pricing. Estimate total workload cost as source-query
load + engine compute/memory + integration/operation effort + agent retries/tokens.
Caching only improves this equation if correctness and invalidation are credible.

## Validation plan and decision gates

Recruit a small set of developers who already need cross-source answers; record
their current workflow before demonstrating Igloo. Proposed targets below are
internal hypotheses, not current results:

- After installation, first useful demo result in under five minutes; measure the
  initial Rust compilation separately.
- A new user connects a permitted development source and completes a schema →
  plan → query workflow without editing engine code.
- For a fixed suite of 20 questions, record correct-answer rate, retries, tool
  calls, result bytes, wall time and manual interventions. Include joins, NULLs,
  large identifiers, wrong schema guesses and source failures.
- Match exact results against direct SQL baselines. Run cold/warm trials and
  record dataset, hardware, source latency, concurrency and versions before making
  any speed or cost claim.
- Test an agent attempting writes/exports, reading misleading row content,
  exceeding budgets and recovering from errors. Restrict source roles independently.
- Look for repeated cross-source use in at least three pilots before investing in
  hosted operation. If users consistently solve the task with their existing
  database tooling more easily, revise the product hypothesis.

Track adoption and retention with opt-in pilot feedback; no analytics or telemetry
collection is introduced by this change. The most useful early evidence is a
developer choosing the workflow again for a real task.

## Verification of this implementation

Validated locally on Rust 1.88 with disposable PostgreSQL 15 and SQLite fixtures:
**175 Rust tests passed**, including the six connector tests with both ADBC
drivers enabled, live source discovery, allowlists, uncached
source updates, policy/budget/encoding behavior, SDK MCP calls and the JSON CLI.
The native stdio binary also passed an independent subprocess initialization,
tool discovery and query smoke check. Both the legacy PostgreSQL/ADBC demonstration
and the crypto analytics demonstration completed successfully.

Formatting, strict all-targets/all-features Clippy and rustdoc with warnings denied
passed. The starting checkout's 80 `uninlined_format_args` diagnostics were
mechanically normalized so the strict lint check can validate this work.

The paired website passed its production build and static checks for **13 HTML
pages**, local links and agent discovery assets. **30 desktop/mobile browser tests
passed**, including accessibility, search, copy controls, navigation, Markdown
discovery and the illustrative playground. Desktop/mobile screenshots were also
reviewed. Client configuration examples were not manually installed in every
listed product. These checks establish implementation behavior, not market fit,
production readiness or comparative performance. This is an experimental source
update, not a tagged production release.
