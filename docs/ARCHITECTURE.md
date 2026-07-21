# Igloo Architecture

Igloo is a single-crate, single-process proof-of-concept SQL query engine with an in-memory result cache. It uses Apache DataFusion for SQL execution over two registered tables — a Parquet-backed "iceberg" table and a custom `tokio-postgres`-backed `pg_table` provider with conservative filter pushdown — caches the Arrow record batches of executed queries keyed by canonicalized SQL text, runs a separate ADBC FFI test query against PostgreSQL (demo mode), and invalidates the whole cache when the file-based CDC listener finds any JSON event. The README's formerly aspirational claims (distributed, ADBC-through-DataFusion, Iceberg materialized views) were corrected in this branch to describe the actual state; Iceberg integration remains roadmap-only.

> **Evolution note.** This document was written as a point-in-time analysis and is kept accurate for the core engine paths (`postgres_table`, `datafusion_engine`, `adbc_postgres`, `errors`, the CDC listener's semantics). Since then, `main` has grown beyond it (see `ROADMAP.md`): a lib/bin split (`src/lib.rs`), fail-fast TOML+env configuration (`src/config.rs` — no more silent defaults; new `IGLOO_CONFIG`, `IGLOO_LISTEN_ADDR`, `IGLOO_CACHE_MAX_ENTRIES`, `IGLOO_CACHE_TTL_SECONDS`, `IGLOO_CDC_POLL_SECONDS` vars), a pgwire server (`src/server.rs`, `igloo serve`) serving queries from the cache with a background CDC polling loop (`CdcListener::spawn_polling`), an Arrow-native bounded LRU+TTL thread-safe cache, dynamic PostgreSQL catalog introspection (F1.3, `src/catalog.rs` — see the module notes below), integration test suites under `tests/`, and cargo-deny/coverage CI jobs. The `main.rs` wiring and the Configuration Surface table below are superseded by `config.rs` where they disagree; per-module sections carry inline notes where the newer design changes them.

## Directory Structure

```
igloodb/
├── Cargo.toml               # crate manifest; pins arrow 53 / datafusion 44 / adbc_core 0.16
├── Cargo.lock
├── README.md                # user-facing docs
├── ROADMAP.md               # phased feature roadmap with acceptance criteria
├── CONTRIBUTING.md          # fmt + clippy workflow, testing policy
├── LICENSE                  # MIT
├── .env.example             # documents IGLOO_* / DATABASE_URL env vars
├── deny.toml                # cargo-deny config (licenses + advisories, gated in CI)
├── igloo.example.toml       # example config file (see src/config.rs)
├── Dockerfile               # multi-stage build (rust:1.87 -> debian:bookworm-slim)
├── docker-compose.yml       # postgres:15 + igloo services
├── .dockerignore
├── .gitignore
├── .github/workflows/
│   ├── rust.yml             # CI: build (fmt/clippy/docs/test), deny, coverage, integration (live Postgres)
│   └── docs.yml             # GitHub Pages: rustdoc + this document
├── scripts/
│   └── seed_test_db.sql     # seed fixture for the in-crate integration_* tests (CI + local)
├── dummy_iceberg_cdc/
│   └── event1.json          # sole CDC event fixture; NO .parquet files ship here
├── docs/
│   └── ARCHITECTURE.md      # this document
├── tests/
│   ├── postgres_federation.rs  # live-DB federated join test (env-gated)
│   ├── catalog.rs              # live-DB introspection tests (env-gated)
│   └── pgwire_server.rs        # pgwire server integration test
└── src/
    ├── lib.rs               # library crate root exposing the modules below
    ├── main.rs              # thin binary: config load, `igloo serve` or one demo query
    ├── config.rs            # fail-fast configuration: TOML file + env overrides
    ├── catalog.rs           # information_schema introspection -> Arrow schemas (F1.3)
    ├── server.rs            # pgwire server serving queries from the cache
    ├── datafusion_engine.rs # SessionContext + table registration + query()
    ├── postgres_table.rs    # custom TableProvider over tokio-postgres w/ filter pushdown
    ├── adbc_postgres.rs     # standalone ADBC FFI query path + batch printer
    ├── cache_layer.rs       # Arrow-native bounded LRU+TTL thread-safe cache
    ├── cdc_sync.rs          # CDC listener: sync + background polling loop
    └── errors.rs            # IglooError enum + Result alias
```

## Module Inventory

### `src/main.rs` — application entrypoint
> **Superseded in part** (see Evolution note): `main.rs` is now a thin binary over the `igloo` library — it loads fail-fast configuration via `config::Config` (TOML file + env overrides; missing required values abort startup instead of silently defaulting), dispatches `igloo serve` to the pgwire server (`server.rs`) with a background CDC polling loop, and otherwise runs the single demo query below, caching Arrow batches rather than display strings. The ADBC smoke test remains on the demo path's critical path.

The original analysis of the demo flow (still the shape of the non-`serve` path):
Responsibility: process wiring and the single demo run; `#[tokio::main]`.
Public API: `async fn main() -> errors::Result<()>`.
Key decisions:
- Initializes `env_logger` with a default `info` filter, overridable by `RUST_LOG` (`src/main.rs:19`).
- Reads config from env with hardcoded fallbacks: `IGLOO_CDC_PATH` (`src/main.rs:23`), `IGLOO_PARQUET_PATH` (`src/main.rs:28`), and `DATABASE_URL` preferred over `IGLOO_POSTGRES_URI` (`src/main.rs:29-31`).
- Runs exactly one hardcoded query joining `iceberg` and `pg_table` on `user_id` with `WHERE i.user_id = 42` (`src/main.rs:36`); there is no query API or loop.
Collaboration: constructs `Cache` (`src/main.rs:22`), `CdcListener` (`src/main.rs:24`), `DataFusionEngine` (`src/main.rs:33`); on cache miss calls `engine.query` + `pretty_format_batches` + `cache.set` (`src/main.rs:43-45`); then calls `adbc_postgres::adbc_postgres_query_example` with `"SELECT 1 AS test_col"` (`src/main.rs:50-51`) and finally `cdc.sync(&mut cache)` (`src/main.rs:55`). Note the ADBC call sits on the critical path between `cache.set` and `cdc.sync`, so an ADBC failure aborts the run before CDC sync executes.

### `src/datafusion_engine.rs` — query engine facade
Responsibility: owns a DataFusion `SessionContext`, registers the Parquet-backed `iceberg` table, and (since F1.3 on `main`) dynamically registers **every PostgreSQL base table** discovered by `catalog` introspection.
Public API: `struct DataFusionEngine { pub ctx: SessionContext }`; `async fn new(parquet_path, postgres_conn_str, postgres_schemas) -> Result<Self>`; `async fn query(&self, sql) -> Result<Vec<RecordBatch>>`.
Key decisions:
- The context enables DataFusion's `information_schema` so BI tools can run `SHOW TABLES` against Igloo.
- `iceberg` is a plain `ListingTable` over Parquet — no Iceberg format involvement. Its Arrow schema remains hardcoded to `user_id: Int64 (non-null)`, `data: Utf8` and must match the files. File extension is filtered to `.parquet` and partitions default to `num_cpus::get()`.
- PostgreSQL tables are no longer hardcoded: `register_postgres_tables` introspects `information_schema.columns` via `src/catalog.rs` over one shared connection (`PostgresTable::from_client`), registering each discovered table (with a backward-compatible `pg_table` alias for `public.my_pg_table`).
- `query` is a thin wrapper over `ctx.sql(...).await?.collect().await?`.
Collaboration: uses `catalog::introspect_tables`/`resolve_registration_names` and `PostgresTable::from_client`; consumed by `main` and `server`. Errors convert into `IglooError::DataFusion` via `?`.
Tests: three unit tests over the Parquet path; the introspection path is covered by `tests/catalog.rs` (env-gated, live DB).

### `src/postgres_table.rs` — custom TableProvider over PostgreSQL
Responsibility: expose a live Postgres table to DataFusion as an Arrow-producing `TableProvider`.
Public API: `struct PostgresTable`; `async fn try_new(conn_str, schema_name, table_name, schema) -> IglooResult<Self>` (`:267`); `fn from_client(client, schema_name, table_name, schema)` (`:294`) for catalog registration sharing one connection; `impl TableProvider`. Private pure helpers: `quote_ident`/`quote_relation` (`:23-33` — scans are schema-qualified so tables outside `search_path` resolve), the filter-translation machinery `literal_to_sql`/`column_to_sql`/`comparison_to_sql`/`try_expr_to_sql` (`:58-195`), and the SQL assembler `build_scan_sql` (`:214`).
Key decisions:
- `try_new` connects with **`NoTls`** and spawns the connection driver task in the background for the client's lifetime; the `Client` is held in an `Arc`.
- **Conservative filter pushdown**: `supports_filters_pushdown` (`:333`) reports `Inexact` for every filter that `try_expr_to_sql` (`:172`) can translate into an *exactly* semantics-preserving PostgreSQL snippet, `Unsupported` otherwise; `scan` (`:349`) translates with the same single source of truth and emits the clauses as `WHERE ... AND ...`. Whitelist: column-vs-literal comparisons on integers/booleans (all six operators), string `=`/`<>` only (ordering comparisons are collation-sensitive and never pushed; equality assumes deterministic collations), `IS [NOT] NULL` on schema columns, and `AND`/`OR`/`NOT` when every child translates. Floats, decimals, dates, timestamps, NULL literals, casts, LIKE, and column-vs-column are rejected. `Inexact` means DataFusion re-applies each pushed predicate above the scan, so a translation bug cannot silently corrupt results.
- `scan` honors projection (`:321-324`) and a pushed-down `LIMIT` appended with no `ORDER BY` (`build_scan_sql`, `:214`).
- Empty projection (e.g. `SELECT COUNT(*)`) produces `SELECT COUNT(*) FROM (SELECT 1 FROM <t>[WHERE ...][LIMIT n]) AS t` and returns a row-count-only batch (`:218-224`, `:345-361`).
- Column decoding is a macro `build_array!` (`:376`) dispatched over Arrow `DataType` (`:394-457`) covering Int16/32/64, Float32/64, Boolean, Utf8, Binary, `Timestamp(ns, None)`, and Date32; any other type yields `IglooError::UnsupportedArrowType` (`:454`). The requested Rust type is driven by the Arrow field type, so the hardcoded schema must match the live column types.
- Results are materialized into a single in-memory batch served by `MemoryExec` (`:465-469`).
Collaboration: constructed by `datafusion_engine`; feeds Arrow batches into DataFusion joins; errors are boxed as `DataFusionError::External(IglooError::Postgres(..))`.
Tests: 23 unit tests (`:474` onward) covering `quote_ident`, `build_scan_sql` (incl. WHERE/LIMIT/COUNT paths), and the translation whitelist (accepted shapes and seven rejection cases), plus 4 integration tests (`:784` onward) gated on `IGLOO_TEST_POSTGRES_URI` that assert live results through a DataFusion `SessionContext`; `try_new`'s failure paths remain untested.

### `src/adbc_postgres.rs` — standalone ADBC FFI path
Responsibility: a separate, self-contained demonstration of querying Postgres through the ADBC C driver via FFI. It is NOT wired into DataFusion.
Public API: `async fn adbc_postgres_query_example(uri, sql) -> Result<()>` (`:15`). `print_arrow_batch` is private (`:71`).
Key decisions:
- Dynamically loads `adbc_driver_postgresql` at runtime with `AdbcVersion::V110` (`:22-23`); the shared library must be discoverable via the OS loader path (see comment `:20`). Passes the URI via `OptionDatabase::Uri` (`:25`).
- Executes the SQL, collects batches, and pretty-prints them; a collection error is mapped to `IglooError::Arrow` (`:54-57`).
- `print_arrow_batch` matches the common Arrow types, including Int16/Int64/Float32 arms (`:90`, `:106`, `:114`) alongside Int32/Float64/Utf8/Boolean/Date32/Binary/Timestamp(ns); exotic types still fall through to a catch-all printing `[unsupported: ..]`.
Collaboration: called once from `main` with `"SELECT 1 AS test_col"` (`src/main.rs:50-51`); shares only the `IglooError` type with the rest of the app. It does not use `PostgresTable` and opens its own connection.
Tests: 4 unit tests of `print_arrow_batch` (supported types incl. nulls, zero-row batch, Int64, Int16+Float32); `adbc_postgres_query_example` itself is untested (requires driver + DB).

### `src/cache_layer.rs` — in-memory result cache
Responsibility: thread-safe, bounded, Arrow-native query-result cache (shared as `Arc<Cache>`).
Public API: `pub fn normalize_query(&str) -> String` (`:30`); `struct Cache` with `new(max_entries, ttl)`, `with_clock` (injectable clock for TTL tests), `get(&str) -> Option<Arc<Vec<RecordBatch>>>`, `set(&str, Vec<RecordBatch>)`, `clear`, `len`, `is_empty`, and `stats() -> CacheStats` (hit/miss/eviction counters).
Key decisions:
- Values are the actual Arrow record batches, not display strings; entries expire after a TTL and the least-recently-used entry is evicted at capacity (O(n) scan, fine at configured scales).
- Keys are **canonicalized query text** via `normalize_query` (`:30`): the primary path parses with sqlparser's `PostgreSqlDialect` (re-exported by DataFusion, no extra dependency) and uses the statements' `Display` form, so whitespace, keyword casing, and trailing-semicolon variants share one entry; input that does not parse as SQL falls back to a lexical normalizer (`normalize_lexical`, `:45`) that trims, drops one trailing `;`, and collapses whitespace outside quoted literals/identifiers. Unquoted identifier case is deliberately NOT folded — the failure mode is a spurious miss, never a wrong hit. Plan-based fingerprinting remains roadmap F1.4.
Collaboration: demo `main` and the pgwire `server` read/write it; `CdcListener::sync` clears it.
Tests: twelve unit tests covering normalization equivalence/non-equivalence pairs, escaped quotes, keyword-case folding, the non-SQL fallback, get/set/overwrite/clear behavior, LRU eviction, TTL expiry (clock-injected), and a concurrency smoke test.

### `src/cdc_sync.rs` — file-based CDC listener
Responsibility: detect change events and conservatively invalidate the cache.
Public API: `struct CdcListener` (`:11-13`); `new(&str)` (`:15`); `sync(&mut Cache) -> usize` returns the number of events found (`:27`).
Key decisions:
- Non-directory paths (e.g. `s3://...`) are unsupported and log a warning, returning 0 (`:29-36`).
- Any event triggers a **whole-cache `clear()`** — no per-table or per-key invalidation (`:42-52`); the clear only runs if the cache is non-empty (`:45`).
- `read_local_events` reads every `*.json` file's contents as an opaque string (`:57-77`, extension filter `:69`); event bodies are logged but never parsed.
Collaboration: mutates `Cache`; driven synchronously by `main`.
Tests: four tests covering events-present invalidation, no-events retention, missing/remote directory, and non-JSON files (`:93-153`).

### `src/errors.rs` — error model
Responsibility: unify library errors.
Public API: `enum IglooError` (`:4-23`) and `type Result<T> = std::result::Result<T, IglooError>` (`:25`).
Key decisions: a flat `thiserror` enum with `#[from]` conversions for DataFusion (`:6-7`), Arrow (`:9-10`), `tokio_postgres` (`:12-13`), `adbc_core` (`:15-16`), and `std::io` (`:18-19`), plus a domain variant `UnsupportedArrowType(DataType)` (`:21-22`). No backtraces, error codes, or added context.
Collaboration: used as the return type across all modules; `PostgresTable::scan` cannot return `IglooError` directly (it must return `DFResult`), so it boxes into `DataFusionError::External` instead of using `?`.

## End-to-End Data Flow

Runtime sequence of `main()` (`src/main.rs:16-60`):
1. Initialize `env_logger` (respects `RUST_LOG`, default `info`) (`:19`).
2. Construct an empty `Cache` (`:22`).
3. Read `IGLOO_CDC_PATH` (default `./dummy_iceberg_cdc`) and build `CdcListener` (`:23-24`). No I/O yet.
4. Read `IGLOO_PARQUET_PATH` (default `./dummy_iceberg_cdc/`) and the Postgres connection string, preferring `DATABASE_URL` then `IGLOO_POSTGRES_URI` then a hardcoded default (`:27-31`).
5. `DataFusionEngine::new` (`:33`): create `SessionContext`, register `iceberg` (ListingTable over Parquet, hardcoded schema) and `pg_table` (this eagerly connects to Postgres via `tokio_postgres::connect` and spawns the connection task) (`src/datafusion_engine.rs:20-26`, `src/postgres_table.rs:37-53`). If Postgres is unreachable, startup fails here.
6. Define the hardcoded demo query (`:36`).
7. `cache.get(query)` (`:38`) — always a miss on first run (empty cache).
8. On miss: `engine.query(query)` plans and executes the SQL — DataFusion scans the Parquet `ListingTable` and the `PostgresTable` (which issues `SELECT "user_id","extra_info" FROM "my_pg_table"`, plus pushed `WHERE` clauses whenever DataFusion hands the scan translatable predicates), joins them in memory, then `pretty_format_batches` renders the result and `cache.set` stores it keyed by the canonicalized query text (`:43-45`). Because no `.parquet` files ship in `dummy_iceberg_cdc/`, the `iceberg` side is empty and the join yields zero rows even against a live Postgres.
9. ADBC test: `adbc_postgres_query_example(conn_str, "SELECT 1 AS test_col")` loads the ADBC driver, opens its own connection, executes, and prints batches (`:50-51`). Failure here (missing driver `.so` or DB down) aborts `main` before step 10.
10. `cdc.sync(&mut cache)` scans the CDC directory; finding `event1.json` it clears the entire cache (`:55`).
11. Return `Ok(())`; the process exits and the spawned Postgres connection task is dropped.

```mermaid
sequenceDiagram
    participant Main as main.rs
    participant Env
    participant Cache
    participant Engine as DataFusionEngine
    participant Listing as iceberg (ListingTable/Parquet)
    participant PG as pg_table (PostgresTable)
    participant Postgres as PostgreSQL (tokio-postgres)
    participant ADBC as ADBC driver (FFI)
    participant CDC as CdcListener

    Main->>Env: read IGLOO_CDC_PATH / IGLOO_PARQUET_PATH / DATABASE_URL|IGLOO_POSTGRES_URI
    Main->>Engine: new(parquet_path, conn_str)
    Engine->>Listing: register iceberg (hardcoded schema)
    Engine->>PG: try_new -> tokio_postgres::connect (NoTls) + spawn conn task
    PG->>Postgres: TCP connect
    Main->>Cache: get(query)
    Cache-->>Main: None (miss)
    Main->>Engine: query(SELECT ... JOIN ... WHERE user_id=42)
    Engine->>Listing: scan Parquet (0 rows; no files ship)
    Engine->>PG: scan()
    PG->>Postgres: SELECT "user_id","extra_info" FROM "my_pg_table" (+ pushed WHERE when translatable)
    Postgres-->>PG: rows -> Arrow RecordBatch
    Engine->>Engine: hash join in memory
    Engine-->>Main: Vec<RecordBatch>
    Main->>Cache: set(query, pretty_format_batches)
    Main->>ADBC: adbc_postgres_query_example("SELECT 1 AS test_col")
    ADBC->>Postgres: SELECT 1 (separate connection)
    Postgres-->>ADBC: batch -> printed
    Main->>CDC: sync(&mut cache)
    CDC->>CDC: read *.json events (event1.json)
    CDC->>Cache: clear() (whole cache invalidated)
```

## Configuration Surface

> **Superseded** (see Evolution note): configuration is now owned by `src/config.rs` — an optional TOML file (path from `IGLOO_CONFIG`) with env-var overrides, and **fail-fast** semantics: `parquet_path`, `cdc_path`, and the Postgres URI are required (no hardcoded defaults). The same variable names below are still honored as env overrides, joined by `IGLOO_LISTEN_ADDR`, `IGLOO_CACHE_MAX_ENTRIES`, `IGLOO_CACHE_TTL_SECONDS`, and `IGLOO_CDC_POLL_SECONDS`. The `main.rs` line references below describe the pre-`config.rs` layout.

Env vars read by the original demo wiring:

| Variable | Default (in code) | Read at (`file:line`) |
| --- | --- | --- |
| `RUST_LOG` | filter `info` if unset | `src/main.rs:19` (via `env_logger::Env::default()`) |
| `IGLOO_CDC_PATH` | `./dummy_iceberg_cdc` | `src/main.rs:23` |
| `IGLOO_PARQUET_PATH` | `./dummy_iceberg_cdc/` | `src/main.rs:28` |
| `DATABASE_URL` | (falls through to `IGLOO_POSTGRES_URI`) | `src/main.rs:29` |
| `IGLOO_POSTGRES_URI` | `postgres://postgres:postgres@localhost:5432/mydb` | `src/main.rs:30-31` |
| `IGLOO_TEST_POSTGRES_URI` | none — integration tests print a skip note and return when unset | `src/postgres_table.rs:793` (test-only; seed data via `scripts/seed_test_db.sql`) |

Documented but never read in code (flagged discrepancies):

| Variable | Documented at | Status |
| --- | --- | --- |
| `TEST_ADBC_POSTGRESQL_URI` | formerly in README | Was never referenced anywhere in `src/`; removed from the README in this branch in favor of `IGLOO_TEST_POSTGRES_URI` (which IS read, see above). |
| `LD_LIBRARY_PATH` | `README.md:180-187` | Not read via `env::var`; consumed by the OS dynamic linker when the ADBC driver is loaded (`src/adbc_postgres.rs:22`, comment `:20`). Real, but an OS-level variable, not app config. |
| `CARGO_TERM_COLOR` | `.github/workflows/rust.yml:10` | CI-only; not application configuration. |

## Error Handling

All fallible code returns `IglooError`/`Result` (`src/errors.rs:4-25`). Library errors funnel in via `#[from]`:
- DataFusion errors from `ctx.sql`/`collect` become `IglooError::DataFusion` through `?` in `query` (`src/datafusion_engine.rs:65-66`, variant `src/errors.rs:6-7`).
- Arrow errors become `IglooError::Arrow` — used explicitly for the ADBC batch-collection failure (`src/adbc_postgres.rs:56`, variant `src/errors.rs:9-10`).
- `tokio_postgres::Error` becomes `IglooError::Postgres` (`src/errors.rs:12-13`), used directly in `try_new` (`src/postgres_table.rs:40`).
- `adbc_core::error::Error` becomes `IglooError::AdbcCore` (`src/errors.rs:15-16`), produced implicitly by `?` on the ADBC driver/statement calls (`src/adbc_postgres.rs:22-34`).
- `UnsupportedArrowType` is raised for unmapped column types during a scan (`src/postgres_table.rs:213-217`).

A structural wrinkle: inside `PostgresTable::scan` the function signature is DataFusion's `DFResult`, so it cannot use `?` to yield an `IglooError`. Instead Postgres errors are boxed as `DataFusionError::External(Box::new(IglooError::Postgres(..)))` (`src/postgres_table.rs:96`, `:127-129`) and Arrow build errors as `DataFusionError::ArrowError(..)` (`:101`, `:223`). When such an error later surfaces from `engine.query`, it is re-wrapped as `IglooError::DataFusion`, so a Postgres failure during scan reaches `main` labeled as a DataFusion error, not a Postgres one.

## Concurrency Model

- The process runs on a multi-threaded Tokio runtime (`#[tokio::main]`, features `macros` + `rt-multi-thread` in `Cargo.toml:17`).
- `PostgresTable::try_new` spawns exactly one background task to drive the `tokio-postgres` connection for the client's lifetime (`src/postgres_table.rs:42-46`); errors on that task are only logged, and the task is dropped at process exit. The ADBC path opens and closes its own connection independently (`src/adbc_postgres.rs:28`).
- DataFusion internally parallelizes execution; `iceberg` scan target-partitions default to `num_cpus::get()` (`src/datafusion_engine.rs:38`). The `PostgresTable` scan, however, issues a single blocking-style `query`/`query_one` and returns one batch via `MemoryExec` (`src/postgres_table.rs:126-129`, `:222-230`) — no scan parallelism or streaming.
- The cache and the CDC sync are entirely synchronous and single-threaded: `Cache` is a plain `HashMap` with `&mut self` mutation (`src/cache_layer.rs`), and `CdcListener::sync` does blocking `std::fs` reads on the runtime thread (`src/cdc_sync.rs:57-77`). There is no shared-state locking because `main` owns the single `Cache` and touches it sequentially.
- Implication: the design is single-request and sequential end to end. Concurrent queries, async/live CDC refresh, and a shared thread-safe cache would all require new synchronization that does not exist today (consistent with the roadmap items).

## Testing Surface

| Module | Tests present | Coverage |
| --- | --- | --- |
| `cache_layer.rs` | 12 | normalization equivalence/non-equivalence/escaped-quotes/keyword-case/non-SQL-fallback, get/set/overwrite/clear, LRU eviction, clock-injected TTL expiry, concurrency smoke. |
| `config.rs` | (see file) | fail-fast validation and precedence tests added on `main`. |
| `tests/postgres_federation.rs` | 1 (env-gated) | live federated Parquet ⋈ Postgres join end-to-end. |
| `tests/pgwire_server.rs` | 2 | pgwire server round-trips against the cache. |
| `cdc_sync.rs` | 4 (`:93-153`) | events-present invalidation, no-events retention, missing/remote dir, non-JSON ignored. Good coverage of `sync`/`read_local_events`. |
| `datafusion_engine.rs` | 3 | Parquet `iceberg` registration + query with filter/projection, empty-parquet-directory (the shipped demo condition) yields zero rows, second-row projection. |
| `postgres_table.rs` | 23 unit + 4 integration | `quote_ident`; `build_scan_sql` incl. WHERE/LIMIT/COUNT paths and quoting; the translation whitelist (all accepted operators, literal-on-left, escaping, IS NULL, AND/OR/NOT nesting) and seven rejection cases; integration (gated on `IGLOO_TEST_POSTGRES_URI`, seeded via `scripts/seed_test_db.sql`): pushed `=`, pushed `IS NULL`, unsupported text-ordering re-filtered above the scan, and filtered COUNT — all through a real DataFusion `SessionContext` against live PostgreSQL. |
| `adbc_postgres.rs` | 4 | `print_arrow_batch`: supported types incl. nulls, zero-row batch, Int64, Int16+Float32. |
| `errors.rs` | 0 | none (trivial). |
| `main.rs` | 0 | none. |

Concrete coverage gaps (untested public/observable paths):
- `DataFusionEngine::new` / `register_postgres_table` end-to-end (the provider path is integration-tested from `postgres_table.rs`, but not through `DataFusionEngine` itself).
- `PostgresTable::try_new` failure paths, and live decode coverage beyond BIGINT/TEXT (the seeded fixture exercises only those two types; Float/Date/Timestamp/Binary decoding in `build_array!` has no live test).
- `adbc_postgres::adbc_postgres_query_example` (requires the ADBC driver shared library + DB).
- `main` orchestration/ordering is untested.

## Known Limitations & Technical Debt

| # | Item | Severity | Evidence |
| --- | --- | --- | --- |
| 1 | ~~Cache keyed by exact raw query string~~ **Resolved**: keys are canonicalized via a sqlparser parse round-trip with a lexical fallback (`normalize_query`). Residual: unquoted identifier case is not folded (spurious miss only) and keys remain text-based, not plan-based (roadmap F1.4) | Low (residual) | `src/cache_layer.rs:30` |
| 2 | Any CDC event clears the entire cache — no per-table/per-key invalidation | Med | `src/cdc_sync.rs:42-52` |
| 3 | ~~Hardcoded Arrow schemas~~ **Postgres side resolved on `main` (F1.3)**: table schemas are introspected from `information_schema` at startup via `src/catalog.rs`. Residual: the `iceberg` Parquet schema is still hardcoded and must match the files, and introspection is startup-time only (DDL after start is invisible until restart) | Low (residual) | `src/catalog.rs`; Parquet schema at `src/datafusion_engine.rs:50-54` |
| 4 | ~~No filter pushdown in `PostgresTable::scan`~~ **Resolved in this branch** (conservatively): translatable predicates are pushed as WHERE clauses and re-applied above the scan (`Inexact`). Residual: only whitelisted shapes push down — no floats/decimals/dates/timestamps, no string ordering, no LIKE/IN/casts — so queries outside the whitelist still fetch the full table | Low (residual) | `src/postgres_table.rs:164`, `:298-312`, `:331-334` |
| 5 | Postgres connection uses `NoTls` — credentials and data travel in cleartext | Med | `src/postgres_table.rs:18`, `:256` |
| 6 | `LIMIT` is appended with no `ORDER BY`; a pushed-down limit returns an arbitrary, nondeterministic row set (e.g. `SELECT * FROM pg_table LIMIT 5`) | Med | `src/postgres_table.rs:214` |
| 7 | Two independent Postgres access paths (DataFusion via `tokio-postgres` vs ADBC FFI) with different drivers, auth handling, and type mapping; the ADBC path only runs `SELECT 1` and is unused by queries | Med | `src/postgres_table.rs:37-53` vs `src/adbc_postgres.rs:15-34`; both invoked from `src/main.rs:33`, `:50-51` |
| 8 | ~~No query API~~ **Partially resolved on `main`**: `igloo serve` exposes a pgwire server (`src/server.rs`); the non-`serve` demo path still runs one hardcoded query | Low (residual) | `src/main.rs`, `src/server.rs` |
| 9 | CDC directory ships no `.parquet` files, so the `iceberg` scan returns zero rows and the demo join is always empty | Med | `dummy_iceberg_cdc/` holds only `event1.json`; schema at `src/datafusion_engine.rs:31-34` |
| 10 | ADBC test query sits on the critical path (after `cache.set`, before `cdc.sync`); a missing driver `.so` or DB outage aborts the whole run so CDC sync never executes | Med | `src/adbc_postgres.rs:22-23`; ordering at `src/main.rs:51`, `:55` |
| 11 | "Iceberg" is a plain Parquet `ListingTable` — no Iceberg manifests, snapshots, or materialized views exist | Med | `src/datafusion_engine.rs:29-49`; claims at `README.md:9-10`, `:41` |
| 12 | ~~Cache is unbounded~~ **Resolved on `main`**: bounded LRU with TTL expiry and eviction counters | — | `src/cache_layer.rs` (`Cache::new(max_entries, ttl)`) |
| 13 | ~~`print_arrow_batch` lacks Int64/Float32/Int16 arms~~ **Resolved in this branch**. Residual: exotic Arrow types (e.g. decimals, nested) still print `[unsupported: ..]` via the catch-all | Low (residual) | `src/adbc_postgres.rs:90`, `:106`, `:114` |
| 14 | CDC event bodies are read as opaque strings and only logged; content (table, keys, op) is never parsed | Low | `src/cdc_sync.rs:57-77` |
| 15 | Metrics: the cache now tracks hit/miss/eviction counters (`CacheStats`), but nothing exports them and no timings exist for scan latency etc. — no Prometheus/OTel integration | Low | `src/cache_layer.rs` (`CacheStats`); roadmap |
| 16 | `TEST_ADBC_POSTGRESQL_URI` documented but never read; `LD_LIBRARY_PATH` documented as config but is an OS-loader variable | Low | `README.md:180-197`; not present in `src/` |
| 17 | Declared MSRV `rust-version = "1.80.1"` is not satisfiable with the committed lockfile: the locked transitive dependency `ar_archive_writer 0.5.2` requires the `edition2024` Cargo feature (Cargo ≥ 1.85), so `cargo +1.80.1 check --all-targets --locked` fails while parsing the dependency graph. Either the MSRV must be raised or the offending dependencies pinned lower. | Med | `Cargo.toml:5`; `Cargo.lock` (`ar_archive_writer 0.5.2`); verified empirically with a local 1.80.1 toolchain |

## Roadmap Alignment

README roadmap (`README.md:209-213`):

| Roadmap item | State | Justification |
| --- | --- | --- |
| Async CDC updates & live cache refresh | Not started | `CdcListener::sync` is synchronous blocking `std::fs` I/O and only invalidates, never refreshes (`src/cdc_sync.rs:27-55`). |
| REST or gRPC query API | Not started | `main` runs one hardcoded query and exits; no server, router, or transport dependency (`src/main.rs:36`, `Cargo.toml:11-24`). |
| Query planner-aware caching | Partial step | Cache keys are now canonicalized SQL text via a parse round-trip (`src/cache_layer.rs:31`) — still text-based, not derived from the logical/physical plan. |
| Metrics (Prometheus, OpenTelemetry) | Not started | Only the `log` crate is present; no metrics/telemetry dependency (`Cargo.toml:11-24`). |
| Optional persistent cache backend (RocksDB, Redis) | Not started | Cache is a concrete in-memory `HashMap` with no backend trait (`src/cache_layer.rs:7-10`). |

Separately, the README "Features" section now matches the code: "Fast SQL Execution with DataFusion" (done — `src/datafusion_engine.rs:63-72`), "Join Support for Postgres + Arrow datasets" (done in-memory, with conservative filter pushdown; schemas still hardcoded — see item 3), caching keyed by canonicalized SQL (done — `src/cache_layer.rs:31`), and CDC-driven invalidation from JSON event files (done, with Iceberg explicitly marked as planned).
