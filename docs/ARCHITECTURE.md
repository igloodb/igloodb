# Igloo Architecture

Igloo is a single-crate, single-process proof-of-concept SQL query engine with an in-memory result cache. It uses Apache DataFusion for SQL execution over two registered tables — a Parquet-backed "iceberg" table and a custom `tokio-postgres`-backed `pg_table` provider — caches the pretty-printed result of one hardcoded demo query by exact string, runs a separate ADBC FFI test query against PostgreSQL, and invalidates the whole cache when a file-based CDC listener finds any JSON event. Several README claims (distributed, ADBC-through-DataFusion, Iceberg materialized views, query-fingerprint caching) are aspirational and not implemented; see the discrepancies noted throughout.

## Directory Structure

```
igloodb/
├── Cargo.toml               # crate manifest; pins arrow 53 / datafusion 44 / adbc_core 0.16
├── Cargo.lock
├── README.md                # user-facing docs (partly aspirational; see discrepancies)
├── CONTRIBUTING.md          # fmt + clippy workflow
├── LICENSE                  # MIT
├── .env.example             # documents IGLOO_* / DATABASE_URL env vars
├── Dockerfile               # multi-stage build (rust:1.87 -> debian:bookworm-slim)
├── docker-compose.yml       # postgres:15 + igloo services
├── .dockerignore
├── .gitignore
├── .github/workflows/rust.yml   # CI: fmt, clippy -D warnings, build, test
├── dummy_iceberg_cdc/
│   └── event1.json          # sole CDC event fixture; NO .parquet files ship here
├── docs/
│   └── ARCHITECTURE.md      # this document
└── src/
    ├── main.rs              # entrypoint; wires cache, CDC, engine, ADBC; one demo query
    ├── datafusion_engine.rs # SessionContext + table registration + query()
    ├── postgres_table.rs    # custom DataFusion TableProvider over tokio-postgres
    ├── adbc_postgres.rs     # standalone ADBC FFI query path + batch printer
    ├── cache_layer.rs       # in-memory HashMap<String,String> cache
    ├── cdc_sync.rs          # directory-polling CDC listener -> whole-cache clear
    └── errors.rs            # IglooError enum + Result alias
```

## Module Inventory

### `src/main.rs` — application entrypoint
Responsibility: process wiring and the single demo run. Declares all sibling modules (`src/main.rs:2-7`) and is `#[tokio::main]` (`src/main.rs:16`).
Public API: `async fn main() -> errors::Result<()>`.
Key decisions:
- Initializes `env_logger` with a default `info` filter, overridable by `RUST_LOG` (`src/main.rs:19`).
- Reads config from env with hardcoded fallbacks: `IGLOO_CDC_PATH` (`src/main.rs:23`), `IGLOO_PARQUET_PATH` (`src/main.rs:28`), and `DATABASE_URL` preferred over `IGLOO_POSTGRES_URI` (`src/main.rs:29-31`).
- Runs exactly one hardcoded query joining `iceberg` and `pg_table` on `user_id` with `WHERE i.user_id = 42` (`src/main.rs:36`); there is no query API or loop.
Collaboration: constructs `Cache` (`src/main.rs:22`), `CdcListener` (`src/main.rs:24`), `DataFusionEngine` (`src/main.rs:33`); on cache miss calls `engine.query` + `pretty_format_batches` + `cache.set` (`src/main.rs:43-45`); then calls `adbc_postgres::adbc_postgres_query_example` with `"SELECT 1 AS test_col"` (`src/main.rs:50-51`) and finally `cdc.sync(&mut cache)` (`src/main.rs:55`). Note the ADBC call sits on the critical path between `cache.set` and `cdc.sync`, so an ADBC failure aborts the run before CDC sync executes.

### `src/datafusion_engine.rs` — query engine facade
Responsibility: owns a DataFusion `SessionContext` and registers the two tables.
Public API: `struct DataFusionEngine { pub ctx: SessionContext }` (`src/datafusion_engine.rs:15-17`); `async fn new(parquet_path, postgres_conn_str) -> Result<Self>` (`:20`); `async fn query(&self, sql) -> Result<Vec<RecordBatch>>` (`:63`). The two `register_*` methods are private.
Key decisions:
- `iceberg` is a plain `ListingTable` over Parquet — no Iceberg format involvement (`src/datafusion_engine.rs:29-49`). Its Arrow schema is hardcoded to `user_id: Int64 (non-null)`, `data: Utf8` and must match the files (`:31-34`, `:44`). File extension is filtered to `.parquet` and partitions default to `num_cpus::get()` (`:36-38`).
- `pg_table` is registered by constructing a `PostgresTable` with a hardcoded Arrow schema (`user_id: Int64`, `extra_info: Utf8`) and hardcoded physical table name `"my_pg_table"` (`:53-58`).
- `query` is a thin wrapper over `ctx.sql(...).await?.collect().await?` (`:65-66`).
Collaboration: calls `PostgresTable::try_new` (`:57-58`); consumed by `main`. Errors convert into `IglooError::DataFusion` via `?`.
Tests: one `#[tokio::test]` writes a temp Parquet file and asserts `iceberg` is queryable (`:105-133`); the Postgres path is not tested.

### `src/postgres_table.rs` — custom TableProvider over PostgreSQL
Responsibility: expose a live Postgres table to DataFusion as an Arrow-producing `TableProvider`.
Public API: `struct PostgresTable` (`:27-32`); `async fn try_new(conn_str, table_name, schema) -> IglooResult<Self>` (`:37`); `impl TableProvider` (`:56-232`). `quote_ident` is a private helper (`:22-24`).
Key decisions:
- `try_new` connects with **`NoTls`** (`:38`) and spawns the connection driver task in the background for the client's lifetime (`:42-46`); the `Client` is held in an `Arc` (`:49`).
- `scan` ignores pushed-down filters entirely — `_filters` is unused and DataFusion applies predicates above the scan (`:74`); `supports_filters_pushdown` is not overridden, so all filters default to `Unsupported`.
- `scan` honors projection (`:77-80`) and a pushed-down `LIMIT` appended verbatim with no `ORDER BY` (`:82`, `:118-123`).
- Empty projection (e.g. `SELECT COUNT(*)`) is special-cased to `SELECT COUNT(*) FROM (SELECT 1 FROM <t>[LIMIT n]) AS t` and returns a row-count-only batch (`:86-107`).
- Column decoding is a macro `build_array!` (`:137-153`) dispatched over Arrow `DataType` (`:155-218`) covering Int16/32/64, Float32/64, Boolean, Utf8, Binary, `Timestamp(ns, None)`, and Date32; any other type yields `IglooError::UnsupportedArrowType` (`:213-217`). The requested Rust type is driven by the Arrow field type, so the hardcoded schema must match the live column types.
- Results are materialized into a single in-memory batch served by `MemoryExec` (`:222-230`).
Collaboration: constructed by `datafusion_engine`; feeds Arrow batches into DataFusion joins; errors are boxed as `DataFusionError::External(IglooError::Postgres(..))` (`:96`, `:127-129`).
Tests: only `quote_ident` behavior (`:238-246`); `try_new` and `scan` are untested (require live Postgres).

### `src/adbc_postgres.rs` — standalone ADBC FFI path
Responsibility: a separate, self-contained demonstration of querying Postgres through the ADBC C driver via FFI. It is NOT wired into DataFusion.
Public API: `async fn adbc_postgres_query_example(uri, sql) -> Result<()>` (`:15`). `print_arrow_batch` is private (`:71`).
Key decisions:
- Dynamically loads `adbc_driver_postgresql` at runtime with `AdbcVersion::V110` (`:22-23`); the shared library must be discoverable via the OS loader path (see comment `:20`). Passes the URI via `OptionDatabase::Uri` (`:25`).
- Executes the SQL, collects batches, and pretty-prints them; a collection error is mapped to `IglooError::Arrow` (`:54-57`).
- `print_arrow_batch` matches a subset of Arrow types (`:89-157`); notably **no Int64/Float32/Int16 arms**, so those fall through to the `other` branch printing `[unsupported: ..]` (`:153-156`).
Collaboration: called once from `main` with `"SELECT 1 AS test_col"` (`src/main.rs:50-51`); shares only the `IglooError` type with the rest of the app. It does not use `PostgresTable` and opens its own connection.
Tests: none.

### `src/cache_layer.rs` — in-memory result cache
Responsibility: store query→result strings.
Public API: `struct Cache` (`:7-10`); `new` (`:13`), `get(&str) -> Option<&String>` (`:18`), `set(&str,&str)` (`:22`), `clear` (`:28`), `len` (`:34`), `is_empty` (`:38`).
Key decisions: a `HashMap<String,String>` keyed by the **exact raw query string** (`:9`); no normalization, hashing, TTL, or size bound. `clear` logs the eviction count (`:29-31`).
Collaboration: `main` reads/writes it; `CdcListener::sync` clears it.
Tests: four unit tests covering get-miss, set/get round-trip, overwrite, and clear (`:47-77`).

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
8. On miss: `engine.query(query)` plans and executes the SQL — DataFusion scans the Parquet `ListingTable` and the `PostgresTable` (which issues `SELECT "user_id","extra_info" FROM "my_pg_table"`), joins them in memory, then `pretty_format_batches` renders the result and `cache.set` stores it keyed by the exact query string (`:43-45`). Because no `.parquet` files ship in `dummy_iceberg_cdc/`, the `iceberg` side is empty and the join yields zero rows even against a live Postgres.
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
    PG->>Postgres: SELECT "user_id","extra_info" FROM "my_pg_table"
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

Env vars actually read by code:

| Variable | Default (in code) | Read at (`file:line`) |
| --- | --- | --- |
| `RUST_LOG` | filter `info` if unset | `src/main.rs:19` (via `env_logger::Env::default()`) |
| `IGLOO_CDC_PATH` | `./dummy_iceberg_cdc` | `src/main.rs:23` |
| `IGLOO_PARQUET_PATH` | `./dummy_iceberg_cdc/` | `src/main.rs:28` |
| `DATABASE_URL` | (falls through to `IGLOO_POSTGRES_URI`) | `src/main.rs:29` |
| `IGLOO_POSTGRES_URI` | `postgres://postgres:postgres@localhost:5432/mydb` | `src/main.rs:30-31` |

Documented but never read in code (flagged discrepancies):

| Variable | Documented at | Status |
| --- | --- | --- |
| `TEST_ADBC_POSTGRESQL_URI` | `README.md:191-197` | Never referenced anywhere in `src/` (verified by grep); dead documentation — no integration tests read it. |
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
| `cache_layer.rs` | 4 (`:47-77`) | get-miss, set/get, overwrite semantics + `len`, `clear` + `is_empty`. Good coverage of the public surface. |
| `cdc_sync.rs` | 4 (`:93-153`) | events-present invalidation, no-events retention, missing/remote dir, non-JSON ignored. Good coverage of `sync`/`read_local_events`. |
| `datafusion_engine.rs` | 1 (`:105-133`) | Parquet `iceberg` registration + query with filter/projection. |
| `postgres_table.rs` | 2 (`:238-246`) | `quote_ident` only. |
| `adbc_postgres.rs` | 0 | none. |
| `errors.rs` | 0 | none (trivial). |
| `main.rs` | 0 | none. |

Concrete coverage gaps (untested public/observable paths):
- `DataFusionEngine::new`, `register_postgres_table`, and `query` end-to-end (require Postgres); no test for the empty-parquet-directory case that the shipped demo actually hits.
- `PostgresTable::try_new` and the entire `scan` path — projection ordering, `LIMIT` appending, empty-projection `COUNT(*)`, per-type `build_array!` decoding, and the `UnsupportedArrowType` branch — are all untested. None of the scan SQL-building logic is extracted into a pure function, so it cannot be tested without a live connection today.
- `adbc_postgres::adbc_postgres_query_example` (requires driver + DB) and the pure `print_arrow_batch` (testable now, but untested) — including its missing Int64 arm.
- `main` orchestration/ordering is untested.

## Known Limitations & Technical Debt

| # | Item | Severity | Evidence |
| --- | --- | --- | --- |
| 1 | Cache keyed by exact raw query string — whitespace/case/semantic-equivalent variants miss; no normalization or fingerprinting despite README's "query fingerprints" claim | High | `src/cache_layer.rs:9`, `:18`, `:22-23`; claim at `README.md:202` |
| 2 | Any CDC event clears the entire cache — no per-table/per-key invalidation | Med | `src/cdc_sync.rs:42-52` |
| 3 | Hardcoded Arrow schemas and physical table name; the requested Rust decode type is driven by the Arrow field, so any mismatch with live column types fails every scan | High | `src/datafusion_engine.rs:31-34`, `:53-58`; decode at `src/postgres_table.rs:141`, `:155-218` |
| 4 | No filter pushdown in `PostgresTable::scan` — full table is fetched into memory, then DataFusion filters on top | High | `src/postgres_table.rs:74`; `supports_filters_pushdown` not implemented (absent) |
| 5 | Postgres connection uses `NoTls` — credentials and data travel in cleartext | Med | `src/postgres_table.rs:17`, `:38` |
| 6 | `LIMIT` is appended with no `ORDER BY`; a pushed-down limit returns an arbitrary, nondeterministic row set (e.g. `SELECT * FROM pg_table LIMIT 5`) | Med | `src/postgres_table.rs:82`, `:118-123` |
| 7 | Two independent Postgres access paths (DataFusion via `tokio-postgres` vs ADBC FFI) with different drivers, auth handling, and type mapping; the ADBC path only runs `SELECT 1` and is unused by queries | Med | `src/postgres_table.rs:37-53` vs `src/adbc_postgres.rs:15-34`; both invoked from `src/main.rs:33`, `:50-51` |
| 8 | Single hardcoded demo query; no query API, argument, or loop | Med | `src/main.rs:36` |
| 9 | CDC directory ships no `.parquet` files, so the `iceberg` scan returns zero rows and the demo join is always empty | Med | `dummy_iceberg_cdc/` holds only `event1.json`; schema at `src/datafusion_engine.rs:31-34` |
| 10 | ADBC test query sits on the critical path (after `cache.set`, before `cdc.sync`); a missing driver `.so` or DB outage aborts the whole run so CDC sync never executes | Med | `src/adbc_postgres.rs:22-23`; ordering at `src/main.rs:51`, `:55` |
| 11 | "Iceberg" is a plain Parquet `ListingTable` — no Iceberg manifests, snapshots, or materialized views exist | Med | `src/datafusion_engine.rs:29-49`; claims at `README.md:9-10`, `:41` |
| 12 | Cache is unbounded — no TTL, size cap, or eviction beyond the wholesale CDC clear | Low | `src/cache_layer.rs:7-10` |
| 13 | `print_arrow_batch` lacks Int64/Float32/Int16 arms; such columns print `[unsupported: ..]` | Low | `src/adbc_postgres.rs:89-157`, `:153-156` |
| 14 | CDC event bodies are read as opaque strings and only logged; content (table, keys, op) is never parsed | Low | `src/cdc_sync.rs:57-77` |
| 15 | No metrics/instrumentation — only `log`; no counters or timings for cache hit rate, scan latency, etc. | Low | dependencies in `Cargo.toml:11-24` (no metrics crate); roadmap `README.md:212` |
| 16 | `TEST_ADBC_POSTGRESQL_URI` documented but never read; `LD_LIBRARY_PATH` documented as config but is an OS-loader variable | Low | `README.md:180-197`; not present in `src/` |

## Roadmap Alignment

README roadmap (`README.md:209-213`):

| Roadmap item | State | Justification |
| --- | --- | --- |
| Async CDC updates & live cache refresh | Not started | `CdcListener::sync` is synchronous blocking `std::fs` I/O and only invalidates, never refreshes (`src/cdc_sync.rs:27-55`). |
| REST or gRPC query API | Not started | `main` runs one hardcoded query and exits; no server, router, or transport dependency (`src/main.rs:36`, `Cargo.toml:11-24`). |
| Query planner-aware caching | Not started | Cache keys are exact query strings, independent of the plan (`src/cache_layer.rs:9`, `:18`). |
| Metrics (Prometheus, OpenTelemetry) | Not started | Only the `log` crate is present; no metrics/telemetry dependency (`Cargo.toml:11-24`). |
| Optional persistent cache backend (RocksDB, Redis) | Not started | Cache is a concrete in-memory `HashMap` with no backend trait (`src/cache_layer.rs:7-10`). |

Separately, the "Features" section (`README.md:200-205`) claims some capabilities as done: "Fast SQL Execution with DataFusion" (done — `src/datafusion_engine.rs:63-72`) and "Join Support for Postgres + Arrow datasets" (partial — DataFusion joins the two tables in memory, but with no filter pushdown and hardcoded schemas). "Smart Result Caching using query fingerprints" and "CDC-Driven Invalidation from Iceberg logs" overstate the exact-string cache and file-based CDC respectively.
