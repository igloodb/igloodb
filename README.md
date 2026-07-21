# 🍙 Igloo

Igloo is a distributed SQL query engine with an intelligent caching layer, built in Rust. It connects to external databases via ADBC drivers, leveraging DataFusion for query execution and Apache Arrow for in-memory data representation. Igloo caches query results and keeps them up to date using Change Data Capture (CDC) stored in the Iceberg format.


## 🏗️ How Igloo Works

- **Data Querying:** Igloo uses ADBC drivers (via Rust FFI) to connect to external databases (e.g., PostgreSQL) and query data efficiently through Apache DataFusion. This enables high-performance, Arrow-native SQL execution across multiple sources.
- **Materialized Views & Auto-Cache:** Apache Iceberg is leveraged to maintain materialized views and track data changes. This allows Igloo to update cached query results automatically and materialized views in response to underlying data changes, ensuring freshness and consistency.
- **Smart Caching:** Query results are cached in memory (with pluggable backends planned), and cache invalidation/refresh is driven by Change Data Capture (CDC) events from Iceberg.

## 🧩 Architecture Overview

```mermaid
flowchart LR
    subgraph DataSources["Relational DB Sources"]
        PG["Postgres"]
        MY["MySQL"]
        MSSQL["SQL Server"]
        JDBC["Generic JDBC"]
    end
    PG --> IGLOO
    MY --> IGLOO
    MSSQL --> IGLOO
    JDBC --> IGLOO
    subgraph IGLOO["Igloo"]
        CACHE["Smart Cache"]
        ENGINE["Query Engine (DataFusion)"]
        CDC["CDC Listener"]
    end
    IGLOO --> CACHE
    IGLOO --> ENGINE
    IGLOO --> CDC
    CACHE -- "Fresh/Hot Data" --> ANALYTICS["Real-time Analytics"]
    ENGINE -- "Query Results" --> ANALYTICS
    CDC -- "Change Events" --> CACHE
```

- 🧠 **Query Engine:** Apache DataFusion — Arrow-native SQL execution
- 💾 **Cache Layer:** In-memory cache (MVP), pluggable (e.g., Sled/RocksDB)
- 🔄 **CDC Integration:** Monitors Iceberg CDC streams and invalidates/updates cache entries
- 📦 **Data Format:** Apache Arrow in memory, Parquet on disk (from Iceberg)

## 🚀 Running the Project

There are two primary ways to run Igloo: using Docker Compose (recommended for ease of setup) or running it locally.

### Using Docker Compose

This is the simplest way to get Igloo and its PostgreSQL dependency running.

1.  **Ensure Docker and Docker Compose are installed.**
2.  From the project root, run:
    ```sh
    docker-compose up -d --build
    ```
    This command will:
    *   Build the Igloo Docker image.
    *   Start a PostgreSQL container.
    *   Start the Igloo application container.
    *   Run services in detached mode (`-d`).

    The Igloo service's environment variables (like database connection strings and paths) are pre-configured in the `docker-compose.yml` file to work within the Docker network.

3.  To view logs:
    ```sh
    docker-compose logs -f igloo
    ```
4.  To stop the services:
    ```sh
    docker-compose down
    ```

### Locally (without Docker)

Running Igloo locally requires you to manage dependencies and environment setup yourself.

1.  **Prerequisites:**
    *   **Rust toolchain:** Install Rust (if not already installed) via [rustup.rs](https://rustup.rs/).
    *   **Running PostgreSQL instance:** You need a PostgreSQL server running and accessible.
    *   **Dummy data:** Ensure the dummy Parquet/Iceberg data (from `dummy_iceberg_cdc/`) is available at the location Igloo expects (see environment variable configuration below).
    *   **ADBC Drivers:** Specific C++ ADBC drivers are needed. See the `LD_LIBRARY_PATH` details in the "🛠️ Environment Variable Reference" section.

2.  **Configure Environment:**
    *   Copy the example environment file:
        ```sh
        cp .env.example .env
        ```
    *   Edit the `.env` file to match your local setup, especially:
        *   `DATABASE_URL` or `IGLOO_POSTGRES_URI` (point to your PostgreSQL instance).
        *   `IGLOO_PARQUET_PATH` (path to your `dummy_iceberg_cdc/` directory).
        *   `IGLOO_CDC_PATH` (path for CDC, usually same as `IGLOO_PARQUET_PATH` for this project).
        *   Ensure `LD_LIBRARY_PATH` is correctly set in your shell environment or within the `.env` file if your tool supports it (e.g., using a dotenv-cli).

3.  **Build and Run:**
    *   From the project root, execute:
        ```sh
        cargo run
        ```
    This will compile and run the Igloo application.

### Running the SQL server (`igloo serve`)

Igloo can run as a long-lived server speaking the **PostgreSQL wire protocol**, so `psql`, BI tools, and any Postgres driver can query it directly:

```sh
IGLOO_LISTEN_ADDR=127.0.0.1:5442 cargo run -- serve
# in another shell:
psql -h 127.0.0.1 -p 5442 -c "SELECT * FROM iceberg LIMIT 10"
```

`listen_addr`/`IGLOO_LISTEN_ADDR` is required in serve mode (fail-fast). The registered tables (`iceberg`, `pg_table`) are queryable with arbitrary SQL, including joins and aggregates. **The endpoint is currently unauthenticated plaintext** (see roadmap F4.2 for auth/TLS) — keep it on localhost or a trusted network.

## 🏗️ Example Code

```rust
use std::time::Duration;

use datafusion::arrow::util::pretty::pretty_format_batches;
use igloo::cache_layer::Cache;
use igloo::cdc_sync::CdcListener;
use igloo::datafusion_engine::DataFusionEngine;

#[tokio::main]
async fn main() -> igloo::errors::Result<()> {
    // Arrow-native cache: bounded (LRU) with a TTL, safe to share as Arc<Cache>.
    let cache = Cache::new(1024, Duration::from_secs(300));
    let cdc = CdcListener::new("./dummy_iceberg_cdc");

    let engine = DataFusionEngine::new(
        "./dummy_iceberg_cdc/",
        "postgres://postgres:postgres@localhost:5432/mydb",
    )
    .await?;

    let query = "SELECT i.user_id, i.data, p.extra_info \
                 FROM iceberg i \
                 JOIN pg_table p ON i.user_id = p.user_id \
                 WHERE i.user_id = 42";

    if let Some(batches) = cache.get(query) {
        println!("Cache hit:\n{}", pretty_format_batches(&batches)?);
    } else {
        let batches = engine.query(query).await?;
        println!(
            "Cache miss. Executed with DataFusion:\n{}",
            pretty_format_batches(&batches)?
        );
        cache.set(query, batches);
    }

    // Invalidates cached results when CDC events are found.
    // (In production, CDC sync should run asynchronously.)
    cdc.sync(&cache);
    Ok(())
}
```

## 🧪 Development

```sh
cargo test                                            # unit tests (no external services needed)
cargo fmt --all -- --check                            # formatting (enforced by CI)
cargo clippy --all-targets --all-features -- -D warnings  # lints (enforced by CI)
```

Integration tests exercise the federated Parquet ⋈ PostgreSQL path against a live database and are skipped unless `IGLOO_TEST_POSTGRES_URI` points at a PostgreSQL instance the tests may freely create tables in (CI runs them against a service container):

```sh
IGLOO_TEST_POSTGRES_URI=postgres://postgres:postgres@localhost:5432/igloo_test \
    cargo test --test postgres_federation
```

## 🛠️ Environment Variable Reference

Igloo's behavior is controlled by a small set of required settings. They can come from a TOML config file (`igloo.toml` in the working directory, or any path via `IGLOO_CONFIG` — see `igloo.example.toml`) and/or environment variables, with **environment variables taking precedence**. When running locally, environment variables can be set in a `.env` file (by copying `.env.example`) or directly in your shell. When using Docker Compose, these are set within the `docker-compose.yml` file for the `igloo` service.

**Igloo fails fast:** every setting below is required, and startup aborts with an error naming the missing setting if one is absent. There are no built-in localhost defaults.

### General Configuration

*   `DATABASE_URL`:
    *   **Purpose:** The primary connection string for your PostgreSQL database. If set, Igloo prioritizes this over `IGLOO_POSTGRES_URI`. This is a commonly used standard variable name.
    *   **Example (local):** `postgres://postgres:postgres@localhost:5432/mydb`
    *   **Example (Docker):** `postgres://postgres:postgres@postgres:5432/mydb` (points to the `postgres` service in Docker)

*   `IGLOO_POSTGRES_URI`:
    *   **Purpose:** Specifies the connection string for the PostgreSQL database if `DATABASE_URL` is not set (config file key: `postgres_uri`).
    *   **Note:** Both the URI scheme and the keyword/value format (`host=... user=...`) are accepted for the DataFusion Postgres table; the ADBC driver requires the URI scheme.

*   `IGLOO_PARQUET_PATH`:
    *   **Purpose:** Defines the file system path to the directory containing Parquet files, which represent the Iceberg table data for this project (config file key: `parquet_path`).
    *   **Example (local):** `./dummy_iceberg_cdc/`
    *   **Example (Docker):** `/app/dummy_iceberg_cdc/` (path inside the Igloo container)

*   `IGLOO_CDC_PATH`:
    *   **Purpose:** Sets the file system path for the Change Data Capture (CDC) listener to monitor for changes. In this project, it's often the same as `IGLOO_PARQUET_PATH` (config file key: `cdc_path`).
    *   **Example (local):** `./dummy_iceberg_cdc`
    *   **Example (Docker):** `/app/dummy_iceberg_cdc` (path inside the Igloo container)

*   `IGLOO_POSTGRES_SCHEMAS`:
    *   **Purpose:** Comma-separated list of PostgreSQL schemas (namespaces) to introspect. Every base table found in these schemas is registered automatically by name (views are skipped); columns whose type has no Arrow mapping are dropped with a warning (config file key: `postgres_schemas`, a TOML array). Optional — defaults to `public`. Must list at least one schema.
    *   **Example:** `public,analytics`

*   `IGLOO_CONFIG`:
    *   **Purpose:** Optional path to a TOML config file providing the settings above (see `igloo.example.toml`). Environment variables override file values. If unset, `./igloo.toml` is used when present.

### ADBC Driver Configuration (for Local Execution)

Igloo relies on ADBC C++ drivers (such as the PostgreSQL driver) via Rust's Foreign Function Interface (FFI). This is because native ADBC Rust drivers are still under active development. For local execution (not Docker), you must have these C++ driver libraries available and tell the system where to find them.

*   `LD_LIBRARY_PATH` (Linux/macOS):
    *   **Purpose:** This environment variable tells the dynamic linker where to find shared libraries (like the ADBC PostgreSQL driver) when the Igloo application starts. This is **essential** if you are running Igloo directly on your host machine without Docker.
    *   **Example:**
        ```bash
        export LD_LIBRARY_PATH=/path/to/your/adbc_driver_libs:$LD_LIBRARY_PATH
        ```
    *   The specific paths depend on how and where you installed the ADBC driver libraries (e.g., via a package manager like Conda/Mamba, or compiled from source). The example path in previous README versions (`/home/ubuntu/.local/share/mamba/pkgs/...`) is specific to a particular Mamba installation. You'll need to find the `libadbc_driver_postgresql.so` (or `.dylib` on macOS) file and its dependencies on your system.
    *   **Note:** This is not typically needed when running via Docker Compose, as the Docker image is built with the necessary libraries included and correctly pathed.

### Test Configuration

*   `TEST_ADBC_POSTGRESQL_URI` (for tests):
    *   **Purpose:** Specifies the PostgreSQL connection URI specifically for running integration tests (e.g., via `cargo test`).
    *   **Example:**
        ```bash
        export TEST_ADBC_POSTGRESQL_URI="postgresql://user:password@localhost:5432/dbname_test"
        ```
    *   Ensure this points to a database that can be used for testing (it might get cleaned or have specific test data).

## ✅ Features

- ⚡ Fast SQL Execution with Apache DataFusion
- 🧊 Smart Result Caching using query fingerprints
- 🔄 CDC-Driven Invalidation from Iceberg logs
- 🔌 Join Support for Postgres + Arrow datasets
- 🧪 Designed for extensibility (remote cache, metrics, etc.)

## 🔮 Roadmap

- ⏱️ Async CDC updates & live cache refresh
- 🌐 REST or gRPC query API
- 🧠 Query planner-aware caching
- 📊 Metrics (e.g., Prometheus, OpenTelemetry)
- 📦 Optional persistent cache backend (e.g., RocksDB, Redis)


## 📚 Documentation

- [Architecture deep-dive](docs/ARCHITECTURE.md) — module inventory, data flow, and tech-debt notes.
- CI publishes rustdoc + these docs to GitHub Pages via the `Docs` workflow (an admin must set the repository's Pages Source to "GitHub Actions" once for this to work).

## 🤝 Contributing

Contributions, suggestions, and PRs are welcome! See CONTRIBUTING.md for more details.
