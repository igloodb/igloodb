# Use specific Rust version with build dependencies
FROM rust:1.87 AS builder

WORKDIR /app

# Build dependencies first so they are cached independently of source changes.
COPY Cargo.toml Cargo.lock ./
RUN mkdir src \
    && echo "fn main() {}" > src/main.rs \
    && cargo build --release \
    && rm -rf src target/release/igloo target/release/deps/igloo-*

# Now copy the real source code and build the actual binary.
COPY src ./src
RUN cargo build --release

# Create minimal runtime image
FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y ca-certificates && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Only copy what's needed
COPY --from=builder /app/target/release/igloo /app/igloo
COPY dummy_iceberg_cdc ./dummy_iceberg_cdc

CMD ["/app/igloo"]
