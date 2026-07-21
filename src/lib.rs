// src/lib.rs
//! Igloo: a SQL query engine with a caching layer, built on DataFusion,
//! Arrow and ADBC. This library crate exposes the building blocks; the
//! `igloo` binary in `main.rs` wires them together.

pub mod adbc_postgres;
pub mod cache_layer;
pub mod catalog;
pub mod cdc_sync;
pub mod config;
pub mod crypto_metrics;
pub mod datafusion_engine;
pub mod errors;
pub mod postgres_table;
pub mod server;
