// src/errors.rs
use thiserror::Error;

#[derive(Error, Debug)]
pub enum IglooError {
    #[error("DataFusion error: {0}")]
    DataFusion(#[from] datafusion::error::DataFusionError),

    #[error("Arrow error: {0}")]
    Arrow(#[from] arrow::error::ArrowError),

    #[error("Postgres error: {0}")]
    Postgres(#[from] tokio_postgres::Error),

    #[error("ADBC error: {0}")]
    AdbcCore(#[from] adbc_core::error::Error),

    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Configuration error: {0}")]
    Config(String),

    #[error("Unsupported Arrow type in PostgresTable: {0:?}")]
    UnsupportedArrowType(arrow::datatypes::DataType),
}

pub type Result<T> = std::result::Result<T, IglooError>;
