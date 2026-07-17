// src/server.rs
//! `igloo serve`: a long-running server speaking the PostgreSQL wire
//! protocol, so any Postgres client (`psql`, BI tools, drivers) can send
//! SQL to Igloo's DataFusion engine.
//!
//! Roadmap F1.1 walking skeleton: simple query protocol only, plaintext
//! TCP, **no authentication** (F4.2 adds SCRAM/TLS). Do not expose beyond
//! localhost or a trusted network.

use std::fmt::Debug;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::array::{
    Array, BooleanArray, Date32Array, Float32Array, Float64Array, Int16Array, Int32Array,
    Int64Array, StringArray, TimestampNanosecondArray,
};
use datafusion::arrow::datatypes::{DataType, TimeUnit};
use datafusion::arrow::record_batch::RecordBatch;
use futures::{stream, Sink};
use pgwire::api::auth::noop::NoopStartupHandler;
use pgwire::api::copy::NoopCopyHandler;
use pgwire::api::query::{PlaceholderExtendedQueryHandler, SimpleQueryHandler};
use pgwire::api::results::{DataRowEncoder, FieldFormat, FieldInfo, QueryResponse, Response, Tag};
use pgwire::api::{ClientInfo, NoopErrorHandler, PgWireServerHandlers, Type};
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};
use pgwire::messages::data::DataRow;
use pgwire::messages::PgWireBackendMessage;
use pgwire::tokio::process_socket;
use tokio::net::TcpListener;

use crate::datafusion_engine::DataFusionEngine;
use crate::errors::Result;

/// Binds `listen_addr` and serves connections until the task is aborted.
pub async fn serve(engine: Arc<DataFusionEngine>, listen_addr: &str) -> Result<()> {
    let listener = TcpListener::bind(listen_addr).await?;
    log::warn!(
        "pgwire endpoint on {} is UNAUTHENTICATED plaintext (F1.1 spike); \
         do not expose it beyond localhost or a trusted network",
        listen_addr
    );
    serve_with_listener(engine, listener).await
}

/// Serves connections on an already-bound listener. Split from [`serve`]
/// so tests can bind port 0 and discover the address first.
pub async fn serve_with_listener(
    engine: Arc<DataFusionEngine>,
    listener: TcpListener,
) -> Result<()> {
    let factory = Arc::new(IglooHandlerFactory {
        handler: Arc::new(IglooQueryHandler { engine }),
    });
    log::info!(
        "Igloo pgwire server listening on {}",
        listener.local_addr()?
    );
    loop {
        let (socket, peer_addr) = listener.accept().await?;
        log::debug!("pgwire connection accepted from {}", peer_addr);
        let factory = factory.clone();
        tokio::spawn(async move {
            if let Err(e) = process_socket(socket, None, factory).await {
                log::error!(
                    "pgwire connection from {} ended with error: {}",
                    peer_addr,
                    e
                );
            }
        });
    }
}

struct IglooQueryHandler {
    engine: Arc<DataFusionEngine>,
}

/// Startup without authentication: every connection is accepted. This is
/// deliberate for the spike and loudly warned about in [`serve`].
impl NoopStartupHandler for IglooQueryHandler {}

#[async_trait]
impl SimpleQueryHandler for IglooQueryHandler {
    async fn do_query<'a, C>(&self, _client: &mut C, query: &str) -> PgWireResult<Vec<Response<'a>>>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        log::debug!("pgwire query: {}", query);
        // A failed query becomes an ErrorResponse; pgwire keeps the
        // connection alive for the next query.
        let batches = self
            .engine
            .query(query)
            .await
            .map_err(|e| user_error(e.to_string()))?;
        Ok(vec![batches_to_response(&batches)?])
    }
}

fn user_error(message: String) -> PgWireError {
    // 42000: syntax_error_or_access_rule_violation — closest generic class
    // until errors carry structured codes.
    PgWireError::UserError(Box::new(ErrorInfo::new(
        "ERROR".to_string(),
        "42000".to_string(),
        message,
    )))
}

/// Converts collected Arrow batches into a pgwire response.
fn batches_to_response<'a>(batches: &[RecordBatch]) -> PgWireResult<Response<'a>> {
    let Some(first) = batches.first() else {
        return Ok(Response::Execution(Tag::new("SELECT").with_rows(0)));
    };

    let fields: Vec<FieldInfo> = first
        .schema()
        .fields()
        .iter()
        .map(|f| {
            Ok(FieldInfo::new(
                f.name().clone(),
                None,
                None,
                map_arrow_type(f.data_type())?,
                FieldFormat::Text,
            ))
        })
        .collect::<PgWireResult<_>>()?;
    let fields = Arc::new(fields);

    let mut rows: Vec<PgWireResult<DataRow>> = Vec::new();
    for batch in batches {
        rows.extend(
            encode_batch_rows(fields.clone(), batch)?
                .into_iter()
                .map(Ok),
        );
    }

    Ok(Response::Query(QueryResponse::new(
        fields,
        stream::iter(rows),
    )))
}

/// Maps an Arrow column type to the PostgreSQL type reported to clients.
fn map_arrow_type(dt: &DataType) -> PgWireResult<Type> {
    match dt {
        DataType::Int16 => Ok(Type::INT2),
        DataType::Int32 => Ok(Type::INT4),
        DataType::Int64 => Ok(Type::INT8),
        DataType::Float32 => Ok(Type::FLOAT4),
        DataType::Float64 => Ok(Type::FLOAT8),
        DataType::Utf8 => Ok(Type::VARCHAR),
        DataType::Boolean => Ok(Type::BOOL),
        DataType::Date32 => Ok(Type::DATE),
        DataType::Timestamp(TimeUnit::Nanosecond, _) => Ok(Type::TIMESTAMP),
        other => Err(user_error(format!(
            "column type {:?} is not supported over the pgwire endpoint yet",
            other
        ))),
    }
}

/// Encodes every row of `batch` in PostgreSQL text format.
fn encode_batch_rows(
    fields: Arc<Vec<FieldInfo>>,
    batch: &RecordBatch,
) -> PgWireResult<Vec<DataRow>> {
    let mut rows = Vec::with_capacity(batch.num_rows());
    for row_idx in 0..batch.num_rows() {
        let mut encoder = DataRowEncoder::new(fields.clone());
        for column in batch.columns() {
            encode_cell(&mut encoder, column.as_ref(), row_idx)?;
        }
        rows.push(encoder.finish()?);
    }
    Ok(rows)
}

fn encode_cell(
    encoder: &mut DataRowEncoder,
    column: &dyn Array,
    row_idx: usize,
) -> PgWireResult<()> {
    macro_rules! encode_primitive {
        ($array_ty:ty) => {{
            let array = column.as_any().downcast_ref::<$array_ty>().unwrap();
            let value = (!array.is_null(row_idx)).then(|| array.value(row_idx));
            encoder.encode_field(&value)
        }};
    }

    match column.data_type() {
        DataType::Int16 => encode_primitive!(Int16Array),
        DataType::Int32 => encode_primitive!(Int32Array),
        DataType::Int64 => encode_primitive!(Int64Array),
        DataType::Float32 => encode_primitive!(Float32Array),
        DataType::Float64 => encode_primitive!(Float64Array),
        DataType::Utf8 => encode_primitive!(StringArray),
        DataType::Boolean => encode_primitive!(BooleanArray),
        DataType::Date32 => {
            let array = column.as_any().downcast_ref::<Date32Array>().unwrap();
            let value = (!array.is_null(row_idx))
                .then(|| {
                    chrono::NaiveDate::from_ymd_opt(1970, 1, 1)
                        .unwrap()
                        .checked_add_signed(chrono::Duration::days(array.value(row_idx) as i64))
                        .ok_or_else(|| user_error("date out of range".to_string()))
                })
                .transpose()?;
            encoder.encode_field(&value)
        }
        DataType::Timestamp(TimeUnit::Nanosecond, _) => {
            let array = column
                .as_any()
                .downcast_ref::<TimestampNanosecondArray>()
                .unwrap();
            let value = (!array.is_null(row_idx))
                .then(|| chrono::DateTime::from_timestamp_nanos(array.value(row_idx)).naive_utc());
            encoder.encode_field(&value)
        }
        // map_arrow_type already rejected everything else at schema time.
        other => Err(user_error(format!(
            "internal: unhandled column type {:?} during row encoding",
            other
        ))),
    }
}

struct IglooHandlerFactory {
    handler: Arc<IglooQueryHandler>,
}

impl PgWireServerHandlers for IglooHandlerFactory {
    type StartupHandler = IglooQueryHandler;
    type SimpleQueryHandler = IglooQueryHandler;
    type ExtendedQueryHandler = PlaceholderExtendedQueryHandler;
    type CopyHandler = NoopCopyHandler;
    type ErrorHandler = NoopErrorHandler;

    fn simple_query_handler(&self) -> Arc<Self::SimpleQueryHandler> {
        self.handler.clone()
    }

    fn extended_query_handler(&self) -> Arc<Self::ExtendedQueryHandler> {
        Arc::new(PlaceholderExtendedQueryHandler)
    }

    fn startup_handler(&self) -> Arc<Self::StartupHandler> {
        self.handler.clone()
    }

    fn copy_handler(&self) -> Arc<Self::CopyHandler> {
        Arc::new(NoopCopyHandler)
    }

    fn error_handler(&self) -> Arc<Self::ErrorHandler> {
        Arc::new(NoopErrorHandler)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{
        BooleanArray, Date32Array, Float64Array, Int64Array, StringArray, TimestampNanosecondArray,
    };
    use datafusion::arrow::datatypes::{Field, Schema as ArrowSchema};

    fn field_infos(batch: &RecordBatch) -> Arc<Vec<FieldInfo>> {
        Arc::new(
            batch
                .schema()
                .fields()
                .iter()
                .map(|f| {
                    FieldInfo::new(
                        f.name().clone(),
                        None,
                        None,
                        map_arrow_type(f.data_type()).unwrap(),
                        FieldFormat::Text,
                    )
                })
                .collect(),
        )
    }

    /// Encodes expected primitive values through pgwire directly, so the
    /// test verifies our Arrow dispatch against pgwire's own encoding.
    fn expected_row(
        fields: Arc<Vec<FieldInfo>>,
        encode: impl FnOnce(&mut DataRowEncoder) -> PgWireResult<()>,
    ) -> DataRow {
        let mut encoder = DataRowEncoder::new(fields);
        encode(&mut encoder).unwrap();
        encoder.finish().unwrap()
    }

    #[test]
    fn maps_supported_arrow_types() {
        assert_eq!(map_arrow_type(&DataType::Int64).unwrap(), Type::INT8);
        assert_eq!(map_arrow_type(&DataType::Utf8).unwrap(), Type::VARCHAR);
        assert_eq!(map_arrow_type(&DataType::Boolean).unwrap(), Type::BOOL);
        assert_eq!(map_arrow_type(&DataType::Date32).unwrap(), Type::DATE);
        assert_eq!(
            map_arrow_type(&DataType::Timestamp(TimeUnit::Nanosecond, None)).unwrap(),
            Type::TIMESTAMP
        );
    }

    #[test]
    fn rejects_unsupported_arrow_types() {
        assert!(map_arrow_type(&DataType::Binary).is_err());
    }

    #[test]
    fn encodes_primitive_types_and_nulls() {
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
            Field::new("score", DataType::Float64, true),
            Field::new("active", DataType::Boolean, true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![42, 7])),
                Arc::new(StringArray::from(vec![Some("hello"), None])),
                Arc::new(Float64Array::from(vec![Some(1.5), None])),
                Arc::new(BooleanArray::from(vec![Some(true), Some(false)])),
            ],
        )
        .unwrap();

        let fields = field_infos(&batch);
        let rows = encode_batch_rows(fields.clone(), &batch).unwrap();
        assert_eq!(rows.len(), 2);

        let expected_first = expected_row(fields.clone(), |e| {
            e.encode_field(&Some(42i64))?;
            e.encode_field(&Some("hello"))?;
            e.encode_field(&Some(1.5f64))?;
            e.encode_field(&Some(true))
        });
        assert_eq!(rows[0], expected_first);

        let expected_second = expected_row(fields, |e| {
            e.encode_field(&Some(7i64))?;
            e.encode_field(&None::<&str>)?;
            e.encode_field(&None::<f64>)?;
            e.encode_field(&Some(false))
        });
        assert_eq!(rows[1], expected_second);
    }

    #[test]
    fn encodes_dates_and_timestamps() {
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("d", DataType::Date32, true),
            Field::new("ts", DataType::Timestamp(TimeUnit::Nanosecond, None), true),
        ]));
        // 2024-01-15 is 19737 days after the epoch.
        let ts_nanos: i64 = 1_705_312_800_000_000_000; // 2024-01-15T10:00:00Z
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Date32Array::from(vec![Some(19737), None])),
                Arc::new(TimestampNanosecondArray::from(vec![Some(ts_nanos), None])),
            ],
        )
        .unwrap();

        let fields = field_infos(&batch);
        let rows = encode_batch_rows(fields.clone(), &batch).unwrap();

        let expected_first = expected_row(fields.clone(), |e| {
            e.encode_field(&Some(chrono::NaiveDate::from_ymd_opt(2024, 1, 15).unwrap()))?;
            e.encode_field(&Some(
                chrono::DateTime::from_timestamp_nanos(ts_nanos).naive_utc(),
            ))
        });
        assert_eq!(rows[0], expected_first);

        let expected_second = expected_row(fields, |e| {
            e.encode_field(&None::<chrono::NaiveDate>)?;
            e.encode_field(&None::<chrono::NaiveDateTime>)
        });
        assert_eq!(rows[1], expected_second);
    }

    #[test]
    fn empty_result_becomes_zero_row_tag() {
        let response = batches_to_response(&[]).unwrap();
        assert!(matches!(response, Response::Execution(_)));
    }
}
