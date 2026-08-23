// src/server.rs
//! `igloo serve`: a long-running server speaking the PostgreSQL wire
//! protocol, so any Postgres client (`psql`, BI tools, drivers) can send
//! SQL to Igloo's DataFusion engine.
//!
//! Roadmap F1.1: both the simple query protocol and the **extended query
//! protocol** are served — clients may parse/bind/execute prepared
//! statements with parameters (`$1, $2, ...`). Parameter types the client
//! leaves unspecified (oid 0) are inferred by DataFusion from the query
//! context during planning; values arrive in either text or binary format.
//! Still open for F1.1: authentication (F4.2 adds SCRAM/TLS), portal-based
//! partial fetch (`Execute`'s max_rows is accepted but ignored — full
//! result sets are always returned), and transaction statements. Do not
//! expose beyond localhost or a trusted network.

use std::fmt::Debug;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::array::{
    Array, BooleanArray, Date32Array, Float32Array, Float64Array, Int16Array, Int32Array,
    Int64Array, StringArray, TimestampNanosecondArray,
};
use datafusion::arrow::datatypes::{DataType, Date32Type, Schema, TimeUnit};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::scalar::ScalarValue;
use futures::{stream, Sink};
use pgwire::api::auth::noop::NoopStartupHandler;
use pgwire::api::copy::NoopCopyHandler;
use pgwire::api::portal::{Format, Portal};
use pgwire::api::query::{ExtendedQueryHandler, SimpleQueryHandler};
use pgwire::api::results::{
    DataRowEncoder, DescribePortalResponse, DescribeStatementResponse, FieldInfo, QueryResponse,
    Response, Tag,
};
use pgwire::api::stmt::{NoopQueryParser, StoredStatement};
use pgwire::api::store::PortalStore;
use pgwire::api::{ClientInfo, ClientPortalStore, NoopErrorHandler, PgWireServerHandlers, Type};
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};
use pgwire::messages::data::DataRow;
use pgwire::messages::PgWireBackendMessage;
use pgwire::tokio::process_socket;
use postgres_types::{FromSql, FromSqlOwned};
use tokio::net::TcpListener;

use crate::cache_layer::Cache;
use crate::datafusion_engine::DataFusionEngine;
use crate::errors::Result;

/// Binds `listen_addr` and serves connections until the task is aborted.
pub async fn serve(
    engine: Arc<DataFusionEngine>,
    cache: Arc<Cache>,
    listen_addr: &str,
) -> Result<()> {
    let listener = TcpListener::bind(listen_addr).await?;
    log::warn!(
        "pgwire endpoint on {} is UNAUTHENTICATED plaintext; \
         do not expose it beyond localhost or a trusted network",
        listen_addr
    );
    serve_with_listener(engine, cache, listener).await
}

/// Serves connections on an already-bound listener. Split from [`serve`]
/// so tests can bind port 0 and discover the address first.
pub async fn serve_with_listener(
    engine: Arc<DataFusionEngine>,
    cache: Arc<Cache>,
    listener: TcpListener,
) -> Result<()> {
    let factory = Arc::new(IglooHandlerFactory {
        handler: Arc::new(IglooQueryHandler { engine, cache }),
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
    cache: Arc<Cache>,
}

/// Only read-only statements may be served from (or populate) the cache;
/// anything else could have side effects that must reach the engine.
fn is_cacheable(query: &str) -> bool {
    let head = query.trim_start();
    let keyword: String = head
        .chars()
        .take_while(|c| c.is_ascii_alphabetic())
        .collect();
    keyword.eq_ignore_ascii_case("SELECT") || keyword.eq_ignore_ascii_case("WITH")
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

        if is_cacheable(query) {
            if let Some(batches) = self.cache.get(query) {
                log::debug!("pgwire cache hit");
                return Ok(vec![batches_to_response(&batches)?]);
            }
        }

        // A failed query becomes an ErrorResponse; pgwire keeps the
        // connection alive for the next query.
        let batches = self
            .engine
            .query(query)
            .await
            .map_err(|e| user_error(e.to_string()))?;
        let response = batches_to_response(&batches)?;
        if is_cacheable(query) {
            self.cache.set(query, batches);
        }
        Ok(vec![response])
    }
}

#[async_trait]
impl ExtendedQueryHandler for IglooQueryHandler {
    type Statement = String;
    type QueryParser = NoopQueryParser;

    fn query_parser(&self) -> Arc<Self::QueryParser> {
        Arc::new(NoopQueryParser)
    }

    /// Reports statement metadata without executing: inferred parameter
    /// types (client-declared OIDs win where given) and the output schema.
    /// Planning failures surface as protocol errors at describe time.
    async fn do_describe_statement<C>(
        &self,
        _client: &mut C,
        target: &StoredStatement<Self::Statement>,
    ) -> PgWireResult<DescribeStatementResponse>
    where
        C: ClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::PortalStore: PortalStore<Statement = Self::Statement>,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let described = self
            .engine
            .describe_query(&target.statement)
            .await
            .map_err(|e| user_error(e.to_string()))?;
        let fields = schema_to_field_infos(described.schema.as_ref(), &Format::UnifiedText)?;
        let parameters = describe_parameters(&target.parameter_types, &described.param_types);
        Ok(DescribeStatementResponse::new(
            parameters,
            (*fields).clone(),
        ))
    }

    async fn do_describe_portal<C>(
        &self,
        _client: &mut C,
        target: &Portal<Self::Statement>,
    ) -> PgWireResult<DescribePortalResponse>
    where
        C: ClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::PortalStore: PortalStore<Statement = Self::Statement>,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let described = self
            .engine
            .describe_query(&target.statement.statement)
            .await
            .map_err(|e| user_error(e.to_string()))?;
        let fields =
            schema_to_field_infos(described.schema.as_ref(), &target.result_column_format)?;
        Ok(DescribePortalResponse::new((*fields).clone()))
    }

    /// Executes a bound portal. Parameters are decoded against the types
    /// DataFusion inferred while planning, so clients that leave parameter
    /// OIDs unspecified still bind correctly. Zero-parameter statements
    /// share the cache with the simple-query path; parameterized ones
    /// bypass it (each binding is a distinct result — plan-keyed caching,
    /// roadmap F1.4, subsumes this).
    ///
    /// `max_rows` is accepted but ignored for now: results are always
    /// returned in full, which every stock client tolerates. True portal
    /// streaming lands with F3.2.
    async fn do_query<'a, C>(
        &self,
        _client: &mut C,
        portal: &Portal<Self::Statement>,
        _max_rows: usize,
    ) -> PgWireResult<Response<'a>>
    where
        C: ClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::PortalStore: PortalStore<Statement = Self::Statement>,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let sql: &str = portal.statement.statement.as_str();
        log::debug!("pgwire extended query (portal {:?}): {}", portal.name, sql);

        let cacheable = portal.parameter_len() == 0 && is_cacheable(sql);
        if cacheable {
            if let Some(batches) = self.cache.get(sql) {
                log::debug!("pgwire cache hit");
                return batches_to_response_in_format(&batches, &portal.result_column_format);
            }
        }

        // With parameters, plan once first so values decode against the
        // engine-inferred placeholder types before execution binds them.
        let params = if portal.parameter_len() == 0 {
            Vec::new()
        } else {
            let described = self
                .engine
                .describe_query(sql)
                .await
                .map_err(|e| user_error(e.to_string()))?;
            decode_portal_params(portal, &described.param_types)?
        };

        // A failed query becomes an ErrorResponse; pgwire keeps the
        // connection alive for the next query.
        let batches = self
            .engine
            .query_with_params(sql, params)
            .await
            .map_err(|e| user_error(e.to_string()))?;

        if cacheable {
            self.cache.set(sql, batches.clone());
        }
        batches_to_response_in_format(&batches, &portal.result_column_format)
    }
}

/// Parameter types reported by `Describe` statement: client-declared OIDs
/// win; UNKNOWN entries fall back to server-inferred types, else UNKNOWN(0)
/// (the protocol's "type to be determined").
fn describe_parameters(declared: &[Type], inferred: &[Option<DataType>]) -> Vec<Type> {
    let count = declared.len().max(inferred.len());
    (0..count)
        .map(|i| match declared.get(i) {
            Some(t) if *t != Type::UNKNOWN => t.clone(),
            _ => inferred
                .get(i)
                .and_then(|dt| dt.as_ref().and_then(arrow_to_pg_type))
                .unwrap_or(Type::UNKNOWN),
        })
        .collect()
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

/// Converts collected Arrow batches into a pgwire response, encoding every
/// column in text format (the simple-query protocol's fixed encoding).
fn batches_to_response<'a>(batches: &[RecordBatch]) -> PgWireResult<Response<'a>> {
    batches_to_response_in_format(batches, &Format::UnifiedText)
}

/// Converts collected Arrow batches into a pgwire response honoring the
/// portal's requested result-column format: extended-protocol clients may
/// ask for binary encodings per column (tokio-postgres always does), and
/// sending text where binary was negotiated breaks client decoding.
fn batches_to_response_in_format<'a>(
    batches: &[RecordBatch],
    result_format: &Format,
) -> PgWireResult<Response<'a>> {
    let Some(first) = batches.first() else {
        return Ok(Response::Execution(Tag::new("SELECT").with_rows(0)));
    };

    let fields = schema_to_field_infos(first.schema().as_ref(), result_format)?;

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

/// Builds the pgwire field list for a statement's Arrow output schema in
/// the requested column formats. `DataType::Binary` result columns are not
/// supported over either wire encoding yet and are rejected here.
fn schema_to_field_infos(
    schema: &Schema,
    result_format: &Format,
) -> PgWireResult<Arc<Vec<FieldInfo>>> {
    let fields: Vec<FieldInfo> = schema
        .fields()
        .iter()
        .enumerate()
        .map(|(idx, f)| {
            Ok(FieldInfo::new(
                f.name().clone(),
                None,
                None,
                map_arrow_type(f.data_type())?,
                result_format.format_for(idx),
            ))
        })
        .collect::<PgWireResult<_>>()?;
    Ok(Arc::new(fields))
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

/// Inverse of [`map_arrow_type`] for types we can also decode from bind
/// parameters; `None` means "no PostgreSQL spelling" (reported UNKNOWN).
///
/// Deliberately wider than [`map_arrow_type`]: `bytea` values are accepted
/// as parameters (decoded to Arrow `Binary`) while Binary *result* columns
/// stay unsupported over the wire until row encoding learns them.
fn arrow_to_pg_type(dt: &DataType) -> Option<Type> {
    match dt {
        DataType::Binary => Some(Type::BYTEA),
        other => map_arrow_type(other).ok(),
    }
}

/// Maps a client-declared PostgreSQL parameter type to the Arrow type its
/// values decode into; `None` for types we cannot read.
fn pg_type_to_arrow(ty: &Type) -> Option<DataType> {
    match *ty {
        Type::INT2 => Some(DataType::Int16),
        Type::INT4 => Some(DataType::Int32),
        Type::INT8 => Some(DataType::Int64),
        Type::FLOAT4 => Some(DataType::Float32),
        Type::FLOAT8 => Some(DataType::Float64),
        Type::TEXT | Type::VARCHAR | Type::BPCHAR => Some(DataType::Utf8),
        Type::BOOL => Some(DataType::Boolean),
        Type::DATE => Some(DataType::Date32),
        Type::TIMESTAMP => Some(DataType::Timestamp(TimeUnit::Nanosecond, None)),
        Type::BYTEA => Some(DataType::Binary),
        _ => None,
    }
}

// --- extended-query parameter decoding -------------------------------------
//
// Clients bind parameters as raw bytes tagged with a format code (text or
// binary) and, optionally, a declared type OID (0 = "server infers"). The
// engine plans the statement first and infers each placeholder's type from
// context (`WHERE id = $1` against an Int64 column ⇒ Int64). These helpers
// turn the raw bytes into `ScalarValue`s of exactly those types so binding
// type-checks. Anything undecodable becomes a clean protocol error, never
// a panic.

/// Decodes every bound parameter of `portal` into `ScalarValue`s matching
/// `expected` — the engine-inferred placeholder types in ordinal order.
fn decode_portal_params(
    portal: &Portal<String>,
    expected: &[Option<DataType>],
) -> PgWireResult<Vec<ScalarValue>> {
    let count = portal.parameter_len();
    if count != expected.len() {
        return Err(user_error(format!(
            "bind sent {} parameter(s) but statement {:?} has {} placeholder(s)",
            count,
            portal.statement.id,
            expected.len()
        )));
    }

    let mut scalars = Vec::with_capacity(count);
    for idx in 0..count {
        // Precedence: what the engine inferred > what the client declared >
        // treat as text (Postgres's own fallback for untyped parameters).
        let inferred = expected.get(idx).cloned().flatten().or_else(|| {
            portal.statement.parameter_types.get(idx).and_then(|t| {
                (*t != Type::UNKNOWN)
                    .then_some(())
                    .and_then(|_| pg_type_to_arrow(t))
            })
        });
        scalars.push(decode_portal_param(portal, idx, inferred)?);
    }
    Ok(scalars)
}

/// Decodes the parameter at `idx` of an already length-checked portal.
/// `raw == None` (explicit NULL) yields a typed-null scalar.
fn decode_portal_param(
    portal: &Portal<String>,
    idx: usize,
    expected: Option<DataType>,
) -> PgWireResult<ScalarValue> {
    match portal.parameters.get(idx) {
        // Out-of-range cannot happen (length checked); treat like NULL.
        None => Ok(null_scalar(expected)),
        Some(None) => Ok(null_scalar(expected)),
        Some(Some(raw)) => {
            let is_text = portal.parameter_format.is_text(idx);
            decode_scalar_value(raw.as_ref(), is_text, expected, idx)
        }
    }
}

/// Protocol error for an undecodable parameter value.
fn param_decode_error(idx: usize, reason: impl std::fmt::Display) -> PgWireError {
    user_error(format!("cannot decode parameter ${}: {}", idx + 1, reason))
}

/// Builds a typed-null `ScalarValue`; untyped positions become text nulls
/// (Postgres's own fallback for untyped parameters).
fn null_scalar(expected: Option<DataType>) -> ScalarValue {
    match expected {
        Some(DataType::Int16) => ScalarValue::Int16(None),
        Some(DataType::Int32) => ScalarValue::Int32(None),
        Some(DataType::Int64) => ScalarValue::Int64(None),
        Some(DataType::Float32) => ScalarValue::Float32(None),
        Some(DataType::Float64) => ScalarValue::Float64(None),
        Some(DataType::Boolean) => ScalarValue::Boolean(None),
        Some(DataType::Date32) => ScalarValue::Date32(None),
        Some(DataType::Timestamp(TimeUnit::Nanosecond, _)) => {
            ScalarValue::TimestampNanosecond(None, None)
        }
        Some(DataType::Binary) => ScalarValue::Binary(None),
        _ => ScalarValue::Utf8(None),
    }
}

/// Decodes one raw parameter value into a `ScalarValue` of the expected
/// Arrow type. Text-format values are parsed from their textual spelling;
/// binary-format values are decoded with the PostgreSQL binary encoding.
fn decode_scalar_value(
    raw: &[u8],
    is_text: bool,
    expected: Option<DataType>,
    idx: usize,
) -> PgWireResult<ScalarValue> {
    match expected {
        Some(DataType::Int64) => scalar_from(
            raw,
            is_text,
            &Type::INT8,
            |s| s.parse::<i64>().ok(),
            ScalarValue::Int64,
            idx,
        ),
        Some(DataType::Int32) => scalar_from(
            raw,
            is_text,
            &Type::INT4,
            |s| s.parse::<i32>().ok(),
            ScalarValue::Int32,
            idx,
        ),
        Some(DataType::Int16) => scalar_from(
            raw,
            is_text,
            &Type::INT2,
            |s| s.parse::<i16>().ok(),
            ScalarValue::Int16,
            idx,
        ),
        Some(DataType::Float64) => scalar_from(
            raw,
            is_text,
            &Type::FLOAT8,
            |s| s.parse::<f64>().ok(),
            ScalarValue::Float64,
            idx,
        ),
        Some(DataType::Float32) => scalar_from(
            raw,
            is_text,
            &Type::FLOAT4,
            |s| s.parse::<f32>().ok(),
            ScalarValue::Float32,
            idx,
        ),
        Some(DataType::Boolean) => {
            if is_text {
                parse_bool_text(
                    std::str::from_utf8(raw)
                        .map_err(|_| param_decode_error(idx, "value is not valid UTF-8"))?,
                )
                .map(|b| ScalarValue::Boolean(Some(b)))
                .ok_or_else(|| {
                    param_decode_error(idx, "expected one of true/false/t/f/yes/no/on/off/1/0")
                })
            } else {
                bool::from_sql(&Type::BOOL, raw)
                    .map(|v| ScalarValue::Boolean(Some(v)))
                    .map_err(|e| param_decode_error(idx, e))
            }
        }
        Some(DataType::Date32) => {
            let date = if is_text {
                let s = std::str::from_utf8(raw)
                    .map_err(|_| param_decode_error(idx, "value is not valid UTF-8"))?;
                chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d")
                    .map_err(|e| param_decode_error(idx, e))
            } else {
                chrono::NaiveDate::from_sql(&Type::DATE, raw)
                    .map_err(|e| param_decode_error(idx, e))
            }?;
            Ok(ScalarValue::Date32(Some(Date32Type::from_naive_date(date))))
        }
        Some(DataType::Timestamp(TimeUnit::Nanosecond, _)) => {
            let ts = if is_text {
                let s = std::str::from_utf8(raw)
                    .map_err(|_| param_decode_error(idx, "value is not valid UTF-8"))?;
                parse_timestamp_text(s).ok_or_else(|| {
                    param_decode_error(idx, format!("{s:?} is not a supported timestamp spelling"))
                })?
            } else {
                chrono::NaiveDateTime::from_sql(&Type::TIMESTAMP, raw)
                    .map_err(|e| param_decode_error(idx, e))?
            };
            let nanos = ts.and_utc().timestamp_nanos_opt().ok_or_else(|| {
                param_decode_error(idx, "timestamp is out of range for nanosecond precision")
            })?;
            Ok(ScalarValue::TimestampNanosecond(Some(nanos), None))
        }
        Some(DataType::Binary) => {
            let bytes = if is_text {
                parse_bytea_hex(
                    std::str::from_utf8(raw)
                        .map_err(|_| param_decode_error(idx, "bytea value is not valid UTF-8"))?,
                )
                .ok_or_else(|| {
                    param_decode_error(idx, r#"expected PostgreSQL hex format ("\x…")"#)
                })?
            } else {
                Vec::<u8>::from_sql(&Type::BYTEA, raw).map_err(|e| param_decode_error(idx, e))?
            };
            Ok(ScalarValue::Binary(Some(bytes)))
        }
        // Utf8, untyped (inference had no context), and any other mapped
        // type fall back to text: Postgres resolves unknown-typed
        // parameters as text too.
        _ => {
            let s = std::str::from_utf8(raw)
                .map_err(|_| param_decode_error(idx, "value is not valid UTF-8"))?
                .to_owned();
            Ok(ScalarValue::Utf8(Some(s)))
        }
    }
}

/// Shared decoder for the simple numeric types: text parsed with
/// `parse_text`, binary via the PostgreSQL wire encoding.
fn scalar_from<T, F, G>(
    raw: &[u8],
    is_text: bool,
    binary_ty: &Type,
    parse_text: F,
    to_scalar: G,
    idx: usize,
) -> PgWireResult<ScalarValue>
where
    F: Fn(&str) -> Option<T>,
    T: FromSqlOwned,
    G: Fn(Option<T>) -> ScalarValue,
{
    if is_text {
        let s = std::str::from_utf8(raw).map_err(|_| param_decode_error(idx, "not valid UTF-8"))?;
        let v = parse_text(s).ok_or_else(|| {
            param_decode_error(idx, format!("{s:?} is not a valid {}", binary_ty.name()))
        })?;
        Ok(to_scalar(Some(v)))
    } else {
        T::from_sql(binary_ty, raw)
            .map(|v| to_scalar(Some(v)))
            .map_err(|e| param_decode_error(idx, e))
    }
}

/// Parses PostgreSQL's accepted textual spellings of a boolean.
fn parse_bool_text(s: &str) -> Option<bool> {
    match s.trim().to_ascii_lowercase().as_str() {
        "true" | "t" | "yes" | "on" | "1" => Some(true),
        "false" | "f" | "no" | "off" | "0" => Some(false),
        _ => None,
    }
}

/// Parses the common textual timestamp spellings into a naive datetime:
/// `YYYY-MM-DD HH:MM:SS[.f]`, ISO `T` separator, date-only (midnight), or
/// a full RFC 3339 offset form (converted to UTC).
fn parse_timestamp_text(s: &str) -> Option<chrono::NaiveDateTime> {
    let s = s.trim();
    for fmt in [
        "%Y-%m-%d %H:%M:%S%.f",
        "%Y-%m-%dT%H:%M:%S%.f",
        "%Y-%m-%d %H:%M",
        "%Y-%m-%dT%H:%M",
        "%Y-%m-%d",
    ] {
        if let Ok(ts) = chrono::NaiveDateTime::parse_from_str(s, fmt) {
            return Some(ts);
        }
        if let Ok(date) = chrono::NaiveDate::parse_from_str(s, fmt) {
            return date.and_hms_opt(0, 0, 0);
        }
    }
    // Offset form ("2024-01-15T10:00:00Z" / "+02:00") → UTC naive.
    chrono::DateTime::parse_from_rfc3339(s)
        .ok()
        .map(|dt| dt.naive_utc())
}

/// Decodes PostgreSQL hex-format bytea text (`"\x48656c6c6f"`, prefix
/// optional). Returns `None` for anything that is not valid hex pairs.
fn parse_bytea_hex(s: &str) -> Option<Vec<u8>> {
    let hex = s.trim().strip_prefix("\\x").unwrap_or_else(|| s.trim());
    if !hex.len().is_multiple_of(2) {
        return None;
    }
    (0..hex.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(hex.get(i..i + 2)?, 16).ok())
        .collect()
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
    type ExtendedQueryHandler = IglooQueryHandler;
    type CopyHandler = NoopCopyHandler;
    type ErrorHandler = NoopErrorHandler;

    fn simple_query_handler(&self) -> Arc<Self::SimpleQueryHandler> {
        self.handler.clone()
    }

    fn extended_query_handler(&self) -> Arc<Self::ExtendedQueryHandler> {
        self.handler.clone()
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
    use crate::cache_layer::normalize_query;
    use datafusion::arrow::array::{
        BooleanArray, Date32Array, Float64Array, Int64Array, StringArray, TimestampNanosecondArray,
    };
    use datafusion::arrow::datatypes::{Field, Schema as ArrowSchema};
    use pgwire::api::results::FieldFormat;

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

    #[test]
    fn only_read_statements_are_cacheable() {
        assert!(is_cacheable("SELECT 1"));
        assert!(is_cacheable("  select * from t"));
        assert!(is_cacheable("WITH x AS (SELECT 1) SELECT * FROM x"));
        assert!(!is_cacheable("INSERT INTO t VALUES (1)"));
        assert!(!is_cacheable("SET search_path = public"));
        assert!(!is_cacheable("CREATE TABLE t (a int)"));
        assert!(!is_cacheable(""));
    }

    // --- extended-query parameter decoding ----------------------------------

    use bytes::BytesMut;
    use postgres_types::ToSql;

    /// Encodes `value` in the PostgreSQL binary wire format for `ty`.
    fn binary_bytes(value: &impl ToSql, ty: &Type) -> Vec<u8> {
        let mut out = BytesMut::new();
        value.to_sql(ty, &mut out).unwrap();
        out.to_vec()
    }

    const INT: Option<DataType> = Some(DataType::Int64);
    const STR: Option<DataType> = Some(DataType::Utf8);

    fn decode(raw: &[u8], is_text: bool, expected: Option<DataType>) -> ScalarValue {
        decode_scalar_value(raw, is_text, expected, 0).expect("decodes")
    }

    fn decode_err(raw: &[u8], is_text: bool, expected: Option<DataType>) -> String {
        decode_scalar_value(raw, is_text, expected, 2)
            .expect_err("must fail")
            .to_string()
    }

    #[test]
    fn decodes_text_parameters() {
        assert_eq!(decode(b"42", true, INT), ScalarValue::Int64(Some(42)));
        assert_eq!(
            decode(b"-7", true, Some(DataType::Int32)),
            ScalarValue::Int32(Some(-7))
        );
        assert_eq!(
            decode(b"1.5", true, Some(DataType::Float64)),
            ScalarValue::Float64(Some(1.5))
        );
        assert_eq!(
            decode(b"true", true, Some(DataType::Boolean)),
            ScalarValue::Boolean(Some(true))
        );
        assert_eq!(
            decode(b"off", true, Some(DataType::Boolean)),
            ScalarValue::Boolean(Some(false))
        );
        assert_eq!(
            decode(b"hello", true, STR),
            ScalarValue::Utf8(Some("hello".into()))
        );
        assert_eq!(
            decode(b"2024-01-15", true, Some(DataType::Date32)),
            ScalarValue::Date32(Some(Date32Type::from_naive_date(
                chrono::NaiveDate::from_ymd_opt(2024, 1, 15).unwrap()
            )))
        );
        assert_eq!(
            decode(b"2024-01-15 10:00:00", true, timestamp_type()),
            ScalarValue::TimestampNanosecond(Some(1_705_312_800_000_000_000), None)
        );
        assert_eq!(
            decode(b"2024-01-15T10:00:00Z", true, timestamp_type()),
            ScalarValue::TimestampNanosecond(Some(1_705_312_800_000_000_000), None)
        );
        // Date-only text binds as midnight when a timestamp is expected.
        assert_eq!(
            decode(b"2024-01-15", true, timestamp_type()),
            ScalarValue::TimestampNanosecond(Some(1_705_276_800_000_000_000), None)
        );
    }

    #[test]
    fn decodes_binary_parameters() {
        assert_eq!(
            decode(&binary_bytes(&42i64, &Type::INT8), false, INT),
            ScalarValue::Int64(Some(42))
        );
        assert_eq!(
            decode(
                &binary_bytes(&1.5f64, &Type::FLOAT8),
                false,
                Some(DataType::Float64)
            ),
            ScalarValue::Float64(Some(1.5))
        );
        assert_eq!(
            decode(
                &binary_bytes(&true, &Type::BOOL),
                false,
                Some(DataType::Boolean)
            ),
            ScalarValue::Boolean(Some(true))
        );
        assert_eq!(
            decode(&binary_bytes(&"hi".to_string(), &Type::TEXT), false, STR),
            ScalarValue::Utf8(Some("hi".into()))
        );
        let date = chrono::NaiveDate::from_ymd_opt(2024, 1, 15).unwrap();
        assert_eq!(
            decode(
                &binary_bytes(&date, &Type::DATE),
                false,
                Some(DataType::Date32)
            ),
            ScalarValue::Date32(Some(Date32Type::from_naive_date(date)))
        );
        let ts = chrono::NaiveDateTime::parse_from_str("2024-01-15 10:00:00", "%Y-%m-%d %H:%M:%S")
            .unwrap();
        assert_eq!(
            decode(
                &binary_bytes(&ts, &Type::TIMESTAMP),
                false,
                timestamp_type()
            ),
            ScalarValue::TimestampNanosecond(Some(1_705_312_800_000_000_000), None)
        );
        assert_eq!(
            decode(
                &binary_bytes(&vec![1u8, 2, 3], &Type::BYTEA),
                false,
                Some(DataType::Binary)
            ),
            ScalarValue::Binary(Some(vec![1, 2, 3]))
        );
    }

    fn timestamp_type() -> Option<DataType> {
        Some(DataType::Timestamp(TimeUnit::Nanosecond, None))
    }

    #[test]
    fn nulls_decode_to_typed_scalars() {
        assert_eq!(null_scalar(INT), ScalarValue::Int64(None));
        assert_eq!(null_scalar(STR), ScalarValue::Utf8(None));
        assert_eq!(
            null_scalar(timestamp_type()),
            ScalarValue::TimestampNanosecond(None, None)
        );
        assert_eq!(
            null_scalar(Some(DataType::Binary)),
            ScalarValue::Binary(None)
        );
        assert_eq!(null_scalar(None), ScalarValue::Utf8(None));
    }

    #[test]
    fn undecodable_values_are_clean_errors_not_panics() {
        // Text path: bad numbers, bad bools, bad dates, invalid UTF-8.
        assert!(decode_err(b"nope", true, INT).contains("$3"));
        assert!(decode_err(b"", true, INT).contains("cannot decode"));
        assert!(decode_err(b"maybe", true, Some(DataType::Boolean)).contains("cannot decode"));
        assert!(decode_err(b"31-02-2024", true, Some(DataType::Date32)).contains("cannot decode"));
        assert!(decode_err(b"\xff\xfe", true, INT).contains("UTF-8"));

        // Binary path: wrong lengths / garbage.
        assert!(decode_err(&[0u8; 3], false, INT).contains("cannot decode"));
        assert!(decode_err(&[0u8; 0], false, Some(DataType::Boolean)).contains("cannot decode"));
        // Bytea hex must be pairs.
        assert!(
            decode_err(b"\\xabc", true, Some(DataType::Binary)).contains("hex format"),
            "odd-length hex rejected"
        );
    }

    /// Property-style loop with deterministic pseudo-random blobs: decoding
    /// must either succeed or return an error — never panic.
    #[test]
    fn random_blobs_never_panic_the_decoder() {
        let mut seed: u64 = 0x1234_5678_9abc_def0;
        let mut next = move || {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            seed
        };
        let expected_types = [
            INT,
            Some(DataType::Int16),
            Some(DataType::Float32),
            Some(DataType::Boolean),
            Some(DataType::Date32),
            timestamp_type(),
            Some(DataType::Binary),
            STR,
            None,
        ];
        for i in 0..2000 {
            let len = (next() % 24) as usize;
            let blob: Vec<u8> = (0..len).map(|_| (next() & 0xff) as u8).collect();
            let is_text = next() % 2 == 0;
            let expected = expected_types[(next() as usize) % expected_types.len()].clone();
            let _ = decode_scalar_value(&blob, is_text, expected, i % 5);
        }
    }

    #[test]
    fn bytea_hex_round_trip() {
        assert_eq!(parse_bytea_hex("\\x48656c6c6f"), Some(b"Hello".to_vec()));
        assert_eq!(parse_bytea_hex("48656c6c6f"), Some(b"Hello".to_vec()));
        assert_eq!(parse_bytea_hex(""), Some(Vec::new()));
        assert_eq!(parse_bytea_hex("\\xzz"), None);
        assert_eq!(parse_bytea_hex("\\xabc"), None);
    }

    #[test]
    fn describe_parameter_precedence_declared_beats_inferred() {
        let declared = vec![Type::INT8, Type::UNKNOWN];
        let inferred = vec![Some(DataType::Utf8), Some(DataType::Int64)];
        let params = describe_parameters(&declared, &inferred);
        assert_eq!(params[0], Type::INT8, "declared OID wins");
        assert_eq!(params[1], Type::INT8, "UNKNOWN falls back to inference");
    }

    #[test]
    fn describe_parameters_fills_from_inference_and_defaults_to_unknown() {
        // Client declared nothing; server inferred one parameter.
        let params = describe_parameters(&[], &[Some(DataType::Float64)]);
        assert_eq!(params, vec![Type::FLOAT8]);

        // Nothing known anywhere: UNKNOWN (oid 0), not a fabricated type.
        let params = describe_parameters(&[Type::UNKNOWN], &[None]);
        assert_eq!(params, vec![Type::UNKNOWN]);

        // Declared count drives length even without inference.
        let params = describe_parameters(&[Type::INT4, Type::INT4], &[]);
        assert_eq!(params, vec![Type::INT4, Type::INT4]);
    }

    #[test]
    fn random_query_text_never_panics_and_normalizes_idempotently() {
        // Property-style loop over pseudo-random query text (deterministic
        // LCG): normalization, the cacheable-keyword gate, and cache round
        // trips must tolerate arbitrary client input without panicking, and
        // normalization must be idempotent so keys stay stable.
        let mut seed: u64 = 0x0dda_ba11_5eed_cafe;
        let mut next_byte = move || {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            (seed & 0xff) as u8
        };
        for _ in 0..2000 {
            let len = ((seed >> 8) % 40) as usize + 1;
            let blob: Vec<u8> = (0..len).map(|_| next_byte()).collect();
            let text = String::from_utf8_lossy(&blob).into_owned();
            let normalized = normalize_query(&text);
            assert_eq!(normalize_query(&normalized), normalized, "idempotent");
            let _ = is_cacheable(&text);

            let cache = Cache::new(8, std::time::Duration::from_secs(60));
            cache.set(&text, Vec::new());
            assert!(cache.get(&text).is_some());
        }
    }

    #[test]
    fn arrow_and_pg_param_types_round_trip() {
        for dt in [
            DataType::Int16,
            DataType::Int32,
            DataType::Int64,
            DataType::Float32,
            DataType::Float64,
            DataType::Utf8,
            DataType::Boolean,
            DataType::Date32,
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            DataType::Binary,
        ] {
            let pg = arrow_to_pg_type(&dt).expect("mapped");
            assert_eq!(pg_type_to_arrow(&pg), Some(dt.clone()), "{dt:?} round trip");
        }
        // Unmapped types have no spelling on either side.
        assert_eq!(arrow_to_pg_type(&DataType::Null), None);
        assert_eq!(pg_type_to_arrow(&Type::UUID), None);
    }
}
