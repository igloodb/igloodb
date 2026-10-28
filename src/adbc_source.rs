//! Generic ADBC-to-DataFusion adapter. Metadata and blocking C calls run off
//! the async executor; scans stream Arrow batches through a bounded channel.
use std::any::Any;
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex, OnceLock};

use adbc_core::driver_manager::{ManagedDatabase, ManagedDriver};
use adbc_core::options::{AdbcVersion, ObjectDepth, OptionConnection, OptionDatabase, OptionValue};
use adbc_core::{Connection, Database, Driver, Statement};
use arrow::array::{Array, ListArray, StringArray, StructArray};
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use datafusion::catalog::{CatalogProvider, Session, TableProvider};
use datafusion::catalog_common::memory::{MemoryCatalogProvider, MemorySchemaProvider};
use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::execution::TaskContext;
use datafusion::logical_expr::{Expr, TableType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::streaming::{PartitionStream, StreamingTableExec};
use datafusion::physical_plan::{ExecutionPlan, SendableRecordBatchStream};
use datafusion::prelude::SessionContext;

use crate::connectors::AdbcSource;
use crate::errors::{IglooError, Result};

// FFI Arrow buffers can outlive their session. Keep each trusted driver library
// loaded for the process lifetime so its buffer-release callbacks remain valid.
type DriverKey = (String, Option<String>);
type DriverRegistry = Mutex<HashMap<DriverKey, ManagedDriver>>;
static DRIVERS: OnceLock<DriverRegistry> = OnceLock::new();

fn source_error(source: &str, operation: &'static str, reason: impl Into<String>) -> IglooError {
    IglooError::Connector {
        source_name: source.into(),
        operation,
        reason: reason.into(),
    }
}

fn driver_error(
    source: &str,
    operation: &'static str,
    error: adbc_core::error::Error,
) -> IglooError {
    // Driver messages may echo URIs, passwords or tokens. Expose only the
    // standardized status and operation, never vendor-provided error text.
    source_error(source, operation, format!("ADBC status {:?}; check driver installation, metadata support, credentials and source permissions", error.status))
}

struct Runtime {
    config: AdbcSource,
    database: ManagedDatabase,
}

impl Runtime {
    fn open(config: AdbcSource) -> Result<Self> {
        let mut drivers = DRIVERS
            .get_or_init(|| Mutex::new(HashMap::new()))
            .lock()
            .map_err(|_| {
                source_error(
                    &config.name,
                    "driver loading",
                    "driver registry unavailable",
                )
            })?;
        let key = (config.driver.clone(), config.entrypoint.clone());
        if !drivers.contains_key(&key) {
            let driver = ManagedDriver::load_dynamic_from_filename(
                &config.driver,
                config.entrypoint.as_ref().map(|value| value.as_bytes()),
                AdbcVersion::V110,
            )
            .map_err(|error| driver_error(&config.name, "driver loading", error))?;
            drivers.insert(key.clone(), driver);
        }
        let mut driver = drivers.get(&key).expect("driver inserted above").clone();
        drop(drivers);
        let mut options: Vec<_> = config
            .options
            .iter()
            .map(|(key, value)| {
                (
                    OptionDatabase::from(key.as_str()),
                    OptionValue::from(value.expose()),
                )
            })
            .collect();
        if let Some(uri) = &config.uri {
            options.push((OptionDatabase::Uri, OptionValue::from(uri.expose())));
        }
        let database = driver
            .new_database_with_opts(options)
            .map_err(|error| driver_error(&config.name, "database initialization", error))?;
        Ok(Self { config, database })
    }

    fn connection(&self) -> Result<adbc_core::driver_manager::ManagedConnection> {
        let options = self.config.connection_options.iter().map(|(key, value)| {
            (
                OptionConnection::Other(key.clone()),
                OptionValue::from(value.expose()),
            )
        });
        self.database
            .clone()
            .new_connection_with_opts(options)
            .map_err(|error| driver_error(&self.config.name, "connection initialization", error))
    }
}

#[derive(Debug, Clone)]
struct Relation {
    catalog: Option<String>,
    schema: Option<String>,
    table: String,
}

fn metadata_error() -> IglooError {
    source_error(
        "adbc",
        "metadata decoding",
        "driver returned a non-standard GetObjects schema",
    )
}

fn typed<T: Array + 'static>(array: &dyn Array) -> Result<&T> {
    array
        .as_any()
        .downcast_ref::<T>()
        .ok_or_else(metadata_error)
}

fn text(array: &StringArray, index: usize) -> Option<String> {
    (!array.is_null(index)).then(|| array.value(index).to_string())
}

fn child<'a>(array: &'a StructArray, name: &str) -> Result<&'a dyn Array> {
    array
        .column_by_name(name)
        .map(|column| column.as_ref())
        .ok_or_else(metadata_error)
}

fn decode_objects(batch: &RecordBatch) -> Result<Vec<Relation>> {
    let catalogs: &StringArray = typed(
        batch
            .column_by_name("catalog_name")
            .ok_or_else(metadata_error)?
            .as_ref(),
    )?;
    let schemas: &ListArray = typed(
        batch
            .column_by_name("catalog_db_schemas")
            .ok_or_else(metadata_error)?
            .as_ref(),
    )?;
    let mut relations = Vec::new();
    for i in 0..batch.num_rows() {
        if schemas.is_null(i) {
            continue;
        }
        let schema_values = schemas.value(i);
        let schema_values: &StructArray = typed(schema_values.as_ref())?;
        let schema_names: &StringArray = typed(child(schema_values, "db_schema_name")?)?;
        let table_lists: &ListArray = typed(child(schema_values, "db_schema_tables")?)?;
        for j in 0..schema_values.len() {
            if schema_values.is_null(j) || table_lists.is_null(j) {
                continue;
            }
            let tables = table_lists.value(j);
            let tables: &StructArray = typed(tables.as_ref())?;
            let names: &StringArray = typed(child(tables, "table_name")?)?;
            let types: &StringArray = typed(child(tables, "table_type")?)?;
            for k in 0..tables.len() {
                if tables.is_null(k) || names.is_null(k) || types.is_null(k) {
                    continue;
                }
                if !["table", "base table"]
                    .iter()
                    .any(|kind| types.value(k).eq_ignore_ascii_case(kind))
                {
                    continue;
                }
                relations.push(Relation {
                    catalog: text(catalogs, i),
                    schema: text(schema_names, j),
                    table: names.value(k).into(),
                });
            }
        }
    }
    Ok(relations)
}

fn discover(config: AdbcSource) -> Result<Vec<(String, String, Arc<AdbcTable>)>> {
    let runtime = Arc::new(Runtime::open(config)?);
    let connection = runtime.connection()?;
    let config = &runtime.config;
    let single_schema = config
        .schemas
        .as_ref()
        .filter(|schemas| schemas.len() == 1)
        .map(|schemas| schemas[0].as_str());
    let single_table = config
        .tables
        .as_ref()
        .filter(|tables| tables.len() == 1)
        .map(|tables| tables[0].as_str());
    let reader = connection
        .get_objects(
            ObjectDepth::Tables,
            config.catalog.as_deref(),
            single_schema,
            single_table,
            None,
            None,
        )
        .map_err(|error| driver_error(&config.name, "table discovery", error))?;
    let mut relations = Vec::new();
    for batch in reader {
        let batch = batch.map_err(|_| {
            source_error(
                &config.name,
                "table discovery",
                "could not read metadata batches",
            )
        })?;
        for relation in decode_objects(&batch)? {
            // Metadata filters can be SQL LIKE patterns. Recheck exact names
            // locally so wildcards never widen configured access allowlists.
            if config
                .catalog
                .as_ref()
                .is_some_and(|catalog| relation.catalog.as_deref().unwrap_or("") != catalog)
            {
                continue;
            }
            if config.schemas.as_ref().is_some_and(|schemas| {
                !schemas
                    .iter()
                    .any(|name| Some(name) == relation.schema.as_ref())
            }) {
                continue;
            }
            if config
                .tables
                .as_ref()
                .is_some_and(|tables| !tables.contains(&relation.table))
            {
                continue;
            }
            relations.push(relation);
            if relations.len() > 10_000 {
                return Err(source_error(
                    &config.name,
                    "table discovery",
                    "more than 10000 tables; narrow catalog/schema/table filters",
                ));
            }
        }
    }
    let catalogs: HashSet<_> = relations.iter().map(|table| &table.catalog).collect();
    if catalogs.len() > 1 {
        return Err(source_error(&config.name, "table discovery", "multiple remote catalogs found; set catalog explicitly and configure one source per remote database"));
    }
    if config.tables.as_ref().is_some_and(|tables| {
        tables
            .iter()
            .any(|name| !relations.iter().any(|relation| &relation.table == name))
    }) {
        return Err(source_error(
            &config.name,
            "table discovery",
            "a configured table was not found; check catalog, schemas and exact table names",
        ));
    }
    let mut tables = Vec::new();
    let mut names = HashSet::new();
    for relation in relations {
        let schema = connection
            .get_table_schema(
                relation.catalog.as_deref(),
                relation.schema.as_deref(),
                &relation.table,
            )
            .map_err(|error| driver_error(&config.name, "table schema discovery", error))?;
        let local_schema = relation
            .schema
            .as_ref()
            .filter(|name| !name.is_empty())
            .cloned()
            .unwrap_or_else(|| "public".into());
        if !names.insert((local_schema.clone(), relation.table.clone())) {
            return Err(source_error(
                &config.name,
                "table discovery",
                "duplicate local table identity; narrow the remote catalog/schema filters",
            ));
        }
        tables.push((
            local_schema,
            relation.table.clone(),
            Arc::new(AdbcTable {
                runtime: runtime.clone(),
                relation,
                schema: Arc::new(schema),
            }),
        ));
    }
    Ok(tables)
}

pub async fn register(ctx: &SessionContext, sources: &[AdbcSource]) -> Result<()> {
    for source in sources {
        if ctx.catalog(&source.name).is_some() {
            return Err(source_error(
                &source.name,
                "catalog registration",
                "catalog name already exists",
            ));
        }
        let config = source.clone();
        let tables = tokio::task::spawn_blocking(move || discover(config))
            .await
            .map_err(|_| source_error(&source.name, "table discovery", "driver worker failed"))??;
        let catalog = Arc::new(MemoryCatalogProvider::new());
        for (schema_name, table_name, table) in tables {
            if catalog.schema(&schema_name).is_none() {
                catalog.register_schema(&schema_name, Arc::new(MemorySchemaProvider::new()))?;
            }
            catalog
                .schema(&schema_name)
                .expect("registered schema")
                .register_table(table_name, table)?;
        }
        ctx.register_catalog(source.name.clone(), catalog);
    }
    Ok(())
}

pub struct AdbcTable {
    runtime: Arc<Runtime>,
    relation: Relation,
    schema: SchemaRef,
}

impl std::fmt::Debug for AdbcTable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AdbcTable")
            .field("source", &self.runtime.config.name)
            .field("table", &self.relation.table)
            .finish_non_exhaustive()
    }
}

impl AdbcTable {
    fn scan_sql(&self, projection: &[usize]) -> Result<String> {
        let quote = |name: &str| self.runtime.config.dialect.quote(name);
        let relation = self
            .relation
            .catalog
            .iter()
            .chain(self.relation.schema.iter())
            .filter(|name| !name.is_empty())
            .map(|name| name.as_str())
            .chain(std::iter::once(self.relation.table.as_str()))
            .map(quote)
            .collect::<Result<Vec<_>>>()?
            .join(".");
        let columns = if projection.is_empty() {
            "1 AS __igloo_row".into()
        } else {
            projection
                .iter()
                .map(|index| quote(self.schema.field(*index).name()))
                .collect::<Result<Vec<_>>>()?
                .join(", ")
        };
        Ok(format!("SELECT {columns} FROM {relation}"))
    }
}

#[async_trait]
impl TableProvider for AdbcTable {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        let indices = projection
            .cloned()
            .unwrap_or_else(|| (0..self.schema.fields().len()).collect());
        let schema = Arc::new(self.schema.project(&indices)?);
        let sql = self.scan_sql(&indices).map_err(external)?;
        let partition = Arc::new(AdbcPartition {
            runtime: self.runtime.clone(),
            sql,
            schema: schema.clone(),
            empty_projection: indices.is_empty(),
        });
        // Filters are Unsupported (the TableProvider default). DataFusion
        // evaluates them locally, avoiding cross-dialect semantic guesses.
        let limit = if filters.is_empty() { limit } else { None };
        Ok(Arc::new(StreamingTableExec::try_new(
            schema,
            vec![partition],
            None,
            [],
            false,
            limit,
        )?))
    }
}

fn external(error: IglooError) -> DataFusionError {
    DataFusionError::External(Box::new(error))
}

struct AdbcPartition {
    runtime: Arc<Runtime>,
    sql: String,
    schema: SchemaRef,
    empty_projection: bool,
}

impl std::fmt::Debug for AdbcPartition {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AdbcPartition")
            .field("source", &self.runtime.config.name)
            .finish_non_exhaustive()
    }
}

impl PartitionStream for AdbcPartition {
    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    fn execute(&self, _ctx: Arc<TaskContext>) -> SendableRecordBatchStream {
        let (sender, receiver) = tokio::sync::mpsc::channel(2);
        let runtime = self.runtime.clone();
        let schema = self.schema.clone();
        let sql = self.sql.clone();
        let empty = self.empty_projection;
        let source_name = runtime.config.name.clone();
        tokio::spawn(async move {
            let worker_sender = sender.clone();
            let worker = tokio::task::spawn_blocking(move || -> Result<()> {
                if worker_sender.is_closed() {
                    return Ok(());
                }
                let mut connection = runtime.connection()?;
                let mut statement = connection.new_statement().map_err(|error| {
                    driver_error(&source_name, "statement initialization", error)
                })?;
                statement
                    .set_sql_query(&sql)
                    .map_err(|error| driver_error(&source_name, "scan planning", error))?;
                let reader = statement
                    .execute()
                    .map_err(|error| driver_error(&source_name, "scan execution", error))?;
                for batch in reader {
                    if worker_sender.is_closed() {
                        break;
                    }
                    let batch = batch.map_err(|_| {
                        source_error(
                            &source_name,
                            "scan streaming",
                            "driver failed while reading result batches",
                        )
                    })?;
                    let batch = if empty {
                        batch.project(&[])?
                    } else {
                        RecordBatch::try_new(schema.clone(), batch.columns().to_vec())
                            .map_err(|_| source_error(&source_name, "scan streaming", "result schema changed; refresh source metadata before retrying"))?
                    };
                    if worker_sender.blocking_send(Ok(batch)).is_err() {
                        break;
                    }
                }
                Ok(())
            });
            let error = match worker.await {
                Ok(Ok(())) => None,
                Ok(Err(error)) => Some(external(error)),
                Err(_) => Some(DataFusionError::Execution(
                    "ADBC worker failed; driver details redacted".into(),
                )),
            };
            if let Some(error) = error {
                let _ = sender.send(Err(error)).await;
            }
        });
        let stream = futures::stream::unfold(receiver, |mut receiver| async move {
            receiver.recv().await.map(|batch| (batch, receiver))
        });
        Box::pin(RecordBatchStreamAdapter::new(self.schema.clone(), stream))
    }
}
