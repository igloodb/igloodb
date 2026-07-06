// src/adbc_postgres.rs
use adbc_core::driver_manager::ManagedDriver;
use adbc_core::options::{AdbcVersion, OptionDatabase};
use adbc_core::{Connection, Database, Driver, Statement};
use arrow::array::{
    Array, BooleanArray, Date32Array, Float64Array, GenericBinaryArray, Int32Array, StringArray,
    TimestampNanosecondArray,
};
use arrow::datatypes::{DataType, TimeUnit};
use arrow::record_batch::RecordBatch;

// Using our project's error types
use crate::errors::{IglooError, Result};

pub async fn adbc_postgres_query_example(uri: &str, sql: &str) -> Result<()> {
    // The URI is deliberately not logged: it may embed credentials.
    log::info!(target: "adbc_example", "Attempting ADBC query. SQL: {}", sql);

    // Load the Postgres ADBC driver dynamically
    // Ensure the .so/.dylib/.dll is in your LD_LIBRARY_PATH/DYLD_LIBRARY_PATH/PATH
    // Using V110 which is common for newer drivers.
    let mut driver =
        ManagedDriver::load_dynamic_from_name("adbc_driver_postgresql", None, AdbcVersion::V110)?;

    let opts = [(OptionDatabase::Uri, uri.into())];
    let mut database = driver.new_database_with_opts(opts)?;

    let mut connection = database.new_connection()?;
    log::info!(target: "adbc_example", "ADBC connection established.");

    let mut statement = connection.new_statement()?;
    statement.set_sql_query(sql)?;

    let reader = statement.execute()?;

    log::info!(target: "adbc_example", "ADBC statement executed. Reading results for SQL: {}", sql);

    // The reader is a RecordBatchReader, i.e. an iterator of RecordBatch results.
    let collected_batches_result: std::result::Result<Vec<RecordBatch>, arrow::error::ArrowError> =
        reader.collect();

    match collected_batches_result {
        Ok(collected_batches) => {
            if collected_batches.is_empty() {
                log::info!(target: "adbc_example", "Query returned no record batches.");
            } else {
                log::info!(target: "adbc_example", "Query returned {} record batch(es).", collected_batches.len());
                for (i, batch) in collected_batches.iter().enumerate() {
                    log::info!(target: "adbc_example", "Printing Batch {}:", i + 1);
                    print_arrow_batch(batch)?;
                }
            }
        }
        Err(arrow_error) => {
            log::error!(target: "adbc_example", "Failed to collect record batches: {}", arrow_error);
            return Err(IglooError::Arrow(arrow_error));
        }
    }

    // According to ADBC spec, statement, connection, and database should be closed.
    // This happens when they go out of scope due to RAII (drop trait implementation).
    // Explicit close calls are available if needed for more precise resource management.
    // statement.close()?;
    // connection.close()?;
    // database.close()?;

    log::info!(target: "adbc_example", "ADBC query example completed successfully for SQL: {}", sql);
    Ok(())
}

fn print_arrow_batch(batch: &RecordBatch) -> Result<()> {
    if batch.num_rows() == 0 {
        // Using log::info instead of println for consistency if this becomes part of library code
        log::info!(target: "adbc_print", "Batch is empty.");
        return Ok(());
    }
    let schema = batch.schema();
    println!("--- Batch ({} rows) ---", batch.num_rows());
    for col_idx in 0..batch.num_columns() {
        let col_name = schema.field(col_idx).name();
        let data_type = schema.field(col_idx).data_type();
        print!("Column '{}' ({}): [", col_name, data_type);
        let array = batch.column(col_idx);

        for row_idx in 0..array.len() {
            if array.is_null(row_idx) {
                print!("NULL");
            } else {
                match data_type {
                    DataType::Int32 => print!(
                        "{}",
                        array
                            .as_any()
                            .downcast_ref::<Int32Array>()
                            .unwrap()
                            .value(row_idx)
                    ),
                    DataType::Float64 => print!(
                        "{}",
                        array
                            .as_any()
                            .downcast_ref::<Float64Array>()
                            .unwrap()
                            .value(row_idx)
                    ),
                    DataType::Utf8 => print!(
                        "'{}'",
                        array
                            .as_any()
                            .downcast_ref::<StringArray>()
                            .unwrap()
                            .value(row_idx)
                    ),
                    DataType::Timestamp(TimeUnit::Nanosecond, tz_opt) => {
                        let val = array
                            .as_any()
                            .downcast_ref::<TimestampNanosecondArray>()
                            .unwrap()
                            .value(row_idx);
                        // Naive formatting for now
                        print!(
                            "{}ns{}",
                            val,
                            tz_opt
                                .as_ref()
                                .map_or("".to_string(), |s| format!(" ({})", s))
                        );
                    }
                    DataType::Boolean => print!(
                        "{}",
                        array
                            .as_any()
                            .downcast_ref::<BooleanArray>()
                            .unwrap()
                            .value(row_idx)
                    ),
                    DataType::Date32 => print!(
                        "{}d",
                        array
                            .as_any()
                            .downcast_ref::<Date32Array>()
                            .unwrap()
                            .value(row_idx)
                    ), // days since epoch
                    DataType::Binary => {
                        let val = array
                            .as_any()
                            .downcast_ref::<GenericBinaryArray<i32>>()
                            .unwrap()
                            .value(row_idx);
                        print!("[binary data: {} bytes]", val.len());
                    }
                    other => {
                        log::warn!(target: "adbc_print", "Unsupported data type for printing: {:?}", other);
                        print!("[unsupported: {:?}]", other);
                    }
                }
            }
            if row_idx < array.len() - 1 {
                print!(", ");
            }
        }
        println!("]");
    }
    println!("--- End Batch ---");
    Ok(())
}
