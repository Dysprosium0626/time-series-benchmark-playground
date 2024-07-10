use std::{fs::File, path::PathBuf};

use arrow::array::RecordBatch;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use snafu::{location, Location, ResultExt};

use crate::common::error::{
    ArrowFileSnafu, EndOfParquetFileSnafu, InvalidFilePathSnafu, ReadParquetFileSnafu, Result,
};
use crate::generator::data_generator::UseCase;
use crate::usql::usql::Usql;

pub struct DataLoaderConfig {
    pub use_case: UseCase,
}

pub trait DataLoader {
    // Load data by Usql with auto-generated insert statement
    fn load_data_by_usql(&self, usql: Option<Usql>);
    fn load_data_from_raw_sql(&self, usql: Option<Usql>, raw_sql: &str);
    fn load_data_from_parquet_file(
        &self,
    ) -> impl std::future::Future<Output = Result<Vec<u32>>> + Send;
}

// Execute SQL statement via usql
pub fn execute_sql(usql_conn: &mut Usql, sql: &str) {
    match usql_conn.execute(sql) {
        Ok(result) => println!("Success: {:?}", result),
        Err(e) => eprintln!("Error executing query: {:?}", e),
    }
}

// Generate create table statement, currently we only support GreptimeDB dialect
fn gen_create_table_stmt(hypertable: &str, cols: &[&str]) -> String {
    let mut pk: Option<&str> = None;
    let mut columns_def: Vec<String> = cols
        .iter()
        .map(|&col| {
            if col == "tag" {
                pk = Some(col);
                format!("{} STRING", col)
            } else if col == "ts" {
                format!("{} TIMESTAMP DEFAULT CURRENT_TIMESTAMP() TIME INDEX", col)
            } else {
                format!("{} STRING", col)
            }
        })
        .collect();
    let pk_def = pk
        .map(|pk| format!("PRIMARY KEY ({})", pk))
        .unwrap_or_default();
    columns_def.push(pk_def);
    format!("CREATE TABLE {} ({});", hypertable, columns_def.join(", "))
}

// Generate insert statement, currently we only support GreptimeDB dialect
fn gen_insert_stmt(hypertable: &str, cols: &[&str], data: &[Vec<String>]) -> String {
    let mut insert_stmt = format!("INSERT INTO {}({}) VALUES", hypertable, cols.join(","));

    for (i, row) in data.iter().enumerate() {
        let values = row
            .iter()
            .map(|value| format!("'{}'", value.replace('\'', "''")))
            .collect::<Vec<_>>()
            .join(",");

        insert_stmt.push_str(&format!("{} ({})", if i == 0 { "" } else { "," }, values));
    }
    insert_stmt.push(';');

    insert_stmt
}

// Read parquet file and return RecordBatch
pub fn read_parquet_file(path: PathBuf) -> Result<RecordBatch> {
    let file = File::open(path).context(InvalidFilePathSnafu {
        location: location!(),
    })?;
    let builder =
        ParquetRecordBatchReaderBuilder::try_new(file).context(ReadParquetFileSnafu {})?;
    let mut reader = builder.build().context(ReadParquetFileSnafu {})?;
    let record_batch = reader
        .next()
        .transpose()
        .context(ArrowFileSnafu {})?
        .ok_or_else(|| EndOfParquetFileSnafu {}.build())?;
    Ok(record_batch)
}
