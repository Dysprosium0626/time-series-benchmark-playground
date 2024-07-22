use std::path::PathBuf;

use arrow::{
    array::{RecordBatch, StringArray},
    datatypes::{DataType, TimeUnit},
};
use greptime_proto::v1::{ColumnSchema, Row, RowInsertRequest, RowInsertRequests, Rows};

use crate::{
    client::greptime,
    generator::{
        data_generator::{DataGenerator, UseCase},
        log_data_generator::LogDataGenerator,
    },
    loader::data_loader::read_parquet_file,
    usql::usql::Usql,
};

use super::{
    data_loader::{execute_sql, DataLoader, DataLoaderConfig},
    i32_value, string_value, timestamp_microsecond_value,
};

use crate::common::error::Result;

pub struct GreptimeDataLoader {
    pub config: DataLoaderConfig,
    pub client: greptime::DatabaseClient,
}

impl GreptimeDataLoader {
    pub fn new(use_case: UseCase, client: greptime::DatabaseClient) -> Self {
        GreptimeDataLoader {
            config: DataLoaderConfig { use_case },
            client,
        }
    }
    pub fn config(&self) -> &DataLoaderConfig {
        &self.config
    }
}

impl DataLoader for GreptimeDataLoader {
    fn load_data_by_usql(&self, usql: Option<Usql>) {
        // TODO(Yue): Generate sql string from parquet file
        // let mut usql_conn = usql.unwrap_or_else(|| Usql::new("mysql://127.0.0.1:4002"));

        // let file = File::open("./data.gz").expect("Failed to open file");
        // let gz = flate2::read::GzDecoder::new(file);
        // let mut reader = BufReader::new(gz);

        // // 读取 header
        // let mut header = String::new();
        // reader
        //     .read_line(&mut header)
        //     .expect("Failed to read header");
        // let header = header.trim_end();
        // let cols: Vec<&str> = header.split(',').collect();

        // let create_table_stmt = gen_create_table_stmt("measurement", &cols);
        // execute_sql(&mut usql_conn, &create_table_stmt);

        // // 读取数据行
        // let data: Vec<Vec<String>> = reader
        //     .lines()
        //     .map(|line| {
        //         line.expect("Failed to read line")
        //             .split(',')
        //             .map(|v| v.trim().to_string())
        //             .collect()
        //     })
        //     .collect();

        // let insert_stmt = gen_insert_stmt("measurement", &cols, &data);
        // execute_sql(&mut usql_conn, &insert_stmt);
    }

    fn load_data_from_raw_sql(&self, usql: Option<Usql>, raw_sql: &str) {
        let mut usql_conn = usql.unwrap_or_else(|| Usql::new("mysql://127.0.0.1:4002"));
        execute_sql(&mut usql_conn, raw_sql);
    }

    async fn load_data_from_parquet_file(&self) -> Result<Vec<u32>> {
        // Read parquet file
        let mut res = Vec::new();
        for table_name in LogDataGenerator::table_names() {
            let path = PathBuf::from(format!("{}.parquet", table_name));
            let record_batch = read_parquet_file(path)?;
            let schema = match self.config.use_case {
                UseCase::Log => LogDataGenerator::schema(table_name),
                UseCase::Others => unimplemented!(),
            };
            let insert_request = record_batch_to_insert_request(record_batch, table_name, schema)?;
            let affected_rows = self.client.row_insert(insert_request).await?;
            res.push(affected_rows);
        }
        Ok(res)
    }
}

// Generate Insert grpc from GreptimeDB from RecordBatch
fn record_batch_to_insert_request(
    record_batch: RecordBatch,
    table_name: &str,
    schema: Vec<ColumnSchema>,
) -> Result<RowInsertRequests> {
    let mut rows = Vec::new();

    for row_index in 0..record_batch.num_rows() {
        let mut values = Vec::new();

        for col in record_batch.columns() {
            let value = match col.data_type() {
                DataType::Utf8 => {
                    let array = col.as_any().downcast_ref::<StringArray>().unwrap();
                    string_value(array.value(row_index).to_string())
                }
                DataType::Int32 => {
                    let array = col
                        .as_any()
                        .downcast_ref::<arrow::array::Int32Array>()
                        .unwrap();
                    i32_value(array.value(row_index))
                }
                DataType::Timestamp(TimeUnit::Microsecond, None) => {
                    let array = col
                        .as_any()
                        .downcast_ref::<arrow::array::TimestampMicrosecondArray>()
                        .unwrap();
                    timestamp_microsecond_value(array.value(row_index))
                }
                _ => {
                    unimplemented!()
                }
            };
            values.push(value);
        }

        rows.push(Row { values });
    }

    Ok(RowInsertRequests {
        inserts: vec![RowInsertRequest {
            table_name: table_name.to_string(),
            rows: Some(Rows { schema, rows }),
        }],
    })
}

#[cfg(test)]
mod tests {
    // use std::path::PathBuf;

    // use crate::client::greptime::DatabaseClient;
    // use crate::common::error::Result;
    // use crate::generator::data_generator::UseCase;
    // use crate::loader::data_loader::DataLoader;
    // use crate::loader::greptime_data_loader::GreptimeDataLoader;

    // #[tokio::test]
    // async fn test_row_insert() -> Result<()> {
    //     let client = DatabaseClient::new("my_db_demo".to_string()).await?;
    //     let loader = GreptimeDataLoader::new(UseCase::Log, client);
    //     let path = PathBuf::from("./data.parquet");
    //     let affected_rows = loader.load_data_from_parquet_file(path).await?;
    //     assert!(affected_rows > 0, "Affected rows should greater than 0");
    //     Ok(())
    // }
}
