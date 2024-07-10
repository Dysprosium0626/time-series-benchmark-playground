use crate::{
    common::error::{ArrowFileSnafu, Result, WriteParquetFileSnafu},
    loader::{field, tag, timestamp},
};
use arrow::{
    array::{Int32Array, RecordBatch, StringArray, TimestampMicrosecondArray},
    datatypes::{DataType, Field, Schema, TimeUnit},
    error::ArrowError,
};
use greptime_proto::v1::{ColumnDataType, ColumnSchema};
use parquet::{arrow::ArrowWriter, file::properties::WriterProperties};
use snafu::ResultExt;
use std::{fs::File, iter::zip, sync::Arc};

use super::data_generator::{DataGenerator, DataGeneratorConfig};

/// LogData with schema for different tables
pub struct LogData {
    users_schema: Arc<Schema>,
    pages_schema: Arc<Schema>,
    devices_schema: Arc<Schema>,
    web_logs_schema: Arc<Schema>,
    requests_schema: Arc<Schema>,
    responses_schema: Arc<Schema>,
    sessions_schema: Arc<Schema>,
    error_logs_schema: Arc<Schema>,
}

impl LogData {
    pub fn new() -> Self {
        LogData {
            users_schema: Arc::new(Schema::new(vec![
                Field::new("user_id", DataType::Int32, false),
                Field::new("username", DataType::Utf8, false),
                Field::new("email", DataType::Utf8, false),
                Field::new(
                    "signup_date",
                    DataType::Timestamp(TimeUnit::Microsecond, None),
                    false,
                ),
            ])),
            pages_schema: Arc::new(Schema::new(vec![
                Field::new("page_id", DataType::Int32, false),
                Field::new("page_url", DataType::Utf8, false),
                Field::new("page_title", DataType::Utf8, false),
                Field::new(
                    "created_date",
                    DataType::Timestamp(TimeUnit::Microsecond, None),
                    false,
                ),
            ])),
            devices_schema: Arc::new(Schema::new(vec![
                Field::new("device_id", DataType::Int32, false),
                Field::new("device_type", DataType::Utf8, false),
                Field::new("os", DataType::Utf8, false),
                Field::new("browser", DataType::Utf8, false),
            ])),
            web_logs_schema: Arc::new(Schema::new(vec![
                Field::new("log_id", DataType::Int32, false),
                Field::new("user_id", DataType::Int32, false),
                Field::new("page_id", DataType::Int32, false),
                Field::new("device_id", DataType::Int32, false),
                Field::new("ip_address", DataType::Utf8, false),
                Field::new(
                    "timestamp",
                    DataType::Timestamp(TimeUnit::Microsecond, None),
                    false,
                ),
            ])),
            requests_schema: Arc::new(Schema::new(vec![
                Field::new("request_id", DataType::Int32, false),
                Field::new("log_id", DataType::Int32, false),
                Field::new("method", DataType::Utf8, false),
                Field::new("url", DataType::Utf8, false),
                Field::new("http_version", DataType::Utf8, false),
            ])),
            responses_schema: Arc::new(Schema::new(vec![
                Field::new("response_id", DataType::Int32, false),
                Field::new("log_id", DataType::Int32, false),
                Field::new("status_code", DataType::Int32, false),
                Field::new("response_size", DataType::Int32, false),
                Field::new("response_time", DataType::Int32, false),
            ])),
            sessions_schema: Arc::new(Schema::new(vec![
                Field::new("session_id", DataType::Int32, false),
                Field::new("user_id", DataType::Int32, false),
                Field::new(
                    "start_time",
                    DataType::Timestamp(TimeUnit::Microsecond, None),
                    false,
                ),
                Field::new(
                    "end_time",
                    DataType::Timestamp(TimeUnit::Microsecond, None),
                    false,
                ),
                Field::new("ip_address", DataType::Utf8, false),
            ])),
            error_logs_schema: Arc::new(Schema::new(vec![
                Field::new("error_log_id", DataType::Int32, false),
                Field::new("log_id", DataType::Int32, false),
                Field::new("error_code", DataType::Int32, false),
                Field::new("error_message", DataType::Utf8, false),
                Field::new(
                    "timestamp",
                    DataType::Timestamp(TimeUnit::Microsecond, None),
                    false,
                ),
            ])),
        }
    }
}

/// LogDataGenerator, the schema of Log Data can be referred to
/// Web Server Access Logs https://www.kaggle.com/datasets/eliasdabbas/web-server-access-logs/
pub struct LogDataGenerator {
    pub config: DataGeneratorConfig,
    pub log_data: LogData,
}

impl DataGenerator for LogDataGenerator {
    fn generate(&self) -> Result<Vec<RecordBatch>> {
        // Generate data for different tables
        let user_id = Int32Array::from(vec![1]);
        let username = StringArray::from(vec!["user1"]);
        let email = StringArray::from(vec!["user1@example.com"]);
        let signup_date = TimestampMicrosecondArray::from(vec![1548114974000000]);

        let users_batch = RecordBatch::try_new(
            self.log_data.users_schema.clone(),
            vec![
                Arc::new(user_id.clone()),
                Arc::new(username),
                Arc::new(email),
                Arc::new(signup_date.clone()),
            ],
        )
        .context(ArrowFileSnafu {})?;

        let page_id = Int32Array::from(vec![1]);
        let page_url = StringArray::from(vec!["http://example.com/page1"]);
        let page_title = StringArray::from(vec!["Page 1"]);
        let created_date = TimestampMicrosecondArray::from(vec![1548114974000000]);

        let pages_batch = RecordBatch::try_new(
            self.log_data.pages_schema.clone(),
            vec![
                Arc::new(page_id.clone()),
                Arc::new(page_url),
                Arc::new(page_title),
                Arc::new(created_date),
            ],
        )
        .context(ArrowFileSnafu {})?;

        let device_id = Int32Array::from(vec![1]);
        let device_type = StringArray::from(vec!["mobile"]);
        let os = StringArray::from(vec!["Android 6.0"]);
        let browser = StringArray::from(vec!["Chrome 66.0.3359.158"]);

        let devices_batch = RecordBatch::try_new(
            self.log_data.devices_schema.clone(),
            vec![
                Arc::new(device_id.clone()),
                Arc::new(device_type),
                Arc::new(os),
                Arc::new(browser),
            ],
        )
        .context(ArrowFileSnafu {})?;

        let log_id = Int32Array::from(vec![1]);
        let ip_address = StringArray::from(vec!["54.36.149.41"]);
        let timestamp = TimestampMicrosecondArray::from(vec![1548114974000000]);

        let web_logs_batch = RecordBatch::try_new(
            self.log_data.web_logs_schema.clone(),
            vec![
                Arc::new(log_id.clone()),
                Arc::new(user_id.clone()),
                Arc::new(page_id.clone()),
                Arc::new(device_id.clone()),
                Arc::new(ip_address),
                Arc::new(timestamp.clone()),
            ],
        )
        .context(ArrowFileSnafu {})?;

        let request_method = StringArray::from(vec!["GET"]);
        let request_url = StringArray::from(vec!["/filter/27|13%20%D9%85%DA%AF%D8%A7%D9%BE%DB%8C%DA%A9%D8%B3%D9%84,27|%DA%A9%D9%85%D8%AA%D8%B1%20%D8%A7%D8%B2%205%20%D9%85%DA%AF%D8%A7%D9%BE%DB%8C%DA%A9%D8%B3%D9%84,p53"]);
        let http_version = StringArray::from(vec!["HTTP/1.1"]);

        let requests_batch = RecordBatch::try_new(
            self.log_data.requests_schema.clone(),
            vec![
                Arc::new(log_id.clone()),
                Arc::new(log_id.clone()),
                Arc::new(request_method),
                Arc::new(request_url),
                Arc::new(http_version),
            ],
        )
        .context(ArrowFileSnafu {})?;

        let response_code = Int32Array::from(vec![200]);
        let response_size = Int32Array::from(vec![30577]);
        let response_time = Int32Array::from(vec![123]);

        let responses_batch = RecordBatch::try_new(
            self.log_data.responses_schema.clone(),
            vec![
                Arc::new(log_id.clone()),
                Arc::new(log_id.clone()),
                Arc::new(response_code),
                Arc::new(response_size),
                Arc::new(response_time),
            ],
        )
        .context(ArrowFileSnafu {})?;

        // Return all batches
        Ok(vec![
            users_batch,
            pages_batch,
            devices_batch,
            web_logs_batch,
            requests_batch,
            responses_batch,
        ])
    }

    fn write(&self, to_write: Vec<RecordBatch>, file: File) -> Result<()> {
        let props = WriterProperties::builder().build();

        for (batch, table_name) in zip(to_write.iter(), Self::table_names()) {
            let file_name = format!("{}.parquet", table_name);
            let file = File::create(&file_name).expect("Unable to create file");

            let mut parquet_writer =
                ArrowWriter::try_new(file, batch.schema(), Some(props.clone()))
                    .context(WriteParquetFileSnafu {})?;

            parquet_writer
                .write(batch)
                .context(WriteParquetFileSnafu {})?;

            parquet_writer.close().context(WriteParquetFileSnafu {})?;
        }

        Ok(())
    }

    fn schema(table_name: &str) -> Vec<ColumnSchema> {
        match table_name {
            "users" => vec![
                tag("user_id", ColumnDataType::Int32),
                field("username", ColumnDataType::String),
                field("email", ColumnDataType::String),
                timestamp("signup_date", ColumnDataType::TimestampMicrosecond),
            ],
            "pages" => vec![
                tag("page_id", ColumnDataType::Int32),
                field("page_url", ColumnDataType::String),
                field("page_title", ColumnDataType::String),
                timestamp("created_date", ColumnDataType::TimestampMicrosecond),
            ],
            "devices" => vec![
                tag("device_id", ColumnDataType::Int32),
                field("device_type", ColumnDataType::String),
                field("os", ColumnDataType::String),
                field("browser", ColumnDataType::String),
            ],
            "web_logs" => vec![
                tag("log_id", ColumnDataType::Int32),
                field("user_id", ColumnDataType::Int32),
                field("page_id", ColumnDataType::Int32),
                field("device_id", ColumnDataType::Int32),
                field("ip_address", ColumnDataType::String),
                timestamp("timestamp", ColumnDataType::TimestampMicrosecond),
            ],
            "requests" => vec![
                tag("request_id", ColumnDataType::Int32),
                field("log_id", ColumnDataType::Int32),
                field("method", ColumnDataType::String),
                field("url", ColumnDataType::String),
                field("http_version", ColumnDataType::String),
            ],
            "responses" => vec![
                tag("response_id", ColumnDataType::Int32),
                field("log_id", ColumnDataType::Int32),
                field("status_code", ColumnDataType::Int32),
                field("response_size", ColumnDataType::Int32),
                field("response_time", ColumnDataType::Int32),
            ],
            "sessions" => vec![
                tag("session_id", ColumnDataType::Int32),
                field("user_id", ColumnDataType::Int32),
                timestamp("start_time", ColumnDataType::TimestampMicrosecond),
                timestamp("end_time", ColumnDataType::TimestampMicrosecond),
                field("ip_address", ColumnDataType::String),
            ],
            "error_logs" => vec![
                tag("error_log_id", ColumnDataType::Int32),
                field("log_id", ColumnDataType::Int32),
                field("error_code", ColumnDataType::Int32),
                field("error_message", ColumnDataType::String),
                timestamp("timestamp", ColumnDataType::TimestampMicrosecond),
            ],
            _ => vec![],
        }
    }

    fn table_name(table_name: &str) -> &'static str {
        match table_name {
            "users" => "users",
            "pages" => "pages",
            "devices" => "devices",
            "web_logs" => "web_logs",
            "requests" => "requests",
            "responses" => "responses",
            "sessions" => "sessions",
            "error_logs" => "error_logs",
            _ => "unknown",
        }
    }

    fn table_names() -> Vec<&'static str> {
        vec![
            "users",
            "pages",
            "devices",
            "web_logs",
            "requests",
            "responses",
            "sessions",
            "error_logs",
        ]
    }
}

#[cfg(test)]
mod tests {

    use crate::generator::{
        data_generator::UseCase,
        log_data_generator::{LogData, LogDataGenerator},
    };
    use std::io::{Read, Seek, SeekFrom};
    use tempfile::tempfile;

    use super::{DataGenerator, DataGeneratorConfig};

    // #[test]
    // fn test_data_generator_output() {
    //     let log_data_generator = LogDataGenerator {
    //         config: DataGeneratorConfig::new(
    //             60,
    //             "2021-01-01T00:00:00Z".to_string(),
    //             "2021-01-01T01:00:00Z".to_string(),
    //             123,
    //             10,
    //             UseCase::Log,
    //         ),
    //         log_data: LogData::new(),
    //     };

    //     let record_batch = log_data_generator
    //         .generate()
    //         .expect("Failed to generate record batch");

    //     let mut file = tempfile().expect("Failed to create temporary file");

    //     log_data_generator
    //         .write(&record_batch, file.try_clone().unwrap())
    //         .expect("Failed to write record batch to file");

    //     let mut buffer = Vec::new();
    //     file.seek(SeekFrom::Start(0)).unwrap();
    //     file.read_to_end(&mut buffer).unwrap();

    //     assert!(!buffer.is_empty(), "File should not be empty");
    // }
}
