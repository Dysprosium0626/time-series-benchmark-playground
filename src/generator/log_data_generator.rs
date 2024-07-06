use crate::{
    common::error::{Result, WriteParquetFileSnafu},
    loader::{field, tag, timestamp},
};
use arrow::{
    array::{Int32Array, RecordBatch, StringArray, TimestampMicrosecondArray},
    datatypes::{DataType, Field, Schema, TimeUnit},
};
use greptime_proto::v1::{ColumnDataType, ColumnSchema};
use parquet::{arrow::ArrowWriter, file::properties::WriterProperties};
use snafu::ResultExt;
use std::{fs::File, sync::Arc};

use super::data_generator::{DataGenerator, DataGeneratorConfig};

/// LogData with schema
pub struct LogData {
    field_schema: Arc<Schema>,
}

impl LogData {
    pub fn new() -> Self {
        LogData {
            field_schema: Arc::new(Schema::new(vec![
                Field::new("ip_address", DataType::Utf8, false),
                Field::new("user_agent", DataType::Utf8, false),
                Field::new(
                    "timestamp",
                    DataType::Timestamp(TimeUnit::Microsecond, None),
                    false,
                ),
                Field::new("request_method", DataType::Utf8, false),
                Field::new("request_url", DataType::Utf8, false),
                Field::new("response_code", DataType::Int32, false),
                Field::new("response_size", DataType::Int32, false),
                Field::new("referrer", DataType::Utf8, false),
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
    fn generate(&self) -> Result<RecordBatch> {
        // Currently the data is generated randomly with no meaning
        let ip_addresses = StringArray::from(vec!["54.36.149.41"]);
        let user_agents = StringArray::from(vec![
            "Mozilla/5.0 (compatible; AhrefsBot/6.1; +http://ahrefs.com/robot/)",
        ]);

        let timestamps = TimestampMicrosecondArray::from(vec![1548114974000000]);
        let request_method = StringArray::from(vec!["GET"]);
        let request_url = StringArray::from(vec!["/filter/27|13%20%D9%85%DA%AF%D8%A7%D9%BE%DB%8C%DA%A9%D8%B3%D9%84,27|%DA%A9%D9%85%D8%AA%D8%B1%20%D8%A7%D8%B2%205%20%D9%85%DA%AF%D8%A7%D9%BE%DB%8C%DA%A9%D8%B3%D9%84,p53"]);
        let response_code = Int32Array::from(vec![200]);
        let response_size = Int32Array::from(vec![30577]);
        let referrer = StringArray::from(vec!["https://www.zanbil.ir/m/filter/b113"]);

        let to_write = RecordBatch::try_new(
            self.log_data.field_schema.clone(),
            vec![
                Arc::new(ip_addresses),
                Arc::new(user_agents),
                Arc::new(timestamps),
                Arc::new(request_method),
                Arc::new(request_url),
                Arc::new(response_code),
                Arc::new(response_size),
                Arc::new(referrer),
            ],
        )
        .unwrap();
        Ok(to_write)
    }

    fn write(&self, to_write: &RecordBatch, file: File) -> Result<()> {
        let props = WriterProperties::builder().build();
        let mut parquet_writer = ArrowWriter::try_new(file, to_write.schema(), Some(props))
            .context(WriteParquetFileSnafu {})?;

        parquet_writer
            .write(to_write)
            .context(WriteParquetFileSnafu {})?;
        parquet_writer.close().context(WriteParquetFileSnafu {})?;
        Ok(())
    }

    fn schema() -> Vec<ColumnSchema> {
        vec![
            tag("ip_address", ColumnDataType::String),
            field("user_agent", ColumnDataType::String),
            timestamp("timestamp", ColumnDataType::TimestampMicrosecond),
            field("request_method", ColumnDataType::String),
            field("request_url", ColumnDataType::String),
            field("response_code", ColumnDataType::Int32),
            field("response_size", ColumnDataType::Int32),
            field("referrer", ColumnDataType::String),
        ]
    }

    fn table_name() -> &'static str {
        "greptime-bench-log"
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

    #[test]
    fn test_data_generator_output() {
        let log_data_generator = LogDataGenerator {
            config: DataGeneratorConfig::new(
                60,
                "2021-01-01T00:00:00Z".to_string(),
                "2021-01-01T01:00:00Z".to_string(),
                123,
                10,
                UseCase::Log,
            ),
            log_data: LogData::new(),
        };

        let record_batch = log_data_generator
            .generate()
            .expect("Failed to generate record batch");

        let mut file = tempfile().expect("Failed to create temporary file");

        log_data_generator
            .write(&record_batch, file.try_clone().unwrap())
            .expect("Failed to write record batch to file");

        let mut buffer = Vec::new();
        file.seek(SeekFrom::Start(0)).unwrap();
        file.read_to_end(&mut buffer).unwrap();

        assert!(!buffer.is_empty(), "File should not be empty");
    }
}
