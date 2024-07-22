use crate::{
    common::error::{ArrowFileSnafu, ParseDateSnafu, Result, WriteParquetFileSnafu},
    loader::{field, tag, timestamp},
};
use arrow::{
    array::{Array, Int32Array, RecordBatch, StringArray, TimestampMicrosecondArray},
    datatypes::{DataType, Field, Schema, TimeUnit},
};
use fake::{
    faker::{
        internet::en::{DomainSuffix, FreeEmail, UserAgent, Username, IP},
        lorem::en::Sentence,
        time::en::DateTimeBefore,
    },
    Fake, Faker,
};
use greptime_proto::v1::{ColumnDataType, ColumnSchema};
use parquet::{arrow::ArrowWriter, file::properties::WriterProperties};
use rand::{distributions::Distribution, rngs::StdRng, Rng};
use rand::{distributions::WeightedIndex, seq::SliceRandom};
use snafu::ResultExt;
use std::{fs::File, iter::zip, sync::Arc};
use time::{format_description::well_known::Iso8601, OffsetDateTime};

use super::data_generator::{DataGenerator, DataGeneratorConfig};

/// LogData with schema for different tables
pub struct LogData {
    users_schema: Arc<Schema>,
    pages_schema: Arc<Schema>,
    devices_schema: Arc<Schema>,
    web_logs_schema: Arc<Schema>,
    requests_schema: Arc<Schema>,
    responses_schema: Arc<Schema>,
    error_logs_schema: Arc<Schema>,
}

impl Default for LogData {
    fn default() -> Self {
        Self::new()
    }
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
                Field::new("browser", DataType::Utf8, false),
            ])),
            web_logs_schema: Arc::new(Schema::new(vec![
                Field::new("log_id", DataType::Int32, false),
                Field::new("user_id", DataType::Int32, false),
                Field::new("page_id", DataType::Int32, false),
                Field::new("device_id", DataType::Int32, false),
                Field::new("runtime", DataType::Int32, false),
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
                Field::new("status_code", DataType::Utf8, false),
                Field::new("response_size", DataType::Int32, false),
                Field::new("response_time", DataType::Int32, false),
            ])),

            error_logs_schema: Arc::new(Schema::new(vec![
                Field::new("error_log_id", DataType::Int32, false),
                Field::new("log_id", DataType::Int32, false),
                Field::new("error_code", DataType::Utf8, false),
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

/// LogConfig is used to
pub struct LogConfig {
    num_of_users: usize,
    num_of_pages: usize,
}

/// LogDataGenerator, the schema of Log Data can be referred to
/// Web Server Access Logs https://www.kaggle.com/datasets/eliasdabbas/web-server-access-logs/
pub struct LogDataGenerator {
    pub generator_config: DataGeneratorConfig,
    pub log_config: LogConfig,
    pub log_data: LogData,
}

impl DataGenerator for LogDataGenerator {
    fn generate(&self) -> Result<Vec<RecordBatch>> {
        // Generate data for different tables
        let users_batch = self.generate_users_data()?;
        let pages_batch = self.generate_pages_data()?;
        let devices_batch = self.generate_devices_data()?;
        let (web_logs_batch, num_of_logs) =
            self.generate_web_logs_data(&users_batch, &pages_batch, &devices_batch)?;
        let requests_batch =
            self.generate_requests_data(&web_logs_batch, &pages_batch, num_of_logs)?;
        let responses_batch = self.generate_responses_data(&web_logs_batch, num_of_logs)?;
        let error_logs_batch = self.generate_error_logs_data(&web_logs_batch, num_of_logs)?;

        // Return all batches
        Ok(vec![
            users_batch,
            pages_batch,
            devices_batch,
            web_logs_batch,
            requests_batch,
            responses_batch,
            error_logs_batch,
        ])
    }

    fn write(&self, to_write: Vec<RecordBatch>) -> Result<()> {
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
                field("browser", ColumnDataType::String),
            ],
            "web_logs" => vec![
                tag("log_id", ColumnDataType::Int32),
                field("user_id", ColumnDataType::Int32),
                field("page_id", ColumnDataType::Int32),
                field("device_id", ColumnDataType::Int32),
                field("runtime", ColumnDataType::Int32),
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
                field("status_code", ColumnDataType::String),
                field("response_size", ColumnDataType::Int32),
                field("response_time", ColumnDataType::Int32),
            ],
            "error_logs" => vec![
                tag("error_log_id", ColumnDataType::Int32),
                field("log_id", ColumnDataType::Int32),
                field("error_code", ColumnDataType::String),
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
            "error_logs",
        ]
    }
}

impl LogDataGenerator {
    // Generate data for `users` table
    // Return `RecordBatch`
    fn generate_users_data(&self) -> Result<RecordBatch> {
        let num_of_user = self.log_config.num_of_users;
        let seed = &mut self.generator_config.seed.clone();
        let mut user_ids: Vec<i32> = Vec::with_capacity(num_of_user);
        let mut usernames: Vec<String> = Vec::with_capacity(num_of_user);
        let mut emails: Vec<String> = Vec::with_capacity(num_of_user);
        let mut signup_dates: Vec<i64> = Vec::with_capacity(num_of_user);

        for i in 0..num_of_user {
            user_ids.push(i as i32);
            usernames.push(Username().fake_with_rng(seed));
            emails.push(FreeEmail().fake_with_rng(seed));

            let date_before =
                OffsetDateTime::parse(&self.generator_config.time_start, &Iso8601::DEFAULT)
                    .context(ParseDateSnafu {})?;
            let date = DateTimeBefore(date_before).fake_with_rng::<OffsetDateTime, _>(seed);
            signup_dates.push(date.unix_timestamp() * 1_000_000 + date.microsecond() as i64);
        }

        let user_id = Int32Array::from(user_ids.clone());
        let username = StringArray::from(usernames.clone());
        let email = StringArray::from(emails.clone());
        let signup_date = TimestampMicrosecondArray::from(signup_dates.clone());

        let users_batch = RecordBatch::try_new(
            self.log_data.users_schema.clone(),
            vec![
                Arc::new(user_id),
                Arc::new(username),
                Arc::new(email),
                Arc::new(signup_date),
            ],
        )
        .context(ArrowFileSnafu {})?;

        Ok(users_batch)
    }

    // Generate data for `pages` table
    // Return `RecordBatch`
    fn generate_pages_data(&self) -> Result<RecordBatch> {
        let num_of_pages = self.log_config.num_of_pages;
        let seed = &mut self.generator_config.seed.clone();
        let mut page_ids: Vec<i32> = Vec::with_capacity(num_of_pages);
        let mut page_urls: Vec<String> = Vec::with_capacity(num_of_pages);
        let mut page_titles: Vec<String> = Vec::with_capacity(num_of_pages);
        let mut created_date: Vec<i64> = Vec::with_capacity(num_of_pages);

        for i in 0..num_of_pages {
            page_ids.push(i as i32);

            let domain_name: String = Username().fake_with_rng(seed);
            let domain_suffix: String = DomainSuffix().fake_with_rng(seed);
            let url = format!("https://www.{}.{}", domain_name, domain_suffix);
            page_urls.push(url);

            let title: String = Sentence(3..6).fake_with_rng(seed); // 生成3到6个单词的句子
            page_titles.push(title);

            let date = OffsetDateTime::parse(&self.generator_config.time_start, &Iso8601::DEFAULT)
                .context(ParseDateSnafu {})?;
            created_date.push(date.unix_timestamp() * 1_000_000 + date.microsecond() as i64);
        }

        let page_id = Int32Array::from(page_ids);
        let page_url = StringArray::from(page_urls);
        let page_title = StringArray::from(page_titles);
        let created_date = TimestampMicrosecondArray::from(created_date);

        let pages_batch = RecordBatch::try_new(
            self.log_data.pages_schema.clone(),
            vec![
                Arc::new(page_id),
                Arc::new(page_url),
                Arc::new(page_title),
                Arc::new(created_date),
            ],
        )
        .context(ArrowFileSnafu {})?;

        Ok(pages_batch)
    }

    // Generate data for `devices` table
    // Return `RecordBatch`
    fn generate_devices_data(&self) -> Result<RecordBatch> {
        // One user have one device
        let num_of_devices = self.log_config.num_of_users;
        let seed = &mut self.generator_config.seed.clone();
        let mut device_ids: Vec<i32> = Vec::with_capacity(num_of_devices);
        let mut browsers: Vec<String> = Vec::with_capacity(num_of_devices);

        for i in 0..num_of_devices {
            device_ids.push(i as i32);
            browsers.push(UserAgent().fake_with_rng(seed));
        }

        let device_id = Int32Array::from(device_ids);
        let browser = StringArray::from(browsers);

        let device_batch = RecordBatch::try_new(
            self.log_data.devices_schema.clone(),
            vec![Arc::new(device_id), Arc::new(browser)],
        )
        .context(ArrowFileSnafu {})?;

        Ok(device_batch)
    }

    // Generate data for `web_logs` table
    // Return `RecordBatch`
    fn generate_web_logs_data(
        &self,
        users_batch: &RecordBatch,
        pages_batch: &RecordBatch,
        devices_batch: &RecordBatch,
    ) -> Result<(RecordBatch, usize)> {
        let mut seed = self.generator_config.seed.clone();
        let mut log_ids: Vec<i32> = Vec::new();
        let mut user_ids: Vec<i32> = Vec::new();
        let mut page_ids: Vec<i32> = Vec::new();
        let mut device_ids: Vec<i32> = Vec::new();
        let mut runtimes: Vec<i32> = Vec::new();
        let mut ip_addresses: Vec<String> = Vec::new();
        let mut timestamps: Vec<i64> = Vec::new();

        let user_id_array = users_batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let page_id_array = pages_batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let device_id_array = devices_batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();

        // Get timestamp
        let start_time =
            OffsetDateTime::parse(&self.generator_config.time_start, &Iso8601::DEFAULT)
                .context(ParseDateSnafu {})?
                .unix_timestamp()
                * 1_000_000;
        let end_time = OffsetDateTime::parse(&self.generator_config.time_end, &Iso8601::DEFAULT)
            .context(ParseDateSnafu {})?
            .unix_timestamp()
            * 1_000_000;

        let mut current_time = start_time;

        // The scope of web_logs = (end_time - start_time) / interval
        while current_time <= end_time {
            log_ids.push(log_ids.len() as i32);

            // Get user_id, page_id, device_id from previous generated data
            let user_id = user_id_array.value(log_ids.len() % user_id_array.len());
            user_ids.push(user_id);

            let page_id = page_id_array.value(log_ids.len() % page_id_array.len());
            page_ids.push(page_id);

            let device_id = device_id_array.value(log_ids.len() % device_id_array.len());
            device_ids.push(device_id);

            let runtime = seed.gen_range(50..300);
            runtimes.push(runtime);

            ip_addresses.push(IP().fake_with_rng(&mut seed));

            // Add +/- 500ms jitter to make timestamp real
            let jitter: i64 = seed.gen_range(-500_000..500_000);
            timestamps.push(current_time + jitter);

            // Add interval
            current_time += self.generator_config.interval;
        }

        let log_id = Int32Array::from(log_ids);
        let user_id = Int32Array::from(user_ids);
        let page_id = Int32Array::from(page_ids);
        let device_id = Int32Array::from(device_ids);
        let runtime = Int32Array::from(runtimes);
        let ip_address = StringArray::from(ip_addresses);
        let timestamp = TimestampMicrosecondArray::from(timestamps.clone());

        let web_logs_batch = RecordBatch::try_new(
            self.log_data.web_logs_schema.clone(),
            vec![
                Arc::new(log_id),
                Arc::new(user_id),
                Arc::new(page_id),
                Arc::new(device_id),
                Arc::new(runtime),
                Arc::new(ip_address),
                Arc::new(timestamp),
            ],
        )
        .context(ArrowFileSnafu {})?;

        Ok((web_logs_batch, timestamps.len()))
    }

    // Generate data for `requests` table
    // Return `RecordBatch`
    fn generate_requests_data(
        &self,
        web_logs_batch: &RecordBatch,
        pages_batch: &RecordBatch,
        num_of_logs: usize,
    ) -> Result<RecordBatch> {
        let mut seed = self.generator_config.seed.clone();
        let mut request_ids: Vec<i32> = Vec::with_capacity(num_of_logs);
        let mut log_ids: Vec<i32> = Vec::with_capacity(num_of_logs);
        let mut methods: Vec<String> = Vec::with_capacity(num_of_logs);
        let mut urls: Vec<String> = Vec::with_capacity(num_of_logs);
        let mut versions: Vec<String> = Vec::with_capacity(num_of_logs);

        let log_id_array = web_logs_batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();

        let page_url_array = pages_batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();

        let http_methods = ["GET", "POST", "PUT", "DELETE", "PATCH"];
        let http_versions = ["HTTP/1.1", "HTTP/2", "HTTP/3"];

        for i in 0..num_of_logs {
            request_ids.push(i as i32);

            let log_id = log_id_array.value(i);
            log_ids.push(log_id);

            let method = http_methods.choose(&mut seed).unwrap().to_string();
            methods.push(method);

            let url = page_url_array.value(i % page_url_array.len()).to_string();
            urls.push(url);

            let http_version = http_versions.choose(&mut seed).unwrap().to_string();
            versions.push(http_version);
        }

        let request_id = Int32Array::from(request_ids);
        let log_id = Int32Array::from(log_ids);
        let method = StringArray::from(methods);
        let url = StringArray::from(urls);
        let http_version = StringArray::from(versions);

        let requests_batch = RecordBatch::try_new(
            self.log_data.requests_schema.clone(),
            vec![
                Arc::new(request_id),
                Arc::new(log_id),
                Arc::new(method),
                Arc::new(url),
                Arc::new(http_version),
            ],
        )
        .context(ArrowFileSnafu {})?;

        Ok(requests_batch)
    }

    // Generate data for `response` table
    // Return `RecordBatch`
    fn generate_responses_data(
        &self,
        web_logs_batch: &RecordBatch,
        num_of_logs: usize,
    ) -> Result<RecordBatch> {
        let mut seed = self.generator_config.seed.clone();
        let mut response_ids: Vec<i32> = Vec::with_capacity(num_of_logs);
        let mut log_ids: Vec<i32> = Vec::with_capacity(num_of_logs);
        let mut status_codes: Vec<String> = Vec::with_capacity(num_of_logs);
        let mut response_sizes: Vec<i32> = Vec::with_capacity(num_of_logs);
        let mut response_times: Vec<i32> = Vec::with_capacity(num_of_logs);

        let log_id_array = web_logs_batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();

        //  Generate more 20X and less 40X/50X base on weight
        let http_status_codes = [
            "200", "201", "202", "204", "400", "401", "403", "404", "500", "502", "503",
        ];
        let weights = vec![40, 20, 10, 10, 5, 2, 2, 5, 3, 2, 1];
        let dist = WeightedIndex::new(weights).unwrap();

        for i in 0..num_of_logs {
            response_ids.push(i as i32);

            let log_id = log_id_array.value(i);
            log_ids.push(log_id);

            let status_code = http_status_codes[dist.sample(&mut seed)].to_string();
            status_codes.push(status_code);

            response_sizes.push(Faker.fake_with_rng::<i32, StdRng>(&mut seed));
            response_times.push(Faker.fake_with_rng::<i32, StdRng>(&mut seed));
        }

        let response_id = Int32Array::from(response_ids);
        let log_id = Int32Array::from(log_ids);
        let status_code = StringArray::from(status_codes);
        let response_size = Int32Array::from(response_sizes);
        let response_time = Int32Array::from(response_times);

        let responses_batch = RecordBatch::try_new(
            self.log_data.responses_schema.clone(),
            vec![
                Arc::new(response_id),
                Arc::new(log_id),
                Arc::new(status_code),
                Arc::new(response_size),
                Arc::new(response_time),
            ],
        )
        .context(ArrowFileSnafu {})?;

        Ok(responses_batch)
    }

    // Generate data for `error_logs` table
    // Mostly normal logs will generate no error, but little proportion of logs will generate 1-3 error logs
    // Return `RecordBatch`
    fn generate_error_logs_data(
        &self,
        web_logs_batch: &RecordBatch,
        num_of_logs: usize,
    ) -> Result<RecordBatch> {
        let mut seed = self.generator_config.seed.clone();
        // Leave enough space for error logs
        let mut error_log_ids: Vec<i32> = Vec::new();
        let mut log_ids: Vec<i32> = Vec::new();
        let mut error_codes: Vec<String> = Vec::new();
        let mut error_messages: Vec<String> = Vec::new();
        let mut timestamps: Vec<i64> = Vec::new();

        let log_id_array = web_logs_batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();

        let timestamp_array = web_logs_batch
            .column(6)
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .unwrap();

        for i in 0..num_of_logs {
            let log_id = log_id_array.value(i);
            let base_timestamp = timestamp_array.value(i);

            // 80% of the logs do not generate error logs, 20% of the logs generate 1-3 error logs
            if seed.gen_range(0..100) < 20 {
                let num_of_errors = seed.gen_range(1..=3);
                for _ in 0..num_of_errors {
                    error_log_ids.push(error_log_ids.len() as i32);

                    log_ids.push(log_id);

                    // Error code: 500-509
                    let error_code = 500 + seed.gen_range(0..10);
                    error_codes.push(error_code.to_string());

                    error_messages.push(format!("Error message {}", error_code));
                    // +500ms jitter
                    let jitter: i64 = seed.gen_range(0..500_000);
                    timestamps.push(base_timestamp + jitter);
                }
            }
        }

        let error_log_id = Int32Array::from(error_log_ids);
        let log_id = Int32Array::from(log_ids);
        let error_code = StringArray::from(error_codes);
        let error_message = StringArray::from(error_messages);
        let timestamp = TimestampMicrosecondArray::from(timestamps);

        let error_logs_batch = RecordBatch::try_new(
            self.log_data.error_logs_schema.clone(),
            vec![
                Arc::new(error_log_id),
                Arc::new(log_id),
                Arc::new(error_code),
                Arc::new(error_message),
                Arc::new(timestamp),
            ],
        )
        .context(ArrowFileSnafu {})?;

        Ok(error_logs_batch)
    }
}

#[cfg(test)]
mod tests {
    use crate::generator::data_generator::UseCase;
    use crate::loader::data_loader::read_parquet_file;

    use super::*;
    use rand::SeedableRng;
    use std::fs::{self};
    use std::path::PathBuf;

    // Helper function to create a LogDataGenerator instance
    fn create_log_data_generator() -> LogDataGenerator {
        let log_data = LogData::new();
        let log_config = LogConfig {
            num_of_users: 10,
            num_of_pages: 5,
        };

        let generator_config = DataGeneratorConfig {
            seed: StdRng::seed_from_u64(42),
            time_start: "2023-01-01T00:00:00Z".to_string(),
            time_end: "2023-01-02T00:00:00Z".to_string(),
            interval: 60 * 60 * 1_000_000,
            use_case: UseCase::Log,
        };

        LogDataGenerator {
            generator_config,
            log_config,
            log_data,
        }
    }

    #[test]
    fn test_generate() {
        let generator = create_log_data_generator();
        let result = generator.generate();

        assert!(result.is_ok());
        let batches = result.unwrap();

        assert_eq!(batches.len(), 7);

        let users_batch = &batches[0];
        assert_eq!(users_batch.schema().fields().len(), 4);
        assert_eq!(users_batch.num_rows(), 10);

        let pages_batch = &batches[1];
        assert_eq!(pages_batch.schema().fields().len(), 4);
        assert_eq!(pages_batch.num_rows(), 5);

        let devices_batch = &batches[2];
        assert_eq!(devices_batch.schema().fields().len(), 2);
        assert_eq!(devices_batch.num_rows(), 10);

        let web_logs_batch = &batches[3];
        assert_eq!(web_logs_batch.schema().fields().len(), 7);
        assert!(web_logs_batch.num_rows() > 0);

        let requests_batch = &batches[4];
        assert_eq!(requests_batch.schema().fields().len(), 5);
        assert_eq!(requests_batch.num_rows(), web_logs_batch.num_rows());

        let responses_batch = &batches[5];
        assert_eq!(responses_batch.schema().fields().len(), 5);
        assert_eq!(responses_batch.num_rows(), web_logs_batch.num_rows());

        let error_logs_batch = &batches[6];
        assert_eq!(error_logs_batch.schema().fields().len(), 5);
        assert!(error_logs_batch.num_rows() <= web_logs_batch.num_rows() * 3);
    }

    #[test]
    fn test_write() {
        // Delete all .parquet file
        let dir = std::env::current_dir().expect("Unable to get current directory");
        for entry in fs::read_dir(dir).expect("Unable to read directory") {
            let entry = entry.expect("Unable to get directory entry");
            let path = entry.path();
            if path.extension().and_then(|ext| ext.to_str()) == Some("parquet") {
                fs::remove_file(path).expect("Unable to delete file");
            }
        }

        let generator = create_log_data_generator();
        let result = generator.generate();
        assert!(result.is_ok());
        let batches = result.unwrap();

        let write_result = generator.write(batches.clone());
        assert!(write_result.is_ok());

        for table_name in LogDataGenerator::table_names() {
            let file_name = format!("{}.parquet", table_name);

            // 使用 read_parquet_file 函数读取文件
            let record_batch =
                read_parquet_file(PathBuf::from(&file_name)).expect("Unable to read parquet file");

            let total_rows = record_batch.num_rows();

            let expected_rows = match table_name {
                "users" => 10,
                "pages" => 5,
                "devices" => 10,
                "web_logs" => batches[3].num_rows(),
                "requests" => batches[4].num_rows(),
                "responses" => batches[5].num_rows(),
                "error_logs" => batches[6].num_rows(),
                _ => 0,
            };

            assert_eq!(
                total_rows, expected_rows,
                "Mismatch in table: {}",
                table_name
            );
        }
    }
}
