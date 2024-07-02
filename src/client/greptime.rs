use crate::common::error::{self,  IllegalDatabaseResponseSnafu, Result};
use greptime_proto::v1::{
    auth_header::AuthScheme, greptime_database_client::GreptimeDatabaseClient,
    greptime_request::Request, greptime_response::Response, AffectedRows, AuthHeader,
    GreptimeRequest, RequestHeader, RowInsertRequests,
};
use snafu::{location, Location, OptionExt, ResultExt};
use tonic::transport::Channel;

pub(crate) struct DatabaseClient {
    pub(crate) inner: GreptimeDatabaseClient<Channel>,
    dbname: String,
    auth_header: Option<AuthHeader>,
}

impl DatabaseClient {
    pub async fn new(dbname: impl Into<String>) -> Result<Self> {
        let inner = GreptimeDatabaseClient::connect("http://localhost:4001")
            .await
            .context(error::CreateChannelSnafu {
                location: location!(),
            })?;
        Ok(DatabaseClient {
            inner,
            dbname: dbname.into(),
            auth_header: None,
        })
    }

    /// Get associated dbname of this client
    pub fn dbname(&self) -> &String {
        &self.dbname
    }

    /// Update dbname of this client
    pub fn set_dbname(&mut self, dbname: impl Into<String>) {
        self.dbname = dbname.into();
    }

    /// Set authentication information
    pub fn set_auth(&mut self, auth: AuthScheme) {
        self.auth_header = Some(AuthHeader {
            auth_scheme: Some(auth),
        });
    }

    /// Write Row based insert requests to GreptimeDB and get rows written
    pub async fn row_insert(&self, requests: RowInsertRequests) -> Result<u32> {
        self.handle(Request::RowInserts(requests)).await
    }

    async fn handle(&self, request: Request) -> Result<u32> {
        let mut client = self.inner.clone();
        let request = self.to_rpc_request(request);
        let response = client
            .handle(request)
            .await?
            .into_inner()
            .response
            .context(IllegalDatabaseResponseSnafu {
                err_msg: "GreptimeResponse is empty",
            })?;
        let Response::AffectedRows(AffectedRows { value }) = response;
        Ok(value)
    }

    #[inline]
    fn to_rpc_request(&self, request: Request) -> GreptimeRequest {
        GreptimeRequest {
            header: Some(RequestHeader {
                authorization: self.auth_header.clone(),
                dbname: self.dbname.clone(),
                ..Default::default()
            }),
            request: Some(request),
        }
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use derive_new::new;
    use greptime_proto::v1::{
        value::ValueData, ColumnDataType, ColumnSchema, Row, RowInsertRequest, RowInsertRequests,
        Rows, SemanticType, Value,
    };

    pub fn tag(name: &str, datatype: ColumnDataType) -> ColumnSchema {
        ColumnSchema {
            column_name: name.to_string(),
            semantic_type: SemanticType::Tag as i32,
            datatype: datatype as i32,
            ..Default::default()
        }
    }

    pub fn timestamp(name: &str, datatype: ColumnDataType) -> ColumnSchema {
        ColumnSchema {
            column_name: name.to_string(),
            semantic_type: SemanticType::Timestamp as i32,
            datatype: datatype as i32,
            ..Default::default()
        }
    }

    pub fn field(name: &str, datatype: ColumnDataType) -> ColumnSchema {
        ColumnSchema {
            column_name: name.to_string(),
            semantic_type: SemanticType::Field as i32,
            datatype: datatype as i32,
            ..Default::default()
        }
    }

    #[derive(new)]
    struct WeatherRecord {
        timestamp_millis: i64,
        collector: String,
        temperature: f32,
        humidity: i32,
    }

    fn weather_records() -> Vec<WeatherRecord> {
        vec![
            WeatherRecord::new(1686109527000, "c1".to_owned(), 26.4, 15),
            WeatherRecord::new(1686023127000, "c1".to_owned(), 29.3, 20),
            WeatherRecord::new(1685936727010, "c1".to_owned(), 31.8, 13),
            WeatherRecord::new(1686109527000, "c2".to_owned(), 20.4, 67),
            WeatherRecord::new(1686023127000, "c2".to_owned(), 18.0, 74),
            WeatherRecord::new(1685936727000, "c2".to_owned(), 19.2, 81),
        ]
    }

    fn weather_schema() -> Vec<ColumnSchema> {
        vec![
            timestamp("ts", ColumnDataType::TimestampMillisecond),
            tag("collector", ColumnDataType::String),
            field("temperature", ColumnDataType::Float32),
            field("humidity", ColumnDataType::Int32),
        ]
    }

    #[inline]
    pub fn timestamp_millisecond_value(v: i64) -> Value {
        Value {
            value_data: Some(ValueData::TimestampMillisecondValue(v)),
        }
    }

    #[inline]
    pub fn string_value(v: String) -> Value {
        Value {
            value_data: Some(ValueData::StringValue(v)),
        }
    }

    #[inline]
    pub fn f32_value(v: f32) -> Value {
        Value {
            value_data: Some(ValueData::F32Value(v)),
        }
    }

    #[inline]
    pub fn i32_value(v: i32) -> Value {
        Value {
            value_data: Some(ValueData::I32Value(v)),
        }
    }

    /// This function generates some random data and bundle them into a
    /// `InsertRequest`.
    ///
    /// Data structure:
    ///
    /// - `ts`: a timestamp column
    /// - `collector`: a tag column
    /// - `temperature`: a value field of f32
    /// - `humidity`: a value field of i32
    ///
    fn to_insert_request(records: Vec<WeatherRecord>) -> RowInsertRequests {
        let rows = records
            .into_iter()
            .map(|record| Row {
                values: vec![
                    timestamp_millisecond_value(record.timestamp_millis),
                    string_value(record.collector),
                    f32_value(record.temperature),
                    i32_value(record.humidity),
                ],
            })
            .collect();

        RowInsertRequests {
            inserts: vec![RowInsertRequest {
                table_name: "weather_demo".to_owned(),
                rows: Some(Rows {
                    schema: weather_schema(),
                    rows,
                }),
            }],
        }
    }

    #[tokio::test]
    async fn test_row_insert() -> Result<()> {
        // 创建 DatabaseClient 实例，连接到本地运行的GreptimeDB
        let client = DatabaseClient::new("my_db_demo".to_string()).await?;

        let records = weather_records();
        let affected_rows = client.row_insert(to_insert_request(records)).await?;

        // 验证返回的受影响行数
        assert!(affected_rows > 0, "Affected rows should greater than 0");
        Ok(())
    }
}
