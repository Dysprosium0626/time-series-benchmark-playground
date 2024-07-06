use std::fs::File;

use arrow::array::RecordBatch;
use derive_new::new;
use greptime_proto::v1::ColumnSchema;

use crate::common::error::Result;

/// Data use case, currently we only implement Log data.
pub enum UseCase {
    Log,
    Others,
}

/// Data Generator Config
#[derive(new)]
pub struct DataGeneratorConfig {
    // scale: i64,
    pub interval: i64,
    // ISO 8601
    pub time_start: String,
    pub time_end: String,
    pub seed: u64,
    pub limit: i64,
    pub use_case: UseCase,
}

pub trait DataGenerator {
    /// Generate Data
    fn generate(&self) -> Result<RecordBatch>;
    /// Write Data
    fn write(&self, to_write: &RecordBatch, file: File) -> Result<()>;
    // Get schema of generated data
    fn schema() -> Vec<ColumnSchema>;
    // Table name
    fn table_name() -> &'static str;
}
