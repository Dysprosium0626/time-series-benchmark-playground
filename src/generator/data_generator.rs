use rand::{Rng, SeedableRng};
use std::io::{self, Write};

use crate::common::config::FileFormat;

struct DataGeneratorConfig {
    scale: i64,
    interval: i64,
    // ISO 8601
    time_start: String,
    time_end: String,

    limit: i64,
    use_case: String,
    format: FileFormat,
    seed: u64,
}

struct DataGenerator {
    config: DataGeneratorConfig,
}

impl DataGenerator {
    fn new(config: DataGeneratorConfig) -> Self {
        DataGenerator { config }
    }

    fn generate<W: Write>(&self, writer: &mut W) {
        writer
            .write_all(b"time.value\n")
            .expect("Unable to write header");
        let mut rng = rand::rngs::StdRng::seed_from_u64(self.config.seed);
        let format = &self.config.format;
        match format {
            FileFormat::Greptime => {
                let mut start_time = chrono::DateTime::parse_from_rfc3339(&self.config.time_start)
                    .expect("Unable to parse time")
                    .timestamp();
                let end_time = chrono::DateTime::parse_from_rfc3339(&self.config.time_end)
                    .expect("Unable to parse time")
                    .timestamp();
                while start_time < end_time {
                    let value = rng.gen_range(0..self.config.limit);
                    if let Some(formatted_time) = chrono::DateTime::from_timestamp(start_time, 0) {
                        let line = format!("{},{}\n", formatted_time.to_rfc3339(), value);
                        writer
                            .write_all(line.as_bytes())
                            .expect("Unable to write data");
                        // Increment the current time by the interval
                        start_time += self.config.interval;
                    }
                }
                writer.flush().expect("Unable to flush writer");
            }
            FileFormat::Other => todo!(),
        }
    }
}

#[cfg(test)]
mod tests {

    use crate::common::config;

    use super::{DataGenerator, DataGeneratorConfig};

    #[test]
    fn test_data_generator_output() {
        let config = DataGeneratorConfig {
            scale: 10,
            interval: 60,
            time_start: "2021-01-01T00:00:00Z".to_string(),
            time_end: "2021-01-01T01:00:00Z".to_string(),
            format: config::FileFormat::Greptime,
            seed: 123,
            limit: 10,
            use_case: "log".to_string(),
        };

        let generator = DataGenerator { config };
        let mut buf = Vec::new();
        generator.generate(&mut buf);
        let output = String::from_utf8(buf).expect("Not UTF-8");

        let expected_start_time = "2021-01-01T00:00:00+00:00";
        let expected_end_time = "2021-01-01T00:59:00+00:00";
        println!("{:?}", output);
        assert!(output.contains(expected_start_time));
        assert!(output.contains(expected_end_time));
    }
}
