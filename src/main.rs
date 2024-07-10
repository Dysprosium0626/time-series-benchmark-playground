use std::fs::File;

use greptime_bench::generator::{
    data_generator::{DataGenerator, DataGeneratorConfig, UseCase},
    log_data_generator::{LogData, LogDataGenerator},
};
use std::env;

fn main() {
    let args: Vec<String> = env::args().collect();

    // 检查是否有足够的参数
    if args.len() < 2 {
        println!("Usage: cargo run <command>");
        println!("Commands:");
        println!("  generate_data     Generate data");
        println!("  load              Generate insert statements and send to usql");
        println!("  generate_queries  Generate queries");
        return;
    }

    match args[1].as_str() {
        "generate_data" => {
            let file_path = "./data.parquet";
            generate_data(file_path);
        }
        "load" => load_data(),
        "generate_queries" => generate_queries(),
        _ => println!("Invalid command"),
    }
}
fn generate_data(file_path: &str) {
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

    // 打开指定的文件路径
    let file = File::create(file_path).expect("Failed to create file");

    // log_data_generator
    //     .write(&record_batch, file)
    //     .expect("Failed to write record batch to file");
}
fn load_data() {
    // let config = DataLoaderConfig {
    //     workers: 1,
    //     chunk_size: 10,
    // };

    // let loader = GreptimeDataLoader { config };
    // let usql = Usql::new("mysql://127.0.0.1:4002");
    // loader.load_data(Some(usql));
}

fn generate_queries() {
    println!("Generating queries...");
}
