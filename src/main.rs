use std::io;

use greptime_bench::{
    common::config,
    generator::data_generator::{DataGenerator, DataGeneratorConfig},
    loader::data_loader::{DataLoaderConfig, GreptimeDataLoader},
    usql::usql::Usql,
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
        "generate_data" => generate_data(),
        "load" => load_data(),
        "generate_queries" => generate_queries(),
        _ => println!("Invalid command"),
    }
}

fn generate_data() {
    let config = DataGeneratorConfig::new(
        // 10,
        60,
        "2021-01-01T00:00:00Z".to_string(),
        "2021-01-01T01:00:00Z".to_string(),
        config::FileFormat::Greptime,
        123,
        10,
        // "log".to_string(),
    );

    let generator = DataGenerator::new(config);
    let stdout = io::stdout();
    let mut handle = stdout.lock();
    generator.generate(&mut handle);
}

fn load_data() {
    let config = DataLoaderConfig {
        workers: 1,
        chunk_size: 10,
    };

    let loader = GreptimeDataLoader { config };
    let usql = Usql::new("mysql://127.0.0.1:4002");
    loader.generate_inserts(Some(usql));
}

fn generate_queries() {
    println!("Generating queries...");
}
