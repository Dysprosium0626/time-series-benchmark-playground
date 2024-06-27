use std::{
    fs::File,
    io::{BufRead, BufReader},
};

use crate::usql::usql::Usql;

#[derive(Clone)]
pub struct DataLoaderConfig {
    // path: PathBuf,
    pub workers: usize,
    pub chunk_size: usize,
}

pub trait DataLoader {
    fn load_data(&self, usql: Option<Usql>);
}

pub struct GreptimeDataLoader {
    pub config: DataLoaderConfig,
}

impl GreptimeDataLoader {
    pub fn config(&self) -> &DataLoaderConfig {
        &self.config
    }
}

impl DataLoader for GreptimeDataLoader {
    fn load_data(&self, usql: Option<Usql>) {
        let mut usql_conn = usql.unwrap_or_else(|| Usql::new("mysql://127.0.0.1:4002"));

        let file = File::open("./data.gz").expect("Failed to open file");
        let gz = flate2::read::GzDecoder::new(file);
        let mut reader = BufReader::new(gz);

        // 读取 header
        let mut header = String::new();
        reader
            .read_line(&mut header)
            .expect("Failed to read header");
        let header = header.trim_end();
        let cols: Vec<&str> = header.split(',').collect();

        let create_table_stmt = gen_create_table_stmt("measurement", &cols);
        execute_sql(&mut usql_conn, &create_table_stmt);

        // 读取数据行
        let data: Vec<Vec<String>> = reader
            .lines()
            .map(|line| {
                line.expect("Failed to read line")
                    .split(',')
                    .map(|v| v.trim().to_string())
                    .collect()
            })
            .collect();

        let insert_stmt = gen_insert_stmt("measurement", &cols, &data);
        execute_sql(&mut usql_conn, &insert_stmt);
    }
}
fn execute_sql(usql_conn: &mut Usql, sql: &str) {
    println!("{:?}", sql);
    match usql_conn.execute(sql) {
        Ok(result) => println!("Success: {:?}", result),
        Err(e) => eprintln!("Error executing query: {:?}", e),
    }
}

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

fn gen_insert_stmt(hypertable: &str, cols: &[&str], data: &[Vec<String>]) -> String {
    let mut insert_stmt = format!("INSERT INTO {}({}) VALUES", hypertable, cols.join(","));

    for (i, row) in data.iter().enumerate() {
        let values = row
            .iter()
            .map(|value| format!("'{}'", value.replace('\'', "''"))) // 转义单引号
            .collect::<Vec<_>>()
            .join(",");

        insert_stmt.push_str(&format!("{} ({})", if i == 0 { "" } else { "," }, values));
    }
    insert_stmt.push(';');

    insert_stmt
}

#[cfg(test)]
mod tests {
    use crate::usql::usql::Usql;

    use super::{DataLoader, DataLoaderConfig, GreptimeDataLoader};

    #[test]
    fn test_generate_inserts() {
        let config = DataLoaderConfig {
            workers: 1,
            chunk_size: 10,
        };

        let loader = GreptimeDataLoader { config };
        let usql = Usql::new("mysql://127.0.0.1:4002");
        loader.load_data(Some(usql));
    }
}
