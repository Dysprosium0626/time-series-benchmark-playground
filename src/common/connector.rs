use sqlx::mysql::MySqlPool;
use sqlx::Error;

pub struct Connector {
    pool: MySqlPool,
}

impl Connector {
    pub async fn new(database_url: &str) -> Result<Self, Error> {
        let pool = MySqlPool::connect(database_url).await?;
        Ok(Self { pool })
    }

    pub async fn insert(&self, insert_sql: String) -> Result<(), Error> {
        sqlx::query(&insert_sql).execute(&self.pool).await?;
        Ok(())
    }
}
