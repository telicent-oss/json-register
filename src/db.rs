use sqlx::{PgPool, Row};
use sqlx::postgres::{PgPoolOptions, PgRow};

pub struct Db {
    pool: PgPool,
    register_query: String,
    register_batch_query: String,
}

impl Db {
    pub async fn new(
        connection_string: &str,
        table_name: &str,
        id_column: &str,
        jsonb_column: &str,
        pool_size: u32,
    ) -> Result<Self, sqlx::Error> {
        let pool = PgPoolOptions::new()
            .max_connections(pool_size)
            .connect(connection_string)
            .await?;
        
        let register_query = format!(
            r#"
            WITH inserted AS (
                INSERT INTO {table_name} ({jsonb_column})
                VALUES ($1)
                ON CONFLICT ({jsonb_column}) DO NOTHING
                RETURNING {id_column}
            )
            SELECT {id_column} FROM inserted
            UNION ALL
            SELECT {id_column} FROM {table_name}
            WHERE {jsonb_column} = $2
              AND NOT EXISTS (SELECT 1 FROM inserted)
            LIMIT 1
            "#
        );

        let register_batch_query = format!(
            r#"
            WITH input_objects AS (
                SELECT
                    ord as original_order,
                    value as json_value
                FROM unnest($1::jsonb[]) WITH ORDINALITY AS t(value, ord)
            ),
            inserted AS (
                INSERT INTO {table_name} ({jsonb_column})
                SELECT json_value FROM input_objects
                ON CONFLICT ({jsonb_column}) DO NOTHING
                RETURNING {id_column}, {jsonb_column}
            ),
            existing AS (
                SELECT t.{id_column}, t.{jsonb_column}
                FROM {table_name} t
                JOIN input_objects io ON t.{jsonb_column} = io.json_value
            )
            SELECT COALESCE(i.{id_column}, e.{id_column}) as {id_column}, io.original_order
            FROM input_objects io
            LEFT JOIN inserted i ON io.json_value = i.{jsonb_column}
            LEFT JOIN existing e ON io.json_value = e.{jsonb_column}
            ORDER BY io.original_order
            "#
        );

        Ok(Self {
            pool,
            register_query,
            register_batch_query,
        })
    }

    pub async fn register_object(&self, json_str: &str) -> Result<i64, sqlx::Error> {
        let row: PgRow = sqlx::query(&self.register_query)
            .bind(json_str) // $1
            .bind(json_str) // $2
            .fetch_one(&self.pool)
            .await?;
        
        row.try_get(0)
    }

    pub async fn register_batch_objects(&self, json_strs: &[String]) -> Result<Vec<i64>, sqlx::Error> {
        if json_strs.is_empty() {
            return Ok(vec![]);
        }

        let rows = sqlx::query(&self.register_batch_query)
            .bind(json_strs) // $1::jsonb[]
            .fetch_all(&self.pool)
            .await?;

        let mut ids = Vec::with_capacity(rows.len());
        for row in rows {
            let id: i64 = row.try_get(0)?;
            ids.push(id);
        }
        
        Ok(ids)
    }
}
