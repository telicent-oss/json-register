use sqlx::postgres::{PgPoolOptions, PgRow};
use sqlx::{PgPool, Row};

/// Handles database interactions for registering JSON objects.
///
/// This struct manages the connection pool and executes SQL queries to insert
/// or retrieve JSON objects. It uses optimized queries to handle concurrency
/// and minimize round-trips.
pub struct Db {
    pool: PgPool,
    register_query: String,
    register_batch_query: String,
}

impl Db {
    /// Creates a new `Db` instance.
    ///
    /// # Arguments
    ///
    /// * `connection_string` - The PostgreSQL connection string.
    /// * `table_name` - The name of the table.
    /// * `id_column` - The name of the ID column.
    /// * `jsonb_column` - The name of the JSONB column.
    /// * `pool_size` - The maximum number of connections in the pool.
    ///
    /// # Returns
    ///
    /// A `Result` containing the new `Db` instance or a `sqlx::Error`.
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

        // Query to register a single object.
        // It attempts to insert the object. If it exists (ON CONFLICT), it does nothing.
        // Then it selects the ID, either from the inserted row or the existing row.
        let register_query = format!(
            r#"
            WITH inserted AS (
                INSERT INTO {table_name} ({jsonb_column})
                VALUES ($1::jsonb)
                ON CONFLICT ({jsonb_column}) DO NOTHING
                RETURNING {id_column}
            )
            SELECT {id_column} FROM inserted
            UNION ALL
            SELECT {id_column} FROM {table_name}
            WHERE {jsonb_column} = $2::jsonb
              AND NOT EXISTS (SELECT 1 FROM inserted)
            LIMIT 1
            "#
        );

        // Query to register a batch of objects.
        // It uses `unnest` to handle the array of inputs, attempts to insert new ones,
        // and then joins the results to ensure every input gets its corresponding ID
        // in the correct order.
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

    /// Registers a single JSON object string in the database.
    ///
    /// # Arguments
    ///
    /// * `json_str` - The canonicalised JSON string.
    ///
    /// # Returns
    ///
    /// A `Result` containing the ID (i32) or a `sqlx::Error`.
    pub async fn register_object(&self, json_str: &str) -> Result<i32, sqlx::Error> {
        let row: PgRow = sqlx::query(&self.register_query)
            .bind(json_str) // $1
            .bind(json_str) // $2
            .fetch_one(&self.pool)
            .await?;

        row.try_get(0)
    }

    /// Registers a batch of JSON object strings in the database.
    ///
    /// # Arguments
    ///
    /// * `json_strs` - A slice of canonicalised JSON strings.
    ///
    /// # Returns
    ///
    /// A `Result` containing a vector of IDs or a `sqlx::Error`.
    pub async fn register_batch_objects(
        &self,
        json_strs: &[String],
    ) -> Result<Vec<i32>, sqlx::Error> {
        if json_strs.is_empty() {
            return Ok(vec![]);
        }

        let rows = sqlx::query(&self.register_batch_query)
            .bind(json_strs) // $1::jsonb[]
            .fetch_all(&self.pool)
            .await?;

        let mut ids = Vec::with_capacity(rows.len());
        for row in rows {
            let id: i32 = row.try_get(0)?;
            ids.push(id);
        }

        Ok(ids)
    }
}
