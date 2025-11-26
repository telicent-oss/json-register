use deadpool::managed::{PoolError, QueueMode};
use deadpool_postgres::{Config, ManagerConfig, Pool, RecyclingMethod, Runtime};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use tokio_postgres::NoTls;

/// Validates that an SQL identifier (table or column name) is safe to use.
///
/// # Arguments
///
/// * `identifier` - The identifier to validate.
/// * `name` - A descriptive name for error messages (e.g., "table_name", "column_name").
///
/// # Returns
///
/// `Ok(())` if valid, or an error describing the issue.
fn validate_sql_identifier(identifier: &str, name: &str) -> Result<(), String> {
    if identifier.is_empty() {
        return Err(format!("{} cannot be empty", name));
    }

    if identifier.len() > 63 {
        return Err(format!("{} exceeds PostgreSQL's 63 character limit", name));
    }

    // Validate that identifier contains only safe characters: alphanumeric, underscore
    // Must start with a letter or underscore
    let first_char = identifier.chars().next().unwrap();
    if !first_char.is_ascii_alphabetic() && first_char != '_' {
        return Err(format!("{} must start with a letter or underscore", name));
    }

    for c in identifier.chars() {
        if !c.is_ascii_alphanumeric() && c != '_' {
            return Err(format!(
                "{} contains invalid character '{}'. Only alphanumeric and underscore allowed",
                name, c
            ));
        }
    }

    Ok(())
}

/// Handles database interactions for registering JSON objects.
///
/// This struct manages the connection pool and executes SQL queries to insert
/// or retrieve JSON objects. It uses optimized queries to handle concurrency
/// and minimize round-trips.
pub struct Db {
    pool: Pool,
    register_query: String,
    register_batch_query: String,
    queries_executed: AtomicU64,
    query_errors: AtomicU64,
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
    /// * `acquire_timeout_secs` - Optional timeout for acquiring connections (default: 5s).
    /// * `idle_timeout_secs` - Optional timeout for idle connections (default: 600s).
    /// * `max_lifetime_secs` - Optional maximum lifetime for connections (default: 1800s).
    ///
    /// # Returns
    ///
    /// A `Result` containing the new `Db` instance or a `JsonRegisterError`.
    #[allow(clippy::too_many_arguments)]
    pub async fn new(
        connection_string: &str,
        table_name: &str,
        id_column: &str,
        jsonb_column: &str,
        pool_size: u32,
        acquire_timeout_secs: Option<u64>,
        idle_timeout_secs: Option<u64>,
        max_lifetime_secs: Option<u64>,
    ) -> Result<Self, crate::errors::JsonRegisterError> {
        // Validate SQL identifiers to prevent SQL injection
        validate_sql_identifier(table_name, "table_name")
            .map_err(crate::errors::JsonRegisterError::Configuration)?;
        validate_sql_identifier(id_column, "id_column")
            .map_err(crate::errors::JsonRegisterError::Configuration)?;
        validate_sql_identifier(jsonb_column, "jsonb_column")
            .map_err(crate::errors::JsonRegisterError::Configuration)?;

        // Use provided timeouts or sensible defaults
        let acquire_timeout = Duration::from_secs(acquire_timeout_secs.unwrap_or(5));
        let _idle_timeout = idle_timeout_secs.map(Duration::from_secs);
        let _max_lifetime = max_lifetime_secs.map(Duration::from_secs);

        // Parse connection string into deadpool config
        let mut cfg = Config::new();
        cfg.url = Some(connection_string.to_string());
        cfg.manager = Some(ManagerConfig {
            recycling_method: RecyclingMethod::Fast,
        });
        cfg.pool = Some(deadpool_postgres::PoolConfig {
            max_size: pool_size as usize,
            timeouts: deadpool_postgres::Timeouts {
                wait: Some(acquire_timeout),
                create: Some(Duration::from_secs(10)),
                recycle: Some(Duration::from_secs(10)),
            },
            queue_mode: QueueMode::Fifo,
        });

        let pool = cfg.create_pool(Some(Runtime::Tokio1), NoTls).map_err(|e| {
            // Sanitize any connection strings that might appear in error messages
            let error_msg = e.to_string();
            let sanitized_msg = crate::sanitize_connection_string(&error_msg);
            crate::errors::JsonRegisterError::Configuration(sanitized_msg)
        })?;

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
            queries_executed: AtomicU64::new(0),
            query_errors: AtomicU64::new(0),
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
    /// A `Result` containing the ID (i32) or a `tokio_postgres::Error`.
    pub async fn register_object(&self, json_str: &str) -> Result<i32, tokio_postgres::Error> {
        self.queries_executed.fetch_add(1, Ordering::Relaxed);

        let client = self
            .pool
            .get()
            .await
            .map_err(|e: PoolError<tokio_postgres::Error>| {
                self.query_errors.fetch_add(1, Ordering::Relaxed);
                match e {
                    PoolError::Backend(db_err) => db_err,
                    PoolError::Timeout(_) => tokio_postgres::Error::__private_api_timeout(),
                    _ => tokio_postgres::Error::__private_api_timeout(),
                }
            })?;

        let result = client
            .query_one(&self.register_query, &[&json_str, &json_str])
            .await;

        match result {
            Ok(row) => Ok(row.get(0)),
            Err(e) => {
                self.query_errors.fetch_add(1, Ordering::Relaxed);
                Err(e)
            }
        }
    }

    /// Registers a batch of JSON object strings in the database.
    ///
    /// # Arguments
    ///
    /// * `json_strs` - A slice of canonicalised JSON strings.
    ///
    /// # Returns
    ///
    /// A `Result` containing a vector of IDs or a `tokio_postgres::Error`.
    pub async fn register_batch_objects(
        &self,
        json_strs: &[String],
    ) -> Result<Vec<i32>, tokio_postgres::Error> {
        if json_strs.is_empty() {
            return Ok(vec![]);
        }

        self.queries_executed.fetch_add(1, Ordering::Relaxed);

        let client = self
            .pool
            .get()
            .await
            .map_err(|e: PoolError<tokio_postgres::Error>| {
                self.query_errors.fetch_add(1, Ordering::Relaxed);
                match e {
                    PoolError::Backend(db_err) => db_err,
                    PoolError::Timeout(_) => tokio_postgres::Error::__private_api_timeout(),
                    _ => tokio_postgres::Error::__private_api_timeout(),
                }
            })?;

        let result = client
            .query(&self.register_batch_query, &[&json_strs])
            .await;

        match result {
            Ok(rows) => {
                let mut ids = Vec::with_capacity(rows.len());
                for row in rows {
                    let id: i32 = row.get(0);
                    ids.push(id);
                }
                Ok(ids)
            }
            Err(e) => {
                self.query_errors.fetch_add(1, Ordering::Relaxed);
                Err(e)
            }
        }
    }

    /// Returns the current size of the connection pool.
    ///
    /// This is the total number of connections (both idle and active) currently
    /// in the pool. Useful for monitoring pool utilization.
    ///
    /// # Returns
    ///
    /// The number of connections in the pool.
    pub fn pool_size(&self) -> usize {
        let status = self.pool.status();
        status.size
    }

    /// Returns the number of idle connections in the pool.
    ///
    /// Idle connections are available for immediate use. A low idle count
    /// during high load may indicate the pool is undersized.
    ///
    /// # Returns
    ///
    /// The number of idle connections.
    pub fn idle_connections(&self) -> usize {
        let status = self.pool.status();
        status.available
    }

    /// Checks if the connection pool is closed.
    ///
    /// A closed pool cannot create new connections and will error on acquire attempts.
    ///
    /// # Returns
    ///
    /// `true` if the pool is closed, `false` otherwise.
    pub fn is_closed(&self) -> bool {
        // deadpool doesn't have is_closed, check if pool is available
        let status = self.pool.status();
        status.max_size == 0
    }

    /// Returns the total number of database queries executed.
    ///
    /// # Returns
    ///
    /// The total number of queries executed since instance creation.
    pub fn queries_executed(&self) -> u64 {
        self.queries_executed.load(Ordering::Relaxed)
    }

    /// Returns the total number of database query errors.
    ///
    /// # Returns
    ///
    /// The total number of failed queries since instance creation.
    pub fn query_errors(&self) -> u64 {
        self.query_errors.load(Ordering::Relaxed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_validate_sql_identifier_valid() {
        // Valid identifiers should pass
        assert!(validate_sql_identifier("table_name", "test").is_ok());
        assert!(validate_sql_identifier("_underscore", "test").is_ok());
        assert!(validate_sql_identifier("table123", "test").is_ok());
        assert!(validate_sql_identifier("CamelCase", "test").is_ok());
        assert!(validate_sql_identifier("snake_case_123", "test").is_ok());
    }

    #[test]
    fn test_validate_sql_identifier_empty() {
        // Empty identifier should fail
        let result = validate_sql_identifier("", "test_name");
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("cannot be empty"));
    }

    #[test]
    fn test_validate_sql_identifier_too_long() {
        // Identifier exceeding 63 characters should fail
        let long_name = "a".repeat(64);
        let result = validate_sql_identifier(&long_name, "test_name");
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("63 character limit"));
    }

    #[test]
    fn test_validate_sql_identifier_starts_with_number() {
        // Identifier starting with number should fail
        let result = validate_sql_identifier("123table", "test_name");
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("must start with a letter or underscore"));
    }

    #[test]
    fn test_validate_sql_identifier_invalid_characters() {
        // Identifiers with special characters should fail
        let test_cases = vec![
            "table-name",  // hyphen
            "table.name",  // dot
            "table name",  // space
            "table;name",  // semicolon
            "table'name",  // quote
            "table\"name", // double quote
            "table(name)", // parentheses
            "table*name",  // asterisk
            "table/name",  // slash
        ];

        for test_case in test_cases {
            let result = validate_sql_identifier(test_case, "test_name");
            assert!(result.is_err(), "Expected '{}' to be invalid", test_case);
            assert!(result
                .unwrap_err()
                .to_string()
                .contains("invalid character"));
        }
    }

    #[test]
    fn test_validate_sql_identifier_boundary_cases() {
        // Test boundary cases
        assert!(validate_sql_identifier("a", "test").is_ok()); // Single character
        assert!(validate_sql_identifier("_", "test").is_ok()); // Just underscore

        let exactly_63 = "a".repeat(63);
        assert!(validate_sql_identifier(&exactly_63, "test").is_ok());
    }
}
