use sqlx::postgres::{PgPoolOptions, PgRow};
use sqlx::{PgPool, Row};
use std::time::Duration;

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
fn validate_sql_identifier(identifier: &str, name: &str) -> Result<(), sqlx::Error> {
    if identifier.is_empty() {
        return Err(sqlx::Error::Configuration(
            format!("{} cannot be empty", name).into(),
        ));
    }

    if identifier.len() > 63 {
        return Err(sqlx::Error::Configuration(
            format!("{} exceeds PostgreSQL's 63 character limit", name).into(),
        ));
    }

    // Validate that identifier contains only safe characters: alphanumeric, underscore
    // Must start with a letter or underscore
    let first_char = identifier.chars().next().unwrap();
    if !first_char.is_ascii_alphabetic() && first_char != '_' {
        return Err(sqlx::Error::Configuration(
            format!("{} must start with a letter or underscore", name).into(),
        ));
    }

    for c in identifier.chars() {
        if !c.is_ascii_alphanumeric() && c != '_' {
            return Err(sqlx::Error::Configuration(
                format!(
                    "{} contains invalid character '{}'. Only alphanumeric and underscore allowed",
                    name, c
                )
                .into(),
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
    /// * `acquire_timeout_secs` - Optional timeout for acquiring connections (default: 5s).
    /// * `idle_timeout_secs` - Optional timeout for idle connections (default: 600s).
    /// * `max_lifetime_secs` - Optional maximum lifetime for connections (default: 1800s).
    ///
    /// # Returns
    ///
    /// A `Result` containing the new `Db` instance or a `sqlx::Error`.
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
    ) -> Result<Self, sqlx::Error> {
        // Validate SQL identifiers to prevent SQL injection
        validate_sql_identifier(table_name, "table_name")?;
        validate_sql_identifier(id_column, "id_column")?;
        validate_sql_identifier(jsonb_column, "jsonb_column")?;

        // Use provided timeouts or sensible defaults
        let acquire_timeout = Duration::from_secs(acquire_timeout_secs.unwrap_or(5));
        let idle_timeout = idle_timeout_secs.map(Duration::from_secs);
        let max_lifetime = max_lifetime_secs.map(Duration::from_secs);

        let pool = PgPoolOptions::new()
            .max_connections(pool_size)
            // Acquire timeout: get a connection from the pool
            .acquire_timeout(acquire_timeout)
            // Idle timeout: close connections idle for too long (default: 10 min)
            .idle_timeout(idle_timeout.or(Some(Duration::from_secs(600))))
            // Max lifetime: close connections after max age (default: 30 min)
            .max_lifetime(max_lifetime.or(Some(Duration::from_secs(1800))))
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
