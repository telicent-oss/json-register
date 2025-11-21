//! # JSON Register
//!
//! `json-register` is a library for registering JSON objects into a PostgreSQL database
//! with canonicalisation and caching. It ensures that semantically equivalent JSON objects
//! are stored only once and assigned a unique identifier.
//!
//! This library provides both a Rust API and Python bindings.

#[cfg(feature = "python")]
use pyo3::prelude::*;
#[cfg(feature = "python")]
use pyo3::types::PyList;
#[cfg(feature = "python")]
use tokio::runtime::Runtime;

use serde_json::Value;

mod cache;
mod canonicalise;
mod db;
mod errors;

pub use cache::Cache;
pub use canonicalise::canonicalise;
pub use db::Db;
pub use errors::JsonRegisterError;

/// Builds a PostgreSQL connection string from its components.
///
/// # Arguments
///
/// * `user` - Database user name
/// * `password` - Database password
/// * `host` - Database host (e.g., "localhost")
/// * `port` - Database port (e.g., 5432)
/// * `database` - Database name
///
/// # Returns
///
/// A formatted PostgreSQL connection string
pub fn build_connection_string(
    user: &str,
    password: &str,
    host: &str,
    port: u16,
    database: &str,
) -> String {
    format!(
        "postgres://{}:{}@{}:{}/{}",
        user, password, host, port, database
    )
}

/// Sanitizes a connection string by replacing the password with asterisks.
///
/// This prevents passwords from leaking in error messages, logs, or stack traces.
///
/// # Arguments
///
/// * `connection_string` - The connection string to sanitize
///
/// # Returns
///
/// A sanitized connection string with the password replaced by "****"
///
/// # Example
///
/// ```
/// use json_register::sanitize_connection_string;
/// let sanitized = sanitize_connection_string("postgres://user:secret@localhost:5432/db");
/// assert_eq!(sanitized, "postgres://user:****@localhost:5432/db");
/// ```
pub fn sanitize_connection_string(connection_string: &str) -> String {
    // Handle postgres:// or postgresql:// schemes
    if let Some(scheme_end) = connection_string.find("://") {
        let scheme = &connection_string[..scheme_end + 3];
        let rest = &connection_string[scheme_end + 3..];

        // Find the LAST @ symbol before any / (to handle @ in passwords)
        // The @ separates user:password from host:port/db
        let at_idx = if let Some(slash_idx) = rest.find('/') {
            // Find last @ before the slash
            rest[..slash_idx].rfind('@')
        } else {
            // No slash, find last @ in entire string
            rest.rfind('@')
        };

        if let Some(at_idx) = at_idx {
            let user_pass = &rest[..at_idx];
            let host_db = &rest[at_idx..];

            // Find FIRST : separator between user and password
            // (username shouldn't have :, but password might)
            if let Some(colon_idx) = user_pass.find(':') {
                let user = &user_pass[..colon_idx];
                return format!("{}{}:****{}", scheme, user, host_db);
            }
        }
    }

    // If parsing fails, return as-is (no password to hide)
    connection_string.to_string()
}

/// The main registry structure that coordinates database interactions and caching.
///
/// This struct maintains a connection pool to the PostgreSQL database and an
/// in-memory LRU cache to speed up lookups of frequently accessed JSON objects.
pub struct Register {
    db: Db,
    cache: Cache,
}

impl Register {
    /// Creates a new `Register` instance.
    ///
    /// # Arguments
    ///
    /// * `connection_string` - The PostgreSQL connection string.
    /// * `table_name` - The name of the table where JSON objects are stored.
    /// * `id_column` - The name of the column storing the unique ID.
    /// * `jsonb_column` - The name of the column storing the JSONB data.
    /// * `pool_size` - The maximum number of connections in the database pool.
    /// * `lru_cache_size` - The capacity of the in-memory LRU cache.
    /// * `acquire_timeout_secs` - Optional timeout for acquiring connections (default: 5s).
    /// * `idle_timeout_secs` - Optional timeout for idle connections (default: 600s).
    /// * `max_lifetime_secs` - Optional maximum lifetime for connections (default: 1800s).
    ///
    /// # Returns
    ///
    /// A `Result` containing the new `Register` instance or a `JsonRegisterError`.
    #[allow(clippy::too_many_arguments)]
    pub async fn new(
        connection_string: &str,
        table_name: &str,
        id_column: &str,
        jsonb_column: &str,
        pool_size: u32,
        lru_cache_size: usize,
        acquire_timeout_secs: Option<u64>,
        idle_timeout_secs: Option<u64>,
        max_lifetime_secs: Option<u64>,
    ) -> Result<Self, JsonRegisterError> {
        let db = Db::new(
            connection_string,
            table_name,
            id_column,
            jsonb_column,
            pool_size,
            acquire_timeout_secs,
            idle_timeout_secs,
            max_lifetime_secs,
        )
        .await
        .map_err(JsonRegisterError::DbError)?;
        let cache = Cache::new(lru_cache_size);
        Ok(Self { db, cache })
    }

    /// Registers a single JSON object.
    ///
    /// This method canonicalises the input JSON, checks the cache, and if necessary,
    /// inserts the object into the database. It returns the unique ID associated
    /// with the JSON object.
    ///
    /// # Arguments
    ///
    /// * `value` - The JSON value to register.
    ///
    /// # Returns
    ///
    /// A `Result` containing the unique ID (i32) or a `JsonRegisterError`.
    pub async fn register_object(&self, value: &Value) -> Result<i32, JsonRegisterError> {
        let canonical = canonicalise(value).map_err(JsonRegisterError::SerdeError)?;

        if let Some(id) = self.cache.get(&canonical) {
            return Ok(id);
        }

        let id = self
            .db
            .register_object(&canonical)
            .await
            .map_err(JsonRegisterError::DbError)?;

        self.cache.put(canonical, id);

        Ok(id)
    }

    /// Registers a batch of JSON objects.
    ///
    /// This method processes multiple JSON objects efficiently. It first checks the
    /// cache for all items. If any are missing, it performs a batch insert/select
    /// operation in the database. The order of the returned IDs corresponds to the
    /// order of the input values.
    ///
    /// # Arguments
    ///
    /// * `values` - A slice of JSON values to register.
    ///
    /// # Returns
    ///
    /// A `Result` containing a vector of unique IDs or a `JsonRegisterError`.
    pub async fn register_batch_objects(
        &self,
        values: &[Value],
    ) -> Result<Vec<i32>, JsonRegisterError> {
        let mut canonicals = Vec::with_capacity(values.len());
        for value in values {
            canonicals.push(canonicalise(value).map_err(JsonRegisterError::SerdeError)?);
        }

        // Check cache for existing entries
        let mut all_cached = true;
        let mut cached_ids = Vec::with_capacity(values.len());
        for canonical in &canonicals {
            if let Some(id) = self.cache.get(canonical) {
                cached_ids.push(id);
            } else {
                all_cached = false;
                break;
            }
        }

        if all_cached {
            return Ok(cached_ids);
        }

        // If not all items are in the cache, query the database
        let ids = self
            .db
            .register_batch_objects(&canonicals)
            .await
            .map_err(JsonRegisterError::DbError)?;

        // Update the cache with the newly retrieved IDs
        for (canonical, id) in canonicals.into_iter().zip(ids.iter()) {
            self.cache.put(canonical, *id);
        }

        Ok(ids)
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
        self.db.pool_size()
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
        self.db.idle_connections()
    }

    /// Checks if the connection pool is closed.
    ///
    /// A closed pool cannot create new connections and will error on acquire attempts.
    ///
    /// # Returns
    ///
    /// `true` if the pool is closed, `false` otherwise.
    pub fn is_closed(&self) -> bool {
        self.db.is_closed()
    }

    /// Returns the number of cache hits.
    ///
    /// # Returns
    ///
    /// The total number of successful cache lookups.
    pub fn cache_hits(&self) -> u64 {
        self.cache.hits()
    }

    /// Returns the number of cache misses.
    ///
    /// # Returns
    ///
    /// The total number of unsuccessful cache lookups.
    pub fn cache_misses(&self) -> u64 {
        self.cache.misses()
    }

    /// Returns the cache hit rate as a percentage.
    ///
    /// # Returns
    ///
    /// The hit rate as a float between 0.0 and 100.0.
    /// Returns 0.0 if no cache operations have occurred.
    pub fn cache_hit_rate(&self) -> f64 {
        self.cache.hit_rate()
    }
}

#[cfg(feature = "python")]
#[pyclass(name = "JsonRegister")]
/// Python wrapper for the `Register` struct.
struct PyJsonRegister {
    inner: Register,
    rt: Runtime,
}

#[cfg(feature = "python")]
#[pymethods]
impl PyJsonRegister {
    #[new]
    #[pyo3(signature = (
        database_name,
        database_host,
        database_port,
        database_user,
        database_password,
        lru_cache_size=1000,
        table_name="json_objects",
        id_column="id",
        jsonb_column="json_object",
        pool_size=10,
        acquire_timeout_secs=None,
        idle_timeout_secs=None,
        max_lifetime_secs=None
    ))]
    #[allow(clippy::too_many_arguments)]
    /// Initializes a new `JsonRegister` instance from Python.
    ///
    /// # Optional Timeout Parameters
    ///
    /// * `acquire_timeout_secs` - Timeout for acquiring a connection from pool (default: 5)
    /// * `idle_timeout_secs` - Timeout for idle connections before closure (default: 600)
    /// * `max_lifetime_secs` - Maximum lifetime of connections (default: 1800)
    fn new(
        database_name: String,
        database_host: String,
        database_port: u16,
        database_user: String,
        database_password: String,
        lru_cache_size: usize,
        table_name: &str,
        id_column: &str,
        jsonb_column: &str,
        pool_size: u32,
        acquire_timeout_secs: Option<u64>,
        idle_timeout_secs: Option<u64>,
        max_lifetime_secs: Option<u64>,
    ) -> PyResult<Self> {
        // Validate configuration parameters
        if database_name.is_empty() {
            return Err(
                JsonRegisterError::Configuration("database_name cannot be empty".into()).into(),
            );
        }

        if database_host.is_empty() {
            return Err(
                JsonRegisterError::Configuration("database_host cannot be empty".into()).into(),
            );
        }

        if database_port == 0 {
            return Err(JsonRegisterError::Configuration(
                "database_port must be between 1 and 65535".into(),
            )
            .into());
        }

        if pool_size == 0 {
            return Err(JsonRegisterError::Configuration(
                "pool_size must be greater than 0".into(),
            )
            .into());
        }

        if pool_size > 10000 {
            return Err(JsonRegisterError::Configuration(
                "pool_size exceeds reasonable maximum of 10000".into(),
            )
            .into());
        }

        if table_name.is_empty() {
            return Err(
                JsonRegisterError::Configuration("table_name cannot be empty".into()).into(),
            );
        }

        if id_column.is_empty() {
            return Err(
                JsonRegisterError::Configuration("id_column cannot be empty".into()).into(),
            );
        }

        if jsonb_column.is_empty() {
            return Err(
                JsonRegisterError::Configuration("jsonb_column cannot be empty".into()).into(),
            );
        }

        let connection_string = build_connection_string(
            &database_user,
            &database_password,
            &database_host,
            database_port,
            &database_name,
        );

        let rt = Runtime::new().map_err(|e| JsonRegisterError::RuntimeError(e.to_string()))?;

        let inner = rt.block_on(async {
            Register::new(
                &connection_string,
                table_name,
                id_column,
                jsonb_column,
                pool_size,
                lru_cache_size,
                acquire_timeout_secs,
                idle_timeout_secs,
                max_lifetime_secs,
            )
            .await
        })?;

        Ok(PyJsonRegister { inner, rt })
    }

    /// Registers a single JSON object from Python.
    fn register_object(&self, json_obj: &Bound<'_, PyAny>) -> PyResult<i32> {
        let value: Value = pythonize::depythonize(json_obj)
            .map_err(|e| JsonRegisterError::SerializationError(e.to_string()))?;
        self.rt
            .block_on(self.inner.register_object(&value))
            .map_err(Into::into)
    }

    /// Registers a batch of JSON objects from Python.
    fn register_batch_objects(&self, json_objects: &Bound<'_, PyList>) -> PyResult<Vec<i32>> {
        let mut values = Vec::with_capacity(json_objects.len());
        for obj in json_objects {
            let value: Value = pythonize::depythonize(&obj)
                .map_err(|e| JsonRegisterError::SerializationError(e.to_string()))?;
            values.push(value);
        }
        self.rt
            .block_on(self.inner.register_batch_objects(&values))
            .map_err(Into::into)
    }

    /// Returns the current size of the connection pool.
    ///
    /// This is the total number of connections (both idle and active) currently
    /// in the pool. Useful for monitoring pool utilization.
    fn pool_size(&self) -> usize {
        self.inner.pool_size()
    }

    /// Returns the number of idle connections in the pool.
    ///
    /// Idle connections are available for immediate use. A low idle count
    /// during high load may indicate the pool is undersized.
    fn idle_connections(&self) -> usize {
        self.inner.idle_connections()
    }

    /// Checks if the connection pool is closed.
    ///
    /// A closed pool cannot create new connections and will error on acquire attempts.
    fn is_closed(&self) -> bool {
        self.inner.is_closed()
    }

    /// Returns the number of cache hits.
    ///
    /// This is the total number of successful cache lookups since the instance was created.
    fn cache_hits(&self) -> u64 {
        self.inner.cache_hits()
    }

    /// Returns the number of cache misses.
    ///
    /// This is the total number of unsuccessful cache lookups since the instance was created.
    fn cache_misses(&self) -> u64 {
        self.inner.cache_misses()
    }

    /// Returns the cache hit rate as a percentage.
    ///
    /// Returns a value between 0.0 and 100.0. Returns 0.0 if no cache operations have occurred.
    fn cache_hit_rate(&self) -> f64 {
        self.inner.cache_hit_rate()
    }
}

#[cfg(feature = "python")]
#[pyfunction(name = "canonicalise")]
/// Canonicalises a Python object into its JSON string representation (as bytes).
fn py_canonicalise(json_obj: &Bound<'_, PyAny>) -> PyResult<Vec<u8>> {
    let value: Value = pythonize::depythonize(json_obj)
        .map_err(|e| JsonRegisterError::SerializationError(e.to_string()))?;
    crate::canonicalise::canonicalise(&value)
        .map(|s| s.into_bytes())
        .map_err(|e| JsonRegisterError::SerdeError(e).into())
}

/// A Python module implemented in Rust.
#[cfg(feature = "python")]
#[pymodule]
fn json_register(_m: &Bound<'_, PyModule>) -> PyResult<()> {
    _m.add_class::<PyJsonRegister>()?;
    _m.add_function(wrap_pyfunction!(py_canonicalise, _m)?)?;
    Ok(())
}

#[cfg(test)]
mod connection_tests {
    use super::*;

    #[test]
    fn test_sanitize_connection_string_with_password() {
        let input = "postgres://user:secret123@localhost:5432/mydb";
        let expected = "postgres://user:****@localhost:5432/mydb";
        assert_eq!(sanitize_connection_string(input), expected);
    }

    #[test]
    fn test_sanitize_connection_string_postgresql_scheme() {
        let input = "postgresql://admin:p@ssw0rd@db.example.com:5432/production";
        let expected = "postgresql://admin:****@db.example.com:5432/production";
        assert_eq!(sanitize_connection_string(input), expected);
    }

    #[test]
    fn test_sanitize_connection_string_no_password() {
        // No password in connection string
        let input = "postgres://user@localhost:5432/mydb";
        assert_eq!(sanitize_connection_string(input), input);
    }

    #[test]
    fn test_sanitize_connection_string_with_special_chars() {
        let input = "postgres://user:p@ss:word@localhost:5432/mydb";
        let expected = "postgres://user:****@localhost:5432/mydb";
        assert_eq!(sanitize_connection_string(input), expected);
    }

    #[test]
    fn test_sanitize_connection_string_not_postgres() {
        // Works with other schemes too
        let input = "mysql://user:password@localhost:3306/mydb";
        let expected = "mysql://user:****@localhost:3306/mydb";
        assert_eq!(sanitize_connection_string(input), expected);
    }

    #[test]
    fn test_sanitize_connection_string_malformed() {
        // Malformed string - return as-is
        let input = "not a connection string";
        assert_eq!(sanitize_connection_string(input), input);
    }
}
