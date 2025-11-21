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
    ///
    /// # Returns
    ///
    /// A `Result` containing the new `Register` instance or a `JsonRegisterError`.
    pub async fn new(
        connection_string: &str,
        table_name: &str,
        id_column: &str,
        jsonb_column: &str,
        pool_size: u32,
        lru_cache_size: usize,
    ) -> Result<Self, JsonRegisterError> {
        let db = Db::new(
            connection_string,
            table_name,
            id_column,
            jsonb_column,
            pool_size,
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
        pool_size=10
    ))]
    #[allow(clippy::too_many_arguments)]
    /// Initializes a new `JsonRegister` instance from Python.
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

        let connection_string = format!(
            "postgres://{}:{}@{}:{}/{}",
            database_user, database_password, database_host, database_port, database_name
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
