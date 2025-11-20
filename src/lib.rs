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

pub struct Register {
    db: Db,
    cache: Cache,
}

impl Register {
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

    pub async fn register_object(&self, value: &Value) -> Result<i64, JsonRegisterError> {
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

    pub async fn register_batch_objects(
        &self,
        values: &[Value],
    ) -> Result<Vec<i64>, JsonRegisterError> {
        let mut canonicals = Vec::with_capacity(values.len());
        for value in values {
            canonicals.push(canonicalise(value).map_err(JsonRegisterError::SerdeError)?);
        }

        // Check cache
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

        // Not all cached, go to DB
        let ids = self
            .db
            .register_batch_objects(&canonicals)
            .await
            .map_err(JsonRegisterError::DbError)?;

        // Update cache
        for (canonical, id) in canonicals.into_iter().zip(ids.iter()) {
            self.cache.put(canonical, *id);
        }

        Ok(ids)
    }
}

#[cfg(feature = "python")]
#[pyclass(name = "JsonRegister")]
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
        if database_name.is_empty() {
            return Err(
                JsonRegisterError::Configuration("database_name cannot be empty".into()).into(),
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

    fn register_object(&self, json_obj: &Bound<'_, PyAny>) -> PyResult<i64> {
        let value: Value = pythonize::depythonize(json_obj)
            .map_err(|e| JsonRegisterError::SerializationError(e.to_string()))?;
        self.rt
            .block_on(self.inner.register_object(&value))
            .map_err(Into::into)
    }

    fn register_batch_objects(&self, json_objects: &Bound<'_, PyList>) -> PyResult<Vec<i64>> {
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
