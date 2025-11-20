use thiserror::Error;
#[cfg(feature = "python")]
use pyo3::PyErr;
#[cfg(feature = "python")]
use pyo3::exceptions::PyRuntimeError;

#[derive(Error, Debug)]
pub enum JsonRegisterError {
    #[error("Database error: {0}")]
    DbError(#[from] sqlx::Error),
    #[error("Serialization error: {0}")]
    SerdeError(#[from] serde_json::Error),
    #[error("Configuration error: {0}")]
    Configuration(String),
    #[error("Runtime error: {0}")]
    RuntimeError(String),
    #[error("Python serialization error: {0}")]
    SerializationError(String),
}

#[cfg(feature = "python")]
impl From<JsonRegisterError> for PyErr {
    fn from(err: JsonRegisterError) -> PyErr {
        PyRuntimeError::new_err(err.to_string())
    }
}
