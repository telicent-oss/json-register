#[cfg(feature = "python")]
use pyo3::exceptions::PyRuntimeError;
#[cfg(feature = "python")]
use pyo3::PyErr;
use thiserror::Error;

/// Errors that can occur during JSON registration.
#[derive(Error, Debug)]
pub enum JsonRegisterError {
    /// An error occurred while interacting with the database.
    #[error("Database error: {0}")]
    DbError(#[from] sqlx::Error),

    /// An error occurred while serializing or deserializing JSON.
    #[error("Serialization error: {0}")]
    SerdeError(#[from] serde_json::Error),

    /// An invalid configuration was provided.
    #[error("Configuration error: {0}")]
    Configuration(String),

    /// A runtime error occurred (e.g., initializing the Tokio runtime).
    #[error("Runtime error: {0}")]
    RuntimeError(String),

    /// An error occurred during Python serialization/deserialization.
    #[error("Python serialization error: {0}")]
    SerializationError(String),
}

#[cfg(feature = "python")]
impl From<JsonRegisterError> for PyErr {
    fn from(err: JsonRegisterError) -> PyErr {
        PyRuntimeError::new_err(err.to_string())
    }
}
