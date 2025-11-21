# json-register

[![CI](https://github.com/telicent-oss/json-register/actions/workflows/ci.yml/badge.svg)](https://github.com/telicent-oss/json-register/actions/workflows/ci.yml)

> **Note**: This library is currently in beta. The API is stable but may change in future releases based on user feedback and production usage.

`json-register` is a caching registry for JSON objects, with storage in a PostgreSQL database, using their JSONB encoding. It ensures that semantically equivalent JSON objects are cached only once by employing a canonicalisation strategy in the cache, and using JSONB comparisons in the database. The database assigns a uniqiue 32-bit integer identifier to each object.

This library is written in Rust and provides native bindings for Python, allowing for seamless integration into applications written in either language.

## Features

*   **Canonicalisation**: JSON objects are canonicalised (keys sorted, whitespace removed) before storage to ensure uniqueness based on content.
*   **Caching**: An in-memory Least Recently Used (LRU) cache minimizes database lookups for frequently accessed objects.
*   **PostgreSQL Integration**: Efficiently stores and retrieves JSON data using PostgreSQL's `JSONB` type.
*   **Batch Processing**: Supports batch registration of objects to reduce network round-trips and improve throughput.
*   **Cross-Language Support**: Provides a native Rust API and a Python extension module.
*   **Security**: SQL injection prevention through identifier validation and automatic password sanitization in error messages.
*   **Configurable Timeouts**: Optional connection pool timeouts for acquire, idle, and maximum lifetime settings.
*   **Monitoring**: Query methods for connection pool metrics and cache hit rate statistics.

## Installation

### Rust

Add the following to your `Cargo.toml`:

```toml
[dependencies]
json-register = "0.1.0"
tokio = { version = "1.0", features = ["full"] }
serde_json = "1.0"
```

### Python

Ensure you have a compatible Python environment (3.8+) and install the package.

Currently available on TestPyPI:

```bash
pip install --index-url https://test.pypi.org/simple/ --extra-index-url https://pypi.org/simple/ json-register-rust
```

Once published to PyPI:

```bash
pip install json-register-rust
```

## Usage

### Rust Example

The following example demonstrates how to initialize the registry and register JSON objects using the Rust API.

```rust
use json_register::Register;
use serde_json::json;
use std::error::Error;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    // Configuration parameters
    let connection_string = "postgres://user:password@localhost:5432/dbname";
    let table_name = "json_objects";
    let id_column = "id";
    let jsonb_column = "data";
    let pool_size = 10;
    let lru_cache_size = 1000;

    // Initialize the register
    let register = Register::new(
        connection_string,
        table_name,
        id_column,
        jsonb_column,
        pool_size,
        lru_cache_size,
        None, // acquire_timeout_secs (defaults to 5)
        None, // idle_timeout_secs (defaults to 600)
        None, // max_lifetime_secs (defaults to 1800)
    ).await?;

    // Register a single object
    let object = json!({
        "name": "Alice",
        "role": "Engineer",
        "active": true
    });

    let id = register.register_object(&object).await?;
    println!("Registered object with ID: {}", id);

    // Register a batch of objects
    let batch = vec![
        json!({"name": "Bob", "role": "Manager"}),
        json!({"name": "Charlie", "role": "Designer"}),
    ];

    let ids = register.register_batch_objects(&batch).await?;
    println!("Registered batch IDs: {:?}", ids);

    Ok(())
}
```

### Python Example

The following example demonstrates how to use the library within a Python application.

```python
from json_register import JsonRegister
import asyncio

def main():
    # Initialize the register
    # Note: The Python constructor accepts individual connection parameters.
    register = JsonRegister(
        database_name="dbname",
        database_host="localhost",
        database_port=5432,
        database_user="user",
        database_password="password",
        lru_cache_size=1000,
        table_name="json_objects",
        id_column="id",
        jsonb_column="data",
        pool_size=10
    )

    # Register a single object
    obj = {
        "name": "Alice",
        "role": "Engineer",
        "active": True
    }
    
    # The register_object method is synchronous in the Python bindings
    # as it handles the async runtime internally.
    obj_id = register.register_object(obj)
    print(f"Registered object with ID: {obj_id}")

    # Register a batch of objects
    batch = [
        {"name": "Bob", "role": "Manager"},
        {"name": "Charlie", "role": "Designer"}
    ]
    
    batch_ids = register.register_batch_objects(batch)
    print(f"Registered batch IDs: {batch_ids}")

if __name__ == "__main__":
    main()
```

## Configuration

### Timeout Parameters

Optional timeout parameters can be specified when initializing the register. All timeouts are in seconds.

*   `acquire_timeout_secs`: Timeout for acquiring a connection from the pool (default: 5)
*   `idle_timeout_secs`: Timeout before closing idle connections (default: 600)
*   `max_lifetime_secs`: Maximum lifetime of a connection (default: 1800)

### Rust Example with Custom Timeouts

```rust
let register = Register::new(
    connection_string,
    table_name,
    id_column,
    jsonb_column,
    pool_size,
    lru_cache_size,
    Some(10),   // 10 second acquire timeout
    Some(300),  // 5 minute idle timeout
    Some(3600), // 1 hour max lifetime
).await?;
```

### Python Example with Custom Timeouts

```python
register = JsonRegister(
    database_name="dbname",
    database_host="localhost",
    database_port=5432,
    database_user="user",
    database_password="password",
    acquire_timeout_secs=10,   # 10 second acquire timeout
    idle_timeout_secs=300,     # 5 minute idle timeout
    max_lifetime_secs=3600,    # 1 hour max lifetime
)
```

## Monitoring

The library provides methods to query connection pool and cache metrics. Applications can use these to integrate with monitoring systems such as Prometheus, OpenTelemetry, or custom logging.

### Connection Pool Metrics

*   `pool_size()`: Total number of connections in the pool (idle and active)
*   `idle_connections()`: Number of idle connections available for use
*   `is_closed()`: Whether the connection pool is closed

### Cache Metrics

*   `cache_hits()`: Total number of successful cache lookups
*   `cache_misses()`: Total number of unsuccessful cache lookups
*   `cache_hit_rate()`: Hit rate as a percentage (0.0 to 100.0)

### Rust Monitoring Example

```rust
// Query pool metrics
let total = register.pool_size();
let idle = register.idle_connections();
println!("Pool: {}/{} connections, {} idle", total, pool_size, idle);

// Query cache metrics
let hits = register.cache_hits();
let misses = register.cache_misses();
let rate = register.cache_hit_rate();
println!("Cache: {} hits, {} misses ({:.2}% hit rate)", hits, misses, rate);
```

### Python Monitoring Example

```python
# Query pool metrics
total = register.pool_size()
idle = register.idle_connections()
print(f"Pool: {total} connections, {idle} idle")

# Query cache metrics
hits = register.cache_hits()
misses = register.cache_misses()
rate = register.cache_hit_rate()
print(f"Cache: {hits} hits, {misses} misses ({rate:.2f}% hit rate)")
```

## License

This project is licensed under the Apache-2.0 License.