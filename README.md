# json-register

`json-register` is a caching registry for JSON objects, with storage in a PostgreSQL database, using their JSONB encoding. It ensures that semantically equivalent JSON objects are cached only once by employing a canonicalisation strategy in the cache, and using JSONB comparisons in the database. The database assigns a uniqiue 32-bit integer identifier to each object.

This library is written in Rust and provides native bindings for Python, allowing for seamless integration into applications written in either language.

## Features

*   **Canonicalisation**: JSON objects are canonicalised (keys sorted, whitespace removed) before storage to ensure uniqueness based on content.
*   **Caching**: An in-memory Least Recently Used (LRU) cache minimizes database lookups for frequently accessed objects.
*   **PostgreSQL Integration**: Efficiently stores and retrieves JSON data using PostgreSQL's `JSONB` type.
*   **Batch Processing**: Supports batch registration of objects to reduce network round-trips and improve throughput.
*   **Cross-Language Support**: Provides a native Rust API and a Python extension module.

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

Ensure you have a compatible Python environment (3.8+) and install the package:

```bash
pip install json-register
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

## License

This project is licensed under the Apache-2.0 License.