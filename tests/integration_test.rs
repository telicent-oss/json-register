use json_register::{build_connection_string, Register};
use serde_json::json;
use std::collections::HashSet;
use std::env;
use std::time::{SystemTime, UNIX_EPOCH};

/// Retrieves database configuration from environment variables.
///
/// Defaults are provided for local testing convenience.
fn get_config() -> (
    String,
    String,
    String,
    String,
    String,
    usize,
    String,
    String,
    String,
    u32,
) {
    (
        env::var("TEST_DB_NAME").unwrap_or_else(|_| "access".to_string()),
        env::var("TEST_DB_HOST").unwrap_or_else(|_| "localhost".to_string()),
        env::var("TEST_DB_PORT").unwrap_or_else(|_| "5432".to_string()),
        env::var("TEST_DB_USER").unwrap_or_else(|_| "postgres".to_string()),
        env::var("TEST_DB_PASSWORD").unwrap_or_else(|_| "".to_string()),
        100,
        env::var("TEST_DB_TABLE").unwrap_or_else(|_| "labels".to_string()),
        env::var("TEST_DB_ID_COLUMN").unwrap_or_else(|_| "id".to_string()),
        env::var("TEST_DB_JSONB_COLUMN").unwrap_or_else(|_| "label".to_string()),
        5,
    )
}

/// Creates a `Register` instance for testing.
///
/// This function sets up a unique table for each test run (based on the suffix)
/// to ensure test isolation and avoid concurrency issues.
async fn create_register(suffix: &str) -> Register {
    let (db_name, host, port, user, password, cache_size, base_table, id_col, json_col, pool_size) =
        get_config();
    let table = format!("{}_{}", base_table, suffix);
    let port_num: u16 = port.parse().expect("Invalid port number");
    let conn_str = build_connection_string(&user, &password, &host, port_num, &db_name);

    // Ensure the test table exists.
    let (client, connection) = tokio_postgres::connect(&conn_str, tokio_postgres::NoTls)
        .await
        .expect("Failed to connect to DB for setup");

    // Spawn connection in background
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });

    client
        .execute(
            &format!(
                r#"
        CREATE TABLE IF NOT EXISTS {table} (
            {id_col} SERIAL PRIMARY KEY,
            {json_col} JSONB UNIQUE NOT NULL
        )
        "#
            ),
            &[],
        )
        .await
        .expect("Failed to create table");

    Register::new(
        &conn_str, &table, &id_col, &json_col, pool_size, cache_size,
        None, // acquire_timeout_secs
        None, // idle_timeout_secs
        None, // max_lifetime_secs
    )
    .await
    .expect("Failed to connect to DB")
}

fn get_timestamp() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_micros()
}

#[tokio::test]
#[ignore]
async fn test_register_object() {
    // Verifies that registering the same object twice returns the same ID.
    let register = create_register("obj").await;
    let obj = json!({"name": "Alice", "age": 30});

    let id1 = register.register_object(&obj).await.unwrap();
    let id2 = register.register_object(&obj).await.unwrap();

    assert_eq!(id1, id2);
}

#[tokio::test]
#[ignore]
async fn test_register_batch_objects() {
    // Verifies that batch registration returns unique IDs for unique objects.
    let register = create_register("batch").await;
    let objects = vec![
        json!({"name": "Alice"}),
        json!({"name": "Bob"}),
        json!({"name": "Carol"}),
    ];

    let ids = register.register_batch_objects(&objects).await.unwrap();

    assert_eq!(ids.len(), 3);
    let unique_ids: HashSet<_> = ids.iter().collect();
    assert_eq!(unique_ids.len(), 3);
}

#[tokio::test]
#[ignore]
async fn test_batch_order_preserved_all_new() {
    // Verifies that the order of returned IDs matches the order of input objects
    // when all objects are new.
    let register = create_register("order_new").await;
    let timestamp = get_timestamp();

    let objects = vec![
        json!({"test": "batch_order_1", "timestamp": timestamp, "index": 0}),
        json!({"test": "batch_order_2", "timestamp": timestamp, "index": 1}),
        json!({"test": "batch_order_3", "timestamp": timestamp, "index": 2}),
        json!({"test": "batch_order_4", "timestamp": timestamp, "index": 3}),
    ];

    let batch_ids = register.register_batch_objects(&objects).await.unwrap();
    assert_eq!(batch_ids.len(), 4);

    for (i, obj) in objects.iter().enumerate() {
        let individual_id = register.register_object(obj).await.unwrap();
        assert_eq!(
            batch_ids[i], individual_id,
            "Object at index {} should have matching ID",
            i
        );
    }
}

#[tokio::test]
#[ignore]
async fn test_batch_order_preserved_mixed_existing() {
    // Verifies that the order of returned IDs matches the order of input objects
    // when some objects already exist in the database.
    let register = create_register("order_mixed").await;
    let timestamp = get_timestamp();

    let obj1 = json!({"test": "mixed_1", "timestamp": timestamp});
    let obj3 = json!({"test": "mixed_3", "timestamp": timestamp});

    let id1 = register.register_object(&obj1).await.unwrap();
    let id3 = register.register_object(&obj3).await.unwrap();

    let obj2 = json!({"test": "mixed_2", "timestamp": timestamp});
    let obj4 = json!({"test": "mixed_4", "timestamp": timestamp});

    let batch = vec![obj1.clone(), obj2.clone(), obj3.clone(), obj4.clone()];
    let batch_ids = register.register_batch_objects(&batch).await.unwrap();

    assert_eq!(batch_ids.len(), 4);
    assert_eq!(batch_ids[0], id1);
    assert_eq!(batch_ids[2], id3);

    let unique_ids: HashSet<_> = batch_ids.iter().collect();
    assert_eq!(unique_ids.len(), 4);

    let id2 = register.register_object(&obj2).await.unwrap();
    let id4 = register.register_object(&obj4).await.unwrap();

    assert_eq!(batch_ids[1], id2);
    assert_eq!(batch_ids[3], id4);
}

#[tokio::test]
#[ignore]
async fn test_batch_different_key_orders_same_ids() {
    // Verifies that objects with different key orders are treated as identical.
    let register = create_register("key_order").await;
    let timestamp = get_timestamp();

    let batch1 = vec![
        json!({"name": "Alice", "age": 30, "timestamp": timestamp}),
        json!({"name": "Bob", "age": 25, "timestamp": timestamp}),
    ];

    let ids1 = register.register_batch_objects(&batch1).await.unwrap();

    let batch2 = vec![
        json!({"age": 30, "timestamp": timestamp, "name": "Alice"}),
        json!({"timestamp": timestamp, "age": 25, "name": "Bob"}),
    ];

    let ids2 = register.register_batch_objects(&batch2).await.unwrap();

    assert_eq!(ids1, ids2);
}

#[tokio::test]
#[ignore]
async fn test_batch_large_order_preservation() {
    // Verifies order preservation for a larger batch of objects.
    let register = create_register("large").await;
    let timestamp = get_timestamp();

    let mut objects = Vec::new();
    for i in 0..20 {
        objects.push(json!({
            "test": "large_batch",
            "timestamp": timestamp,
            "index": i,
            "data": format!("item_{}", i)
        }));
    }

    let batch_ids = register.register_batch_objects(&objects).await.unwrap();
    assert_eq!(batch_ids.len(), 20);

    for (i, obj) in objects.iter().enumerate() {
        let individual_id = register.register_object(obj).await.unwrap();
        assert_eq!(batch_ids[i], individual_id);
    }

    let batch_ids_repeat = register.register_batch_objects(&objects).await.unwrap();
    assert_eq!(batch_ids, batch_ids_repeat);
}

#[tokio::test]
#[ignore]
async fn test_batch_order_preservation_stress() {
    // Stress test for order preservation with a mix of pre-registered, new, and duplicate objects.
    let register = create_register("stress").await;
    let timestamp = get_timestamp();

    let pre_registered = vec![
        json!({"type": "pre", "id": 0, "timestamp": timestamp}),
        json!({"type": "pre", "id": 2, "timestamp": timestamp}),
        json!({"type": "pre", "id": 5, "timestamp": timestamp}),
        json!({"type": "pre", "id": 7, "timestamp": timestamp}),
        json!({"type": "pre", "id": 9, "timestamp": timestamp}),
    ];

    let mut pre_registered_ids = std::collections::HashMap::new();
    for obj in &pre_registered {
        let id = register.register_object(obj).await.unwrap();
        pre_registered_ids.insert(obj["id"].as_i64().unwrap(), id);
    }

    let batch = vec![
        pre_registered[0].clone(),
        json!({"type": "new", "id": 1, "timestamp": timestamp}),
        pre_registered[1].clone(),
        json!({"type": "new", "id": 3, "timestamp": timestamp}),
        json!({"type": "new", "id": 4, "timestamp": timestamp}),
        pre_registered[2].clone(),
        json!({"type": "new", "id": 6, "timestamp": timestamp}),
        pre_registered[3].clone(),
        json!({"type": "new", "id": 8, "timestamp": timestamp}),
        pre_registered[4].clone(),
    ];

    let batch_ids = register.register_batch_objects(&batch).await.unwrap();
    assert_eq!(batch_ids.len(), 10);

    assert_eq!(batch_ids[0], *pre_registered_ids.get(&0).unwrap());
    assert_eq!(batch_ids[2], *pre_registered_ids.get(&2).unwrap());
    assert_eq!(batch_ids[5], *pre_registered_ids.get(&5).unwrap());
    assert_eq!(batch_ids[7], *pre_registered_ids.get(&7).unwrap());
    assert_eq!(batch_ids[9], *pre_registered_ids.get(&9).unwrap());

    let unique_ids: HashSet<_> = batch_ids.iter().collect();
    assert_eq!(unique_ids.len(), 10);

    for (i, obj) in batch.iter().enumerate() {
        let individual_id = register.register_object(obj).await.unwrap();
        assert_eq!(batch_ids[i], individual_id);
    }

    let batch_with_dupes = vec![
        json!({"type": "dupe_test", "value": "A", "timestamp": timestamp}),
        json!({"type": "dupe_test", "value": "B", "timestamp": timestamp}),
        json!({"type": "dupe_test", "value": "A", "timestamp": timestamp}),
        json!({"type": "dupe_test", "value": "C", "timestamp": timestamp}),
        json!({"type": "dupe_test", "value": "B", "timestamp": timestamp}),
    ];

    let dupe_ids = register
        .register_batch_objects(&batch_with_dupes)
        .await
        .unwrap();
    assert_eq!(dupe_ids.len(), 5);
    assert_eq!(dupe_ids[0], dupe_ids[2]);
    assert_eq!(dupe_ids[1], dupe_ids[4]);

    let unique_dupe_ids: HashSet<_> = dupe_ids.iter().collect();
    assert_eq!(unique_dupe_ids.len(), 3);
}
