use serde_json::Value;

/// Convert a JSON object to its canonical string representation.
///
/// This function ensures that:
/// - Object keys are sorted alphabetically (serde_json::Value uses BTreeMap by default)
/// - Whitespace is removed (serde_json::to_string produces compact JSON)
/// - Unicode characters are preserved (serde_json default behavior)
pub fn canonicalise(json: &Value) -> Result<String, serde_json::Error> {
    serde_json::to_string(json)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_canonicalise_simple_object() {
        let obj = json!({"b": 2, "a": 1});
        let result = canonicalise(&obj).unwrap();
        assert_eq!(result, r#"{"a":1,"b":2}"#);
    }

    #[test]
    fn test_canonicalise_different_key_order_same_result() {
        let obj1 = json!({"name": "Alice", "age": 30});
        let obj2 = json!({"age": 30, "name": "Alice"});
        assert_eq!(canonicalise(&obj1).unwrap(), canonicalise(&obj2).unwrap());
    }

    #[test]
    fn test_canonicalise_nested_object() {
        let obj = json!({"outer": {"b": 2, "a": 1}});
        let result = canonicalise(&obj).unwrap();
        assert_eq!(result, r#"{"outer":{"a":1,"b":2}}"#);
    }

    #[test]
    fn test_canonicalise_deeply_nested() {
        let obj = json!({"level1": {"level2": {"level3": {"level4": {"d": 4, "c": 3, "b": 2, "a": 1}}}}});
        let result = canonicalise(&obj).unwrap();
        assert_eq!(result, r#"{"level1":{"level2":{"level3":{"level4":{"a":1,"b":2,"c":3,"d":4}}}}}"#);
    }

    #[test]
    fn test_canonicalise_array_order_preserved() {
        let obj = json!({"items": [3, 1, 2]});
        let result = canonicalise(&obj).unwrap();
        assert_eq!(result, r#"{"items":[3,1,2]}"#);
    }

    #[test]
    fn test_canonicalise_array_with_objects() {
        let obj = json!({"users": [{"name": "Bob", "age": 25}, {"name": "Alice", "age": 30}]});
        let result = canonicalise(&obj).unwrap();
        // Array order preserved, but object keys sorted
        assert_eq!(result, r#"{"users":[{"age":25,"name":"Bob"},{"age":30,"name":"Alice"}]}"#);
    }

    #[test]
    fn test_canonicalise_primitives() {
        assert_eq!(canonicalise(&json!("hello")).unwrap(), r#""hello""#);
        assert_eq!(canonicalise(&json!(42)).unwrap(), "42");
        // Note: serde_json might format floats differently than Python's json.dumps
        // Python: 3.14 -> "3.14"
        // Rust: 3.14 -> "3.14"
        assert_eq!(canonicalise(&json!(3.14)).unwrap(), "3.14");
        assert_eq!(canonicalise(&json!(true)).unwrap(), "true");
        assert_eq!(canonicalise(&json!(false)).unwrap(), "false");
        assert_eq!(canonicalise(&json!(null)).unwrap(), "null");
    }

    #[test]
    fn test_canonicalise_empty_structures() {
        assert_eq!(canonicalise(&json!({})).unwrap(), "{}");
        assert_eq!(canonicalise(&json!([])).unwrap(), "[]");
    }

    #[test]
    fn test_canonicalise_number_formatting() {
        assert_eq!(canonicalise(&json!(42)).unwrap(), "42");
        assert_eq!(canonicalise(&json!(0)).unwrap(), "0");
        assert_eq!(canonicalise(&json!(-10)).unwrap(), "-10");

        assert_eq!(canonicalise(&json!(3.14)).unwrap(), "3.14");
        // serde_json preserves 0.0 as 0.0
        assert_eq!(canonicalise(&json!(0.0)).unwrap(), "0.0");
        assert_eq!(canonicalise(&json!(-2.5)).unwrap(), "-2.5");
        
        // Scientific notation might differ slightly depending on precision, but 1e10 is usually 10000000000.0
        // Let's check what serde_json does.
        // serde_json::to_string(&json!(1e10)) -> "10000000000.0"
        assert_eq!(canonicalise(&json!(1e10)).unwrap(), "10000000000.0");
    }

    #[test]
    fn test_canonicalise_unicode() {
        let obj = json!({"russian": "Алиса", "emoji": "🎉", "chinese": "你好", "arabic": "مرحبا"});
        let result = canonicalise(&obj).unwrap();
        
        // serde_json by default does NOT escape unicode characters unless configured otherwise,
        // but to_string() usually produces compact JSON.
        // Let's verify the content is present.
        assert!(result.contains("Алиса"));
        assert!(result.contains("🎉"));
        assert!(result.contains("你好"));
        assert!(result.contains("مرحبا"));
    }

    #[test]
    fn test_canonicalise_special_characters() {
        let obj = json!({
            "quote": "He said \"hello\"",
            "newline": "line1\nline2",
            "tab": "col1\tcol2",
            "backslash": "path\\to\\file",
        });
        let result = canonicalise(&obj).unwrap();

        // Special characters should be properly escaped
        // Note: Rust raw strings might make this tricky to assert exactly without escaping the assertion string too.
        // Expected: "quote":"He said \"hello\""
        assert!(result.contains(r#""quote":"He said \"hello\"""#));
        assert!(result.contains(r#"\n"#));
        assert!(result.contains(r#"\t"#));
        // Backslash is escaped as \\
        assert!(result.contains(r#"\\"#));
    }

    #[test]
    fn test_canonicalise_mixed_types() {
        let obj = json!({
            "string": "hello",
            "number": 42,
            "float": 3.14,
            "bool": true,
            "null": null,
            "array": [1, "two", 3.0],
            "object": {"nested": "value"},
        });
        let result = canonicalise(&obj).unwrap();

        let expected = r#"{"array":[1,"two",3.0],"bool":true,"float":3.14,"null":null,"number":42,"object":{"nested":"value"},"string":"hello"}"#;
        assert_eq!(result, expected);
    }

    #[test]
    fn test_canonicalise_with_arrays() {
        let obj = json!({"z_last": [{"b": 2, "a": 1}, {"d": 4, "c": 3}], "a_first": [3, 2, 1]});
        let result = canonicalise(&obj).unwrap();

        let expected = r#"{"a_first":[3,2,1],"z_last":[{"a":1,"b":2},{"c":3,"d":4}]}"#;
        assert_eq!(result, expected);
    }
}
