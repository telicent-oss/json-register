from json_register import canonicalise


def test_empty_dict():
    """Verifies canonicalisation of an empty dictionary."""
    assert canonicalise({}) == b"{}"

def test_simple_dict():
    """Verifies canonicalisation of a simple dictionary."""
    assert canonicalise({"a": 1, "b": 2}) == b'{"a":1,"b":2}'

def test_nested_dict():
    """Verifies canonicalisation of a nested dictionary."""
    assert canonicalise({"a": {"b": 1}}) == b'{"a":{"b":1}}'

def test_list_ordering():
    """Verifies that list order is preserved."""
    assert canonicalise({"a": [2, 1]}) == b'{"a":[2,1]}'
    assert canonicalise({"a": [1, 2]}) == b'{"a":[1,2]}'
    assert canonicalise({"a": [2, 1]}) != canonicalise({"a": [1, 2]})

def test_dict_key_ordering():
    """Verifies that dictionary keys are sorted alphabetically."""
    obj1 = {"a": 1, "b": 2}
    obj2 = {"b": 2, "a": 1}
    assert canonicalise(obj1) == canonicalise(obj2)
    assert canonicalise(obj1) == b'{"a":1,"b":2}'

def test_types():
    """Verifies canonicalisation of various JSON types."""
    obj = {"a": 1, "b": "s", "c": True, "d": None}
    expected = b'{"a":1,"b":"s","c":true,"d":null}'
    assert canonicalise(obj) == expected

def test_unicode():
    """
    Verifies that Unicode characters are preserved as UTF-8 bytes.

    Rust's serde_json::to_string produces compact JSON with Unicode characters unescaped.
    This test ensures that the Python bindings return the expected UTF-8 bytes.
    """
    obj = {"a": "café"}
    # "café" in UTF-8 bytes is b'caf\xc3\xa9'
    # JSON string: '{"a":"café"}' -> b'{"a":"caf\xc3\xa9"}'
    assert canonicalise(obj) == b'{"a":"caf\xc3\xa9"}'

def test_numbers():
    """Verifies canonicalisation of numbers (integers and floats)."""
    # Integers
    assert canonicalise({"a": 1}) == b'{"a":1}'
    # Floats
    # 1.5 -> 1.5
    assert canonicalise({"b": 1.5}) == b'{"b":1.5}'

def test_deeply_nested():
    """Verifies canonicalisation of a deeply nested structure."""
    obj = {"level1": {"level2": {"level3": {"level4": {"d": 4, "c": 3, "b": 2, "a": 1}}}}}
    expected = b'{"level1":{"level2":{"level3":{"level4":{"a":1,"b":2,"c":3,"d":4}}}}}'
    assert canonicalise(obj) == expected

def test_mixed_types_in_list():
    """Verifies canonicalisation of a list containing mixed types."""
    obj = {"a": [1, "two", 3.0, True, None]}
    assert canonicalise(obj) == b'{"a":[1,"two",3.0,true,null]}'

def test_empty_list():
    """Verifies canonicalisation of an empty list."""
    assert canonicalise([]) == b"[]"

def test_whitespace():
    """
    Placeholder for whitespace tests.

    Input whitespace in Python objects doesn't affect the output because
    we are serializing the object structure, not parsing a JSON string.
    """
    pass

def test_utf8_ordering():
    """
    Verifies that keys are sorted based on their UTF-8 byte representation.

    'z' (0x7A) comes before 'ä' (0xC3 0xA4) in UTF-8 byte order.
    """
    obj = {"z": 1, "ä": 2}
    expected = b'{"z":1,"\xc3\xa4":2}'
    assert canonicalise(obj) == expected
