from json_register import canonicalise


def test_empty_dict():
    assert canonicalise({}) == b"{}"

def test_simple_dict():
    assert canonicalise({"a": 1, "b": 2}) == b'{"a":1,"b":2}'

def test_nested_dict():
    assert canonicalise({"a": {"b": 1}}) == b'{"a":{"b":1}}'

def test_list_ordering():
    # Lists should preserve order
    assert canonicalise({"a": [2, 1]}) == b'{"a":[2,1]}'
    assert canonicalise({"a": [1, 2]}) == b'{"a":[1,2]}'
    assert canonicalise({"a": [2, 1]}) != canonicalise({"a": [1, 2]})

def test_dict_key_ordering():
    # Dict keys should be sorted
    obj1 = {"a": 1, "b": 2}
    obj2 = {"b": 2, "a": 1}
    assert canonicalise(obj1) == canonicalise(obj2)
    assert canonicalise(obj1) == b'{"a":1,"b":2}'

def test_types():
    obj = {"a": 1, "b": "s", "c": True, "d": None}
    expected = b'{"a":1,"b":"s","c":true,"d":null}'
    assert canonicalise(obj) == expected

def test_unicode():
    # Unicode should be preserved (UTF-8)
    # Note: Rust serde_json::to_string usually produces compact JSON with unicode characters as is.
    # Python json.dumps by default escapes non-ascii, but we want to match Rust's behavior.
    # If Rust returns UTF-8 bytes, Python should see them.
    obj = {"a": "café"}
    # "café" in UTF-8 bytes is b'caf\xc3\xa9'
    # JSON string: '{"a":"café"}' -> b'{"a":"caf\xc3\xa9"}'
    assert canonicalise(obj) == b'{"a":"caf\xc3\xa9"}'

def test_numbers():
    # Integers
    assert canonicalise({"a": 1}) == b'{"a":1}'
    # Floats
    # 1.5 -> 1.5
    assert canonicalise({"b": 1.5}) == b'{"b":1.5}'

def test_deeply_nested():
    obj = {"level1": {"level2": {"level3": {"level4": {"d": 4, "c": 3, "b": 2, "a": 1}}}}}
    expected = b'{"level1":{"level2":{"level3":{"level4":{"a":1,"b":2,"c":3,"d":4}}}}}'
    assert canonicalise(obj) == expected

def test_mixed_types_in_list():
    obj = {"a": [1, "two", 3.0, True, None]}
    # Note: Rust might output 3.0 as 3.0
    assert canonicalise(obj) == b'{"a":[1,"two",3.0,true,null]}'

def test_empty_list():
    assert canonicalise([]) == b"[]"

def test_whitespace():
    # Input whitespace shouldn't matter if it's just python objects,
    # but if we were parsing JSON string it would.
    # Here we pass python objects.
    pass

def test_utf8_ordering():
    # Keys should be sorted by their UTF-8 bytes
    # "a" < "b"
    # "ä" (C3 A4) > "a" (61) ? No, 0xC3 > 0x61.
    # "z" (7A) < "ä" (C3 A4) ? Yes.
    obj = {"z": 1, "ä": 2}
    # Expected order: "z", "ä" because 'z' is 0x7A, 'ä' starts with 0xC3.
    # Wait, standard string sort?
    # Rust BTreeMap sorts by String (UTF-8 bytes).
    # 'z' (122) vs 'ä' (228).
    # So "z" comes before "ä".
    expected = b'{"z":1,"\xc3\xa4":2}'
    assert canonicalise(obj) == expected
