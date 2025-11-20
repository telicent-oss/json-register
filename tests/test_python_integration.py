import os

import pytest
from json_register import JsonRegister


# Helper to parse DATABASE_URL
def get_db_config():
    db_url = os.environ.get("DATABASE_URL", "postgres://postgres:postgres@localhost:5432/json_register_test")
    # Simple parsing
    # postgres://user:pass@host:port/dbname
    try:
        if "://" in db_url:
            prefix, rest = db_url.split("://")
        else:
            rest = db_url

        if "@" in rest:
            user_pass, host_port_db = rest.split("@")
            if ":" in user_pass:
                user, password = user_pass.split(":")
            else:
                user = user_pass
                password = ""
        else:
            user = "postgres"
            password = ""
            host_port_db = rest

        if "/" in host_port_db:
            host_port, dbname = host_port_db.split("/")
        else:
            host_port = host_port_db
            dbname = "postgres"

        if ":" in host_port:
            host, port = host_port.split(":")
        else:
            host = host_port
            port = "5432"

        return {
            "database_name": dbname,
            "database_host": host,
            "database_port": int(port),
            "database_user": user,
            "database_password": password
        }
    except Exception as e:
        print(f"Failed to parse DATABASE_URL: {e}")
        return None

@pytest.fixture
def db_config():
    config = get_db_config()
    if not config:
        pytest.skip("Invalid or missing DATABASE_URL")
    return config

@pytest.fixture
def register(db_config):
    # Create a unique table name for each test run?
    # For simplicity, use a fixed test table.
    table_name = "json_objects_test_py"

    # Ensure table exists using psycopg (since we can't rely on the Rust code to create it yet)
    try:
        import psycopg
        conn_str = f"postgresql://{db_config['database_user']}:{db_config['database_password']}@{db_config['database_host']}:{db_config['database_port']}/{db_config['database_name']}"
        with psycopg.connect(conn_str) as conn:
            with conn.cursor() as cur:
                cur.execute(f"""
                    CREATE TABLE IF NOT EXISTS {table_name} (
                        id BIGSERIAL PRIMARY KEY,
                        json_object JSONB UNIQUE NOT NULL
                    )
                """)
            conn.commit()
    except ImportError:
        # If psycopg is not available, we might fail if the table doesn't exist.
        # But we should have it installed in the dev environment.
        print("Warning: psycopg not found, skipping table creation")
    except Exception as e:
        print(f"Warning: Failed to create table: {e}")

    try:
        reg = JsonRegister(
            database_name=db_config["database_name"],
            database_host=db_config["database_host"],
            database_port=db_config["database_port"],
            database_user=db_config["database_user"],
            database_password=db_config["database_password"],
            lru_cache_size=1000,
            table_name=table_name,
            id_column="id",
            jsonb_column="json_object",
            pool_size=5
        )
        return reg
    except Exception as e:
        pytest.skip(f"Failed to connect to DB: {e}")

def test_register_object(register):
    obj = {"a": 1, "b": 2}
    id1 = register.register_object(obj)
    assert isinstance(id1, int)

    # Register same object again, should get same ID
    id2 = register.register_object(obj)
    assert id1 == id2

    # Register different object
    obj2 = {"a": 1, "b": 3}
    id3 = register.register_object(obj2)
    assert id1 != id3

def test_register_batch_objects(register):
    objs = [{"a": 1}, {"b": 2}, {"a": 1}]
    ids = register.register_batch_objects(objs)
    assert len(ids) == 3
    assert ids[0] == ids[2]
    assert ids[0] != ids[1]

def test_batch_order_preservation(register):
    # Create a list of objects
    objs = [{"k": i} for i in range(100)]
    ids = register.register_batch_objects(objs)

    assert len(ids) == 100

    # Verify IDs are unique (since objects are unique)
    assert len(set(ids)) == 100

    # Register again mixed with new ones
    objs2 = [{"k": i} for i in range(50, 150)]
    ids2 = register.register_batch_objects(objs2)

    assert len(ids2) == 100
    # First 50 of ids2 should match last 50 of ids
    assert ids2[:50] == ids[50:]

def test_types_roundtrip(register):
    # Test that we can register various types and they are handled correctly
    id1 = register.register_object({"a": 1})
    id2 = register.register_object({"a": "1"})
    assert id1 != id2

    id3 = register.register_object({"a": True})
    id4 = register.register_object({"a": False})
    assert id3 != id4

    id5 = register.register_object({"a": None})
    id6 = register.register_object({"a": []})
    assert id5 != id6
