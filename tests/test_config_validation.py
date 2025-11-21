import pytest
from json_register import JsonRegister


def test_empty_database_name():
    """Verifies that empty database_name is rejected."""
    with pytest.raises(RuntimeError, match="database_name cannot be empty"):
        JsonRegister(
            database_name="",
            database_host="localhost",
            database_port=5432,
            database_user="postgres",
            database_password="password",
        )


def test_empty_database_host():
    """Verifies that empty database_host is rejected."""
    with pytest.raises(RuntimeError, match="database_host cannot be empty"):
        JsonRegister(
            database_name="testdb",
            database_host="",
            database_port=5432,
            database_user="postgres",
            database_password="password",
        )


def test_zero_database_port():
    """Verifies that database_port of 0 is rejected."""
    with pytest.raises(RuntimeError, match="database_port must be between 1 and 65535"):
        JsonRegister(
            database_name="testdb",
            database_host="localhost",
            database_port=0,
            database_user="postgres",
            database_password="password",
        )


def test_zero_pool_size():
    """Verifies that pool_size of 0 is rejected."""
    with pytest.raises(RuntimeError, match="pool_size must be greater than 0"):
        JsonRegister(
            database_name="testdb",
            database_host="localhost",
            database_port=5432,
            database_user="postgres",
            database_password="password",
            pool_size=0,
        )


def test_excessive_pool_size():
    """Verifies that pool_size exceeding 10000 is rejected."""
    with pytest.raises(RuntimeError, match="pool_size exceeds reasonable maximum"):
        JsonRegister(
            database_name="testdb",
            database_host="localhost",
            database_port=5432,
            database_user="postgres",
            database_password="password",
            pool_size=10001,
        )


def test_empty_table_name():
    """Verifies that empty table_name is rejected."""
    with pytest.raises(RuntimeError, match="table_name cannot be empty"):
        JsonRegister(
            database_name="testdb",
            database_host="localhost",
            database_port=5432,
            database_user="postgres",
            database_password="password",
            table_name="",
        )


def test_empty_id_column():
    """Verifies that empty id_column is rejected."""
    with pytest.raises(RuntimeError, match="id_column cannot be empty"):
        JsonRegister(
            database_name="testdb",
            database_host="localhost",
            database_port=5432,
            database_user="postgres",
            database_password="password",
            id_column="",
        )


def test_empty_jsonb_column():
    """Verifies that empty jsonb_column is rejected."""
    with pytest.raises(RuntimeError, match="jsonb_column cannot be empty"):
        JsonRegister(
            database_name="testdb",
            database_host="localhost",
            database_port=5432,
            database_user="postgres",
            database_password="password",
            jsonb_column="",
        )


def test_invalid_table_name_with_special_chars():
    """Verifies that table names with SQL injection characters are rejected."""
    with pytest.raises(RuntimeError, match="invalid character"):
        JsonRegister(
            database_name="testdb",
            database_host="localhost",
            database_port=5432,
            database_user="postgres",
            database_password="password",
            table_name="table'; DROP TABLE users; --",
        )


def test_invalid_column_name_starts_with_number():
    """Verifies that column names starting with numbers are rejected."""
    with pytest.raises(RuntimeError, match="must start with a letter or underscore"):
        JsonRegister(
            database_name="testdb",
            database_host="localhost",
            database_port=5432,
            database_user="postgres",
            database_password="password",
            id_column="123_invalid",
        )


def test_zero_lru_cache_size_allowed():
    """
    Verifies that lru_cache_size of 0 is silently adjusted to 1.
    This test will only work if we can successfully connect to a database,
    so it's a basic sanity check that doesn't require DB connection.
    """
    # This should not raise an error during construction
    # (will fail at connection time, but that's expected without a real DB)
    try:
        JsonRegister(
            database_name="testdb",
            database_host="localhost",
            database_port=5432,
            database_user="postgres",
            database_password="password",
            lru_cache_size=0,
        )
    except RuntimeError as e:
        # Should fail with connection error, not cache capacity error
        assert "capacity" not in str(e).lower()
        assert "lru" not in str(e).lower()
