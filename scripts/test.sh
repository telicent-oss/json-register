#!/bin/bash
set -e

# Activates the virtual environment if it exists.
if [ -d ".venv" ]; then
    source .venv/bin/activate
fi

echo "Running Rust tests..."
# Executes Rust unit tests.
# Note: Excludes default features to prevent linking issues with the Python library during binary test execution.
cargo test --no-default-features

if [ -n "$DATABASE_URL" ] || [ -n "$TEST_DB_HOST" ]; then
    echo "Running Rust integration tests..."
    # Executes Rust integration tests if database configuration is present.
    # Note: Assumes the database is configured via TEST_DB_* environment variables or defaults (localhost:5432).
    cargo test --test integration_test --no-default-features -- --ignored
else
    echo "Skipping Rust integration tests (DATABASE_URL or TEST_DB_HOST not set)"
fi

echo "Building and installing Python extension..."
# Builds and installs the Python extension using maturin.
maturin develop

echo "Running Python tests..."
python -m pytest tests/test_python_*.py
