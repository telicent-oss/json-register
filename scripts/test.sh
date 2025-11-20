#!/bin/bash
set -e

# Activate venv if it exists
if [ -d ".venv" ]; then
    source .venv/bin/activate
fi

echo "Running Rust tests..."
# Run Rust tests (excluding Python features to avoid linking issues)
cargo test --no-default-features

if [ -n "$DATABASE_URL" ] || [ -n "$TEST_DB_HOST" ]; then
    echo "Running Rust integration tests..."
    # Note: This assumes the DB is configured according to TEST_DB_* env vars or defaults (localhost:5432)
    cargo test --test integration_test --no-default-features -- --ignored
else
    echo "Skipping Rust integration tests (DATABASE_URL or TEST_DB_HOST not set)"
fi

echo "Building and installing Python extension..."
# Ensure we are in a venv or have maturin installed
maturin develop

echo "Running Python tests..."
python -m pytest tests/test_python_*.py
