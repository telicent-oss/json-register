#!/bin/bash
set -e

# Activates the virtual environment if it exists.
if [ -d ".venv" ]; then
    source .venv/bin/activate
fi

echo "Running Rust formatting check..."
cargo fmt --all -- --check

echo "Running Rust clippy..."
cargo clippy --all-targets --no-default-features -- -D warnings

echo "Running Python linting (ruff)..."
ruff check .

echo "Running Python type checking (mypy)..."
mypy tests

echo "Linting complete!"
