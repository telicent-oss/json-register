#!/bin/bash
set -e

function show_help {
    echo "Usage: $0 [options]"
    echo "Options:"
    echo "  --pypi          Publish to PyPI"
    echo "  --test-pypi     Publish to TestPyPI"
    echo "  --crates        Publish to Crates.io"
    echo "  --all           Publish to both PyPI and Crates.io"
    echo "  --dry-run       Build and verify but do not upload"
    echo "  --help          Show this help message"
}

PUBLISH_PYPI=false
PUBLISH_TEST_PYPI=false
PUBLISH_CRATES=false
DRY_RUN=false

if [ $# -eq 0 ]; then
    show_help
    exit 1
fi

while [[ "$#" -gt 0 ]]; do
    case $1 in
        --pypi) PUBLISH_PYPI=true ;;
        --test-pypi) PUBLISH_TEST_PYPI=true ;;
        --crates) PUBLISH_CRATES=true ;;
        --all) PUBLISH_PYPI=true; PUBLISH_CRATES=true ;;
        --dry-run) DRY_RUN=true ;;
        --help) show_help; exit 0 ;;
        *) echo "Unknown parameter passed: $1"; exit 1 ;;
    esac
    shift
done

echo "Starting publication process..."

# Ensure we are in the project root
if [ ! -f "Cargo.toml" ]; then
    echo "Error: Cargo.toml not found. Please run this script from the project root."
    exit 1
fi

# 1. Run Tests
echo "Running tests..."
cargo test --all-features

# 2. Publish to Crates.io
if [ "$PUBLISH_CRATES" = true ]; then
    echo "Publishing to Crates.io..."
    if [ "$DRY_RUN" = true ]; then
        cargo publish --dry-run
    else
        cargo publish
    fi
fi

# 3. Publish to PyPI
if [ "$PUBLISH_PYPI" = true ] || [ "$PUBLISH_TEST_PYPI" = true ]; then
    echo "Building and Publishing to PyPI..."
    
    MATURIN_ARGS=""
    if [ "$PUBLISH_TEST_PYPI" = true ]; then
        MATURIN_ARGS="--repository testpypi"
    fi

    if [ "$DRY_RUN" = true ]; then
        # Just build
        maturin build --release
        echo "Dry run: skipping upload"
    else
        # Build and upload
        # Note: This requires MATURIN_PYPI_TOKEN or login
        maturin publish $MATURIN_ARGS
    fi
fi

echo "Done."
