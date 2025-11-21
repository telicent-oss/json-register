#!/bin/bash
set -e

# Validates that a version argument is provided.
if [ -z "$1" ]; then
    echo "Usage: $0 <new_version>"
    exit 1
fi

NEW_VERSION=$1
CARGO_TOML="Cargo.toml"

# Verifies the existence of the Cargo.toml configuration file.
if [ ! -f "$CARGO_TOML" ]; then
    echo "Error: $CARGO_TOML not found"
    exit 1
fi

# Updates the version field in Cargo.toml.
# Note: macOS sed requires an empty string argument for the -i flag.
sed -i '' "s/^version = \".*\"/version = \"$NEW_VERSION\"/" "$CARGO_TOML"

echo "Updated version to $NEW_VERSION in $CARGO_TOML"
echo "Don't forget to commit and tag:"
echo "  git add $CARGO_TOML"
echo "  git commit -m \"Bump version to $NEW_VERSION\""
echo "  git tag -a v$NEW_VERSION -m \"Release v$NEW_VERSION\""
