#!/usr/bin/env bash
# Build and publish the Python package to PyPI.
# Requires: uv; set UV_PUBLISH_TOKEN (or a .pypirc / trusted publisher).
# Usage: scripts/publish_python.sh [--check-only]
set -euo pipefail
cd "$(dirname "$0")/../python"

echo "== uv build =="
uv build --clear

if [[ "${1:-}" == "--check-only" ]]; then
    echo "== check only: dist/ contents =="
    ls -lh dist/
    exit 0
fi

echo "== uv publish =="
git-uv-publish
echo "done"
