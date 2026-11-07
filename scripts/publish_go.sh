#!/usr/bin/env bash
# "Publish" the Go module: tag + push. Go has no registry upload —
# proxy.golang.org picks up pushed semver tags automatically.
# Tag must be go/light-telemetry/vX.Y.Z (subdirectory-module convention).
# Usage: scripts/publish_go.sh [push]
set -euo pipefail
cd "$(dirname "$0")/.."

MOD_DIR="go/light-telemetry"
VERSION=$(grep -m1 'version = ' Cargo.toml | cut -d'"' -f2)
TAG="$MOD_DIR/v$VERSION"

echo "== go module: $(grep '^module' "$MOD_DIR/go.mod") =="
echo "== version:   v$VERSION =="

if git rev-parse -q --verify "refs/tags/$TAG" >/dev/null; then
    echo "tag $TAG already exists"
else
    git tag -a "$TAG" -m "$TAG"
    echo "created tag $TAG"
fi

if [[ "${1:-}" == "push" ]]; then
    echo "== git push origin $TAG =="
    git push origin "$TAG"
    echo "== resolving via proxy =="
    (cd "$MOD_DIR" && go list -m "$(grep '^module' go.mod | awk '{print $2}')@v$VERSION")
    echo "done — https://pkg.go.dev/$(grep '^module' "$MOD_DIR/go.mod" | awk '{print $2}')@v$VERSION"
else
    echo "tag is local only — run: git push origin $TAG   (or: $0 push)"
fi
