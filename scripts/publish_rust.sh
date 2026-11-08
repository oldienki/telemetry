#!/usr/bin/env bash
# Publish the Rust crates to crates.io, in dependency order.
# Requires: `cargo login` (or CARGO_REGISTRY_TOKEN).
# Usage: scripts/publish_rust.sh [--dry-run]
set -euo pipefail
cd "$(dirname "$0")/.."

DRY=""
if [[ "${1:-}" == "--dry-run" ]]; then
    DRY="--dry-run"
    echo "== dry run =="
fi

# Publish order matters: dependents go after their dependencies.
for crate in light-telemetry-client light-telemetry-tracing light-telemetry; do
    if [[ -n "$DRY" ]]; then
        # `publish --dry-run`/`package` rewrite path deps to registry deps, which
        # fails while workspace deps aren't on crates.io yet. Fall back to
        # listing the packaged files in that case.
        echo "== cargo package -p $crate --no-verify =="
        if ! cargo package -p "$crate" --no-verify --allow-dirty; then
            echo "-- packaging needs deps on crates.io; listing files instead --"
            cargo package -p "$crate" --list --allow-dirty
        fi
    else
        echo "== cargo publish -p $crate =="
        git-cargo-publish -p "$crate" --allow-dirty
    fi
done
echo "done"
