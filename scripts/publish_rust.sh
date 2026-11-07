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
    echo "== cargo publish -p $crate $DRY =="
    cargo publish -p "$crate" $DRY --allow-dirty
done
echo "done"
