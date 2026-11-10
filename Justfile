# LightTelemetry monorepo recipes. Run `just --list` for an overview.

# Dev environment bootstrap.
install-deps:
    cargo check --workspace
    cd python && uv sync
    cd go/light-telemetry && go mod download

# Everything: fmt check, all unit tests, lints.
all: fmt-check test lint

# Format all code.
fmt:
    cargo fmt --all
    cd python && uv run ruff format .
    cd go/light-telemetry && gofmt -w .

# Check formatting without writing.
fmt-check:
    cargo fmt --all -- --check
    cd python && uv run ruff format --check .
    cd go/light-telemetry && test -z "$(gofmt -l .)"

# All unit tests across languages.
test: test-server test-clients

# All unit tests (alias of test-units for the original recipe).
test-units: test

# Server crate tests (memory/sqlite + env-gated pg).
test-server:
    cargo test -p light-telemetry --all-features

# All client tests (Rust, Python, Go).
test-clients:
    cargo test -p light-telemetry-client -p light-telemetry-tracing
    cd python && uv run pytest
    cd go/light-telemetry && go test ./...

# Lint everything.
lint:
    cargo clippy --workspace --all-features --all-targets -- -D warnings
    cd python && uv run ruff check . && uv run mypy light_telemetry
    cd go/light-telemetry && go vet ./...

# End-to-end: build server, emit from all clients, assert via /wait.
test-e2e *ARGS:
    python3 tests/e2e/run_e2e.py {{ARGS}}

# Run the server with defaults (memory engine).
run:
    cargo run -p light-telemetry -- --storage memory

# Run the server against SQLite.
run-sqlite db="sqlite:telemetry.db":
    cargo run -p light-telemetry -- --storage sqlite --db-url {{db}}

# Bump the unified version (patch|minor|major|X.Y.Z) across all manifests.
version-bump part="patch" *ARGS:
    python3 scripts/version_bump.py {{part}} {{ARGS}}

# Publish Rust crates to crates.io (dependency order).
publish-rust *ARGS:
    bash scripts/publish_rust.sh {{ARGS}}

# Build and publish the Python package to PyPI.
publish-python *ARGS:
    bash scripts/publish_python.sh {{ARGS}}

# Tag (and optionally push) the Go module — Go versions via git tags.
publish-go *ARGS:
    bash scripts/publish_go.sh {{ARGS}}

# Docker image name (override: IMAGE=foo/bar just docker-build).
IMAGE := env_var_or_default("IMAGE", "oldienki/light-telemetry")

# Package version from the workspace manifest.
VERSION := `grep -m1 '^version' Cargo.toml | cut -d'"' -f2`

# Build the light-telemetry server image locally.
docker-build *ARGS:
    docker build -t {{IMAGE}}:latest {{ARGS}} .

# Build and push the image to Docker Hub (tags: latest + package version).
docker-publish-fn *ARGS:
    #!/usr/bin/env bash
    set -euo pipefail
    just docker-build {{ARGS}}
    docker tag {{IMAGE}}:latest {{IMAGE}}:{{VERSION}}
    git-docker-publish {{IMAGE}}:latest
    git-docker-publish {{IMAGE}}:{{VERSION}}

# Publish to GHCR.
publish-ghcr:
    IMAGE=ghcr.io/oldienki/light-telemetry just docker-publish-fn

# Publish to Docker Hub.
publish-docker:
    IMAGE=docker.io/oldienki/light-telemetry just docker-publish-fn

