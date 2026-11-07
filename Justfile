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
    cargo test -p light-telemetry-server --all-features

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
    cargo run -p light-telemetry-server -- --storage memory

# Run the server against SQLite.
run-sqlite db="sqlite:telemetry.db":
    cargo run -p light-telemetry-server -- --storage sqlite --db-url {{db}}
