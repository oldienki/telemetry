install-deps:
    cargo check --workspace
    cd python && uv sync
    cd go/light-telemetry && go mod download

test-units:
    cargo test --workspace
    cd go/light-telemetry && go test -v ./...
    cd python && uv run pytest

test-e2e:
    cargo build -p light-telemetry-server
    cd python && uv run python ../tests/e2e/run_e2e.py

fmt:
    cargo fmt --all
    cd go/light-telemetry && gofmt -l -w .
    cd python && uv run ruff format .

lint:
    cargo clippy --workspace --all-targets -- -D warnings
    cd go/light-telemetry && go vet ./...
    cd python && uv run ruff check . && uv run mypy light_telemetry
