# syntax=docker/dockerfile:1

# Build the light-telemetry server binary from the workspace.
FROM rust:1-bookworm AS build
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY crates ./crates
RUN cargo build --release --locked -p light-telemetry \
    && cp target/release/light-telemetry /light-telemetry

# Minimal runtime image.
FROM debian:bookworm-slim
RUN useradd --system --uid 10001 --home /nonexistent --shell /usr/sbin/nologin light-telemetry
COPY --from=build /light-telemetry /usr/local/bin/light-telemetry
USER light-telemetry

# HTTP API/WebSocket and UDP ingest (defaults; override via CLI flags or telemetry.toml).
EXPOSE 8080/tcp
EXPOSE 9876/udp

ENTRYPOINT ["light-telemetry"]
