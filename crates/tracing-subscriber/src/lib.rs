//! `tracing_subscriber::Layer` integration for LightTelemetry.
//!
//! `TelemetryLayer` forwards every `tracing::Event` to a
//! `light_telemetry_client::TelemetryClient` as a telemetry packet under a
//! configurable topic (default `logger`).

mod layer;

pub use layer::TelemetryLayer;
