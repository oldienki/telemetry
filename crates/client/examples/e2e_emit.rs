//! E2E emitter: `e2e_emit <addr> <topic> <id>` — sends one JSON event, exits.
//! Used by `tests/e2e/run_e2e.py`.

use light_telemetry_client::{TelemetryClient, TelemetryConfig};

fn main() {
    let args: Vec<String> = std::env::args().collect();
    if args.len() != 4 {
        eprintln!("usage: e2e_emit <addr> <topic> <id>");
        std::process::exit(2);
    }
    let addr = args[1].parse().expect("invalid addr");
    let cfg = TelemetryConfig::new(addr, &args[3], "e2e").with_heartbeat_timeout(None);
    let client = TelemetryClient::new(cfg).expect("client");
    client.emit(&args[2], serde_json::json!({"lang": "rust"}));
    println!("emitted");
}
