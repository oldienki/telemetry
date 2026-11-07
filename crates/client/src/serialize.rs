//! Wire packet type and encoding (JSON or MessagePack) with MTU truncation.

use serde::Serialize;
use serde_json::Value;
use std::time::{SystemTime, UNIX_EPOCH};

/// Maximum wire size before the payload is replaced by a truncation marker.
/// Sized to stay under a single IPv4 UDP datagram over typical MTUs.
pub const MTU_SAFE: usize = 1_432;

/// One telemetry packet on the wire — field names match `spec/schemas/`.
#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct TelemetryPacket {
    pub topic: String,
    pub id: String,
    pub class: String,
    /// Client-side timestamp, microseconds since UNIX epoch.
    pub ts: u64,
    pub payload: Value,
}

/// Current UNIX epoch time in microseconds.
pub fn now_micros() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_micros() as u64
}

/// Encode `packet` as JSON (`use_msgpack == false`) or MessagePack.
///
/// If the wire size exceeds [`MTU_SAFE`], the payload is replaced with
/// `{"truncated": true}` and the packet re-encoded. Returns `(bytes, was_truncated)`.
pub fn encode(packet: &TelemetryPacket, use_msgpack: bool) -> std::io::Result<(Vec<u8>, bool)> {
    let bytes = encode_raw(packet, use_msgpack)?;
    if bytes.len() <= MTU_SAFE {
        return Ok((bytes, false));
    }
    let mut truncated = packet.clone();
    truncated.payload = serde_json::json!({"truncated": true});
    let bytes = encode_raw(&truncated, use_msgpack)?;
    debug_assert!(bytes.len() <= MTU_SAFE);
    Ok((bytes, true))
}

fn encode_raw(packet: &TelemetryPacket, use_msgpack: bool) -> std::io::Result<Vec<u8>> {
    if use_msgpack {
        rmp_serde::to_vec_named(packet).map_err(std::io::Error::other)
    } else {
        serde_json::to_vec(packet).map_err(std::io::Error::other)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    const GOLDEN_JSON: &[u8] = include_bytes!("../../../spec/test-vectors/packet_sample.json");
    const GOLDEN_MSGPACK: &[u8] =
        include_bytes!("../../../spec/test-vectors/packet_sample.msgpack");

    fn golden_packet() -> TelemetryPacket {
        TelemetryPacket {
            topic: "db.query".into(),
            id: "worker-node-04".into(),
            class: "storage".into(),
            ts: 1_772_874_900_123_456,
            payload: json!({
                "query": "SELECT * FROM users WHERE id = $1",
                "duration_ms": 1.42,
                "status": "ok"
            }),
        }
    }

    #[test]
    fn json_encoding_matches_golden_vector() {
        let (bytes, truncated) = encode(&golden_packet(), false).unwrap();
        assert!(!truncated);
        let got: Value = serde_json::from_slice(&bytes).unwrap();
        let want: Value = serde_json::from_slice(GOLDEN_JSON).unwrap();
        assert_eq!(got, want);
    }

    #[test]
    fn msgpack_encoding_matches_golden_vector() {
        let (bytes, truncated) = encode(&golden_packet(), true).unwrap();
        assert!(!truncated);
        let got: Value = rmp_serde::from_slice(&bytes).unwrap();
        let want: Value = serde_json::from_slice(GOLDEN_JSON).unwrap();
        assert_eq!(got, want);
        // And the golden msgpack itself decodes to the same value.
        let golden_decoded: Value = rmp_serde::from_slice(GOLDEN_MSGPACK).unwrap();
        assert_eq!(got, golden_decoded);
    }

    #[test]
    fn oversized_payload_is_truncated() {
        let mut p = golden_packet();
        p.payload = json!({"big": "x".repeat(2000)});
        let (bytes, truncated) = encode(&p, false).unwrap();
        assert!(truncated);
        assert!(bytes.len() <= MTU_SAFE);
        let v: Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(v["payload"]["truncated"], true);
    }

    #[test]
    fn exact_mtu_boundary_not_truncated() {
        // Find a payload size that lands the encoding exactly on MTU_SAFE.
        let mut p = golden_packet();
        let mut exact = None;
        for n in 0..=MTU_SAFE {
            p.payload = json!({"pad": "x".repeat(n)});
            let (bytes, truncated) = encode(&p, false).unwrap();
            if bytes.len() == MTU_SAFE && !truncated {
                exact = Some(n);
                break;
            }
        }
        let n = exact.expect("an exact-fit payload size must exist");
        // One byte over → truncated.
        p.payload = json!({"pad": "x".repeat(n + 1)});
        let (bytes, truncated) = encode(&p, false).unwrap();
        assert!(truncated);
        assert!(bytes.len() <= MTU_SAFE);
    }

    #[test]
    fn unicode_payload_round_trips() {
        let mut p = golden_packet();
        p.payload = json!({"msg": "héllo  🚀"});
        for msgpack in [false, true] {
            let (bytes, _) = encode(&p, msgpack).unwrap();
            let got: Value = if msgpack {
                rmp_serde::from_slice(&bytes).unwrap()
            } else {
                serde_json::from_slice(&bytes).unwrap()
            };
            assert_eq!(got["payload"]["msg"], "héllo  🚀");
        }
    }

    #[test]
    fn msgpack_also_truncates() {
        let mut p = golden_packet();
        p.payload = json!({"big": "x".repeat(2000)});
        let (bytes, truncated) = encode(&p, true).unwrap();
        assert!(truncated);
        assert!(bytes.len() <= MTU_SAFE);
    }
}
