//! Wire-format detection and decoding for telemetry datagrams.
//!
//! JSON datagrams start with `0x7B` (`{`). MessagePack map markers
//! (`0x80..=0x8F`, `0xDE`, `0xDF`) select the binary format.

use crate::event::TelemetryEvent;
use thiserror::Error;

/// Maximum accepted datagram size (UDP payload limit).
pub const MAX_DATAGRAM: usize = 65_507;

/// Errors produced while decoding a single datagram.
#[derive(Debug, Error)]
pub enum DecodeError {
    #[error("empty datagram")]
    Empty,
    #[error("oversized datagram: {0} bytes")]
    Oversized(usize),
    #[error("unknown wire format byte: 0x{0:02X}")]
    UnknownFormat(u8),
    #[error("json decode failed: {0}")]
    Json(#[from] serde_json::Error),
    #[error("msgpack decode failed: {0}")]
    MsgPack(#[from] rmp_serde::decode::Error),
}

/// Decode one datagram into a [`TelemetryEvent`] and stamp `ingress_ts`.
pub fn decode(datagram: &[u8], ingress_ts: u64) -> Result<TelemetryEvent, DecodeError> {
    if datagram.is_empty() {
        return Err(DecodeError::Empty);
    }
    if datagram.len() > MAX_DATAGRAM {
        return Err(DecodeError::Oversized(datagram.len()));
    }
    let mut event = match datagram[0] {
        b'{' => serde_json::from_slice::<TelemetryEvent>(datagram)?,
        0x80..=0x8F | 0xDE | 0xDF => rmp_serde::from_slice::<TelemetryEvent>(datagram)?,
        first => return Err(DecodeError::UnknownFormat(first)),
    };
    event.ingress_ts = ingress_ts;
    Ok(event)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::{json, Value};

    const GOLDEN_JSON: &[u8] = include_bytes!("../../../spec/test-vectors/packet_sample.json");
    const GOLDEN_MSGPACK: &[u8] =
        include_bytes!("../../../spec/test-vectors/packet_sample.msgpack");

    fn max_sized_json(fill_extra: usize) -> Vec<u8> {
        let prefix = r#"{"topic":"t","id":"i","class":"c","ts":1,"payload":{"big":""#;
        let suffix = r#""}}"#;
        let fill = MAX_DATAGRAM - prefix.len() - suffix.len() + fill_extra;
        let mut d = String::from(prefix);
        d.push_str(&"x".repeat(fill));
        d.push_str(suffix);
        d.into_bytes()
    }

    #[test]
    fn json_and_msgpack_decode_to_identical_events() {
        let from_json = decode(GOLDEN_JSON, 42).unwrap();
        let from_msgpack = decode(GOLDEN_MSGPACK, 42).unwrap();
        assert_eq!(from_json, from_msgpack);
        assert_eq!(from_json.topic, "db.query");
        assert_eq!(from_json.ingress_ts, 42);
    }

    #[test]
    fn missing_payload_defaults_to_null() {
        let raw = br#"{"topic":"t","id":"i","class":"c","ts":7}"#;
        let ev = decode(raw, 1).unwrap();
        assert_eq!(ev.payload, Value::Null);
    }

    #[test]
    fn datagram_at_max_size_decodes() {
        let d = max_sized_json(0);
        assert_eq!(d.len(), MAX_DATAGRAM);
        assert!(decode(&d, 0).is_ok());
    }

    #[test]
    fn oversized_datagram_rejected() {
        let d = max_sized_json(1);
        assert_eq!(d.len(), MAX_DATAGRAM + 1);
        assert!(matches!(
            decode(&d, 0),
            Err(DecodeError::Oversized(n)) if n == MAX_DATAGRAM + 1
        ));
    }

    #[test]
    fn empty_datagram_rejected() {
        assert!(matches!(decode(b"", 0), Err(DecodeError::Empty)));
    }

    #[test]
    fn unknown_first_byte_rejected() {
        assert!(matches!(
            decode(&[0x00, 0x01], 0),
            Err(DecodeError::UnknownFormat(0x00))
        ));
        assert!(matches!(
            decode(b"[1,2]", 0),
            Err(DecodeError::UnknownFormat(0x5B))
        ));
    }

    #[test]
    fn malformed_json_reports_error() {
        assert!(matches!(decode(b"{not json", 0), Err(DecodeError::Json(_))));
    }

    #[test]
    fn missing_required_field_reports_error() {
        let raw = br#"{"id":"i","class":"c","ts":7}"#;
        assert!(decode(raw, 0).is_err());
    }

    #[test]
    fn wire_ingress_ts_never_survives_decode() {
        let raw = json!({"topic":"t","id":"i","class":"c","ts":1,"ingress_ts":999});
        let ev = decode(raw.to_string().as_bytes(), 5).unwrap();
        assert_eq!(ev.ingress_ts, 5);
    }
}
