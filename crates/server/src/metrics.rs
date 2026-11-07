//! Lightweight internal counters. Exposed via API in a later task.

use std::sync::atomic::{AtomicU64, Ordering};

/// Datagrams dropped because the decode channel was full.
static DROPPED_PACKETS: AtomicU64 = AtomicU64::new(0);

/// Datagrams rejected by the decoder.
static DECODE_ERRORS: AtomicU64 = AtomicU64::new(0);

/// Increment the dropped-packet counter (Drop-Latest path).
pub fn inc_dropped() {
    DROPPED_PACKETS.fetch_add(1, Ordering::Relaxed);
}

/// Increment the decode-error counter.
pub fn inc_decode_error() {
    DECODE_ERRORS.fetch_add(1, Ordering::Relaxed);
}

/// Current dropped-packet total.
pub fn dropped_packets_total() -> u64 {
    DROPPED_PACKETS.load(Ordering::Relaxed)
}

/// Current decode-error total.
pub fn decode_errors_total() -> u64 {
    DECODE_ERRORS.load(Ordering::Relaxed)
}
