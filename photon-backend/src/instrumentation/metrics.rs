//! Spectra counter helpers for Photon self-telemetry.

use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::OnceLock;

use dashmap::DashMap;
use photon_telemetry::ops_log;

use super::labels::FailureReason;

static TOPIC_BACKLOG: OnceLock<DashMap<String, AtomicI64>> = OnceLock::new();

fn topic_backlog_map() -> &'static DashMap<String, AtomicI64> {
    TOPIC_BACKLOG.get_or_init(DashMap::new)
}

fn bump_backlog(topic: &str, delta: i64) {
    let gap = {
        let entry = topic_backlog_map()
            .entry(topic.to_string())
            .or_insert_with(|| AtomicI64::new(0));
        entry.fetch_add(delta, Ordering::Relaxed) + delta
    };
    #[allow(clippy::cast_precision_loss)]
    let gap_f64 = gap as f64;
    ops_log().record_gauge("photon_backlog", &[("topic", topic)], gap_f64);
}

/// Increment `photon_publishes` and topic backlog gauge.
pub fn record_publish(topic: &str, backend_label: &str) {
    ops_log().record_counter(
        "photon_publishes",
        &[("topic", topic), ("backend", backend_label)],
        1.0,
    );
    bump_backlog(topic, 1);
}

/// Increment `photon_publish_errors`.
pub fn record_publish_error(topic: &str, backend_label: &str) {
    ops_log().record_counter(
        "photon_publish_errors",
        &[("topic", topic), ("backend", backend_label)],
        1.0,
    );
}

/// Increment `photon_drains` and decrement topic backlog gauge.
pub fn record_drain(topic: &str, subscription: &str) {
    ops_log().record_counter(
        "photon_drains",
        &[("topic", topic), ("subscription", subscription)],
        1.0,
    );
    bump_backlog(topic, -1);
}

/// Increment `photon_handler_failures` for the given reason.
pub fn record_handler_failure(topic: &str, reason: FailureReason) {
    ops_log().record_counter(
        "photon_handler_failures",
        &[("topic", topic), ("reason", reason.as_str())],
        1.0,
    );
}

/// Increment `photon_retention_reclaims` by events removed.
pub fn record_retention_reclaim(topic: &str, removed: u64) {
    #[allow(clippy::cast_precision_loss)]
    let removed_f64 = removed as f64;
    ops_log().record_counter(
        "photon_retention_reclaims",
        &[("topic", topic)],
        removed_f64,
    );
}

/// Record `photon_retention_safe_seq` gauge for a partition.
pub fn record_retention_safe_seq(topic: &str, topic_key: Option<&str>, safe_seq: i64) {
    let key_label = topic_key.unwrap_or("_all");
    #[allow(clippy::cast_precision_loss)]
    let safe_seq_f64 = safe_seq as f64;
    ops_log().record_gauge(
        "photon_retention_safe_seq",
        &[("topic", topic), ("topic_key", key_label)],
        safe_seq_f64,
    );
}
