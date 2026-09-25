//! Spectra event field builders for Photon self-telemetry.

use chrono::{DateTime, Utc};
use serde::Serialize;
use serde_json::{json, Value};

use crate::sanitize::sanitize_error_message;

use super::labels::FailureReason;

/// Sanitize and truncate an error message for ops-log / DLQ fields.
#[must_use]
pub fn truncate_error(message: &str) -> String {
    sanitize_error_message(message)
}

/// Build JSON fields for a `photon_dlq` ops-log event.
#[must_use]
pub fn dlq_fields(
    event_id: &str,
    topic: &str,
    topic_key: Option<&str>,
    seq: i64,
    subscription: Option<&str>,
    reason: FailureReason,
    error: &str,
) -> Value {
    json!({
        "event_id": event_id,
        "topic": topic,
        "topic_key": topic_key.unwrap_or(""),
        "seq": seq,
        "subscription": subscription.unwrap_or(""),
        "reason": reason.as_str(),
        "error": truncate_error(error),
    })
}

/// Row shape for DLQ / failure telemetry append (includes `ts`).
#[derive(Debug, Serialize)]
pub struct PhotonDlqRow {
    /// Failed event id.
    pub event_id: String,
    /// Topic name.
    pub topic: String,
    /// Partition key, or empty string when none.
    pub topic_key: String,
    /// Event sequence number.
    pub seq: i64,
    /// Subscription name, or empty string when none.
    pub subscription: String,
    /// Failure reason label.
    pub reason: String,
    /// Truncated error message.
    pub error: String,
    /// Row timestamp.
    pub ts: DateTime<Utc>,
}

impl PhotonDlqRow {
    /// Build a DLQ row from delivery failure parts.
    #[must_use]
    pub fn from_parts(
        event_id: &str,
        topic: &str,
        topic_key: Option<&str>,
        seq: i64,
        subscription: Option<&str>,
        reason: FailureReason,
        error: &str,
    ) -> Self {
        Self {
            event_id: event_id.to_string(),
            topic: topic.to_string(),
            topic_key: topic_key.unwrap_or("").to_string(),
            seq,
            subscription: subscription.unwrap_or("").to_string(),
            reason: reason.as_str().to_string(),
            error: truncate_error(error),
            ts: Utc::now(),
        }
    }
}

/// Build JSON for `photon_ops_log`.
#[must_use]
pub fn ops_log_fields(
    component: &str,
    operation: &str,
    message: &str,
    topic: &str,
    subscription: &str,
    error: &str,
) -> Value {
    json!({
        "component": component,
        "operation": operation,
        "message": message,
        "topic": topic,
        "subscription": subscription,
        "error": truncate_error(error),
    })
}

use photon_telemetry::ops_log;

/// Emit a `photon_ops_log` Spectra event row via the installed [`photon_telemetry::OpsLog`].
pub fn log_ops(
    component: &str,
    operation: &str,
    message: &str,
    topic: &str,
    subscription: &str,
    error: &str,
) {
    ops_log().log_event(
        "photon_ops_log",
        &ops_log_fields(component, operation, message, topic, subscription, error),
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::instrumentation::FailureReason;

    #[test]
    fn dlq_fields_sanitizes_error_happy_path() {
        let fields = dlq_fields(
            "e1",
            "t",
            None,
            1,
            Some("sub"),
            FailureReason::HandlerError,
            "failed password=hunter2",
        );
        let err = fields["error"].as_str().expect("error field");
        assert!(err.contains("[redacted]"));
        assert!(!err.contains("hunter2"));
        assert!(fields.get("actor_json").is_none());
        assert!(fields.get("payload_json").is_none());
    }

    #[test]
    fn truncate_error_caps_length_sad_path() {
        let long = "y".repeat(800);
        let out = truncate_error(&long);
        assert!(out.ends_with('…'));
        assert!(out.chars().count() <= crate::MAX_ERROR_MESSAGE_CHARS + 1);
    }
}
