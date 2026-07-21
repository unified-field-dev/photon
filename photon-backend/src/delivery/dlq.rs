//! Dead-letter metadata (no payload) for failed handler delivery.

use chrono::Utc;

use crate::error::Result;
use crate::instrumentation::{dlq_fields, record_handler_failure, FailureReason};
use photon_telemetry::ops_log;

/// Metadata-only DLQ record shape.
#[derive(Debug, Clone)]
pub struct DlqRecord {
    /// Failed event id.
    pub event_id: String,
    /// Topic the event belonged to.
    pub topic_name: String,
    /// Optional partition key.
    pub topic_key: Option<String>,
    /// Event sequence number.
    pub seq: i64,
    /// Durable subscription name when known.
    pub subscription_name: Option<String>,
    /// Truncated error message.
    pub error: String,
    /// Delivery attempt count at failure.
    pub attempt: u32,
    /// When the DLQ row was recorded.
    pub recorded_at: chrono::DateTime<Utc>,
}

/// Parameters for [`DlqSink::record`].
pub struct DlqRecordParams<'a> {
    /// Failed event id.
    pub event_id: &'a str,
    /// Topic the event belonged to.
    pub topic_name: &'a str,
    /// Optional partition key.
    pub topic_key: Option<&'a str>,
    /// Event sequence number.
    pub seq: i64,
    /// Durable subscription name when known.
    pub subscription_name: Option<&'a str>,
    /// Failure classification for metrics and ops log.
    pub reason: FailureReason,
    /// Error message (truncated on record).
    pub error: String,
}

/// In-memory DLQ sink until a persistent schema is wired by the host.
#[derive(Default)]
pub struct DlqSink {
    records: std::sync::Mutex<Vec<DlqRecord>>,
}

impl DlqSink {
    /// Create an empty in-memory DLQ sink.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Record a failed delivery and emit DLQ telemetry.
    ///
    /// # Errors
    ///
    /// Returns an error if the operation fails.
    pub fn record(&self, params: &DlqRecordParams<'_>) -> Result<()> {
        record_handler_failure(params.topic_name, params.reason);
        tracing::warn!(
            event_id = params.event_id,
            topic = params.topic_name,
            topic_key = ?params.topic_key,
            seq = params.seq,
            subscription = ?params.subscription_name,
            reason = ?params.reason,
            error = %params.error,
            "handler delivery failed; recorded to DLQ"
        );
        {
            let mut guard = self
                .records
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            guard.push(DlqRecord {
                event_id: params.event_id.to_string(),
                topic_name: params.topic_name.to_string(),
                topic_key: params.topic_key.map(String::from),
                seq: params.seq,
                subscription_name: params.subscription_name.map(String::from),
                error: params.error.clone(),
                attempt: 1,
                recorded_at: Utc::now(),
            });
        }
        ops_log().log_event(
            "photon_dlq",
            &dlq_fields(
                params.event_id,
                params.topic_name,
                params.topic_key,
                params.seq,
                params.subscription_name,
                params.reason,
                &params.error,
            ),
        );
        Ok(())
    }

    /// Number of recorded DLQ rows.
    pub fn len(&self) -> usize {
        self.records.lock().map_or(0, |g| g.len())
    }

    /// Whether no DLQ rows have been recorded.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Minimum seq among DLQ records for a transport partition (retention pin).
    pub fn min_seq_for(&self, topic: &str, topic_key: Option<&str>) -> Option<i64> {
        self.records.lock().ok().and_then(|guard| {
            guard
                .iter()
                .filter(|r| r.topic_name == topic && r.topic_key.as_deref() == topic_key)
                .map(|r| r.seq)
                .min()
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn params(seq: i64, topic_key: Option<&'static str>) -> DlqRecordParams<'static> {
        DlqRecordParams {
            event_id: "evt-1",
            topic_name: "orders.created",
            topic_key,
            seq,
            subscription_name: Some("worker-a"),
            reason: FailureReason::HandlerError,
            error: "boom".to_string(),
        }
    }

    #[test]
    fn record_appends_and_reports_len() {
        let sink = DlqSink::new();
        assert!(sink.is_empty());

        sink.record(&params(7, None)).expect("record");
        assert_eq!(sink.len(), 1);
        assert!(!sink.is_empty());
    }

    #[test]
    fn min_seq_pins_lowest_matching_partition() {
        let sink = DlqSink::new();
        sink.record(&params(9, Some("alice"))).expect("record");
        sink.record(&params(4, Some("alice"))).expect("record");
        sink.record(&params(2, Some("bob"))).expect("record");
        sink.record(&params(1, None)).expect("record");

        assert_eq!(sink.min_seq_for("orders.created", Some("alice")), Some(4));
        assert_eq!(sink.min_seq_for("orders.created", Some("bob")), Some(2));
        assert_eq!(sink.min_seq_for("orders.created", None), Some(1));
    }

    #[test]
    fn min_seq_returns_none_when_no_partition_matches() {
        let sink = DlqSink::new();
        sink.record(&params(5, Some("alice"))).expect("record");

        assert_eq!(sink.min_seq_for("orders.created", Some("carol")), None);
        assert_eq!(sink.min_seq_for("other.topic", Some("alice")), None);
        assert_eq!(sink.min_seq_for("orders.created", None), None);
    }
}
