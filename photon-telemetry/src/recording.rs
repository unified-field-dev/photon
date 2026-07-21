//! In-memory [`OpsLog`] for tests (`feature = "recording"`).

use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use serde_json::Value;

use super::OpsLog;

/// Captured counter increment.
#[derive(Debug, Clone, PartialEq)]
pub struct RecordedCounter {
    /// Metric name.
    pub name: String,
    /// Label key/value pairs.
    pub labels: Vec<(String, String)>,
    /// Increment amount.
    pub value: f64,
}

/// Captured gauge sample.
#[derive(Debug, Clone, PartialEq)]
pub struct RecordedGauge {
    /// Metric name.
    pub name: String,
    /// Label key/value pairs.
    pub labels: Vec<(String, String)>,
    /// Gauge value.
    pub value: f64,
}

/// Captured structured event.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecordedEvent {
    /// Event name.
    pub name: String,
    /// Event payload.
    pub payload: Value,
}

#[derive(Debug, Default)]
struct Inner {
    counters: Vec<RecordedCounter>,
    gauges: Vec<RecordedGauge>,
    events: Vec<RecordedEvent>,
}

/// Append-only in-memory ops log for assertions in unit/integration tests.
#[derive(Debug, Clone)]
pub struct RecordingOpsLog {
    inner: Arc<Mutex<Inner>>,
}

fn lock_inner(inner: &Mutex<Inner>) -> MutexGuard<'_, Inner> {
    inner.lock().unwrap_or_else(PoisonError::into_inner)
}

impl RecordingOpsLog {
    /// Create an empty recording log.
    #[must_use]
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(Inner::default())),
        }
    }

    /// Drop all recorded counters, gauges, and events.
    pub fn clear(&self) {
        let mut g = lock_inner(&self.inner);
        g.counters.clear();
        g.gauges.clear();
        g.events.clear();
    }

    /// Snapshot of recorded counters.
    #[must_use]
    pub fn counters(&self) -> Vec<RecordedCounter> {
        lock_inner(&self.inner).counters.clone()
    }

    /// Snapshot of recorded gauges.
    #[must_use]
    pub fn gauges(&self) -> Vec<RecordedGauge> {
        lock_inner(&self.inner).gauges.clone()
    }

    /// Snapshot of recorded events.
    #[must_use]
    pub fn events(&self) -> Vec<RecordedEvent> {
        lock_inner(&self.inner).events.clone()
    }

    /// Counters whose name matches and labels contain `label_subset`.
    #[must_use]
    pub fn recorded_counters_matching(
        &self,
        name: &str,
        label_subset: &[(&str, &str)],
    ) -> Vec<RecordedCounter> {
        self.counters()
            .into_iter()
            .filter(|c| c.name == name && labels_contain(&c.labels, label_subset))
            .collect()
    }

    /// Events whose name equals `event_name`.
    #[must_use]
    pub fn recorded_events_for(&self, event_name: &str) -> Vec<RecordedEvent> {
        self.events()
            .into_iter()
            .filter(|e| e.name == event_name)
            .collect()
    }
}

fn labels_contain(labels: &[(String, String)], subset: &[(&str, &str)]) -> bool {
    subset.iter().all(|(k, v)| {
        labels
            .iter()
            .any(|(lk, lv)| lk.as_str() == *k && lv.as_str() == *v)
    })
}

impl OpsLog for RecordingOpsLog {
    fn record_counter(&self, name: &str, labels: &[(&str, &str)], value: f64) {
        let labels: Vec<(String, String)> = labels
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        lock_inner(&self.inner).counters.push(RecordedCounter {
            name: name.to_string(),
            labels,
            value,
        });
    }

    fn record_gauge(&self, name: &str, labels: &[(&str, &str)], value: f64) {
        let labels: Vec<(String, String)> = labels
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        lock_inner(&self.inner).gauges.push(RecordedGauge {
            name: name.to_string(),
            labels,
            value,
        });
    }

    fn log_event(&self, name: &str, payload: &Value) {
        lock_inner(&self.inner).events.push(RecordedEvent {
            name: name.to_string(),
            payload: payload.clone(),
        });
    }
}

impl Default for RecordingOpsLog {
    fn default() -> Self {
        Self::new()
    }
}
