#![allow(clippy::print_stderr)] // ConsoleOpsLog is the explicit stderr adapter.

use super::OpsLog;

/// stderr/stdout structured lines (default dev adapter).
#[derive(Debug, Default, Clone, Copy)]
pub struct ConsoleOpsLog;

impl OpsLog for ConsoleOpsLog {
    fn record_counter(&self, name: &str, labels: &[(&str, &str)], value: f64) {
        eprintln!("[photon-telemetry] counter {name}={value} {labels:?}");
    }

    fn record_gauge(&self, name: &str, labels: &[(&str, &str)], value: f64) {
        eprintln!("[photon-telemetry] gauge {name}={value} {labels:?}");
    }

    fn log_event(&self, name: &str, payload: &serde_json::Value) {
        eprintln!("[photon-telemetry] event {name} {payload}");
    }
}
