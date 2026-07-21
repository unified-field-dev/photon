//! `OpsLog` adapter installation for matrix telemetry dimension.

use std::io::Write;
use std::sync::{Arc, Mutex};

use photon_telemetry::{install_ops_log, ConsoleOpsLog, NoOpsLog, OpsLog, RecordingOpsLog};

use crate::matrix::TelemetryAdapter;

/// Install the process-wide ops log for the given telemetry adapter.
pub fn apply_telemetry(adapter: TelemetryAdapter) {
    let env = telemetry_env_value(adapter);
    std::env::set_var("PHOTON_TELEMETRY", env);
    install_ops_log(build_ops_log(adapter));
}

const fn telemetry_env_value(adapter: TelemetryAdapter) -> &'static str {
    match adapter {
        TelemetryAdapter::Off => "off",
        TelemetryAdapter::Console => "console",
        TelemetryAdapter::ExternalOpsLog => "external-ops-log",
        TelemetryAdapter::StructuredOpsLog => "structured-ops-log",
    }
}

fn build_ops_log(adapter: TelemetryAdapter) -> Arc<dyn OpsLog> {
    match adapter {
        TelemetryAdapter::Off => Arc::new(NoOpsLog),
        TelemetryAdapter::Console => Arc::new(ConsoleOpsLog),
        TelemetryAdapter::StructuredOpsLog => Arc::new(RecordingOpsLog::new()),
        TelemetryAdapter::ExternalOpsLog => {
            let file = tempfile::NamedTempFile::new().expect("external ops log temp file");
            Arc::new(PersistingOpsLog::new(file))
        }
    }
}

/// Wraps [`RecordingOpsLog`] and appends JSON lines to a temp file (external persist tax).
struct PersistingOpsLog {
    inner: RecordingOpsLog,
    _keepalive: tempfile::NamedTempFile,
    file: Mutex<std::fs::File>,
}

impl PersistingOpsLog {
    fn new(file: tempfile::NamedTempFile) -> Self {
        let std_file = file.reopen().expect("reopen external ops log");
        Self {
            inner: RecordingOpsLog::new(),
            _keepalive: file,
            file: Mutex::new(std_file),
        }
    }

    fn append_line(&self, kind: &str, name: &str, payload: &serde_json::Value) {
        let line = serde_json::json!({
            "kind": kind,
            "name": name,
            "payload": payload,
        });
        if let Ok(mut f) = self.file.lock() {
            let _ = writeln!(f, "{line}");
            let _ = f.sync_all();
        }
    }
}

impl OpsLog for PersistingOpsLog {
    fn record_counter(&self, name: &str, labels: &[(&str, &str)], value: f64) {
        self.inner.record_counter(name, labels, value);
        let payload = serde_json::json!({
            "labels": labels.iter().map(|(k, v)| (k, v)).collect::<Vec<_>>(),
            "value": value,
        });
        self.append_line("counter", name, &payload);
    }

    fn record_gauge(&self, name: &str, labels: &[(&str, &str)], value: f64) {
        self.inner.record_gauge(name, labels, value);
        let payload = serde_json::json!({
            "labels": labels.iter().map(|(k, v)| (k, v)).collect::<Vec<_>>(),
            "value": value,
        });
        self.append_line("gauge", name, &payload);
    }

    fn log_event(&self, name: &str, payload: &serde_json::Value) {
        self.inner.log_event(name, payload);
        self.append_line("event", name, payload);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn persisting_ops_log_appends_to_file() {
        let file = tempfile::NamedTempFile::new().expect("temp file");
        let path = file.path().to_path_buf();
        let log = PersistingOpsLog::new(file);
        log.record_counter("persist_test", &[], 1.0);
        let content = std::fs::read_to_string(&path).expect("read log");
        assert!(content.contains("persist_test"), "{content}");
    }

    #[test]
    #[serial_test::serial(photon_process_env)]
    fn telemetry_adapters() {
        let direct = RecordingOpsLog::new();
        direct.record_counter("direct", &[], 1.0);
        assert_eq!(direct.counters().len(), 1);

        apply_telemetry(TelemetryAdapter::Console);
        photon_telemetry::ops_log().record_counter("console_test", &[], 1.0);

        apply_telemetry(TelemetryAdapter::StructuredOpsLog);
        photon_telemetry::ops_log().record_counter("structured_test", &[], 1.0);
    }
}
