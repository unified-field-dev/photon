//! Photon platform observability via injected [`photon_telemetry::OpsLog`].
//!
//! L0 backend instrumentation lives in [`photon_backend::instrumentation`]; this module
//! re-exports shared helpers for host applications.

pub use photon_backend::instrumentation::{
    dlq_fields, log_ops, ops_log_fields, record_drain, record_handler_failure, record_publish,
    record_publish_error, wrap_backend, FailureReason, InstrumentedPhotonBackend, PhotonDlqRow,
};
