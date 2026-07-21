//! Broker / fleet store availability for benchmark skip logic.

use crate::matrix::{MatrixSpec, StorageAdapter, Topology};

/// Whether the matrix row can run without external infrastructure.
#[must_use]
pub const fn lab_store_available(matrix: &MatrixSpec) -> bool {
    matches!(matrix.storage, StorageAdapter::Mem | StorageAdapter::Sqlite)
}

/// Skip reason when lab store is unavailable.
#[must_use]
pub const fn lab_store_skip_reason(matrix: &MatrixSpec) -> Option<&'static str> {
    if lab_store_available(matrix) {
        None
    } else {
        Some("lab store requires mem or sqlite storage")
    }
}

/// Whether fleet-tier benchmarks can run (broker cluster + env).
#[must_use]
pub fn fleet_store_available(matrix: &MatrixSpec) -> bool {
    match matrix.storage {
        StorageAdapter::Mem | StorageAdapter::Sqlite => false,
        StorageAdapter::Nats => nats_env_set(),
        StorageAdapter::Fluvio => fluvio_env_set(),
        StorageAdapter::Kafka => kafka_env_set(),
    }
}

fn nats_env_set() -> bool {
    std::env::var("PHOTON_NATS_URL").is_ok()
}

fn fluvio_env_set() -> bool {
    std::env::var("PHOTON_FLUVIO_ENDPOINT").is_ok()
}

fn kafka_env_set() -> bool {
    std::env::var("PHOTON_KAFKA_BROKERS").is_ok()
}

/// Pause after opening a live-tail subscribe so broker consumers attach before publish.
///
/// Fluvio SPU consumer registration is especially slow in CI; under-delivery
/// (e.g. 99/100, 1/2) is almost always a missed first event from a too-short settle.
#[must_use]
pub fn subscribe_attach_warmup() -> std::time::Duration {
    use std::time::Duration;
    if fluvio_env_set() || nats_env_set() || kafka_env_set() {
        Duration::from_secs(3)
    } else {
        Duration::from_millis(50)
    }
}

/// Skip reason for fleet-tier benchmarks (BM-PF*).
#[must_use]
pub fn fleet_store_skip_reason(matrix: &MatrixSpec) -> Option<String> {
    if matrix.topology != Topology::BrokerCluster && !is_broker_storage(matrix.storage) {
        return Some("fleet benchmarks require broker storage (nats|fluvio|kafka)".into());
    }
    match matrix.storage {
        StorageAdapter::Mem | StorageAdapter::Sqlite => {
            Some("fleet benchmarks require broker storage, not mem/sqlite".into())
        }
        StorageAdapter::Nats if !nats_env_set() => {
            Some("PHOTON_NATS_URL not set for nats fleet benchmarks".into())
        }
        StorageAdapter::Fluvio if !fluvio_env_set() => {
            Some("PHOTON_FLUVIO_ENDPOINT not set for fluvio fleet benchmarks".into())
        }
        StorageAdapter::Kafka if !kafka_env_set() => {
            Some("PHOTON_KAFKA_BROKERS not set for kafka fleet benchmarks".into())
        }
        StorageAdapter::Nats | StorageAdapter::Fluvio | StorageAdapter::Kafka => None,
    }
}

const fn is_broker_storage(storage: StorageAdapter) -> bool {
    matches!(
        storage,
        StorageAdapter::Nats | StorageAdapter::Fluvio | StorageAdapter::Kafka
    )
}

/// Alias — shared store == broker env for fleet rows.
#[must_use]
pub fn shared_store_available(matrix: &MatrixSpec) -> bool {
    fleet_store_available(matrix)
}

/// Alias skip reason.
#[must_use]
pub fn shared_store_skip_reason(matrix: &MatrixSpec) -> Option<String> {
    fleet_store_skip_reason(matrix)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::matrix::MatrixSpec;

    #[test]
    fn mem_is_lab_only() {
        assert!(lab_store_available(&MatrixSpec::default()));
        assert!(!fleet_store_available(&MatrixSpec::default()));
    }
}
