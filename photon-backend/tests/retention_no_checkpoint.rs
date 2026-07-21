//! Integration test: retention sweep is a no-op when no checkpoint exists.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::sync::Arc;

use photon_backend::{
    ExecutorServices, InProcStoragePort, RetentionPolicy, StoragePort, TransportCrypto,
};

const TOPIC: &str = "test.retention.no_checkpoint";

#[tokio::test]
async fn sweep_without_checkpoint_does_not_truncate() {
    std::env::set_var("PHOTON_RETENTION_SWEEP_MS", "0");
    std::env::set_var("PHOTON_TRANSPORT_RETAIN_SEQ", "5");

    let port: Arc<dyn StoragePort> = Arc::new(InProcStoragePort::new(TransportCrypto::from_bytes(
        *b"photon-dev-transport-key-32bytes",
    )));

    for i in 0..20 {
        port.append(
            TOPIC,
            None,
            serde_json::json!({"n": i}),
            serde_json::json!({"n": i}),
        )
        .await
        .expect("append");
    }

    let services = ExecutorServices::new(
        Arc::clone(&port),
        RetentionPolicy {
            sweep_interval_ms: 0,
            ..RetentionPolicy::default()
        },
        None,
    );

    let report = services
        .retention_reclaimer
        .sweep_partition(TOPIC, None)
        .await
        .expect("sweep");

    assert_eq!(report.safe_seq, None);
    assert_eq!(report.truncate_bound, 0);
    assert_eq!(report.removed, 0);
}
