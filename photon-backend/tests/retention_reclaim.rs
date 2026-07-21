//! Integration tests for in-process retention / reclaim.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::sync::Arc;

use photon_backend::{
    ExecutorServices, InProcStoragePort, RetentionPolicy, StoragePort, SubscriptionPartition,
    TransportCrypto,
};
use serial_test::serial;

const TOPIC: &str = "test.retention";

#[tokio::test]
#[serial]
async fn checkpoint_then_sweep_truncates_replay_buffer() {
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

    port.commit_checkpoint("sub-a", TOPIC, None, 15)
        .await
        .expect("checkpoint");

    let services = ExecutorServices::new(
        Arc::clone(&port),
        RetentionPolicy {
            sweep_interval_ms: 0,
            extra_subscriptions: vec![SubscriptionPartition {
                subscription_name: "sub-a".into(),
                topic_name: TOPIC.into(),
                topic_key: None,
            }],
            ..RetentionPolicy::default()
        },
        None,
    );

    let report = services
        .retention_reclaimer
        .sweep_partition(TOPIC, None)
        .await
        .expect("sweep");

    assert!(report.truncate_bound > 0);
}
