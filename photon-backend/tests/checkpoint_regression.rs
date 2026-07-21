//! Integration test: regressive checkpoint commits on the in-process storage port.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::sync::Arc;

use photon_backend::{InProcStoragePort, StoragePort, TransportCrypto};

const TOPIC: &str = "test.checkpoint.regression";

#[tokio::test]
async fn regressive_commit_overwrites_stored_checkpoint_on_mem() {
    let port: Arc<dyn StoragePort> = Arc::new(InProcStoragePort::new(TransportCrypto::from_bytes(
        *b"photon-dev-transport-key-32bytes",
    )));

    for i in 0..10 {
        port.append(
            TOPIC,
            None,
            serde_json::json!({"n": i}),
            serde_json::json!({"n": i}),
        )
        .await
        .expect("append");
    }

    port.commit_checkpoint("sub-a", TOPIC, None, 10)
        .await
        .expect("commit high watermark");

    port.commit_checkpoint("sub-a", TOPIC, None, 5)
        .await
        .expect("mem adapter allows regressive commit");

    let loaded = port
        .load_checkpoint("sub-a", TOPIC, None)
        .await
        .expect("load checkpoint");
    assert_eq!(loaded, Some(5));
}
