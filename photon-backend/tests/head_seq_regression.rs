//! Integration tests: `head_seq` tracks the highest assigned sequence per partition.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::sync::Arc;

use photon_backend::{InProcStoragePort, StoragePort, TransportCrypto};

const TOPIC: &str = "test.head_seq.regression";

fn mem_port() -> Arc<dyn StoragePort> {
    Arc::new(InProcStoragePort::new(TransportCrypto::from_bytes(
        *b"photon-dev-transport-key-32bytes",
    )))
}

#[tokio::test]
async fn head_seq_is_none_before_any_events_on_mem() {
    let port = mem_port();

    let head = port.head_seq(TOPIC, None).await.expect("read head seq");
    assert_eq!(head, None);
}

#[tokio::test]
async fn head_seq_tracks_appended_events_on_mem() {
    let port = mem_port();

    for _ in 0..3 {
        port.append(TOPIC, None, serde_json::json!({}), serde_json::json!({}))
            .await
            .expect("append event");
    }

    let head = port.head_seq(TOPIC, None).await.expect("read head seq");
    assert_eq!(head, Some(3));
}

#[tokio::test]
async fn head_seq_is_independent_per_partition_on_mem() {
    let port = mem_port();

    port.append(
        TOPIC,
        Some("key-a"),
        serde_json::json!({}),
        serde_json::json!({}),
    )
    .await
    .expect("append to key-a");
    for _ in 0..2 {
        port.append(
            TOPIC,
            Some("key-b"),
            serde_json::json!({}),
            serde_json::json!({}),
        )
        .await
        .expect("append to key-b");
    }

    let head_a = port
        .head_seq(TOPIC, Some("key-a"))
        .await
        .expect("read head seq for key-a");
    let head_b = port
        .head_seq(TOPIC, Some("key-b"))
        .await
        .expect("read head seq for key-b");
    assert_eq!(head_a, Some(1));
    assert_eq!(head_b, Some(2));
}
