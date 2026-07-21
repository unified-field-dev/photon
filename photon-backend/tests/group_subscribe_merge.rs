//! Integration tests: consumer-group shard stream merging over the mem port.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use futures::StreamExt;
use photon_backend::{
    merge_shard_streams, shard_id, BackendContext, GenericPhotonBackend, InProcStoragePort,
    PhotonBackend, TopicDescriptor, TopicRegistry, TransportCrypto,
};

const TOPIC: &str = "test.group.merge";
const SHARDS: u32 = 4;

static GROUP_TOPIC: TopicDescriptor = TopicDescriptor::group(TOPIC, SHARDS, Some("k"), "{}");

fn mem_backend() -> Arc<dyn PhotonBackend> {
    let mut registry = TopicRegistry::new();
    registry.register(&GROUP_TOPIC);
    let port = Arc::new(InProcStoragePort::new(TransportCrypto::from_bytes(
        *b"photon-dev-transport-key-32bytes",
    )));
    GenericPhotonBackend::install_with_port(BackendContext { registry }, port)
        .expect("install mem backend")
}

async fn publish_keyed(backend: &Arc<dyn PhotonBackend>, key: &str, n: i64) {
    backend
        .publish(
            TOPIC,
            None,
            serde_json::json!({}),
            serde_json::json!({ "k": key, "n": n }),
        )
        .await
        .expect("publish");
}

async fn next_event(
    stream: &mut std::pin::Pin<
        Box<dyn futures::Stream<Item = photon_backend::Result<photon_backend::Event>> + Send>,
    >,
) -> photon_backend::Event {
    tokio::time::timeout(Duration::from_secs(5), stream.next())
        .await
        .expect("merged stream should deliver within timeout")
        .expect("stream should stay open")
        .expect("event should be Ok")
}

#[tokio::test]
async fn empty_shard_list_ends_immediately() {
    let backend = mem_backend();
    let mut stream = merge_shard_streams(backend, TOPIC.to_string(), &[], HashMap::new());
    assert!(stream.next().await.is_none());
}

#[tokio::test]
async fn merged_stream_delivers_events_from_all_assigned_shards() {
    let backend = mem_backend();

    let keys = ["alice", "bob", "carol", "dave", "erin"];
    for key in keys {
        publish_keyed(&backend, key, 0).await;
    }

    let shard_ids: Vec<u32> = (0..SHARDS).collect();
    let after: HashMap<u32, Option<i64>> = shard_ids.iter().map(|id| (*id, Some(0))).collect();
    let mut stream = merge_shard_streams(backend, TOPIC.to_string(), &shard_ids, after);

    let mut seen = Vec::new();
    for _ in 0..keys.len() {
        let event = next_event(&mut stream).await;
        seen.push(event.payload_json["k"].as_str().unwrap().to_string());
    }

    let mut expected: Vec<String> = keys.iter().map(ToString::to_string).collect();
    expected.sort();
    seen.sort();
    assert_eq!(seen, expected);
}

#[tokio::test]
async fn merged_stream_respects_per_shard_replay_cursor() {
    let backend = mem_backend();

    publish_keyed(&backend, "alice", 1).await;
    publish_keyed(&backend, "alice", 2).await;

    let shard = shard_id("alice", SHARDS);

    // Full replay: first delivered event on alice's shard is n=1.
    let mut stream = merge_shard_streams(
        Arc::clone(&backend),
        TOPIC.to_string(),
        &[shard],
        HashMap::from([(shard, Some(0))]),
    );
    let first = next_event(&mut stream).await;
    assert_eq!(first.payload_json["n"], 1);
    drop(stream);

    // Replay after the first event's seq: only n=2 remains.
    let mut stream = merge_shard_streams(
        backend,
        TOPIC.to_string(),
        &[shard],
        HashMap::from([(shard, Some(first.seq))]),
    );
    let second = next_event(&mut stream).await;
    assert_eq!(second.payload_json["n"], 2);
}
