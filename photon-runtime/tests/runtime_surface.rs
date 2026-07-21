//! Integration tests for the Photon runtime surface (mem tier): builder wiring,
//! publish/subscribe happy paths, executor lifecycle including failing-handler
//! DLQ routing, and admin snapshot composition.

#![cfg(feature = "mem")]
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use futures::StreamExt;
use photon_backend::{
    BackendContext, GenericPhotonBackend, HandlerDescriptor, InProcStoragePort, PhotonError,
    RetentionPolicy, StoragePort, TopicDescriptor, TopicRegistry, TransportCrypto,
};
use photon_core::JsonIdentityFactory;
use photon_runtime::Photon;
use photon_telemetry::NoOpsLog;

const TOPIC: &str = "rt.orders";

static DELIVERED: AtomicUsize = AtomicUsize::new(0);

fn ok_invoke<'a>(
    _identity: &'a dyn photon_core::IdentityFactory,
    _event: &'a photon_backend::Event,
) -> Pin<Box<dyn Future<Output = photon_backend::Result<()>> + Send + 'a>> {
    Box::pin(async {
        DELIVERED.fetch_add(1, Ordering::SeqCst);
        Ok(())
    })
}

fn fail_invoke<'a>(
    _identity: &'a dyn photon_core::IdentityFactory,
    _event: &'a photon_backend::Event,
) -> Pin<Box<dyn Future<Output = photon_backend::Result<()>> + Send + 'a>> {
    Box::pin(async { Err(PhotonError::Internal("handler rejected event".into())) })
}

photon_backend::inventory::submit! {
    TopicDescriptor::new(TOPIC, None, "{}")
}

photon_backend::inventory::submit! {
    HandlerDescriptor::new(TOPIC, "rt-ok", "rt.orders:rt-ok", ok_invoke)
}

photon_backend::inventory::submit! {
    HandlerDescriptor::new(TOPIC, "rt-fail", "rt.orders:rt-fail", fail_invoke)
}

fn mem_port() -> Arc<dyn StoragePort> {
    Arc::new(InProcStoragePort::new(TransportCrypto::from_bytes(
        *b"photon-dev-transport-key-32bytes",
    )))
}

fn build_photon() -> Photon {
    Photon::builder()
        .storage_port(mem_port())
        .auto_registry()
        .build()
        .expect("build mem runtime")
}

async fn wait_until(mut condition: impl FnMut() -> bool) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while !condition() {
        assert!(
            tokio::time::Instant::now() < deadline,
            "condition not reached within timeout"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

#[tokio::test]
async fn executor_dispatches_ok_handler_and_dead_letters_failing_handler() {
    let photon = build_photon();
    let identity = Arc::new(JsonIdentityFactory);

    photon
        .start_executor(Arc::clone(&identity) as _)
        .expect("first start succeeds");

    // Sad path: a second start on the same runtime must be rejected.
    let err = photon
        .start_executor(identity as _)
        .expect_err("second start is rejected");
    assert!(matches!(err, PhotonError::Internal(_)));

    let before = DELIVERED.load(Ordering::SeqCst);
    photon
        .publish(
            TOPIC,
            None,
            serde_json::json!({}),
            serde_json::json!({"n": 1}),
        )
        .await
        .expect("publish");

    // Happy path: the ok handler runs; sad path: the failing handler lands in the DLQ.
    let dlq = Arc::clone(&photon.runtime().executor_services.dlq);
    wait_until(|| DELIVERED.load(Ordering::SeqCst) > before).await;
    wait_until(|| !dlq.is_empty()).await;
    assert_eq!(dlq.min_seq_for(TOPIC, None), Some(1));

    photon.shutdown_executor();
    photon.join_executor().await;
}

#[tokio::test]
async fn admin_snapshot_reports_backend_topics_handlers_and_checkpoints() {
    let photon = build_photon();
    let snapshot = photon.admin_snapshot().await.expect("snapshot");

    assert_eq!(snapshot.backend.telemetry_label, "mem");
    assert!(snapshot.backend.supports_get_event);

    assert!(snapshot.topics.iter().any(|t| t.topic_name == TOPIC));

    let modes: Vec<&str> = snapshot
        .handlers
        .iter()
        .filter(|h| h.topic_name == TOPIC)
        .map(|h| h.mode.as_str())
        .collect();
    assert_eq!(modes.len(), 2);
    assert!(modes.iter().all(|m| *m == "durable"));

    // Fresh runtime: inventory handlers are listed with no committed checkpoints.
    let cursors: Vec<_> = snapshot
        .checkpoints
        .iter()
        .filter(|c| c.topic_name == TOPIC)
        .collect();
    assert_eq!(cursors.len(), 2);
    assert!(cursors.iter().all(|c| c.last_seq.is_none()));
}

#[tokio::test]
async fn get_event_and_checkpoints_roundtrip() {
    let photon = build_photon();

    let event_id = photon
        .publish(
            TOPIC,
            None,
            serde_json::json!({}),
            serde_json::json!({"n": 7}),
        )
        .await
        .expect("publish");

    let event = photon
        .get_event(&event_id)
        .await
        .expect("get_event")
        .expect("event stored");
    assert_eq!(event.topic_name, TOPIC);
    assert_eq!(event.payload_json["n"], 7);

    // Sad path: unknown event id resolves to None, not an error.
    assert!(photon
        .get_event("does-not-exist")
        .await
        .expect("get_event")
        .is_none());

    photon
        .set_checkpoint("sub-a", TOPIC, None, 41)
        .await
        .expect("set checkpoint");
    assert_eq!(
        photon
            .get_checkpoint_seq("sub-a", TOPIC, None)
            .await
            .expect("get checkpoint"),
        Some(41)
    );
    // Sad path: unknown subscription has no cursor.
    assert_eq!(
        photon
            .get_checkpoint_seq("sub-unknown", TOPIC, None)
            .await
            .expect("get checkpoint"),
        None
    );
}

#[tokio::test]
async fn subscribe_stream_replays_published_events_in_order() {
    let photon = build_photon();

    for n in 1..=3 {
        photon
            .publish(
                TOPIC,
                None,
                serde_json::json!({}),
                serde_json::json!({"n": n}),
            )
            .await
            .expect("publish");
    }

    let mut stream = photon.subscribe(TOPIC, None, Some(0));
    let mut seqs = Vec::new();
    for _ in 0..3 {
        let event = tokio::time::timeout(Duration::from_secs(5), stream.next())
            .await
            .expect("replay within timeout")
            .expect("stream open")
            .expect("event ok");
        seqs.push(event.seq);
    }
    assert!(seqs.windows(2).all(|w| w[0] < w[1]), "seqs: {seqs:?}");
}

#[tokio::test]
async fn subscribe_consumer_group_with_no_shards_ends_immediately() {
    let photon = build_photon();
    let mut stream = photon.subscribe_consumer_group(TOPIC, &[], std::collections::HashMap::new());
    assert!(stream.next().await.is_none());
}

#[tokio::test]
async fn reclaim_transport_succeeds_on_fresh_runtime() {
    let photon = build_photon();
    let reports = photon.reclaim_transport().await.expect("reclaim");
    // A fresh runtime has nothing past the safe watermark to remove.
    assert!(
        reports.iter().all(|r| r.removed == 0),
        "reports: {reports:?}"
    );
}

#[tokio::test]
async fn configure_sets_process_default() {
    let photon = build_photon();
    assert_eq!(photon.backend_label(), "mem");

    photon_runtime::configure(photon);
    let handle = photon_runtime::default().expect("default configured");
    assert_eq!(handle.backend_label(), "mem");
}

#[tokio::test]
async fn builder_accepts_prebuilt_backend_and_install_context() {
    // Pre-built backend path.
    let backend = GenericPhotonBackend::install_with_port(
        BackendContext {
            registry: TopicRegistry::new(),
        },
        mem_port(),
    )
    .expect("install backend");
    let photon = Photon::builder()
        .backend(backend)
        .storage_port(mem_port())
        .build()
        .expect("build with prebuilt backend");
    assert_eq!(photon.backend_label(), "mem");

    // Install-from-context path, plus ops log and retention policy wiring.
    let port = mem_port();
    let photon = Photon::builder()
        .backend_with_context(move |ctx| GenericPhotonBackend::install_with_port(ctx, port))
        .storage_port(mem_port())
        .ops_log(NoOpsLog)
        .ops_log_arc(Arc::new(NoOpsLog))
        .retention_policy(RetentionPolicy::default())
        .build()
        .expect("build with install fn");
    assert_eq!(photon.backend_label(), "mem");
}
