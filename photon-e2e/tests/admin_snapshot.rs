//! Integration test: `Photon::admin_snapshot` durable handler introspection.
#![allow(missing_docs)]
#![allow(clippy::unused_async)]

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use photon::{configure, subscribe, topic, JsonIdentityFactory, Photon};
use photon_core::Actor;

static HANDLER_INVOCATIONS: AtomicUsize = AtomicUsize::new(0);

#[topic(name = "test.admin.snapshot")]
pub struct TestAdminSnapshotEvent {
    pub value: u32,
}

#[subscribe(topic = "test.admin.snapshot", durable = "admin-test-handler")]
async fn on_admin_snapshot_test(
    actor: Box<dyn Actor>,
    event: TestAdminSnapshotEvent,
) -> photon::Result<()> {
    assert_eq!(actor.label(), "photon_publish");
    assert_eq!(event.value, 7);
    HANDLER_INVOCATIONS.fetch_add(1, Ordering::SeqCst);
    Ok(())
}

#[tokio::test]
async fn admin_snapshot_reports_topics_handlers_and_checkpoint() {
    HANDLER_INVOCATIONS.store(0, Ordering::SeqCst);

    let photon = Photon::builder()
        .auto_registry()
        .build()
        .expect("build photon");

    photon
        .start_executor(Arc::new(JsonIdentityFactory))
        .expect("start executor");
    configure(photon.clone());

    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    TestAdminSnapshotEvent { value: 7 }
        .publish()
        .await
        .expect("publish");

    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    assert_eq!(HANDLER_INVOCATIONS.load(Ordering::SeqCst), 1);

    photon
        .runtime()
        .executor_services
        .checkpoint_coalescer
        .flush()
        .await
        .expect("flush checkpoints");

    let snapshot = photon.admin_snapshot().await.expect("admin snapshot");

    assert!(snapshot.backend.supports_get_event);
    assert!(snapshot.backend.supports_list_events);
    assert_eq!(snapshot.backend.telemetry_label, "mem");

    let topic = snapshot
        .topics
        .iter()
        .find(|t| t.topic_name == "test.admin.snapshot")
        .expect("topic in snapshot");
    assert!(topic.schema_json.is_object());

    let handler = snapshot
        .handlers
        .iter()
        .find(|h| h.registry_key == "test.admin.snapshot:admin-test-handler")
        .expect("handler in snapshot");
    assert_eq!(handler.mode, "durable");
    assert_eq!(
        handler.subscription_name.as_deref(),
        Some("admin-test-handler")
    );
    assert!(handler.consumer_group.is_none());

    let checkpoint = snapshot
        .checkpoints
        .iter()
        .find(|c| {
            c.subscription_name == "admin-test-handler"
                && c.topic_name == "test.admin.snapshot"
                && c.topic_key.is_none()
        })
        .expect("checkpoint in snapshot");
    assert!(checkpoint.last_seq.is_some());
}
