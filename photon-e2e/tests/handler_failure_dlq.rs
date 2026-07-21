//! Integration test: handler failure records a DLQ row via the Photon executor.

#![allow(missing_docs)]
#![allow(clippy::unused_async)]

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use photon::{configure, subscribe, topic, JsonIdentityFactory, Photon};
use photon_core::Actor;

static FAILING_HANDLER_INVOCATIONS: AtomicUsize = AtomicUsize::new(0);

#[topic(name = "test.handler.dlq")]
pub struct DlqTestEvent {
    pub value: u32,
}

#[subscribe(topic = "test.handler.dlq", durable = "failing-handler")]
async fn on_dlq_test_event(_actor: Box<dyn Actor>, _event: DlqTestEvent) -> photon::Result<()> {
    FAILING_HANDLER_INVOCATIONS.fetch_add(1, Ordering::SeqCst);
    Err(photon::PhotonError::Internal(
        "handler failed intentionally".into(),
    ))
}

#[tokio::test]
async fn handler_failure_records_dlq_row() {
    FAILING_HANDLER_INVOCATIONS.store(0, Ordering::SeqCst);

    let photon = Photon::builder()
        .auto_registry()
        .build()
        .expect("build photon");

    photon
        .start_executor(Arc::new(JsonIdentityFactory))
        .expect("start executor");
    configure(photon.clone());

    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    DlqTestEvent { value: 1 }.publish().await.expect("publish");

    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    assert_eq!(FAILING_HANDLER_INVOCATIONS.load(Ordering::SeqCst), 1);
    assert!(
        !photon.runtime().executor_services.dlq.is_empty(),
        "expected DLQ row after handler failure"
    );
}
