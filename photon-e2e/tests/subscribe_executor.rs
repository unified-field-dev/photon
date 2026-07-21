//! Integration test: `#[photon::subscribe]` + `start_executor` dispatch.
#![allow(missing_docs)]
#![allow(clippy::unused_async)]

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use photon::{configure, subscribe, topic, JsonIdentityFactory, Photon};
use photon_core::Actor;

static HANDLER_INVOCATIONS: AtomicUsize = AtomicUsize::new(0);

#[topic(name = "test.subscribe.dispatch")]
pub struct TestDispatchEvent {
    pub value: u32,
}

#[subscribe(topic = "test.subscribe.dispatch", durable = "test-handler")]
async fn on_test_dispatch(actor: Box<dyn Actor>, event: TestDispatchEvent) -> photon::Result<()> {
    assert_eq!(actor.label(), "photon_publish");
    assert_eq!(event.value, 42);
    HANDLER_INVOCATIONS.fetch_add(1, Ordering::SeqCst);
    Ok(())
}

#[tokio::test]
async fn subscribe_macro_dispatches_on_publish() {
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

    TestDispatchEvent { value: 42 }
        .publish()
        .await
        .expect("publish");

    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    assert_eq!(HANDLER_INVOCATIONS.load(Ordering::SeqCst), 1);
}
