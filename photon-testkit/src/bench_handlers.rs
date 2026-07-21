//! Inventory-registered handler for bench/e2e executor scenarios (BM-PFE).

use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicU32, Ordering};

use photon_backend::models::Event;
use photon_backend::{HandlerDescriptor, Result};
use photon_core::IdentityFactory;

/// Topic used by executor fixture scenarios.
pub const EXECUTOR_FIXTURE_TOPIC: &str = "testkit.executor";

static EXECUTOR_INVOCATIONS: AtomicU32 = AtomicU32::new(0);

/// Reset the executor fixture invocation counter.
pub fn reset_executor_invocations() {
    EXECUTOR_INVOCATIONS.store(0, Ordering::SeqCst);
}

/// Current executor fixture invocation count.
#[must_use]
pub fn executor_invocation_count() -> u32 {
    EXECUTOR_INVOCATIONS.load(Ordering::SeqCst)
}

#[allow(clippy::unused_async)] // HandlerInvoker requires async fn signature
async fn dispatch_testkit_executor(_factory: &dyn IdentityFactory, _event: &Event) -> Result<()> {
    EXECUTOR_INVOCATIONS.fetch_add(1, Ordering::SeqCst);
    Ok(())
}

fn dispatch_testkit_executor_fn<'a>(
    factory: &'a dyn IdentityFactory,
    event: &'a Event,
) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'a>> {
    Box::pin(dispatch_testkit_executor(factory, event))
}

photon_backend::inventory::submit! {
    HandlerDescriptor::new(
        EXECUTOR_FIXTURE_TOPIC,
        "testkit-exec",
        "testkit.executor:testkit-exec",
        dispatch_testkit_executor_fn,
    )
}
