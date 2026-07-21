//! Install [`ConsoleOpsLog`] at boot — **Integrating the host** (telemetry).
//!
//! Run: `cargo run -p uf-photon --example telemetry_ops_log --features runtime,mem`
//!
//! Requires `PHOTON_TRANSPORT_KEY` (see `docs/configuration.md`).
#![allow(missing_docs)]
#![allow(clippy::unused_async, clippy::used_underscore_binding)]
#![allow(clippy::unwrap_used, clippy::expect_used)]
use std::sync::Arc;

use photon::{subscribe, topic, Actor, ConsoleOpsLog, JsonIdentityFactory, Photon};

#[topic(name = "examples.telemetry.demo")]
pub struct DemoEvent {
    pub n: u32,
}

#[subscribe(topic = "examples.telemetry.demo", durable = "telemetry-demo")]
async fn on_demo(_actor: Box<dyn Actor>, event: DemoEvent) -> photon::Result<()> {
    tracing::info!(n = event.n, "handled with ops log installed");
    Ok(())
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt().with_env_filter("info").init();

    // ops_log installs before build — publish/handler paths emit self-metrics.
    let photon = Photon::builder()
        .ops_log(ConsoleOpsLog)
        .auto_registry()
        .build()?;
    photon.start_executor(Arc::new(JsonIdentityFactory))?;

    DemoEvent { n: 1 }.publish_on(&photon).await?;
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    Ok(())
}
