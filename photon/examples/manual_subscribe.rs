//! Manual / raw subscribe stream — advanced alternative to typed `EventType::subscribe_on`.
//!
//! Prefer `Ping::subscribe_on(&photon, opts)` (see `keyed_topic`) or `#[subscribe]` + executor
//! (`embedded_mem`) in application code. This example uses [`Photon::subscribe`] by topic name.
//!
//! Run: `cargo run -p uf-photon --example manual_subscribe --features runtime,mem`
//!
//! Requires `PHOTON_TRANSPORT_KEY` (see `docs/configuration.md`).
#![allow(missing_docs)]
#![allow(clippy::unused_async)]
#![allow(clippy::unwrap_used, clippy::expect_used)]
use std::time::Duration;

use futures::StreamExt;
use photon::{topic, Photon};

#[topic(name = "examples.manual_ping")]
pub struct Ping {
    pub n: u32,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt().with_env_filter("info").init();

    // Boot (no start_executor — we consume the stream in-process).
    let photon = Photon::builder().auto_registry().build()?;

    // Open a raw event stream on the topic name (alternative to typed SubscribeOpts).
    let mut stream = photon.subscribe("examples.manual_ping", None, None);

    // Publish on a background task so the main task can await the stream.
    let photon_pub = photon.clone();
    let publish = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(50)).await;
        Ping { n: 1 }.publish_on(&photon_pub).await
    });

    if let Some(result) = stream.next().await {
        let event = result?;
        tracing::info!(event_id = %event.event_id, seq = event.seq, "received via manual subscribe");
    }

    publish.await??;
    Ok(())
}
