//! Subscribe macro v2 — `Arc<dyn Actor>` and `HandlerCtx`.
//!
//! Run: `cargo run -p uf-photon --example subscribe_v2 --features runtime,mem`
#![allow(missing_docs)]
#![allow(clippy::unused_async, clippy::used_underscore_binding)]
#![allow(clippy::unwrap_used, clippy::expect_used)]
use std::sync::Arc;

use photon::{configure, subscribe, topic, Actor, HandlerCtx, JsonIdentityFactory, Photon};

#[topic(name = "examples.subscribe_v2", keyed_by = "name")]
pub struct GreetingSent {
    pub name: String,
    pub message: String,
}

#[subscribe(topic = "examples.subscribe_v2", durable = "arc-logger")]
async fn on_greeting_arc(
    actor: Arc<dyn Actor>,
    event: GreetingSent,
    ctx: HandlerCtx<'_>,
) -> photon::Result<()> {
    tracing::info!(
        actor = %actor.label(),
        event_id = %ctx.event_id,
        seq = ctx.seq,
        name = %event.name,
        message = %event.message,
        "received greeting (subscribe v2)"
    );
    Ok(())
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt().with_env_filter("info").init();

    let photon = Photon::builder().auto_registry().build()?;
    photon.start_executor(Arc::new(JsonIdentityFactory))?;
    configure(photon);

    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    let event_id = GreetingSent {
        name: "v2".into(),
        message: "hello from subscribe_v2 example".into(),
    }
    .publish()
    .await?;

    tracing::info!(event_id = %event_id, "published");
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    Ok(())
}
