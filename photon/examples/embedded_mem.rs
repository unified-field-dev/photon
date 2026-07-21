//! Minimal embedded Photon — **Integrating the host** + **Creating topics**.
//!
//! Run: `cargo run -p uf-photon --example embedded_mem --features runtime,mem`
//!
//! Requires `PHOTON_TRANSPORT_KEY` (base64-encoded 32-byte key). Smoke / CI use
//! `cGhvdG9uLWRldi10cmFuc3BvcnQta2V5LTMyYnl0ZXM=` (dev key). See `docs/configuration.md`.
#![allow(missing_docs)]
#![allow(clippy::unused_async, clippy::used_underscore_binding)]
#![allow(clippy::unwrap_used, clippy::expect_used)]
use std::sync::Arc;

use photon::{subscribe, topic, Actor, JsonIdentityFactory, Photon};

// Step 1 — Define a typed topic (registers metadata via inventory).
#[topic(name = "examples.greeting", keyed_by = "name")]
pub struct GreetingSent {
    pub name: String,
    pub message: String,
}

// Step 2 — Register a durable handler for that topic (dispatched after start_executor).
#[subscribe(topic = "examples.greeting", durable = "logger")]
async fn on_greeting(_actor: Box<dyn Actor>, event: GreetingSent) -> photon::Result<()> {
    tracing::info!(name = %event.name, message = %event.message, "received greeting");
    Ok(())
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt().with_env_filter("info").init();

    // Step 3 — Boot: build runtime, start executor. Keep the handle (handle-first).
    // Optional sugar: configure(photon) enables .publish() without an explicit handle.
    let photon = Photon::builder().auto_registry().build()?;
    photon.start_executor(Arc::new(JsonIdentityFactory))?;

    // Step 4 — Give the executor a moment to attach subscriptions.
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    // Step 5 — Publish via typed handle-first API.
    let event_id = GreetingSent {
        name: "world".into(),
        message: "hello from embedded_mem example".into(),
    }
    .publish_on(&photon)
    .await?;

    tracing::info!(event_id = %event_id, "published");
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    Ok(())
}
