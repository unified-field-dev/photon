//! Keyed topic with partition filter — **Creating topics**.
//!
//! Run: `cargo run -p uf-photon --example keyed_topic --features runtime,mem`
//!
//! Requires `PHOTON_TRANSPORT_KEY` (see `docs/configuration.md`).
#![allow(missing_docs)]
#![allow(clippy::unused_async)]
#![allow(clippy::unwrap_used, clippy::expect_used)]
use std::time::Duration;

use futures::StreamExt;
use photon::{topic, Photon, SubscribeOpts};

// keyed_by routes events to partitions; subscribe can filter to one key.
#[topic(name = "examples.orders", keyed_by = "customer_id")]
pub struct OrderPlaced {
    pub customer_id: String,
    pub amount_cents: u64,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt().with_env_filter("info").init();

    let photon = Photon::builder().auto_registry().build()?;

    // Ephemeral typed stream — only "alice" partitions are delivered.
    let opts = SubscribeOpts::default_ephemeral().topic_key_filter("alice");
    let mut stream = OrderPlaced::subscribe_on(&photon, opts).await?;

    // Publish bob first (filtered out), then alice (should arrive).
    OrderPlaced {
        customer_id: "bob".into(),
        amount_cents: 200,
    }
    .publish_on(&photon)
    .await?;
    OrderPlaced {
        customer_id: "alice".into(),
        amount_cents: 100,
    }
    .publish_on(&photon)
    .await?;

    let result = tokio::time::timeout(Duration::from_secs(2), stream.next()).await?;
    if let Some(Ok(envelope)) = result {
        tracing::info!(
            customer = %envelope.payload.customer_id,
            cents = envelope.payload.amount_cents,
            "filtered event"
        );
    }

    Ok(())
}
