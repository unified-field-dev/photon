//! Consumer group topic + group handler — **Creating topics**.
//!
//! Run: `cargo run -p uf-photon --example consumer_group --features runtime,mem`
//!
//! Requires `PHOTON_TRANSPORT_KEY` (see `docs/configuration.md`).
#![allow(missing_docs)]
#![allow(clippy::unused_async, clippy::used_underscore_binding)]
#![allow(clippy::unwrap_used, clippy::expect_used)]
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;

use photon::{subscribe, topic, Actor, JsonIdentityFactory, Photon};

const SHARD_COUNT: u32 = 4;
const PUBLISH_COUNT: u32 = 40;

static HANDLED: AtomicU32 = AtomicU32::new(0);

// Group delivery + sharding: load-balanced handlers across virtual shards.
#[topic(
    name = "examples.group.work",
    delivery = "group",
    shards = 4,
    shard_by = "partition_key"
)]
pub struct WorkItem {
    pub partition_key: String,
    pub n: u32,
}

#[subscribe(topic = "examples.group.work", group = "workers")]
async fn on_work(_actor: Box<dyn Actor>, event: WorkItem) -> photon::Result<()> {
    HANDLED.fetch_add(1, Ordering::SeqCst);
    tracing::debug!(n = event.n, key = %event.partition_key, "handled work item");
    Ok(())
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt().with_env_filter("info").init();

    // Assign all virtual shards to this single process (multi-node would split 0-3).
    std::env::set_var("PHOTON_GROUP_SHARD_COUNT", SHARD_COUNT.to_string());
    std::env::set_var("PHOTON_GROUP_SHARD_ASSIGNMENT", "0-3");

    let photon = Photon::builder().auto_registry().build()?;
    photon.start_executor(Arc::new(JsonIdentityFactory))?;

    for i in 0..PUBLISH_COUNT {
        WorkItem {
            partition_key: format!("pk-{i}"),
            n: i,
        }
        .publish_on(&photon)
        .await?;
    }

    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(10);
    loop {
        let handled = HANDLED.load(Ordering::SeqCst);
        if handled >= PUBLISH_COUNT {
            tracing::info!(handled, "consumer group example OK");
            return Ok(());
        }
        if tokio::time::Instant::now() >= deadline {
            anyhow::bail!("timeout: handled {handled}/{PUBLISH_COUNT}");
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
}
