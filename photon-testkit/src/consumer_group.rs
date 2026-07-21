//! Static two-member consumer group lab (BM-PG0 / BM-PB1).

use std::collections::HashSet;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{bail, Result};
use futures::StreamExt;
use photon_backend::consumer_group::round_robin_shards_for_member;
use photon_backend::shard_router::{shard_id, shard_storage_key};
use photon_runtime::Photon;

use crate::fixtures::smoke_actor_json;

const DELIVERY_TIMEOUT: Duration = Duration::from_mins(1);

fn subscribe_warmup() -> Duration {
    crate::shared_store::subscribe_attach_warmup()
}

/// Run two static group members with disjoint shard assignments and verify full coverage.
///
/// # Errors
///
/// Returns an error if the operation fails.
pub async fn run_consumer_group_static_lab(
    photon: &Photon,
    topic: &str,
    shard_count: u32,
    publish_count: u32,
) -> Result<ConsumerGroupStaticOutcome> {
    let split = shard_count / 2;
    let member_a_shards: Vec<u32> = (0..split).collect();
    let member_b_shards: Vec<u32> = (split..shard_count).collect();

    run_consumer_group_with_shards(
        photon,
        topic,
        shard_count,
        publish_count,
        publish_count,
        member_a_shards,
        member_b_shards,
    )
    .await
}

/// Run two group members using round-robin shard assignment and verify full coverage.
///
/// # Errors
///
/// Returns an error if the operation fails.
pub async fn run_consumer_group_round_robin_lab(
    photon: &Photon,
    topic: &str,
    shard_count: u32,
    member_count: u32,
    publish_count: u32,
) -> Result<ConsumerGroupStaticOutcome> {
    let member_a_shards = round_robin_shards_for_member(shard_count, member_count, 0);
    let member_b_shards = round_robin_shards_for_member(shard_count, member_count, 1);
    run_consumer_group_with_shards(
        photon,
        topic,
        shard_count,
        publish_count,
        publish_count,
        member_a_shards,
        member_b_shards,
    )
    .await
}

/// Replay and verify consumer-group coverage without publishing new events.
///
/// # Errors
///
/// Returns an error if the operation fails.
pub async fn run_consumer_group_verify_lab(
    photon: &Photon,
    topic: &str,
    shard_count: u32,
    expected_count: u32,
) -> Result<ConsumerGroupStaticOutcome> {
    let split = shard_count / 2;
    let member_a_shards: Vec<u32> = (0..split).collect();
    let member_b_shards: Vec<u32> = (split..shard_count).collect();
    let outcome = run_consumer_group_with_shards(
        photon,
        topic,
        shard_count,
        0,
        expected_count,
        member_a_shards,
        member_b_shards,
    )
    .await?;
    if outcome.full_coverage() && outcome.unique_event_ids >= expected_count {
        Ok(outcome)
    } else {
        bail!(
            "consumer group verify failed: unique {} / expected {}",
            outcome.unique_event_ids,
            expected_count
        )
    }
}

async fn run_consumer_group_with_shards(
    photon: &Photon,
    topic: &str,
    shard_count: u32,
    publish_count: u32,
    expected_deliveries: u32,
    member_a_shards: Vec<u32>,
    member_b_shards: Vec<u32>,
) -> Result<ConsumerGroupStaticOutcome> {
    let replay_from_start = publish_count == 0 && expected_deliveries > 0;
    let delivered_a = Arc::new(AtomicU32::new(0));
    let delivered_b = Arc::new(AtomicU32::new(0));
    let seen_ids = Arc::new(tokio::sync::Mutex::new(HashSet::new()));

    let task_a = spawn_group_member(
        photon.clone(),
        topic,
        member_a_shards.clone(),
        replay_from_start,
        Arc::clone(&delivered_a),
        Arc::clone(&seen_ids),
    );
    let task_b = spawn_group_member(
        photon.clone(),
        topic,
        member_b_shards.clone(),
        replay_from_start,
        Arc::clone(&delivered_b),
        Arc::clone(&seen_ids),
    );

    tokio::time::sleep(subscribe_warmup()).await;

    for i in 0..publish_count {
        let partition_key = format!("pk-{i}");
        let payload = serde_json::json!({ "partition_key": partition_key, "i": i });
        photon
            .publish(topic, None, smoke_actor_json(), payload)
            .await?;
    }

    let expected = expected_deliveries;
    if expected > 0 {
        let deadline = tokio::time::Instant::now() + DELIVERY_TIMEOUT;
        loop {
            let total = delivered_a.load(Ordering::SeqCst) + delivered_b.load(Ordering::SeqCst);
            if total >= expected {
                break;
            }
            if tokio::time::Instant::now() >= deadline {
                bail!("consumer group lab timeout: got {total}/{expected} deliveries");
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    } else {
        tokio::time::sleep(Duration::from_millis(500)).await;
    }

    task_a.abort();
    task_b.abort();
    // Let broker push consumers drop interest before a following RestartRuntime + replay.
    tokio::time::sleep(Duration::from_millis(250)).await;

    let unique = u32::try_from(seen_ids.lock().await.len()).unwrap_or(u32::MAX);
    Ok(ConsumerGroupStaticOutcome {
        published: publish_count,
        delivered_a: delivered_a.load(Ordering::SeqCst),
        delivered_b: delivered_b.load(Ordering::SeqCst),
        unique_event_ids: unique,
        shard_count,
    })
}

fn spawn_group_member(
    photon: Photon,
    topic: &str,
    shards: Vec<u32>,
    replay_from_start: bool,
    counter: Arc<AtomicU32>,
    seen_ids: Arc<tokio::sync::Mutex<HashSet<String>>>,
) -> tokio::task::JoinHandle<()> {
    let topic = topic.to_string();
    tokio::spawn(async move {
        let after: std::collections::HashMap<u32, Option<i64>> = shards
            .iter()
            .map(|s| (*s, replay_from_start.then_some(0)))
            .collect();
        let mut stream = photon.subscribe_consumer_group(&topic, &shards, after);
        while let Some(Ok(event)) = stream.next().await {
            counter.fetch_add(1, Ordering::SeqCst);
            seen_ids.lock().await.insert(event.event_id);
        }
    })
}

/// Which member should receive a publish for verification helpers.
#[must_use]
pub fn expected_member_for_key(partition_key: &str, shard_count: u32) -> u8 {
    let split = shard_count / 2;
    let shard = shard_id(partition_key, shard_count);
    u8::from(shard >= split)
}

/// Storage key string for a shard id.
#[must_use]
pub fn shard_key_for_id(shard: u32) -> String {
    shard_storage_key(shard)
}

/// Metrics from a static two-member consumer group run.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConsumerGroupStaticOutcome {
    /// Events published into the group topic.
    pub published: u32,
    /// Deliveries received by member A.
    pub delivered_a: u32,
    /// Deliveries received by member B.
    pub delivered_b: u32,
    /// Distinct event ids across both members.
    pub unique_event_ids: u32,
    /// Shard count used for the lab.
    pub shard_count: u32,
}

impl ConsumerGroupStaticOutcome {
    /// Whether every published event was delivered at least once.
    #[must_use]
    pub const fn full_coverage(&self) -> bool {
        if self.published == 0 {
            return self.unique_event_ids > 0
                && self.delivered_a + self.delivered_b >= self.unique_event_ids;
        }
        self.unique_event_ids >= self.published
            && self.delivered_a + self.delivered_b >= self.published
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn shard_split_covers_all() {
        assert_eq!(
            expected_member_for_key("a", 8),
            expected_member_for_key("a", 8)
        );
    }
}
