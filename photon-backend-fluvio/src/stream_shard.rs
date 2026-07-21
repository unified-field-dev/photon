//! Fluvio topic shard routing (distinct from Photon virtual shard topic keys).

use photon_backend::shard_router::shard_id;

use crate::config::FluvioConfig;

/// Stride for encoding `(topic_shard, local_seq)` into one `Event.seq`.
pub const SEQ_STRIDE: i64 = 1_000_000_000_000;

/// Max supported topic shards.
pub const MAX_TOPIC_SHARDS: u32 = 256;

/// Environment variable for topic shard count (bench/scripts fallback).
pub const TOPIC_SHARDS_ENV: &str = "PHOTON_FLUVIO_TOPIC_SHARDS";

/// Pick a topic shard index from routing key and configured shard count.
#[must_use]
pub fn pick_shard(routing_key: &str, shard_count: u32) -> u32 {
    shard_id(routing_key, shard_count.max(1))
}

/// Routing key for publish shard assignment.
#[must_use]
pub fn publish_routing_key(topic_key: Option<&str>, event_id: &str) -> String {
    topic_key.map_or_else(|| event_id.to_string(), str::to_string)
}

/// Fluvio topic name for a shard (or base layout when `shards == 1`).
#[must_use]
pub fn fluvio_topic_for(config: &FluvioConfig, shard: u32, topic_name: &str) -> String {
    if config.topic_shards <= 1 {
        crate::subject::photon_topic(topic_name)
    } else {
        crate::subject::truncate_or_hash(
            &format!("{}-{}", crate::subject::SHARDED_TOPIC_PREFIX, shard),
            topic_name,
        )
    }
}

/// Encode per-shard local sequence and shard into one composite `Event.seq`.
#[must_use]
pub fn composite_seq(shard: u32, local_seq: u64) -> i64 {
    i64::from(shard)
        .saturating_mul(SEQ_STRIDE)
        .saturating_add(i64::try_from(local_seq).unwrap_or(i64::MAX))
}

/// Split composite sequence into `(shard, local_seq)`.
#[must_use]
pub fn decompose_seq(composite: i64) -> (u32, i64) {
    if composite <= 0 {
        return (0, 0);
    }
    let shard = u32::try_from(composite / SEQ_STRIDE).unwrap_or(0);
    let local = composite % SEQ_STRIDE;
    (shard, local)
}

/// Per-shard replay cursor for subscribe when caller passes one composite `after_seq`.
#[must_use]
pub fn local_after_seq_for_shard(shard: u32, after_seq: Option<i64>) -> Option<i64> {
    if after_seq == Some(0) {
        return Some(0);
    }
    let composite = after_seq.filter(|&s| s > 0)?;
    let (seq_shard, local) = decompose_seq(composite);
    match seq_shard.cmp(&shard) {
        std::cmp::Ordering::Equal => Some(local),
        std::cmp::Ordering::Greater => Some(0),
        std::cmp::Ordering::Less => None,
    }
}

/// Parse topic shard count from env (default 1).
#[must_use]
pub fn topic_shards_from_env() -> u32 {
    std::env::var(TOPIC_SHARDS_ENV)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(1)
        .clamp(1, MAX_TOPIC_SHARDS)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{FluvioConfig, ReplayCursor};

    fn test_config(topic_shards: u32) -> FluvioConfig {
        FluvioConfig {
            endpoint: String::new(),
            topic_prefix: "photon".into(),
            retention: std::time::Duration::from_mins(15),
            replicas: 1,
            crypto: photon_backend::TransportCrypto::from_bytes(
                *b"photon-dev-transport-key-32bytes",
            ),
            replay_cursor: ReplayCursor::StreamSeq,
            sync_ack: true,
            max_inflight: 1,
            topic_shards,
        }
    }

    #[test]
    fn pick_shard_is_stable() {
        assert_eq!(pick_shard("user-1", 4), pick_shard("user-1", 4));
    }

    #[test]
    fn composite_roundtrip() {
        let c = composite_seq(3, 42);
        assert_eq!(decompose_seq(c), (3, 42));
    }

    #[test]
    fn local_after_zero_replays_from_start() {
        assert_eq!(local_after_seq_for_shard(2, Some(0)), Some(0));
    }

    #[test]
    fn legacy_topic_when_single_shard() {
        assert_eq!(
            fluvio_topic_for(&test_config(1), 0, "orders"),
            "photon-orders"
        );
    }

    #[test]
    fn sharded_topic_layout() {
        let name = fluvio_topic_for(&test_config(4), 2, "orders");
        assert!(name.starts_with("photon-s-2-"));
        assert!(name.len() <= 63);
    }

    #[test]
    fn dots_become_hyphens() {
        assert_eq!(
            fluvio_topic_for(&test_config(1), 0, "testkit.contract.uuid"),
            "photon-testkit-contract-uuid"
        );
    }

    #[test]
    fn long_names_are_hashed() {
        let long = "testkit.".to_string() + &"x".repeat(80);
        let topic = fluvio_topic_for(&test_config(1), 0, &long);
        assert!(topic.starts_with("photon-"));
        assert!(topic.len() <= 63);
    }
}
