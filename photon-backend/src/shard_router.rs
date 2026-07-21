//! Virtual shard routing for consumer-group topics.

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

use serde_json::Value;

use crate::delivery_mode::ShardConfig;
use crate::descriptor::TopicDescriptor;

/// Storage key prefix for virtual shard streams.
pub const SHARD_KEY_PREFIX: &str = "__photon/shard/";

/// Stable hash routing key → shard index in `[0, shard_count)`.
#[must_use]
pub fn shard_id(routing_key: &str, shard_count: u32) -> u32 {
    let n = shard_count.max(1);
    let mut hasher = DefaultHasher::new();
    routing_key.hash(&mut hasher);
    u32::try_from(hasher.finish() % u64::from(n)).unwrap_or(0)
}

/// Storage key for a virtual shard stream.
#[must_use]
pub fn shard_storage_key(shard_id: u32) -> String {
    format!("{SHARD_KEY_PREFIX}{shard_id}")
}

/// Whether a storage key is a reserved virtual shard key.
#[must_use]
pub fn is_shard_storage_key(key: &str) -> bool {
    key.starts_with(SHARD_KEY_PREFIX)
}

/// Parse shard id from storage key, if present.
#[must_use]
pub fn parse_shard_storage_key(key: &str) -> Option<u32> {
    key.strip_prefix(SHARD_KEY_PREFIX)?.parse().ok()
}

/// Resolve routing key for shard assignment before append.
#[must_use]
pub fn routing_key(
    publish_key: Option<&str>,
    payload: &Value,
    shard_by: Option<&str>,
    event_id: &str,
) -> String {
    if let Some(k) = publish_key {
        return k.to_string();
    }
    if let Some(field) = shard_by {
        if let Some(v) = payload.get(field) {
            if let Some(s) = v.as_str() {
                return s.to_string();
            }
            return v.to_string();
        }
    }
    event_id.to_string()
}

/// Compute shard storage key for a group-topic publish.
pub fn group_publish_storage_key(
    desc: &TopicDescriptor,
    publish_key: Option<&str>,
    payload: &Value,
    event_id: &str,
) -> String {
    let cfg = desc.shard_config.unwrap_or_else(ShardConfig::default_count);
    let rk = routing_key(publish_key, payload, cfg.shard_by, event_id);
    let id = shard_id(&rk, cfg.shard_count);
    shard_storage_key(id)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::delivery_mode::{DeliveryMode, ShardConfig};

    #[test]
    fn shard_id_stable() {
        assert_eq!(shard_id("order-1", 32), shard_id("order-1", 32));
        assert!(shard_id("a", 32) < 32);
    }

    #[test]
    fn storage_key_roundtrip() {
        let k = shard_storage_key(7);
        assert_eq!(parse_shard_storage_key(&k), Some(7));
        assert!(is_shard_storage_key(&k));
    }

    #[test]
    fn routing_key_prefers_publish_key() {
        let payload = serde_json::json!({"order_id": "x"});
        assert_eq!(
            routing_key(Some("explicit"), &payload, Some("order_id"), "ev"),
            "explicit"
        );
    }

    #[test]
    fn group_publish_uses_descriptor() {
        let desc = TopicDescriptor {
            topic_name: "t",
            keyed_by: None,
            schema_json: "{}",
            delivery: DeliveryMode::ConsumerGroup,
            shard_config: Some(ShardConfig::new(8, Some("k"))),
        };
        let payload = serde_json::json!({"k": "abc"});
        let sk = group_publish_storage_key(&desc, None, &payload, "ev-fallback");
        assert!(is_shard_storage_key(&sk));
    }
}
