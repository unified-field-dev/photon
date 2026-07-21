//! Durable subscription checkpoints in `JetStream` KV.

use std::collections::HashMap;

use async_nats::jetstream::kv;
use bytes::Bytes;
use photon_backend::{PhotonError, Result};
use serde_json::Value;

use crate::config::NatsConfig;
use crate::kv::ensure_kv_bucket;
use crate::stream_shard::{composite_seq, decompose_seq, pick_shard, publish_routing_key};
use crate::subject::{checkpoint_key, checkpoint_key_sharded, checkpoint_key_unkeyed_shards};

/// KV bucket storing durable subscription high-water marks.
pub const CHECKPOINT_BUCKET: &str = "photon-checkpoints";

/// KV-backed checkpoint store.
#[derive(Clone)]
pub struct CheckpointStore {
    store: kv::Store,
    config: NatsConfig,
}

impl CheckpointStore {
    /// Open or create the checkpoint bucket.
    ///
    /// # Errors
    ///
    /// Returns an error when the bucket cannot be initialized.
    pub async fn connect(
        jetstream: &async_nats::jetstream::Context,
        config: &NatsConfig,
    ) -> Result<Self> {
        let store = ensure_kv_bucket(jetstream, CHECKPOINT_BUCKET).await?;
        Ok(Self {
            store,
            config: config.clone(),
        })
    }

    /// Load a durable subscription checkpoint.
    ///
    /// # Errors
    ///
    /// Returns an error when the KV read fails.
    pub async fn load(
        &self,
        subscription_name: &str,
        topic_name: &str,
        topic_key: Option<&str>,
    ) -> Result<Option<i64>> {
        if !self.config.is_sharded() {
            let key = checkpoint_key(subscription_name, topic_name, topic_key);
            return load_i64(&self.store, &key).await;
        }

        if let Some(key) = topic_key {
            let shard = pick_shard(key, self.config.stream_shards);
            let ck = checkpoint_key_sharded(subscription_name, topic_name, Some(key), shard);
            let local = load_i64(&self.store, &ck).await?;
            return Ok(
                local.map(|seq| composite_seq(shard, u64::try_from(seq.max(0)).unwrap_or(0)))
            );
        }

        let map = self
            .load_unkeyed_shard_map(subscription_name, topic_name)
            .await?;
        Ok(max_composite_from_map(&map))
    }

    /// Persist a durable subscription checkpoint.
    ///
    /// # Errors
    ///
    /// Returns an error when the KV write fails.
    pub async fn commit(
        &self,
        subscription_name: &str,
        topic_name: &str,
        topic_key: Option<&str>,
        last_seq: i64,
    ) -> Result<()> {
        if !self.config.is_sharded() {
            let key = checkpoint_key(subscription_name, topic_name, topic_key);
            return put_i64(&self.store, &key, last_seq).await;
        }

        let (shard, local) = decompose_seq(last_seq);
        if topic_key.is_some() {
            let routing = publish_routing_key(topic_key, "");
            let expected = pick_shard(&routing, self.config.stream_shards);
            let shard =
                if self.config.stream_shards > 1 && last_seq < crate::stream_shard::SEQ_STRIDE {
                    expected
                } else {
                    shard
                };
            let key = checkpoint_key_sharded(subscription_name, topic_name, topic_key, shard);
            return put_i64(&self.store, &key, local).await;
        }

        let mut map = self
            .load_unkeyed_shard_map(subscription_name, topic_name)
            .await?;
        map.insert(shard, local);
        let key = checkpoint_key_unkeyed_shards(subscription_name, topic_name);
        put_json_map(&self.store, &key, &map).await
    }

    /// Per-stream-shard replay cursors for an unkeyed subscription.
    ///
    /// # Errors
    ///
    /// Returns an error when the KV read fails.
    pub async fn load_unkeyed_shard_map(
        &self,
        subscription_name: &str,
        topic_name: &str,
    ) -> Result<HashMap<u32, i64>> {
        if !self.config.is_sharded() {
            let key = checkpoint_key(subscription_name, topic_name, None);
            return Ok(load_i64(&self.store, &key)
                .await?
                .map(|seq| {
                    let mut map = HashMap::new();
                    map.insert(0, seq);
                    map
                })
                .unwrap_or_default());
        }

        let key = checkpoint_key_unkeyed_shards(subscription_name, topic_name);
        load_shard_map(&self.store, &key).await
    }
}

async fn load_i64(store: &kv::Store, key: &str) -> Result<Option<i64>> {
    match store.get(key).await {
        Ok(Some(bytes)) => parse_checkpoint_bytes(&bytes).map(Some),
        Ok(None) => Ok(None),
        Err(e) => Err(PhotonError::caused(
            format!("nats checkpoint load {key}"),
            e,
        )),
    }
}

async fn put_i64(store: &kv::Store, key: &str, value: i64) -> Result<()> {
    store
        .put(key, Bytes::from(value.to_string()))
        .await
        .map_err(|e| PhotonError::caused(format!("nats checkpoint commit {key}"), e))?;
    Ok(())
}

async fn load_shard_map(store: &kv::Store, key: &str) -> Result<HashMap<u32, i64>> {
    match store.get(key).await {
        Ok(Some(bytes)) => parse_shard_map_bytes(&bytes),
        Ok(None) => Ok(HashMap::new()),
        Err(e) => Err(PhotonError::caused(
            format!("nats checkpoint load {key}"),
            e,
        )),
    }
}

async fn put_json_map(store: &kv::Store, key: &str, map: &HashMap<u32, i64>) -> Result<()> {
    let json = serde_json::to_string(map)
        .map_err(|e| PhotonError::caused("nats checkpoint encode shard map:", e))?;
    store
        .put(key, Bytes::from(json))
        .await
        .map_err(|e| PhotonError::caused(format!("nats checkpoint commit {key}"), e))?;
    Ok(())
}

fn parse_checkpoint_bytes(bytes: &Bytes) -> Result<i64> {
    let raw = std::str::from_utf8(bytes)
        .map_err(|e| PhotonError::caused("nats checkpoint parse utf8:", e))?;
    raw.parse::<i64>()
        .map_err(|e| PhotonError::caused(format!("nats checkpoint parse i64 '{raw}'"), e))
}

fn parse_shard_map_bytes(bytes: &Bytes) -> Result<HashMap<u32, i64>> {
    let raw = std::str::from_utf8(bytes)
        .map_err(|e| PhotonError::caused("nats checkpoint parse utf8:", e))?;
    let value: Value = serde_json::from_str(raw)
        .map_err(|e| PhotonError::caused("nats checkpoint parse json:", e))?;
    let Some(obj) = value.as_object() else {
        return Ok(HashMap::new());
    };
    let mut map = HashMap::new();
    for (k, v) in obj {
        if let (Ok(shard), Some(seq)) = (k.parse::<u32>(), v.as_i64()) {
            map.insert(shard, seq);
        }
    }
    Ok(map)
}

fn max_composite_from_map(map: &HashMap<u32, i64>) -> Option<i64> {
    map.iter()
        .map(|(shard, local)| composite_seq(*shard, u64::try_from((*local).max(0)).unwrap_or(0)))
        .max()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn max_composite_picks_highest_shard_seq() {
        let mut map = HashMap::new();
        map.insert(1, 10);
        map.insert(3, 5);
        assert_eq!(max_composite_from_map(&map), Some(composite_seq(3, 5)));
    }
}
