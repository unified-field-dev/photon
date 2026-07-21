//! Durable subscription checkpoints in a compact Fluvio topic.

use std::collections::HashMap;
use std::sync::Arc;

use bytes::Bytes;
use dashmap::DashMap;
use fluvio::consumer::{ConsumerConfigExtBuilder, OffsetManagementStrategy};
use fluvio::Offset;
use futures::StreamExt;
use photon_backend::{PhotonError, Result};

use crate::config::FluvioConfig;
use crate::connect::SharedClient;
use crate::publish::PublishPipeline;
use crate::stream_shard::{composite_seq, decompose_seq, pick_shard, publish_routing_key};
use crate::subject::{checkpoint_key, checkpoint_key_sharded, checkpoint_key_unkeyed_shards};

/// In-memory cache backed by compact topic persistence.
#[derive(Clone)]
pub struct CheckpointStore {
    cache: Arc<DashMap<String, Bytes>>,
    client: SharedClient,
    config: FluvioConfig,
    _pipeline: PublishPipeline,
}

impl CheckpointStore {
    /// Open checkpoint topic, scan existing keys, and return store handle.
    ///
    /// # Errors
    ///
    /// Returns an error when the topic cannot be initialized or scanned.
    pub async fn connect(client: SharedClient, config: &FluvioConfig) -> Result<Self> {
        crate::topic::ensure_checkpoint_topic(&client, config).await?;
        let cache = Arc::new(DashMap::new());
        scan_checkpoint_topic(&client, config, Arc::clone(&cache)).await?;
        let pipeline = PublishPipeline::new(Arc::clone(&client), config);
        Ok(Self {
            cache,
            client,
            config: config.clone(),
            _pipeline: pipeline,
        })
    }

    /// Load a durable subscription checkpoint.
    ///
    /// # Errors
    ///
    /// Returns an error when the read fails.
    pub fn load(
        &self,
        subscription_name: &str,
        topic_name: &str,
        topic_key: Option<&str>,
    ) -> Result<Option<i64>> {
        if !self.config.is_sharded() {
            let key = checkpoint_key(subscription_name, topic_name, topic_key);
            return load_i64(&self.cache, &key);
        }

        if let Some(key) = topic_key {
            let shard = pick_shard(key, self.config.topic_shards);
            let ck = checkpoint_key_sharded(subscription_name, topic_name, Some(key), shard);
            let local = load_i64(&self.cache, &ck)?;
            return Ok(
                local.map(|seq| composite_seq(shard, u64::try_from(seq.max(0)).unwrap_or(0)))
            );
        }

        let map = self.load_unkeyed_shard_map(subscription_name, topic_name)?;
        Ok(max_composite_from_map(&map))
    }

    /// Persist a durable subscription checkpoint.
    ///
    /// # Errors
    ///
    /// Returns an error when the write fails.
    pub async fn commit(
        &self,
        subscription_name: &str,
        topic_name: &str,
        topic_key: Option<&str>,
        last_seq: i64,
    ) -> Result<()> {
        if !self.config.is_sharded() {
            let key = checkpoint_key(subscription_name, topic_name, topic_key);
            return put_i64(self, &key, last_seq).await;
        }

        let (shard, local) = decompose_seq(last_seq);
        if topic_key.is_some() {
            let routing = publish_routing_key(topic_key, "");
            let expected = pick_shard(&routing, self.config.topic_shards);
            let shard =
                if self.config.topic_shards > 1 && last_seq < crate::stream_shard::SEQ_STRIDE {
                    expected
                } else {
                    shard
                };
            let key = checkpoint_key_sharded(subscription_name, topic_name, topic_key, shard);
            return put_i64(self, &key, local).await;
        }

        let mut map = self.load_unkeyed_shard_map(subscription_name, topic_name)?;
        map.insert(shard, local);
        let key = checkpoint_key_unkeyed_shards(subscription_name, topic_name);
        put_json_map(self, &key, &map).await
    }

    /// Per-shard replay cursors for an unkeyed subscription.
    ///
    /// # Errors
    ///
    /// Returns an error when the read fails.
    pub fn load_unkeyed_shard_map(
        &self,
        subscription_name: &str,
        topic_name: &str,
    ) -> Result<HashMap<u32, i64>> {
        if !self.config.is_sharded() {
            let key = checkpoint_key(subscription_name, topic_name, None);
            return Ok(load_i64(&self.cache, &key)?
                .map(|seq| {
                    let mut map = HashMap::new();
                    map.insert(0, seq);
                    map
                })
                .unwrap_or_default());
        }

        let key = checkpoint_key_unkeyed_shards(subscription_name, topic_name);
        load_shard_map(&self.cache, &key)
    }
}

async fn scan_checkpoint_topic(
    client: &SharedClient,
    config: &FluvioConfig,
    cache: Arc<DashMap<String, Bytes>>,
) -> Result<()> {
    let topic = config.checkpoint_topic();
    let consumer_config = ConsumerConfigExtBuilder::default()
        .topic(topic)
        .partition(0)
        .offset_start(Offset::beginning())
        .offset_strategy(OffsetManagementStrategy::None)
        .disable_continuous(true)
        .build()
        .map_err(|e| PhotonError::caused("fluvio checkpoint scanner config:", e))?;

    let mut stream = client
        .consumer_with_config(consumer_config)
        .await
        .map_err(|e| PhotonError::caused("fluvio checkpoint scanner:", e))?;

    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(5);
    loop {
        if tokio::time::Instant::now() >= deadline {
            break;
        }
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        match tokio::time::timeout(remaining, stream.next()).await {
            Ok(Some(Ok(record))) => {
                if let Some(key_data) = record.get_key() {
                    let key = String::from_utf8_lossy(key_data).into_owned();
                    let value = record.value();
                    cache.insert(key, Bytes::copy_from_slice(value));
                }
            }
            Ok(Some(Err(e))) => {
                return Err(PhotonError::Internal(format!(
                    "fluvio checkpoint scan: {e:?}"
                )));
            }
            Ok(None) | Err(_) => break,
        }
    }
    Ok(())
}

fn load_i64(cache: &DashMap<String, Bytes>, key: &str) -> Result<Option<i64>> {
    cache
        .get(key)
        .map_or(Ok(None), |bytes| parse_checkpoint_bytes(&bytes).map(Some))
}

async fn put_i64(store: &CheckpointStore, key: &str, value: i64) -> Result<()> {
    let payload = Bytes::from(value.to_string());
    store.cache.insert(key.to_string(), payload.clone());
    produce_checkpoint(store, key, payload).await
}

fn load_shard_map(cache: &DashMap<String, Bytes>, key: &str) -> Result<HashMap<u32, i64>> {
    cache
        .get(key)
        .map_or_else(|| Ok(HashMap::new()), |bytes| parse_shard_map_bytes(&bytes))
}

async fn put_json_map(store: &CheckpointStore, key: &str, map: &HashMap<u32, i64>) -> Result<()> {
    let json = serde_json::to_string(map)
        .map_err(|e| PhotonError::caused("fluvio checkpoint encode shard map:", e))?;
    let payload = Bytes::from(json);
    store.cache.insert(key.to_string(), payload.clone());
    produce_checkpoint(store, key, payload).await
}

async fn produce_checkpoint(store: &CheckpointStore, key: &str, payload: Bytes) -> Result<()> {
    let topic = store.config.checkpoint_topic();
    let producer = store
        .client
        .topic_producer(topic)
        .await
        .map_err(|e| PhotonError::caused(format!("fluvio checkpoint producer {key}"), e))?;
    producer
        .send(key, payload.to_vec())
        .await
        .map_err(|e| PhotonError::caused(format!("fluvio checkpoint commit {key}"), e))?
        .wait()
        .await
        .map_err(|e| PhotonError::caused(format!("fluvio checkpoint ack {key}"), e))?;
    Ok(())
}

fn parse_checkpoint_bytes(bytes: &Bytes) -> Result<i64> {
    let raw = std::str::from_utf8(bytes)
        .map_err(|e| PhotonError::caused("fluvio checkpoint parse utf8:", e))?;
    raw.parse::<i64>()
        .map_err(|e| PhotonError::caused(format!("fluvio checkpoint parse i64 '{raw}'"), e))
}

fn parse_shard_map_bytes(bytes: &Bytes) -> Result<HashMap<u32, i64>> {
    let raw = std::str::from_utf8(bytes)
        .map_err(|e| PhotonError::caused("fluvio checkpoint parse utf8:", e))?;
    let value: serde_json::Value = serde_json::from_str(raw)
        .map_err(|e| PhotonError::caused("fluvio checkpoint parse json:", e))?;
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
