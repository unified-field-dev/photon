//! Fluvio [`StoragePort`] implementation.

use std::pin::Pin;
use std::sync::Arc;

use async_trait::async_trait;
use chrono::Utc;
use futures::stream::Stream;
use photon_backend::models::Event;
use photon_backend::{
    seal_event_for_storage, PhotonError, Result, StorageCapabilities, StoragePort,
};
use serde_json::Value;
use uuid::Uuid;

use crate::checkpoint::CheckpointStore;
use crate::config::{FluvioConfig, FluvioStoragePortBuilder, ReplayCursor, ENDPOINT_ENV};
use crate::connect::{connect_fluvio, SharedClient};
use crate::consumer::subscribe_stream;
use crate::publish::PublishPipeline;
use crate::stream_shard::{composite_seq, fluvio_topic_for, pick_shard, publish_routing_key};
use crate::topic::{ensure_checkpoint_topic, ensure_data_topic, warn_replication_settings};

/// Read Fluvio endpoint from the environment.
///
/// # Errors
///
/// Returns an error when `PHOTON_FLUVIO_ENDPOINT` is unset.
pub fn fluvio_endpoint_from_env() -> Result<String> {
    std::env::var(ENDPOINT_ENV).map_err(|_| {
        PhotonError::Internal(format!("{ENDPOINT_ENV} not set for fluvio storage adapter"))
    })
}

/// Fluvio-backed storage port.
pub struct FluvioStoragePort {
    client: SharedClient,
    config: FluvioConfig,
    pipeline: PublishPipeline,
    checkpoint_store: CheckpointStore,
    ensured_topics: Arc<dashmap::DashSet<String>>,
}

impl FluvioStoragePort {
    /// Start a builder for explicit host wiring.
    #[must_use]
    pub fn builder() -> FluvioStoragePortBuilder {
        FluvioStoragePortBuilder::new()
    }

    /// Connect using env (`PHOTON_FLUVIO_*` defaults via builder).
    ///
    /// # Errors
    ///
    /// Returns an error when env is missing or connection fails.
    pub async fn from_env() -> Result<Self> {
        Self::builder().from_env_defaults().build().await
    }

    /// Resolved adapter configuration.
    #[must_use]
    pub const fn config(&self) -> &FluvioConfig {
        &self.config
    }

    async fn connect_with_config(config: FluvioConfig) -> Result<Self> {
        warn_replication_settings(&config);
        let client = connect_fluvio(&config).await?;
        ensure_checkpoint_topic(&client, &config).await?;
        let checkpoint_store = CheckpointStore::connect(Arc::clone(&client), &config).await?;
        let pipeline = PublishPipeline::new(Arc::clone(&client), &config);
        Ok(Self {
            client,
            config,
            pipeline,
            checkpoint_store,
            ensured_topics: Arc::new(dashmap::DashSet::new()),
        })
    }

    async fn ensure_topic_once(&self, topic: &str) -> Result<()> {
        if self.ensured_topics.contains(topic) {
            return Ok(());
        }
        ensure_data_topic(&self.client, &self.config, topic).await?;
        self.ensured_topics.insert(topic.to_string());
        Ok(())
    }
}

impl FluvioStoragePortBuilder {
    /// Connect and return a configured [`FluvioStoragePort`].
    ///
    /// # Errors
    ///
    /// Returns an error when configuration or connection fails.
    pub async fn build(self) -> Result<FluvioStoragePort> {
        let config = self.resolve()?;
        FluvioStoragePort::connect_with_config(config).await
    }
}

#[async_trait]
impl StoragePort for FluvioStoragePort {
    fn capabilities(&self) -> StorageCapabilities {
        StorageCapabilities::broker("fluvio")
    }

    async fn append(
        &self,
        topic_name: &str,
        topic_key: Option<&str>,
        actor_json: Value,
        payload_json: Value,
    ) -> Result<Event> {
        let event = Event {
            event_id: Uuid::new_v4().to_string(),
            topic_name: topic_name.to_string(),
            topic_key: topic_key.map(String::from),
            seq: 0,
            actor_json,
            payload_json,
            created_at: Utc::now(),
        };
        let (mut plain, sealed) = seal_event_for_storage(&self.config.crypto, event)?;

        let routing = publish_routing_key(topic_key, &plain.event_id);
        let shard = pick_shard(&routing, self.config.topic_shards);
        let fluvio_topic = fluvio_topic_for(&self.config, shard, topic_name);
        self.ensure_topic_once(&fluvio_topic).await?;

        let offset_seq = self.pipeline.publish(&fluvio_topic, &sealed).await?;

        if self.config.replay_cursor == ReplayCursor::StreamSeq {
            if let Some(seq) = offset_seq {
                plain.seq = if self.config.is_sharded() {
                    composite_seq(shard, u64::try_from(seq.max(0)).unwrap_or(0))
                } else {
                    seq
                };
            }
        }

        Ok(plain)
    }

    fn subscribe(
        &self,
        topic_name: String,
        topic_key_filter: Option<String>,
        after_seq: Option<i64>,
    ) -> Pin<Box<dyn Stream<Item = Result<Event>> + Send>> {
        let effective_after = if self.config.replay_cursor == ReplayCursor::TailOnly {
            None
        } else {
            after_seq
        };
        subscribe_stream(
            Arc::clone(&self.client),
            self.config.clone(),
            self.checkpoint_store.clone(),
            Arc::clone(&self.ensured_topics),
            topic_name,
            topic_key_filter,
            effective_after,
        )
    }

    async fn get_event(&self, _event_id: &str) -> Result<Option<Event>> {
        Ok(None)
    }

    async fn list_by_topic(
        &self,
        _topic_name: &str,
        _topic_key: Option<&str>,
        _after_seq: Option<i64>,
        _limit: usize,
    ) -> Result<Vec<Event>> {
        Ok(Vec::new())
    }

    async fn list_recent(&self, _limit: usize) -> Result<Vec<Event>> {
        Ok(Vec::new())
    }

    #[allow(clippy::unused_async)] // `StoragePort` async trait; load is an in-memory cache read
    async fn load_checkpoint(
        &self,
        subscription_name: &str,
        topic_name: &str,
        topic_key: Option<&str>,
    ) -> Result<Option<i64>> {
        if self.config.replay_cursor == ReplayCursor::TailOnly {
            return Ok(None);
        }
        self.checkpoint_store
            .load(subscription_name, topic_name, topic_key)
    }

    async fn commit_checkpoint(
        &self,
        subscription_name: &str,
        topic_name: &str,
        topic_key: Option<&str>,
        last_seq: i64,
    ) -> Result<()> {
        if self.config.replay_cursor == ReplayCursor::TailOnly {
            return Ok(());
        }
        self.checkpoint_store
            .commit(subscription_name, topic_name, topic_key, last_seq)
            .await
    }
}
