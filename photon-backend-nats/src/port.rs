//! NATS `JetStream` [`StoragePort`] — live adapter when `PHOTON_NATS_URL` is set.

use std::pin::Pin;

use async_trait::async_trait;
use chrono::Utc;
use futures::stream::Stream;
use photon_backend::models::Event;
use photon_backend::{PhotonError, Result, StorageCapabilities, StoragePort};
use serde_json::Value;
use uuid::Uuid;

use crate::checkpoint::CheckpointStore;
use crate::config::{NatsConfig, NatsStoragePortBuilder, ReplayCursor};
use crate::connect::connect_nats;
use crate::consumer::subscribe_push;
use crate::message::encode_event;
use crate::publish::PublishPipeline;
use crate::stream::ensure_streams;
use crate::stream_shard::{composite_seq, photon_subject_for, pick_shard, publish_routing_key};

/// Read NATS URL from the environment.
///
/// # Errors
///
/// Returns an error when `PHOTON_NATS_URL` is unset.
pub fn nats_url_from_env() -> Result<String> {
    std::env::var(crate::config::URL_ENV).map_err(|_| {
        PhotonError::Internal(format!(
            "{} not set for nats storage adapter",
            crate::config::URL_ENV
        ))
    })
}

/// NATS JetStream-backed storage port.
pub struct NatsStoragePort {
    jetstream: async_nats::jetstream::Context,
    config: NatsConfig,
    pipeline: PublishPipeline,
    checkpoint_store: CheckpointStore,
}

impl NatsStoragePort {
    /// Start a builder for explicit host wiring.
    #[must_use]
    pub fn builder() -> NatsStoragePortBuilder {
        NatsStoragePortBuilder::new()
    }

    /// Connect using env (`PHOTON_NATS_*` defaults via builder).
    ///
    /// # Errors
    ///
    /// Returns an error when env is missing or connection fails.
    pub async fn from_env() -> Result<Self> {
        Self::builder().from_env_defaults().build().await
    }

    /// Connect to NATS with explicit URL and stream name (legacy; uses env defaults for firehose options).
    ///
    /// # Errors
    ///
    /// Returns an error when connection or stream setup fails.
    pub async fn connect(url: &str, stream_name: &str) -> Result<Self> {
        Self::builder()
            .url(url)
            .stream_name(stream_name)
            .from_env_defaults()
            .build()
            .await
    }

    /// Resolved adapter configuration.
    #[must_use]
    pub const fn config(&self) -> &NatsConfig {
        &self.config
    }

    async fn connect_with_config(config: NatsConfig) -> Result<Self> {
        let client = connect_nats(&config.url).await?;
        let jetstream = async_nats::jetstream::new(client);
        ensure_streams(&jetstream, &config).await?;
        let checkpoint_store = CheckpointStore::connect(&jetstream, &config).await?;
        let pipeline = PublishPipeline::new(&config);
        Ok(Self {
            jetstream,
            config,
            pipeline,
            checkpoint_store,
        })
    }
}

impl NatsStoragePortBuilder {
    /// Connect and return a configured [`NatsStoragePort`].
    ///
    /// # Errors
    ///
    /// Returns an error when configuration or connection fails.
    pub async fn build(self) -> Result<NatsStoragePort> {
        let config = self.resolve()?;
        NatsStoragePort::connect_with_config(config).await
    }
}

#[async_trait]
impl StoragePort for NatsStoragePort {
    fn capabilities(&self) -> StorageCapabilities {
        StorageCapabilities::broker("nats")
    }

    async fn append(
        &self,
        topic_name: &str,
        topic_key: Option<&str>,
        actor_json: Value,
        payload_json: Value,
    ) -> Result<Event> {
        let _ = self.config.crypto.encrypt(&actor_json, &payload_json)?;
        let mut event = Event {
            event_id: Uuid::new_v4().to_string(),
            topic_name: topic_name.to_string(),
            topic_key: topic_key.map(String::from),
            seq: 0,
            actor_json,
            payload_json,
            created_at: Utc::now(),
        };

        let routing = publish_routing_key(topic_key, &event.event_id);
        let shard = pick_shard(&routing, self.config.stream_shards);
        let subject = photon_subject_for(shard, self.config.stream_shards, topic_name);
        let (headers, body) = encode_event(&event)?;
        let stream_seq = self
            .pipeline
            .publish(&self.jetstream, subject, Some(headers), body)
            .await?;

        if self.config.replay_cursor == ReplayCursor::StreamSeq {
            if let Some(seq) = stream_seq {
                let local = i64::try_from(seq).unwrap_or(i64::MAX);
                event.seq = if self.config.is_sharded() {
                    composite_seq(shard, seq)
                } else {
                    local
                };
            }
        }

        Ok(event)
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
        subscribe_push(
            self.jetstream.clone(),
            self.config.clone(),
            self.checkpoint_store.clone(),
            topic_name,
            topic_key_filter,
            effective_after,
        )
    }

    async fn get_event(&self, _event_id: &str) -> Result<Option<Event>> {
        Ok(None)
    }

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
            .await
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
