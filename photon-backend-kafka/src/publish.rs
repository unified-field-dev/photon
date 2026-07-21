//! Pipelined Kafka produce with optional ack wait.

use std::sync::Arc;

use chrono::Utc;
use photon_backend::{PhotonError, Result};
use rskafka::client::partition::{Compression, OffsetAt, UnknownTopicHandling};
use tokio::sync::Semaphore;

use crate::config::KafkaConfig;
use crate::connect::SharedClient;
use crate::message::encode_event_record;

/// Limits concurrent in-flight Kafka produces for one port.
pub struct PublishPipeline {
    client: SharedClient,
    semaphore: Arc<Semaphore>,
    sync_ack: bool,
}

impl PublishPipeline {
    /// Create a publish pipeline from resolved config.
    #[must_use]
    pub fn new(client: SharedClient, config: &KafkaConfig) -> Self {
        let max = config.max_inflight.max(1) as usize;
        Self {
            client,
            semaphore: Arc::new(Semaphore::new(max)),
            sync_ack: config.sync_ack,
        }
    }

    /// Publish and optionally await broker ack; returns offset+1 when acked.
    ///
    /// # Errors
    ///
    /// Returns an error when publish or ack fails.
    pub async fn publish(
        &self,
        topic: &str,
        event: &photon_backend::models::Event,
    ) -> Result<Option<i64>> {
        let permit = self
            .semaphore
            .clone()
            .acquire_owned()
            .await
            .map_err(|e| PhotonError::caused("kafka publish pipeline:", e))?;

        let partition_client = self
            .client
            .partition_client(topic.to_string(), 0, UnknownTopicHandling::Retry)
            .await
            .map_err(|e| PhotonError::caused(format!("kafka partition client {topic}"), e))?;

        let (record, _) = encode_event_record(event, Utc::now())?;

        if self.sync_ack {
            partition_client
                .produce(vec![record], Compression::default())
                .await
                .map_err(|e| PhotonError::caused(format!("kafka publish {topic}"), e))?;
            let high_watermark = partition_client
                .get_offset(OffsetAt::Latest)
                .await
                .map_err(|e| PhotonError::caused(format!("kafka publish watermark {topic}"), e))?;
            drop(permit);
            Ok(Some(high_watermark))
        } else {
            tokio::spawn(async move {
                let _ = partition_client
                    .produce(vec![record], Compression::default())
                    .await;
                drop(permit);
            });
            Ok(None)
        }
    }
}
