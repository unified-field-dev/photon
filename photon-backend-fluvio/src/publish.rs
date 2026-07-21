//! Pipelined Fluvio produce with optional ack wait.

use std::sync::Arc;

use dashmap::DashMap;
use fluvio::TopicProducerPool;
use photon_backend::{PhotonError, Result};
use tokio::sync::Semaphore;

use crate::config::FluvioConfig;
use crate::connect::SharedClient;
use crate::message::encode_event_record;

/// Limits concurrent in-flight Fluvio produces for one port.
#[derive(Clone)]
pub struct PublishPipeline {
    client: SharedClient,
    producers: DashMap<String, Arc<TopicProducerPool>>,
    semaphore: Arc<Semaphore>,
    sync_ack: bool,
}

impl PublishPipeline {
    /// Create a publish pipeline from resolved config.
    #[must_use]
    pub fn new(client: SharedClient, config: &FluvioConfig) -> Self {
        let max = config.max_inflight.max(1) as usize;
        Self {
            client,
            producers: DashMap::new(),
            semaphore: Arc::new(Semaphore::new(max)),
            sync_ack: config.sync_ack,
        }
    }

    async fn producer_for(&self, topic: &str) -> Result<Arc<TopicProducerPool>> {
        if let Some(existing) = self.producers.get(topic) {
            return Ok(Arc::clone(&existing));
        }
        // Topic create can succeed before spu_pool.topic_exists; retry briefly.
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(15);
        let mut last_err = None;
        while tokio::time::Instant::now() < deadline {
            match self.client.topic_producer(topic.to_string()).await {
                Ok(producer) => {
                    let shared = Arc::new(producer);
                    self.producers
                        .insert(topic.to_string(), Arc::clone(&shared));
                    return Ok(shared);
                }
                Err(e) => {
                    let msg = e.to_string();
                    if msg.contains("Topic not found") {
                        last_err = Some(e);
                        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                        continue;
                    }
                    return Err(PhotonError::Internal(format!(
                        "fluvio topic producer {topic}: {e}"
                    )));
                }
            }
        }
        Err(PhotonError::Internal(format!(
            "fluvio topic producer {topic}: {}",
            last_err.map_or_else(|| "Topic not found".into(), |e| e.to_string())
        )))
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
            .map_err(|e| PhotonError::caused("fluvio publish pipeline:", e))?;

        let producer = self.producer_for(topic).await?;
        let (key, body) = encode_event_record(event)?;

        if self.sync_ack {
            let output = producer
                .send(key, body)
                .await
                .map_err(|e| PhotonError::caused(format!("fluvio publish {topic}"), e))?;
            let metadata = output
                .wait()
                .await
                .map_err(|e| PhotonError::caused(format!("fluvio publish ack {topic}"), e))?;
            drop(permit);
            Ok(Some(metadata.offset().saturating_add(1)))
        } else {
            tokio::spawn(async move {
                if let Ok(output) = producer.send(key, body).await {
                    let _ = output.wait().await;
                }
                drop(permit);
            });
            Ok(None)
        }
    }
}
