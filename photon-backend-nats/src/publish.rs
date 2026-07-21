//! Pipelined `JetStream` publish with optional ack wait.

use std::sync::Arc;

use async_nats::jetstream::context::PublishAckFuture;
use bytes::Bytes;
use photon_backend::{PhotonError, Result};
use tokio::sync::Semaphore;

use crate::config::NatsConfig;

/// Limits concurrent in-flight `JetStream` publishes for one port.
pub struct PublishPipeline {
    semaphore: Arc<Semaphore>,
    sync_ack: bool,
}

impl PublishPipeline {
    #[must_use]
    pub fn new(config: &NatsConfig) -> Self {
        let max = config.max_inflight.max(1) as usize;
        Self {
            semaphore: Arc::new(Semaphore::new(max)),
            sync_ack: config.sync_ack,
        }
    }

    /// Publish and optionally await `JetStream` ack; returns stream sequence when acked.
    ///
    /// # Errors
    ///
    /// Returns an error when publish or ack fails.
    pub async fn publish(
        &self,
        jetstream: &async_nats::jetstream::Context,
        subject: String,
        headers: Option<async_nats::HeaderMap>,
        body: Bytes,
    ) -> Result<Option<u64>> {
        let permit = self
            .semaphore
            .clone()
            .acquire_owned()
            .await
            .map_err(|e| PhotonError::caused("nats publish pipeline:", e))?;

        let ack_future: PublishAckFuture = if let Some(headers) = headers {
            jetstream
                .publish_with_headers(subject.clone(), headers, body)
                .await
                .map_err(|e| PhotonError::caused(format!("nats publish {subject}"), e))?
        } else {
            jetstream
                .publish(subject.clone(), body)
                .await
                .map_err(|e| PhotonError::caused(format!("nats publish {subject}"), e))?
        };

        if self.sync_ack {
            let ack = ack_future
                .await
                .map_err(|e| PhotonError::caused(format!("nats publish ack {subject}"), e))?;
            drop(permit);
            Ok(Some(ack.sequence))
        } else {
            tokio::spawn(async move {
                let _ = ack_future.await;
                drop(permit);
            });
            Ok(None)
        }
    }
}
