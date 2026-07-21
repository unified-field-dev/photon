//! L0 [`PhotonBackend`] decorator for publish telemetry.

use std::pin::Pin;
use std::sync::Arc;

use async_trait::async_trait;
use futures::stream::Stream;
use serde_json::Value;

use crate::backend::PhotonBackend;
use crate::error::Result;
use crate::models::Event;
use crate::registry::TopicRegistry;

use super::metrics;

/// Wraps an inner backend and emits `photon_publishes` on successful publish.
pub struct InstrumentedPhotonBackend {
    inner: Arc<dyn PhotonBackend>,
    backend_label: &'static str,
}

impl InstrumentedPhotonBackend {
    /// Wrap `inner`, using its [`PhotonBackend::telemetry_label`] for metrics.
    pub fn new(inner: Arc<dyn PhotonBackend>) -> Self {
        let backend_label = inner.telemetry_label();
        Self {
            inner,
            backend_label,
        }
    }
}

/// Wrap `inner` with [`InstrumentedPhotonBackend`].
pub fn wrap_backend(inner: Arc<dyn PhotonBackend>) -> Arc<dyn PhotonBackend> {
    Arc::new(InstrumentedPhotonBackend::new(inner))
}

#[async_trait]
impl PhotonBackend for InstrumentedPhotonBackend {
    fn telemetry_label(&self) -> &'static str {
        self.backend_label
    }

    #[tracing::instrument(
        name = "photon.publish",
        skip(self, actor_json, payload_json),
        fields(backend = self.backend_label, topic = topic_name, topic_key = ?topic_key)
    )]
    async fn publish(
        &self,
        topic_name: &str,
        topic_key: Option<&str>,
        actor_json: Value,
        payload_json: Value,
    ) -> Result<String> {
        match self
            .inner
            .publish(topic_name, topic_key, actor_json, payload_json)
            .await
        {
            Ok(id) => {
                metrics::record_publish(topic_name, self.backend_label);
                Ok(id)
            }
            Err(e) => {
                metrics::record_publish_error(topic_name, self.backend_label);
                tracing::warn!(error = %e, "publish failed");
                Err(e)
            }
        }
    }

    fn subscribe(
        &self,
        topic_name: String,
        topic_key_filter: Option<String>,
        after_seq: Option<i64>,
    ) -> Pin<Box<dyn Stream<Item = Result<Event>> + Send>> {
        self.inner
            .subscribe(topic_name, topic_key_filter, after_seq)
    }

    async fn get_event(&self, event_id: &str) -> Result<Option<Event>> {
        self.inner.get_event(event_id).await
    }

    fn registry(&self) -> &TopicRegistry {
        self.inner.registry()
    }

    async fn get_checkpoint_seq(
        &self,
        subscription_name: &str,
        topic_name: &str,
        topic_key: Option<&str>,
    ) -> Result<Option<i64>> {
        self.inner
            .get_checkpoint_seq(subscription_name, topic_name, topic_key)
            .await
    }

    async fn set_checkpoint(
        &self,
        subscription_name: &str,
        topic_name: &str,
        topic_key: Option<&str>,
        last_seq: i64,
    ) -> Result<()> {
        self.inner
            .set_checkpoint(subscription_name, topic_name, topic_key, last_seq)
            .await
    }
}
