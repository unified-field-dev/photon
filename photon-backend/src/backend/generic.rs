//! Single [`PhotonBackend`] implementation wrapping any [`StoragePort`].

use std::pin::Pin;
use std::sync::Arc;

use async_trait::async_trait;
use futures::stream::Stream;
use serde_json::Value;

use super::capabilities::BackendCapabilities;
use super::context::BackendContext;
use super::photon_backend::PhotonBackend;
use crate::error::Result;
use crate::models::Event;
use crate::publish_routing::resolve_publish_target;
use crate::registry::TopicRegistry;
use crate::storage::{InProcStoragePort, StoragePort};

/// Unified backend — all storage adapters install this wrapping their port.
///
/// Prefer selecting a [`StoragePort`](crate::storage::StoragePort) and letting
/// [`PhotonBuilder`](https://docs.rs/uf-photon/latest/photon/struct.PhotonBuilder.html) install this
/// automatically. See also the [`EmbeddedBackend`](crate::EmbeddedBackend) alias.
pub struct GenericPhotonBackend {
    port: Arc<dyn StoragePort>,
    registry: TopicRegistry,
    capabilities: BackendCapabilities,
}

impl GenericPhotonBackend {
    /// Wrap an existing storage port and topic registry.
    #[must_use]
    pub fn new(port: Arc<dyn StoragePort>, registry: TopicRegistry) -> Self {
        let capabilities = port.capabilities();
        Self {
            port,
            registry,
            capabilities,
        }
    }

    /// Capabilities for contract tests and host introspection.
    #[must_use]
    pub const fn capabilities(&self) -> BackendCapabilities {
        self.capabilities
    }

    /// Install the default `mem` tier from builder context.
    ///
    /// Loads `PHOTON_TRANSPORT_KEY` via [`TransportCrypto::from_env`](crate::event::TransportCrypto::from_env).
    ///
    /// # Errors
    ///
    /// Returns an error if the transport key cannot be loaded from the environment.
    pub fn install_mem(ctx: BackendContext) -> Result<Arc<dyn PhotonBackend>> {
        let port = Arc::new(InProcStoragePort::new(
            crate::event::TransportCrypto::from_env()?,
        ));
        Ok(Arc::new(Self::new(port, ctx.registry)))
    }

    /// Install a custom storage port from builder context.
    ///
    /// # Errors
    ///
    /// Returns an error if the operation fails.
    pub fn install_with_port(
        ctx: BackendContext,
        port: Arc<dyn StoragePort>,
    ) -> Result<Arc<dyn PhotonBackend>> {
        Ok(Arc::new(Self::new(port, ctx.registry)))
    }
}

#[async_trait]
impl PhotonBackend for GenericPhotonBackend {
    fn telemetry_label(&self) -> &'static str {
        self.capabilities.telemetry_label
    }

    fn capabilities(&self) -> BackendCapabilities {
        self.capabilities
    }

    async fn publish(
        &self,
        topic_name: &str,
        topic_key: Option<&str>,
        actor_json: Value,
        payload_json: Value,
    ) -> Result<String> {
        let target = resolve_publish_target(
            &self.registry,
            topic_name,
            topic_key,
            &payload_json,
        );
        let event = self
            .port
            .append(
                topic_name,
                target.topic_key.as_deref(),
                actor_json,
                payload_json,
            )
            .await?;
        Ok(event.event_id)
    }

    fn subscribe(
        &self,
        topic_name: String,
        topic_key_filter: Option<String>,
        after_seq: Option<i64>,
    ) -> Pin<Box<dyn Stream<Item = Result<Event>> + Send>> {
        self.port
            .subscribe(topic_name, topic_key_filter, after_seq)
    }

    async fn get_event(&self, event_id: &str) -> Result<Option<Event>> {
        if !self.capabilities.supports_get_event {
            return Ok(None);
        }
        self.port.get_event(event_id).await
    }

    fn registry(&self) -> &TopicRegistry {
        &self.registry
    }

    async fn get_checkpoint_seq(
        &self,
        subscription_name: &str,
        topic_name: &str,
        topic_key: Option<&str>,
    ) -> Result<Option<i64>> {
        self.port
            .load_checkpoint(subscription_name, topic_name, topic_key)
            .await
    }

    async fn set_checkpoint(
        &self,
        subscription_name: &str,
        topic_name: &str,
        topic_key: Option<&str>,
        last_seq: i64,
    ) -> Result<()> {
        self.port
            .commit_checkpoint(subscription_name, topic_name, topic_key, last_seq)
            .await
    }
}
