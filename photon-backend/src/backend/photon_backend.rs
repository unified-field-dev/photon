//! Object-safe backend trait for Photon publish/subscribe runtimes.
//!
//! Prefer wiring a [`crate::storage::StoragePort`] through
//! [`PhotonBuilder::storage_port`](https://docs.rs/uf-photon/latest/photon/struct.PhotonBuilder.html#method.storage_port).
//! Implement this trait only for custom delivery stacks, then install via
//! [`PhotonBuilder::backend_with_context`](https://docs.rs/uf-photon/latest/photon/struct.PhotonBuilder.html#method.backend_with_context).
//!
//! See also: [`crate::storage::StoragePort`], [`crate::GenericPhotonBackend`].

use std::pin::Pin;

use async_trait::async_trait;
use futures::stream::Stream;
use serde_json::Value;

use crate::backend::BackendCapabilities;
use crate::error::Result;
use crate::models::Event;
use crate::registry::TopicRegistry;

/// Backend for publish/subscribe delivery and checkpoint persistence.
///
/// Most hosts never implement this — use a storage adapter and the default
/// [`GenericPhotonBackend`](crate::GenericPhotonBackend). Custom delivery example:
///
/// ```rust,ignore
/// use std::sync::Arc;
/// use photon_backend::{BackendContext, PhotonBackend};
/// use photon_runtime::Photon;
///
/// let _photon = Photon::builder()
///     .backend_with_context(|ctx: BackendContext| {
///         // return Ok(Arc::new(MyBackend::new(ctx)) as Arc<dyn PhotonBackend>)
///         photon_backend::EmbeddedBackend::install_mem(ctx)
///     })
///     .auto_registry()
///     .build()?;
/// ```
#[async_trait]
pub trait PhotonBackend: Send + Sync {
    /// Stable telemetry label for ops metrics (e.g. `"mem"`, `"nats"`).
    fn telemetry_label(&self) -> &'static str {
        "custom"
    }

    /// Adapter capabilities (replay window, get-by-id support, …).
    fn capabilities(&self) -> BackendCapabilities {
        BackendCapabilities::mem()
    }

    /// Append an event and return its event id.
    ///
    /// # Contract
    ///
    /// - Returns the stable `event_id` assigned by the underlying storage port.
    /// - Ordering and dedupe are per `(topic_name, topic_key)` partition, not global.
    async fn publish(
        &self,
        topic_name: &str,
        topic_key: Option<&str>,
        actor_json: Value,
        payload_json: Value,
    ) -> Result<String>;

    /// Stream events for a topic partition, optionally replaying after `after_seq`.
    ///
    /// # Contract
    ///
    /// - Same semantics as [`crate::storage::StoragePort::subscribe`].
    fn subscribe(
        &self,
        topic_name: String,
        topic_key_filter: Option<String>,
        after_seq: Option<i64>,
    ) -> Pin<Box<dyn Stream<Item = Result<Event>> + Send>>;

    /// Load a single event by id.
    ///
    /// # Contract
    ///
    /// - Returns `None` when unknown or truncated; see backend [`BackendCapabilities`].
    async fn get_event(&self, event_id: &str) -> Result<Option<Event>>;

    /// Inventory-discovered topic descriptors.
    fn registry(&self) -> &TopicRegistry;

    /// Load the last committed checkpoint seq for a subscription partition.
    ///
    /// # Contract
    ///
    /// - Returns `None` when no checkpoint exists.
    async fn get_checkpoint_seq(
        &self,
        subscription_name: &str,
        topic_name: &str,
        topic_key: Option<&str>,
    ) -> Result<Option<i64>>;

    /// Persist the high-water checkpoint seq for a subscription partition.
    ///
    /// # Contract
    ///
    /// - `last_seq` must not regress; coalesced writes may batch behind the scenes.
    async fn set_checkpoint(
        &self,
        subscription_name: &str,
        topic_name: &str,
        topic_key: Option<&str>,
        last_seq: i64,
    ) -> Result<()>;
}
