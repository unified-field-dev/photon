//! Object-safe storage port for publish, subscribe, checkpoints, and optional retention.
//!
//! **Application authors** usually select a built-in adapter (`mem`, `sqlite`, `nats`, `kafka`, `fluvio`)
//! via [`PhotonBuilder::storage_port`](https://docs.rs/uf-photon/latest/photon/struct.PhotonBuilder.html#method.storage_port)
//! — you rarely implement this trait directly.
//! **Adapter authors** implement [`StoragePort`] in a storage crate and wire it at boot.
//!
//! Reference in-process implementation: [`InProcStoragePort`]. Runnable host example:
//! `cargo run -p uf-photon --example embedded_mem --features runtime,mem`.
//!
//! Getting started: [Mode 1](https://docs.rs/uf-photon/latest/photon/#mode-1--embedded-one-binary),
//! [Mode 2](https://docs.rs/uf-photon/latest/photon/#mode-2--brokered-publisher--worker-binaries).
//!
//! See also: [`crate::checkpoint`], [`crate::retention`], [`crate::backend`].

use std::pin::Pin;
use std::time::Duration;

use async_trait::async_trait;
use futures::stream::Stream;
use serde_json::Value;

use crate::error::Result;
use crate::models::Event;

/// Capabilities advertised by a storage port (surfaced via [`crate::backend::GenericPhotonBackend`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StorageCapabilities {
    /// Whether [`StoragePort::get_event`] is supported.
    pub supports_get_event: bool,
    /// Maximum replay window for bounded retention adapters.
    pub max_replay_window: Option<Duration>,
    /// Stable telemetry / bench label (`mem`, `nats`, …).
    pub telemetry_label: &'static str,
}

impl StorageCapabilities {
    /// In-process embedded tier defaults.
    #[must_use]
    pub const fn mem() -> Self {
        Self {
            supports_get_event: true,
            max_replay_window: None,
            telemetry_label: "mem",
        }
    }

    /// Embedded `SQLite` tier defaults.
    #[must_use]
    pub const fn sqlite() -> Self {
        Self {
            supports_get_event: true,
            max_replay_window: None,
            telemetry_label: "sqlite",
        }
    }

    /// Broker tier defaults (~15 min replay window).
    #[must_use]
    pub const fn broker(label: &'static str) -> Self {
        Self {
            supports_get_event: false,
            max_replay_window: Some(Duration::from_mins(15)),
            telemetry_label: label,
        }
    }
}

/// Storage adapter contract — append, subscribe, checkpoints, and optional retention.
///
/// Each method documents its behavior under **Contract**. Built-in implementations:
///
/// | Adapter | Type | Crate |
/// |---------|------|-------|
/// | In-process | [`InProcStoragePort`](super::InProcStoragePort) | `photon-backend` (`mem`) |
/// | `SQLite` | `SqliteStoragePort` | `photon-backend-sqlite` |
/// | NATS `JetStream` | `NatsStoragePort` | `photon-backend-nats` |
/// | Kafka | `KafkaStoragePort` | `photon-backend-kafka` |
/// | Fluvio | `FluvioStoragePort` | `photon-backend-fluvio` |
///
/// # Example (use a built-in port)
///
/// ```rust,no_run
/// use std::sync::Arc;
///
/// use photon_backend::{InProcStoragePort, StoragePort, TransportCrypto};
///
/// # fn main() -> photon_backend::Result<()> {
/// let port: Arc<dyn StoragePort> = Arc::new(InProcStoragePort::new(
///     TransportCrypto::from_env()?,
/// ));
/// let _caps = port.capabilities();
/// # Ok(())
/// # }
/// ```
///
/// Install at boot via the facade [`PhotonBuilder`](https://docs.rs/uf-photon/latest/photon/struct.PhotonBuilder.html).
/// Host walkthrough: [Integrating the host](https://docs.rs/uf-photon/latest/photon/#integrating-the-host).
#[async_trait]
pub trait StoragePort: Send + Sync {
    /// Adapter capabilities for contract tests and telemetry.
    fn capabilities(&self) -> StorageCapabilities;

    /// Append one event to a topic partition.
    ///
    /// # Contract
    ///
    /// - Assigns a monotonically increasing `seq` per `(topic_name, topic_key)` partition.
    /// - Returns the persisted [`Event`] including stable `event_id`.
    /// - Payload and actor JSON are opaque to the adapter (encryption happens above the port).
    async fn append(
        &self,
        topic_name: &str,
        topic_key: Option<&str>,
        actor_json: Value,
        payload_json: Value,
    ) -> Result<Event>;

    /// Stream events for a topic partition, optionally replaying after `after_seq`.
    ///
    /// # Contract
    ///
    /// - When `after_seq` is set, only events with `seq > after_seq` are yielded.
    /// - When `topic_key_filter` is set, only matching partition keys are delivered.
    /// - The stream runs until dropped or an error; live adapters may block for new events.
    fn subscribe(
        &self,
        topic_name: String,
        topic_key_filter: Option<String>,
        after_seq: Option<i64>,
    ) -> Pin<Box<dyn Stream<Item = Result<Event>> + Send>>;

    /// Point lookup by event id (optional — see [`StorageCapabilities::supports_get_event`]).
    ///
    /// # Contract
    ///
    /// - Returns `None` when the id is unknown or the event was truncated by retention.
    async fn get_event(&self, event_id: &str) -> Result<Option<Event>>;

    /// Load durable subscription high-water seq.
    ///
    /// # Contract
    ///
    /// - Returns `None` when no checkpoint exists (caller chooses replay start policy).
    async fn load_checkpoint(
        &self,
        subscription_name: &str,
        topic_name: &str,
        topic_key: Option<&str>,
    ) -> Result<Option<i64>>;

    /// Persist durable subscription high-water seq.
    ///
    /// # Contract
    ///
    /// - `last_seq` is monotonic per subscription partition; adapters may reject regressions.
    async fn commit_checkpoint(
        &self,
        subscription_name: &str,
        topic_name: &str,
        topic_key: Option<&str>,
        last_seq: i64,
    ) -> Result<()>;

    /// Trim events before `truncate_bound` for a partition (no-op when unsupported).
    ///
    /// # Contract
    ///
    /// - Default implementation returns `Ok(0)` without removing events.
    /// - Supporting adapters remove events with `seq < truncate_bound`.
    async fn truncate_before(
        &self,
        topic_name: &str,
        topic_key: Option<&str>,
        truncate_bound: i64,
    ) -> Result<u64> {
        let _ = (topic_name, topic_key, truncate_bound);
        Ok(0)
    }

    /// Last delivered seq pin for retention watermarks (optional).
    ///
    /// # Contract
    ///
    /// - Default returns `None` (no delivery-layer retention pin).
    async fn delivery_seq_pin(&self, topic_name: &str, topic_key: Option<&str>) -> Option<i64> {
        let _ = (topic_name, topic_key);
        None
    }
}
