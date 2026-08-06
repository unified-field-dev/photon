//! Serde DTOs for host ops introspection (JSON mapping in product packages).

use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Topic catalog entry from the compile-time [`TopicRegistry`](photon_backend::TopicRegistry).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AdminTopicSummary {
    /// Stable topic name (e.g. `user.notifications`).
    pub topic_name: String,
    /// JSON payload field used as partition key, when keyed.
    pub keyed_by: Option<String>,
    /// Parsed topic schema JSON.
    pub schema_json: Value,
}

/// Inventory entry for a `#[photon::subscribe]` handler.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AdminHandlerSummary {
    /// Topic this handler listens on.
    pub topic_name: String,
    /// Durable subscription name (`None` for consumer-group handlers).
    pub subscription_name: Option<String>,
    /// Consumer group id when load-balanced (`None` for durable handlers).
    pub consumer_group: Option<String>,
    /// Stable registry key from inventory.
    pub registry_key: String,
    /// Delivery mode: `"durable"` or `"consumer_group"`.
    pub mode: String,
}

/// Last committed sequence for a subscription/topic partition.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AdminCheckpointSummary {
    /// Subscription or consumer-group id used for checkpoint storage.
    pub subscription_name: String,
    /// Topic name.
    pub topic_name: String,
    /// Optional partition or virtual-shard key.
    pub topic_key: Option<String>,
    /// Last committed sequence, if a checkpoint exists.
    pub last_seq: Option<i64>,
}

/// Storage adapter capabilities surfaced for admin UIs.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AdminBackendSummary {
    /// Stable telemetry label (`mem`, `nats`, …).
    pub telemetry_label: String,
    /// Whether [`Photon::get_event`](crate::Photon::get_event) is supported.
    pub supports_get_event: bool,
    /// Whether list/browse APIs (`list_events_by_topic` / `list_recent_events`) are supported.
    pub supports_list_events: bool,
    /// Maximum replay window in seconds for bounded retention adapters.
    pub max_replay_window_secs: Option<u64>,
}

/// Point-in-time ops introspection snapshot (read-only).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AdminSnapshot {
    /// Installed backend capabilities.
    pub backend: AdminBackendSummary,
    /// Registered topics from `#[photon::topic]`.
    pub topics: Vec<AdminTopicSummary>,
    /// Registered handlers from `#[photon::subscribe]`.
    pub handlers: Vec<AdminHandlerSummary>,
    /// Checkpoint cursors for inventory handlers.
    pub checkpoints: Vec<AdminCheckpointSummary>,
}
