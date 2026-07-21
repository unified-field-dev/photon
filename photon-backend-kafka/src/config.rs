//! Builder and resolved configuration for the Kafka storage adapter.

use std::time::Duration;

use photon_backend::{PhotonError, Result, TransportCrypto};

use crate::replicas::replicas_from_env;
use crate::retention::retention_from_env;

/// Environment variable for Kafka bootstrap brokers.
pub const BROKERS_ENV: &str = "PHOTON_KAFKA_BROKERS";

/// Environment variable for topic name prefix.
pub const PREFIX_ENV: &str = "PHOTON_KAFKA_TOPIC_PREFIX";

/// How durable replay and checkpoints map to Kafka offsets.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ReplayCursor {
    /// Broker offset+1 on publish ack; seek replay by seq.
    #[default]
    StreamSeq,
    /// Live tail only; checkpoints no-op; maximum publish throughput.
    TailOnly,
}

/// Resolved Kafka adapter settings (no env lookups at append time).
#[derive(Clone)]
pub struct KafkaConfig {
    /// Kafka bootstrap servers (comma-separated).
    pub brokers: String,
    /// Topic prefix for Photon events.
    pub topic_prefix: String,
    /// Topic retention / replay window.
    pub retention: Duration,
    /// Topic replication factor.
    pub replicas: i32,
    /// Payload envelope crypto.
    pub crypto: TransportCrypto,
    /// Replay and checkpoint semantics.
    pub replay_cursor: ReplayCursor,
    /// Await produce ack per message.
    pub sync_ack: bool,
    /// Max concurrent in-flight publishes.
    pub max_inflight: u32,
    /// Independent Kafka topics for ingress sharding (`1` = legacy single layout).
    pub topic_shards: u32,
}

impl KafkaConfig {
    /// Effective replication factor when creating shard topics.
    #[must_use]
    pub const fn effective_replicas(&self) -> i32 {
        if self.topic_shards > 1 {
            1
        } else {
            self.replicas
        }
    }

    /// Whether publishes route across multiple topic shards.
    #[must_use]
    pub const fn is_sharded(&self) -> bool {
        self.topic_shards > 1
    }

    /// Compact checkpoint topic name.
    #[must_use]
    pub fn checkpoint_topic(&self) -> String {
        format!("{}-checkpoints", self.topic_prefix)
    }
}

/// Environment variable for replay cursor mode.
pub const REPLAY_CURSOR_ENV: &str = "PHOTON_KAFKA_REPLAY_CURSOR";

/// Environment variable for synchronous publish ack (`1` / `0`).
pub const SYNC_ACK_ENV: &str = "PHOTON_KAFKA_SYNC_ACK";

/// Environment variable for max in-flight publishes per port.
pub const MAX_INFLIGHT_ENV: &str = "PHOTON_KAFKA_MAX_INFLIGHT";

/// Builder for [`super::port::KafkaStoragePort`].
///
/// **Configuration lives here.** Set builder methods explicitly; unset fields fall back to
/// `PHOTON_KAFKA_*` environment variables via [`from_env_defaults`](Self::from_env_defaults).
///
/// Use the **same** builder settings on every Mode 2 publisher and worker binary that shares a
/// cluster. Getting started:
/// [Mode 2](https://docs.rs/uf-photon/latest/photon/#mode-2--brokered-publisher--worker-binaries).
///
/// # Options
///
/// | Method / env | Default | Purpose |
/// |--------------|---------|---------|
/// | [`.brokers`](Self::brokers) / [`BROKERS_ENV`] | **required** | Kafka bootstrap servers (comma-separated). |
/// | [`.topic_prefix`](Self::topic_prefix) / [`PREFIX_ENV`] | `photon` | Topic name prefix. |
/// | [`.retention`](Self::retention) / [`RETENTION_ENV`](crate::retention::RETENTION_ENV) | `15m` | Topic retention window. |
/// | [`.replicas`](Self::replicas) / [`REPLICAS_ENV`](crate::replicas::REPLICAS_ENV) | `1` | Topic replication factor. |
/// | [`.replay_cursor`](Self::replay_cursor) / [`REPLAY_CURSOR_ENV`] | `stream_seq` | [`ReplayCursor::StreamSeq`] or [`ReplayCursor::TailOnly`]. |
/// | [`.sync_ack`](Self::sync_ack) / [`SYNC_ACK_ENV`] | `1` | Await produce ack (`0` = firehose). |
/// | [`.max_inflight`](Self::max_inflight) / [`MAX_INFLIGHT_ENV`] | `1` / `256` | Concurrent in-flight publishes (`256` when `sync_ack` off). |
/// | [`.topic_shards`](Self::topic_shards) / [`TOPIC_SHARDS_ENV`](crate::stream_shard::TOPIC_SHARDS_ENV) | `1` | Ingress shard count (`K>1` → `photon-s.{i}.{topic}`). |
///
/// # Examples
///
/// ## Publisher binary
///
/// Publish only — skip `start_executor` unless this process also runs handlers.
///
/// ```rust,ignore
/// use std::sync::Arc;
///
/// use photon_backend_kafka::{KafkaStoragePort, ReplayCursor};
/// use photon_runtime::Photon;
///
/// # async fn boot_publisher() -> photon_backend::Result<()> {
/// let port = Arc::new(
///     KafkaStoragePort::builder()
///         .from_env_defaults()
///         .replay_cursor(ReplayCursor::StreamSeq)
///         .sync_ack(true)
///         .build()
///         .await?,
/// );
/// let photon = Photon::builder()
///     .storage_port(port)
///     .auto_registry()
///     .build()?;
/// // EventType { … }.publish_on(&photon).await?;
/// # let _ = photon;
/// # Ok(())
/// # }
/// ```
///
/// ## Worker binary
///
/// Same port wiring as the publisher, plus `#[subscribe]` handlers and `start_executor`.
/// Start workers before publishers.
///
/// ```rust,ignore
/// use std::sync::Arc;
///
/// use photon_backend_kafka::{KafkaStoragePort, ReplayCursor};
/// use photon_core::JsonIdentityFactory;
/// use photon_runtime::Photon;
///
/// # async fn boot_worker() -> photon_backend::Result<()> {
/// let port = Arc::new(
///     KafkaStoragePort::builder()
///         .from_env_defaults()
///         .replay_cursor(ReplayCursor::StreamSeq)
///         .sync_ack(true)
///         .build()
///         .await?,
/// );
/// let photon = Photon::builder()
///     .storage_port(port)
///     .auto_registry()
///     .build()?;
/// photon.start_executor(Arc::new(JsonIdentityFactory))?;
/// # let _ = photon;
/// # Ok(())
/// # }
/// ```
///
/// See also: [`KafkaConfig`] (resolved snapshot), crate-level topic mapping in [`crate`](index.html).
#[derive(Default)]
pub struct KafkaStoragePortBuilder {
    brokers: Option<String>,
    topic_prefix: Option<String>,
    retention: Option<Duration>,
    replicas: Option<i32>,
    crypto: Option<TransportCrypto>,
    replay_cursor: Option<ReplayCursor>,
    sync_ack: Option<bool>,
    max_inflight: Option<u32>,
    topic_shards: Option<u32>,
}

impl KafkaStoragePortBuilder {
    /// Empty builder; set fields or call [`Self::from_env_defaults`].
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Fill unset fields from `PHOTON_KAFKA_*` environment variables.
    #[must_use]
    pub fn from_env_defaults(mut self) -> Self {
        if self.brokers.is_none() {
            self.brokers = std::env::var(BROKERS_ENV).ok();
        }
        if self.topic_prefix.is_none() {
            self.topic_prefix = Some(std::env::var(PREFIX_ENV).unwrap_or_else(|_| "photon".into()));
        }
        if self.retention.is_none() {
            self.retention = Some(retention_from_env());
        }
        if self.replicas.is_none() {
            self.replicas = Some(replicas_from_env());
        }
        if self.replay_cursor.is_none() {
            self.replay_cursor = Some(replay_cursor_from_env());
        }
        if self.sync_ack.is_none() {
            self.sync_ack = Some(sync_ack_from_env());
        }
        if self.max_inflight.is_none() {
            self.max_inflight = Some(max_inflight_from_env(self.sync_ack.unwrap_or(true)));
        }
        if self.topic_shards.is_none() {
            self.topic_shards = Some(crate::stream_shard::topic_shards_from_env());
        }
        self
    }

    /// Kafka bootstrap brokers.
    #[must_use]
    pub fn brokers(mut self, brokers: impl Into<String>) -> Self {
        self.brokers = Some(brokers.into());
        self
    }

    /// Topic prefix.
    #[must_use]
    pub fn topic_prefix(mut self, prefix: impl Into<String>) -> Self {
        self.topic_prefix = Some(prefix.into());
        self
    }

    /// Topic retention duration.
    #[must_use]
    pub const fn retention(mut self, retention: Duration) -> Self {
        self.retention = Some(retention);
        self
    }

    /// Topic replication factor.
    #[must_use]
    pub const fn replicas(mut self, replicas: i32) -> Self {
        self.replicas = Some(replicas);
        self
    }

    /// Transport crypto for publish path.
    #[must_use]
    pub fn crypto(mut self, crypto: TransportCrypto) -> Self {
        self.crypto = Some(crypto);
        self
    }

    /// Replay cursor mode.
    #[must_use]
    pub const fn replay_cursor(mut self, cursor: ReplayCursor) -> Self {
        self.replay_cursor = Some(cursor);
        self
    }

    /// Whether to await produce ack.
    #[must_use]
    pub const fn sync_ack(mut self, wait: bool) -> Self {
        self.sync_ack = Some(wait);
        self
    }

    /// Pipeline depth for concurrent publishes.
    #[must_use]
    pub fn max_inflight(mut self, n: u32) -> Self {
        self.max_inflight = Some(n.max(1));
        self
    }

    /// Topic shard count for ingress scaling (`1` = single shared layout).
    #[must_use]
    pub fn topic_shards(mut self, n: u32) -> Self {
        self.topic_shards = Some(n.clamp(1, crate::stream_shard::MAX_TOPIC_SHARDS));
        self
    }

    /// Resolve configuration.
    ///
    /// # Errors
    ///
    /// Returns an error when required fields (brokers) are missing.
    pub fn resolve(self) -> Result<KafkaConfig> {
        let builder = self.from_env_defaults();
        let brokers = builder.brokers.ok_or_else(|| {
            PhotonError::Internal(format!("{BROKERS_ENV} not set for kafka storage adapter"))
        })?;
        Ok(KafkaConfig {
            brokers,
            topic_prefix: builder.topic_prefix.unwrap_or_else(|| "photon".into()),
            retention: builder.retention.unwrap_or_else(retention_from_env),
            replicas: builder.replicas.unwrap_or_else(replicas_from_env),
            crypto: match builder.crypto {
                Some(c) => c,
                None => TransportCrypto::from_env()?,
            },
            replay_cursor: builder.replay_cursor.unwrap_or(ReplayCursor::StreamSeq),
            sync_ack: builder.sync_ack.unwrap_or(true),
            max_inflight: builder
                .max_inflight
                .unwrap_or_else(|| max_inflight_from_env(builder.sync_ack.unwrap_or(true))),
            topic_shards: builder
                .topic_shards
                .unwrap_or_else(crate::stream_shard::topic_shards_from_env),
        })
    }
}

fn replay_cursor_from_env() -> ReplayCursor {
    match std::env::var(REPLAY_CURSOR_ENV)
        .unwrap_or_else(|_| "stream_seq".into())
        .to_ascii_lowercase()
        .as_str()
    {
        "tail_only" | "tail" | "none" => ReplayCursor::TailOnly,
        _ => ReplayCursor::StreamSeq,
    }
}

fn sync_ack_from_env() -> bool {
    !matches!(
        std::env::var(SYNC_ACK_ENV)
            .unwrap_or_else(|_| "1".into())
            .as_str(),
        "0" | "false" | "off" | "no"
    )
}

fn max_inflight_from_env(sync_ack: bool) -> u32 {
    if let Ok(raw) = std::env::var(MAX_INFLIGHT_ENV) {
        if let Ok(n) = raw.parse::<u32>() {
            return n.max(1);
        }
    }
    if sync_ack {
        1
    } else {
        256
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn replay_cursor_parses_tail_only() {
        std::env::set_var(REPLAY_CURSOR_ENV, "tail_only");
        assert_eq!(replay_cursor_from_env(), ReplayCursor::TailOnly);
        std::env::remove_var(REPLAY_CURSOR_ENV);
    }
}
