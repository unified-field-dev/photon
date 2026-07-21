//! Builder and resolved configuration for the NATS storage adapter.

use std::time::Duration;

use photon_backend::{PhotonError, Result, TransportCrypto};

use crate::replicas::replicas_from_env;
use crate::retention::retention_from_env;

/// Environment variable for NATS server URL.
pub const URL_ENV: &str = "PHOTON_NATS_URL";

/// Environment variable for `JetStream` stream name.
pub const STREAM_ENV: &str = "PHOTON_NATS_STREAM";

/// How durable replay and checkpoints map to `JetStream`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ReplayCursor {
    /// Broker stream sequence on publish ack; `ByStartSequence` replay.
    #[default]
    StreamSeq,
    /// Live tail only; checkpoints no-op; maximum publish throughput.
    TailOnly,
}

/// Resolved NATS adapter settings (no env lookups at append time).
#[derive(Clone)]
pub struct NatsConfig {
    /// NATS server URL(s).
    pub url: String,
    /// `JetStream` stream name.
    pub stream_name: String,
    /// Stream max age / replay window.
    pub retention: Duration,
    /// `JetStream` stream replica count.
    pub replicas: usize,
    /// Payload envelope crypto.
    pub crypto: TransportCrypto,
    /// Replay and checkpoint semantics.
    pub replay_cursor: ReplayCursor,
    /// Await `JetStream` publish ack per message.
    pub sync_ack: bool,
    /// Max concurrent in-flight publishes.
    pub max_inflight: u32,
    /// Independent `JetStream` streams for ingress sharding (`1` = legacy single stream).
    pub stream_shards: u32,
}

impl NatsConfig {
    /// Replica count applied when creating shard streams.
    #[must_use]
    pub const fn effective_replicas(&self) -> usize {
        if self.stream_shards > 1 {
            1
        } else {
            self.replicas
        }
    }

    /// Whether publishes route across multiple `JetStream` streams.
    #[must_use]
    pub const fn is_sharded(&self) -> bool {
        self.stream_shards > 1
    }
}

/// Environment variable for replay cursor mode.
pub const REPLAY_CURSOR_ENV: &str = "PHOTON_NATS_REPLAY_CURSOR";

/// Environment variable for synchronous publish ack (`1` / `0`).
pub const SYNC_ACK_ENV: &str = "PHOTON_NATS_SYNC_ACK";

/// Environment variable for max in-flight publishes per port.
pub const MAX_INFLIGHT_ENV: &str = "PHOTON_NATS_MAX_INFLIGHT";

/// Builder for [`super::port::NatsStoragePort`].
///
/// **Configuration lives here.** Set builder methods explicitly; unset fields fall back to
/// `PHOTON_NATS_*` environment variables via [`from_env_defaults`](Self::from_env_defaults).
///
/// Use the **same** builder settings on every Mode 2 publisher and worker binary that shares a
/// cluster. Getting started:
/// [Mode 2](https://docs.rs/uf-photon/latest/photon/#mode-2--brokered-publisher--worker-binaries).
///
/// # Options
///
/// | Method / env | Default | Purpose |
/// |--------------|---------|---------|
/// | [`.url`](Self::url) / [`URL_ENV`] | **required** | NATS server URL(s). |
/// | [`.stream_name`](Self::stream_name) / [`STREAM_ENV`] | `photon` | `JetStream` stream name. |
/// | [`.retention`](Self::retention) / [`RETENTION_ENV`](crate::retention::RETENTION_ENV) | `15m` | Stream max age. |
/// | [`.replicas`](Self::replicas) / [`REPLICAS_ENV`](crate::replicas::REPLICAS_ENV) | `1` | Stream replica count. |
/// | [`.replay_cursor`](Self::replay_cursor) / [`REPLAY_CURSOR_ENV`] | `stream_seq` | [`ReplayCursor::StreamSeq`] or [`ReplayCursor::TailOnly`]. |
/// | [`.sync_ack`](Self::sync_ack) / [`SYNC_ACK_ENV`] | `1` | Await `JetStream` publish ack (`0` = firehose). |
/// | [`.max_inflight`](Self::max_inflight) / [`MAX_INFLIGHT_ENV`] | `1` / `256` | Concurrent in-flight publishes (`256` when `sync_ack` off). |
/// | [`.stream_shards`](Self::stream_shards) / [`STREAM_SHARDS_ENV`](crate::stream_shard::STREAM_SHARDS_ENV) | `1` | `JetStream` stream shard count (`K>1` → `photon-0..K-1`). |
///
/// Set `.stream_shards(K)` consistently across embedded hosts (builder-first; not tied to publisher count).
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
/// use photon_backend_nats::{NatsStoragePort, ReplayCursor};
/// use photon_runtime::Photon;
///
/// # async fn boot_publisher() -> photon_backend::Result<()> {
/// let port = Arc::new(
///     NatsStoragePort::builder()
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
/// use photon_backend_nats::{NatsStoragePort, ReplayCursor};
/// use photon_core::JsonIdentityFactory;
/// use photon_runtime::Photon;
///
/// # async fn boot_worker() -> photon_backend::Result<()> {
/// let port = Arc::new(
///     NatsStoragePort::builder()
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
/// See also: [`NatsConfig`] (resolved snapshot), crate-level topic mapping in [`crate`](index.html).
#[derive(Default)]
pub struct NatsStoragePortBuilder {
    url: Option<String>,
    stream_name: Option<String>,
    retention: Option<Duration>,
    replicas: Option<usize>,
    crypto: Option<TransportCrypto>,
    replay_cursor: Option<ReplayCursor>,
    sync_ack: Option<bool>,
    max_inflight: Option<u32>,
    stream_shards: Option<u32>,
}

impl NatsStoragePortBuilder {
    /// Empty builder; set fields or call [`Self::from_env_defaults`].
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Fill unset fields from `PHOTON_NATS_*` environment variables.
    #[must_use]
    pub fn from_env_defaults(mut self) -> Self {
        if self.url.is_none() {
            self.url = std::env::var(URL_ENV).ok();
        }
        if self.stream_name.is_none() {
            self.stream_name = Some(std::env::var(STREAM_ENV).unwrap_or_else(|_| "photon".into()));
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
        if self.stream_shards.is_none() {
            self.stream_shards = Some(crate::stream_shard::stream_shards_from_env());
        }
        self
    }

    /// NATS server URL.
    #[must_use]
    pub fn url(mut self, url: impl Into<String>) -> Self {
        self.url = Some(url.into());
        self
    }

    /// `JetStream` stream name.
    #[must_use]
    pub fn stream_name(mut self, name: impl Into<String>) -> Self {
        self.stream_name = Some(name.into());
        self
    }

    /// Stream retention duration.
    #[must_use]
    pub const fn retention(mut self, retention: Duration) -> Self {
        self.retention = Some(retention);
        self
    }

    /// Stream replica count.
    #[must_use]
    pub const fn replicas(mut self, replicas: usize) -> Self {
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

    /// Whether to await publish ack.
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

    /// `JetStream` stream shard count for ingress scaling (`1` = single shared stream).
    #[must_use]
    pub fn stream_shards(mut self, n: u32) -> Self {
        self.stream_shards = Some(n.clamp(1, crate::stream_shard::MAX_STREAM_SHARDS));
        self
    }

    /// Resolve configuration.
    ///
    /// # Errors
    ///
    /// Returns an error when required fields (URL) are missing.
    pub fn resolve(self) -> Result<NatsConfig> {
        let builder = self.from_env_defaults();
        let url = builder.url.ok_or_else(|| {
            PhotonError::Internal(format!("{URL_ENV} not set for nats storage adapter"))
        })?;
        Ok(NatsConfig {
            url,
            stream_name: builder.stream_name.unwrap_or_else(|| "photon".into()),
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
            stream_shards: builder
                .stream_shards
                .unwrap_or_else(crate::stream_shard::stream_shards_from_env),
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
