//! NATS `JetStream` [`StoragePort`](photon_backend::StoragePort) for distributed Photon delivery.
//!
//! Wraps `JetStream` consumer groups, checkpoint persistence, and stream sharding behind the shared
//! storage contract. Enable via the `nats` feature on the
//! [`photon`](https://docs.rs/uf-photon/latest/photon/) facade.
//!
//! Connection and stream options: [`NatsConfig`] and [`NatsStoragePortBuilder`]
//! (builder methods + env fallbacks documented on the builder).
//!
//! Wire with [`PhotonBuilder::storage_port`](https://docs.rs/uf-photon/latest/photon/struct.PhotonBuilder.html#method.storage_port)
//! after [`NatsStoragePortBuilder::build`](NatsStoragePortBuilder::build).
//!
//! ## Entry points
//!
//! - [`NatsStoragePort`] — `JetStream` storage adapter
//! - [`NatsConfig`] / [`NatsStoragePortBuilder`] — connection and stream options
//! - [`nats_url_from_env`] — resolve broker URL from environment
//!
//! Performance methodology: [`photon-bench/PERFORMANCE_STUDY.md`](../../photon-bench/PERFORMANCE_STUDY.md).
//!
//! ## Topic mapping (NATS)
//!
//! - **Subject:** `photon.{topic}` when `stream_shards = 1`; `photon-s.{shard}.{topic}` when sharded.
//! - **`JetStream` stream:** default `photon` with `photon.>`; sharded mode uses `photon-0`…`photon-(K-1)`.
//! - **Checkpoints:** KV bucket `photon-checkpoints` (skipped when [`ReplayCursor::TailOnly`]).
//! - **Replay:** [`ReplayCursor::StreamSeq`] uses `JetStream` stream sequence; [`ReplayCursor::TailOnly`] tails live only.

#![cfg_attr(test, allow(clippy::unwrap_used, clippy::expect_used))]
mod checkpoint;
mod config;
mod connect;
mod consumer;
mod kv;
mod message;
mod port;
mod publish;
mod replicas;
mod retention;
mod stream;
pub mod stream_shard;
mod subject;

pub use config::{
    NatsConfig, NatsStoragePortBuilder, ReplayCursor, MAX_INFLIGHT_ENV, REPLAY_CURSOR_ENV,
    STREAM_ENV, SYNC_ACK_ENV, URL_ENV,
};
pub use consumer::{deliver_subject_for, durable_consumer_name};
pub use port::{nats_url_from_env, NatsStoragePort};
pub use replicas::REPLICAS_ENV;
pub use retention::RETENTION_ENV;
pub use stream_shard::STREAM_SHARDS_ENV;
