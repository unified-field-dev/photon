//! Fluvio [`StoragePort`](photon_backend::StoragePort) for distributed Photon delivery.
//!
//! Wraps Fluvio consumer groups, checkpoint persistence, and topic sharding behind the shared
//! storage contract. Enable via the `fluvio` feature on the
//! [`photon`](https://docs.rs/uf-photon/latest/photon/) facade.
//!
//! Connection and topic options: [`FluvioConfig`] and [`FluvioStoragePortBuilder`]
//! (builder methods + env fallbacks documented on the builder).
//!
//! Wire with [`PhotonBuilder::storage_port`](https://docs.rs/uf-photon/latest/photon/struct.PhotonBuilder.html#method.storage_port)
//! after [`FluvioStoragePortBuilder::build`](FluvioStoragePortBuilder::build).
//!
//! ## Entry points
//!
//! - [`FluvioStoragePort`] — Fluvio storage adapter
//! - [`FluvioConfig`] / [`FluvioStoragePortBuilder`] — connection and topic options
//! - [`fluvio_endpoint_from_env`] — resolve SC endpoint from environment
//!
//! Performance methodology: [`photon-bench/PERFORMANCE_STUDY.md`](../../photon-bench/PERFORMANCE_STUDY.md).
//!
//! ## Topic mapping (Fluvio)
//!
//! - **Topic:** `photon-{topic}` when `topic_shards = 1`; `photon-s-{shard}-{topic}` when sharded (hyphens only).
//! - **Checkpoints:** compact topic `photon-checkpoints`.
//! - **Replay:** [`ReplayCursor::StreamSeq`] uses produce offset+1; [`ReplayCursor::TailOnly`] uses `Offset::end()`.

#![cfg_attr(test, allow(clippy::unwrap_used, clippy::expect_used))]
mod checkpoint;
mod config;
mod connect;
mod consumer;
mod message;
mod port;
mod publish;
mod replicas;
mod retention;
pub mod stream_shard;
mod subject;
mod topic;

pub use config::{
    FluvioConfig, FluvioStoragePortBuilder, ReplayCursor, ENDPOINT_ENV, MAX_INFLIGHT_ENV,
    PREFIX_ENV, REPLAY_CURSOR_ENV, SYNC_ACK_ENV,
};
pub use consumer::{consumer_group_for, durable_consumer_name};
pub use port::{fluvio_endpoint_from_env, FluvioStoragePort};
pub use replicas::REPLICAS_ENV;
pub use retention::RETENTION_ENV;
pub use stream_shard::TOPIC_SHARDS_ENV;
