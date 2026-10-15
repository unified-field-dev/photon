//! Kafka [`StoragePort`](photon_backend::StoragePort) for distributed Photon delivery.
//!
//! Wraps Kafka consumer groups, checkpoint persistence, and topic sharding behind the shared
//! storage contract. Enable via the `kafka` feature on the
//! [`photon`](https://docs.rs/uf-photon/latest/photon/) public crate.
//!
//! Connection and topic options: [`KafkaConfig`] and [`KafkaStoragePortBuilder`]
//! (builder methods + env fallbacks documented on the builder).
//!
//! Wire with [`PhotonBuilder::storage_port`](https://docs.rs/uf-photon/latest/photon/struct.PhotonBuilder.html#method.storage_port)
//! after [`KafkaStoragePortBuilder::build`](KafkaStoragePortBuilder::build).
//!
//! ## Entry points
//!
//! - [`KafkaStoragePort`] — Kafka storage adapter
//! - [`KafkaConfig`] / [`KafkaStoragePortBuilder`] — connection and topic options
//! - [`kafka_brokers_from_env`] — resolve broker list from environment
//!
//! Performance notes: [`photon-bench/PERFORMANCE.md`](../../photon-bench/PERFORMANCE.md).
//!
//! ## Topic mapping (Kafka)
//!
//! - **Topic:** `photon-{topic}` when `topic_shards = 1`; `photon-s-{shard}-{topic}` when sharded.
//! - **Retention:** `PHOTON_KAFKA_RETENTION` is **not** applied at auto-create (`rskafka` limitation).
//!   Pre-create topics with [`topic::retention_ms`] or set broker `log.retention.ms` (see `SECURITY.md`).
//! - **Checkpoints:** compact topic `photon-checkpoints` (same key layout as other broker adapters).
//! - **Replay:** [`ReplayCursor::StreamSeq`] stores offset+1; [`ReplayCursor::TailOnly`] tails live only.

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
    KafkaConfig, KafkaStoragePortBuilder, ReplayCursor, BROKERS_ENV, MAX_INFLIGHT_ENV, PREFIX_ENV,
    REPLAY_CURSOR_ENV, SYNC_ACK_ENV,
};
pub use consumer::{consumer_group_for, durable_consumer_name};
pub use port::{kafka_brokers_from_env, KafkaStoragePort};
pub use replicas::REPLICAS_ENV;
pub use retention::RETENTION_ENV;
pub use stream_shard::TOPIC_SHARDS_ENV;
pub use topic::retention_ms;
