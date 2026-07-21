//! Runtime feature surface: backend, runtime, and telemetry re-exports.

pub mod host;
pub mod instrumentation;
pub mod prelude;

pub use photon_backend::{
    backend, checkpoint, consumer_group, delivery, delivery_mode, descriptor, error, event,
    handler_ctx, handler_descriptor, handler_registry, instrumentation as backend_instrumentation,
    models, registry, shard_router, storage, BackendCapabilities, BackendContext, DeliveryMode,
    EmbeddedBackend, Envelope, Event, GenericPhotonBackend, GroupOpts, HandlerCtx,
    HandlerDescriptor, HandlerRegistry, InProcStoragePort, PhotonBackend, PhotonError, Result,
    ShardConfig, StoragePort, SubscribeOpts, Subscription, SubscriptionHandle, SubscriptionMode,
    TopicDescriptor, TopicMetadata, TopicRegistry, TransportCrypto,
};

pub use photon_runtime::{
    collect_admin_snapshot, configure, default, resolve_photon_remote_base_url,
    runtime as runtime_parts, AdminBackendSummary, AdminCheckpointSummary, AdminHandlerSummary,
    AdminSnapshot, AdminTopicSummary, Photon, PhotonBuilder, PhotonRuntimeState,
};

pub use photon_telemetry::{install_ops_log, ops_log, ConsoleOpsLog, NoOpsLog, OpsLog};

#[cfg(feature = "nats")]
pub use photon_backend_nats::{
    NatsConfig, NatsStoragePort, NatsStoragePortBuilder, ReplayCursor, MAX_INFLIGHT_ENV,
    REPLAY_CURSOR_ENV, RETENTION_ENV as NATS_RETENTION_ENV, STREAM_ENV as NATS_STREAM_ENV,
    STREAM_SHARDS_ENV as NATS_STREAM_SHARDS_ENV, SYNC_ACK_ENV, URL_ENV as NATS_URL_ENV,
};

#[cfg(feature = "kafka")]
pub use photon_backend_kafka::{
    consumer_group_for, durable_consumer_name, kafka_brokers_from_env, KafkaConfig,
    KafkaStoragePort, KafkaStoragePortBuilder, ReplayCursor as KafkaReplayCursor,
    BROKERS_ENV as KAFKA_BROKERS_ENV, MAX_INFLIGHT_ENV as KAFKA_MAX_INFLIGHT_ENV,
    PREFIX_ENV as KAFKA_PREFIX_ENV, REPLAY_CURSOR_ENV as KAFKA_REPLAY_CURSOR_ENV,
    REPLICAS_ENV as KAFKA_REPLICAS_ENV, RETENTION_ENV as KAFKA_RETENTION_ENV,
    SYNC_ACK_ENV as KAFKA_SYNC_ACK_ENV, TOPIC_SHARDS_ENV as KAFKA_TOPIC_SHARDS_ENV,
};

#[cfg(feature = "fluvio")]
pub use photon_backend_fluvio::{
    consumer_group_for as fluvio_consumer_group_for,
    durable_consumer_name as fluvio_durable_consumer_name, fluvio_endpoint_from_env, FluvioConfig,
    FluvioStoragePort, FluvioStoragePortBuilder, ReplayCursor as FluvioReplayCursor,
    ENDPOINT_ENV as FLUVIO_ENDPOINT_ENV, MAX_INFLIGHT_ENV as FLUVIO_MAX_INFLIGHT_ENV,
    PREFIX_ENV as FLUVIO_PREFIX_ENV, REPLAY_CURSOR_ENV as FLUVIO_REPLAY_CURSOR_ENV,
    REPLICAS_ENV as FLUVIO_REPLICAS_ENV, RETENTION_ENV as FLUVIO_RETENTION_ENV,
    SYNC_ACK_ENV as FLUVIO_SYNC_ACK_ENV, TOPIC_SHARDS_ENV as FLUVIO_TOPIC_SHARDS_ENV,
};

#[cfg(feature = "sqlite")]
pub use photon_backend_sqlite::{
    sqlite_path_from_env, SqliteStoragePort, PATH_ENV as SQLITE_PATH_ENV,
};
