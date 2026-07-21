//! `Photon` runtime builder and process-wide configuration — **Integrating the host**.

#![cfg(feature = "runtime")]
#![cfg_attr(test, allow(clippy::unwrap_used, clippy::expect_used))]

pub mod admin;

mod builder;
mod executor;
mod photon;
pub mod runtime;
mod subsystem_remote_url;

pub use admin::{
    collect_admin_snapshot, AdminBackendSummary, AdminCheckpointSummary, AdminHandlerSummary,
    AdminSnapshot, AdminTopicSummary,
};
pub use builder::PhotonBuilder;
pub use executor::ExecutorController;
pub use photon::{configure, default, Photon, PhotonRuntimeState};
pub use subsystem_remote_url::resolve_photon_remote_base_url;

pub use photon_backend::{
    BackendCapabilities, BackendContext, EmbeddedBackend, Envelope, Event, GenericPhotonBackend,
    GroupOpts, HandlerCtx, HandlerDescriptor, HandlerRegistry, InProcStoragePort, PhotonBackend,
    PhotonError, Result, StoragePort, SubscribeOpts, Subscription, SubscriptionHandle,
    SubscriptionMode, TopicDescriptor, TopicRegistry, TransportCrypto,
};
