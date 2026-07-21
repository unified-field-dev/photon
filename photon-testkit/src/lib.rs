//! Shared test/bench harness for Photon.
//!
//! [`BootstrapSession`] installs a storage adapter and builds a Photon runtime for a [`MatrixSpec`] row.
//! [`ScenarioRunner`] drives multi-step correctness scenarios used by `photon-e2e` and `photon-bench`.

pub mod bootstrap;
pub mod consumer_group;
pub mod cross_node;
pub mod fixtures;
pub mod identity;
pub mod matrix;
pub mod runner;
pub mod scenario;
pub mod shared_store;

mod backends;
mod bench_handlers;
mod group_topics;

#[cfg(feature = "backend-author")]
pub mod backend_author;

pub use bench_handlers::{
    executor_invocation_count, reset_executor_invocations, EXECUTOR_FIXTURE_TOPIC,
};
pub use bootstrap::BootstrapSession;
pub use consumer_group::{
    run_consumer_group_round_robin_lab, run_consumer_group_static_lab,
    run_consumer_group_verify_lab, ConsumerGroupStaticOutcome,
};
pub use cross_node::{
    apply_cross_node_fanout_step, run_cross_node_fanout_lab, CrossNodeFanoutResult,
    CrossNodeStepOutcome, FANOUT_PUBLISH_COUNT,
};
pub use identity::StubIdentityFactory;
pub use matrix::{MatrixSpec, ShardStrategy, StorageAdapter, TelemetryAdapter, Topology};
pub use runner::{LoadMetrics, RunMode, ScenarioResult, ScenarioRunner, StepTiming};
pub use scenario::ScenarioSpec;
pub use shared_store::{
    fleet_store_available, fleet_store_skip_reason, lab_store_available, lab_store_skip_reason,
    shared_store_available, shared_store_skip_reason, subscribe_attach_warmup,
};
