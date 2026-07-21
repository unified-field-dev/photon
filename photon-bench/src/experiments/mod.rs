//! Experiment registry and resolution.

mod registry;
mod resolve;
mod specs;

pub use registry::{status_label, REGISTRY};
pub use resolve::{requires_shared_store, resolve_experiment, ExperimentPlan};
