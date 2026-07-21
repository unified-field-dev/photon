//! Map experiment IDs to scenario specs.

use anyhow::{bail, Result};
use photon_testkit::MatrixSpec;

use super::registry::meta_for;
use super::specs;

#[derive(Debug, Clone)]
pub struct ExperimentPlan {
    pub id: String,
    pub scenario: photon_testkit::ScenarioSpec,
    pub target_rate: Option<u32>,
    pub duration_secs: Option<u32>,
    pub subscriber_count: Option<u32>,
}

pub fn requires_shared_store(id: &str) -> bool {
    super::registry::requires_shared_store(id)
}

pub fn resolve_experiment(
    id: &str,
    ops: Option<u32>,
    _matrix: &MatrixSpec,
) -> Result<ExperimentPlan> {
    let normalized = id.to_ascii_lowercase();
    if meta_for(&normalized).is_none() {
        bail!("unknown experiment {normalized}; see photon-bench/EXPERIMENTS.md");
    }
    specs::build_plan(&normalized, ops)
        .ok_or_else(|| anyhow::anyhow!("unknown experiment {normalized}"))
}

pub fn plan(
    id: &str,
    scenario: photon_testkit::ScenarioSpec,
    target_rate: Option<u32>,
    duration_secs: Option<u32>,
    subscriber_count: Option<u32>,
) -> ExperimentPlan {
    ExperimentPlan {
        id: id.into(),
        scenario,
        target_rate,
        duration_secs,
        subscriber_count,
    }
}
