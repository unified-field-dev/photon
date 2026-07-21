//! Campaign slice matrix row expansion (see `EXPERIMENTS.md`).

use photon_testkit::{MatrixSpec, StorageAdapter, TelemetryAdapter, Topology};

/// Expand a campaign slice into concrete matrix rows.
pub fn slice_matrix_rows(slice: &str, base: MatrixSpec) -> Vec<MatrixSpec> {
    match slice {
        "photon-ops" => vec![
            base.clone().with_telemetry(TelemetryAdapter::Off),
            base.clone().with_telemetry(TelemetryAdapter::Console),
            base.with_telemetry(TelemetryAdapter::ExternalOpsLog),
        ],
        "deploy-shape" => deploy_shape_rows(&base),
        "broker-spike" | "broker-fleet" | "distributed-fleet" => vec![base
            .with_storage(StorageAdapter::Nats)
            .with_topology(Topology::BrokerCluster)],
        _ => vec![base],
    }
}

fn deploy_shape_rows(base: &MatrixSpec) -> Vec<MatrixSpec> {
    vec![
        base.clone().with_topology(Topology::IsolatedLab),
        base.clone().with_topology(Topology::EmbeddedComposite),
        base.clone().with_topology(Topology::SplitRuntime),
    ]
}

/// Filter experiment × matrix pairs for deploy-shape slice.
pub fn deploy_shape_runs(experiment: &str, matrix: &MatrixSpec) -> bool {
    let id = experiment.to_ascii_lowercase();
    match id.as_str() {
        "bm-p0" => matches!(
            matrix.topology,
            Topology::IsolatedLab | Topology::EmbeddedComposite
        ),
        "bm-p6" => matches!(
            matrix.topology,
            Topology::IsolatedLab | Topology::SplitRuntime | Topology::BrokerCluster
        ),
        _ => true,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn photon_ops_expands_telemetry() {
        let rows = slice_matrix_rows("photon-ops", MatrixSpec::ci_mem_embedded());
        assert_eq!(rows.len(), 3);
        assert!(rows
            .iter()
            .any(|m| m.telemetry == TelemetryAdapter::Console));
    }
}
