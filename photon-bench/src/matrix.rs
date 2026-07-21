//! Matrix dimension parsing for CLI flags.

use anyhow::{bail, Result};
use photon_testkit::{MatrixSpec, ShardStrategy, StorageAdapter, TelemetryAdapter, Topology};

pub fn matrix_from_cli(
    storage: &str,
    telemetry: &str,
    topology: Option<&str>,
) -> Result<MatrixSpec> {
    Ok(MatrixSpec {
        storage: parse_storage(storage)?,
        topology: parse_topology(topology)?,
        telemetry: parse_telemetry(telemetry)?,
        shard_strategy: ShardStrategy::None,
    })
}

pub fn shared_store_skip_reason(matrix: &MatrixSpec, experiment: &str) -> Option<String> {
    use photon_testkit::fleet_store_skip_reason;
    let id = experiment.to_ascii_lowercase();
    if !crate::experiments::requires_shared_store(&id) {
        return None;
    }
    fleet_store_skip_reason(matrix)
}

fn parse_storage(s: &str) -> Result<StorageAdapter> {
    match s {
        "mem" | "embedded" => Ok(StorageAdapter::Mem),
        "nats" => Ok(StorageAdapter::Nats),
        "fluvio" => Ok(StorageAdapter::Fluvio),
        "kafka" => Ok(StorageAdapter::Kafka),
        "sqlite" => Ok(StorageAdapter::Sqlite),
        other => bail!(
            "unknown storage adapter: {other}; valid: mem, nats, fluvio, kafka, sqlite (embedded aliases mem)"
        ),
    }
}

fn parse_topology(s: Option<&str>) -> Result<Topology> {
    match s.unwrap_or("isolated-lab") {
        "isolated-lab" => Ok(Topology::IsolatedLab),
        "embedded-composite" => Ok(Topology::EmbeddedComposite),
        "split-runtime" => Ok(Topology::SplitRuntime),
        "broker-cluster" => Ok(Topology::BrokerCluster),
        "remote-surreal" => bail!("remote-surreal topology removed; use broker-cluster"),
        other => bail!("unknown topology: {other}"),
    }
}

fn parse_telemetry(s: &str) -> Result<TelemetryAdapter> {
    match s {
        "off" => Ok(TelemetryAdapter::Off),
        "console" => Ok(TelemetryAdapter::Console),
        "external-ops-log" => Ok(TelemetryAdapter::ExternalOpsLog),
        "structured-ops-log" => Ok(TelemetryAdapter::StructuredOpsLog),
        other => bail!("unknown telemetry adapter: {other}"),
    }
}

/// Campaign slice experiment lists (see `EXPERIMENTS.md`).
pub fn slice_experiments(slice: &str) -> Result<Vec<&'static str>> {
    match slice {
        "photon-minimal" | "adapter-minimal" => Ok(vec!["bm-p0", "bm-p1", "bm-pg0", "bm-pg2"]),
        "photon-durable" => Ok(vec!["bm-p2", "bm-p4"]),
        "photon-ops" => Ok(vec!["bm-p3", "bm-pl0", "bm-pl1"]),
        "photon-load" => Ok(vec!["bm-pl2", "bm-pl3"]),
        "deploy-shape" => Ok(vec!["bm-p0", "bm-p6"]),
        "executor" => Ok(vec!["bm-p7", "bm-p8"]),
        "crypto" => Ok(vec!["bm-p9"]),
        "broker-spike" => Ok(vec!["bm-pb0", "bm-pb1", "bm-pb2", "bm-pb3"]),
        "broker-fleet" | "distributed-fleet" => Ok(vec![
            "bm-pf0", "bm-pf1", "bm-pf2", "bm-pf3", "bm-pf4", "bm-p6", "bm-pfs", "bm-pfe",
            "bm-pfh", "bm-pb4", "bm-pb5", "bm-pg0", "bm-pg1", "bm-pg2",
        ]),
        "projection-inputs" => Ok(vec![
            "bm-p0", "bm-p2", "bm-pl2", "bm-p3", "bm-p8", "bm-p6", "bm-pf1", "bm-pf2", "bm-pf3",
            "bm-pf4",
        ]),
        other => bail!("unknown campaign slice: {other}"),
    }
}

pub fn report_path(
    reports_dir: &std::path::Path,
    experiment: &str,
    matrix: &MatrixSpec,
    hardware: &str,
) -> std::path::PathBuf {
    reports_dir.join(format!(
        "{}-{}-{}.json",
        experiment,
        matrix.report_slug(),
        hardware
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use photon_testkit::{MatrixSpec, TelemetryAdapter, Topology};

    #[test]
    fn report_path_includes_topology_and_telemetry() {
        let matrix = MatrixSpec::ci_mem_embedded()
            .with_topology(Topology::EmbeddedComposite)
            .with_telemetry(TelemetryAdapter::Console);
        let path = report_path(std::path::Path::new("reports"), "bm-p0", &matrix, "dev-wsl");
        let name = path.file_name().unwrap().to_string_lossy();
        assert!(name.contains("embedded-composite"), "{name}");
        assert!(name.contains("console"), "{name}");
        assert!(name.starts_with("bm-p0-"), "{name}");
    }

    #[test]
    fn matrix_from_cli_parses_topology() {
        let m = matrix_from_cli("mem", "off", Some("embedded-composite")).unwrap();
        assert_eq!(m.topology, Topology::EmbeddedComposite);
    }
}
