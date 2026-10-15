//! Benchmark/e2e dimension matrix (extends the `photon-bench` experiment registry; list IDs with `cargo run -p photon-bench -- experiments`).

mod presets;

use serde::{Deserialize, Serialize};

/// Storage adapter selection (`--storage`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Default)]
#[serde(rename_all = "kebab-case")]
pub enum StorageAdapter {
    /// In-process `InProcStoragePort`.
    #[default]
    Mem,
    /// NATS `JetStream`.
    Nats,
    /// Fluvio.
    Fluvio,
    /// Kafka.
    Kafka,
    /// Embedded `SQLite` file store.
    Sqlite,
}

impl StorageAdapter {
    /// Stable CLI / report string.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Mem => "mem",
            Self::Nats => "nats",
            Self::Fluvio => "fluvio",
            Self::Kafka => "kafka",
            Self::Sqlite => "sqlite",
        }
    }
}

/// Where the runtime and bootstrap run.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Default)]
#[serde(rename_all = "kebab-case")]
pub enum Topology {
    /// Single isolated lab process.
    #[default]
    IsolatedLab,
    /// Embedded composite host layout.
    EmbeddedComposite,
    /// Split runtime processes.
    SplitRuntime,
    /// Remote broker cluster (lab).
    BrokerCluster,
}

impl Topology {
    /// Value written to topology env vars.
    #[must_use]
    pub const fn env_value(self) -> &'static str {
        match self {
            Self::IsolatedLab => "isolated-lab",
            Self::EmbeddedComposite => "embedded-composite",
            Self::SplitRuntime => "split-runtime",
            Self::BrokerCluster => "broker-cluster",
        }
    }
}

/// Telemetry adapter for the run.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Default)]
#[serde(rename_all = "kebab-case")]
pub enum TelemetryAdapter {
    /// No ops log.
    #[default]
    Off,
    /// Console ops log.
    Console,
    /// External ops log sink.
    ExternalOpsLog,
    /// Structured ops log sink.
    StructuredOpsLog,
}

/// Reserved for partition / multi-logical routing studies.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Default)]
#[serde(rename_all = "kebab-case")]
pub enum ShardStrategy {
    /// No sharding.
    #[default]
    None,
    /// Shard by topic key.
    ByTopicKey,
}

/// Full cross-product selector for e2e and bench drivers.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MatrixSpec {
    /// Storage adapter id.
    pub storage: StorageAdapter,
    /// Runtime topology.
    pub topology: Topology,
    /// Telemetry adapter.
    pub telemetry: TelemetryAdapter,
    /// Shard strategy (reserved).
    pub shard_strategy: ShardStrategy,
}

impl Default for MatrixSpec {
    fn default() -> Self {
        Self {
            storage: StorageAdapter::Mem,
            topology: Topology::IsolatedLab,
            telemetry: TelemetryAdapter::Off,
            shard_strategy: ShardStrategy::None,
        }
    }
}

impl MatrixSpec {
    /// Stable string id for report filenames.
    #[must_use]
    pub fn report_slug(&self) -> String {
        format!(
            "{}-{}-{}-{}",
            self.storage.as_str(),
            serde_json::to_string(&self.topology)
                .unwrap_or_default()
                .trim_matches('"'),
            serde_json::to_string(&self.telemetry)
                .unwrap_or_default()
                .trim_matches('"'),
            serde_json::to_string(&self.shard_strategy)
                .unwrap_or_default()
                .trim_matches('"'),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ci_mem_default() {
        let m = MatrixSpec::ci_mem();
        assert_eq!(m.storage, StorageAdapter::Mem);
    }

    #[test]
    fn report_slug_is_stable() {
        let slug = MatrixSpec::ci_mem().report_slug();
        assert!(slug.contains("mem"));
    }
}
