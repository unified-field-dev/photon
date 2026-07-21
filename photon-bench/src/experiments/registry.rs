//! Pre-registered experiment IDs.

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExperimentStatus {
    AdapterReady,
    #[allow(dead_code)]
    BrokerPending,
    BrokerReady,
    GroupReady,
}

pub struct ExperimentMeta {
    pub id: &'static str,
    pub status: ExperimentStatus,
    pub summary: &'static str,
}

pub const REGISTRY: &[ExperimentMeta] = &[
    meta("bm-p0", AdapterReady, "publish-only (0 subscribers)"),
    meta("bm-p1", AdapterReady, "publish + 1 subscriber delivery"),
    meta(
        "bm-p2",
        AdapterReady,
        "publish + N durable subscribers (fanout)",
    ),
    meta(
        "bm-p3",
        AdapterReady,
        "sustained publish (append-buffer stress)",
    ),
    meta(
        "bm-p4",
        AdapterReady,
        "checkpoint replay after runtime restart",
    ),
    meta("bm-p5", AdapterReady, "keyed topic partition filter"),
    meta(
        "bm-p6",
        BrokerReady,
        "cross-node fanout latency gate (PF0 = functional; P6 registers p99 budget)",
    ),
    meta("bm-p7", AdapterReady, "durable handler + checkpoint commit"),
    meta("bm-p8", AdapterReady, "executor pool / DLQ pressure"),
    meta("bm-p9", AdapterReady, "encryption envelope overhead delta"),
    meta("bm-pl0", AdapterReady, "sustained ~100 publishes/s"),
    meta("bm-pl1", AdapterReady, "sustained ~1k publishes/s"),
    meta("bm-pl2", AdapterReady, "sustained ~10k publishes/s"),
    meta("bm-pl3", AdapterReady, "sustained ~100k publishes/s"),
    meta("bm-pb0", BrokerReady, "broker publish-only smoke"),
    meta(
        "bm-pb1",
        BrokerReady,
        "queue group load-balance (2 workers)",
    ),
    meta("bm-pb2", BrokerReady, "fanout delivery p99 vs N"),
    meta("bm-pb3", BrokerReady, "bounded replay after worker kill"),
    meta(
        "bm-pb4",
        BrokerReady,
        "informational n=1 vs n=3 sweep (ingress: PFH/PF2/PF4)",
    ),
    meta("bm-pb5", BrokerReady, "failover — kill one broker node"),
    meta(
        "bm-pf0",
        BrokerReady,
        "fleet smoke (2 nodes + broker cluster)",
    ),
    meta("bm-pf1", BrokerReady, "N subscriber nodes fanout scaling"),
    meta("bm-pf2", BrokerReady, "M publisher nodes aggregate rate"),
    meta("bm-pf3", BrokerReady, "sustained distributed fleet load"),
    meta("bm-pf4", BrokerReady, "multi-partition sustained load"),
    meta(
        "bm-pfh",
        BrokerReady,
        "single-topic firehose throughput sweep",
    ),
    meta(
        "bm-pfs",
        BrokerReady,
        "sustained fanout with 4 ephemeral subscribers (10k/s × 30s)",
    ),
    meta(
        "bm-pfe",
        BrokerReady,
        "sustained executor dispatch with durable handler (1k/s × 60s)",
    ),
    meta(
        "bm-pg0",
        GroupReady,
        "static 2-member consumer group full shard coverage",
    ),
    meta(
        "bm-pg1",
        GroupReady,
        "consumer group throughput vs member count",
    ),
    meta(
        "bm-pg2",
        GroupReady,
        "post-restart group replay verify (not fleet lease rebalance)",
    ),
];

const fn meta(id: &'static str, status: ExperimentStatus, summary: &'static str) -> ExperimentMeta {
    ExperimentMeta {
        id,
        status,
        summary,
    }
}

use ExperimentStatus::{AdapterReady, BrokerReady, GroupReady};

pub const fn status_label(status: ExperimentStatus) -> &'static str {
    match status {
        ExperimentStatus::AdapterReady => "adapter-ready",
        ExperimentStatus::BrokerPending => "broker-pending",
        ExperimentStatus::BrokerReady => "broker-ready",
        ExperimentStatus::GroupReady => "group-ready",
    }
}

pub fn meta_for(id: &str) -> Option<&'static ExperimentMeta> {
    REGISTRY.iter().find(|m| m.id == id)
}

pub fn requires_shared_store(id: &str) -> bool {
    matches!(
        id,
        "bm-p6"
            | "bm-pf0"
            | "bm-pf1"
            | "bm-pf2"
            | "bm-pf3"
            | "bm-pf4"
            | "bm-pfh"
            | "bm-pfs"
            | "bm-pfe"
            | "bm-pb0"
            | "bm-pb1"
            | "bm-pb2"
            | "bm-pb3"
            | "bm-pb4"
            | "bm-pb5"
    )
}
