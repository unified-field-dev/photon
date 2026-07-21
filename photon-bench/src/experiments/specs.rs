//! Table-driven experiment ID → scenario spec builders.

use photon_testkit::ScenarioSpec;

use super::resolve::{plan, ExperimentPlan};

type SpecBuilder = fn(&str, Option<u32>) -> ExperimentPlan;

struct ExperimentSpec {
    id: &'static str,
    build: SpecBuilder,
}

const SPECS: &[ExperimentSpec] = &[
    spec("bm-p0", build_p0),
    spec("bm-p1", build_p1),
    spec("bm-p2", build_p2),
    spec("bm-p3", build_p3),
    spec("bm-p4", build_p4),
    spec("bm-p5", build_p5),
    spec("bm-p6", build_p6),
    spec("bm-p7", build_p7),
    spec("bm-p8", build_p8),
    spec("bm-p9", build_p9),
    spec("bm-pl0", build_pl0),
    spec("bm-pl1", build_pl1),
    spec("bm-pl2", build_pl2),
    spec("bm-pl3", build_pl3),
    spec("bm-pb0", build_pb0),
    spec("bm-pb1", build_pb1),
    spec("bm-pb2", build_pb2),
    spec("bm-pb3", build_pb3),
    spec("bm-pb4", build_pb4),
    spec("bm-pb5", build_pb5),
    spec("bm-pf0", build_pf0),
    spec("bm-pf1", build_pf1),
    spec("bm-pf2", build_pf2),
    spec("bm-pf3", build_pf3),
    spec("bm-pf4", build_pf4),
    spec("bm-pfh", build_pfh),
    spec("bm-pfs", build_pfs),
    spec("bm-pfe", build_pfe),
    spec("bm-pg0", build_pg0),
    spec("bm-pg1", build_pg1),
    spec("bm-pg2", build_pg2),
];

const fn spec(id: &'static str, build: SpecBuilder) -> ExperimentSpec {
    ExperimentSpec { id, build }
}

pub fn build_plan(id: &str, ops: Option<u32>) -> Option<ExperimentPlan> {
    SPECS
        .iter()
        .find(|s| s.id == id)
        .map(|s| (s.build)(id, ops))
}

fn build_p0(id: &str, ops: Option<u32>) -> ExperimentPlan {
    let count = ops.unwrap_or(5000);
    plan(id, ScenarioSpec::publish_only(count), None, None, None)
}

fn build_p1(id: &str, ops: Option<u32>) -> ExperimentPlan {
    let count = ops.unwrap_or(5000);
    plan(
        id,
        ScenarioSpec::publish_subscribe_bench(count),
        None,
        None,
        Some(1),
    )
}

fn build_p2(id: &str, ops: Option<u32>) -> ExperimentPlan {
    let n = ops.unwrap_or(2).max(1);
    plan(id, ScenarioSpec::multi_subscriber_n(n), None, None, Some(n))
}

fn build_p3(id: &str, ops: Option<u32>) -> ExperimentPlan {
    let duration = ops.unwrap_or(60);
    plan(
        id,
        ScenarioSpec::append_buffer_stress(1000, duration),
        Some(1000),
        Some(duration),
        Some(1),
    )
}

fn build_p4(id: &str, ops: Option<u32>) -> ExperimentPlan {
    plan(
        id,
        ScenarioSpec::checkpoint_replay_throughput(ops.unwrap_or(10_000)),
        None,
        None,
        None,
    )
}

fn build_p5(id: &str, _ops: Option<u32>) -> ExperimentPlan {
    plan(
        id,
        ScenarioSpec::keyed_filter_cardinality(),
        None,
        None,
        None,
    )
}

fn build_p6(id: &str, _ops: Option<u32>) -> ExperimentPlan {
    plan(
        id,
        ScenarioSpec::cross_node_fanout("testkit.fanout"),
        None,
        None,
        None,
    )
}

fn build_p7(id: &str, ops: Option<u32>) -> ExperimentPlan {
    plan(
        id,
        ScenarioSpec::handler_checkpoint_stress(ops.unwrap_or(10_000)),
        None,
        None,
        None,
    )
}

fn build_p8(id: &str, ops: Option<u32>) -> ExperimentPlan {
    let duration = ops.unwrap_or(30);
    plan(
        id,
        ScenarioSpec::executor_pressure(500, duration),
        Some(500),
        Some(duration),
        Some(1),
    )
}

fn build_p9(id: &str, ops: Option<u32>) -> ExperimentPlan {
    let count = ops.unwrap_or(5000);
    plan(id, ScenarioSpec::publish_only(count), None, None, None)
}

fn build_pl0(id: &str, ops: Option<u32>) -> ExperimentPlan {
    let duration = ops.unwrap_or(60);
    plan(
        id,
        ScenarioSpec::publish_at_rate(100, duration),
        Some(100),
        Some(duration),
        None,
    )
}

fn build_pl1(id: &str, ops: Option<u32>) -> ExperimentPlan {
    let duration = ops.unwrap_or(60);
    plan(
        id,
        ScenarioSpec::publish_at_rate(1000, duration),
        Some(1000),
        Some(duration),
        None,
    )
}

fn build_pl2(id: &str, ops: Option<u32>) -> ExperimentPlan {
    let duration = ops.unwrap_or(60);
    plan(
        id,
        ScenarioSpec::publish_at_rate(10_000, duration),
        Some(10_000),
        Some(duration),
        None,
    )
}

fn build_pl3(id: &str, ops: Option<u32>) -> ExperimentPlan {
    let duration = ops.unwrap_or(60);
    plan(
        id,
        ScenarioSpec::publish_at_rate(100_000, duration),
        Some(100_000),
        Some(duration),
        None,
    )
}

fn build_pb0(id: &str, ops: Option<u32>) -> ExperimentPlan {
    let count = ops.unwrap_or(1000);
    plan(id, ScenarioSpec::publish_only(count), None, None, None)
}

fn build_pb1(id: &str, _ops: Option<u32>) -> ExperimentPlan {
    plan(
        id,
        ScenarioSpec::consumer_group_static(
            "testkit.broker.group",
            "testkit-broker-workers",
            8,
            200,
        ),
        None,
        None,
        Some(2),
    )
}

fn build_pb2(id: &str, ops: Option<u32>) -> ExperimentPlan {
    let n = ops.unwrap_or(4).max(1);
    plan(id, ScenarioSpec::multi_subscriber_n(n), None, None, Some(n))
}

fn build_pb3(id: &str, ops: Option<u32>) -> ExperimentPlan {
    plan(
        id,
        ScenarioSpec::checkpoint_replay_throughput(ops.unwrap_or(1000)),
        None,
        None,
        None,
    )
}

fn build_pb4(id: &str, ops: Option<u32>) -> ExperimentPlan {
    let duration = ops.unwrap_or(60);
    plan(
        id,
        ScenarioSpec::publish_at_rate(1000, duration),
        Some(1000),
        Some(duration),
        None,
    )
}

fn build_pb5(id: &str, ops: Option<u32>) -> ExperimentPlan {
    let duration = ops.unwrap_or(45);
    plan(
        id,
        ScenarioSpec::publish_at_rate(100, duration),
        Some(100),
        Some(duration),
        None,
    )
}

fn build_pf0(id: &str, _ops: Option<u32>) -> ExperimentPlan {
    plan(
        id,
        ScenarioSpec::cross_node_fanout("testkit.fleet.smoke"),
        None,
        None,
        None,
    )
}

fn build_pf1(id: &str, ops: Option<u32>) -> ExperimentPlan {
    let n = ops.unwrap_or(4).max(1);
    plan(id, ScenarioSpec::multi_subscriber_n(n), None, None, Some(n))
}

fn build_pf2(id: &str, ops: Option<u32>) -> ExperimentPlan {
    let aggregate = ops.unwrap_or(1000);
    let publishers = 4;
    plan(
        id,
        ScenarioSpec::publish_parallel_at_rate(publishers, aggregate, 10),
        Some(aggregate),
        Some(10),
        None,
    )
}

fn build_pf3(id: &str, ops: Option<u32>) -> ExperimentPlan {
    let duration = ops.unwrap_or(60);
    plan(
        id,
        ScenarioSpec::publish_at_rate(1000, duration),
        Some(1000),
        Some(duration),
        None,
    )
}

fn build_pf4(id: &str, ops: Option<u32>) -> ExperimentPlan {
    let streams = ops.unwrap_or(4);
    plan(
        id,
        ScenarioSpec::multi_stream_sustained(streams, 250, 60),
        Some(250 * streams),
        Some(60),
        Some(streams),
    )
}

fn build_pfh(id: &str, ops: Option<u32>) -> ExperimentPlan {
    let aggregate = ops.unwrap_or(10_000);
    let publishers = publishers_from_env().unwrap_or(32);
    let duration = 30;
    plan(
        id,
        ScenarioSpec::firehose_parallel(publishers, aggregate, duration),
        Some(aggregate),
        Some(duration),
        None,
    )
}

fn build_pfs(id: &str, _ops: Option<u32>) -> ExperimentPlan {
    plan(
        id,
        ScenarioSpec::fanout_sustained_at_rate(4, 10_000, 30),
        Some(10_000),
        Some(30),
        Some(4),
    )
}

fn build_pfe(id: &str, _ops: Option<u32>) -> ExperimentPlan {
    plan(
        id,
        ScenarioSpec::executor_sustained_at_rate(1_000, 60),
        Some(1_000),
        Some(60),
        None,
    )
}

fn build_pg0(id: &str, _ops: Option<u32>) -> ExperimentPlan {
    plan(
        id,
        ScenarioSpec::consumer_group_static("testkit.group", "testkit-workers", 8, 200),
        None,
        None,
        Some(2),
    )
}

fn build_pg1(id: &str, ops: Option<u32>) -> ExperimentPlan {
    let count = ops.unwrap_or(2000);
    plan(
        id,
        ScenarioSpec::consumer_group_throughput(2, count),
        None,
        None,
        Some(2),
    )
}

fn build_pg2(id: &str, ops: Option<u32>) -> ExperimentPlan {
    let count = ops.unwrap_or(100);
    plan(
        id,
        ScenarioSpec::consumer_group_rebalance(count),
        None,
        None,
        Some(2),
    )
}

fn publishers_from_env() -> Option<u32> {
    std::env::var("PHOTON_BENCH_PUBLISHERS")
        .ok()
        .and_then(|v| v.parse().ok())
        .filter(|&n| n > 0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::experiments::registry::REGISTRY;

    #[test]
    fn every_registry_id_has_a_spec() {
        for meta in REGISTRY {
            assert!(
                build_plan(meta.id, None).is_some(),
                "missing spec for {}",
                meta.id
            );
        }
    }
}
