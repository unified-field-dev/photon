//! Scenario correctness tests over the CI matrix slice.
#![allow(unused_macros)]

use photon_testkit::{BootstrapSession, MatrixSpec, RunMode, ScenarioRunner, ScenarioSpec};

async fn run_scenario_sync(matrix: MatrixSpec, spec: ScenarioSpec) {
    let mut session = BootstrapSession::new(matrix);
    session.install().expect("sync bootstrap");
    run_scenario_on_session(&session, spec).await;
}

#[cfg(any(
    feature = "nats",
    feature = "kafka",
    feature = "fluvio",
    feature = "sqlite"
))]
async fn run_scenario_async(matrix: MatrixSpec, spec: ScenarioSpec) {
    let mut session = BootstrapSession::new(matrix);
    session.install_async().await.expect("async bootstrap");
    run_scenario_on_session(&session, spec).await;
}

macro_rules! sqlite_embedded_scenarios {
    ($($name:ident => $spec:expr),* $(,)?) => {
        $(
            #[cfg(feature = "sqlite")]
            #[tokio::test(flavor = "multi_thread")]
            async fn $name() {
                run_scenario_async(MatrixSpec::ci_sqlite_embedded(), $spec).await;
            }
        )*
    };
}

macro_rules! fluvio_broker_scenarios {
    ($($name:ident => $spec:expr),* $(,)?) => {
        $(
            #[cfg(feature = "fluvio")]
            #[tokio::test(flavor = "multi_thread")]
            #[ignore = "requires PHOTON_FLUVIO_ENDPOINT — run with: cargo test -p photon-e2e --features fluvio -- --ignored"]
            async fn $name() {
                if std::env::var("PHOTON_FLUVIO_ENDPOINT").is_err() {
                    return;
                }
                run_scenario_async(MatrixSpec::ci_fluvio_broker(), $spec).await;
            }
        )*
    };
}

macro_rules! kafka_broker_scenarios {
    ($($name:ident => $spec:expr),* $(,)?) => {
        $(
            #[cfg(feature = "kafka")]
            #[tokio::test(flavor = "multi_thread")]
            #[ignore = "requires PHOTON_KAFKA_BROKERS — run with: cargo test -p photon-e2e --features kafka -- --ignored"]
            async fn $name() {
                if std::env::var("PHOTON_KAFKA_BROKERS").is_err() {
                    return;
                }
                run_scenario_async(MatrixSpec::ci_kafka_broker(), $spec).await;
            }
        )*
    };
}

async fn run_scenario_on_session(session: &BootstrapSession, spec: ScenarioSpec) {
    let result = ScenarioRunner::new(session)
        .run(&spec, RunMode::Correctness)
        .await
        .expect("scenario run");
    assert!(
        result.error.is_none(),
        "scenario {} failed: {:?}",
        result.scenario_id,
        result.error
    );
}

macro_rules! mem_embedded_scenarios {
    ($($name:ident => $spec:expr),* $(,)?) => {
        $(
            #[tokio::test(flavor = "multi_thread")]
            async fn $name() {
                run_scenario_sync(MatrixSpec::ci_mem_embedded(), $spec).await;
            }
        )*
    };
}

macro_rules! nats_broker_scenarios {
    ($($name:ident => $spec:expr),* $(,)?) => {
        $(
            #[cfg(feature = "nats")]
            #[tokio::test(flavor = "multi_thread")]
            #[ignore = "requires PHOTON_NATS_URL — run with: cargo test -p photon-e2e --features nats -- --ignored"]
            async fn $name() {
                if std::env::var("PHOTON_NATS_URL").is_err() {
                    return;
                }
                run_scenario_async(MatrixSpec::ci_nats_broker(), $spec).await;
            }
        )*
    };
}

mem_embedded_scenarios! {
    publish_subscribe_smoke_mem_embedded => ScenarioSpec::publish_subscribe_smoke(),
    consumer_group_static_mem_embedded => ScenarioSpec::consumer_group_static("testkit.group", "testkit-workers", 8, 200),
    consumer_group_round_robin_mem_embedded => ScenarioSpec::consumer_group_round_robin("testkit.group.rr", "testkit-workers-rr", 8, 200),
    consumer_group_rebalance_mem_embedded => ScenarioSpec::consumer_group_rebalance(200),
    after_seq_replay_mem_embedded => ScenarioSpec::after_seq_replay(),
    multi_subscriber_mem_embedded => ScenarioSpec::multi_subscriber(),
    fanout_multi_subscriber_mem_embedded => ScenarioSpec::fanout_multi_subscriber(4, 10),
    executor_fanout_dispatch_mem_embedded => ScenarioSpec::executor_sustained_dispatch(10),
    keyed_filter_mem_embedded => ScenarioSpec::keyed_filter(),
    keyed_filter_miss_mem_embedded => ScenarioSpec::keyed_filter_miss(),
    ephemeral_tail_only_mem_embedded => ScenarioSpec::ephemeral_tail_only(),
    checkpoint_resume_mem_embedded => ScenarioSpec::checkpoint_resume(),
    cross_node_fanout_mem_embedded => ScenarioSpec::cross_node_fanout("testkit.fanout.mem"),
}

sqlite_embedded_scenarios! {
    publish_subscribe_smoke_sqlite_embedded => ScenarioSpec::publish_subscribe_smoke(),
    consumer_group_static_sqlite_embedded => ScenarioSpec::consumer_group_static("testkit.group", "testkit-workers", 8, 200),
    consumer_group_round_robin_sqlite_embedded => ScenarioSpec::consumer_group_round_robin("testkit.group.rr", "testkit-workers-rr", 8, 200),
    consumer_group_rebalance_sqlite_embedded => ScenarioSpec::consumer_group_rebalance(200),
    after_seq_replay_sqlite_embedded => ScenarioSpec::after_seq_replay(),
    multi_subscriber_sqlite_embedded => ScenarioSpec::multi_subscriber(),
    fanout_multi_subscriber_sqlite_embedded => ScenarioSpec::fanout_multi_subscriber(4, 10),
    executor_fanout_dispatch_sqlite_embedded => ScenarioSpec::executor_sustained_dispatch(10),
    keyed_filter_sqlite_embedded => ScenarioSpec::keyed_filter(),
    keyed_filter_miss_sqlite_embedded => ScenarioSpec::keyed_filter_miss(),
    ephemeral_tail_only_sqlite_embedded => ScenarioSpec::ephemeral_tail_only(),
    checkpoint_resume_sqlite_embedded => ScenarioSpec::checkpoint_resume(),
    cross_node_fanout_sqlite_embedded => ScenarioSpec::cross_node_fanout("testkit.fanout.sqlite"),
}

nats_broker_scenarios! {
    publish_subscribe_smoke_nats_broker => ScenarioSpec::publish_subscribe_smoke(),
    consumer_group_static_nats_broker => ScenarioSpec::consumer_group_static("testkit.group", "testkit-workers", 8, 200),
    consumer_group_round_robin_nats_broker => ScenarioSpec::consumer_group_round_robin("testkit.group.rr", "testkit-workers-rr", 8, 200),
    consumer_group_rebalance_nats_broker => ScenarioSpec::consumer_group_rebalance(200),
    after_seq_replay_nats_broker => ScenarioSpec::after_seq_replay(),
    multi_subscriber_nats_broker => ScenarioSpec::multi_subscriber(),
    fanout_multi_subscriber_nats_broker => ScenarioSpec::fanout_multi_subscriber(4, 10),
    executor_fanout_dispatch_nats_broker => ScenarioSpec::executor_sustained_dispatch(10),
    keyed_filter_nats_broker => ScenarioSpec::keyed_filter(),
    keyed_filter_miss_nats_broker => ScenarioSpec::keyed_filter_miss(),
    checkpoint_resume_nats_broker => ScenarioSpec::checkpoint_resume(),
    cross_node_fanout_nats_broker => ScenarioSpec::cross_node_fanout("testkit.fanout.nats"),
}

fluvio_broker_scenarios! {
    publish_subscribe_smoke_fluvio_broker => ScenarioSpec::publish_subscribe_smoke(),
    consumer_group_static_fluvio_broker => ScenarioSpec::consumer_group_static("testkit.group", "testkit-workers", 8, 200),
    consumer_group_round_robin_fluvio_broker => ScenarioSpec::consumer_group_round_robin("testkit.group.rr", "testkit-workers-rr", 8, 200),
    consumer_group_rebalance_fluvio_broker => ScenarioSpec::consumer_group_rebalance(200),
    after_seq_replay_fluvio_broker => ScenarioSpec::after_seq_replay(),
    multi_subscriber_fluvio_broker => ScenarioSpec::multi_subscriber(),
    fanout_multi_subscriber_fluvio_broker => ScenarioSpec::fanout_multi_subscriber(4, 10),
    executor_fanout_dispatch_fluvio_broker => ScenarioSpec::executor_sustained_dispatch(10),
    keyed_filter_fluvio_broker => ScenarioSpec::keyed_filter(),
    keyed_filter_miss_fluvio_broker => ScenarioSpec::keyed_filter_miss(),
    checkpoint_resume_fluvio_broker => ScenarioSpec::checkpoint_resume(),
    cross_node_fanout_fluvio_broker => ScenarioSpec::cross_node_fanout("testkit.fanout.fluvio"),
}

kafka_broker_scenarios! {
    publish_subscribe_smoke_kafka_broker => ScenarioSpec::publish_subscribe_smoke(),
    consumer_group_static_kafka_broker => ScenarioSpec::consumer_group_static("testkit.group", "testkit-workers", 8, 200),
    consumer_group_round_robin_kafka_broker => ScenarioSpec::consumer_group_round_robin("testkit.group.rr", "testkit-workers-rr", 8, 200),
    consumer_group_rebalance_kafka_broker => ScenarioSpec::consumer_group_rebalance(200),
    after_seq_replay_kafka_broker => ScenarioSpec::after_seq_replay(),
    multi_subscriber_kafka_broker => ScenarioSpec::multi_subscriber(),
    fanout_multi_subscriber_kafka_broker => ScenarioSpec::fanout_multi_subscriber(4, 10),
    executor_fanout_dispatch_kafka_broker => ScenarioSpec::executor_sustained_dispatch(10),
    keyed_filter_kafka_broker => ScenarioSpec::keyed_filter(),
    keyed_filter_miss_kafka_broker => ScenarioSpec::keyed_filter_miss(),
    checkpoint_resume_kafka_broker => ScenarioSpec::checkpoint_resume(),
    cross_node_fanout_kafka_broker => ScenarioSpec::cross_node_fanout("testkit.fanout.kafka"),
}

macro_rules! broker_tail_only_scenario {
    ($name:ident, $env_var:literal, $replay_cursor_env:literal, $matrix:expr) => {
        #[tokio::test(flavor = "multi_thread")]
        #[ignore = concat!("requires ", $env_var, " and ", $replay_cursor_env, "=tail_only")]
        async fn $name() {
            if std::env::var($env_var).is_err() {
                return;
            }
            std::env::set_var($replay_cursor_env, "tail_only");
            run_scenario_async($matrix, ScenarioSpec::ephemeral_tail_only()).await;
        }
    };
}

#[cfg(feature = "nats")]
broker_tail_only_scenario!(
    ephemeral_tail_only_nats_broker,
    "PHOTON_NATS_URL",
    "PHOTON_NATS_REPLAY_CURSOR",
    MatrixSpec::ci_nats_broker()
);
#[cfg(feature = "kafka")]
broker_tail_only_scenario!(
    ephemeral_tail_only_kafka_broker,
    "PHOTON_KAFKA_BROKERS",
    "PHOTON_KAFKA_REPLAY_CURSOR",
    MatrixSpec::ci_kafka_broker()
);
#[cfg(feature = "fluvio")]
broker_tail_only_scenario!(
    ephemeral_tail_only_fluvio_broker,
    "PHOTON_FLUVIO_ENDPOINT",
    "PHOTON_FLUVIO_REPLAY_CURSOR",
    MatrixSpec::ci_fluvio_broker()
);

macro_rules! topology_smoke {
    ($name:ident, $topology:expr) => {
        #[tokio::test(flavor = "multi_thread")]
        async fn $name() {
            run_scenario_sync(
                MatrixSpec::ci_mem_embedded().with_topology($topology),
                ScenarioSpec::publish_subscribe_smoke(),
            )
            .await;
        }
    };
}

macro_rules! telemetry_smoke {
    ($name:ident, $telemetry:expr) => {
        #[tokio::test(flavor = "multi_thread")]
        async fn $name() {
            run_scenario_sync(
                MatrixSpec::ci_mem_embedded().with_telemetry($telemetry),
                ScenarioSpec::publish_subscribe_smoke(),
            )
            .await;
        }
    };
}

topology_smoke!(
    publish_subscribe_smoke_topology_embedded_composite,
    photon_testkit::Topology::EmbeddedComposite
);
topology_smoke!(
    publish_subscribe_smoke_topology_split_runtime,
    photon_testkit::Topology::SplitRuntime
);

telemetry_smoke!(
    publish_subscribe_smoke_telemetry_console,
    photon_testkit::TelemetryAdapter::Console
);
telemetry_smoke!(
    publish_subscribe_smoke_telemetry_structured_ops_log,
    photon_testkit::TelemetryAdapter::StructuredOpsLog
);
telemetry_smoke!(
    publish_subscribe_smoke_telemetry_external_ops_log,
    photon_testkit::TelemetryAdapter::ExternalOpsLog
);
