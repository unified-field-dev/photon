//! Declarative scenario steps shared by e2e (assert) and bench (measure).

use serde::{Deserialize, Serialize};

/// One step in a publish/subscribe scenario.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "step", rename_all = "snake_case")]
pub enum ScenarioStep {
    /// Publish `count` events to `topic` (optional partition key).
    PublishN {
        /// Topic name.
        topic: String,
        /// Number of events to publish.
        count: u32,
        /// Whether publishes use a partition key.
        keyed: bool,
        /// Partition key when `keyed` is true.
        #[serde(default)]
        topic_key: Option<String>,
    },
    /// Open a subscription (optional replay `after_seq` and partition filter).
    SubscribeDurable {
        /// Topic name.
        topic: String,
        /// Replay from this sequence (inclusive of later events).
        after_seq: Option<i64>,
        /// Optional partition key filter.
        #[serde(default)]
        topic_key_filter: Option<String>,
        /// When `true`, keep prior subscription tasks and delivery counter.
        #[serde(default)]
        accumulate: bool,
        /// Artificial per-delivery delay for executor pressure scenarios (BM-P8).
        #[serde(default)]
        delivery_delay_ms: Option<u32>,
    },
    /// Tear down and rebuild Photon on the same storage port.
    RestartRuntime,
    /// Expect at least `expected_count` deliveries since the last non-accumulating subscribe.
    AssertDelivery {
        /// Minimum deliveries required.
        expected_count: u32,
    },
    /// Publish `count` times and assert each returns a non-empty, unique `event_id`.
    AssertPublishEventIds {
        /// Topic name.
        topic: String,
        /// Number of publishes to probe.
        count: u32,
        /// Whether publishes use a partition key.
        keyed: bool,
        /// Partition key when `keyed` is true.
        #[serde(default)]
        topic_key: Option<String>,
    },
    /// Publish at approximately `rate_per_sec` for `duration_secs` (BM-P3 / BM-PL*).
    PublishAtRate {
        /// Topic name.
        topic: String,
        /// Target publishes per second.
        rate_per_sec: u32,
        /// How long to sustain the rate.
        duration_secs: u32,
        /// Whether publishes use a partition key.
        keyed: bool,
        /// Partition key when `keyed` is true.
        #[serde(default)]
        topic_key: Option<String>,
    },
    /// Exercise cross-node fanout via shared broker storage.
    CrossNodeFanout {
        /// Topic name.
        topic: String,
    },
    /// Static two-member consumer group with full shard coverage (BM-PG0).
    ConsumerGroupStatic {
        /// Group topic name.
        topic: String,
        /// Consumer group id.
        group: String,
        /// Total shard count.
        shard_count: u32,
        /// Events to publish (0 triggers verify-only when prior publishes exist).
        publish_count: u32,
    },
    /// Round-robin shard assignment across group members.
    ConsumerGroupRoundRobin {
        /// Group topic name.
        topic: String,
        /// Consumer group id.
        group: String,
        /// Total shard count.
        shard_count: u32,
        /// Number of group members.
        member_count: u32,
        /// Events to publish.
        publish_count: u32,
    },
    /// Wait then assert no deliveries occurred (sad-path / tail-only).
    AssertNoDelivery {
        /// Milliseconds to wait before asserting zero deliveries.
        wait_ms: u32,
    },
    /// Parallel multi-topic sustained publish (BM-PF4).
    ParallelMultiStream {
        /// Number of independent topics.
        stream_count: u32,
        /// Target publish rate per topic.
        rate_per_stream: u32,
        /// Duration per parallel publish loop.
        duration_secs: u32,
    },
    /// Parallel publishers on one topic at aggregate rate (BM-PF2).
    ParallelPublishAtRate {
        /// Topic name.
        topic: String,
        /// Number of concurrent publisher tasks.
        publishers: u32,
        /// Combined target publishes per second across all publishers.
        aggregate_rate: u32,
        /// How long to sustain the aggregate rate.
        duration_secs: u32,
    },
    /// Single-topic firehose — all publishers share one unkeyed topic (BM-PFH).
    FirehoseParallel {
        /// Topic name.
        topic: String,
        /// Number of concurrent publisher tasks on the same topic.
        publishers: u32,
        /// Combined target publishes per second.
        aggregate_rate: u32,
        /// Sustain duration in seconds.
        duration_secs: u32,
    },
    /// Start the handler executor (requires inventory-registered handlers).
    StartExecutor,
    /// Expect the executor fixture handler ran at least `expected_count` times.
    AssertExecutorInvocations {
        /// Minimum handler invocations required.
        expected_count: u32,
    },
}

/// Ordered scenario consumed by both drivers.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ScenarioSpec {
    /// Stable scenario identifier.
    pub id: String,
    /// Ordered steps to execute.
    pub steps: Vec<ScenarioStep>,
}

impl ScenarioSpec {
    /// Publish + 1 subscriber bench (BM-P1).
    #[must_use]
    pub fn publish_subscribe_bench(count: u32) -> Self {
        Self {
            id: format!("publish-subscribe-bench-{count}"),
            steps: vec![
                ScenarioStep::SubscribeDurable {
                    topic: "testkit.bench".into(),
                    after_seq: None,
                    topic_key_filter: None,
                    accumulate: false,
                    delivery_delay_ms: None,
                },
                ScenarioStep::PublishN {
                    topic: "testkit.bench".into(),
                    count,
                    keyed: false,
                    topic_key: None,
                },
                ScenarioStep::AssertDelivery {
                    expected_count: count,
                },
            ],
        }
    }

    /// Subscribe + sustained publish for append-buffer stress (BM-P3).
    #[must_use]
    pub fn append_buffer_stress(rate_per_sec: u32, duration_secs: u32) -> Self {
        Self {
            id: format!("append-buffer-{rate_per_sec}-{duration_secs}"),
            steps: vec![
                ScenarioStep::SubscribeDurable {
                    topic: "testkit.load".into(),
                    after_seq: None,
                    topic_key_filter: None,
                    accumulate: false,
                    delivery_delay_ms: None,
                },
                ScenarioStep::PublishAtRate {
                    topic: "testkit.load".into(),
                    rate_per_sec,
                    duration_secs,
                    keyed: false,
                    topic_key: None,
                },
            ],
        }
    }

    /// Publish N, restart, replay from store (BM-P4).
    #[must_use]
    pub fn checkpoint_replay_throughput(count: u32) -> Self {
        Self {
            id: format!("checkpoint-replay-{count}"),
            steps: vec![
                ScenarioStep::PublishN {
                    topic: "testkit.replay".into(),
                    count,
                    keyed: false,
                    topic_key: None,
                },
                ScenarioStep::RestartRuntime,
                ScenarioStep::SubscribeDurable {
                    topic: "testkit.replay".into(),
                    after_seq: Some(0),
                    topic_key_filter: None,
                    accumulate: false,
                    delivery_delay_ms: None,
                },
                ScenarioStep::AssertDelivery {
                    expected_count: count,
                },
            ],
        }
    }

    /// Key cardinality sweep with partition filter (BM-P5).
    #[must_use]
    pub fn keyed_filter_cardinality() -> Self {
        let topic = "testkit.keyed".to_string();
        let mut steps = vec![ScenarioStep::SubscribeDurable {
            topic: topic.clone(),
            after_seq: None,
            topic_key_filter: Some("key-a".into()),
            accumulate: false,
            delivery_delay_ms: None,
        }];
        for card in [1u32, 10, 100, 1000] {
            for i in 0..card {
                let key = if i == 0 {
                    "key-a".to_string()
                } else {
                    format!("key-{i}")
                };
                steps.push(ScenarioStep::PublishN {
                    topic: topic.clone(),
                    count: 1,
                    keyed: true,
                    topic_key: Some(key),
                });
            }
        }
        steps.push(ScenarioStep::AssertDelivery { expected_count: 4 });
        Self {
            id: "keyed-filter-cardinality".into(),
            steps,
        }
    }

    /// Durable handler + checkpoint path at volume (BM-P7).
    #[must_use]
    pub fn handler_checkpoint_stress(count: u32) -> Self {
        Self {
            id: format!("handler-checkpoint-{count}"),
            steps: vec![
                ScenarioStep::PublishN {
                    topic: "testkit.handler".into(),
                    count,
                    keyed: false,
                    topic_key: None,
                },
                ScenarioStep::SubscribeDurable {
                    topic: "testkit.handler".into(),
                    after_seq: Some(0),
                    topic_key_filter: None,
                    accumulate: false,
                    delivery_delay_ms: Some(1),
                },
                ScenarioStep::RestartRuntime,
                ScenarioStep::SubscribeDurable {
                    topic: "testkit.handler".into(),
                    after_seq: Some(0),
                    topic_key_filter: None,
                    accumulate: false,
                    delivery_delay_ms: None,
                },
                ScenarioStep::AssertDelivery {
                    expected_count: count,
                },
            ],
        }
    }

    /// Slow consumer + high publish rate (BM-P8).
    #[must_use]
    pub fn executor_pressure(rate_per_sec: u32, duration_secs: u32) -> Self {
        Self {
            id: format!("executor-pressure-{rate_per_sec}-{duration_secs}"),
            steps: vec![
                ScenarioStep::SubscribeDurable {
                    topic: "testkit.exec".into(),
                    after_seq: None,
                    topic_key_filter: None,
                    accumulate: false,
                    delivery_delay_ms: Some(5),
                },
                ScenarioStep::PublishAtRate {
                    topic: "testkit.exec".into(),
                    rate_per_sec,
                    duration_secs,
                    keyed: false,
                    topic_key: None,
                },
            ],
        }
    }

    /// Cross-node fanout lab scenario (BM-P6 scaffold).
    #[must_use]
    pub fn cross_node_fanout(topic: &str) -> Self {
        Self {
            id: "cross-node-fanout".into(),
            steps: vec![ScenarioStep::CrossNodeFanout {
                topic: topic.into(),
            }],
        }
    }

    /// Static 2-member consumer group, disjoint shards, full coverage (BM-PG0).
    #[must_use]
    pub fn consumer_group_static(
        topic: &str,
        group: &str,
        shard_count: u32,
        publish_count: u32,
    ) -> Self {
        Self {
            id: "consumer-group-static".into(),
            steps: vec![ScenarioStep::ConsumerGroupStatic {
                topic: topic.into(),
                group: group.into(),
                shard_count,
                publish_count,
            }],
        }
    }

    /// Throughput scaling with N group members (BM-PG1).
    #[must_use]
    pub fn consumer_group_throughput(member_count: u32, publish_count: u32) -> Self {
        Self {
            id: format!("consumer-group-throughput-{member_count}"),
            steps: vec![ScenarioStep::ConsumerGroupStatic {
                topic: "testkit.group".into(),
                group: "testkit-workers".into(),
                shard_count: 8,
                publish_count,
            }],
        }
    }

    /// Rebalance — publish, restart runtime, verify group replay (BM-PG2).
    ///
    /// Uses a dedicated inventory-registered topic (`testkit.group.rebalance`) so serial
    /// broker suites do not share stream state with `consumer_group_static`.
    #[must_use]
    pub fn consumer_group_rebalance(publish_count: u32) -> Self {
        Self {
            id: "consumer-group-rebalance".into(),
            steps: vec![
                ScenarioStep::ConsumerGroupStatic {
                    topic: "testkit.group.rebalance".into(),
                    group: "testkit-workers-rebalance".into(),
                    shard_count: 8,
                    publish_count,
                },
                ScenarioStep::RestartRuntime,
                ScenarioStep::ConsumerGroupStatic {
                    topic: "testkit.group.rebalance".into(),
                    group: "testkit-workers-rebalance".into(),
                    shard_count: 8,
                    publish_count: 0,
                },
            ],
        }
    }

    /// Multi-stream sustained load (BM-PF4).
    #[must_use]
    pub fn multi_stream_sustained(streams: u32, rate_per_stream: u32, duration_secs: u32) -> Self {
        Self {
            id: format!("multi-stream-{streams}-{rate_per_stream}-{duration_secs}"),
            steps: vec![ScenarioStep::ParallelMultiStream {
                stream_count: streams,
                rate_per_stream,
                duration_secs,
            }],
        }
    }

    /// Minimal publish + subscribe smoke (Phase 5 default).
    #[must_use]
    pub fn publish_subscribe_smoke() -> Self {
        // Unique topic: shared `testkit.smoke` races under parallel Fluvio matrix runs.
        let suffix = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| d.as_nanos());
        let topic = format!("testkit.smoke.{suffix}");
        Self {
            id: "publish-subscribe-smoke".into(),
            steps: vec![
                ScenarioStep::SubscribeDurable {
                    topic: topic.clone(),
                    after_seq: None,
                    topic_key_filter: None,
                    accumulate: false,
                    delivery_delay_ms: None,
                },
                ScenarioStep::PublishN {
                    topic: topic.clone(),
                    count: 1,
                    keyed: false,
                    topic_key: None,
                },
                ScenarioStep::AssertPublishEventIds {
                    topic,
                    count: 1,
                    keyed: false,
                    topic_key: None,
                },
                // PublishN (1) + AssertPublishEventIds probe publish (1).
                // Live-tail readiness probe deliveries are reset before PublishN.
                ScenarioStep::AssertDelivery { expected_count: 2 },
            ],
        }
    }

    /// Publish-only workload (BM-P0).
    #[must_use]
    pub fn publish_only(count: u32) -> Self {
        Self {
            id: "publish-only".into(),
            steps: vec![ScenarioStep::PublishN {
                topic: "testkit.bench".into(),
                count,
                keyed: false,
                topic_key: None,
            }],
        }
    }

    /// Subscribe with `after_seq` replays only events after the given sequence.
    #[must_use]
    pub fn after_seq_replay() -> Self {
        Self {
            id: "after-seq-replay".into(),
            steps: vec![
                ScenarioStep::PublishN {
                    topic: "testkit.replay".into(),
                    count: 3,
                    keyed: false,
                    topic_key: None,
                },
                ScenarioStep::SubscribeDurable {
                    topic: "testkit.replay".into(),
                    after_seq: Some(2),
                    topic_key_filter: None,
                    accumulate: false,
                    delivery_delay_ms: None,
                },
                ScenarioStep::AssertDelivery { expected_count: 1 },
            ],
        }
    }

    /// N subscribers on the same topic receive the same publish (BM-P2).
    #[must_use]
    pub fn multi_subscriber_n(n: u32) -> Self {
        let suffix = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| d.as_nanos());
        let topic = format!("testkit.multi.{suffix}");
        let mut steps = Vec::new();
        for i in 0..n {
            steps.push(ScenarioStep::SubscribeDurable {
                topic: topic.clone(),
                after_seq: None,
                topic_key_filter: None,
                accumulate: i > 0,
                delivery_delay_ms: None,
            });
        }
        steps.push(ScenarioStep::PublishN {
            topic,
            count: 1,
            keyed: false,
            topic_key: None,
        });
        steps.push(ScenarioStep::AssertDelivery { expected_count: n });
        Self {
            id: format!("multi-subscriber-{n}"),
            steps,
        }
    }

    /// Sustained publish at target rate (BM-PL0–PL3 / BM-P3).
    #[must_use]
    pub fn publish_at_rate(rate_per_sec: u32, duration_secs: u32) -> Self {
        Self {
            id: format!("publish-at-rate-{rate_per_sec}-{duration_secs}"),
            steps: vec![ScenarioStep::PublishAtRate {
                topic: "testkit.load".into(),
                rate_per_sec,
                duration_secs,
                keyed: false,
                topic_key: None,
            }],
        }
    }

    /// M parallel publishers on one topic at aggregate rate (BM-PF2).
    #[must_use]
    pub fn publish_parallel_at_rate(
        publishers: u32,
        aggregate_rate: u32,
        duration_secs: u32,
    ) -> Self {
        Self {
            id: format!("parallel-publish-{publishers}x{aggregate_rate}-{duration_secs}"),
            steps: vec![ScenarioStep::ParallelPublishAtRate {
                topic: "testkit.load".into(),
                publishers,
                aggregate_rate,
                duration_secs,
            }],
        }
    }

    /// Single-topic firehose — all publishers on one unkeyed topic (BM-PFH).
    #[must_use]
    pub fn firehose_parallel(publishers: u32, aggregate_rate: u32, duration_secs: u32) -> Self {
        Self {
            id: format!("firehose-{publishers}x{aggregate_rate}-{duration_secs}"),
            steps: vec![ScenarioStep::FirehoseParallel {
                topic: "testkit.firehose".into(),
                publishers,
                aggregate_rate,
                duration_secs,
            }],
        }
    }

    /// Two subscribers on the same topic receive the same publish.
    #[must_use]
    pub fn multi_subscriber() -> Self {
        Self::multi_subscriber_n(2)
    }

    /// N ephemeral subscribers each receive all published events (fanout correctness / BM-PFS slice).
    #[must_use]
    pub fn fanout_multi_subscriber(subscriber_count: u32, publish_count: u32) -> Self {
        // Unique topic avoids leftover Fluvio consumer/offset state across serial broker suites.
        let suffix = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| d.as_nanos());
        let topic = format!("testkit.fanout.multi.{suffix}");
        let mut steps = Vec::new();
        for i in 0..subscriber_count {
            steps.push(ScenarioStep::SubscribeDurable {
                topic: topic.clone(),
                after_seq: None,
                topic_key_filter: None,
                accumulate: i > 0,
                delivery_delay_ms: None,
            });
        }
        steps.push(ScenarioStep::PublishN {
            topic,
            count: publish_count,
            keyed: false,
            topic_key: None,
        });
        steps.push(ScenarioStep::AssertDelivery {
            expected_count: subscriber_count.saturating_mul(publish_count),
        });
        Self {
            id: format!("fanout-multi-subscriber-{subscriber_count}x{publish_count}"),
            steps,
        }
    }

    /// Sustained publish with N fanout subscribers (BM-PFS).
    #[must_use]
    pub fn fanout_sustained_at_rate(
        subscriber_count: u32,
        rate_per_sec: u32,
        duration_secs: u32,
    ) -> Self {
        let topic = "testkit.fanout.load".to_string();
        let mut steps = Vec::new();
        for i in 0..subscriber_count {
            steps.push(ScenarioStep::SubscribeDurable {
                topic: topic.clone(),
                after_seq: None,
                topic_key_filter: None,
                accumulate: i > 0,
                delivery_delay_ms: None,
            });
        }
        steps.push(ScenarioStep::PublishAtRate {
            topic,
            rate_per_sec,
            duration_secs,
            keyed: false,
            topic_key: None,
        });
        Self {
            id: format!("fanout-sustained-{subscriber_count}-{rate_per_sec}-{duration_secs}"),
            steps,
        }
    }

    /// Executor + sustained publish on fixture topic (BM-PFE).
    #[must_use]
    pub fn executor_sustained_dispatch(publish_count: u32) -> Self {
        use crate::bench_handlers::EXECUTOR_FIXTURE_TOPIC;
        Self {
            id: format!("executor-sustained-{publish_count}"),
            steps: vec![
                ScenarioStep::StartExecutor,
                ScenarioStep::PublishN {
                    topic: EXECUTOR_FIXTURE_TOPIC.into(),
                    count: publish_count,
                    keyed: false,
                    topic_key: None,
                },
                ScenarioStep::AssertExecutorInvocations {
                    expected_count: publish_count,
                },
            ],
        }
    }

    /// Executor + sustained publish at rate (BM-PFE bench on NATS).
    #[must_use]
    pub fn executor_sustained_at_rate(rate_per_sec: u32, duration_secs: u32) -> Self {
        use crate::bench_handlers::EXECUTOR_FIXTURE_TOPIC;
        Self {
            id: format!("executor-sustained-{rate_per_sec}-{duration_secs}"),
            steps: vec![
                ScenarioStep::StartExecutor,
                ScenarioStep::PublishAtRate {
                    topic: EXECUTOR_FIXTURE_TOPIC.into(),
                    rate_per_sec,
                    duration_secs,
                    keyed: false,
                    topic_key: None,
                },
            ],
        }
    }

    /// Partition filter receives only matching keyed publishes.
    #[must_use]
    pub fn keyed_filter() -> Self {
        let suffix = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| d.as_nanos());
        let topic = format!("testkit.keyed.{suffix}");
        Self {
            id: "keyed-filter".into(),
            steps: vec![
                ScenarioStep::SubscribeDurable {
                    topic: topic.clone(),
                    after_seq: None,
                    topic_key_filter: Some("key-a".into()),
                    accumulate: false,
                    delivery_delay_ms: None,
                },
                ScenarioStep::PublishN {
                    topic: topic.clone(),
                    count: 1,
                    keyed: true,
                    topic_key: Some("key-b".into()),
                },
                ScenarioStep::PublishN {
                    topic,
                    count: 1,
                    keyed: true,
                    topic_key: Some("key-a".into()),
                },
                ScenarioStep::AssertDelivery { expected_count: 1 },
            ],
        }
    }

    /// Partition filter excludes non-matching keyed publishes.
    #[must_use]
    pub fn keyed_filter_miss() -> Self {
        let suffix = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| d.as_nanos());
        let topic = format!("testkit.keyed.miss.{suffix}");
        Self {
            id: "keyed-filter-miss".into(),
            steps: vec![
                ScenarioStep::SubscribeDurable {
                    topic: topic.clone(),
                    after_seq: None,
                    topic_key_filter: Some("key-a".into()),
                    accumulate: false,
                    delivery_delay_ms: None,
                },
                ScenarioStep::PublishN {
                    topic,
                    count: 1,
                    keyed: true,
                    topic_key: Some("key-b".into()),
                },
                ScenarioStep::AssertNoDelivery { wait_ms: 500 },
            ],
        }
    }

    /// Ephemeral tail-only subscription does not replay pre-subscribe history.
    #[must_use]
    pub fn ephemeral_tail_only() -> Self {
        Self {
            id: "ephemeral-tail-only".into(),
            steps: vec![
                ScenarioStep::PublishN {
                    topic: "testkit.ephemeral".into(),
                    count: 1,
                    keyed: false,
                    topic_key: None,
                },
                ScenarioStep::SubscribeDurable {
                    topic: "testkit.ephemeral".into(),
                    after_seq: None,
                    topic_key_filter: None,
                    accumulate: false,
                    delivery_delay_ms: None,
                },
                ScenarioStep::AssertNoDelivery { wait_ms: 500 },
            ],
        }
    }

    /// Round-robin shard assignment with two members (PG0 via env algorithm).
    #[must_use]
    pub fn consumer_group_round_robin(
        topic: &str,
        group: &str,
        shard_count: u32,
        publish_count: u32,
    ) -> Self {
        Self {
            id: "consumer-group-round-robin".into(),
            steps: vec![ScenarioStep::ConsumerGroupRoundRobin {
                topic: topic.into(),
                group: group.into(),
                shard_count,
                member_count: 2,
                publish_count,
            }],
        }
    }

    /// Publish, restart runtime, replay via `after_seq` from durable storage.
    #[must_use]
    pub fn checkpoint_resume() -> Self {
        Self {
            id: "checkpoint-resume".into(),
            steps: vec![
                ScenarioStep::PublishN {
                    topic: "testkit.checkpoint".into(),
                    count: 3,
                    keyed: false,
                    topic_key: None,
                },
                ScenarioStep::SubscribeDurable {
                    topic: "testkit.checkpoint".into(),
                    after_seq: Some(2),
                    topic_key_filter: None,
                    accumulate: false,
                    delivery_delay_ms: None,
                },
                ScenarioStep::AssertDelivery { expected_count: 1 },
                ScenarioStep::RestartRuntime,
                ScenarioStep::SubscribeDurable {
                    topic: "testkit.checkpoint".into(),
                    after_seq: Some(2),
                    topic_key_filter: None,
                    accumulate: false,
                    delivery_delay_ms: None,
                },
                ScenarioStep::AssertDelivery { expected_count: 1 },
            ],
        }
    }
}
