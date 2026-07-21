//! Shared scenario executor for e2e (correctness) and bench (timings).

use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{bail, Result};
use futures::StreamExt;
use photon_core::JsonIdentityFactory;
use photon_runtime::{configure, Photon};

use crate::bench_handlers::{executor_invocation_count, reset_executor_invocations};
use crate::bootstrap::BootstrapSession;
use crate::consumer_group;
use crate::cross_node::{self, FANOUT_PUBLISH_COUNT};
use crate::fixtures::smoke_actor_json;
use crate::scenario::{ScenarioSpec, ScenarioStep};

const DELIVERY_TIMEOUT: Duration = Duration::from_secs(30);

/// Driver mode: assert on delivery counts vs collect timings only.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RunMode {
    /// Fail the run when delivery assertions are not met.
    Correctness,
    /// Collect timings without failing on delivery counts.
    Benchmark,
}

/// Per-step timing samples (milliseconds).
#[derive(Debug, Clone)]
pub struct StepTiming {
    /// Index of the scenario step.
    pub step_index: usize,
    /// Operation label (e.g. `publish`, `delivery_wait`).
    pub op: String,
    /// Per-op samples in milliseconds.
    pub samples_ms: Vec<f64>,
}

/// Sustained-load / replay metrics collected during benchmark runs.
#[derive(Debug, Clone, Default)]
pub struct LoadMetrics {
    /// Successful publish count.
    pub published: u32,
    /// Failed publish count.
    pub publish_errors: u32,
    /// Peak append-buffer backlog observed.
    pub backlog_peak: u64,
    /// Replay throughput after restart, when measured.
    pub replay_events_per_sec: Option<f64>,
    /// Target publish rate for rate-limited steps.
    pub target_rate: Option<u32>,
    /// Duration of rate-limited steps.
    pub duration_secs: Option<u32>,
}

/// Outcome of running one [`ScenarioSpec`].
#[derive(Debug, Clone)]
pub struct ScenarioResult {
    /// Scenario identifier.
    pub scenario_id: String,
    /// Matrix row slug for reports.
    pub matrix_slug: String,
    /// Deliveries observed during the run.
    pub deliveries: u32,
    /// Per-step timing samples.
    pub step_timings: Vec<StepTiming>,
    /// Load metrics when the run exercised publish/replay load.
    pub load: Option<LoadMetrics>,
    /// Failure message when the run did not meet assertions.
    pub error: Option<String>,
}

struct ActiveSubscription {
    _task: tokio::task::JoinHandle<()>,
    /// Partition key filter passed to `subscribe` (None = all keys).
    key_filter: Option<String>,
}

struct RunContext {
    delivery_count: Arc<AtomicU32>,
    subscriptions: Vec<ActiveSubscription>,
    photon: Option<Photon>,
    step_timings: Vec<StepTiming>,
    load: LoadMetrics,
    saw_restart: bool,
    replay_expected: Option<u32>,
}

struct ScenarioRef<'a> {
    spec: &'a ScenarioSpec,
    matrix_slug: &'a str,
}

impl RunContext {
    const fn photon(&self) -> &Photon {
        self.photon.as_ref().expect("photon initialized")
    }

    fn finish_load(&self) -> Option<LoadMetrics> {
        if self.load.published > 0
            || self.load.replay_events_per_sec.is_some()
            || self.load.backlog_peak > 0
            || self.replay_expected.is_some()
        {
            Some(self.load.clone())
        } else {
            None
        }
    }

    fn failure_result(
        &self,
        spec: &ScenarioSpec,
        matrix_slug: String,
        error: String,
    ) -> ScenarioResult {
        ScenarioResult {
            scenario_id: spec.id.clone(),
            matrix_slug,
            deliveries: self.delivery_count.load(Ordering::SeqCst),
            step_timings: self.step_timings.clone(),
            load: Some(self.load.clone()),
            error: Some(error),
        }
    }
}

/// Executes declarative scenarios against a bootstrapped session.
pub struct ScenarioRunner<'a> {
    session: &'a BootstrapSession,
}

impl<'a> ScenarioRunner<'a> {
    /// Bind a runner to an installed bootstrap session.
    #[must_use]
    pub const fn new(session: &'a BootstrapSession) -> Self {
        Self { session }
    }

    /// Run all steps in `spec`, returning timings and delivery counts.
    ///
    /// # Errors
    ///
    /// Returns an error if the operation fails.
    pub async fn run(&self, spec: &ScenarioSpec, mode: RunMode) -> Result<ScenarioResult> {
        if !self.session.is_ready() {
            bail!("BootstrapSession::install must succeed before running scenarios");
        }

        let matrix_slug = self.session.matrix().report_slug();
        let mut ctx = RunContext {
            delivery_count: Arc::new(AtomicU32::new(0)),
            subscriptions: Vec::new(),
            photon: Some(self.session.build_photon()?),
            step_timings: Vec::new(),
            load: LoadMetrics::default(),
            saw_restart: false,
            replay_expected: None,
        };

        for (step_index, step) in spec.steps.iter().enumerate() {
            if let Some(result) = self
                .execute_step(spec, &matrix_slug, step_index, step, mode, &mut ctx)
                .await?
            {
                return Ok(result);
            }
        }

        ctx.subscriptions.clear();
        let deliveries = ctx.delivery_count.load(Ordering::SeqCst);
        let load = ctx.finish_load();
        let step_timings = ctx.step_timings;
        Ok(ScenarioResult {
            scenario_id: spec.id.clone(),
            matrix_slug,
            deliveries,
            step_timings,
            load,
            error: None,
        })
    }

    async fn execute_step(
        &self,
        spec: &ScenarioSpec,
        matrix_slug: &str,
        step_index: usize,
        step: &ScenarioStep,
        mode: RunMode,
        ctx: &mut RunContext,
    ) -> Result<Option<ScenarioResult>> {
        match step {
            ScenarioStep::SubscribeDurable {
                topic,
                after_seq,
                topic_key_filter,
                accumulate,
                delivery_delay_ms,
            } => {
                handle_subscribe_durable(
                    ctx,
                    topic,
                    *after_seq,
                    topic_key_filter.as_deref(),
                    *accumulate,
                    *delivery_delay_ms,
                )
                .await;
            }
            ScenarioStep::PublishN {
                topic,
                count,
                keyed,
                topic_key,
            } => {
                handle_publish_n(ctx, step_index, topic, *count, *keyed, topic_key.as_deref())
                    .await?;
            }
            ScenarioStep::RestartRuntime => {
                handle_restart_runtime(self, ctx).await?;
            }
            ScenarioStep::AssertDelivery { expected_count } => {
                if let Some(result) = handle_assert_delivery(
                    spec,
                    matrix_slug,
                    step_index,
                    mode,
                    ctx,
                    *expected_count,
                )
                .await
                {
                    return Ok(Some(result));
                }
            }
            ScenarioStep::AssertPublishEventIds {
                topic,
                count,
                keyed,
                topic_key,
            } => {
                if let Some(result) = handle_assert_publish_event_ids(
                    ScenarioRef { spec, matrix_slug },
                    mode,
                    ctx,
                    topic,
                    *count,
                    *keyed,
                    topic_key.as_deref(),
                )
                .await?
                {
                    return Ok(Some(result));
                }
            }
            ScenarioStep::PublishAtRate {
                topic,
                rate_per_sec,
                duration_secs,
                keyed,
                topic_key,
            } => {
                handle_publish_at_rate(
                    ctx,
                    step_index,
                    topic,
                    *rate_per_sec,
                    *duration_secs,
                    *keyed,
                    topic_key.as_deref(),
                )
                .await?;
            }
            ScenarioStep::CrossNodeFanout { topic } => {
                if let Some(result) =
                    handle_cross_node_fanout(self, spec, matrix_slug, step_index, ctx, topic)
                        .await?
                {
                    return Ok(Some(result));
                }
            }
            ScenarioStep::ConsumerGroupStatic {
                topic,
                group: _group,
                shard_count,
                publish_count,
            } => {
                if let Some(result) = handle_consumer_group_static(
                    spec,
                    matrix_slug,
                    step_index,
                    ctx,
                    topic,
                    *shard_count,
                    *publish_count,
                )
                .await?
                {
                    return Ok(Some(result));
                }
            }
            ScenarioStep::ConsumerGroupRoundRobin {
                topic,
                group: _group,
                shard_count,
                member_count,
                publish_count,
            } => {
                if let Some(result) = handle_consumer_group_round_robin(
                    spec,
                    matrix_slug,
                    step_index,
                    ctx,
                    topic,
                    *shard_count,
                    *member_count,
                    *publish_count,
                )
                .await?
                {
                    return Ok(Some(result));
                }
            }
            ScenarioStep::AssertNoDelivery { wait_ms } => {
                if let Some(result) =
                    handle_assert_no_delivery(spec, matrix_slug, step_index, mode, ctx, *wait_ms)
                        .await
                {
                    return Ok(Some(result));
                }
            }
            ScenarioStep::ParallelMultiStream {
                stream_count,
                rate_per_stream,
                duration_secs,
            } => {
                handle_parallel_multi_stream(
                    ctx,
                    step_index,
                    *stream_count,
                    *rate_per_stream,
                    *duration_secs,
                )
                .await?;
            }
            ScenarioStep::ParallelPublishAtRate {
                topic,
                publishers,
                aggregate_rate,
                duration_secs,
            } => {
                handle_parallel_publish_at_rate(
                    ctx,
                    step_index,
                    topic,
                    *publishers,
                    *aggregate_rate,
                    *duration_secs,
                )
                .await?;
            }
            ScenarioStep::FirehoseParallel {
                topic,
                publishers,
                aggregate_rate,
                duration_secs,
            } => {
                handle_firehose_parallel(
                    ctx,
                    step_index,
                    topic,
                    *publishers,
                    *aggregate_rate,
                    *duration_secs,
                )
                .await?;
            }
            ScenarioStep::StartExecutor => {
                handle_start_executor(ctx).await?;
            }
            ScenarioStep::AssertExecutorInvocations { expected_count } => {
                if let Some(result) = handle_assert_executor_invocations(
                    spec,
                    matrix_slug,
                    mode,
                    ctx,
                    *expected_count,
                )
                .await
                {
                    return Ok(Some(result));
                }
            }
        }
        Ok(None)
    }
}

async fn handle_subscribe_durable(
    ctx: &mut RunContext,
    topic: &str,
    after_seq: Option<i64>,
    topic_key_filter: Option<&str>,
    accumulate: bool,
    delivery_delay_ms: Option<u32>,
) {
    if !accumulate {
        ctx.subscriptions.clear();
        ctx.delivery_count.store(0, Ordering::SeqCst);
    }
    ctx.subscriptions.push(spawn_subscription(
        Arc::new(ctx.photon().clone()),
        topic.to_string(),
        topic_key_filter.map(str::to_owned),
        after_seq,
        delivery_delay_ms,
        Arc::clone(&ctx.delivery_count),
    ));
    tokio::time::sleep(crate::shared_store::subscribe_attach_warmup()).await;
}

async fn handle_publish_n(
    ctx: &mut RunContext,
    step_index: usize,
    topic: &str,
    count: u32,
    keyed: bool,
    topic_key: Option<&str>,
) -> Result<()> {
    warm_live_subscribers(ctx, topic).await?;
    let (samples, published, errors) =
        publish_n(ctx.photon(), topic, count, keyed, topic_key).await?;
    ctx.load.published += published;
    ctx.load.publish_errors += errors;
    ctx.load.backlog_peak = ctx.load.backlog_peak.max(peak_backlog());
    ctx.step_timings.push(StepTiming {
        step_index,
        op: "publish".into(),
        samples_ms: samples,
    });
    Ok(())
}

/// Probe-publish until every live-tail subscriber has attached (broker labs).
async fn warm_live_subscribers(ctx: &RunContext, topic: &str) -> Result<()> {
    let n = u32::try_from(ctx.subscriptions.len()).unwrap_or(0);
    if n == 0 {
        return Ok(());
    }
    // Mem/sqlite attach instantly; only broker live-tail needs a readiness probe.
    if std::env::var("PHOTON_FLUVIO_ENDPOINT").is_err()
        && std::env::var("PHOTON_NATS_URL").is_err()
        && std::env::var("PHOTON_KAFKA_BROKERS").is_err()
    {
        return Ok(());
    }

    // One probe key per distinct subscribe filter (None = unkeyed / all-keys).
    let mut probe_keys: Vec<Option<String>> = Vec::new();
    if ctx.subscriptions.iter().any(|s| s.key_filter.is_none()) {
        probe_keys.push(None);
    }
    for sub in &ctx.subscriptions {
        if let Some(key) = &sub.key_filter {
            if !probe_keys
                .iter()
                .any(|k| k.as_deref() == Some(key.as_str()))
            {
                probe_keys.push(Some(key.clone()));
            }
        }
    }

    for attempt in 0..5 {
        let before = ctx.delivery_count.load(Ordering::SeqCst);
        for key in &probe_keys {
            ctx.photon()
                .publish(
                    topic,
                    key.as_deref(),
                    smoke_actor_json(),
                    serde_json::json!({"subscriber_warmup": attempt}),
                )
                .await
                .map_err(|e| anyhow::anyhow!("subscriber warmup publish: {e}"))?;
        }
        // Each subscriber should receive exactly one matching probe when filters are
        // homogeneous (our labs). Mixed unfiltered+filtered over-counts; rare in matrix.
        let need = before.saturating_add(n);
        if wait_for_deliveries(&ctx.delivery_count, need, Duration::from_secs(10)).await {
            // Drop probe deliveries so AssertDelivery / AssertNoDelivery see a clean slate.
            ctx.delivery_count.store(0, Ordering::SeqCst);
            tokio::time::sleep(Duration::from_millis(100)).await;
            return Ok(());
        }
        tokio::time::sleep(crate::shared_store::subscribe_attach_warmup()).await;
    }
    bail!("live-tail subscriber warmup failed: expected {n} deliveries per probe on {topic}");
}

async fn handle_restart_runtime(runner: &ScenarioRunner<'_>, ctx: &mut RunContext) -> Result<()> {
    ctx.subscriptions.clear();
    if let Some(photon) = ctx.photon.take() {
        photon.shutdown_executor();
        photon.join_executor().await;
    }
    tokio::task::yield_now().await;
    ctx.photon = Some(runner.session.build_photon()?);
    ctx.saw_restart = true;
    ctx.delivery_count.store(0, Ordering::SeqCst);
    Ok(())
}

async fn handle_start_executor(ctx: &RunContext) -> Result<()> {
    reset_executor_invocations();
    let photon = ctx.photon.as_ref().expect("photon initialized");
    configure(photon.clone());
    photon
        .start_executor(Arc::new(JsonIdentityFactory))
        .map_err(|e| anyhow::anyhow!("start_executor: {e}"))?;
    // Inventory subscribe loops must attach before PublishN (Fluvio SPU registration is slow).
    tokio::time::sleep(crate::shared_store::subscribe_attach_warmup()).await;
    Ok(())
}

async fn handle_assert_executor_invocations(
    spec: &ScenarioSpec,
    matrix_slug: &str,
    mode: RunMode,
    ctx: &RunContext,
    expected_count: u32,
) -> Option<ScenarioResult> {
    let deadline = Instant::now() + DELIVERY_TIMEOUT;
    while Instant::now() < deadline {
        if executor_invocation_count() >= expected_count {
            if mode == RunMode::Correctness {
                return None;
            }
            return None;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    if mode == RunMode::Benchmark {
        return None;
    }
    let got = executor_invocation_count();
    Some(ctx.failure_result(
        spec,
        matrix_slug.to_string(),
        format!("AssertExecutorInvocations: expected >={expected_count}, got {got}"),
    ))
}

async fn handle_assert_delivery(
    spec: &ScenarioSpec,
    matrix_slug: &str,
    step_index: usize,
    mode: RunMode,
    ctx: &mut RunContext,
    expected_count: u32,
) -> Option<ScenarioResult> {
    let start = Instant::now();
    let ok = wait_for_deliveries(&ctx.delivery_count, expected_count, DELIVERY_TIMEOUT).await;
    let elapsed = start.elapsed().as_secs_f64();
    if ctx.saw_restart && expected_count > 0 && elapsed > 0.0 {
        ctx.load.replay_events_per_sec = Some(f64::from(expected_count) / elapsed);
        ctx.saw_restart = false;
    }
    ctx.replay_expected = Some(expected_count);
    if mode == RunMode::Benchmark {
        // Per-delivery wait (ms) so BM-P1 compares like-with-like vs per-publish samples.
        let per_delivery_ms = elapsed * 1000.0 / f64::from(expected_count.max(1));
        ctx.step_timings.push(StepTiming {
            step_index,
            op: "delivery_wait".into(),
            samples_ms: vec![per_delivery_ms],
        });
        return None;
    }
    if ok {
        return None;
    }
    let got = ctx.delivery_count.load(Ordering::SeqCst);
    Some(ctx.failure_result(
        spec,
        matrix_slug.to_string(),
        format!("AssertDelivery: expected >={expected_count}, got {got}"),
    ))
}

async fn handle_assert_publish_event_ids(
    scenario: ScenarioRef<'_>,
    mode: RunMode,
    ctx: &RunContext,
    topic: &str,
    count: u32,
    keyed: bool,
    topic_key: Option<&str>,
) -> Result<Option<ScenarioResult>> {
    if mode != RunMode::Correctness {
        return Ok(None);
    }
    let key = if keyed { topic_key } else { None };
    let mut ids = std::collections::HashSet::new();
    for i in 0..count {
        let payload = serde_json::json!({"event_id_probe": i});
        let id = ctx
            .photon()
            .publish(topic, key, smoke_actor_json(), payload)
            .await
            .map_err(|e| anyhow::anyhow!("publish failed: {e}"))?;
        if id.is_empty() {
            return Ok(Some(ctx.failure_result(
                scenario.spec,
                scenario.matrix_slug.to_string(),
                "AssertPublishEventIds: empty event_id".into(),
            )));
        }
        ids.insert(id);
    }
    if ids.len() != count as usize {
        return Ok(Some(ctx.failure_result(
            scenario.spec,
            scenario.matrix_slug.to_string(),
            format!(
                "AssertPublishEventIds: expected {count} unique ids, got {}",
                ids.len()
            ),
        )));
    }
    Ok(None)
}

async fn handle_publish_at_rate(
    ctx: &mut RunContext,
    step_index: usize,
    topic: &str,
    rate_per_sec: u32,
    duration_secs: u32,
    keyed: bool,
    topic_key: Option<&str>,
) -> Result<()> {
    ctx.load.target_rate = Some(rate_per_sec);
    ctx.load.duration_secs = Some(duration_secs);
    let (samples, published, errors) = publish_at_rate(
        ctx.photon(),
        topic,
        rate_per_sec,
        duration_secs,
        keyed,
        topic_key,
    )
    .await?;
    ctx.load.published += published;
    ctx.load.publish_errors += errors;
    ctx.load.backlog_peak = ctx.load.backlog_peak.max(peak_backlog());
    ctx.step_timings.push(StepTiming {
        step_index,
        op: "publish".into(),
        samples_ms: samples,
    });
    Ok(())
}

async fn handle_parallel_multi_stream(
    ctx: &mut RunContext,
    step_index: usize,
    stream_count: u32,
    rate_per_stream: u32,
    duration_secs: u32,
) -> Result<()> {
    for s in 0..stream_count {
        let topic = format!("testkit.pf4.{s}");
        handle_subscribe_durable(ctx, &topic, None, None, s > 0, None).await;
    }

    ctx.load.target_rate = Some(rate_per_stream.saturating_mul(stream_count));
    ctx.load.duration_secs = Some(duration_secs);

    let photon = Arc::new(ctx.photon().clone());
    let mut tasks = Vec::new();
    for s in 0..stream_count {
        let photon = Arc::clone(&photon);
        let topic = format!("testkit.pf4.{s}");
        tasks.push(tokio::spawn(async move {
            publish_at_rate(&photon, &topic, rate_per_stream, duration_secs, false, None).await
        }));
    }

    let mut all_samples = Vec::new();
    let mut published = 0u32;
    let mut errors = 0u32;
    for task in tasks {
        let (samples, p, e) = task.await??;
        all_samples.extend(samples);
        published += p;
        errors += e;
    }

    ctx.load.published += published;
    ctx.load.publish_errors += errors;
    ctx.load.backlog_peak = ctx.load.backlog_peak.max(peak_backlog());
    ctx.step_timings.push(StepTiming {
        step_index,
        op: "publish".into(),
        samples_ms: all_samples,
    });
    Ok(())
}

async fn handle_parallel_publish_at_rate(
    ctx: &mut RunContext,
    step_index: usize,
    topic: &str,
    publishers: u32,
    aggregate_rate: u32,
    duration_secs: u32,
) -> Result<()> {
    let publishers = publishers.max(1);
    let per_publisher = aggregate_rate.div_ceil(publishers);

    ctx.load.target_rate = Some(aggregate_rate);
    ctx.load.duration_secs = Some(duration_secs);

    let photon = Arc::new(ctx.photon().clone());
    let topic = topic.to_string();
    let mut tasks = Vec::new();
    for p in 0..publishers {
        let photon = Arc::clone(&photon);
        let topic = topic.clone();
        let topic_key = format!("pf2-p{p}");
        let rate = if p + 1 == publishers {
            aggregate_rate.saturating_sub(per_publisher.saturating_mul(publishers - 1))
        } else {
            per_publisher
        };
        tasks.push(tokio::spawn(async move {
            publish_at_rate(&photon, &topic, rate, duration_secs, true, Some(&topic_key)).await
        }));
    }

    let mut all_samples = Vec::new();
    let mut published = 0u32;
    let mut errors = 0u32;
    for task in tasks {
        let (samples, p, e) = task.await??;
        all_samples.extend(samples);
        published += p;
        errors += e;
    }

    ctx.load.published += published;
    ctx.load.publish_errors += errors;
    ctx.load.backlog_peak = ctx.load.backlog_peak.max(peak_backlog());
    ctx.step_timings.push(StepTiming {
        step_index,
        op: "publish".into(),
        samples_ms: all_samples,
    });
    Ok(())
}

async fn handle_firehose_parallel(
    ctx: &mut RunContext,
    step_index: usize,
    topic: &str,
    publishers: u32,
    aggregate_rate: u32,
    duration_secs: u32,
) -> Result<()> {
    let publishers = publishers.max(1);
    let per_publisher = aggregate_rate.div_ceil(publishers);

    ctx.load.target_rate = Some(aggregate_rate);
    ctx.load.duration_secs = Some(duration_secs);

    let photon = Arc::new(ctx.photon().clone());
    let topic = topic.to_string();
    let mut tasks = Vec::new();
    for p in 0..publishers {
        let photon = Arc::clone(&photon);
        let topic = topic.clone();
        let rate = if p + 1 == publishers {
            aggregate_rate.saturating_sub(per_publisher.saturating_mul(publishers - 1))
        } else {
            per_publisher
        };
        tasks.push(tokio::spawn(async move {
            publish_firehose_at_rate(&photon, &topic, rate, duration_secs).await
        }));
    }

    let mut all_samples = Vec::new();
    let mut published = 0u32;
    let mut errors = 0u32;
    for task in tasks {
        let (samples, p, e) = task.await??;
        all_samples.extend(samples);
        published += p;
        errors += e;
    }

    ctx.load.published += published;
    ctx.load.publish_errors += errors;
    ctx.load.backlog_peak = ctx.load.backlog_peak.max(peak_backlog());
    ctx.step_timings.push(StepTiming {
        step_index,
        op: "publish".into(),
        samples_ms: all_samples,
    });
    Ok(())
}

async fn handle_consumer_group_static(
    spec: &ScenarioSpec,
    matrix_slug: &str,
    step_index: usize,
    ctx: &mut RunContext,
    topic: &str,
    shard_count: u32,
    publish_count: u32,
) -> Result<Option<ScenarioResult>> {
    let outcome = if publish_count == 0 {
        let expected = ctx.load.published;
        if expected == 0 {
            return Ok(Some(ctx.failure_result(
                spec,
                matrix_slug.to_string(),
                "consumer group verify requires prior publish_count > 0".into(),
            )));
        }
        consumer_group::run_consumer_group_verify_lab(ctx.photon(), topic, shard_count, expected)
            .await
    } else {
        consumer_group::run_consumer_group_static_lab(
            ctx.photon(),
            topic,
            shard_count,
            publish_count,
        )
        .await
    };

    match outcome {
        Ok(outcome) => {
            if publish_count > 0 {
                ctx.load.published = outcome.published;
            }
            ctx.delivery_count
                .store(outcome.delivered_a + outcome.delivered_b, Ordering::SeqCst);
            ctx.step_timings.push(StepTiming {
                step_index,
                op: if publish_count == 0 {
                    "consumer_group_verify".into()
                } else {
                    "consumer_group_static".into()
                },
                samples_ms: vec![],
            });
            if !outcome.full_coverage() {
                return Ok(Some(ScenarioResult {
                    scenario_id: spec.id.clone(),
                    matrix_slug: matrix_slug.to_string(),
                    deliveries: outcome.delivered_a + outcome.delivered_b,
                    step_timings: std::mem::take(&mut ctx.step_timings),
                    load: Some(std::mem::take(&mut ctx.load)),
                    error: Some(format!(
                        "consumer group coverage gap: unique {} / published {}",
                        outcome.unique_event_ids,
                        outcome.published.max(ctx.load.published)
                    )),
                }));
            }
            Ok(None)
        }
        Err(e) => Ok(Some(ScenarioResult {
            scenario_id: spec.id.clone(),
            matrix_slug: matrix_slug.to_string(),
            deliveries: ctx.delivery_count.load(Ordering::SeqCst),
            step_timings: std::mem::take(&mut ctx.step_timings),
            load: Some(std::mem::take(&mut ctx.load)),
            error: Some(e.to_string()),
        })),
    }
}

#[allow(clippy::too_many_arguments)]
async fn handle_consumer_group_round_robin(
    spec: &ScenarioSpec,
    matrix_slug: &str,
    step_index: usize,
    ctx: &mut RunContext,
    topic: &str,
    shard_count: u32,
    member_count: u32,
    publish_count: u32,
) -> Result<Option<ScenarioResult>> {
    match consumer_group::run_consumer_group_round_robin_lab(
        ctx.photon(),
        topic,
        shard_count,
        member_count,
        publish_count,
    )
    .await
    {
        Ok(outcome) => {
            ctx.load.published = outcome.published;
            ctx.delivery_count
                .store(outcome.delivered_a + outcome.delivered_b, Ordering::SeqCst);
            ctx.step_timings.push(StepTiming {
                step_index,
                op: "consumer_group_round_robin".into(),
                samples_ms: vec![],
            });
            if !outcome.full_coverage() {
                return Ok(Some(ScenarioResult {
                    scenario_id: spec.id.clone(),
                    matrix_slug: matrix_slug.to_string(),
                    deliveries: outcome.delivered_a + outcome.delivered_b,
                    step_timings: std::mem::take(&mut ctx.step_timings),
                    load: Some(std::mem::take(&mut ctx.load)),
                    error: Some(format!(
                        "consumer group round-robin coverage gap: unique {} / published {}",
                        outcome.unique_event_ids, outcome.published
                    )),
                }));
            }
            Ok(None)
        }
        Err(e) => Ok(Some(ScenarioResult {
            scenario_id: spec.id.clone(),
            matrix_slug: matrix_slug.to_string(),
            deliveries: ctx.delivery_count.load(Ordering::SeqCst),
            step_timings: std::mem::take(&mut ctx.step_timings),
            load: Some(std::mem::take(&mut ctx.load)),
            error: Some(e.to_string()),
        })),
    }
}

async fn handle_assert_no_delivery(
    spec: &ScenarioSpec,
    matrix_slug: &str,
    step_index: usize,
    mode: RunMode,
    ctx: &mut RunContext,
    wait_ms: u32,
) -> Option<ScenarioResult> {
    if mode == RunMode::Benchmark {
        return None;
    }
    tokio::time::sleep(Duration::from_millis(u64::from(wait_ms))).await;
    let got = ctx.delivery_count.load(Ordering::SeqCst);
    ctx.step_timings.push(StepTiming {
        step_index,
        op: "assert_no_delivery".into(),
        samples_ms: vec![f64::from(wait_ms)],
    });
    if got == 0 {
        return None;
    }
    Some(ctx.failure_result(
        spec,
        matrix_slug.to_string(),
        format!("AssertNoDelivery: expected 0 deliveries, got {got}"),
    ))
}

async fn handle_cross_node_fanout(
    runner: &ScenarioRunner<'_>,
    spec: &ScenarioSpec,
    matrix_slug: &str,
    step_index: usize,
    ctx: &mut RunContext,
    _topic: &str,
) -> Result<Option<ScenarioResult>> {
    match cross_node::apply_cross_node_fanout_step(runner.session, spec).await {
        cross_node::CrossNodeStepOutcome::Ok { delivery_wait_ms } => {
            ctx.step_timings.push(StepTiming {
                step_index,
                op: "delivery_wait".into(),
                samples_ms: vec![delivery_wait_ms],
            });
            ctx.load.published = FANOUT_PUBLISH_COUNT;
            ctx.delivery_count
                .store(FANOUT_PUBLISH_COUNT, Ordering::SeqCst);
            Ok(None)
        }
        cross_node::CrossNodeStepOutcome::FailDelivery { deliveries, error } => {
            Ok(Some(ScenarioResult {
                scenario_id: spec.id.clone(),
                matrix_slug: matrix_slug.to_string(),
                deliveries,
                step_timings: std::mem::take(&mut ctx.step_timings),
                load: Some(std::mem::take(&mut ctx.load)),
                error: Some(error),
            }))
        }
        cross_node::CrossNodeStepOutcome::Err { error } => Ok(Some(ScenarioResult {
            scenario_id: spec.id.clone(),
            matrix_slug: matrix_slug.to_string(),
            deliveries: 0,
            step_timings: std::mem::take(&mut ctx.step_timings),
            load: Some(std::mem::take(&mut ctx.load)),
            error: Some(error),
        })),
    }
}

async fn publish_n(
    photon: &Photon,
    topic: &str,
    count: u32,
    keyed: bool,
    topic_key: Option<&str>,
) -> Result<(Vec<f64>, u32, u32)> {
    let key = if keyed { topic_key } else { None };
    let mut samples = Vec::with_capacity(count as usize);
    let mut published = 0u32;
    let mut errors = 0u32;
    for i in 0..count {
        let payload = serde_json::json!({"n": i});
        let start = Instant::now();
        match photon
            .publish(topic, key, smoke_actor_json(), payload)
            .await
        {
            Ok(_) => {
                published += 1;
                samples.push(start.elapsed().as_secs_f64() * 1000.0);
            }
            Err(e) => {
                errors += 1;
                if errors == 1 {
                    return Err(anyhow::anyhow!("publish failed: {e}"));
                }
            }
        }
    }
    Ok((samples, published, errors))
}

async fn publish_at_rate(
    photon: &Photon,
    topic: &str,
    rate_per_sec: u32,
    duration_secs: u32,
    keyed: bool,
    topic_key: Option<&str>,
) -> Result<(Vec<f64>, u32, u32)> {
    let key = if keyed { topic_key } else { None };
    let mut samples = Vec::new();
    let mut published = 0u32;
    let mut errors = 0u32;
    if rate_per_sec == 0 || duration_secs == 0 {
        return Ok((samples, published, errors));
    }

    let start = Instant::now();
    let deadline = start + Duration::from_secs(u64::from(duration_secs));
    let interval = Duration::from_secs_f64(1.0 / f64::from(rate_per_sec));
    let mut next = start;
    let mut i = 0u32;

    while Instant::now() < deadline {
        let payload = serde_json::json!({"n": i});
        let op_start = Instant::now();
        let mut ok = false;
        for attempt in 0..10 {
            match photon
                .publish(topic, key, smoke_actor_json(), payload.clone())
                .await
            {
                Ok(_) => {
                    published += 1;
                    samples.push(op_start.elapsed().as_secs_f64() * 1000.0);
                    ok = true;
                    break;
                }
                Err(_) if attempt + 1 < 10 => {
                    tokio::time::sleep(Duration::from_millis(100 * (attempt + 1))).await;
                }
                Err(_) => {}
            }
        }
        if !ok {
            errors += 1;
        }
        i += 1;
        next += interval;
        let now = Instant::now();
        if next > now {
            tokio::time::sleep(next - now).await;
        }
    }
    Ok((samples, published, errors))
}

const FIREHOSE_SAMPLE_EVERY: u32 = 1000;
const FIREHOSE_SAMPLE_CAP: usize = 10_000;

async fn publish_firehose_at_rate(
    photon: &Photon,
    topic: &str,
    rate_per_sec: u32,
    duration_secs: u32,
) -> Result<(Vec<f64>, u32, u32)> {
    let mut samples = Vec::new();
    let mut published = 0u32;
    let mut errors = 0u32;
    if rate_per_sec == 0 || duration_secs == 0 {
        return Ok((samples, published, errors));
    }

    let start = Instant::now();
    let deadline = start + Duration::from_secs(u64::from(duration_secs));
    let interval = Duration::from_secs_f64(1.0 / f64::from(rate_per_sec));
    let mut next = start;
    let mut i = 0u32;

    while Instant::now() < deadline {
        let payload = serde_json::json!({"n": i});
        let op_start = Instant::now();
        let mut ok = false;
        for attempt in 0..10 {
            match photon
                .publish(topic, None, smoke_actor_json(), payload.clone())
                .await
            {
                Ok(_) => {
                    published += 1;
                    if published.is_multiple_of(FIREHOSE_SAMPLE_EVERY)
                        && samples.len() < FIREHOSE_SAMPLE_CAP
                    {
                        samples.push(op_start.elapsed().as_secs_f64() * 1000.0);
                    }
                    ok = true;
                    break;
                }
                Err(_) if attempt + 1 < 10 => {
                    tokio::time::sleep(Duration::from_millis(100 * (attempt + 1))).await;
                }
                Err(_) => {}
            }
        }
        if !ok {
            errors += 1;
        }
        i += 1;
        next += interval;
        let now = Instant::now();
        if next > now {
            tokio::time::sleep(next - now).await;
        }
    }
    Ok((samples, published, errors))
}

const fn peak_backlog() -> u64 {
    0
}

fn spawn_subscription(
    photon: Arc<Photon>,
    topic: String,
    topic_key_filter: Option<String>,
    after_seq: Option<i64>,
    delivery_delay_ms: Option<u32>,
    delivery_count: Arc<AtomicU32>,
) -> ActiveSubscription {
    let key_filter_meta = topic_key_filter.clone();
    let task = tokio::spawn(async move {
        let key_filter = topic_key_filter.as_deref();
        let mut stream = photon.subscribe(&topic, key_filter, after_seq);
        let delay = delivery_delay_ms.map(|ms| Duration::from_millis(u64::from(ms)));
        while let Some(item) = stream.next().await {
            if item.is_ok() {
                if let Some(d) = delay {
                    tokio::time::sleep(d).await;
                }
                delivery_count.fetch_add(1, Ordering::SeqCst);
            }
        }
    });
    ActiveSubscription {
        _task: task,
        key_filter: key_filter_meta,
    }
}

async fn wait_for_deliveries(counter: &AtomicU32, expected: u32, timeout: Duration) -> bool {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if counter.load(Ordering::SeqCst) >= expected {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    counter.load(Ordering::SeqCst) >= expected
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::MatrixSpec;
    use serial_test::serial;

    #[tokio::test(flavor = "multi_thread")]
    #[serial(photon_process_env)]
    async fn runner_publish_subscribe_smoke() {
        let mut session = BootstrapSession::new(MatrixSpec::ci_mem_embedded());
        session.install().expect("install");
        let result = ScenarioRunner::new(&session)
            .run(
                &ScenarioSpec::publish_subscribe_smoke(),
                RunMode::Correctness,
            )
            .await
            .expect("run");
        assert!(result.error.is_none(), "{:?}", result.error);
        assert_eq!(result.deliveries, 2);
    }
}
