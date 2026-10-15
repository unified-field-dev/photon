//! Pass/fail evaluators for each registered experiment's criteria.

use crate::metrics::latency::PublishSlope;
use crate::metrics::load::LoadSummary;
use crate::stats::MetricStats;

#[derive(Debug, Clone, Default)]
pub struct PassContext {
    pub experiment: String,
    pub publish_ms: Option<MetricStats>,
    pub delivery_wait_ms: Option<MetricStats>,
    pub slope: Option<PublishSlope>,
    pub load: Option<LoadSummary>,
    pub target_rate: Option<u32>,
    pub subscriber_count: Option<u32>,
    pub deliveries: Option<u32>,
    pub publish_p50_delta_ms: Option<f64>,
    pub run_error: Option<String>,
    pub status: &'static str,
    pub acked_deliveries: Option<u32>,
    pub consume_ack_ms: Option<MetricStats>,
    pub fanout_acked: Option<Vec<u32>>,
    pub fanout_equal: Option<bool>,
    pub delivered_ops_per_sec: Option<f64>,
}

const P0_DELTA_BUDGET_MS: f64 = 15.0;
const P6_P99_BUDGET_MS: f64 = 103.0;
const PF0_MIN_DELIVERIES: u32 = 100;

type EvalFn = fn(&PassContext) -> bool;

const EVALUATORS: &[(&str, EvalFn)] = &[
    ("bm-p0", eval_p0),
    ("bm-p1", eval_p1),
    ("bm-p2", eval_p2),
    ("bm-p3", eval_p3),
    ("bm-p4", eval_p4),
    ("bm-p5", eval_p5),
    ("bm-p6", eval_p6),
    ("bm-p7", eval_p4),
    ("bm-p8", eval_pl),
    ("bm-p9", eval_p9),
    ("bm-pl0", eval_pl),
    ("bm-pl1", eval_pl),
    ("bm-pl2", eval_pl),
    ("bm-pl3", eval_pl),
    ("bm-pg0", eval_pg),
    ("bm-pg1", eval_pg),
    ("bm-pg2", eval_pg2),
    ("bm-pb0", eval_pb_zero_errors),
    ("bm-pb1", eval_pb_zero_errors),
    ("bm-pb2", eval_pb2),
    ("bm-pb3", eval_pb3),
    ("bm-pb4", eval_pb4),
    ("bm-pb5", eval_pb5),
    ("bm-pf0", eval_pf0),
    ("bm-pf1", eval_pf1),
    ("bm-pf2", eval_pf2),
    ("bm-pf3", eval_pf3),
    ("bm-pf4", eval_pf4),
    ("bm-pfh", eval_pl),
    ("bm-pfs", eval_pfs),
    ("bm-pfe", eval_pfe),
    ("bm-pd0", eval_pd),
    ("bm-pd1", eval_pd),
    ("bm-pd2", eval_pd_capacity),
    ("bm-pd3", eval_pd_capacity),
];

pub fn evaluate_pass(ctx: &PassContext) -> bool {
    if ctx.status != "ok" || ctx.run_error.is_some() {
        return false;
    }
    EVALUATORS
        .iter()
        .find(|(id, _)| *id == ctx.experiment)
        .is_some_and(|(_, eval)| eval(ctx))
}

fn eval_pb_zero_errors(ctx: &PassContext) -> bool {
    ctx.load.is_some_and(|l| l.error_rate == 0.0)
}

fn eval_pb2(ctx: &PassContext) -> bool {
    ctx.load.is_some_and(|l| l.error_rate == 0.0)
        && ctx.delivery_wait_ms.is_some_and(|d| d.p99 > 0.0)
        && ctx.subscriber_count.is_some_and(|n| n > 0)
}

fn eval_pb3(ctx: &PassContext) -> bool {
    ctx.load
        .and_then(|l| l.replay_events_per_sec)
        .is_some_and(|r| r > 0.0)
}

fn eval_pb4(ctx: &PassContext) -> bool {
    ctx.load
        .is_some_and(|l| l.error_rate < 0.001 && l.achieved_ops_per_sec > 0.0)
}

fn eval_pb5(ctx: &PassContext) -> bool {
    ctx.load
        .is_some_and(|l| l.error_rate < 0.01 && l.achieved_ops_per_sec > 0.0)
}

fn eval_pf0(ctx: &PassContext) -> bool {
    ctx.load.is_some_and(|l| l.error_rate == 0.0)
        && ctx.delivery_wait_ms.is_some_and(|d| d.p99 > 0.0)
        && ctx.deliveries.is_some_and(|d| d >= PF0_MIN_DELIVERIES)
}

fn eval_pf1(ctx: &PassContext) -> bool {
    eval_pb2(ctx)
}

fn eval_pf2(ctx: &PassContext) -> bool {
    let Some(load) = &ctx.load else {
        return false;
    };
    let target = f64::from(ctx.target_rate.unwrap_or(1000));
    load.error_rate < 0.001 && load.achieved_ops_per_sec >= 0.9 * target
}

fn eval_pf3(ctx: &PassContext) -> bool {
    eval_pl(ctx)
}

fn eval_pf4(ctx: &PassContext) -> bool {
    let Some(load) = &ctx.load else {
        return false;
    };
    let target = f64::from(ctx.target_rate.unwrap_or(1000));
    load.error_rate < 0.001 && load.achieved_ops_per_sec >= 0.9 * target
}

fn eval_p0(ctx: &PassContext) -> bool {
    let Some(p) = &ctx.publish_ms else {
        return false;
    };
    let slope_ok = ctx.slope.is_none_or(|s| s.slope.abs() < 0.01);
    p.p50 > 0.0 && p.p95 / p.p50.max(f64::EPSILON) < 10.0 && slope_ok
}

fn eval_p1(ctx: &PassContext) -> bool {
    let (Some(p), Some(d)) = (&ctx.publish_ms, &ctx.delivery_wait_ms) else {
        return false;
    };
    p.p95 > 0.0 && d.p95 - p.p95 < 500.0
}

fn eval_p2(ctx: &PassContext) -> bool {
    ctx.subscriber_count.is_some_and(|n| n > 0)
        && ctx.publish_ms.is_some()
        && ctx.delivery_wait_ms.is_some()
}

fn eval_p3(ctx: &PassContext) -> bool {
    let Some(load) = &ctx.load else {
        return false;
    };
    let cap: u64 = std::env::var("PHOTON_APPEND_QUEUE_MAX")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(1024);
    load.backlog_peak <= cap
}

fn eval_p4(ctx: &PassContext) -> bool {
    ctx.load
        .and_then(|l| l.replay_events_per_sec)
        .is_some_and(|r| r > 0.0)
}

const fn eval_p5(_ctx: &PassContext) -> bool {
    true
}

fn eval_p6(ctx: &PassContext) -> bool {
    let Some(d) = &ctx.delivery_wait_ms else {
        return false;
    };
    if d.p99 <= 0.0 || ctx.run_error.is_some() {
        return false;
    }
    std::env::var("PHOTON_BENCH_P6_P99_BUDGET_MS")
        .ok()
        .and_then(|s| s.parse::<f64>().ok())
        .map_or(d.p99 <= P6_P99_BUDGET_MS, |budget| d.p99 <= budget)
}

fn eval_p9(ctx: &PassContext) -> bool {
    ctx.publish_p50_delta_ms
        .is_none_or(|d| d <= P0_DELTA_BUDGET_MS)
}

fn eval_pl(ctx: &PassContext) -> bool {
    ctx.load.is_some_and(|l| l.error_rate < 0.001)
}

fn eval_pg(ctx: &PassContext) -> bool {
    ctx.load
        .is_some_and(|l| l.error_rate < 0.001 && l.achieved_ops_per_sec > 0.0)
}

fn eval_pg2(ctx: &PassContext) -> bool {
    ctx.load.is_some_and(|l| l.error_rate < 0.001)
}

fn eval_pfs(ctx: &PassContext) -> bool {
    ctx.load.is_some_and(|l| l.error_rate < 0.001) && ctx.subscriber_count.is_some_and(|n| n > 0)
}

fn eval_pfe(ctx: &PassContext) -> bool {
    ctx.load.is_some_and(|l| l.error_rate < 0.001)
}

fn eval_pd(ctx: &PassContext) -> bool {
    let Some(acked) = ctx.acked_deliveries else {
        return false;
    };
    if acked == 0 {
        return false;
    }
    let Some(consume) = &ctx.consume_ack_ms else {
        return false;
    };
    if consume.count == 0 || consume.p50 < 0.0 {
        return false;
    }
    let subs = ctx.subscriber_count.unwrap_or(1);
    if subs > 1 {
        let Some(fanout) = &ctx.fanout_acked else {
            return false;
        };
        if fanout.len() != subs as usize {
            return false;
        }
        if ctx.fanout_equal != Some(true) {
            return false;
        }
    }
    ctx.delivered_ops_per_sec.is_some_and(|r| r > 0.0)
}

fn eval_pd_capacity(ctx: &PassContext) -> bool {
    if !eval_pd(ctx) {
        return false;
    }
    let target = f64::from(ctx.target_rate.unwrap_or(0));
    if target <= 0.0 {
        return false;
    }
    ctx.delivered_ops_per_sec
        .is_some_and(|rate| rate >= 0.9 * target)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::stats::MetricStats;

    fn pd_ctx() -> PassContext {
        PassContext {
            experiment: "bm-pd0".into(),
            status: "ok",
            acked_deliveries: Some(8),
            consume_ack_ms: Some(MetricStats::summarize(vec![1.0, 2.0])),
            delivered_ops_per_sec: Some(80.0),
            subscriber_count: Some(1),
            ..PassContext::default()
        }
    }

    #[test]
    fn pd0_passes_with_consume_ack_samples() {
        assert!(evaluate_pass(&pd_ctx()));
    }

    #[test]
    fn pd1_requires_fanout_equality() {
        let mut ctx = pd_ctx();
        ctx.experiment = "bm-pd1".into();
        ctx.subscriber_count = Some(4);
        ctx.acked_deliveries = Some(32);
        ctx.fanout_acked = Some(vec![8, 8, 8, 8]);
        ctx.fanout_equal = Some(true);
        assert!(evaluate_pass(&ctx));
        ctx.fanout_equal = Some(false);
        assert!(!evaluate_pass(&ctx));
    }

    #[test]
    fn pd_fails_without_acks() {
        let mut ctx = pd_ctx();
        ctx.acked_deliveries = Some(0);
        assert!(!evaluate_pass(&ctx));
    }

    #[test]
    fn pd2_requires_delivered_rate_near_offered() {
        let mut ctx = pd_ctx();
        ctx.experiment = "bm-pd2".into();
        ctx.target_rate = Some(100);
        ctx.delivered_ops_per_sec = Some(91.0);
        assert!(evaluate_pass(&ctx));
        ctx.delivered_ops_per_sec = Some(80.0);
        assert!(!evaluate_pass(&ctx));
        ctx.delivered_ops_per_sec = Some(91.0);
        ctx.target_rate = None;
        assert!(!evaluate_pass(&ctx));
    }

    #[test]
    fn pd3_requires_fanout_and_delivered_rate() {
        let mut ctx = pd_ctx();
        ctx.experiment = "bm-pd3".into();
        ctx.subscriber_count = Some(4);
        ctx.acked_deliveries = Some(32);
        ctx.fanout_acked = Some(vec![8, 8, 8, 8]);
        ctx.fanout_equal = Some(true);
        ctx.target_rate = Some(50);
        ctx.delivered_ops_per_sec = Some(50.0);
        assert!(evaluate_pass(&ctx));
        ctx.fanout_equal = Some(false);
        assert!(!evaluate_pass(&ctx));
    }
}
