//! Scaling exponent, bottleneck classification, and target projection.

use std::collections::HashMap;

use super::{ScalingCurve, ScalingPoint};

const TARGETS: &[u64] = &[330_000, 1_000_000, 1_000_000_000];

pub fn fit_scaling_exponent(points: &[ScalingPoint], multibench: bool) -> Option<f64> {
    if points.len() < 2 {
        return None;
    }
    let mut sum_log_n = 0.0;
    let mut sum_log_r = 0.0;
    let mut sum_log_n_sq = 0.0;
    let mut sum_log_n_log_r = 0.0;
    let n = points.len() as f64;
    for p in points {
        let key = if multibench {
            f64::from(p.bench_client_count.unwrap_or(p.broker_nodes))
        } else {
            f64::from(p.broker_nodes)
        };
        let ln = key.ln();
        let lr = p.peak_ops_per_sec.ln();
        sum_log_n += ln;
        sum_log_r += lr;
        sum_log_n_sq = ln.mul_add(ln, sum_log_n_sq);
        sum_log_n_log_r = ln.mul_add(lr, sum_log_n_log_r);
    }
    let denom = n.mul_add(sum_log_n_sq, -(sum_log_n * sum_log_n));
    if denom.abs() < f64::EPSILON {
        return None;
    }
    Some(n.mul_add(sum_log_n_log_r, -(sum_log_n * sum_log_r)) / denom)
}

pub fn classify_bottleneck(
    pfh_n1: Option<f64>,
    nats_bench_n1: Option<f64>,
    points: &[ScalingPoint],
) -> String {
    if let (Some(pfh), Some(bench)) = (pfh_n1, nats_bench_n1) {
        if bench > 0.0 && (pfh / bench) < 0.5 {
            return "photon_adapter".into();
        }
        if (pfh - bench).abs() / bench.max(1.0) < 0.15 {
            if points.len() >= 2 {
                let n4 = points.iter().find(|p| p.broker_nodes == 4);
                let n1 = points.iter().find(|p| p.broker_nodes == 1);
                if let (Some(p4), Some(p1)) = (n4, n1) {
                    let ratio = p4.peak_ops_per_sec / (4.0 * p1.peak_ops_per_sec);
                    if ratio < 0.5 {
                        return "jetstream_cluster".into();
                    }
                }
            }
            return "jetstream_single_node".into();
        }
    }
    "unknown".into()
}

pub fn broker_nodes_for_targets(points: &[ScalingPoint]) -> HashMap<String, u64> {
    let best_per_broker = points
        .iter()
        .max_by(|a, b| {
            a.peak_ops_per_sec
                .partial_cmp(&b.peak_ops_per_sec)
                .unwrap_or(std::cmp::Ordering::Equal)
        })
        .map_or(0.0, |p| {
            p.peak_ops_per_sec / f64::from(p.broker_nodes.max(1))
        });

    TARGETS
        .iter()
        .map(|&target| {
            let nodes = if best_per_broker > 0.0 {
                (target as f64 / best_per_broker).ceil() as u64
            } else {
                0
            };
            (target.to_string(), nodes.max(1))
        })
        .collect()
}

pub fn scaling_disclaimer(multibench_ladder: bool) -> String {
    if multibench_ladder {
        "broker_nodes_for_target uses per-broker peak from multibench aggregate reports; bench_client_count is independent publisher fleet size.".into()
    } else {
        "broker_nodes_for_target = NATS JetStream nodes for ingress; projected from N=1/2/4 sweep peaks.".into()
    }
}

pub fn build_points(
    best: HashMap<u32, (f64, String, String, Option<f64>, u32)>,
    multibench_ladder: bool,
) -> Vec<ScalingPoint> {
    let baseline_nodes = 1;
    let baseline_peak = best.get(&baseline_nodes).map(|(r, _, _, _, _)| *r);

    let mut points: Vec<ScalingPoint> = best
        .into_iter()
        .map(|(key, (peak, config, file, bench, broker_nodes))| {
            let vs = baseline_peak.filter(|b| *b > 0.0).map(|b| peak / b);
            ScalingPoint {
                broker_nodes: if multibench_ladder { broker_nodes } else { key },
                bench_client_count: if multibench_ladder { Some(key) } else { None },
                peak_ops_per_sec: peak,
                config,
                nats_bench_peak: bench,
                vs_baseline: vs,
                report_file: file,
            }
        })
        .collect();
    points.sort_by_key(|p| p.bench_client_count.unwrap_or(p.broker_nodes));
    points
}

pub fn assemble_curve(
    hardware: &str,
    storage: &str,
    stream_shards: Option<u32>,
    match_broker_nodes: bool,
    multibench_ladder: bool,
    nats_bench_peak_n1: Option<f64>,
    points: Vec<ScalingPoint>,
) -> ScalingCurve {
    let baseline_peak = points
        .iter()
        .find(|p| p.bench_client_count.unwrap_or(p.broker_nodes) == 1)
        .map(|p| p.peak_ops_per_sec);
    let scaling_exponent = fit_scaling_exponent(&points, multibench_ladder);
    let broker_nodes_for_target = broker_nodes_for_targets(&points);
    let bottleneck_verdict = classify_bottleneck(baseline_peak, nats_bench_peak_n1, &points);

    ScalingCurve {
        hardware: hardware.into(),
        storage: storage.into(),
        workload: "bm-pfh".into(),
        baseline_broker_nodes: 1,
        stream_shards,
        sharded_ladder: match_broker_nodes,
        multibench_ladder,
        bottleneck_verdict: Some(bottleneck_verdict),
        nats_bench_peak_n1,
        points,
        scaling_exponent,
        broker_nodes_for_target,
        disclaimer: scaling_disclaimer(multibench_ladder),
    }
}
