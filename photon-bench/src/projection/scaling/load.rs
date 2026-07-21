//! Load peak `BM-PFH` rates per broker node or bench client count.

use std::collections::HashMap;
use std::path::Path;

use anyhow::{bail, Result};

use super::filter::{extract_meta, is_primary_row, parse_point, should_include, ScalingFilter};
use super::fit::{assemble_curve, build_points};
use super::ScalingCurve;

use crate::projection::pfh_reports;

/// Load peak `BM-PFH` achieved rate per broker node count (or bench client count for multibench).
pub fn load_scaling_curve(
    reports_dir: &Path,
    hardware: &str,
    storage: &str,
    stream_shards: Option<u32>,
    match_broker_nodes: bool,
    multibench_ladder: bool,
    bench_client_count_filter: Option<u32>,
    primary_row: bool,
) -> Result<ScalingCurve> {
    let filter = ScalingFilter {
        stream_shards,
        match_broker_nodes,
        multibench_ladder,
        bench_client_count_filter,
        primary_row,
    };

    let mut best: HashMap<u32, (f64, String, String, Option<f64>, u32)> = HashMap::new();
    let mut nats_bench_peak_n1: Option<f64> = None;

    for (path, v) in pfh_reports::read_pfh_reports(reports_dir, hardware, storage)? {
        let meta = extract_meta(&path, &v);
        if !should_include(&meta, &filter) {
            continue;
        }
        if filter.primary_row && !is_primary_row(&path, &v) {
            continue;
        }
        let Some(point) = parse_point(&v, &meta, multibench_ladder) else {
            continue;
        };

        if point.point_key == 1 && !multibench_ladder {
            if let Some(bp) = point.bench_peak {
                nats_bench_peak_n1 = Some(nats_bench_peak_n1.map_or(bp, |prev| prev.max(bp)));
            }
        }
        best.entry(point.point_key)
            .and_modify(|(best_rate, best_config, best_file, best_bench, _)| {
                if point.rate > *best_rate {
                    *best_rate = point.rate;
                    best_config.clone_from(&point.config_label);
                    best_file.clone_from(&point.fname);
                    *best_bench = point.bench_peak;
                }
            })
            .or_insert((
                point.rate,
                point.config_label,
                point.fname,
                point.bench_peak,
                point.broker_nodes,
            ));
    }

    if best.is_empty() {
        bail!(
            "no BM-PFH reports for {hardware}/{storage} in {}",
            reports_dir.display()
        );
    }

    let points = build_points(best, multibench_ladder);
    Ok(assemble_curve(
        hardware,
        storage,
        stream_shards,
        match_broker_nodes,
        multibench_ladder,
        nats_bench_peak_n1,
        points,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    use tempfile::TempDir;

    #[test]
    fn scaling_curve_picks_peak_per_broker_nodes() {
        let dir = TempDir::new().unwrap();
        let report = r#"{
            "experiment": "bm-pfh",
            "hardware": "aws-t3-medium",
            "storage": "nats",
            "node_count": 1,
            "achieved_ops_per_sec": 50000.0,
            "dimensions": {"replay_cursor": "tail_only", "sync_ack": "0"}
        }"#;
        let mut f = std::fs::File::create(dir.path().join("a.json")).unwrap();
        write!(f, "{report}").unwrap();
        let mut f2 = std::fs::File::create(dir.path().join("b.json")).unwrap();
        write!(
            f2,
            r#"{{
            "experiment": "bm-pfh",
            "hardware": "aws-t3-medium",
            "storage": "nats",
            "node_count": 2,
            "achieved_ops_per_sec": 90000.0,
            "dimensions": {{"replay_cursor": "tail_only", "sync_ack": "0"}}
        }}"#
        )
        .unwrap();
        let curve = load_scaling_curve(
            dir.path(),
            "aws-t3-medium",
            "nats",
            None,
            false,
            false,
            None,
            false,
        )
        .unwrap();
        assert_eq!(curve.points.len(), 2);
        assert!(
            curve
                .broker_nodes_for_target
                .get("1000000")
                .copied()
                .unwrap_or(0)
                > 0
        );
    }

    #[test]
    fn primary_row_prefers_p256_r100000_over_swept_best_cell() {
        let dir = TempDir::new().unwrap();
        // Best swept cell (higher rate) — should be ignored with primary_row.
        write!(
            std::fs::File::create(
                dir.path()
                    .join("bm-pfh-kafka-n1-sh1-stream_seq-ack1-p128-r10000-aws.json")
            )
            .unwrap(),
            r#"{{
            "experiment": "bm-pfh",
            "hardware": "aws-c6i-large",
            "storage": "kafka",
            "node_count": 1,
            "achieved_ops_per_sec": 2258.0,
            "dimensions": {{
                "broker_nodes": 1,
                "stream_shards": 1,
                "publishers": 128,
                "replay_cursor": "stream_seq",
                "sync_ack": "1"
            }}
        }}"#
        )
        .unwrap();
        // Same publisher count but wrong cursor — still not primary row.
        write!(
            std::fs::File::create(
                dir.path()
                    .join("bm-pfh-kafka-n1-sh1-tail_only-ack1-p256-r100000-aws.json")
            )
            .unwrap(),
            r#"{{
            "experiment": "bm-pfh",
            "hardware": "aws-c6i-large",
            "storage": "kafka",
            "node_count": 1,
            "achieved_ops_per_sec": 1253.1,
            "dimensions": {{
                "broker_nodes": 1,
                "stream_shards": 1,
                "publishers": 256,
                "replay_cursor": "tail_only",
                "sync_ack": "1"
            }}
        }}"#
        )
        .unwrap();
        write!(
            std::fs::File::create(
                dir.path()
                    .join("bm-pfh-kafka-n1-sh1-stream_seq-ack1-p256-r100000-aws.json")
            )
            .unwrap(),
            r#"{{
            "experiment": "bm-pfh",
            "hardware": "aws-c6i-large",
            "storage": "kafka",
            "node_count": 1,
            "achieved_ops_per_sec": 1145.5,
            "dimensions": {{
                "broker_nodes": 1,
                "stream_shards": 1,
                "publishers": 256,
                "replay_cursor": "stream_seq",
                "sync_ack": "1"
            }}
        }}"#
        )
        .unwrap();
        // Multibench per-client file must not leak into baseline ladder.
        write!(
            std::fs::File::create(
                dir.path()
                    .join("bm-pfh-kafka-n4-sh4-bc1-i0-stream_seq-ack1-p256-r100000-aws.json")
            )
            .unwrap(),
            r#"{{
            "experiment": "bm-pfh",
            "hardware": "aws-c6i-large",
            "storage": "kafka",
            "node_count": 4,
            "achieved_ops_per_sec": 3242.0,
            "dimensions": {{
                "broker_nodes": 4,
                "stream_shards": 4,
                "bench_client_count": 1,
                "publishers": 256,
                "replay_cursor": "stream_seq",
                "sync_ack": "1"
            }}
        }}"#
        )
        .unwrap();
        write!(
            std::fs::File::create(
                dir.path()
                    .join("bm-pfh-kafka-n4-sh1-stream_seq-ack1-p256-r100000-aws.json")
            )
            .unwrap(),
            r#"{{
            "experiment": "bm-pfh",
            "hardware": "aws-c6i-large",
            "storage": "kafka",
            "node_count": 4,
            "achieved_ops_per_sec": 1453.4,
            "dimensions": {{
                "broker_nodes": 4,
                "stream_shards": 1,
                "publishers": 256,
                "replay_cursor": "stream_seq",
                "sync_ack": "1"
            }}
        }}"#
        )
        .unwrap();

        let without = load_scaling_curve(
            dir.path(),
            "aws-c6i-large",
            "kafka",
            Some(1),
            false,
            false,
            None,
            false,
        )
        .unwrap();
        assert_eq!(without.points.len(), 2);
        assert!(
            (without.points[0].peak_ops_per_sec - 2258.0).abs() < 0.1,
            "without primary_row should pick best swept cell"
        );

        let with = load_scaling_curve(
            dir.path(),
            "aws-c6i-large",
            "kafka",
            Some(1),
            false,
            false,
            None,
            true,
        )
        .unwrap();
        assert_eq!(with.points.len(), 2);
        let n1 = with.points.iter().find(|p| p.broker_nodes == 1).unwrap();
        assert!(
            (n1.peak_ops_per_sec - 1145.5).abs() < 0.1,
            "primary_row must pick stream_seq p256-r100000 ({})",
            n1.report_file
        );
        assert!(n1.report_file.contains("stream_seq-ack1-p256-r100000"));
        let n4 = with.points.iter().find(|p| p.broker_nodes == 4).unwrap();
        assert!(
            (n4.peak_ops_per_sec - 1453.4).abs() < 0.1,
            "baseline must not use multibench bc file"
        );
        assert!(!n4.report_file.contains("-bc"));
    }
}
