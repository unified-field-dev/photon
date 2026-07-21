//! Markdown snippet rendering for projection reports.

use super::model::FleetProjection;

pub fn render_markdown(p: &FleetProjection) -> String {
    let mut lines = vec![
        format!("## Fleet projection ({}/{})", p.hardware, p.storage),
        String::new(),
        format!("- R_shard: {:?} ops/s", p.r_shard_ops_per_sec),
        format!("- Δ publish p50: {:?} ms", p.delta_publish_p50_ms),
        format!("- Partitions for 1B/s: {:?}", p.partitions_for_1e9),
        format!("- Estimated Photon nodes: {:?}", p.photon_nodes_estimate),
    ];
    if let Some(n) = p.broker_nodes_for_1m {
        lines.push(format!("- NATS broker nodes for 1M/s: {n}"));
    }
    if let Some(n) = p.broker_nodes_for_1e9 {
        lines.push(format!("- NATS broker nodes for 1B/s: {n}"));
    }
    if let Some(v) = &p.nats_bottleneck_verdict {
        lines.push(format!("- NATS bottleneck: {v}"));
    }
    lines.push(format!(
        "- Bottlenecks: {}",
        p.bottleneck_ranking.join(", ")
    ));
    lines.push(format!("- {}", p.disclaimer));
    lines.join("\n")
}
