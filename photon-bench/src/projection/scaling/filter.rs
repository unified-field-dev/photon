//! Report inclusion and point parsing for scaling curve construction.

use std::path::Path;

use serde_json::Value;

/// Authoritative PFH primary row: `stream_seq`, `sync_ack`=1, 256 publishers, 100k target.
pub const PRIMARY_ROW_PUBLISHERS: u64 = 256;
pub const PRIMARY_ROW_FILENAME_MARKER: &str = "p256-r100000";
pub const PRIMARY_ROW_REPLAY_CURSOR: &str = "stream_seq";
pub const PRIMARY_ROW_SYNC_ACK: &str = "1";

pub struct ScalingFilter {
    pub stream_shards: Option<u32>,
    pub match_broker_nodes: bool,
    pub multibench_ladder: bool,
    pub bench_client_count_filter: Option<u32>,
    /// Prefer the decision-grade PFH cell (`stream_seq`/ack=1, 256 pubs / 100k target).
    pub primary_row: bool,
}

pub struct ReportMeta {
    pub report_shards: u32,
    pub nodes: u32,
    pub bench_clients: Option<u32>,
    pub is_aggregate: bool,
    pub fname: String,
}

pub struct ParsedPoint {
    pub rate: f64,
    pub config_label: String,
    pub bench_peak: Option<f64>,
    pub point_key: u32,
    pub broker_nodes: u32,
    pub fname: String,
}

pub fn extract_meta(path: &Path, v: &Value) -> ReportMeta {
    let report_shards = v
        .pointer("/dimensions/stream_shards")
        .and_then(Value::as_u64)
        .unwrap_or(1) as u32;
    let nodes = v
        .get("node_count")
        .or_else(|| v.pointer("/dimensions/broker_nodes"))
        .and_then(Value::as_u64)
        .unwrap_or(1) as u32;
    let bench_clients = v
        .pointer("/dimensions/bench_client_count")
        .and_then(Value::as_u64)
        .map(|n| n as u32);
    let is_aggregate = v
        .pointer("/dimensions/aggregate")
        .and_then(Value::as_bool)
        .unwrap_or(false)
        || path
            .file_name()
            .and_then(|n| n.to_str())
            .is_some_and(|n| n.contains("-aggregate-"));
    let fname = path
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap_or("")
        .to_string();
    ReportMeta {
        report_shards,
        nodes,
        bench_clients,
        is_aggregate,
        fname,
    }
}

pub fn is_primary_row(path: &Path, v: &Value) -> bool {
    let fname = path.file_name().and_then(|n| n.to_str()).unwrap_or("");
    let publishers_ok = fname.contains(PRIMARY_ROW_FILENAME_MARKER)
        || v.pointer("/dimensions/publishers")
            .and_then(Value::as_u64)
            .is_some_and(|p| p == PRIMARY_ROW_PUBLISHERS);
    if !publishers_ok {
        return false;
    }
    let cursor = v
        .pointer("/dimensions/replay_cursor")
        .and_then(Value::as_str)
        .unwrap_or(PRIMARY_ROW_REPLAY_CURSOR);
    let sync_ack = v
        .pointer("/dimensions/sync_ack")
        .and_then(Value::as_str)
        .unwrap_or(PRIMARY_ROW_SYNC_ACK);
    cursor == PRIMARY_ROW_REPLAY_CURSOR && sync_ack == PRIMARY_ROW_SYNC_ACK
}

pub fn should_include(meta: &ReportMeta, filter: &ScalingFilter) -> bool {
    if filter.multibench_ladder {
        return meta.is_aggregate
            && meta.fname.contains("n4-sh4-bc")
            && filter
                .bench_client_count_filter
                .is_none_or(|bc| meta.bench_clients.unwrap_or(1) == bc);
    }
    if meta.is_aggregate {
        return false;
    }
    // Exclude multibench per-client reports from baseline/sharded ladders.
    if meta.fname.contains("-bc") {
        return false;
    }
    if filter.match_broker_nodes && meta.report_shards != meta.nodes {
        return false;
    }
    if !filter.match_broker_nodes {
        if let Some(shard_filter) = filter.stream_shards {
            if meta.report_shards != shard_filter {
                return false;
            }
        }
    }
    filter
        .bench_client_count_filter
        .is_none_or(|bc| meta.bench_clients.unwrap_or(1) == bc)
}

pub fn parse_point(v: &Value, meta: &ReportMeta, multibench_ladder: bool) -> Option<ParsedPoint> {
    let rate = v
        .get("fleet_aggregate_ops_per_sec")
        .and_then(Value::as_f64)
        .or_else(|| v.get("achieved_ops_per_sec").and_then(Value::as_f64))
        .unwrap_or(0.0);
    if rate <= 0.0 {
        return None;
    }
    let config = v
        .pointer("/dimensions/replay_cursor")
        .and_then(Value::as_str)
        .unwrap_or("stream_seq");
    let sync_ack = v
        .pointer("/dimensions/sync_ack")
        .and_then(Value::as_str)
        .unwrap_or("1");
    let config_label = format!("{config}/ack={sync_ack}");
    let bench_peak = v
        .pointer("/diagnostics/nats_bench_peak_ops")
        .and_then(Value::as_f64);
    let point_key = if multibench_ladder {
        meta.bench_clients.unwrap_or(1)
    } else {
        meta.nodes
    };
    Some(ParsedPoint {
        rate,
        config_label,
        bench_peak,
        point_key,
        broker_nodes: meta.nodes,
        fname: meta.fname.clone(),
    })
}
