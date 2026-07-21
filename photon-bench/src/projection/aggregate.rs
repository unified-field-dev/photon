//! Aggregate per-client `BM-PFH` reports into fleet totals.

use std::collections::HashMap;
use std::path::{Path, PathBuf};

use anyhow::{bail, Context, Result};
use serde_json::{json, Value};

use crate::projection::pfh_reports;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct CellKey {
    broker_nodes: u32,
    stream_shards: u32,
    replay_cursor: String,
    sync_ack: String,
    publishers: u32,
    bench_client_count: u32,
}

fn cell_key(v: &Value) -> Option<CellKey> {
    let dims = v.get("dimensions")?;
    let aggregate = dims
        .get("aggregate")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    if aggregate {
        return None;
    }
    Some(CellKey {
        broker_nodes: dims
            .get("broker_nodes")
            .and_then(Value::as_u64)
            .unwrap_or_else(|| v.get("node_count").and_then(Value::as_u64).unwrap_or(1))
            as u32,
        stream_shards: dims
            .get("stream_shards")
            .and_then(Value::as_u64)
            .unwrap_or(1) as u32,
        replay_cursor: dims
            .get("replay_cursor")
            .and_then(Value::as_str)
            .unwrap_or("stream_seq")
            .to_string(),
        sync_ack: dims
            .get("sync_ack")
            .and_then(Value::as_str)
            .unwrap_or("1")
            .to_string(),
        publishers: dims.get("publishers").and_then(Value::as_u64).unwrap_or(32) as u32,
        bench_client_count: dims
            .get("bench_client_count")
            .and_then(Value::as_u64)
            .unwrap_or(1) as u32,
    })
}

fn report_rate(v: &Value) -> f64 {
    v.get("fleet_aggregate_ops_per_sec")
        .or_else(|| v.get("achieved_ops_per_sec"))
        .and_then(Value::as_f64)
        .unwrap_or(0.0)
}

fn aggregate_filename(template: &str, count: u32) -> String {
    if let Some(pos) = template.find("-i0-") {
        let mut name = template.to_string();
        name.replace_range(pos..pos + 4, "-aggregate-");
        if !name.contains("-bc") {
            if let Some(npos) = name.find("-sh") {
                if let Some(rest) = name.get(npos..) {
                    if let Some(end) = rest.find('-') {
                        let insert = format!("-bc{count}");
                        name.insert_str(npos + end, &insert);
                    }
                }
            }
        }
        return name;
    }
    template.replace("-aws.json", &format!("-bc{count}-aggregate-aws.json"))
}

struct AggregateFilters<'a> {
    hardware: Option<&'a str>,
    storage: Option<&'a str>,
    cell_prefix: Option<&'a str>,
}

fn should_include_aggregate_report(fname: &str, v: &Value, filters: &AggregateFilters<'_>) -> bool {
    if fname.contains("-aggregate-") {
        return false;
    }
    if let Some(prefix) = filters.cell_prefix {
        if !fname.starts_with(prefix) {
            return false;
        }
    }
    let has_client_idx = v.pointer("/dimensions/bench_client_index").is_some()
        || fname.contains("-i0-")
        || fname.contains("-i1-")
        || fname.contains("-i2-")
        || fname.contains("-i3-");
    if filters.cell_prefix.is_some() && !has_client_idx {
        return false;
    }
    if let Some(hw) = filters.hardware {
        if v.get("hardware").and_then(|h| h.as_str()) != Some(hw) {
            return false;
        }
    }
    if let Some(st) = filters.storage {
        if v.get("storage").and_then(|s| s.as_str()) != Some(st) {
            return false;
        }
    }
    true
}

fn walk_pfh_reports(
    reports_dir: &Path,
    filters: &AggregateFilters<'_>,
) -> Result<HashMap<CellKey, HashMap<u32, (Value, String)>>> {
    let hardware = filters.hardware.unwrap_or("");
    let storage = filters.storage.unwrap_or("");
    let mut groups: HashMap<CellKey, HashMap<u32, (Value, String)>> = HashMap::new();

    for (path, v) in pfh_reports::read_pfh_reports(reports_dir, hardware, storage)? {
        let fname = path.file_name().and_then(|n| n.to_str()).unwrap_or("");
        if !should_include_aggregate_report(fname, &v, filters) {
            continue;
        }
        let Some(key) = cell_key(&v) else {
            continue;
        };
        let idx = v
            .pointer("/dimensions/bench_client_index")
            .and_then(Value::as_u64)
            .unwrap_or(0) as u32;
        groups
            .entry(key)
            .or_default()
            .insert(idx, (v, fname.to_string()));
    }
    Ok(groups)
}

fn write_aggregate_reports(
    groups: HashMap<CellKey, HashMap<u32, (Value, String)>>,
    out_dir: &Path,
) -> Result<Vec<PathBuf>> {
    let mut written = Vec::new();
    for (key, clients) in groups {
        let count = key.bench_client_count;
        if count < 1 {
            continue;
        }
        for i in 0..count {
            if !clients.contains_key(&i) {
                bail!(
                    "missing bench_client_index={i} for cell broker_nodes={} stream_shards={} bc={count}",
                    key.broker_nodes,
                    key.stream_shards
                );
            }
        }

        let mut total = 0.0;
        let mut max_err = 0.0f64;
        let (base, base_fname) = clients.get(&0).unwrap();
        for i in 0..count {
            let (v, _) = clients.get(&i).unwrap();
            let rate = report_rate(v);
            if rate <= 0.0 {
                bail!("zero achieved rate for bench_client_index={i}");
            }
            let err = v.get("error_rate").and_then(Value::as_f64).unwrap_or(0.0);
            if err >= 0.001 {
                bail!("error_rate {err} >= 0.1% for bench_client_index={i}");
            }
            total += rate;
            max_err = max_err.max(err);
        }

        let mut agg = base.clone();
        if let Some(obj) = agg.as_object_mut() {
            obj.insert("achieved_ops_per_sec".into(), json!(total));
            obj.insert("fleet_aggregate_ops_per_sec".into(), json!(total));
            obj.insert("error_rate".into(), json!(max_err));
            if let Some(dims) = obj.get_mut("dimensions").and_then(Value::as_object_mut) {
                dims.insert("aggregate".into(), json!(true));
                dims.remove("bench_client_index");
            }
            let out_name = aggregate_filename(base_fname, count);
            let out_path = out_dir.join(&out_name);
            std::fs::write(&out_path, serde_json::to_string_pretty(&agg)?)
                .with_context(|| format!("write {}", out_path.display()))?;
            println!(
                "aggregate bc={count} broker_nodes={} stream_shards={}: {:.0} ops/s -> {}",
                key.broker_nodes,
                key.stream_shards,
                total,
                out_path.display()
            );
            written.push(out_path);
        }
    }
    Ok(written)
}

pub fn aggregate_pfh(
    reports_dir: &Path,
    out_dir: Option<&Path>,
    hardware_filter: Option<&str>,
    storage_filter: Option<&str>,
    cell_prefix: Option<&str>,
) -> Result<Vec<PathBuf>> {
    let out_dir = out_dir.unwrap_or(reports_dir);
    std::fs::create_dir_all(out_dir)?;

    let filters = AggregateFilters {
        hardware: hardware_filter,
        storage: storage_filter,
        cell_prefix,
    };
    let groups = walk_pfh_reports(reports_dir, &filters)?;

    if groups.is_empty() {
        bail!("no per-client BM-PFH reports in {}", reports_dir.display());
    }

    write_aggregate_reports(groups, out_dir)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::TempDir;

    fn client_report(idx: u32, count: u32, rate: f64) -> String {
        format!(
            r#"{{
            "experiment": "bm-pfh",
            "hardware": "aws-c6i-large",
            "storage": "nats",
            "node_count": 4,
            "achieved_ops_per_sec": {rate},
            "error_rate": 0.0,
            "dimensions": {{
                "broker_nodes": 4,
                "stream_shards": 4,
                "publishers": 256,
                "replay_cursor": "stream_seq",
                "sync_ack": "1",
                "bench_client_index": {idx},
                "bench_client_count": {count}
            }}
        }}"#
        )
    }

    #[test]
    fn aggregate_sums_clients() {
        let dir = TempDir::new().unwrap();
        let mut f0 = std::fs::File::create(
            dir.path()
                .join("bm-pfh-nats-n4-sh4-bc2-i0-stream_seq-ack1-p256-r100000-aws.json"),
        )
        .unwrap();
        write!(f0, "{}", client_report(0, 2, 90000.0)).unwrap();
        let mut f1 = std::fs::File::create(
            dir.path()
                .join("bm-pfh-nats-n4-sh4-bc2-i1-stream_seq-ack1-p256-r100000-aws.json"),
        )
        .unwrap();
        write!(f1, "{}", client_report(1, 2, 85000.0)).unwrap();

        let written =
            aggregate_pfh(dir.path(), None, Some("aws-c6i-large"), Some("nats"), None).unwrap();
        assert_eq!(written.len(), 1);
        let text = std::fs::read_to_string(&written[0]).unwrap();
        let v: Value = serde_json::from_str(&text).unwrap();
        assert_eq!(
            v.get("fleet_aggregate_ops_per_sec").and_then(Value::as_f64),
            Some(175_000.0)
        );
    }
}
