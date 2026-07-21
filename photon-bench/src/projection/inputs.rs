//! Load projection inputs from collected bench JSON reports.

use std::path::Path;

use anyhow::{Context, Result};
use serde_json::Value;

#[derive(Debug, Default, Clone)]
pub struct ProjectionInputs {
    pub hardware: String,
    pub storage: String,
    pub p0_rate: Option<f64>,
    pub pl1_rate: Option<f64>,
    pub pl2_rate: Option<f64>,
    pub p2_p95_ratio: Option<f64>,
    pub backlog_peak: Option<u64>,
    pub error_rate: Option<f64>,
    pub p6_delivery_p99_ms: Option<f64>,
}

pub fn load_from_dir(
    reports_dir: &Path,
    hardware: &str,
    storage: &str,
) -> Result<ProjectionInputs> {
    let mut inputs = ProjectionInputs {
        hardware: hardware.into(),
        storage: storage.into(),
        ..Default::default()
    };

    for entry in std::fs::read_dir(reports_dir)? {
        let path = entry?.path();
        if path.extension().and_then(|e| e.to_str()) != Some("json") {
            continue;
        }
        let text = std::fs::read_to_string(&path)?;
        let v: Value = serde_json::from_str(&text)?;
        if v.get("hardware").and_then(|h| h.as_str()) != Some(hardware) {
            continue;
        }
        if v.get("storage").and_then(|s| s.as_str()) != Some(storage) {
            continue;
        }
        let id = v.get("experiment").and_then(|e| e.as_str()).unwrap_or("");
        match id {
            "bm-p0" => {
                inputs.p0_rate = v
                    .get("achieved_ops_per_sec")
                    .and_then(serde_json::Value::as_f64);
            }
            "bm-pl1" => {
                inputs.pl1_rate = v
                    .get("achieved_ops_per_sec")
                    .and_then(serde_json::Value::as_f64);
            }
            "bm-pl2" => {
                inputs.pl2_rate = v
                    .get("achieved_ops_per_sec")
                    .and_then(serde_json::Value::as_f64);
            }
            "bm-p3" => {
                inputs.backlog_peak = v.get("backlog_peak").and_then(serde_json::Value::as_u64);
            }
            "bm-p8" | "bm-pl0" | "bm-pl3" => {
                let err = v.get("error_rate").and_then(serde_json::Value::as_f64);
                if err.is_some() {
                    inputs.error_rate = err;
                }
            }
            "bm-p6" => {
                inputs.p6_delivery_p99_ms = v
                    .get("delivery_wait_ms")
                    .and_then(|d| d.get("p99"))
                    .and_then(serde_json::Value::as_f64);
            }
            _ => {}
        }
    }

    if inputs.p0_rate.is_none() && inputs.pl1_rate.is_none() {
        anyhow::bail!(
            "missing projection inputs for {hardware}/{storage} in {}",
            reports_dir.display()
        );
    }
    Ok(inputs)
}

pub fn write_projection(path: &Path, projection: &super::model::FleetProjection) -> Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let json = serde_json::to_string_pretty(projection)?;
    std::fs::write(path, json).with_context(|| format!("write {}", path.display()))?;
    Ok(())
}
