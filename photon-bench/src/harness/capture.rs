//! Capture CPU/RAM/OS metadata (lightweight, no extra deps).

use serde::{Deserialize, Serialize};
use std::process::Command;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct HardwareDetail {
    pub cpu_model: String,
    pub cpu_cores: usize,
    pub ram_gib: f64,
    pub os: String,
    pub root_mount: String,
}

pub fn capture() -> HardwareDetail {
    HardwareDetail {
        cpu_model: read_cpu_model(),
        cpu_cores: std::thread::available_parallelism().map_or(1, std::num::NonZero::get),
        ram_gib: read_ram_gib(),
        os: read_os_string(),
        root_mount: read_root_mount(),
    }
}

fn read_cpu_model() -> String {
    std::fs::read_to_string("/proc/cpuinfo")
        .ok()
        .and_then(|s| {
            s.lines()
                .find(|l| l.starts_with("model name"))
                .map(|l| l.split_once(':').map_or(l, |(_, v)| v.trim()))
                .map(str::to_string)
        })
        .unwrap_or_else(|| "unknown".into())
}

fn read_ram_gib() -> f64 {
    std::fs::read_to_string("/proc/meminfo")
        .ok()
        .and_then(|s| {
            s.lines()
                .find(|l| l.starts_with("MemTotal:"))
                .and_then(|l| l.split_whitespace().nth(1))
                .and_then(|kb| kb.parse::<f64>().ok())
        })
        .map_or(0.0, |kb| kb / (1024.0 * 1024.0))
}

fn read_os_string() -> String {
    let uname = Command::new("uname")
        .arg("-sr")
        .output()
        .ok()
        .and_then(|o| String::from_utf8(o.stdout).ok())
        .map_or_else(|| "unknown".into(), |s| s.trim().to_string());
    if let Ok(rel) = std::fs::read_to_string("/etc/os-release") {
        let pretty = rel
            .lines()
            .find(|l| l.starts_with("PRETTY_NAME="))
            .map_or("", |l| {
                l.trim_start_matches("PRETTY_NAME=").trim_matches('"')
            });
        if !pretty.is_empty() {
            return format!("{uname} ({pretty})");
        }
    }
    uname
}

fn read_root_mount() -> String {
    std::fs::read_to_string("/proc/mounts")
        .ok()
        .and_then(|s| {
            s.lines()
                .find(|l| {
                    let parts: Vec<_> = l.split_whitespace().collect();
                    parts.len() >= 3 && parts[1] == "/"
                })
                .map(|l| l.split_whitespace().next().unwrap_or("unknown").to_string())
        })
        .unwrap_or_else(|| "unknown".into())
}
