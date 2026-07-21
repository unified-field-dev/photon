//! Process CPU/RAM sampling during benchmark scenarios.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};
use tokio::task::JoinHandle;

const SAMPLE_INTERVAL: Duration = Duration::from_millis(500);

/// Resource utilization captured during a run.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ResourceProfile {
    pub process_rss_bytes_start: u64,
    pub process_rss_bytes_end: u64,
    pub process_rss_bytes_peak: u64,
    pub process_cpu_percent_mean: f64,
    pub process_cpu_percent_peak: f64,
    pub system_mem_available_bytes_min: u64,
}

pub fn resource_profiling_enabled(hardware: &str) -> bool {
    if std::env::var("PHOTON_BENCH_RESOURCE_PROFILE")
        .ok()
        .is_some_and(|v| v == "1" || v.eq_ignore_ascii_case("true"))
    {
        return true;
    }
    hardware.starts_with("aws-")
}

pub struct ResourceSampler {
    running: Arc<AtomicBool>,
    handle: JoinHandle<ResourceProfile>,
    rss_start: u64,
}

impl ResourceSampler {
    pub fn start() -> Self {
        let rss_start = read_vm_rss().unwrap_or(0);
        let running = Arc::new(AtomicBool::new(true));
        let running_task = Arc::clone(&running);

        let handle = tokio::spawn(async move {
            let mut peak_rss = rss_start;
            let mut end_rss = rss_start;
            let mut min_mem_avail = read_mem_available().unwrap_or(0);
            let mut cpu_samples: Vec<f64> = Vec::new();

            let clock_ticks = clock_ticks_per_sec();
            let ncpu =
                std::thread::available_parallelism().map_or(1, std::num::NonZero::get) as f64;
            let mut prev_ticks = read_cpu_ticks().unwrap_or(0);
            let mut prev_at = Instant::now();

            let mut interval = tokio::time::interval(SAMPLE_INTERVAL);
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

            while running_task.load(Ordering::Relaxed) {
                interval.tick().await;
                if !running_task.load(Ordering::Relaxed) {
                    break;
                }
                if let Some(rss) = read_vm_rss() {
                    peak_rss = peak_rss.max(rss);
                    end_rss = rss;
                }
                if let Some(avail) = read_mem_available() {
                    min_mem_avail = min_mem_avail.min(avail);
                }
                let now = Instant::now();
                if let Some(ticks) = read_cpu_ticks() {
                    let elapsed = now.duration_since(prev_at).as_secs_f64();
                    if elapsed > 0.0 {
                        let delta = ticks.saturating_sub(prev_ticks) as f64;
                        let pct = (delta / elapsed) / clock_ticks as f64 / ncpu * 100.0;
                        cpu_samples.push(pct);
                    }
                    prev_ticks = ticks;
                    prev_at = now;
                }
            }

            let cpu_mean = if cpu_samples.is_empty() {
                0.0
            } else {
                cpu_samples.iter().sum::<f64>() / cpu_samples.len() as f64
            };
            let cpu_peak = cpu_samples.iter().copied().fold(0.0_f64, f64::max);

            ResourceProfile {
                process_rss_bytes_start: rss_start,
                process_rss_bytes_end: end_rss,
                process_rss_bytes_peak: peak_rss,
                process_cpu_percent_mean: cpu_mean,
                process_cpu_percent_peak: cpu_peak,
                system_mem_available_bytes_min: min_mem_avail,
            }
        });

        Self {
            running,
            handle,
            rss_start,
        }
    }

    pub async fn finish(self) -> ResourceProfile {
        self.running.store(false, Ordering::Relaxed);
        match self.handle.await {
            Ok(profile) => profile,
            Err(_) => ResourceProfile {
                process_rss_bytes_start: self.rss_start,
                process_rss_bytes_end: self.rss_start,
                process_rss_bytes_peak: self.rss_start,
                process_cpu_percent_mean: 0.0,
                process_cpu_percent_peak: 0.0,
                system_mem_available_bytes_min: read_mem_available().unwrap_or(0),
            },
        }
    }
}

fn clock_ticks_per_sec() -> u64 {
    std::env::var("CLK_TCK")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(100)
}

fn read_vm_rss() -> Option<u64> {
    let status = std::fs::read_to_string("/proc/self/status").ok()?;
    for line in status.lines() {
        if let Some(kb) = line.strip_prefix("VmRSS:") {
            let kb: u64 = kb.split_whitespace().next()?.parse().ok()?;
            return Some(kb * 1024);
        }
    }
    None
}

fn read_mem_available() -> Option<u64> {
    let meminfo = std::fs::read_to_string("/proc/meminfo").ok()?;
    for line in meminfo.lines() {
        if let Some(kb) = line.strip_prefix("MemAvailable:") {
            let kb: u64 = kb.split_whitespace().next()?.parse().ok()?;
            return Some(kb * 1024);
        }
    }
    None
}

fn read_cpu_ticks() -> Option<u64> {
    let stat = std::fs::read_to_string("/proc/self/stat").ok()?;
    let rparen = stat.rfind(')')?;
    let rest = stat.get(rparen + 2..)?;
    let fields: Vec<&str> = rest.split_whitespace().collect();
    if fields.len() < 2 {
        return None;
    }
    let utime: u64 = fields[10].parse().ok()?;
    let stime: u64 = fields[11].parse().ok()?;
    Some(utime + stime)
}
