//! Clap CLI for `photon-bench`.

use std::path::PathBuf;

use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(name = "photon-bench", about = "Photon synthetic benchmark runner")]
pub struct Cli {
    #[command(subcommand)]
    pub command: Command,
}

#[derive(Subcommand)]
pub enum Command {
    /// List registered experiment IDs with status and summary.
    Experiments,
    /// Run one experiment id against a matrix slice.
    Run {
        #[arg(long, default_value = "bm-p0")]
        experiment: String,
        #[arg(long, default_value = "mem", value_parser = ["mem", "sqlite", "nats", "fluvio", "kafka"])]
        storage: String,
        #[arg(long, default_value = "off")]
        telemetry: String,
        #[arg(long)]
        topology: Option<String>,
        #[arg(long)]
        ops: Option<u32>,
        #[arg(long, default_value = "0")]
        warmup: u32,
        #[arg(long, default_value = "dev-wsl")]
        hardware: String,
        #[arg(long)]
        report: Option<PathBuf>,
        #[arg(long, help = "Broker node count metadata for fleet scaling reports")]
        nodes: Option<u32>,
        #[arg(long, help = "Parallel publisher count for firehose experiments")]
        publishers: Option<u32>,
        #[arg(
            long,
            help = "Offered publish rate for PD capacity experiments (bm-pd2/bm-pd3)"
        )]
        offered_rate: Option<u32>,
    },
    /// Run a campaign slice across the dimension matrix.
    Matrix {
        #[arg(long, default_value = "dev-wsl")]
        hardware: String,
        #[arg(long, default_value = "photon-minimal")]
        slice: String,
        #[arg(long)]
        from: Option<String>,
        #[arg(long, default_value = "mem", value_parser = ["mem", "sqlite", "nats", "fluvio", "kafka"])]
        storage: Option<String>,
        #[arg(long, default_value = "off")]
        telemetry: String,
        #[arg(long, help = "Override topology for non-sweep slices")]
        topology: Option<String>,
        #[arg(long)]
        skip_existing: bool,
    },
    /// Print hardware profile JSON (Appendix tables).
    Hardware {
        #[arg(long, default_value = "dev-wsl")]
        profile: String,
    },
    /// Build 1B/s fleet projection from collected report JSON files.
    ProjectFleet {
        #[arg(long, default_value = "dev-wsl")]
        hardware: String,
        #[arg(long, default_value = "mem")]
        storage: String,
        #[arg(long, default_value = "photon-bench/reports")]
        reports_dir: PathBuf,
        #[arg(long)]
        out: Option<PathBuf>,
    },
    /// Build `NATS` firehose scaling curve from `BM-PFH` sweep reports.
    ScalingCurve {
        #[arg(long, default_value = "dev-wsl")]
        hardware: String,
        #[arg(long, default_value = "nats")]
        storage: String,
        #[arg(long, default_value = "profiling/photon-bench/reports")]
        reports_dir: PathBuf,
        #[arg(long)]
        out: Option<PathBuf>,
        /// Filter reports by `JetStream` stream shard count (`1` = legacy single stream).
        #[arg(long)]
        stream_shards: Option<u32>,
        /// Include only reports where `stream_shards` equals `broker_nodes` (Phase 3 sharded ladder).
        #[arg(long, default_value_t = false)]
        match_broker_nodes: bool,
        /// Build curve from aggregate multibench reports (x-axis = `bench_client_count`).
        #[arg(long, default_value_t = false)]
        multibench_ladder: bool,
        /// Filter reports by bench client count (multibench dimension).
        #[arg(long)]
        bench_client_count: Option<u32>,
        /// Prefer the decision-grade PFH cell (`stream_seq`/ack=1, 256 pubs / `p256-r100000`).
        #[arg(long, default_value_t = false)]
        primary_row: bool,
    },
    /// Sum per-client `BM-PFH` reports into fleet aggregate JSON per sweep cell.
    AggregatePfh {
        #[arg(long, default_value = "profiling/photon-bench/reports")]
        reports_dir: PathBuf,
        #[arg(long)]
        out_dir: Option<PathBuf>,
        #[arg(long, default_value = "aws-c6i-large")]
        hardware: String,
        #[arg(long, default_value = "nats")]
        storage: String,
        /// Filter aggregate to reports whose filename starts with this prefix.
        #[arg(long)]
        cell_prefix: Option<String>,
    },
}
