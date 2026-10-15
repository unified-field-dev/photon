#![recursion_limit = "512"]
//! Synthetic Photon benchmarks (`BM-P*` / `BM-PL*`).
//!
//! Experiment registry and matrix campaign runner. Run `photon-bench experiments` to list registered IDs.
//!
//! ## Entry points
//!
//! - `photon-bench experiments` — list experiment IDs
//! - `photon-bench run` — single experiment against a matrix slice
//! - `photon-bench matrix` — campaign slice across dimensions

mod cli;
mod experiments;
mod fill_results;
mod harness;
mod matrix;
mod matrix_campaign;
mod matrix_run;
mod metrics;
mod projection;
mod report;
mod run;
mod stats;

use anyhow::Result;
use clap::Parser;
use cli::{Cli, Command};
use experiments::{status_label, REGISTRY};

#[tokio::main]
async fn main() -> Result<()> {
    match Cli::parse().command {
        Command::Experiments => {
            for meta in REGISTRY {
                println!(
                    "{}  {}  {}",
                    meta.id,
                    status_label(meta.status),
                    meta.summary
                );
            }
            println!("Run one with: photon-bench run --experiment <ID>");
        }
        Command::Run {
            experiment,
            storage,
            telemetry,
            topology,
            ops,
            warmup,
            hardware,
            report,
            nodes,
            publishers,
            offered_rate,
        } => {
            run::run_experiment(run::RunArgs {
                experiment,
                storage,
                telemetry,
                topology,
                ops,
                warmup,
                hardware,
                report,
                nodes,
                publishers,
                offered_rate,
            })
            .await?;
        }
        Command::Matrix {
            hardware,
            slice,
            from,
            storage,
            telemetry,
            topology,
            skip_existing,
        } => {
            matrix_run::run_matrix(matrix_run::MatrixRunOptions {
                hardware,
                slice,
                from,
                storage: storage.unwrap_or_else(|| "mem".into()),
                telemetry,
                topology,
                skip_existing,
            })
            .await?;
        }
        Command::Hardware { profile } => {
            std::env::set_var("PHOTON_BENCH_HARDWARE", &profile);
            let detail = harness::capture_hardware();
            println!("{}", serde_json::to_string_pretty(&detail)?);
        }
        Command::ProjectFleet {
            hardware,
            storage,
            reports_dir,
            out,
        } => projection::project_fleet(&hardware, &storage, &reports_dir, out)?,
        Command::ScalingCurve {
            hardware,
            storage,
            reports_dir,
            out,
            stream_shards,
            match_broker_nodes,
            multibench_ladder,
            bench_client_count,
            primary_row,
        } => projection::scaling_curve(
            &hardware,
            &storage,
            &reports_dir,
            out,
            stream_shards,
            match_broker_nodes,
            multibench_ladder,
            bench_client_count,
            primary_row,
        )?,
        Command::AggregatePfh {
            reports_dir,
            out_dir,
            hardware,
            storage,
            cell_prefix,
        } => {
            projection::aggregate_pfh(
                &reports_dir,
                out_dir.as_deref(),
                Some(&hardware),
                Some(&storage),
                cell_prefix.as_deref(),
            )?;
        }
        Command::FillResults { reports_dir } => fill_results::fill_results(&reports_dir)?,
    }
    Ok(())
}
