//! Campaign matrix runner.

use std::path::PathBuf;

use anyhow::Result;

use crate::matrix::{matrix_from_cli, report_path, slice_experiments};
use crate::matrix_campaign::{deploy_shape_runs, slice_matrix_rows};
use crate::run::{run_experiment, RunArgs};

pub struct MatrixRunOptions {
    pub hardware: String,
    pub slice: String,
    pub from: Option<String>,
    pub storage: String,
    pub telemetry: String,
    pub topology: Option<String>,
    pub skip_existing: bool,
}

pub async fn run_matrix(opts: MatrixRunOptions) -> Result<()> {
    let mut experiments = slice_experiments(&opts.slice)?;
    if let Some(start) = opts.from {
        let idx = experiments
            .iter()
            .position(|id| *id == start.as_str())
            .unwrap_or(0);
        experiments = experiments.split_off(idx);
    }

    let base = matrix_from_cli(&opts.storage, &opts.telemetry, opts.topology.as_deref())?;
    let matrix_rows = slice_matrix_rows(&opts.slice, base);

    let reports_dir = PathBuf::from("photon-bench/reports");
    std::fs::create_dir_all(&reports_dir)?;

    for matrix in &matrix_rows {
        for experiment in &experiments {
            if opts.slice == "deploy-shape" && !deploy_shape_runs(experiment, matrix) {
                continue;
            }
            let report = report_path(&reports_dir, experiment, matrix, &opts.hardware);
            if opts.skip_existing && report.exists() {
                println!("skip existing {}", report.display());
                continue;
            }
            println!("matrix run {experiment} -> {}", report.display());
            run_experiment(RunArgs {
                experiment: (*experiment).into(),
                storage: opts.storage.clone(),
                telemetry: serde_json::to_string(&matrix.telemetry)
                    .unwrap_or_default()
                    .trim_matches('"')
                    .to_string(),
                topology: Some(
                    serde_json::to_string(&matrix.topology)
                        .unwrap_or_default()
                        .trim_matches('"')
                        .to_string(),
                ),
                ops: None,
                warmup: 0,
                hardware: opts.hardware.clone(),
                report: Some(report),
                nodes: None,
                publishers: None,
            })
            .await?;
        }
    }
    Ok(())
}
