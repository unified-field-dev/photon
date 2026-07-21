//! 1B/s fleet projection toolchain.

mod aggregate;
mod inputs;
mod model;
mod pfh_reports;
mod render;
mod scaling;

use std::path::{Path, PathBuf};

use anyhow::Result;

pub use aggregate::aggregate_pfh;
pub use scaling::scaling_curve;

pub fn project_fleet(
    hardware: &str,
    storage: &str,
    reports_dir: &Path,
    out: Option<PathBuf>,
) -> Result<()> {
    let inputs = inputs::load_from_dir(reports_dir, hardware, storage)?;
    let mut projection = model::compute(&inputs);
    if storage == "nats" {
        let curve = scaling::load_scaling_curve(
            reports_dir,
            hardware,
            storage,
            None,
            true,
            false,
            None,
            true,
        )
        .or_else(|_| {
            scaling::load_scaling_curve(
                reports_dir,
                hardware,
                storage,
                None,
                false,
                false,
                None,
                true,
            )
        });
        if let Ok(curve) = curve {
            projection.broker_nodes_for_1e9 =
                curve.broker_nodes_for_target.get("1000000000").copied();
            projection.broker_nodes_for_1m = curve.broker_nodes_for_target.get("1000000").copied();
            projection.nats_bottleneck_verdict = curve.bottleneck_verdict;
        }
    }
    let out_path =
        out.unwrap_or_else(|| reports_dir.join(format!("projection-{hardware}-{storage}.json")));
    inputs::write_projection(&out_path, &projection)?;
    println!("wrote {}", out_path.display());
    println!("{}", render::render_markdown(&projection));
    Ok(())
}
