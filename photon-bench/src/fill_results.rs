//! Sync `EXPERIMENTS.md` Results snippets from report JSON (best-effort).

use std::path::Path;

use anyhow::{Context, Result};

pub fn fill_results(reports_dir: &Path) -> Result<()> {
    let experiments_md = Path::new("photon-bench/EXPERIMENTS.md");
    let mut body = std::fs::read_to_string(experiments_md)
        .with_context(|| format!("read {}", experiments_md.display()))?;

    let mut updated = 0usize;
    for entry in std::fs::read_dir(reports_dir)? {
        let entry = entry?;
        let path = entry.path();
        if path.extension().and_then(|e| e.to_str()) != Some("json") {
            continue;
        }
        let text = std::fs::read_to_string(&path)?;
        let v: serde_json::Value = serde_json::from_str(&text)?;
        let experiment = v.get("experiment").and_then(|e| e.as_str()).unwrap_or("");
        let pass = v
            .get("pass")
            .and_then(serde_json::Value::as_bool)
            .unwrap_or(false);
        let status = v.get("status").and_then(|s| s.as_str()).unwrap_or("?");
        let snippet = format!(
            "{status} pass={pass} ({})",
            path.file_name().unwrap().to_string_lossy()
        );
        let needle = format!("| **{experiment}** |");
        if let Some(line_start) = body.find(&needle) {
            if let Some(line_end) = body[line_start..].find('\n') {
                let line = &body[line_start..line_start + line_end];
                if line.contains("| Results |") || line.matches('|').count() >= 6 {
                    let parts: Vec<_> = line.split('|').collect();
                    if parts.len() >= 7 {
                        let new_line = format!("{}| {} |", parts[..6].join("|"), snippet);
                        body.replace_range(line_start..line_start + line_end, &new_line);
                        updated += 1;
                    }
                }
            }
        }
    }

    if updated > 0 {
        std::fs::write(experiments_md, body)?;
        println!("updated {updated} rows in EXPERIMENTS.md");
    } else {
        println!("no matching rows updated in EXPERIMENTS.md");
    }
    Ok(())
}
