//! Build script for the photon facade (stub codegen placeholder).

fn main() {
    let out_dir = match std::env::var("OUT_DIR") {
        Ok(v) => std::path::PathBuf::from(v),
        Err(e) => panic!("OUT_DIR not set: {e}"),
    };
    if let Err(e) = std::fs::write(
        out_dir.join("generated_models.rs"),
        "// Photon facade — ops metadata codegen lives in integration hosts\n",
    ) {
        panic!("write stub generated_models.rs: {e}");
    }
}
