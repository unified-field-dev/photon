# Profiling reports

## Active

[`photon-bench/reports/`](photon-bench/reports/) — authoritative AWS fleet BM-P* JSON (`*-aws.json`, `aws-c6i-large` / `aws-t3-*` hardware labels).

Local smoke runs write to `photon-bench/reports-local/` (gitignored). Do not commit dev-wsl scratch output.

Authoritative NATS fleet numbers: in-VPC runs on `aws-c6i-large` per [`photon-bench/EXPERIMENTS.md`](../photon-bench/EXPERIMENTS.md).

## Criterion microbenches

CPU-path Criterion benches use experiment IDs with the **`criterion-*` / `BM-CRIT-*`** prefix (see [`EXPERIMENTS.md`](../photon-bench/EXPERIMENTS.md#criterion-microbenches-bm-crit-)). Raw Criterion HTML/JSON stays under `target/criterion/` (or your `CARGO_TARGET_DIR`). Decision-grade summaries are exported to this tree as `criterion-*-aws.json` / `mem-profile-*-aws.md` from AWS remote Criterion runs. Summarized in [`PERFORMANCE_STUDY.md` §10](../photon-bench/PERFORMANCE_STUDY.md#10-microbenchmarks--hot-path-baselines-2026-07-12). Use workspace `profile.profiling` (`debug = 1`) when collecting samples for flamegraphs.

## Archival

Surreal-mem rows under `photon-bench/reports/bm-*-surreal-mem-*` are pre-adapter historical artifacts. They are not used by the current bench runner (`photon-bench/src/matrix.rs` accepts `mem|sqlite|nats|fluvio|kafka`).
