# Profiling reports

## Active

Bench runs write JSON to `profiling/photon-bench/reports/` (`matrix` runs write to `photon-bench/reports/` at the repo root). Both are gitignored local scratch. Decision-grade AWS fleet BM-P* JSON (`*-aws.json`, `aws-c6i-large` / `aws-t3-*` hardware labels) isn't committed to this repo.

Local smoke runs write to `profiling/photon-bench/reports-local/` (gitignored). Do not commit dev-wsl scratch output.

Authoritative NATS fleet numbers: in-VPC runs on `aws-c6i-large`; see [`photon-bench/PERFORMANCE.md`](../photon-bench/PERFORMANCE.md).

## Criterion microbenches

CPU-path Criterion benches use experiment IDs with the **`criterion-*` / `BM-CRIT-*`** prefix; the benches live in `photon-backend/benches/`. Raw Criterion HTML/JSON stays under `target/criterion/` (or your `CARGO_TARGET_DIR`). Decision-grade summaries from AWS remote Criterion runs (`criterion-*-aws.json` / `mem-profile-*-aws.md`) aren't committed to this repo. Use workspace `profile.profiling` (`debug = 1`) when collecting samples for flamegraphs.

## Archival

Surreal-mem rows (`bm-*-surreal-mem-*`) are pre-adapter historical artifacts and aren't committed to this repo. They are not used by the current bench runner (`photon-bench/src/matrix.rs` accepts `mem|sqlite|nats|fluvio|kafka`).
