# Documentation verification

Re-run after doc changes. See [CONTRIBUTING.md](CONTRIBUTING.md#documentation).

**Local laptop `cargo` is not the verification path** when the toolchain is broken or incomplete. Prefer an AWS SQLite smoke host: provision + bootstrap an EC2 instance, rsync this repo, then run remote check / CI-subset / full e2e smoke there.

That path runs `cargo check`, full-workspace Clippy (`--all-targets --all-features -- -D warnings`), rustdoc (`RUSTDOCFLAGS=-D warnings`), and `photon-backend` tests. Broader CI subset covers deny, e2e mem/sqlite, bench, examples, and doctests. Full E2E smoke exercises the adapter matrix on the same host class.

## Commands

Run these on the AWS smoke host (or CI), not as a laptop-only gate:

```bash
# Workspace missing_docs deny
cargo check --workspace --features runtime,mem

# Workspace API docs (fail on rustdoc warnings)
RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps --all-features

# Public-crate-only docs (quick check)
RUSTDOCFLAGS="-D warnings" cargo doc -p uf-photon --features runtime,mem --no-deps

# Doctests (rust,no_run compile-checked)
cargo test -p uf-photon --doc --features runtime,mem
cargo test -p photon-runtime --doc --features runtime,mem
cargo test -p photon-macros --doc
cargo test -p photon-backend --doc --features runtime

# Examples on the public crate (need PHOTON_TRANSPORT_KEY — smoke scripts export it)
cargo run -p uf-photon --example embedded_mem --features runtime,mem
cargo run -p uf-photon --example embedded_sqlite --features runtime,sqlite
cargo run -p uf-photon --example subscribe_v2 --features runtime,mem
cargo run -p uf-photon --example consumer_group --features runtime,mem
cargo run -p uf-photon --example manual_subscribe --features runtime,mem
cargo run -p uf-photon --example keyed_topic --features runtime,mem
cargo run -p uf-photon --example telemetry_ops_log --features runtime,mem
# Brokered NATS pair (requires JetStream; see photon/README.md § How to run examples)
# cargo run -p uf-photon --example nats_worker --features runtime,nats
# cargo run -p uf-photon --example nats_publisher --features runtime,nats

# Integration
cargo test -p photon-e2e
cargo test -p photon-backend --features runtime --tests
```

## Encrypted checkpoint delivery (BM-PD*) local smoke

BM-PD0/PD1 measure encrypted publish-to-checkpoint delivery at locked offered rates (1k/s and 500/s). BM-PD2/PD3 keep the same checkpoint contract and sweep offered rate until the first failing cell (`highest_passing_offered_rate`). Durable subscribers record through `CheckpointCoalescer` (the same path as `#[subscribe]` handlers) and flush before the durable sequence check. That is a different metric from BM-P0/BM-PFH publisher ingress and from broker append acknowledgement. Reports use `delivered_ops_per_sec` and per-message `consume_ack_ms` (p50/p95/p99), not BM-P1 `delivery_wait_ms`.

Crypto stays on. `PHOTON_BENCH_CRYPTO=0` fails the PD report closed. PFH campaigns still disable crypto for ingress measurement; do not copy that env into PD runs.

```bash
export PHOTON_TRANSPORT_KEY=cGhvdG9uLWRldi10cmFuc3BvcnQta2V5LTMyYnl0ZXM=

# Harness tests (mem; sqlite needs --features sqlite)
cargo test -p photon-testkit checkpoint
cargo test -p photon-testkit --features sqlite sqlite_encrypted_checkpoint
cargo test -p photon-bench pd

# 1-second CLI smoke (default PD0 is 1k/s × 30s)
cargo run -p photon-bench -- run --experiment bm-pd0 --storage mem --ops 1 --telemetry off
cargo run -p photon-bench --features sqlite -- run --experiment bm-pd0 --storage sqlite --ops 1 --telemetry off
PHOTON_BENCH_OFFERED_RATE=50 cargo run -p photon-bench -- run --experiment bm-pd2 --storage mem --ops 1 --telemetry off

# NATS operator path (JetStream in-VPC or local cluster; 4-shard AWS campaign uses broker-fleet)
cargo run -p photon-bench --features nats -- run --experiment bm-pd1 --storage nats --topology broker-cluster
```

SQLite PD0 on a `t3.medium` is enough for local/harness checks. Authoritative NATS PD numbers come from the AWS 4-shard in-VPC campaign, whose tooling is maintained separately and isn't part of this repo.

## Line coverage (CI artifact)

PR CI runs a non-blocking [`coverage`](../.github/workflows/ci.yml) job with `cargo-llvm-cov`. The job also checks a soft floor (`COVERAGE_FLOOR_PCT` in the workflow); it stays `continue-on-error` until that floor is raised deliberately.

```bash
# Install once
cargo install cargo-llvm-cov

# Summary to stdout (excludes photon-e2e / photon-bench — timing-sensitive under instrumentation)
cargo llvm-cov --workspace --exclude photon-e2e --exclude photon-bench --features runtime,mem --summary-only

# LCOV for local inspection / Codecov upload
cargo llvm-cov --workspace --exclude photon-e2e --exclude photon-bench --features runtime,mem --lcov --output-path lcov.info
```

Download `coverage-lcov` from the GitHub Actions run artifacts for the CI report.

## Notes

- Workspace `[workspace.lints.rust] missing_docs = "deny"`; library members use `[lints] workspace = true`. Test/bench crates (`photon-e2e`, `photon-testkit`, `photon-bench`) declare matching Clippy tables so they can allow `unwrap_used` / `expect_used`.
- Restriction lints (`unwrap_used`, `expect_used`, `dbg_macro`, `print_*`, `todo`) are enforced at the workspace level — see [`CONTRIBUTING.md`](../CONTRIBUTING.md#rust-standards--lint-policy).
- Lane map on public crate: **Creating topics** / **Integrating the host** / **Developing the backend**
- Config reference: `photon::config` includes `docs/configuration.md`
- Macro expansion: `docs/macro-expansion.md`
- Examples: `photon/examples/` via `[[example]]` (`cargo run -p uf-photon --example …`)
- Trait `# Contract` sections on [`StoragePort`](photon-backend/src/storage/port.rs) and [`PhotonBackend`](photon-backend/src/backend/photon_backend.rs)
- Adapter builder options live on each `*StoragePortBuilder` rustdoc; `photon::config` indexes and links there (no duplicated env tables)
