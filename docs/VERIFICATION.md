# Documentation verification

Re-run after doc changes. See [CONTRIBUTING.md](CONTRIBUTING.md#documentation).

**Local laptop `cargo` is not the verification path** when the toolchain is broken or incomplete. Prefer the AWS SQLite smoke host (see [`infra/aws/sqlite-smoke/README.md`](../infra/aws/sqlite-smoke/README.md)):

```bash
# After provision.sh + bootstrap.sh (scripts live under ~/aws/photon-upstream/):
export INSTANCES_ENV=~/aws/photon-upstream/sqlite-smoke/instances.env
~/aws/photon-upstream/sqlite-smoke/run-remote-check.sh
```

That rsyncs the repo to EC2 and runs `cargo check`, full-workspace Clippy (`--all-targets --all-features -- -D warnings`), rustdoc (`RUSTDOCFLAGS=-D warnings`), and `photon-backend` tests. Broader CI subset (deny, e2e mem/sqlite, bench, examples, doctests): `~/aws/photon-upstream/sqlite-smoke/run-remote-ci.sh`. Full E2E smoke: `~/aws/photon-upstream/sqlite-smoke/run-remote-smoke.sh`.

## Commands

Run these on the AWS smoke host (or CI), not as a laptop-only gate:

```bash
# Workspace missing_docs deny
cargo check --workspace --features runtime,mem

# Workspace API docs (fail on rustdoc warnings)
RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps --all-features

# Facade-only docs (quick check)
RUSTDOCFLAGS="-D warnings" cargo doc -p uf-photon --features runtime,mem --no-deps

# Doctests (rust,no_run compile-checked)
cargo test -p uf-photon --doc --features runtime,mem
cargo test -p photon-runtime --doc --features runtime,mem
cargo test -p photon-macros --doc
cargo test -p photon-backend --doc --features runtime

# Examples on the facade (need PHOTON_TRANSPORT_KEY — smoke scripts export it)
cargo run -p uf-photon --example embedded_mem --features runtime,mem
cargo run -p uf-photon --example consumer_group --features runtime,mem
cargo run -p uf-photon --example manual_subscribe --features runtime,mem
cargo run -p uf-photon --example keyed_topic --features runtime,mem
cargo run -p uf-photon --example telemetry_ops_log --features runtime,mem

# Integration
cargo test -p photon-e2e
cargo test -p photon-backend --features runtime --tests
```

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
- Lane map on facade: **Creating topics** / **Integrating the host** / **Developing the backend**
- Config reference: `photon::config` includes `docs/configuration.md`
- Macro expansion: `docs/macro-expansion.md`
- Examples: `photon/examples/` via `[[example]]` (`cargo run -p uf-photon --example …`)
- Trait `# Contract` sections on [`StoragePort`](photon-backend/src/storage/port.rs) and [`PhotonBackend`](photon-backend/src/backend/photon_backend.rs)
- Adapter builder options live on each `*StoragePortBuilder` rustdoc; `photon::config` indexes and links there (no duplicated env tables)
