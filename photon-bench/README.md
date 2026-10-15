# photon-bench

Synthetic benchmarks and pre-registered experiment matrix for Photon pub/sub across storage adapters (`mem`, `sqlite`, `nats`, `fluvio`, `kafka`). Adapter contract: [docs.rs `photon` architecture](https://docs.rs/uf-photon/latest/photon/#architecture).

| Document | Role |
|----------|------|
| [`PERFORMANCE.md`](PERFORMANCE.md) | Capacity numbers and how to read them |
| `cargo run -p photon-bench -- experiments` | Registry: experiment IDs, status, summary (source: `src/experiments/registry.rs`) |

## Workspace cargo profiles

Root `Cargo.toml` defines:

| Profile | Use |
|---------|-----|
| `bench` | Inherits `release` — default for `cargo bench` |
| `profiling` | Inherits `release` with `debug = 1` — release-speed binaries with line tables for `perf` / flamegraphs |

```bash
cargo build -p photon-bench --profile profiling
cargo bench -p photon-backend --features runtime
```

Criterion microbenches (`BM-CRIT-*`) live in `photon-backend/benches/`.

## Upstream repos

| Crate | Repo |
|-------|------|
| Quark (registry) | [unified-field-dev/quark](https://github.com/unified-field-dev/quark) |
| Photon (pub/sub) | [unified-field-dev/photon](https://github.com/unified-field-dev/photon) |

**Reports:** runs write `profiling/photon-bench/reports/{experiment}-{storage}-{topology}-{telemetry}-{hardware}.json` as local scratch (gitignored). Decision-grade AWS results aren't committed to this repo.

**Status:** `run` CLI drives [`ScenarioRunner`](../photon-testkit/src/runner.rs). Adapter-tier experiments (`bm-p0`–`bm-pl3`, `bm-pg*`, `bm-pd*`) run on `--storage mem` (or `sqlite` for durable embedded). NATS delivery / fanout fleet experiments (`bm-pf*`, `bm-p6`, `bm-pfs`, `bm-pfe`, `bm-pb*`) require `--storage nats` and broker cluster env. **BM-PFH** runs on `--storage nats|kafka|fluvio` via the matching AWS fleet (`broker-fleet`, `kafka-fleet`, `fluvio-fleet`); skip with `skipped_broker_pending` when unavailable. **BM-PD0/PD1** (`production-delivery`) keep their locked 1k/s and 500/s offered rates. **BM-PD2/PD3** (`production-capacity`) sweep offered rate until the first failing cell and report `highest_passing_offered_rate`. Decision-grade scaling curves use `--primary-row`.

## Run

```bash
export CARGO_TARGET_DIR=target-photon-bench

# List experiment IDs with status
cargo run -p photon-bench -- experiments

# BM-P1 — same scenario as photon-e2e smoke
cargo run -p photon-bench -- run --experiment bm-p1 --storage mem --telemetry off

# BM-P0 — publish-only latency
cargo run -p photon-bench -- run --experiment bm-p0 --ops 5000 --storage mem --hardware dev-wsl

# BM-PD1 — encrypted checkpoint fanout (crypto on; NATS cluster)
cargo run -p photon-bench --features nats -- run --experiment bm-pd1 --storage nats --topology broker-cluster

# BM-PD2 — capacity sweep, one durable subscriber (omit --offered-rate to sweep)
cargo run -p photon-bench --features sqlite -- run --experiment bm-pd2 --storage sqlite --ops 1

# BM-PD3 — capacity sweep, four-subscriber fanout
PHOTON_BENCH_OFFERED_RATE=50 cargo run -p photon-bench -- run --experiment bm-pd3 --storage mem --ops 1

# Write JSON report (local smoke — gitignored reports-local/)
cargo run -p photon-bench -- run --experiment bm-p0 --ops 5000 \
  --report profiling/photon-bench/reports-local/bm-p0-mem-isolated-lab-off-dev-wsl.json
```

Valid `--storage` values: `mem`, `sqlite`, `nats`, `fluvio`, `kafka` (`embedded` aliases `mem`).
