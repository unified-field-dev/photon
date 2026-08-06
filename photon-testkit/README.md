# photon-testkit

Shared matrix, scenario, and bootstrap helpers for [`photon-e2e`](../photon-e2e/README.md) and [`photon-bench`](../photon-bench/README.md).

## Purpose

Do not duplicate storage adapter install or synthetic topic setup in e2e and bench. This crate owns:

| Module | Responsibility |
|--------|----------------|
| `matrix::MatrixSpec` | `storage`, `topology`, `telemetry`, `shard_strategy` (reserved) |
| `matrix::presets` | CI helpers (`ci_mem_embedded`, `ci_nats_broker`, …) and `with_topology` / `with_telemetry` |
| `scenario::ScenarioSpec` | Declarative steps: publish, subscribe, restart, assert delivery, paced load, cross-node fanout |
| `runner::ScenarioRunner` | Execute scenarios in correctness or benchmark mode |
| `bootstrap::BootstrapSession` | Install storage adapter; build `Photon` for matrix row |
| `bootstrap::topology` | `embedded-composite` → `photon_runtime::configure`; `split-runtime` → headless |
| `bootstrap::telemetry` | `OpsLog` adapters: `NoOpsLog`, `ConsoleOpsLog`, `RecordingOpsLog`, `PersistingOpsLog` |
| `backends::registry` | Sync/async `StoragePort` install per adapter |
| `shared_store` | Broker env detection and fleet skip reasons |
| `cross_node` | Two-Photon cross-node fanout harness (BM-P6 / BM-PF0) |
| `bench_handlers` | Inventory-registered handler for BM-PFE / executor scenarios |
| `fixtures` | Synthetic topic names and JSON payloads |

## Matrix helpers

| Helper | Use |
|--------|-----|
| `MatrixSpec::ci_mem_embedded()` | Default PR CI slice (`mem` + `isolated-lab`) |
| `MatrixSpec::ci_sqlite_embedded()` | Embedded SQLite durable slice |
| `MatrixSpec::ci_nats_broker()` | NATS broker matrix (`nats` + `broker-cluster`) |
| `MatrixSpec::with_topology(...)` | Override topology on any preset |
| `MatrixSpec::with_telemetry(...)` | Override telemetry on any preset |

## Topology (lab harness)

| Topology | Bootstrap behavior |
|----------|-------------------|
| `isolated-lab` | Default — minimal Photon build, no process-wide `configure()` |
| `embedded-composite` | Calls `photon_runtime::configure()` after build |
| `split-runtime` | Headless worker — no process default; sets `PHOTON_TOPOLOGY=split-runtime` |
| `broker-cluster` | Used with `nats` / `fluvio` / `kafka` storage adapters |

## Storage adapters

| Adapter | Install |
|---------|---------|
| `mem` | Sync — `InProcStoragePort` |
| `sqlite` | Async — `PHOTON_SQLITE_PATH` or temp file |
| `nats` | Async — `PHOTON_NATS_URL` + JetStream |
| `fluvio` | Async — `PHOTON_FLUVIO_ENDPOINT` |
| `kafka` | Async — `PHOTON_KAFKA_BROKERS` |

`shard_strategy` (`none`, `by-topic-key`) is reserved — not wired in bootstrap.

## Shared store / fleet tiers

| Tier | Available when | Used by |
|------|----------------|---------|
| Lab | `mem`, `sqlite` | BM-P0–P9, PG*, correctness e2e |
| Fleet | `nats` / `fluvio` / `kafka` + broker env | BM-PF*, P6, PFS, PFE, PB* |

Functions: `fleet_store_available`, `fleet_store_skip_reason`.

## CI default

PR CI runs the full matrix (`mem`, `sqlite`, `nats`, `kafka`, `fluvio`) — see [`.github/workflows/ci.yml`](../.github/workflows/ci.yml) `e2e` job.

**Dev laptop constraint:** run builds/tests on AWS via `$UF_LAB_ROOT/photon/scripts/run-all-e2e-aws.sh`.

## Build

```bash
cargo test -p photon-testkit
cargo test -p photon-testkit --features nats   # requires PHOTON_NATS_URL for broker tests
```

## Status

`ScenarioRunner` + scenario catalog; `BootstrapSession` for `mem` (CI) and broker adapters via `install_async`.
