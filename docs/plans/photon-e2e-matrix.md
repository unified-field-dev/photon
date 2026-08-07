# Photon E2E Matrix Plan

Embedded SQLite storage adapter, full e2e scenario coverage, topology/telemetry in CI, and unified AWS validation.

## Hard constraint

**The dev laptop must never run `cargo build` / `cargo test`.** All validation runs on AWS EC2 (operator campaign).

## Phases

1. **`photon-backend-sqlite`** — write-through SQLite + in-memory broadcast fanout
2. **Matrix wiring** — `StorageAdapter::Sqlite` through testkit, photon-e2e, photon-bench
3. **E2e scenarios** — 13 sqlite scenarios + topology/telemetry smokes promoted from `#[ignore]`
4. **AWS SQLite smoke** — t3.medium provision/bootstrap/remote smoke
5. **AWS orchestration** — sqlite + kafka + fluvio + nats gates on EC2 (operator campaign)
6. **Docs** — STORAGE-ADAPTERS-DESIGN, configuration, ROADMAP, photon-e2e README

## AWS validation

Provision and bootstrap an EC2 SQLite smoke host, rsync this repo, run remote smoke, then tear down. For all backends, run the operator all-gates campaign (sqlite auto-provisions; broker fleets need live `instances.env`). Do not run `cargo build` / `cargo test` on the constrained laptop.

## Out of scope (v1)

Multi-process SQLite WAL, Postgres/Surreal, `shard_strategy` matrix wiring.
