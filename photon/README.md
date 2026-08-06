# Photon

[![CI](https://github.com/unified-field-dev/photon/actions/workflows/ci.yml/badge.svg)](https://github.com/unified-field-dev/photon/actions/workflows/ci.yml)
[![Crates.io](https://img.shields.io/crates/v/uf-photon.svg)](https://crates.io/crates/uf-photon)
[![docs.rs](https://docs.rs/uf-photon/badge.svg)](https://docs.rs/uf-photon)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](../LICENSE)

Feature flags, wiring, and verify commands for adding Photon to a Rust service. Project overview: [README](../README.md).

## Install

crates.io package **`uf-photon`** (Rust crate name remains `photon`):

```toml
photon = { package = "uf-photon", version = "0.1.1", features = ["runtime", "mem"] }
```

## Features

| Feature | Purpose |
|---------|---------|
| `runtime` | Full stack — backends, `Photon`, executor. |
| `mem` | Default in-process storage (`InProcStoragePort`) for tests and dev |
| `sqlite` | Embedded SQLite storage ([`photon-backend-sqlite`](../photon-backend-sqlite/)) |
| `nats` | NATS JetStream storage adapter ([`photon-backend-nats`](../photon-backend-nats/)) |
| `fluvio` | Fluvio storage adapter ([`photon-backend-fluvio`](../photon-backend-fluvio/)) |
| `kafka` | Kafka storage adapter ([`photon-backend-kafka`](../photon-backend-kafka/)) |

Configuration reference: [docs.rs `photon::config`](https://docs.rs/uf-photon/latest/photon/config/). Primary tutorial: [Getting started](https://docs.rs/uf-photon/latest/photon/#getting-started) (Embedded vs Brokered publisher/worker).

Ships with **no default features** (`default = []`). Enable `runtime` + `mem` for the standard evaluation path.

## How to run examples

Navigational index: [`examples/README.md`](examples/README.md) (when-to-use ladder + links to each `.rs` file).

Canonical teaching path (start here). Topology docs:
[Embedded](https://docs.rs/uf-photon/latest/photon/#embedded-one-binary) /
[Brokered](https://docs.rs/uf-photon/latest/photon/#brokered-publisher--worker-binaries).

All examples need `PHOTON_TRANSPORT_KEY` (base64 of 32 bytes). Dev/smoke key:

```bash
export PHOTON_TRANSPORT_KEY=cGhvdG9uLWRldi10cmFuc3BvcnQta2V5LTMyYnl0ZXM=
```

### 1. Embedded — `embedded_mem` (standalone)

One process, in-memory storage. No external services.

```bash
cargo run -p uf-photon --example embedded_mem --features runtime,mem
```

Success: stderr/tracing shows a published event id and a handler log line.

### 2. Embedded durable — `embedded_sqlite` (standalone)

Same API as (1), file-backed SQLite (write-through + in-memory live fanout).

```bash
# optional: PHOTON_SQLITE_PATH=/tmp/photon-example.db
cargo run -p uf-photon --example embedded_sqlite --features runtime,sqlite
```

### 3. Brokered — NATS publisher + worker (multi-process — run as a set)

Publisher and worker share one JetStream cluster. They are **not** useful alone. Teach the broker axis once with NATS; swap the builder for Kafka/Fluvio in production (see adapter rustdoc).

| Rule | Detail |
|------|--------|
| Shared env | Same `PHOTON_TRANSPORT_KEY`, `PHOTON_NATS_URL`, `PHOTON_NATS_STREAM` on every process |
| Start order | Broker → **worker(s)** first → publisher |
| Local plaintext | `PHOTON_ALLOW_INSECURE_BROKER=1` (dev/CI only; never in production) |
| Workers | Each process embeds Photon; call `start_executor` on workers only |
| Stop | Ctrl-C on the worker |

**Local NATS** (single node):

```bash
docker run -d --name photon-nats -p 4222:4222 nats:2.10 -js

export PHOTON_TRANSPORT_KEY=cGhvdG9uLWRldi10cmFuc3BvcnQta2V5LTMyYnl0ZXM=
export PHOTON_NATS_URL=nats://127.0.0.1:4222
export PHOTON_NATS_STREAM=photon
export PHOTON_ALLOW_INSECURE_BROKER=1

# Terminal 1 — worker (leave running)
cargo run -p uf-photon --example nats_worker --features runtime,nats

# Terminal 2 — publisher (exits after one event)
cargo run -p uf-photon --example nats_publisher --features runtime,nats
```

Optional: start a second `nats_worker` in another terminal (same env) before publishing.

**Production:** use TLS (`tls://…`) + `.credentials_file` / `PHOTON_NATS_CREDS`; never set `PHOTON_ALLOW_INSECURE_BROKER` or `PHOTON_ALLOW_DEV_TRANSPORT_KEY`. Cluster labs: [`infra/broker/README.md`](../infra/broker/README.md).

### 4. Secure brokered — NATS TLS + credentials (`nats_secure_worker` / `nats_secure_publisher`)

Same pair topology as (3), but wired the **production** way: `.require_tls()` + `.credentials_file` / `PHOTON_NATS_CREDS` — never `PHOTON_ALLOW_INSECURE_BROKER`. Requires a real TLS-terminated NATS endpoint (e.g. via a sidecar proxy or managed JetStream); without one, both binaries print the runbook and exit cleanly instead of falling back to plaintext.

```bash
export PHOTON_TRANSPORT_KEY=cGhvdG9uLWRldi10cmFuc3BvcnQta2V5LTMyYnl0ZXM=
export PHOTON_NATS_URL=tls://nats.example.internal:4222
export PHOTON_NATS_STREAM=photon
export PHOTON_NATS_CREDS=/run/secrets/photon-nats.creds

cargo run -p uf-photon --example nats_secure_worker --features runtime,nats
# cargo run -p uf-photon --example nats_secure_publisher --features runtime,nats
```

Success (with a TLS broker): worker `worker received greeting`; publisher `published over TLS`. Without one: both print a `PHOTON_NATS_URL must be tls://…` warning and return `Ok`.

### 5. Brokered — Kafka publisher + worker (`kafka_worker` / `kafka_publisher`)

Same publisher/worker contract as (3), backed by `KafkaStoragePortBuilder`. Local single-node lab: `infra/broker/scripts/kafka-single.sh` (KRaft, `127.0.0.1:9092`).

```bash
cd infra/broker && ./scripts/kafka-single.sh && source scripts/export-kafka-env.sh && cd -

export PHOTON_TRANSPORT_KEY=cGhvdG9uLWRldi10cmFuc3BvcnQta2V5LTMyYnl0ZXM=
export PHOTON_ALLOW_INSECURE_BROKER=1

cargo run -p uf-photon --example kafka_worker --features runtime,kafka
# cargo run -p uf-photon --example kafka_publisher --features runtime,kafka
```

Success: worker `worker received greeting`; publisher `kafka_publisher: published`.

### 6. Brokered — Fluvio publisher + worker (`fluvio_worker` / `fluvio_publisher`)

Same publisher/worker contract as (3), backed by `FluvioStoragePortBuilder`. Local single-node lab: `infra/broker/scripts/fluvio-single.sh` (SC + SPU on `127.0.0.1:9103`).

```bash
cd infra/broker && ./scripts/fluvio-single.sh && source scripts/export-fluvio-env.sh && cd -

export PHOTON_TRANSPORT_KEY=cGhvdG9uLWRldi10cmFuc3BvcnQta2V5LTMyYnl0ZXM=
export PHOTON_ALLOW_INSECURE_BROKER=1

cargo run -p uf-photon --example fluvio_worker --features runtime,fluvio
# cargo run -p uf-photon --example fluvio_publisher --features runtime,fluvio
```

Success: worker `worker received greeting`; publisher `fluvio_publisher: published`.

### 7. Durable consumer recovery — `durable_consumer_recovery` (standalone)

Single process, two phases: handles a batch with a `durable = "…"` subscription, force-flushes the checkpoint, "crashes" (drops the `Photon` handle), then opens a **fresh** `SqliteStoragePort` on the same file and resumes — proving the checkpoint-driven restart contract that `nats_worker` / `kafka_worker` / `fluvio_worker` rely on against their own brokers.

```bash
cargo run -p uf-photon --example durable_consumer_recovery --features runtime,sqlite
```

Success: `phase 1: checkpoint committed … simulating a process crash` followed by `phase 2: resumed from checkpoint with no redelivery`.

### Other examples

| Example | Topology | Features | Notes |
|---------|----------|----------|-------|
| `subscribe_v2` | Embedded | `runtime,mem` | `Arc<dyn Actor>` + `HandlerCtx` + `configure` |
| `keyed_topic` | Embedded | `runtime,mem` | `keyed_by` + typed `subscribe_on` filter |
| `manual_subscribe` | Embedded | `runtime,mem` | Raw topic-name subscribe stream |
| `consumer_group` | Embedded | `runtime,mem` | Group delivery / shards (single member) |
| `telemetry_ops_log` | Embedded | `runtime,mem` | `PhotonBuilder::ops_log` |

## Wiring

Follow the rustdoc [Getting started](https://docs.rs/uf-photon/latest/photon/#getting-started) for topology choice. Checklist:

1. Build with `Photon::builder()` — default installs `InProcStoragePort` (`mem`). Requires `PHOTON_TRANSPORT_KEY` (`TransportCrypto::from_env`).
2. Optionally pass `.storage_port(Arc<dyn StoragePort>)` for `sqlite` or broker adapters (Brokered: same port config on every binary).
3. Call `.auto_registry()` when using `#[photon::topic]` / `#[photon::subscribe]`.
4. Keep the [`Photon`](https://docs.rs/uf-photon/latest/photon/struct.Photon.html) handle and call `publish_on(&photon)` / `subscribe_on(&photon, opts)` (preferred).
5. Optional: `photon::configure(photon)` for process-wide `.publish()` / `.subscribe()` sugar.
6. Call `photon.start_executor(identity)` on Embedded hosts and Brokered **workers** (publisher-only binaries can skip).

### Default bootstrap (mem)

```rust
use photon::Photon;

// Loads PHOTON_TRANSPORT_KEY via from_env().
let photon = Photon::builder().auto_registry().build()?;
// EventType { ... }.publish_on(&photon).await?;
// Optional: configure(photon) for .publish() without a handle.
```

### Custom storage port

```rust
use std::sync::Arc;

use photon::Photon;
use photon_backend::storage::InProcStoragePort;
use photon_backend::event::TransportCrypto;

let port = Arc::new(InProcStoragePort::new(TransportCrypto::from_env()?));
let photon = Photon::builder()
    .storage_port(port)
    .auto_registry()
    .build()?;
```

Broker env vars and builder options: each adapter's `*StoragePortBuilder` rustdoc (linked from [`photon::config`](https://docs.rs/uf-photon/latest/photon/config/#storage-adapter-builders)).

### SQLite — durable single-process

Write-through persistence with in-memory live fanout (no external broker):

```rust
use std::sync::Arc;

use photon::{Photon, SqliteStoragePort};

let port = Arc::new(SqliteStoragePort::open("/var/lib/photon/events.db").await?);
// Or: SqliteStoragePort::from_env().await?  // reads PHOTON_SQLITE_PATH
let photon = Photon::builder()
    .storage_port(port)
    .auto_registry()
    .build()?;
```

See [`photon-backend-sqlite`](../photon-backend-sqlite/).

### NATS JetStream — production (durable)

Durable subscriptions, checkpoint replay, and stream-sharded fleet ingress:

```rust
use std::sync::Arc;

use photon::{Photon, NatsStoragePort, ReplayCursor};

let port = Arc::new(
    NatsStoragePort::builder()
        .from_env_defaults()
        .replay_cursor(ReplayCursor::StreamSeq)
        .sync_ack(true)
        .stream_shards(4) // match broker count; rep=1 per shard when K>1
        .build()
        .await?,
);
let photon = Photon::builder()
    .storage_port(port)
    .auto_registry()
    .build()?;
```

### NATS JetStream — high ingress (ephemeral)

Maximum publish throughput when durable replay and checkpoints are not required:

```rust
use std::sync::Arc;

use photon::{Photon, NatsStoragePort, ReplayCursor};

let port = Arc::new(
    NatsStoragePort::builder()
        .from_env_defaults()
        .replay_cursor(ReplayCursor::TailOnly)
        .sync_ack(false)
        .max_inflight(256)
        .stream_shards(4)
        .build()
        .await?,
);
let photon = Photon::builder()
    .storage_port(port)
    .auto_registry()
    .build()?;
```

Builder fields and env fallbacks: [`NatsStoragePortBuilder` rustdoc](https://docs.rs/photon-backend-nats/latest/photon_backend_nats/struct.NatsStoragePortBuilder.html).

## Verify

Prefer AWS when local cargo is unavailable: `$UF_LAB_ROOT/photon/scripts/run-remote-check.sh`.

```bash
export PHOTON_TRANSPORT_KEY=cGhvdG9uLWRldi10cmFuc3BvcnQta2V5LTMyYnl0ZXM=
cargo check -p uf-photon --features runtime,mem
cargo run -p uf-photon --example embedded_mem --features runtime,mem
cargo run -p uf-photon --example embedded_sqlite --features runtime,sqlite
cargo test -p uf-photon --doc --features runtime,mem
```

Full matrix: [root README § Verify](../README.md#verify). Macro expansion: [`docs/macro-expansion.md`](../docs/macro-expansion.md).
