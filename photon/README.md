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

Configuration reference: [docs.rs `photon::config`](https://docs.rs/uf-photon/latest/photon/config/). Primary tutorial: [Getting started](https://docs.rs/uf-photon/latest/photon/#getting-started) (Mode 1 embedded vs Mode 2 brokered publisher/worker).

Ships with **no default features** (`default = []`). Enable `runtime` + `mem` for the standard evaluation path.

## Wiring

Follow the rustdoc [Getting started](https://docs.rs/uf-photon/latest/photon/#getting-started) for topology choice. Checklist:

1. Build with `Photon::builder()` — default installs `InProcStoragePort` (`mem`). Requires `PHOTON_TRANSPORT_KEY` (`TransportCrypto::from_env`).
2. Optionally pass `.storage_port(Arc<dyn StoragePort>)` for `sqlite` or broker adapters (Mode 2: same port config on every binary).
3. Call `.auto_registry()` when using `#[photon::topic]` / `#[photon::subscribe]`.
4. Keep the [`Photon`](https://docs.rs/uf-photon/latest/photon/struct.Photon.html) handle and call `publish_on(&photon)` / `subscribe_on(&photon, opts)` (preferred).
5. Optional: `photon::configure(photon)` for process-wide `.publish()` / `.subscribe()` sugar.
6. Call `photon.start_executor(identity)` on Mode 1 hosts and Mode 2 **workers** (publisher-only binaries can skip).

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

Prefer AWS when local cargo is unavailable: `../infra/aws/sqlite-smok~/aws/photon-upstream/sqlite-smoke/run-remote-check.sh`.

```bash
export PHOTON_TRANSPORT_KEY=cGhvdG9uLWRldi10cmFuc3BvcnQta2V5LTMyYnl0ZXM=
cargo check -p uf-photon --features runtime,mem
cargo run -p uf-photon --example embedded_mem --features runtime,mem
cargo test -p uf-photon --doc --features runtime,mem
```

Full matrix: [root README § Verify](../README.md#verify). Macro expansion: [`docs/macro-expansion.md`](../docs/macro-expansion.md).
