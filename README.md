[![CI](https://github.com/unified-field-dev/photon/actions/workflows/ci.yml/badge.svg)](https://github.com/unified-field-dev/photon/actions/workflows/ci.yml)
[![Crates.io](https://img.shields.io/crates/v/uf-photon.svg)](https://crates.io/crates/uf-photon)
[![docs.rs](https://docs.rs/uf-photon/badge.svg)](https://docs.rs/uf-photon)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

[GitHub](https://github.com/unified-field-dev/photon) · [crates.io](https://crates.io/crates/uf-photon) · `cargo doc -p uf-photon --features runtime,mem --open` · [Benchmarks](photon-bench/README.md)

# Photon

The crates.io package is **`uf-photon`** (`photon` is taken). With `[lib] name = "photon"`, imports stay `use photon::…`:

```toml
photon = { package = "uf-photon", version = "0.1.1", features = ["runtime", "mem"] }
# or: cargo add uf-photon --features runtime,mem
```

```rust
use std::sync::Arc;

use photon::{subscribe, topic, JsonIdentityFactory, Photon};
use photon_core::Actor;

// Typed topic — inventory registers descriptor for discovery.
#[topic(name = "orders.created", keyed_by = "order_id")]
pub struct OrderCreated {
    pub order_id: String,
    pub amount_cents: u64,
}

// Worker — macro submits handler descriptor; executor dispatches on publish.
#[subscribe(topic = "orders.created", durable = "billing")]
async fn on_order_created(actor: Box<dyn Actor>, event: OrderCreated) -> photon::Result<()> {
    tracing::info!(actor = actor.label(), order = %event.order_id, cents = event.amount_cents);
    Ok(())
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Requires PHOTON_TRANSPORT_KEY (base64 of 32 bytes). See docs/configuration.md.
    // Default: in-process mem storage (InProcStoragePort).
    let photon = Photon::builder().auto_registry().build()?;
    photon.start_executor(Arc::new(JsonIdentityFactory))?;

    OrderCreated {
        order_id: "ord-1".into(),
        amount_cents: 9900,
    }
    .publish_on(&photon)
    .await?;
    Ok(())
}
```

## About Photon

Photon is a Rust pub/sub runtime with typed topics and durable subscriptions. Storage is pluggable via [`StoragePort`](photon-backend/src/storage/port.rs). Built-in adapters: `mem`, `sqlite`, `nats`, `fluvio`, `kafka`, wired through [`GenericPhotonBackend`](photon-backend/src/backend/generic.rs). Primary guide: [Getting started](https://docs.rs/uf-photon/latest/photon/#getting-started) (embedded vs brokered topologies).

## Performance

Publish throughput depends on your storage adapter and how many Photon embed hosts you run. The table below is a **2026-07-07** set of **representative fleet ingress snapshots** (AWS in-VPC, crypto off, zero subscribers) — useful for sizing Photon embeds on each adapter path, **not** a fair adapter leaderboard or vendor bake-off. Full methodology and threats to validity: [`PERFORMANCE_STUDY.md`](photon-bench/PERFORMANCE_STUDY.md).

| Adapter | One embed host | Four embed hosts |
|---------|----------------|------------------|
| NATS | ~94k ops/s | ~218k ops/s |
| Fluvio | ~93k ops/s | ~400k ops/s |
| Kafka | ~1–3k ops/s | ~5k ops/s |

## The model

Photon is typed pub/sub over a persistent event log. You publish events to topics; subscriptions consume them. Durable subscriptions pick up where they left off after a restart. A storage adapter handles persistence and cross-process delivery.

The glossary below names each concept and points to where it shows up in the API.

- **Topic** — named stream of events (e.g. `orders.created`). Registered via `#[photon::topic]` and Quark inventory.
- **Event** — one published record: payload, sequence, captured actor JSON, optional partition key.
- **Subscription** — consumer on a topic; **ephemeral** (live stream) or **durable** (checkpointed replay after restart).
- **Checkpoints** — durable subscriptions persist last delivered sequence per partition; coalesced flushes via [`StoragePort`](photon-backend/src/storage/port.rs).
- **Keyed topics** — `keyed_by` on the macro; publish supplies partition key; subscribe can filter by key.
- **Storage adapters:** pluggable [`StoragePort`](photon-backend/src/storage/port.rs); built-in `mem`, `sqlite`, `nats`, `fluvio`, `kafka`. Install via [`PhotonBuilder::storage_port`](photon-runtime/src/builder.rs) or default `mem`.

## Compared to other approaches

### Kafka / RabbitMQ / NATS / Fluvio

Photon embeds in your Rust service with a typed API. Choose **`mem`** for zero deps, **`sqlite`** for durable single-process persistence, or a **broker adapter** when you need horizontal scale — you run the broker cluster; Photon handles encryption, subscriptions, and checkpoints above the port.

### In-process channels (`tokio::sync`, `broadcast`)

Same-process ergonomics plus optional durable log, replay, and multi-node delivery without changing the typed API.

### Hand-rolled HTTP/event APIs

`#[photon::topic]` + shared `PhotonBackend` trait instead of ad-hoc JSON endpoints per service.

## Getting started

```toml
[dependencies]
photon = { version = "0.1", features = ["runtime", "mem"] }
tokio = { version = "1", features = ["rt-multi-thread", "macros"] }
tracing = "0.1"
anyhow = "1"
```

Enable features explicitly — the facade ships with **no default features** (`default = []`).

**Default wiring:** `Photon::builder()` installs [`InProcStoragePort`](photon-backend/src/storage/in_proc.rs) (`mem` tier) automatically.

```rust
use photon::Photon;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // PhotonBuilder loads PHOTON_TRANSPORT_KEY via TransportCrypto::from_env().
    let photon = Photon::builder().auto_registry().build()?;
    // Prefer publish_on(&photon). Optional: configure(photon) for .publish() sugar.
    let _ = photon;
    Ok(())
}
```

**Durable embedded (`sqlite`) or broker tier:** build a storage port and pass it to the builder (examples in [`photon/README.md`](photon/README.md)):

```rust
Photon::builder()
    .storage_port(/* Arc<dyn StoragePort> from photon-backend-sqlite / -nats / etc. */)
    .auto_registry()
    .build()?;
```

See [Getting started](https://docs.rs/uf-photon/latest/photon/#getting-started) (Mode 1 embedded vs Mode 2 brokered) and [`photon::config`](https://docs.rs/uf-photon/latest/photon/config/).

## Learn more

| Link | Why |
|------|-----|
| [Getting started](https://docs.rs/uf-photon/latest/photon/#getting-started) | Topology choice + wiring (primary) |
| [docs.rs `photon::config`](https://docs.rs/uf-photon/latest/photon/config/) | Env vars, macro attrs, builder options |
| [docs/macro-expansion.md](docs/macro-expansion.md) | What `#[topic]` / `#[subscribe]` generate |
| `cargo doc -p uf-photon --features runtime,mem --open` | Full API reference |
| [photon/README.md](photon/README.md) | Features, wiring checklist |
| [photon-bench/PERFORMANCE_STUDY.md](photon-bench/PERFORMANCE_STUDY.md) | Performance methodology |
| [photon-bench/EXPERIMENTS.md](photon-bench/EXPERIMENTS.md) | Benchmark registry |
| [uf-quark](https://crates.io/crates/uf-quark) / [Quark](https://github.com/unified-field-dev/quark) | Topic/handler inventory |
| [.github/workflows/ci.yml](.github/workflows/ci.yml) | CI verify commands |

## FAQ

**Is it production-ready?** v0.1.1 early release; `mem` and `sqlite` tiers are tested in CI; NATS / Fluvio / Kafka adapters have PFH fleet ingress numbers — see [PERFORMANCE_STUDY.md](photon-bench/PERFORMANCE_STUDY.md) and [EXPERIMENTS.md](photon-bench/EXPERIMENTS.md).

**Do I need an external broker?** No for single-process — default `mem` is in-memory; `sqlite` adds durable embedded storage. Use `nats`, `fluvio`, or `kafka` for fleet scale.

**How do I pick a storage adapter?** `mem` for local/dev via default `PhotonBuilder`; `sqlite` for durable single-process (`PHOTON_SQLITE_PATH`); broker tier for multi-node via `PhotonBuilder::storage_port` and adapter env vars (`PHOTON_NATS_URL`, etc.).

**Is the transport log my database?** No — encrypted, transient event transport; canonical data stays in your datastore.

**Why not use Kafka?** Embeddable library, storage portability, no separate broker ops — see **Compared to** above.

**Do I need `#[photon::subscribe]`?** Yes for macro-driven workers — registers handlers and drops stream/executor boilerplate; call `start_executor` at boot. Use typed `.subscribe()` streams only for manual stream control (rustdoc).

**How do keyed topics work?** `keyed_by` on `#[photon::topic]`; partition key on publish; optional filter on subscribe.

## Verify

CI runs on every push and PR ([`.github/workflows/ci.yml`](.github/workflows/ci.yml)). When local `cargo` is unavailable, use AWS:

```bash
./infra/aws/sqlite-smok~/aws/photon-upstream/sqlite-smoke/run-remote-check.sh
```

Doc verification baseline: [docs/VERIFICATION.md](docs/VERIFICATION.md).

Full matrix commands and WSL `CARGO_TARGET_DIR` notes: [photon/README.md](photon/README.md#verify).
