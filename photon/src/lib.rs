//! Pub/sub event pipeline (public crate).
//!
//! Typed topics, durable subscriptions with checkpoints, and the same API in single-process and
//! multi-node deployments. Enable the `runtime` feature for the full stack (`Photon`, backends,
//! executor). Photon uses pluggable **storage adapters**
//! (`mem`, `sqlite`, `nats`, `fluvio`, `kafka`) behind [`StoragePort`] and Quark inventory for
//! topic/handler discovery. Payloads are encrypted before append; storage stays opaque. Canonical
//! business data remains in your datastore — the transport log is for event delivery, not system
//! of record.
//!
//! ## Features
//!
//! - **Typed topics and handlers** — [`topic`] / [`subscribe`] register via Quark inventory
//! - **Same API everywhere** — `publish_on` / `start_executor` whether you run one process or many
//! - **Pluggable storage** — `mem`, `sqlite`, or broker adapters (`nats`, `kafka`, `fluvio`)
//! - **Durable subscriptions** — checkpointed replay after restart on durable adapters
//! - **Host-owned crypto** — `PHOTON_TRANSPORT_KEY` seals envelopes, keeping ciphertext at rest
//!
//! *Typed pub/sub over a persistent event log — pick embedded or brokered topology.*
//!
//! # Getting started
//!
//! You always publish and subscribe the same way (`OrderCreated { … }.publish_on(&photon)`,
//! `#[subscribe]`, [`Photon::start_executor`]). What changes is **whether events stay in one
//! process or cross a broker to another binary**.
//!
//! ## Choose a topology
//!
//! - **[Embedded](#embedded-one-binary)** — one process owns publish and handlers. Start here
//!   (`mem` or `sqlite`).
//! - **[Brokered](#brokered-publisher--worker-binaries)** — publisher binary(ies) and worker
//!   binary(ies) share a broker. Photon does **not** ship a separate server binary; each process
//!   embeds Photon against the same adapter.
//!
//! | Topology | Adapter | Features | When to use |
//! |----------|---------|----------|-------------|
//! | Embedded | [`InProcStoragePort`] (default) | `runtime`,`mem` | Local / tests |
//! | Embedded durable | `SqliteStoragePort` | `runtime`,`sqlite` | Single host, restart-safe |
//! | Brokered | NATS / Kafka / Fluvio | `runtime` + `nats`/`kafka`/`fluvio` | Multi-process / fleet |
//!
//! Adapter builders and env vars: [`config`].
//!
//! After you pick a topology, continue with [declare topics](#3-declare-topics-and-handlers).
//!
//! ## Embedded (one binary)
//!
//! This process publishes **and** runs handlers. There is no second binary and no external broker.
//!
//! ```text
//! Your app ──publish / #[subscribe]──► Photon ──StoragePort──► mem | sqlite
//! ```
//!
//! **Prerequisites:** Cargo features `runtime` + `mem` (or `sqlite`), and `PHOTON_TRANSPORT_KEY`
//! (base64 of 32 bytes). See [`config`].
//!
//! **In-memory** (default — [`PhotonBuilder`] installs [`InProcStoragePort`] when you omit
//! [`storage_port`](PhotonBuilder::storage_port)):
//!
//! ```rust,no_run
//! use std::sync::Arc;
//!
//! use photon::{JsonIdentityFactory, Photon};
//!
//! # fn main() -> photon::Result<()> {
//! let photon = Photon::builder().auto_registry().build()?;
//! photon.start_executor(Arc::new(JsonIdentityFactory))?;
//! // Prefer EventType { … }.publish_on(&photon).await
//! # let _ = photon;
//! # Ok(())
//! # }
//! ```
//!
//! **`SQLite`** — durable single-process (write-through + in-memory live fanout):
//!
//! ```rust,ignore
//! use std::sync::Arc;
//!
//! use photon::{JsonIdentityFactory, Photon, SqliteStoragePort};
//!
//! # async fn boot() -> photon::Result<()> {
//! let port = Arc::new(SqliteStoragePort::open("/var/lib/photon/events.db").await?);
//! let photon = Photon::builder()
//!     .storage_port(port)
//!     .auto_registry()
//!     .build()?;
//! photon.start_executor(Arc::new(JsonIdentityFactory))?;
//! # let _ = photon;
//! # Ok(())
//! # }
//! ```
//!
//! Runnable: `cargo run -p uf-photon --example embedded_mem --features runtime,mem`.
//! Durable: `cargo run -p uf-photon --example embedded_sqlite --features runtime,sqlite`.
//! Then jump to [declare topics](#3-declare-topics-and-handlers).
//!
//! ## Brokered (publisher + worker binaries)
//!
//! Use this when multiple processes (or hosts) must share the same topic log. `mem` and `sqlite`
//! cannot fan out across processes — wire a broker adapter instead.
//!
//! ```text
//! Publisher binary  ──publish_on──► Photon ──StoragePort──► broker (NATS / Kafka / Fluvio)
//! Worker binary     ──start_executor / #[subscribe]──► Photon ──same broker──►
//! ```
//!
//! ### What you create
//!
//! | Piece | Purpose |
//! |-------|---------|
//! | Shared topics | Same `#[topic]` types (shared crate or both binaries compile them) |
//! | Publisher binary | `[[bin]]` that publishes; typically **no** `start_executor` |
//! | Worker binary | `[[bin]]` with `#[subscribe]` handlers; **must** call `start_executor` |
//! | Broker | NATS / Kafka / Fluvio cluster your ops team runs |
//! | Shared env | Same `PHOTON_TRANSPORT_KEY` + broker URL (e.g. `PHOTON_NATS_URL`) on every process |
//! | Broker TLS | Default [`BrokerTransportSecurity::RequireTls`]; plaintext needs explicit opt-in |
//!
//! ### Shared setup (both binaries)
//!
//! 1. Enable `runtime` plus one broker feature (`nats` is the usual first choice).
//! 2. Build the same storage port against the **same** cluster — pick a builder below.
//! 3. Call [`.auto_registry()`](PhotonBuilder::auto_registry) when using macros.
//! 4. Keep the [`Photon`] handle for `publish_on` / `subscribe_on`.
//!
//! | Adapter | Feature | Builder (publisher + worker examples) |
//! |---------|---------|----------------------------------------|
//! | NATS | `nats` | [`NatsStoragePortBuilder`](../photon_backend_nats/struct.NatsStoragePortBuilder.html) |
//! | Kafka | `kafka` | [`KafkaStoragePortBuilder`](../photon_backend_kafka/struct.KafkaStoragePortBuilder.html) |
//! | Fluvio | `fluvio` | [`FluvioStoragePortBuilder`](../photon_backend_fluvio/struct.FluvioStoragePortBuilder.html) |
//!
//! | Concern | Production setting |
//! |---------|-------------------|
//! | Transport key | `PHOTON_TRANSPORT_KEY` from a secret manager (never `PHOTON_ALLOW_DEV_TRANSPORT_KEY`) |
//! | Broker TLS | `.require_tls()` / TLS URL (`tls://…`); never set `PHOTON_ALLOW_INSECURE_BROKER` |
//! | NATS auth | `.credentials_file("/run/secrets/nats.creds")` or `PHOTON_NATS_CREDS` (not URL userinfo) |
//! | Kafka retention | Pre-create topics with `retention.ms` or broker defaults (`rskafka` cannot set configs) |
//! | Fluvio retention | Applied at topic create from `PHOTON_FLUVIO_RETENTION` |
//!
//! Env index: [`config`]. NATS sketches follow; swap the builder for Kafka/Fluvio.
//!
//! ### Publisher binary
//!
//! Wire the broker port, declare topics, publish. Skip [`Photon::start_executor`] unless this
//! process also handles events.
//!
//! ```rust,ignore
//! use std::sync::Arc;
//!
//! use photon::{Photon, NatsStoragePort, ReplayCursor};
//!
//! # async fn boot_publisher() -> photon::Result<()> {
//! let port = Arc::new(
//!     NatsStoragePort::builder()
//!         .url("tls://nats.example:4222")
//!         .credentials_file("/run/secrets/nats.creds")
//!         .require_tls()
//!         .replay_cursor(ReplayCursor::StreamSeq)
//!         .sync_ack(true)
//!         .build()
//!         .await?,
//! );
//! let photon = Photon::builder()
//!     .storage_port(port)
//!     .auto_registry()
//!     .build()?;
//! // OrderCreated { … }.publish_on(&photon).await?;
//! # let _ = photon;
//! # Ok(())
//! # }
//! ```
//!
//! ### Worker binary
//!
//! Same storage port wiring as the publisher, plus `#[subscribe]` handlers and
//! [`Photon::start_executor`]:
//!
//! ```rust,ignore
//! use std::sync::Arc;
//!
//! use photon::{JsonIdentityFactory, Photon, NatsStoragePort, ReplayCursor};
//!
//! # async fn boot_worker() -> photon::Result<()> {
//! let port = Arc::new(
//!     NatsStoragePort::builder()
//!         .from_env_defaults()
//!         .replay_cursor(ReplayCursor::StreamSeq)
//!         .sync_ack(true)
//!         .build()
//!         .await?,
//! );
//! let photon = Photon::builder()
//!     .storage_port(port)
//!     .auto_registry()
//!     .build()?;
//! photon.start_executor(Arc::new(JsonIdentityFactory))?;
//! # let _ = photon;
//! # Ok(())
//! # }
//! ```
//!
//! ### Run both
//!
//! 1. Start the broker.
//! 2. Start **worker(s)** first so subscriptions are ready.
//! 3. Start one or more **publishers**.
//!
//! Runnable: `cargo run -p uf-photon --example nats_worker --features runtime,nats` then
//! `nats_publisher` (same features). Same pair contract with other adapters: `kafka_worker` +
//! `kafka_publisher` (`runtime,kafka`), `fluvio_worker` + `fluvio_publisher` (`runtime,fluvio`),
//! and the production-TLS variant `nats_secure_worker` + `nats_secure_publisher`
//! (`runtime,nats`). Multi-terminal runbook: repository `photon/README.md` § How to run examples.
//!
//! Then continue with [declare topics](#3-declare-topics-and-handlers).
//!
//! ## 3. Declare topics and handlers
//!
//! - [`topic`] — typed event struct + inventory registration; generates `publish_on` / `subscribe_on`
//! - [`subscribe`] — inventory-registered handler; requires [`Photon::start_executor`] on workers
//! - [`prelude`] — common imports (`Event`, `SubscribeOpts`, `Photon`, macros)
//!
//! Attribute tables: [`config`]. Runnable: `keyed_topic`, `consumer_group`, `subscribe_v2`.
//!
//! ## 4. Publish and subscribe
//!
//! After [`topic`], the macro generates typed methods on your event struct. Prefer those over
//! raw [`Photon::publish`] / [`Photon::subscribe`].
//!
//! **Publish** — `publish_on` with an explicit [`Photon`] handle:
//!
//! ```rust,ignore
//! OrderCreated {
//!     order_id: "ord-1".into(),
//!     amount_cents: 9900,
//! }
//! .publish_on(&photon)
//! .await?;
//! ```
//!
//! **Typed stream** — `subscribe_on` (no `#[subscribe]` / executor required):
//!
//! ```rust,ignore
//! use futures::StreamExt;
//! use photon::SubscribeOpts;
//!
//! let opts = SubscribeOpts::default_ephemeral();
//! let mut stream = OrderCreated::subscribe_on(&photon, opts).await?;
//! if let Some(Ok(envelope)) = stream.next().await {
//!     let _payload = envelope.payload; // OrderCreated
//! }
//! ```
//!
//! **Inventory handlers** — `#[subscribe]` + [`Photon::start_executor`] (Embedded hosts and
//! Brokered workers).
//!
//! Optional sugar after [`configure`]: `.publish()` / `.subscribe()` without a handle.
//! Raw topic-name API (advanced): [`Photon::subscribe`].
//!
//! Runnable: `embedded_mem` (`publish_on` + `#[subscribe]`), `keyed_topic` (`subscribe_on`),
//! `manual_subscribe` (raw [`Photon::subscribe`]).
//!
//! ## 5. Run the executor and reclaim
//!
//! - [`Photon::start_executor`] — dispatch inventory-registered `#[subscribe]` handlers (Embedded
//!   hosts and Brokered workers)
//! - [`Photon::reclaim_transport`] — retention sweep past the safe watermark
//! - Optional ops telemetry: [`OpsLog`] via [`PhotonBuilder::ops_log`] (example: `telemetry_ops_log`)
//! - Restart-safe checkpoints: `durable = "…"` resumes from the last committed seq after a crash
//!   (example: `durable_consumer_recovery`, `runtime,sqlite`)
//!
//! ## Notes
//!
//! - **Transport key:** boot fails closed without a valid `PHOTON_TRANSPORT_KEY` (see [`config`]).
//!   Actor/payload JSON is sealed before storage and broker write; handlers receive decrypted events.
//! - **Host responsibilities:** Photon is a trusted-host library — authenticate and authorize at the
//!   process edge before publish/subscribe/admin/WS. See repository `SECURITY.md` (privileged handle,
//!   no in-core topic ACL, production identity factory, broker TLS via `BrokerTransportSecurity` /
//!   `PHOTON_ALLOW_INSECURE_BROKER`, never `PHOTON_ALLOW_DEV_TRANSPORT_KEY` in production).
//! - **Durable multi-process:** use a broker adapter; `mem` does not cross process boundaries.
//! - **Lab topologies:** testkit / bench `PHOTON_TOPOLOGY` values are harness labels for
//!   lab matrices. Product choices remain Embedded / Brokered above.
//! - **Custom adapters:** implement [`StoragePort`] and pass it to
//!   [`PhotonBuilder::storage_port`]. Advanced delivery traits live behind that port.
//!
//! ## Architecture
//!
//! ```text
//! Application  →  Photon (macros + Photon runtime)  →  storage port  →  delivery backend
//! ```
//!
//! ```text
//!   Typed API (#[topic], publish_on / publish, #[subscribe])
//!              │
//!              v
//!   Photon runtime
//!              │
//!              v
//!         StoragePort  ──►  mem (InProcStoragePort)
//!                      ──►  sqlite (SqliteStoragePort)
//!                      ──►  nats / fluvio / kafka (broker crates)
//!                      ──►  custom implementation
//! ```
//!
//! Full option reference: [`config`]. Macro expansion: repository `docs/macro-expansion.md`.

#![cfg_attr(test, allow(clippy::unwrap_used, clippy::expect_used))]
/// Re-export identity port traits and JSON stubs from [`photon_core`].
pub use photon_core::{
    actor_downcast_methods, Actor, IdentityError, IdentityFactory, JsonActor, JsonIdentityFactory,
};
/// Register an async handler (`#[photon::subscribe]`).
pub use photon_macros::subscribe;
/// Register a typed topic struct (`#[photon::topic]`).
pub use photon_macros::topic;
/// Quark inventory for compile-time topic/handler registration.
pub use quark::inventory;

#[cfg(feature = "runtime")]
mod runtime;

#[cfg(feature = "runtime")]
pub use runtime::*;

#[cfg(feature = "runtime")]
pub mod config;
