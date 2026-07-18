# Photon configuration reference

> Canonical index: [docs.rs `photon::config`](https://docs.rs/uf-photon/latest/photon/config/). Edit this file; content is included into rustdoc via `photon/src/config.rs`.

Central index for compile-time options, cross-cutting environment variables, and macro attributes.
**Adapter-specific builder options live on each storage builder type** — this page links there instead of duplicating tables.

**Getting started (topology):** [Mode 1 — Embedded](https://docs.rs/uf-photon/latest/photon/#mode-1--embedded-one-binary) · [Mode 2 — Brokered](https://docs.rs/uf-photon/latest/photon/#mode-2--brokered-publisher--worker-binaries)

## Precedence

| Layer | When applied | Overrides |
|-------|--------------|-----------|
| Cargo features | Compile time | Enables modules/backends (`runtime`, `mem`, `sqlite`, `nats`, …) |
| Proc-macro attributes | Compile time | Topic routing, handler registration |
| `PhotonBuilder` | Process startup | Transport, backend, retention policy, ops log |
| Storage port builders | Process startup | Broker connection, replay, sharding (per adapter) |
| Environment variables | Process startup | Builder fallbacks, retention defaults, telemetry |
| CLI (`photon-bench`) | Bench/e2e only | Matrix dimensions, hardware labels |

**Retention:** [`PhotonBuilder::retention_policy`](https://docs.rs/uf-photon/latest/photon/struct.PhotonBuilder.html#method.retention_policy) → unset fields fall back to env → hardcoded defaults in `photon-backend/src/retention/config.rs`.

**Telemetry:** [`PhotonBuilder::ops_log`](https://docs.rs/uf-photon/latest/photon/struct.PhotonBuilder.html#method.ops_log) installs at build time; otherwise `PHOTON_TELEMETRY` (see `photon-telemetry/src/global.rs`) applies when ops log is resolved from env.

**Envelope crypto:** `TransportCrypto::from_env()` requires `PHOTON_TRANSPORT_KEY` (fail-closed). Development-only fallback via `from_env_or_dev_default()` needs `PHOTON_ALLOW_DEV_TRANSPORT_KEY=1`.

---

## Security-sensitive variables

| Variable | Production | Purpose |
|----------|------------|---------|
| `PHOTON_TRANSPORT_KEY` | **Required** | Base64-encoded 32-byte XChaCha20-Poly1305 transport key. Missing/invalid key fails boot. |
| `PHOTON_ALLOW_DEV_TRANSPORT_KEY` | **Unset** | When `1`/`true`, allows the hard-coded development key via `from_env_or_dev_default()`. Never set in production or CI. |
| `PHOTON_BENCH_CRYPTO` | Unset (encrypt on) | Bench-only: `0`/`false` stores plaintext envelopes to measure crypto cost. |

---

## Cargo features (`photon`)

| Feature | Purpose |
|---------|---------|
| `runtime` | Full stack (`Photon`, backends, executor). |
| `mem` | In-process [`InProcStoragePort`](../../photon_backend/storage/struct.InProcStoragePort.html) (dev/tests). |
| `sqlite` | Embedded `SQLite` adapter (optional facade re-export). |
| `nats` | NATS `JetStream` adapter (optional facade re-export). |
| `kafka` | Kafka adapter (optional facade re-export). |
| `fluvio` | Fluvio adapter (optional facade re-export). |

Default: **none** — enable `runtime` + `mem` explicitly for evaluation.

---

## Host boot — [`PhotonBuilder`](https://docs.rs/uf-photon/latest/photon/struct.PhotonBuilder.html)

Methods, examples, and wiring checklist: **see [`PhotonBuilder` rustdoc](https://docs.rs/uf-photon/latest/photon/struct.PhotonBuilder.html)**.

Runnable host boot: `cargo run -p uf-photon --example embedded_mem --features runtime,mem`.

---

## Storage adapter builders

Choose adapters from the crate [Getting started](https://docs.rs/uf-photon/latest/photon/#choose-a-topology) (Mode 1 embedded vs Mode 2 brokered). Broker connection, replay, sharding, and env fallbacks are documented **on each builder** (not duplicated here). Each broker builder includes **publisher binary** and **worker binary** examples; `mem` / `SQLite` show Mode 1 host (`start_executor`) examples.

| Adapter | Builder (config + example) | Resolved config | Storage port |
|---------|---------------------------|-----------------|--------------|
| NATS | [`NatsStoragePortBuilder`](../../photon_backend_nats/struct.NatsStoragePortBuilder.html) | [`NatsConfig`](../../photon_backend_nats/struct.NatsConfig.html) | [`NatsStoragePort`](../../photon_backend_nats/struct.NatsStoragePort.html) |
| Kafka | [`KafkaStoragePortBuilder`](../../photon_backend_kafka/struct.KafkaStoragePortBuilder.html) | [`KafkaConfig`](../../photon_backend_kafka/struct.KafkaConfig.html) | [`KafkaStoragePort`](../../photon_backend_kafka/struct.KafkaStoragePort.html) |
| Fluvio | [`FluvioStoragePortBuilder`](../../photon_backend_fluvio/struct.FluvioStoragePortBuilder.html) | [`FluvioConfig`](../../photon_backend_fluvio/struct.FluvioConfig.html) | [`FluvioStoragePort`](../../photon_backend_fluvio/struct.FluvioStoragePort.html) |
| `SQLite` | — (`PHOTON_SQLITE_PATH`) | — | [`SqliteStoragePort`](../../photon_backend_sqlite/struct.SqliteStoragePort.html) |
| In-process | — (default) | — | [`InProcStoragePort`](../../photon_backend/storage/struct.InProcStoragePort.html) |

Install any port with [`PhotonBuilder::storage_port`](https://docs.rs/uf-photon/latest/photon/struct.PhotonBuilder.html#method.storage_port). Broker adapters use `StoragePortBuilder::build().await`; `SQLite` uses [`SqliteStoragePort::open`](../../photon_backend_sqlite/struct.SqliteStoragePort.html) / [`from_env`](../../photon_backend_sqlite/struct.SqliteStoragePort.html#method.from_env).

---

## Proc-macro attributes

### `#[photon::topic]`

Defined in `photon-macros/src/topic.rs`. Full usage: [`topic` macro rustdoc](https://docs.rs/uf-photon/latest/photon/attr.topic.html).

| Attribute | Required | Values | Purpose |
|-----------|----------|--------|---------|
| `name` | yes | string | Stable topic name (e.g. `"orders.created"`). |
| `keyed_by` | no | field name | Partition key field. Publish extracts the JSON field (string as-is, other values stringified); missing field fails publish. |
| `delivery` | no | `"broadcast"` (default), `"group"`, `"consumer_group"` | Broadcast fanout vs consumer-group shard routing. |
| `shards` | no | integer | Virtual shard count for group topics (default 32). |
| `shard_by` | no | field name | JSON field for shard routing when publish has no explicit key. |

`schema_json` on [`TopicDescriptor`](../../photon_backend/struct.TopicDescriptor.html) is reserved metadata (macro emits `"{}"`); not validated at publish time. Expansion details: [`docs/macro-expansion.md`](macro-expansion.md).

### `#[photon::subscribe]`

Defined in `photon-macros/src/subscribe.rs`. Full usage: [`subscribe` macro rustdoc](https://docs.rs/uf-photon/latest/photon/attr.subscribe.html). Expansion notes: [`docs/macro-expansion.md`](macro-expansion.md).

| Attribute | Required | Values | Purpose |
|-----------|----------|--------|---------|
| `topic` | yes | string | Topic name matching `#[topic]`. |
| `durable` | one of | subscription name string | Broadcast durable checkpoint owner. |
| `group` | one of | group id string | Consumer-group handler (mutually exclusive with `durable`). |
| `shards` | no | integer | Override topic shard count for this handler. |

**Handler parameters (v2):** first arg is an actor binding — `Box<dyn Actor>`, `Arc<dyn Actor>`, `Box<Concrete>`, or `Arc<Concrete>` (concrete types downcast via `Actor::into_any`; failure → `PhotonError::Identity`). Second arg is the typed payload. Optional trailing injectables: `&Event`, `HandlerCtx`.

Architecture: repository `docs/adr/001-consumer-groups.md`. Runnable: `cargo run -p uf-photon --example subscribe_v2 --features runtime,mem`.

---

## Subscribe options

[`SubscribeOpts`](../../photon_backend/models/subscribe_opts/struct.SubscribeOpts.html) and [`GroupOpts`](../../photon_backend/models/subscribe_opts/struct.GroupOpts.html) — examples on those types. Runnable: `cargo run -p uf-photon --example keyed_topic --features runtime,mem`.

---

## Cross-cutting runtime environment variables

### Transport and append path

| Variable | Default | Source | Purpose |
|----------|---------|--------|---------|
| `PHOTON_TRANSPORT_KEY` | *(required)* | `photon-backend/src/event/envelope.rs` | Base64 32-byte envelope encryption key. Fail-closed when unset. |
| `PHOTON_ALLOW_DEV_TRANSPORT_KEY` | unset | same | Opt-in for hard-coded development key (`1`/`true`). Development only. |
| `PHOTON_APPEND_ASYNC` | `true` | `photon-backend/src/transport/append_buffer.rs` | Async batched append; `0`/`false`/`no` disables. |
| `PHOTON_APPEND_BATCH_MAX` | `32` | same | Max records per append batch. |
| `PHOTON_APPEND_FLUSH_MS` | `10` | same | Flush interval for async append. |
| `PHOTON_APPEND_QUEUE_MAX` | `1024` | same | Pending append queue cap. |
| `PHOTON_TRANSPORT_RETAIN_SEQ` | `1000` | `photon-backend/src/transport/retention/truncate.rs` | Min sequence margin after transport truncate. |

### Checkpoints

| Variable | Default | Source | Purpose |
|----------|---------|--------|---------|
| `PHOTON_CHECKPOINT_COALESCE_EVERY` | `10` | `photon-backend/src/checkpoint/coalescer.rs` | Checkpoint writes per N deliveries. |
| `PHOTON_CHECKPOINT_FLUSH_MS` | `500` | same | Max time between checkpoint flushes (min 50). |

### Retention

| Variable | Default | Source | Purpose |
|----------|---------|--------|---------|
| `PHOTON_TRANSPORT_MAX_AGE_SECS` | unset | `photon-backend/src/retention/config.rs` | TTL in seconds; unset = sequence-only retention. |
| `PHOTON_RETENTION_PIN_DLQ` | `true` | same | Pin reclaim floor at min DLQ seq per partition. |
| `PHOTON_RETENTION_SWEEP_MS` | `30000` | same | Background sweep interval; `0` = manual sweep only. |

### Delivery and tailer

| Variable | Default | Source | Purpose |
|----------|---------|--------|---------|
| `PHOTON_HANDLER_POOL_SIZE` | `64` | `photon-backend/src/delivery/worker_pool.rs` | Max concurrent handler tasks per partition. |

### Consumer groups (static / lab)

| Variable | Default | Source | Purpose |
|----------|---------|--------|---------|
| `PHOTON_GROUP_SHARD_COUNT` | topic default | `photon-backend/src/consumer_group/static_assignment.rs` | Total virtual shards. |
| `PHOTON_GROUP_MEMBER_COUNT` | `1` | same | Group size for round-robin assignment. |
| `PHOTON_GROUP_INSTANCE_ID` | `0` | same | This member's index. |
| `PHOTON_GROUP_SHARD_ASSIGNMENT` | round-robin | same | Comma list or range `0-15` overriding assignment. |

Fleet multi-node delivery uses broker-native fanout (`nats` / `fluvio` / `kafka` adapters). Lab shard assignment uses static env below; durable leases use [`MemoryLeaseStore`](../../photon_backend/consumer_group/struct.MemoryLeaseStore.html) in-process.

### Telemetry and remote URLs

| Variable | Default | Source | Purpose |
|----------|---------|--------|---------|
| `PHOTON_TELEMETRY` | `console` | `photon-telemetry/src/global.rs` | `console`, or `off`/`0`/`false` for noop. |
| `PHOTON_REMOTE_BASE_URL` | unset | `photon-runtime/src/subsystem_remote_url.rs` | Direct Photon remote base URL. |
| `SUBSYSTEM_GATEWAY_BASE_URL` | unset | same | Gateway base for cell URL construction. |
| `SUBSYSTEM_CELL_SLUG` | `home` | same | Cell slug appended to gateway URL. |
| `PHOTON_TOPOLOGY` | unset | testkit bootstrap | Host topology label (`split-runtime`, etc.). |

---

## Bench and test harness only

| Variable | Purpose |
|----------|---------|
| `PHOTON_NATS_URL` | Live `JetStream` for nats e2e/bench matrix. |
| `PHOTON_SQLITE_PATH` | `SQLite` database file for `sqlite` e2e/bench matrix (embedded durable tier). |
| `PHOTON_KAFKA_BROKERS` | Live Kafka for kafka e2e/bench matrix. |
| `PHOTON_FLUVIO_ENDPOINT` | Live Fluvio SC for fluvio e2e/bench matrix. |
| `PHOTON_BENCH_TOPIC_SHARDS` | Storage-agnostic PFH dimension alias (falls back to broker-specific shard env). |
| `PHOTON_BENCH_HARDWARE` | Hardware profile label in report JSON. |
| `PHOTON_BENCH_CRYPTO` | `0` disables crypto in bench runs. |
| `PHOTON_BENCH_P6_P99_BUDGET_MS` | Fanout latency pass budget for BM-P6. Authoritative value from in-VPC run: **103** (rounded from p99 102.58 ms). |

CLI reference: `photon-bench/EXPERIMENTS.md`, `photon-bench/src/cli.rs`.

---

## Related documentation

| Document | Audience |
|----------|----------|
| `cargo doc -p uf-photon --features runtime,mem --open` | **Primary** API + configuration (`photon::config`) |
| `README.md` | Landing, model, FAQ |
| `photon/README.md` | Facade features and wiring checklist |
| [`macro-expansion.md`](macro-expansion.md) | What `#[topic]` / `#[subscribe]` generate |
| `docs/adr/001-consumer-groups.md` | Consumer group architecture |
