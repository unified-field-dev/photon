# ADR-001: Consumer groups and virtual-shard load balancing

**Date:** 2026-06-29  
**Status:** Accepted

## Context

Photon subscriptions are **broadcast-only** today: each durable `subscription_name` tails the full `(topic, key_filter)` stream and checkpoints independently. Cross-node fanout extends broadcast across nodes; it does not divide work among competing consumers.

We need **load-balanced** delivery: N replicas in a **consumer group** each process a disjoint subset of events, while **broadcast** remains the default for fanout use cases.

## Decision

We introduce two **delivery modes**, configured at `#[photon::topic]` (publish routing) and `#[photon::subscribe]` (`durable` vs `group`):

| Mode | Semantics | Checkpoint owner |
|------|-----------|------------------|
| **Broadcast** (default) | Every matching subscriber receives every event | `subscription_name` × stream |
| **ConsumerGroup** | Exactly one active consumer per virtual shard | `group_id` × shard stream |

**Virtual shards** apply to any group topic:

- `shard_count = N` (topic default 32; overridable on subscribe).
- `shard_id = hash(routing_key) % N` where `routing_key` is, in order: publish `topic_key` → topic `shard_by` JSON field → pre-generated `event_id`.
- Storage shard stream key: `__photon/shard/{id}` (reserved prefix).
- Ordering is **per-shard FIFO** only; global topic order is not preserved.

**Publish routing:** group topics append only to shard streams; broadcast topics use the existing path. Group and broadcast must not share the same topic name.

**Coordination:** shard assignment is pluggable — static env for lab (`PHOTON_GROUP_*`), in-process coordinator for single-node, and [`MemoryLeaseStore`](../../photon-backend/src/consumer_group/lease_store.rs) for tests. Fleet cross-host lease rebalance is a future host concern. Checkpoints use **`group_id`**, not per-instance name, so reassignment replays from the group cursor for that shard.

**Compatibility:** existing `#[subscribe(durable = "...")]` handlers are unchanged; `durable` and `group` are mutually exclusive at compile time.

## Consequences

**Positive**

- N group replicas process ~1/N of events under steady membership.
- Broadcast fanout and competing consumers coexist in one runtime via explicit configuration.
- Reuses per-stream checkpoints via `StoragePort`; no second persistence model for event delivery.

**Negative / constraints**

- **At-least-once** delivery; handlers must be idempotent across rebalance.
- Rebalance may duplicate until checkpoint; no exactly-once guarantee in this ADR.
- Hot shards possible if `shard_by` or `shard_count` is poorly chosen.
- Fleet lease split-brain requires TTL (and future fencing); not fully specified here.

## References

| Topic | Location |
|-------|----------|
| Shard routing, publish/subscribe | `photon-backend/src/shard_router.rs`, `publish_routing.rs`, `group_subscribe.rs` |
| Coordinator + leases | `photon-backend/src/consumer_group/` |
| Executor + macros | `photon-runtime/src/executor.rs`, `photon-macros/src/topic.rs`, `subscribe.rs` |
| Verification | BM-PG0–PG2 (`cargo run -p photon-bench -- experiments`), `photon-e2e` `consumer_group_static_mem_embedded` |
