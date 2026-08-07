# photon-e2e

Matrix-driven **correctness** integration tests for Photon pub/sub runtime features.

## vs inline `photon/tests`

| | `photon/tests` | `photon-e2e` |
|---|---|----------------|
| Scope | Fast, crate-scoped defaults | Cross-cutting matrix |
| Storage | In-process `mem` + `sqlite`; brokers in CI/AWS | `mem`, `sqlite`, `nats`, `kafka`, `fluvio` |
| Assertions | Unit/integration | Declarative scenario runner |

Inline tests stay for macro expansion, instrumentation unit tests, and focused backend integration tests.

## Behavioral contract

- publish returns stable, unique `event_id`
- subscribe-after-publish and tail-only delivery semantics
- broadcast fanout (`multi_subscriber`, `cross_node_fanout`, `fanout_multi_subscriber`)
- executor dispatch under publish load (`executor_fanout_dispatch`)
- consumer groups: static assignment, round-robin, rebalance verify
- `after_seq` replay and checkpoint resume
- keyed partition filter (happy and miss paths)

Performance budgets belong in [`photon-bench`](../photon-bench/README.md).

## CI strategy

| Trigger | Scope | Command |
|---------|-------|---------|
| Push / PR | Full matrix — mem (18 incl. topology/telemetry) + sqlite + brokers | see [`.github/workflows/ci.yml`](../.github/workflows/ci.yml) `e2e` job |
| AWS (authoritative for dev laptop) | All gates without local `cargo` | Operator AWS campaign (sqlite + broker fleets) |

CI sets broker env vars from service containers and the Fluvio lab script (`PHOTON_NATS_URL`, `PHOTON_KAFKA_*`, `PHOTON_FLUVIO_*`).

## Coverage matrix

**CI default:** all five storage adapters (`mem`, `sqlite`, `nats`, `kafka`, `fluvio`) on push/PR; scenarios use `isolated-lab` + telemetry `off` unless noted.

| Scenario | mem CI | sqlite (`--features sqlite`) | nats (`--ignored` + `PHOTON_NATS_URL`) | kafka (`--ignored` + `PHOTON_KAFKA_BROKERS`) | fluvio (`--ignored` + `PHOTON_FLUVIO_ENDPOINT`) |
|----------|--------|-------------------------------|----------------------------------------|---------------------------------------------|------------------------------------------------|
| `publish_subscribe_smoke` | ✓ | ✓ | ✓ | ✓ | ✓ |
| `consumer_group_static` | ✓ | ✓ | ✓ | ✓ | ✓ |
| `consumer_group_round_robin` | ✓ | ✓ | ✓ | ✓ | ✓ |
| `consumer_group_rebalance` | ✓ | ✓ | ✓ | ✓ | ✓ |
| `after_seq_replay` | ✓ | ✓ | ✓ | ✓ | ✓ |
| `multi_subscriber` | ✓ | ✓ | ✓ | ✓ | ✓ |
| `fanout_multi_subscriber` | ✓ | ✓ | ✓ | ✓ | ✓ |
| `executor_fanout_dispatch` | ✓ | ✓ | ✓ | ✓ | ✓ |
| `keyed_filter` | ✓ | ✓ | ✓ | ✓ | ✓ |
| `keyed_filter_miss` | ✓ | ✓ | ✓ | ✓ | ✓ |
| `ephemeral_tail_only` | ✓ | ✓ | ✓ (`PHOTON_NATS_REPLAY_CURSOR=tail_only`) | ✓ (`PHOTON_KAFKA_REPLAY_CURSOR=tail_only`) | ✓ (`PHOTON_FLUVIO_REPLAY_CURSOR=tail_only`) |
| `checkpoint_resume` | ✓ | ✓ | ✓ | ✓ | ✓ |
| `cross_node_fanout` | ✓ | ✓ | ✓ | ✓ | ✓ |

**Topology / telemetry smokes** (mem only): included in default `cargo test -p photon-e2e` (5 tests).

Backend integration in CI: `subscribe_executor`, `retention_reclaim`, `handler_failure_dlq`.

## Run (AWS — do not run `cargo` on resource-constrained dev laptops)

Authoritative e2e for constrained laptops runs on AWS: SQLite/mem smoke, then broker fleets (NATS, Kafka, Fluvio). Operator scripts provision EC2, rsync the tree, and run the same `photon-e2e` gates CI uses. See [docs/VERIFICATION.md](../docs/VERIFICATION.md).

Local `cargo test` (brokers need live services):

```bash
cargo test -p photon-e2e
cargo test -p photon-e2e --features sqlite
cargo test -p photon-e2e --features nats -- --ignored
```

## Related

- Harness: [`photon-testkit`](../photon-testkit/README.md)
- Benchmarks: [`photon-bench`](../photon-bench/README.md)
