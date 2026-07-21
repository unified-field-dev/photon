# Photon benchmark experiment registry

Pre-registered experiment IDs, dimension matrix, results log, and runner commands.

**Methodology:** [`PERFORMANCE_STUDY.md`](PERFORMANCE_STUDY.md). Adapter contract: [docs.rs `photon` architecture](https://docs.rs/uf-photon/latest/photon/#architecture).

---

## Validation status

| Tier | Storage | Status | Authoritative hardware |
|------|---------|--------|----------------------|
| **Fleet ingress** | `nats` | **Measured** | `aws-c6i-large` in-VPC (BM-PFH, recommended topology) |
| **Fleet smoke / delivery** | `nats` | **PASS** (delivery gates) | `aws-c6i-large` in-VPC (PF0–PF4, P6, PFS, PFE, PG*, PB5) — **NATS-only** |
| **NATS ingress gate** | `nats` | **PASS** | PFH + PF2/PF4/P6/PG*/PFS/PFE in-VPC (2026-07-07) |
| **Dev / CI** | `mem` | **PASS** | `aws-t3.medium`; local `dev-wsl` smoke |
| **Broker smoke-validated** | `fluvio`, `kafka` | E2E + contract on compose/AWS t3.medium | Delivery PF*/PB* gates not re-run on Fluvio/Kafka |
| **Fleet ingress (PFH)** | `kafka` | **Measured** (2026-07-07) | `aws-c6i-large` in-VPC; primary-row baseline + sharded; multibench bc=1,2 (bc=4 paused) |
| **Fleet ingress (PFH)** | `fluvio` | **Measured** (2026-07-07) | `aws-c6i-large` in-VPC; multibench bc=1/2/4 on 4-SPU cluster (**~400k ops/s** at bc=4) |

---

## Authoritative test environment (recommended topology)

All **decision-grade** broker PFH ingress numbers use this primary row on **`aws-c6i-large`** in-VPC. Do not mix with `dev-wsl` smoke runs or non-primary sweep cells.

| Field | Value |
|-------|-------|
| **Hardware profile** | `aws-c6i-large` |
| **Bench host** | 1× `c6i.large` (2 vCPU, 4 GiB) per embed client; in-VPC |
| **Broker fleet** | 4× `t3.medium` (NATS JetStream / Kafka / Fluvio SPUs) |
| **Shards** | 4 (`stream_shards` / `topic_shards`) |
| **`rep` per shard** | 1 (NATS) |
| **Topology** | `broker-cluster`; `PHOTON_AWS_USE_PUBLIC_IPS=0` |
| **PFH workload** | Single topic; **256** publisher tasks; **0** subscribers; **30 s**; crypto off |
| **Primary row** | `stream_seq`, `sync_ack=1`, target **100k ops/s** |
| **Sweep dimension** | `bench_client_count ∈ {1, 2, 4}` (multi-embed ingress) |
| **Reports** | `profiling/photon-bench/reports/bm-pfh-{nats,kafka,fluvio}-*-aws.json` |
| **Date** | NATS 2026-07-06; Kafka/Fluvio 2026-07-07 |

**Mem baseline:** `aws-t3.medium`, `isolated-lab`, `--storage mem`, telemetry off.

**Scope note:** Delivery / fanout gates (BM-PF*, BM-P6, BM-PFS, BM-PFE, BM-PG*, BM-PB5) are authoritative on **NATS only**. Kafka/Fluvio fleet work measured BM-PFH ingress.

---

## Dimensions

| Dimension | Values | Notes |
|-----------|--------|-------|
| **Storage** | `mem`, `sqlite`, `nats`, `fluvio`, `kafka` | Primary matrix. |
| **Topology** | `isolated-lab`, `broker-cluster` | |
| **Telemetry** | `off`, `console` | |
| **Hardware** | `dev-wsl` (smoke), `aws-c6i-large` (broker PFH), `aws-t3.medium` (mem) | |

---

## Experiment status

| Status | Meaning |
|--------|---------|
| `adapter-ready` | Runs on `mem` / `sqlite` |
| `broker-ready` | Validated on NATS fleet |
| `broker-pending` | Wired; needs live cluster |
| `group-ready` | Consumer group on `mem` |

---

## Photon layer experiment log

| ID | Status | Workload | Primary metric | Pass criteria | Results |
|----|--------|----------|----------------|---------------|----------------|
| **BM-P0** | adapter-ready | Publish-only (0 subscribers) | p50/p95 publish ms | Flat vs op index | mem `aws-t3.medium` p50 **0.008 ms** PASS |
| **BM-P1** | adapter-ready | Publish + 1 ephemeral subscriber | delivery − publish | Per-delivery wait − publish p95 &lt; 500 ms | mem `aws-t3.medium` PASS |
| **BM-P2** | adapter-ready | Publish + N durable subscribers | drain p95 vs N | Sub-linear p95 growth | mem historical PASS |
| **BM-P3** | adapter-ready | Sustained publish (replay buffer) | backlog peak | Backlog bounded | mem historical PASS |
| **BM-P4** | adapter-ready | Checkpoint replay after restart | events/s resumed | ≥ 90% fresh throughput | mem historical PASS |
| **BM-P5** | adapter-ready | Keyed topic partition filter | filter path p95 | Flat vs cardinality | mem historical PASS |
| **BM-P6** | broker-ready | Broker fanout latency | fanout p99 | p99 ≤ 103 ms (`PHOTON_BENCH_P6_P99_BUDGET_MS`) | in-VPC PASS 2026-07-07 |
| **BM-P7** | adapter-ready | Durable handler + checkpoint | replay events/s | Replay &gt; 0 | mem historical PASS |
| **BM-P8** | adapter-ready | Slow consumer + high publish | error rate | err &lt; 0.1% | mem historical PASS |
| **BM-P9** | adapter-ready | Crypto on vs off | publish p50 delta | Δ ≤ 15 ms | mem historical PASS |

### Sustained load

| ID | Status | Target rate | mem (`aws-t3.medium`) |
|----|--------|-------------|------------------------------|
| **BM-PL0** | adapter-ready | ~100/s | PASS |
| **BM-PL1** | adapter-ready | ~1k/s | **~999/s** PASS |
| **BM-PL2** | adapter-ready | ~10k/s | **~10k/s** PASS |
| **BM-PL3** | adapter-ready | ~100k/s | **~12.4k/s** PASS (below target — mem ceiling) |

### Fleet scale (BM-PF*)

Requires `--storage nats` + broker cluster ([`infra/broker/README.md`](../infra/broker/README.md)).

| ID | Status | Workload | Results (`aws-c6i-large` in-VPC) |
|----|--------|----------|-----------------------------------|
| **BM-PF0** | broker-ready | 2-node smoke | PASS — 100 ops/s |
| **BM-PF1** | broker-ready | N-subscriber fanout | PASS |
| **BM-PF2** | broker-ready | 4×250/s keyed | **1000 ops/s** PASS in-VPC (2026-07-07) |
| **BM-PF3** | broker-ready | Sustained 1k/s × 60s | **993 ops/s** PASS in-VPC (2026-07-07) |
| **BM-PF4** | broker-ready | Multi-stream sustained | **1000 ops/s** PASS in-VPC (2026-07-07) |
| **BM-PFH** | broker-ready | Single-topic firehose | **~90k/s** per embed; **218k/s** aggregate (4 embeds) |
| **BM-PFS** | broker-ready | 10k/s × 30s + 4 fanout subs | **972 ops/s**, err 0% in-VPC (2026-07-07) |
| **BM-PFE** | broker-ready | 1k/s × 60s + executor | **1000 ops/s**, err 0% in-VPC (2026-07-07) |

### Consumer groups (BM-PG*)

| ID | Status | Results |
|----|--------|---------|
| **BM-PG0** | group-ready | CI on `mem` — 100% coverage |
| **BM-PG1** | group-ready | CI on `mem` |
| **BM-PG2** | group-ready | `mem` + NATS bench PASS in-VPC (post-restart verify) |

### Broker validation (BM-PB*)

| ID | Workload | AWS results |
|----|----------|------------|
| **BM-PB0** | Publish smoke | Local smoke |
| **BM-PB1** | Queue group balance | Local smoke |
| **BM-PB2** | Fanout p99 vs N | `dev-wsl` smoke |
| **BM-PB3** | Replay after kill | Local smoke |
| **BM-PB4** | Scaling sweep (informational) | N=1 **1000 ops/s**; N=3 **958 ops/s** (ratio **0.96×**); per-run error gate only |
| **BM-PB5** | Kill one broker node | **100 ops/s** PASS failover |

---

## BM-PFH results (consolidated)

### Multi-embed ingress sweep (primary)

Recommended topology (4 brokers, `stream_shards=4`, `rep=1`):

| bench_clients | aggregate peak | vs bc=1 |
|---------------|----------------|---------|
| 1 | **94,327 ops/s** | 1.00× |
| 2 | **126,077 ops/s** | 1.34× |
| 4 | **218,184 ops/s** | 2.31× |

Artifact: `scaling-curve-aws-c6i-large-nats-firehose-multibench.json`

### Stream-sharded cluster (single embed)

Primary row: `stream_seq/ack=1/256 pubs/100k target`

| broker_nodes | stream_shards | peak achieved | vs N=1 |
|--------------|---------------|---------------|--------|
| 1 | 1 | **87,553 ops/s** | 1.00× |
| 2 | 2 | **65,261 ops/s** | 0.75× |
| 4 | 4 | **90,030 ops/s** | 1.03× |

Artifact: `scaling-curve-aws-c6i-large-nats-firehose-sharded.json`

### Anti-pattern: single replicated stream

`stream_shards=1`, `num_replicas=N` on one JetStream stream — do not use for write-heavy ingress:

| broker_nodes | peak achieved | vs N=1 |
|--------------|---------------|--------|
| 1 | **87,553 ops/s** | 1.00× |
| 2 | 41,644 ops/s | 0.48× |
| 4 | 50,000 ops/s | 0.57× |

Artifact: `scaling-curve-aws-c6i-large-nats-firehose.json`

### Commands

```bash
# Local smoke
infra/broker/scripts/run-pfh-sweep.sh

# AWS authoritative
infra/aws/broker-fleet/scripts/run-pfh-campaign-aws.sh
infra/aws/broker-fleet/scripts/run-pfh-phase3-campaign-aws.sh
BENCH_COUNT=4 infra/aws/broker-fleet/scripts/run-pfh-multibench-campaign-aws.sh

# Projection
cargo run -p photon-bench --features nats -- scaling-curve \
  --hardware aws-c6i-large --storage nats --primary-row --stream-shards 1 \
  --reports-dir profiling/photon-bench/reports

cargo run -p photon-bench --features nats -- scaling-curve \
  --hardware aws-c6i-large --storage nats --primary-row --match-broker-nodes \
  --reports-dir profiling/photon-bench/reports

cargo run -p photon-bench --features nats -- scaling-curve \
  --hardware aws-c6i-large --storage nats --primary-row --multibench-ladder \
  --reports-dir profiling/photon-bench/reports
```

Env knobs: `PHOTON_NATS_REPLAY_CURSOR`, `PHOTON_NATS_SYNC_ACK`, `PHOTON_NATS_STREAM_SHARDS`, `PHOTON_BENCH_CLIENT_INDEX/COUNT`, `--publishers`, `--nodes`.

---

## BM-PFH results — Kafka (2026-07-07)

**Decision-grade primary row:** `stream_seq`, `sync_ack=1`, **256** publishers, **100k** target, `PHOTON_KAFKA_MAX_INFLIGHT=256`, crypto off, in-VPC on `aws-c6i-large`. Scaling curves regenerated with `--primary-row`.

### Baseline ladder (`topic_shards=1`, primary row)

| broker_nodes | peak achieved | vs N=1 |
|--------------|---------------|--------|
| 1 | **1,146 ops/s** | 1.00× |
| 2 | **1,660 ops/s** | 1.45× |
| 4 | **1,453 ops/s** | 1.27× |

Artifact: `scaling-curve-aws-c6i-large-kafka-firehose.json`

### Sharded ladder (`topic_shards=N`, primary row)

| broker_nodes | peak achieved | vs N=1 |
|--------------|---------------|--------|
| 1 | **1,146 ops/s** | 1.00× |
| 2 | **1,590 ops/s** | 1.39× |
| 4 | **2,655 ops/s** | 2.32× |

Artifact: `scaling-curve-aws-c6i-large-kafka-firehose-sharded.json`

### Multi-embed (4 brokers / 4 shards, primary row)

| bench_clients | aggregate peak |
|---------------|----------------|
| 1 | **3,242 ops/s** |
| 2 | **5,039 ops/s** |

bc=4 incomplete (broker teardown mid-run). Artifact: `scaling-curve-aws-c6i-large-kafka-firehose-multibench.json`. Reports: `bm-pfh-kafka-n4-sh4-bc*-aggregate-*-aws.json`.

### Best swept cell (exploratory — not decision-grade)

Publisher / rate sweeps found higher single-topic rates at lighter cells (`p128` / `r10000`), e.g. baseline N=1 **2,258**, N=2 **2,244**, N=4 **1,965** ops/s. Prefer primary-row tables above when comparing to NATS/Fluvio.

### Commands

```bash
# Local smoke
infra/broker/scripts/run-pfh-sweep-kafka.sh

# AWS authoritative
cd infra/aws/kafka-fleet
./provision.sh && ./deploy-bench.sh
PFH_SWEEP_MODE=baseline ./scripts/run-pfh-campaign-aws.sh
PFH_SWEEP_MODE=sharded ./scripts/run-pfh-phase3-campaign-aws.sh
BENCH_COUNT=4 ./scripts/run-pfh-multibench-campaign-aws.sh
TEARDOWN_BENCH=0 ./scripts/teardown.sh   # keep bench for Fluvio

cargo run -p photon-bench --features kafka -- scaling-curve \
  --hardware aws-c6i-large --storage kafka --primary-row --stream-shards 1 \
  --reports-dir profiling/photon-bench/reports
cargo run -p photon-bench --features kafka -- scaling-curve \
  --hardware aws-c6i-large --storage kafka --primary-row --match-broker-nodes \
  --reports-dir profiling/photon-bench/reports
cargo run -p photon-bench --features kafka -- scaling-curve \
  --hardware aws-c6i-large --storage kafka --primary-row --multibench-ladder \
  --reports-dir profiling/photon-bench/reports
```

Env knobs: `PHOTON_KAFKA_REPLAY_CURSOR`, `PHOTON_KAFKA_SYNC_ACK`, `PHOTON_KAFKA_TOPIC_SHARDS`, `PHOTON_BENCH_TOPIC_SHARDS`.

---

## BM-PFH results — Fluvio (2026-07-07)

Primary row: same as NATS/Kafka. In-VPC on `aws-c6i-large`; SC on `broker-single`, SPUs on fleet hosts.

| sweep | N / shards | peak achieved |
|-------|------------|---------------|
| Baseline primary | N=1, sh=1 | **99,144 ops/s** |
| Multibench bc=1 | N=4, sh=4 | **92,573 ops/s** |
| Multibench bc=2 | N=4, sh=4 | **199,993 ops/s** aggregate |
| Multibench bc=4 | N=4, sh=4 | **399,997 ops/s** aggregate |

N=2/N=4 baseline+sharded ladders not run (multibench on fixed 4-SPU cluster is authoritative for fleet sizing).

Artifacts: `scaling-curve-aws-c6i-large-fluvio-firehose-multibench.json`

### Commands

```bash
# Local smoke
infra/broker/scripts/run-pfh-sweep-fluvio.sh

# AWS authoritative
cd infra/aws/fluvio-fleet
REUSE_BENCH=1 ./provision.sh   # or fresh provision
./scripts/run-pfh-campaign-aws.sh
./scripts/run-pfh-phase3-campaign-aws.sh
BENCH_COUNT=4 ./scripts/run-pfh-multibench-campaign-aws.sh

cargo run -p photon-bench --features fluvio -- scaling-curve \
  --hardware aws-c6i-large --storage fluvio --primary-row --multibench-ladder \
  --reports-dir profiling/photon-bench/reports
```

Env knobs: `PHOTON_FLUVIO_REPLAY_CURSOR`, `PHOTON_FLUVIO_SYNC_ACK`, `PHOTON_FLUVIO_TOPIC_SHARDS`, `PHOTON_BENCH_TOPIC_SHARDS`.

---

## Cross-broker PFH comparison (primary row, `aws-c6i-large`)

| Broker | Single embed (4b/4s) | Best aggregate | Notes |
|--------|----------------------|----------------|-------|
| NATS | 94,327 | **218,184** (bc=4) | Recommended production topology |
| Fluvio | 92,573 (bc=1) | **399,997** (bc=4) | **1.83×** NATS at bc=4; near-linear multibench |
| Kafka | 2,655 (sharded N=4) | **5,039** (bc=2 only) | Path broker/client-bound; bc=4 campaign paused |

---

## Recommended matrix slices

| Slice | Experiments | Purpose |
|-------|-------------|---------|
| **adapter-minimal** | BM-P0, BM-P1, BM-PG0, BM-PG2 | `mem` publish + delivery + group verify |
| **broker-spike** | BM-PB0–PB3 | NATS functional validation |
| **broker-fleet** | BM-PF*, BM-P6, BM-PFS, BM-PFE, BM-PB4–PB5, BM-PFH, BM-PG* | AWS in-VPC authoritative |

```bash
cargo run -p photon-bench --features nats -- matrix --slice adapter-minimal --storage mem --telemetry off
cargo run -p photon-bench --features nats -- matrix --slice broker-spike --storage nats --telemetry off
cargo run -p photon-bench --features nats -- matrix --slice broker-fleet --storage nats --telemetry off
```

---

## Broker fleet campaigns

See [`infra/aws/broker-fleet/README.md`](../infra/aws/broker-fleet/README.md).

```bash
cd infra/aws/broker-fleet
./provision.sh && ./deploy-brokers.sh && ./deploy-bench.sh
export PHOTON_AWS_USE_PUBLIC_IPS=0
./scripts/run-in-vpc-validation.sh
./scripts/teardown.sh
```

**Do not** run rate-target experiments from a laptop over public NATS URLs.

### Environment

| Variable | Effect |
|----------|--------|
| `PHOTON_NATS_URL` | Comma-separated client URLs |
| `PHOTON_NATS_STREAM_SHARDS` | JetStream stream count; set to broker count on fleet |
| `PHOTON_NATS_REPLICAS` | Per-stream replicas; use `1` per shard when sharded |
| `PHOTON_BENCH_CLIENT_INDEX/COUNT` | Multi-embed ingress sweep |
| `PHOTON_BENCH_RESOURCE_PROFILE` | RSS/CPU in reports (auto on `aws-*`) |

---

## Question coverage matrix

| Question (PERFORMANCE_STUDY §1.3) | Primary experiments |
|-----------------------------------|---------------------|
| RQ1 Publish latency | BM-P0, BM-P9 |
| RQ2 Fanout scaling | BM-P1, BM-P2, BM-PF1 |
| RQ3 Backlog bounded | BM-P3, BM-PL* |
| RQ4 Replay throughput | BM-P4, BM-P7 |
| RQ5 Keyed filter | BM-P5 |
| RQ6 Broker fanout | BM-P6, BM-PB2 |
| RQ7 Handler checkpoint | BM-P7 |
| RQ8 Executor / DLQ | BM-P8 |
| RQ9 Broker fleet ingress | BM-PFH, BM-PF2, BM-PF4 (gates); BM-PB4 (informational sweep) |

---

## Criterion microbenches (BM-CRIT-*)

Local CPU-path microbenches under `photon-backend/benches/`. Not fleet ingress; isolate crypto, shard routing, and Tokio dispatch scaffolding. Prefer AWS via `infra/aws/sqlite-smoke/scripts/run-remote-criterion.sh` (writes `profiling/photon-bench/reports/criterion-*-aws.json`). Prefer `profile.bench` / `profile.profiling` from the workspace root `Cargo.toml` when collecting samples with debug info.

| ID | Status | Workload | Primary metric | Notes |
|----|--------|----------|----------------|-------|
| **BM-CRIT-CRYPTO** | microbench | Envelope encrypt / decrypt / roundtrip | ns/op | `benches/envelope_crypto.rs` |
| **BM-CRIT-SHARD** | microbench | `shard_id`, `routing_key` (publish key + keyed extract + stringify) | ns/op | `benches/shard_routing.rs` |
| **BM-CRIT-DISPATCH** | stub | Empty-task `tokio::spawn` join-all vs `JoinSet` | ns/batch | `benches/dispatch_stub.rs` — Tokio baseline only; not Photon executor |

```bash
# On AWS smoke host (preferred for check-in):
# infra/aws/sqlite-smoke/scripts/run-remote-criterion.sh

export CARGO_BUILD_JOBS=1

cargo bench -p photon-backend --bench envelope_crypto --features runtime
cargo bench -p photon-backend --bench shard_routing --features runtime
cargo bench -p photon-backend --bench dispatch_stub --features runtime

# Optional local Criterion compare (baselines live under target/; do not commit them):
cargo bench -p photon-backend --features runtime -- --save-baseline local
cargo bench -p photon-backend --features runtime -- --baseline local
```

Methodology and recorded study numbers: [`PERFORMANCE_STUDY.md` §10](PERFORMANCE_STUDY.md#10-microbenchmarks--hot-path-baselines-2026-07-12).

### Mem-tier performance gates (CI / lab)

| Gate | Experiment | How |
|------|------------|-----|
| Smoke pass | `bm-p0` (CI `bench-smoke`) | JSON `"pass": true` |
| Latency / sustained publish | `bm-p0`, `bm-pl2` | Targets and methodology in [`PERFORMANCE_STUDY.md`](PERFORMANCE_STUDY.md); do not duplicate snapshot numbers here |

See [`CONTRIBUTING.md`](../CONTRIBUTING.md#performance-targets-mem-tier).

---

## Run (mem tier)

```bash
export CARGO_TARGET_DIR=target-photon-bench

cargo run -p photon-bench -- run --experiment bm-p0 --storage mem --telemetry off --ops 5000
cargo run -p photon-bench -- run --experiment bm-pl2 --storage mem --telemetry off --ops 60
cargo run -p photon-bench -- run --experiment bm-pg0 --storage mem --telemetry off
```

**Reports:** tracked authoritative runs use `profiling/photon-bench/reports/*-aws.json`. Local smoke writes to `profiling/photon-bench/reports-local/` (gitignored).
