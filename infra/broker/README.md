# Broker lab bootstrap

Start broker clusters for BM-PB* / BM-PF* experiments.

Disk-heavy bench runs are best executed on a cloud VM (e.g. AWS `t3.medium`) with `CARGO_TARGET_DIR=/tmp/photon-target` and `CARGO_INCREMENTAL=0`.

## Single-node NATS (broker-spike BM-PB0–PB3)

```bash
docker run -d --name photon-nats -p 4222:4222 nats:2.10 -js
export PHOTON_NATS_URL=nats://127.0.0.1:4222
export PHOTON_NATS_STREAM=photon
export PHOTON_NATS_RETENTION=15m
export PHOTON_NATS_REPLICAS=1

cargo run -p photon-bench --features nats -- run --experiment bm-pb0 --storage nats --telemetry off
```

## 3-node NATS JetStream cluster (BM-PF*, BM-PB4/PB5)

```bash
cd infra/broker
chmod +x scripts/*.sh
./scripts/up.sh
source scripts/export-env.sh

cargo run -p photon-bench --features nats -- run \
  --experiment bm-pf0 --storage nats --topology broker-cluster --telemetry off
```

### Scripts

| Script | Purpose |
|--------|---------|
| `scripts/up.sh` | Start 3-node cluster |
| `scripts/down.sh` | Stop cluster (`--wipe` removes volumes) |
| `scripts/single-node.sh` | Single-node profile (PB4 baseline) |
| `scripts/export-env.sh` | Export `PHOTON_NATS_*` for cluster |
| `scripts/kill-node.sh N` | Stop `photon-nats-N` (PB5 failover) |
| `scripts/run-pb4-sweep.sh` | 1-node vs 3-node sweep — **informational ratio**; gates on per-run `error_rate` only |
| `scripts/run-pb5.sh` | Failover kill at t=22s during 45s sustained publish |

**Ports:** client URLs `4222`, `4225`, `4224` (node 2 uses `4225` when `4223` is reserved on WSL). Monitoring: `8222`, `8225`, `8224`.

### Fleet matrix

```bash
source infra/broker/scripts/export-env.sh
cargo run -p photon-bench --features nats -- matrix --slice broker-fleet --storage nats --telemetry off
```

## Kafka / Fluvio

Single-node Kafka (KRaft) for adapter smoke and e2e:

```bash
cd infra/broker
chmod +x scripts/*.sh
./scripts/kafka-single.sh
source scripts/export-kafka-env.sh
export PHOTON_KAFKA_TOPIC_SHARDS=1

cargo test -p photon-e2e --features kafka -- --ignored
cargo test -p photon-backend-kafka --test kafka_contract -- --ignored
```

Fluvio single-node lab (SC + SPU on localhost:9103):

```bash
cd infra/broker
chmod +x scripts/*.sh
./scripts/fluvio-single.sh
source scripts/export-fluvio-env.sh

cargo test -p photon-e2e --features fluvio -- --ignored
cargo test -p photon-backend-fluvio --test fluvio_contract -- --ignored --test-threads=1
```

AWS smoke gate (t3.medium) and multi-EC2 fleet validation run on AWS (operator campaign). Local compose is for smoke only. Authoritative PF*/PB5 runs use separate EC2 NATS hosts; PB4 ratio is informational. Run fleet validation in-VPC (private IPs) from the bench or broker host.
