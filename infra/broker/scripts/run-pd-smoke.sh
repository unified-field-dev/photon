#!/usr/bin/env bash
# Local PD harness smoke: encrypted checkpoint delivery on mem and sqlite.
# AWS NATS 4-shard campaign tooling is maintained separately and isn't part of this repo.
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
REPO="$(cd "$ROOT/.." && pwd)"
REPORTS="${REPO}/profiling/photon-bench/reports-local"
mkdir -p "$REPORTS"

export PHOTON_TRANSPORT_KEY="${PHOTON_TRANSPORT_KEY:-cGhvdG9uLWRldi10cmFuc3BvcnQta2V5LTMyYnl0ZXM=}"
# PD requires envelope crypto. Do not copy PFH's PHOTON_BENCH_CRYPTO=0.
unset PHOTON_BENCH_CRYPTO || true
HARDWARE="${PHOTON_BENCH_HARDWARE:-dev-wsl}"
# --ops is duration_secs for PD (1s smoke; default bench duration is 30s).
DURATION="${PD_SMOKE_DURATION:-1}"

cd "$REPO"

echo "=== photon-testkit checkpoint tests (mem) ==="
cargo test -p photon-testkit checkpoint -- --nocapture

echo "=== photon-bench PD registry / serde / crypto-gate ==="
cargo test -p photon-bench pd

echo "=== BM-PD0 mem ${DURATION}s ==="
cargo run -p photon-bench -- run \
  --experiment bm-pd0 --storage mem --telemetry off --ops "$DURATION" \
  --hardware "$HARDWARE" \
  --report "$REPORTS/bm-pd0-mem-isolated-lab-off-${HARDWARE}.json"

echo "=== BM-PD0 sqlite ${DURATION}s ==="
cargo run -p photon-bench --features sqlite -- run \
  --experiment bm-pd0 --storage sqlite --telemetry off --ops "$DURATION" \
  --hardware "$HARDWARE" \
  --report "$REPORTS/bm-pd0-sqlite-isolated-lab-off-${HARDWARE}.json"

echo "=== BM-PD1 mem ${DURATION}s (fanout) ==="
cargo run -p photon-bench -- run \
  --experiment bm-pd1 --storage mem --telemetry off --ops "$DURATION" \
  --hardware "$HARDWARE" \
  --report "$REPORTS/bm-pd1-mem-isolated-lab-off-${HARDWARE}.json"

echo "=== BM-PD2 mem ${DURATION}s (capacity cell, offered 50) ==="
PHOTON_BENCH_OFFERED_RATE=50 cargo run -p photon-bench -- run \
  --experiment bm-pd2 --storage mem --telemetry off --ops "$DURATION" \
  --hardware "$HARDWARE" \
  --report "$REPORTS/bm-pd2-mem-isolated-lab-off-${HARDWARE}.json"

echo "PD local smoke complete. Reports in $REPORTS"
