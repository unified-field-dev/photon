//! Criterion microbench: `shard_id` + `routing_key` helpers (BM-CRIT-SHARD).
//!
//! Run: `cargo bench -p photon-backend --bench shard_routing --features runtime`

#![allow(clippy::unwrap_used, clippy::expect_used)]
#![allow(missing_docs)]

use criterion::{black_box, criterion_group, criterion_main, Criterion};
use photon_backend::{routing_key, shard_id, shard_storage_key};
use serde_json::json;

fn bench_shard_routing(c: &mut Criterion) {
    let payload = json!({"customer_id": "alice", "amount_cents": 100u64});
    let event_id = "evt-bench-0001";

    c.bench_function("shard_id", |b| {
        b.iter(|| shard_id(black_box("alice"), black_box(32)));
    });

    c.bench_function("shard_storage_key", |b| {
        b.iter(|| shard_storage_key(black_box(7)));
    });

    c.bench_function("routing_key_publish", |b| {
        b.iter(|| {
            routing_key(
                black_box(Some("alice")),
                black_box(&payload),
                black_box(Some("customer_id")),
                black_box(event_id),
            )
        });
    });

    // Keyed extraction path: no explicit publish key → field lookup + stringify.
    c.bench_function("routing_key_keyed_extract", |b| {
        b.iter(|| {
            routing_key(
                black_box(None),
                black_box(&payload),
                black_box(Some("customer_id")),
                black_box(event_id),
            )
        });
    });

    let numeric_payload = json!({"tenant": 42});
    c.bench_function("routing_key_stringify_non_string", |b| {
        b.iter(|| {
            routing_key(
                black_box(None),
                black_box(&numeric_payload),
                black_box(Some("tenant")),
                black_box(event_id),
            )
        });
    });
}

criterion_group!(benches, bench_shard_routing);
criterion_main!(benches);
