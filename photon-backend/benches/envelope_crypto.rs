//! Criterion microbench: transport envelope encrypt / decrypt (BM-CRIT-CRYPTO).
//!
//! Run: `cargo bench -p photon-backend --bench envelope_crypto --features runtime`

#![allow(clippy::unwrap_used, clippy::expect_used)]
#![allow(missing_docs)]

use criterion::{criterion_group, criterion_main, Criterion};
use photon_backend::TransportCrypto;
use serde_json::json;

fn bench_encrypt_decrypt(c: &mut Criterion) {
    let crypto = TransportCrypto::from_bytes(*b"photon-dev-transport-key-32bytes");
    let actor = json!({"System": {"operation": "bench"}});
    let payload = json!({"order_id": "ord-1", "amount_cents": 9900u64});

    c.bench_function("envelope_encrypt", |b| {
        b.iter(|| crypto.encrypt(&actor, &payload).expect("encrypt"));
    });

    let ciphertext = crypto.encrypt(&actor, &payload).expect("encrypt once");
    c.bench_function("envelope_decrypt", |b| {
        b.iter(|| crypto.decrypt(&ciphertext).expect("decrypt"));
    });

    c.bench_function("envelope_roundtrip", |b| {
        b.iter(|| {
            let ct = crypto.encrypt(&actor, &payload).expect("encrypt");
            crypto.decrypt(&ct).expect("decrypt")
        });
    });
}

criterion_group!(benches, bench_encrypt_decrypt);
criterion_main!(benches);
