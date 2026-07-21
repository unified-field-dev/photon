//! Criterion stub: empty-task spawn vs `JoinSet` (BM-CRIT-DISPATCH).
//!
//! Not a Photon executor bench — isolates Tokio scheduling cost for comparison baselines.
//! Run: `cargo bench -p photon-backend --bench dispatch_stub --features runtime`

#![allow(clippy::unwrap_used, clippy::expect_used)]
#![allow(missing_docs)]

use criterion::{criterion_group, criterion_main, Criterion};
use tokio::runtime::Runtime;
use tokio::task::JoinSet;

const TASKS: usize = 64;

fn bench_dispatch_stub(c: &mut Criterion) {
    let rt = Runtime::new().expect("tokio runtime");

    c.bench_function("tokio_spawn_empty_join_all", |b| {
        b.to_async(&rt).iter(|| async {
            let handles: Vec<_> = (0..TASKS).map(|_| tokio::spawn(async {})).collect();
            for h in handles {
                h.await.expect("join");
            }
        });
    });

    c.bench_function("tokio_joinset_empty", |b| {
        b.to_async(&rt).iter(|| async {
            let mut set = JoinSet::new();
            for _ in 0..TASKS {
                set.spawn(async {});
            }
            while set.join_next().await.is_some() {}
        });
    });
}

criterion_group!(benches, bench_dispatch_stub);
criterion_main!(benches);
