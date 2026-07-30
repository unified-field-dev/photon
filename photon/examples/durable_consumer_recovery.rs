//! Durable consumer recovery — `durable = "…"` checkpoint + restart resume (`SQLite`-backed).
//!
//! Single process, two phases simulate two independent runs sharing one durable checkpoint
//! store: phase 1 processes a batch of events and then "crashes" (its `Photon` handle and
//! executor tasks are dropped). Phase 2 opens a **fresh** `SqliteStoragePort` on the **same**
//! database file and restarts the identical `#[subscribe(durable = "recovery-worker")]`
//! handler — the executor loads the last committed checkpoint seq and resumes from there, so
//! phase 1's events are never redelivered.
//!
//! The same `durable` name + `ReplayCursor::StreamSeq` contract applies to the brokered
//! adapters (`nats_worker`, `kafka_worker`, `fluvio_worker`): a worker process crash-restarting
//! against the same broker/stream resumes exactly the way phase 2 does here, just backed by
//! `JetStream` offsets / Kafka offsets / Fluvio offsets instead of a `SQLite` table.
//!
//! Run: `cargo run -p uf-photon --example durable_consumer_recovery --features runtime,sqlite`
//!
//! Optional: `PHOTON_SQLITE_PATH` (default `/tmp/photon-example-durable.db`; this example
//! removes any pre-existing file so phase 1 always starts from a clean checkpoint store).
#![allow(missing_docs)]
#![allow(clippy::unused_async, clippy::used_underscore_binding)]
#![allow(clippy::unwrap_used, clippy::expect_used)]
use std::collections::HashSet;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use photon::{subscribe, topic, Actor, JsonIdentityFactory, Photon, SqliteStoragePort};

#[topic(name = "examples.durable.ticks")]
pub struct Tick {
    pub n: u32,
}

static HANDLED: AtomicU32 = AtomicU32::new(0);

fn seen_seqs() -> &'static Mutex<HashSet<u32>> {
    static SEEN: OnceLock<Mutex<HashSet<u32>>> = OnceLock::new();
    SEEN.get_or_init(|| Mutex::new(HashSet::new()))
}

// `durable = "recovery-worker"` is the checkpoint owner name: the executor loads its last
// committed seq at startup (`ReplayCursor::StreamSeq`-equivalent semantics) and only delivers
// events after it — so phase 2 below resumes from that checkpoint.
#[subscribe(topic = "examples.durable.ticks", durable = "recovery-worker")]
async fn on_tick(_actor: Box<dyn Actor>, event: Tick) -> photon::Result<()> {
    let first_delivery = seen_seqs().lock().unwrap().insert(event.n);
    HANDLED.fetch_add(1, Ordering::SeqCst);
    if first_delivery {
        tracing::info!(n = event.n, "durable handler processed tick");
    } else {
        tracing::warn!(
            n = event.n,
            "durable handler re-delivered tick (unexpected)"
        );
    }
    Ok(())
}

async fn wait_for_handled(target: u32, timeout: Duration) -> anyhow::Result<()> {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        if HANDLED.load(Ordering::SeqCst) >= target {
            return Ok(());
        }
        if tokio::time::Instant::now() >= deadline {
            anyhow::bail!(
                "timeout waiting for {target} handled events (have {})",
                HANDLED.load(Ordering::SeqCst)
            );
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt().with_env_filter("info").init();

    let path = std::env::var("PHOTON_SQLITE_PATH")
        .unwrap_or_else(|_| "/tmp/photon-example-durable.db".into());
    let _ = std::fs::remove_file(&path); // fresh checkpoint store for a repeatable demo

    // --- Phase 1: "first process" — handle a batch, force-flush the checkpoint, then "crash". ---
    {
        let port = Arc::new(SqliteStoragePort::open(&path).await?);
        let photon = Photon::builder()
            .storage_port(port)
            .auto_registry()
            .build()?;
        photon.start_executor(Arc::new(JsonIdentityFactory))?;
        tokio::time::sleep(Duration::from_millis(50)).await; // let the executor attach first

        for n in 0..3 {
            Tick { n }.publish_on(&photon).await?;
        }
        wait_for_handled(3, Duration::from_secs(5)).await?;

        // Checkpoint writes are coalesced (`PHOTON_CHECKPOINT_FLUSH_MS`, default 500ms) —
        // flush explicitly so the "restart" below deterministically observes the committed seq
        // instead of racing the background flush timer.
        photon
            .runtime()
            .executor_services
            .checkpoint_coalescer
            .flush()
            .await?;

        tracing::info!(
            handled = HANDLED.load(Ordering::SeqCst),
            path = %path,
            "phase 1: checkpoint committed to sqlite; simulating a process crash"
        );
    } // `photon` (executor tasks + sqlite pool) drops here — a real restart would exit the process.

    // --- Phase 2: "restarted process" — fresh port on the same file, same durable name. ---
    let port = Arc::new(SqliteStoragePort::open(&path).await?);
    let photon = Photon::builder()
        .storage_port(port)
        .auto_registry()
        .build()?;
    photon.start_executor(Arc::new(JsonIdentityFactory))?;
    tokio::time::sleep(Duration::from_millis(50)).await; // let the executor attach + replay first

    for n in 3..6 {
        Tick { n }.publish_on(&photon).await?;
    }
    wait_for_handled(6, Duration::from_secs(5)).await?;

    let unique_delivered = u32::try_from(seen_seqs().lock().unwrap().len()).unwrap_or(u32::MAX);
    let redelivered = 6 - unique_delivered;
    if redelivered > 0 {
        anyhow::bail!(
            "durable_consumer_recovery FAILED: {redelivered} tick(s) redelivered after restart"
        );
    }

    tracing::info!(
        handled = HANDLED.load(Ordering::SeqCst),
        "phase 2: resumed from checkpoint with no redelivery — durable_consumer_recovery OK"
    );
    Ok(())
}
