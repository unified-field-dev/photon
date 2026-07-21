//! Two-Photon cross-node fanout lab harness (BM-P6 / broker fleet).

use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{bail, Result};
use futures::StreamExt;

use crate::bootstrap::BootstrapSession;
use crate::fixtures::smoke_actor_json;
use crate::matrix::StorageAdapter;
use crate::scenario::{ScenarioSpec, ScenarioStep};
use crate::shared_store;

/// Events published during a cross-node fanout lab run.
pub const FANOUT_PUBLISH_COUNT: u32 = 100;

/// Outcome of a cross-node fanout lab run.
#[derive(Debug, Clone)]
pub struct CrossNodeFanoutResult {
    /// Deliveries observed on the subscriber node.
    pub deliveries: u32,
    /// Wall time from first publish to delivery completion (ms).
    pub delivery_wait_ms: f64,
    /// Whether broker-native fanout was used.
    pub push_enabled: bool,
}

/// Result of applying a `CrossNodeFanout` scenario step.
#[derive(Debug, Clone)]
pub enum CrossNodeStepOutcome {
    /// Fanout delivered the expected event count.
    Ok {
        /// Wall time from first publish to delivery completion (ms).
        delivery_wait_ms: f64,
    },
    /// Fanout completed but delivery count was short.
    FailDelivery {
        /// Deliveries observed.
        deliveries: u32,
        /// Failure message.
        error: String,
    },
    /// Lab setup or publish failed.
    Err {
        /// Failure message.
        error: String,
    },
}

/// Run publisher + subscriber Photon instances (shared broker or in-proc mem lab).
///
/// # Errors
///
/// Returns an error if the operation fails.
pub async fn run_cross_node_fanout_lab(
    session: &BootstrapSession,
    topic: &str,
) -> Result<CrossNodeFanoutResult> {
    if session.matrix().storage == StorageAdapter::Mem
        || session.matrix().storage == StorageAdapter::Sqlite
    {
        // In-proc mem/sqlite: two Photon handles share the same storage port via separate builds
        // (each gets its own port when built separately — lab uses single-process fanout only).
    } else if let Some(reason) = shared_store::shared_store_skip_reason(session.matrix()) {
        bail!("{reason}");
    }

    let subscriber = session.build_photon()?;
    let publisher = session.build_photon()?;
    let deliveries = Arc::new(AtomicU32::new(0));
    let count = Arc::clone(&deliveries);

    let topic_owned = topic.to_string();
    let sub_task = tokio::spawn(async move {
        let mut stream = subscriber.subscribe(&topic_owned, None, None);
        while let Some(item) = stream.next().await {
            if item.is_ok() {
                count.fetch_add(1, Ordering::Relaxed);
            }
        }
    });

    tokio::time::sleep(crate::shared_store::subscribe_attach_warmup()).await;
    let start = Instant::now();
    for i in 0..FANOUT_PUBLISH_COUNT {
        publisher
            .publish(topic, None, smoke_actor_json(), serde_json::json!({"i": i}))
            .await?;
    }

    let deadline = start + Duration::from_secs(30);
    while deliveries.load(Ordering::Relaxed) < FANOUT_PUBLISH_COUNT && Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let delivery_wait_ms = start.elapsed().as_secs_f64() * 1000.0;
    sub_task.abort();

    Ok(CrossNodeFanoutResult {
        deliveries: deliveries.load(Ordering::Relaxed),
        delivery_wait_ms,
        push_enabled: !matches!(
            session.matrix().storage,
            StorageAdapter::Mem | StorageAdapter::Sqlite
        ),
    })
}

/// Apply a cross-node fanout scenario step.
pub async fn apply_cross_node_fanout_step(
    session: &BootstrapSession,
    spec: &ScenarioSpec,
) -> CrossNodeStepOutcome {
    let topic = spec.steps.iter().find_map(|step| match step {
        ScenarioStep::CrossNodeFanout { topic } => Some(topic.as_str()),
        _ => None,
    });

    let Some(topic) = topic else {
        return CrossNodeStepOutcome::Err {
            error: "expected CrossNodeFanout step in scenario".into(),
        };
    };

    match run_cross_node_fanout_lab(session, topic).await {
        Ok(r) if r.deliveries >= FANOUT_PUBLISH_COUNT => CrossNodeStepOutcome::Ok {
            delivery_wait_ms: r.delivery_wait_ms,
        },
        Ok(r) => CrossNodeStepOutcome::FailDelivery {
            deliveries: r.deliveries,
            error: format!(
                "expected {FANOUT_PUBLISH_COUNT} deliveries, got {}",
                r.deliveries
            ),
        },
        Err(e) => CrossNodeStepOutcome::Err {
            error: e.to_string(),
        },
    }
}
