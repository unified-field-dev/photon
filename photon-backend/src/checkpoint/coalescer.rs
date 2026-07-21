//! Coalesced checkpoint persistence (flush on interval / N events).

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::sync::OnceLock;
use std::time::Duration;

use tokio::sync::Mutex;
use tokio::task::JoinHandle;

use crate::retention::{PartitionReclaim, TopicPartition};
use crate::storage::StoragePort;

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
struct CoalesceKey {
    subscription_name: String,
    topic_name: String,
    topic_key: Option<String>,
}

/// Tracks high-water seq per subscription partition; flushes periodically.
pub struct CheckpointCoalescer {
    port: Arc<dyn StoragePort>,
    pending: Arc<Mutex<HashMap<CoalesceKey, i64>>>,
    flush_every: u32,
    reclaimer: Arc<OnceLock<Arc<dyn PartitionReclaim>>>,
    _task: JoinHandle<()>,
}

impl CheckpointCoalescer {
    /// Start a background flush loop bound to `port`.
    #[must_use]
    pub fn new(port: Arc<dyn StoragePort>) -> Self {
        let pending = Arc::new(Mutex::new(HashMap::new()));
        let reclaimer = Arc::new(OnceLock::new());
        let flush_every = checkpoint_flush_every();
        let flush_interval = Duration::from_millis(checkpoint_flush_ms());
        let task_pending = Arc::clone(&pending);
        let task_port = Arc::clone(&port);
        let task_reclaimer = Arc::clone(&reclaimer);
        let task = tokio::spawn(async move {
            let mut ticker = tokio::time::interval(flush_interval);
            loop {
                ticker.tick().await;
                let reclaimer = task_reclaimer.get().cloned();
                if let Err(e) = flush_map(&task_port, &task_pending, reclaimer).await {
                    tracing::warn!(error = %e, "checkpoint coalescer flush failed");
                    crate::instrumentation::log_ops(
                        "checkpoint",
                        "coalescer_flush",
                        "checkpoint coalescer flush failed",
                        "",
                        "",
                        &e.to_string(),
                    );
                }
            }
        });
        Self {
            port,
            pending,
            flush_every,
            reclaimer,
            _task: task,
        }
    }

    /// Wire opportunistic reclaim after checkpoint commits.
    pub fn attach_reclaimer(&self, reclaimer: Arc<dyn PartitionReclaim>) {
        let _ = self.reclaimer.set(reclaimer);
    }

    /// Record delivered seq; may trigger immediate flush when batch threshold hit.
    ///
    /// # Errors
    ///
    /// Returns an error if the operation fails.
    pub async fn record(
        &self,
        subscription_name: &str,
        topic_name: &str,
        topic_key: Option<&str>,
        seq: i64,
    ) -> crate::Result<()> {
        let key = CoalesceKey {
            subscription_name: subscription_name.to_string(),
            topic_name: topic_name.to_string(),
            topic_key: topic_key.map(String::from),
        };
        let should_flush = {
            let mut guard = self.pending.lock().await;
            guard.insert(key, seq);
            u32::try_from(guard.len()).unwrap_or(u32::MAX) >= self.flush_every
        };
        if should_flush {
            self.flush().await?;
        }
        Ok(())
    }

    /// Flush all pending checkpoints now.
    ///
    /// # Errors
    ///
    /// Returns an error if the operation fails.
    pub async fn flush(&self) -> crate::Result<()> {
        let reclaimer = self.reclaimer.get().cloned();
        flush_map(&self.port, &self.pending, reclaimer).await
    }

    /// Minimum unflushed seq for a storage partition.
    pub async fn pending_min_seq(&self, topic: &str, topic_key: Option<&str>) -> Option<i64> {
        let guard = self.pending.lock().await;
        guard
            .iter()
            .filter(|(k, _)| k.topic_name == topic && k.topic_key.as_deref() == topic_key)
            .map(|(_, seq)| *seq)
            .min()
    }

    /// Storage partitions with unflushed checkpoint state.
    pub async fn pending_partitions(&self) -> Vec<TopicPartition> {
        let keys: Vec<CoalesceKey> = {
            let guard = self.pending.lock().await;
            guard.keys().cloned().collect()
        };
        let mut seen = HashSet::new();
        let mut out = Vec::new();
        for key in keys {
            let part = TopicPartition::new(key.topic_name, key.topic_key);
            if seen.insert(part.clone()) {
                out.push(part);
            }
        }
        out
    }
}

#[tracing::instrument(name = "photon.checkpoint.flush", skip_all)]
async fn flush_map(
    port: &Arc<dyn StoragePort>,
    pending: &Mutex<HashMap<CoalesceKey, i64>>,
    reclaimer: Option<Arc<dyn PartitionReclaim>>,
) -> crate::Result<()> {
    let batch: Vec<(CoalesceKey, i64)> = {
        let mut guard = pending.lock().await;
        guard.drain().collect()
    };

    let mut sweep_parts = HashSet::new();
    for (key, seq) in &batch {
        port.commit_checkpoint(
            &key.subscription_name,
            &key.topic_name,
            key.topic_key.as_deref(),
            *seq,
        )
        .await?;
        sweep_parts.insert(TopicPartition::new(
            key.topic_name.clone(),
            key.topic_key.clone(),
        ));
    }

    if let Some(reclaimer) = reclaimer {
        let partitions: Vec<TopicPartition> = sweep_parts.into_iter().collect();
        if !partitions.is_empty() {
            reclaimer.sweep_partitions(&partitions).await?;
        }
    }

    Ok(())
}

fn checkpoint_flush_every() -> u32 {
    std::env::var("PHOTON_CHECKPOINT_COALESCE_EVERY")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(10)
        .max(1)
}

fn checkpoint_flush_ms() -> u64 {
    std::env::var("PHOTON_CHECKPOINT_FLUSH_MS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(500)
        .max(50)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_are_sane() {
        assert!(checkpoint_flush_every() >= 1);
        assert!(checkpoint_flush_ms() >= 50);
    }
}
