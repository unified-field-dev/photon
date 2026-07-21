//! Retention sweep orchestration.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;

use super::config::RetentionPolicy;
use super::hook::{PartitionReclaim, RetentionHook};
use super::partition::{known_subscriptions, merge_partitions, TopicPartition};
use super::watermark::{
    compute_watermarks, merge_truncate_bound, truncate_before_arg, WatermarkContext,
};
use crate::checkpoint::CheckpointCoalescer;
use crate::delivery::DlqSink;
use crate::error::Result;
use crate::instrumentation::{log_ops, record_retention_reclaim, record_retention_safe_seq};
use crate::storage::StoragePort;

/// Outcome of reclaiming one storage partition.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReclaimReport {
    /// Topic that was reclaimed.
    pub topic_name: String,
    /// Optional partition key.
    pub topic_key: Option<String>,
    /// Safe watermark seq used for truncate, if any.
    pub safe_seq: Option<i64>,
    /// Seq bound passed to truncate.
    pub truncate_bound: i64,
    /// Number of events removed.
    pub removed: u64,
}

/// Dependencies for [`RetentionReclaimer`].
pub struct RetentionDeps {
    /// Storage port for checkpoint loads and truncate.
    pub port: Arc<dyn StoragePort>,
    /// Pending checkpoint high-water marks.
    pub coalescer: Arc<CheckpointCoalescer>,
    /// DLQ sink (optional pin).
    pub dlq: Arc<DlqSink>,
    /// Retention policy.
    pub policy: RetentionPolicy,
    /// Optional host retention hook.
    pub hook: Option<Arc<dyn RetentionHook>>,
}

/// Headless reclaim coordinator (safe watermark + truncate via [`StoragePort`]).
pub struct RetentionReclaimer {
    port: Arc<dyn StoragePort>,
    coalescer: Arc<CheckpointCoalescer>,
    dlq: Arc<DlqSink>,
    policy: RetentionPolicy,
    hook: Option<Arc<dyn RetentionHook>>,
    subscriptions: Vec<super::partition::SubscriptionPartition>,
}

impl RetentionReclaimer {
    /// Build a reclaimer and start the background sweep when configured.
    #[must_use]
    pub fn new(deps: RetentionDeps) -> Arc<Self> {
        let subscriptions = known_subscriptions(&deps.policy, deps.hook.as_deref());
        let reclaimer = Arc::new(Self {
            port: deps.port,
            coalescer: deps.coalescer,
            dlq: deps.dlq,
            policy: deps.policy,
            hook: deps.hook,
            subscriptions,
        });
        start_background_sweep(Arc::clone(&reclaimer));
        reclaimer
    }
}

fn start_background_sweep(reclaimer: Arc<RetentionReclaimer>) {
    if reclaimer.policy.sweep_interval_ms == 0 {
        return;
    }
    let interval = Duration::from_millis(reclaimer.policy.sweep_interval_ms);
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(interval);
        loop {
            ticker.tick().await;
            if let Err(e) = reclaimer.sweep_all().await {
                tracing::warn!(error = %e, "background retention sweep failed");
                log_ops(
                    "retention",
                    "sweep",
                    "background retention sweep failed",
                    "",
                    "",
                    &e.to_string(),
                );
            }
        }
    });
}

impl RetentionReclaimer {
    /// Reclaim one storage partition.
    ///
    /// # Errors
    ///
    /// Returns an error if the operation fails.
    #[tracing::instrument(name = "photon.retention.sweep_partition", skip(self), fields(topic, topic_key = ?topic_key))]
    pub async fn sweep_partition(
        &self,
        topic: &str,
        topic_key: Option<&str>,
    ) -> Result<ReclaimReport> {
        let (checkpoint_safe, ttl_keep_from) = compute_watermarks(
            &WatermarkContext {
                port: &self.port,
                coalescer: self.coalescer.as_ref(),
                dlq: self.dlq.as_ref(),
                policy: &self.policy,
                hook: self.hook.as_deref(),
                subscriptions: &self.subscriptions,
            },
            topic,
            topic_key,
        )
        .await?;

        if checkpoint_safe.is_none() && ttl_keep_from.is_none() {
            return Ok(ReclaimReport {
                topic_name: topic.to_string(),
                topic_key: topic_key.map(String::from),
                safe_seq: None,
                truncate_bound: 0,
                removed: 0,
            });
        }

        let bound = merge_truncate_bound(checkpoint_safe, ttl_keep_from);
        let safe_seq = checkpoint_safe.or(ttl_keep_from);

        if bound <= 0 {
            return Ok(ReclaimReport {
                topic_name: topic.to_string(),
                topic_key: topic_key.map(String::from),
                safe_seq,
                truncate_bound: 0,
                removed: 0,
            });
        }

        let removed = self
            .port
            .truncate_before(topic, topic_key, truncate_before_arg(bound))
            .await?;

        record_retention_reclaim(topic, removed);
        if let Some(safe) = safe_seq {
            record_retention_safe_seq(topic, topic_key, safe);
        }

        Ok(ReclaimReport {
            topic_name: topic.to_string(),
            topic_key: topic_key.map(String::from),
            safe_seq,
            truncate_bound: bound,
            removed,
        })
    }

    /// Reclaim explicit partition list (e.g. after checkpoint flush).
    ///
    /// # Errors
    ///
    /// Returns an error if the operation fails.
    pub async fn sweep_partitions(
        &self,
        partitions: &[TopicPartition],
    ) -> Result<Vec<ReclaimReport>> {
        let mut reports = Vec::with_capacity(partitions.len());
        for part in partitions {
            match self
                .sweep_partition(&part.topic_name, part.topic_key.as_deref())
                .await
            {
                Ok(report) => reports.push(report),
                Err(e) => {
                    log_ops(
                        "retention",
                        "sweep",
                        "partition retention sweep failed",
                        &part.topic_name,
                        "",
                        &e.to_string(),
                    );
                    return Err(e);
                }
            }
        }
        Ok(reports)
    }

    /// Discover and reclaim all known active partitions.
    ///
    /// # Errors
    ///
    /// Returns an error if the operation fails.
    pub async fn sweep_all(&self) -> Result<Vec<ReclaimReport>> {
        let partitions = self.discover_partitions().await;
        self.sweep_partitions(&partitions).await
    }

    async fn discover_partitions(&self) -> Vec<TopicPartition> {
        let handler_parts = self
            .subscriptions
            .iter()
            .map(|s| TopicPartition::new(s.topic_name.clone(), s.topic_key.clone()));

        let pending = self.coalescer.pending_partitions().await;

        merge_partitions(handler_parts.chain(pending))
    }
}

#[async_trait]
impl PartitionReclaim for RetentionReclaimer {
    async fn sweep_partitions(&self, partitions: &[TopicPartition]) -> Result<()> {
        Self::sweep_partitions(self, partitions).await?;
        Ok(())
    }
}
