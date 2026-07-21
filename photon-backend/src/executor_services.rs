//! Shared runtime services for handler delivery (constructed at Photon build time).

use std::sync::Arc;

use crate::checkpoint::CheckpointCoalescer;
use crate::delivery::{DlqSink, WorkerPool};
use crate::retention::{
    PartitionReclaim, RetentionDeps, RetentionHook, RetentionPolicy, RetentionReclaimer,
};
use crate::storage::StoragePort;

/// Shared runtime services for handler delivery.
pub struct ExecutorServices {
    /// Bounded handler concurrency pool.
    pub worker_pool: Arc<WorkerPool>,
    /// Coalesced checkpoint writer.
    pub checkpoint_coalescer: Arc<CheckpointCoalescer>,
    /// Dead-letter sink for failed deliveries.
    pub dlq: Arc<DlqSink>,
    /// Storage retention reclaim coordinator.
    pub retention_reclaimer: Arc<RetentionReclaimer>,
}

impl ExecutorServices {
    /// Construct delivery services bound to the storage port.
    #[allow(clippy::needless_pass_by_value)] // Arc-by-value is the public ownership API
    pub fn new(
        port: Arc<dyn StoragePort>,
        policy: RetentionPolicy,
        hook: Option<Arc<dyn RetentionHook>>,
    ) -> Self {
        let coalescer = Arc::new(CheckpointCoalescer::new(Arc::clone(&port)));
        let dlq = Arc::new(DlqSink::new());
        let reclaimer = RetentionReclaimer::new(RetentionDeps {
            port: Arc::clone(&port),
            coalescer: Arc::clone(&coalescer),
            dlq: Arc::clone(&dlq),
            policy,
            hook,
        });
        coalescer.attach_reclaimer(Arc::clone(&reclaimer) as Arc<dyn PartitionReclaim>);
        Self {
            worker_pool: Arc::new(WorkerPool::from_env()),
            checkpoint_coalescer: coalescer,
            dlq,
            retention_reclaimer: reclaimer,
        }
    }
}
