//! Handler executor: inventory dispatch loop for `#[photon::subscribe]` handlers.
//!
//! [`ExecutorController::start`] is invoked by [`Photon::start_executor`]. It auto-discovers
//! [`photon_backend::HandlerDescriptor`] entries submitted by the proc macro, subscribes to each
//! topic with durable checkpoint replay, and dispatches through [`photon_core::IdentityFactory`].
//!
//! Call [`ExecutorController::shutdown`] then [`ExecutorController::join`] for graceful teardown.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use futures::StreamExt;
use photon_backend::{
    consumer_group::instance_id_from_env,
    consumer_group::static_assigned_shards,
    consumer_group::{ConsumerGroupCoordinator, GroupMember, StaticGroupCoordinator},
    delivery::DlqRecordParams,
    delivery_mode::ShardConfig,
    instrumentation::FailureReason,
    HandlerDescriptor, HandlerRegistry, PhotonError, Result,
};
use photon_core::IdentityFactory;
use tokio::task::{JoinHandle, JoinSet};
use tokio_util::sync::CancellationToken;

use crate::Photon;

/// Controls background handler dispatch tasks started by [`Photon::start_executor`].
pub struct ExecutorController {
    started: AtomicBool,
    cancel: CancellationToken,
    tasks: std::sync::Mutex<Vec<JoinHandle<()>>>,
}

impl Default for ExecutorController {
    fn default() -> Self {
        Self {
            started: AtomicBool::new(false),
            cancel: CancellationToken::new(),
            tasks: std::sync::Mutex::new(Vec::new()),
        }
    }
}

impl ExecutorController {
    /// Spawn subscription loops for every inventory-registered handler.
    ///
    /// # Errors
    ///
    /// Returns an error if the executor was already started on this controller.
    ///
    /// # Contract
    ///
    /// - Start is one-shot per controller; restart requires a new [`Photon`] build.
    /// - Outer loops respect [`Self::shutdown`]; in-flight handler tasks are awaited in
    ///   [`Self::join`].
    pub fn start(&self, photon: &Photon, identity: &Arc<dyn IdentityFactory>) -> Result<()> {
        if self
            .started
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_err()
        {
            return Err(PhotonError::Internal(
                "Photon executor already started".into(),
            ));
        }

        let registry = HandlerRegistry::auto_discover();
        if registry.is_empty() {
            return Ok(());
        }

        let mut tasks = Vec::new();
        for handler in registry.iter() {
            let cancel = self.cancel.child_token();
            let task = if handler.is_consumer_group() {
                spawn_group_handler_loop(photon.clone(), Arc::clone(identity), handler, cancel)
            } else {
                spawn_handler_loop(photon.clone(), Arc::clone(identity), handler, cancel)
            };
            tasks.push(task);
        }

        *self
            .tasks
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = tasks;
        Ok(())
    }

    /// Signal all handler loops to stop accepting new events.
    ///
    /// # Contract
    ///
    /// - Idempotent: repeated calls are no-ops.
    /// - Does not wait for in-flight handlers; call [`Self::join`] afterward.
    pub fn shutdown(&self) {
        self.cancel.cancel();
    }

    /// Await outer subscription loops (and their in-flight handler tasks).
    ///
    /// # Contract
    ///
    /// - Call [`Self::shutdown`] first for prompt exit from stream loops.
    /// - Drains stored outer [`JoinHandle`]s; each loop drains its own in-flight [`JoinSet`]
    ///   before exiting.
    /// - Safe to call when no tasks were started (no-op).
    pub async fn join(&self) {
        let tasks = std::mem::take(
            &mut *self
                .tasks
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner),
        );
        for task in tasks {
            let _ = task.await;
        }
    }
}

const fn failure_reason(err: &PhotonError) -> FailureReason {
    match err {
        PhotonError::Identity(_) => FailureReason::IdentityBuild,
        _ => FailureReason::HandlerError,
    }
}

fn shard_count_for_handler(photon: &Photon, handler: &'static HandlerDescriptor) -> u32 {
    handler
        .group_shard_count
        .or_else(|| {
            photon
                .registry()
                .get(handler.topic_name)
                .and_then(|d| d.shard_config)
                .map(|c| c.shard_count)
        })
        .unwrap_or(ShardConfig::DEFAULT_SHARD_COUNT)
}

fn spawn_group_handler_loop(
    photon: Photon,
    identity: Arc<dyn IdentityFactory>,
    handler: &'static HandlerDescriptor,
    cancel: CancellationToken,
) -> JoinHandle<()> {
    let topic = handler.topic_name;
    let group_id = handler.consumer_group.unwrap_or("");
    let invoke = handler.invoke;
    let services = Arc::clone(&photon.runtime().executor_services);
    let coordinator = StaticGroupCoordinator;

    tokio::spawn(async move {
        let shard_count = shard_count_for_handler(&photon, handler);
        let instance_id = instance_id_from_env().unwrap_or_else(|| "0".into());
        let assigned = coordinator
            .register(GroupMember {
                group_id: group_id.to_string(),
                instance_id: instance_id.clone(),
                topic_name: topic.to_string(),
                shard_count,
            })
            .await
            .unwrap_or_else(|_| static_assigned_shards(shard_count));

        let mut after_seq_by_shard = HashMap::new();
        for shard_id in &assigned {
            let shard_key = photon_backend::shard_storage_key(*shard_id);
            let seq = photon
                .get_checkpoint_seq(group_id, topic, Some(&shard_key))
                .await
                .ok()
                .flatten()
                .unwrap_or(0);
            after_seq_by_shard.insert(*shard_id, Some(seq));
        }

        let mut stream = photon.subscribe_consumer_group(topic, &assigned, after_seq_by_shard);
        let mut inflight = JoinSet::new();

        loop {
            tokio::select! {
                biased;
                () = cancel.cancelled() => break,
                event_result = stream.next() => {
                    let Some(event_result) = event_result else { break };
                    match event_result {
                        Ok(event) => {
                            let permit = match services.worker_pool.acquire().await {
                                Ok(p) => p,
                                Err(e) => {
                                    tracing::warn!(
                                        topic = topic,
                                        group = group_id,
                                        error = %e,
                                        "worker pool acquire failed"
                                    );
                                    continue;
                                }
                            };
                            let identity = Arc::clone(&identity);
                            let services = Arc::clone(&services);
                            let group = group_id.to_string();

                            inflight.spawn(async move {
                                let _permit = permit;
                                let event_id = event.event_id.clone();
                                let topic_name = event.topic_name.clone();
                                let topic_key = event.topic_key.clone();
                                let seq = event.seq;
                                let dispatch = invoke(identity.as_ref(), &event);
                                match dispatch.await {
                                    Ok(()) => {
                                        if let Err(e) = services
                                            .checkpoint_coalescer
                                            .record(&group, &topic_name, topic_key.as_deref(), seq)
                                            .await
                                        {
                                            let _ = services.dlq.record(&DlqRecordParams {
                                                event_id: &event_id,
                                                topic_name: &topic_name,
                                                topic_key: topic_key.as_deref(),
                                                seq,
                                                subscription_name: Some(&group),
                                                reason: FailureReason::CheckpointError,
                                                error: e.to_string(),
                                            });
                                        }
                                    }
                                    Err(e) => {
                                        let _ = services.dlq.record(&DlqRecordParams {
                                            event_id: &event_id,
                                            topic_name: &topic_name,
                                            topic_key: topic_key.as_deref(),
                                            seq,
                                            subscription_name: Some(&group),
                                            reason: failure_reason(&e),
                                            error: e.to_string(),
                                        });
                                    }
                                }
                            });
                        }
                        Err(e) => {
                            tracing::warn!(
                                topic = topic,
                                group = group_id,
                                error = %e,
                                "consumer group subscription stream error"
                            );
                        }
                    }
                }
            }
        }

        while inflight.join_next().await.is_some() {}
    })
}

fn spawn_handler_loop(
    photon: Photon,
    identity: Arc<dyn IdentityFactory>,
    handler: &'static HandlerDescriptor,
    cancel: CancellationToken,
) -> JoinHandle<()> {
    let topic = handler.topic_name;
    let subscription_name = handler.subscription_name;
    let invoke = handler.invoke;
    let services = Arc::clone(&photon.runtime().executor_services);

    tokio::spawn(async move {
        // Durable handlers always replay from a cursor. Missing checkpoint → start at 0
        // (not live-only `None`), so publishes that land before the subscribe handshake
        // are still delivered from storage.
        let after_seq = Some(
            photon
                .get_checkpoint_seq(subscription_name, topic, None)
                .await
                .ok()
                .flatten()
                .unwrap_or(0),
        );

        let mut stream = photon.subscribe(topic, None, after_seq);
        let mut inflight = JoinSet::new();

        loop {
            tokio::select! {
                biased;
                () = cancel.cancelled() => break,
                event_result = stream.next() => {
                    let Some(event_result) = event_result else { break };
                    match event_result {
                        Ok(event) => {
                            let permit = match services.worker_pool.acquire().await {
                                Ok(p) => p,
                                Err(e) => {
                                    tracing::warn!(
                                        topic = topic,
                                        subscription = subscription_name,
                                        error = %e,
                                        "worker pool acquire failed"
                                    );
                                    continue;
                                }
                            };
                            let identity = Arc::clone(&identity);
                            let services = Arc::clone(&services);
                            let sub_name = subscription_name.to_string();

                            inflight.spawn(async move {
                                let _permit = permit;
                                let event_id = event.event_id.clone();
                                let topic_name = event.topic_name.clone();
                                let topic_key = event.topic_key.clone();
                                let seq = event.seq;
                                let dispatch = invoke(identity.as_ref(), &event);
                                match dispatch.await {
                                    Ok(()) => {
                                        if let Err(e) = services
                                            .checkpoint_coalescer
                                            .record(&sub_name, &topic_name, topic_key.as_deref(), seq)
                                            .await
                                        {
                                            let _ = services.dlq.record(&DlqRecordParams {
                                                event_id: &event_id,
                                                topic_name: &topic_name,
                                                topic_key: topic_key.as_deref(),
                                                seq,
                                                subscription_name: Some(&sub_name),
                                                reason: FailureReason::CheckpointError,
                                                error: e.to_string(),
                                            });
                                        }
                                    }
                                    Err(e) => {
                                        let _ = services.dlq.record(&DlqRecordParams {
                                            event_id: &event_id,
                                            topic_name: &topic_name,
                                            topic_key: topic_key.as_deref(),
                                            seq,
                                            subscription_name: Some(&sub_name),
                                            reason: failure_reason(&e),
                                            error: e.to_string(),
                                        });
                                    }
                                }
                            });
                        }
                        Err(e) => {
                            tracing::warn!(
                                topic = topic,
                                subscription = subscription_name,
                                error = %e,
                                "handler subscription stream error"
                            );
                        }
                    }
                }
            }
        }

        while inflight.join_next().await.is_some() {}
    })
}
