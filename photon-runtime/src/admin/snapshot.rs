//! Compose [`AdminSnapshot`] from registries and storage checkpoint reads.

use photon_backend::{
    shard_storage_key, BackendCapabilities, HandlerDescriptor, HandlerRegistry, ShardConfig,
    TopicDescriptor,
};

use crate::Photon;

use super::types::{
    AdminBackendSummary, AdminCheckpointSummary, AdminHandlerSummary, AdminSnapshot,
    AdminTopicSummary,
};

/// Build a full admin snapshot for the given runtime (async checkpoint reads).
///
/// # Errors
///
/// Returns an error if a checkpoint load fails.
pub async fn collect_admin_snapshot(photon: &Photon) -> photon_backend::Result<AdminSnapshot> {
    let topics = collect_topics(photon);
    let handlers = collect_handlers();
    let backend = collect_backend(photon.backend_capabilities());
    let checkpoints = collect_checkpoints(photon).await?;

    Ok(AdminSnapshot {
        backend,
        topics,
        handlers,
        checkpoints,
    })
}

fn collect_topics(photon: &Photon) -> Vec<AdminTopicSummary> {
    photon
        .registry()
        .sorted_topic_names()
        .into_iter()
        .filter_map(|name| photon.registry().get(name).map(topic_from_descriptor))
        .collect()
}

fn topic_from_descriptor(descriptor: &TopicDescriptor) -> AdminTopicSummary {
    AdminTopicSummary {
        topic_name: descriptor.topic_name.to_string(),
        keyed_by: descriptor.keyed_by.map(str::to_string),
        schema_json: descriptor
            .schema_value()
            .unwrap_or_else(|| serde_json::json!({})),
    }
}

fn collect_handlers() -> Vec<AdminHandlerSummary> {
    let registry = HandlerRegistry::auto_discover();
    registry.iter().map(handler_from_descriptor).collect()
}

fn handler_from_descriptor(handler: &HandlerDescriptor) -> AdminHandlerSummary {
    let is_group = handler.is_consumer_group();
    AdminHandlerSummary {
        topic_name: handler.topic_name.to_string(),
        subscription_name: if is_group || handler.subscription_name.is_empty() {
            None
        } else {
            Some(handler.subscription_name.to_string())
        },
        consumer_group: handler.consumer_group.map(str::to_string),
        registry_key: handler.registry_key.to_string(),
        mode: if is_group {
            "consumer_group".to_string()
        } else {
            "durable".to_string()
        },
    }
}

fn collect_backend(caps: BackendCapabilities) -> AdminBackendSummary {
    AdminBackendSummary {
        telemetry_label: caps.telemetry_label.to_string(),
        supports_get_event: caps.supports_get_event,
        max_replay_window_secs: caps.max_replay_window.map(|d| d.as_secs()),
    }
}

async fn collect_checkpoints(
    photon: &Photon,
) -> photon_backend::Result<Vec<AdminCheckpointSummary>> {
    let registry = HandlerRegistry::auto_discover();
    let mut out = Vec::new();

    for handler in registry.iter() {
        if handler.is_consumer_group() {
            let group_id = handler.consumer_group.unwrap_or("");
            let shard_count = group_shard_count(photon, handler);
            for shard_id in 0..shard_count {
                let shard_key = shard_storage_key(shard_id);
                let last_seq = photon
                    .get_checkpoint_seq(group_id, handler.topic_name, Some(&shard_key))
                    .await?;
                out.push(AdminCheckpointSummary {
                    subscription_name: group_id.to_string(),
                    topic_name: handler.topic_name.to_string(),
                    topic_key: Some(shard_key),
                    last_seq,
                });
            }
        } else {
            let last_seq = photon
                .get_checkpoint_seq(handler.subscription_name, handler.topic_name, None)
                .await?;
            out.push(AdminCheckpointSummary {
                subscription_name: handler.subscription_name.to_string(),
                topic_name: handler.topic_name.to_string(),
                topic_key: None,
                last_seq,
            });
        }
    }

    Ok(out)
}

fn group_shard_count(photon: &Photon, handler: &'static HandlerDescriptor) -> u32 {
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
