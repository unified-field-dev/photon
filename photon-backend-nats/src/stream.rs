//! `JetStream` stream setup for Photon events.

use std::time::Duration;

use async_nats::jetstream::{self, stream::StorageType};
use photon_backend::{PhotonError, Result};

use crate::config::NatsConfig;
use crate::stream_shard::stream_subject_wildcard;
use crate::subject::STREAM_SUBJECTS;

/// Ensure all Photon `JetStream` streams exist with bounded retention.
///
/// # Errors
///
/// Returns an error when stream creation or lookup fails.
pub async fn ensure_streams(jetstream: &jetstream::Context, config: &NatsConfig) -> Result<()> {
    let replicas = config.effective_replicas();
    if config.stream_shards <= 1 && replicas > 1 {
        tracing::warn!(
            stream_shards = config.stream_shards,
            replicas,
            "PHOTON_NATS_REPLICAS>1 with stream_shards=1 causes sublinear publish ingress; \
             set stream_shards to broker count for write-heavy workloads"
        );
    }
    if config.stream_shards <= 1 {
        ensure_one_stream(
            jetstream,
            &config.stream_name,
            vec![STREAM_SUBJECTS.into()],
            config.retention,
            replicas,
        )
        .await?;
        return Ok(());
    }

    for shard in 0..config.stream_shards {
        let name = format!("{}-{}", config.stream_name, shard);
        let subjects = vec![stream_subject_wildcard(shard, config.stream_shards)];
        ensure_one_stream(jetstream, &name, subjects, config.retention, replicas).await?;
    }
    Ok(())
}

async fn ensure_one_stream(
    jetstream: &jetstream::Context,
    name: &str,
    subjects: Vec<String>,
    retention: Duration,
    num_replicas: usize,
) -> Result<()> {
    jetstream
        .get_or_create_stream(jetstream::stream::Config {
            name: name.to_string(),
            subjects,
            max_age: retention,
            storage: StorageType::File,
            num_replicas,
            ..Default::default()
        })
        .await
        .map_err(|e| PhotonError::caused(format!("nats ensure stream {name}"), e))?;
    Ok(())
}
