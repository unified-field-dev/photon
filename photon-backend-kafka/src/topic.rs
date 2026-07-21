//! Kafka topic setup for Photon events.

use photon_backend::{PhotonError, Result};
use rskafka::client::partition::UnknownTopicHandling;
use tracing::warn;

use crate::config::KafkaConfig;
use crate::connect::SharedClient;

const TOPIC_TIMEOUT_MS: i32 = 30_000;

/// Ensure the compact checkpoint topic exists.
///
/// # Errors
///
/// Returns an error when topic creation fails.
pub async fn ensure_checkpoint_topic(client: &SharedClient, config: &KafkaConfig) -> Result<()> {
    create_topic_if_missing(
        client,
        &config.checkpoint_topic(),
        1,
        config.effective_replicas(),
    )
    .await
}

/// Ensure a data topic exists before publish/subscribe.
///
/// # Errors
///
/// Returns an error when topic creation fails.
pub async fn ensure_data_topic(
    client: &SharedClient,
    config: &KafkaConfig,
    topic_name: &str,
) -> Result<()> {
    create_topic_if_missing(client, topic_name, 1, config.effective_replicas()).await
}

async fn create_topic_if_missing(
    client: &SharedClient,
    name: &str,
    partitions: i32,
    replication: i32,
) -> Result<()> {
    if topic_exists(client, name).await? {
        return Ok(());
    }

    let controller = client
        .controller_client()
        .map_err(|e| PhotonError::caused(format!("kafka controller client {name}"), e))?;
    match controller
        .create_topic(
            name,
            partitions,
            i16::try_from(replication).unwrap_or(1),
            TOPIC_TIMEOUT_MS,
        )
        .await
    {
        Ok(()) => Ok(()),
        Err(e) if e.to_string().contains("TopicAlreadyExists") => Ok(()),
        Err(e) => Err(PhotonError::Internal(format!(
            "kafka create topic {name}: {e}"
        ))),
    }
}

async fn topic_exists(client: &SharedClient, name: &str) -> Result<bool> {
    match client
        .partition_client(name.to_string(), 0, UnknownTopicHandling::Error)
        .await
    {
        Ok(_) => Ok(true),
        Err(e) if e.to_string().contains("UnknownTopicOrPartition") => Ok(false),
        Err(e) => Err(PhotonError::caused(format!("kafka metadata {name}"), e)),
    }
}

/// Warn when replication settings may limit ingress scaling.
pub fn warn_replication_settings(config: &KafkaConfig) {
    let replicas = config.effective_replicas();
    if config.topic_shards <= 1 && replicas > 1 {
        warn!(
            topic_shards = config.topic_shards,
            replicas,
            "PHOTON_KAFKA_REPLICAS>1 with topic_shards=1 causes sublinear publish ingress; \
             set topic_shards to broker count for write-heavy workloads"
        );
    }
}
