//! Kafka client connection helpers.

use std::sync::Arc;

use photon_backend::{PhotonError, Result};
use rskafka::client::Client;
use rskafka::client::ClientBuilder;

use crate::config::KafkaConfig;

/// Shared Kafka client handle.
pub type SharedClient = Arc<Client>;

/// Connect to Kafka bootstrap brokers.
///
/// # Errors
///
/// Returns an error when connection fails.
pub async fn connect_kafka(config: &KafkaConfig) -> Result<SharedClient> {
    validate_brokers(&config.brokers)?;
    let brokers: Vec<String> = config
        .brokers
        .split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_string)
        .collect();
    let client = ClientBuilder::new(brokers)
        .build()
        .await
        .map_err(|e| PhotonError::caused(format!("kafka connect {}", config.brokers), e))?;
    Ok(Arc::new(client))
}

/// Validate broker list is non-empty.
///
/// # Errors
///
/// Returns an error when brokers string is empty after parsing.
pub fn validate_brokers(brokers: &str) -> Result<()> {
    if brokers.split(',').map(str::trim).all(str::is_empty) {
        return Err(PhotonError::Internal(
            "PHOTON_KAFKA_BROKERS empty after parsing".into(),
        ));
    }
    Ok(())
}
