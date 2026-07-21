//! Fluvio client connection helpers.

use std::sync::Arc;

use fluvio::{Fluvio, FluvioClusterConfig};
use photon_backend::{PhotonError, Result};

use crate::config::FluvioConfig;

/// Shared Fluvio client handle.
pub type SharedClient = Arc<Fluvio>;

/// Connect to a Fluvio cluster via SC endpoint.
///
/// # Errors
///
/// Returns an error when connection fails.
pub async fn connect_fluvio(config: &FluvioConfig) -> Result<SharedClient> {
    validate_endpoint(&config.endpoint)?;
    let cluster_config = FluvioClusterConfig::new(config.endpoint.clone());
    let client = Fluvio::connect_with_config(&cluster_config)
        .await
        .map_err(|e| PhotonError::caused(format!("fluvio connect {}", config.endpoint), e))?;
    Ok(Arc::new(client))
}

/// Validate endpoint is non-empty.
///
/// # Errors
///
/// Returns an error when endpoint string is empty after parsing.
pub fn validate_endpoint(endpoint: &str) -> Result<()> {
    if endpoint.trim().is_empty() {
        return Err(PhotonError::Internal(
            "PHOTON_FLUVIO_ENDPOINT empty after parsing".into(),
        ));
    }
    Ok(())
}
