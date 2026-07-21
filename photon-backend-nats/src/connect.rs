//! NATS client connection helpers.

use photon_backend::{PhotonError, Result};

/// Connect to one or more NATS servers (`PHOTON_NATS_URL` may be comma-separated).
///
/// # Errors
///
/// Returns an error when connection fails.
pub async fn connect_nats(urls: &str) -> Result<async_nats::Client> {
    let servers: Vec<&str> = urls
        .split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .collect();
    if servers.is_empty() {
        return Err(PhotonError::Internal(
            "PHOTON_NATS_URL empty after parsing".into(),
        ));
    }
    if servers.len() == 1 {
        async_nats::connect(servers[0])
            .await
            .map_err(|e| PhotonError::caused(format!("nats connect {}", servers[0]), e))
    } else {
        async_nats::ConnectOptions::new()
            .retry_on_initial_connect()
            .connect(servers)
            .await
            .map_err(|e| PhotonError::caused(format!("nats connect {urls}"), e))
    }
}
