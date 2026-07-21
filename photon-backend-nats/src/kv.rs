//! Shared `JetStream` KV bucket helpers.

use async_nats::jetstream::{self, kv, stream::StorageType};
use photon_backend::{PhotonError, Result};

/// Open an existing KV bucket or create it when missing.
///
/// # Errors
///
/// Returns an error when the bucket cannot be opened or created.
pub async fn ensure_kv_bucket(jetstream: &jetstream::Context, bucket: &str) -> Result<kv::Store> {
    match jetstream.get_key_value(bucket).await {
        Ok(store) => Ok(store),
        Err(_) => jetstream
            .create_key_value(kv::Config {
                bucket: bucket.to_string(),
                storage: StorageType::File,
                ..Default::default()
            })
            .await
            .map_err(|e| PhotonError::caused(format!("nats create kv bucket {bucket}"), e)),
    }
}
