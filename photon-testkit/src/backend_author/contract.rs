//! Minimal publish/subscribe contract for custom backends.

use futures::StreamExt;
use photon_backend::Result;
use photon_runtime::Photon;

use crate::fixtures::smoke_actor_json;

/// Publish → subscribe → optional `get_event` roundtrip on a fresh topic.
///
/// # Errors
///
/// Returns an error if the operation fails.
pub async fn run_backend_contract(photon: &Photon, topic: &str) -> Result<()> {
    let payload = serde_json::json!({"contract": true});
    let event_id = photon
        .publish(topic, None, smoke_actor_json(), payload)
        .await?;

    let mut stream = photon.subscribe(topic, None, None);
    let received = stream.next().await.ok_or_else(|| {
        photon_backend::PhotonError::Internal("subscribe stream ended empty".into())
    })??;

    if received.event_id != event_id {
        return Err(photon_backend::PhotonError::Internal(format!(
            "expected event_id {event_id}, got {}",
            received.event_id
        )));
    }

    if photon.backend_label() == "mem" || photon_capabilities_support_get_event(photon) {
        let loaded = photon.get_event(&event_id).await?.ok_or_else(|| {
            photon_backend::PhotonError::Internal("get_event returned None".into())
        })?;

        if loaded.event_id != event_id {
            return Err(photon_backend::PhotonError::Internal(
                "get_event id mismatch".into(),
            ));
        }
    }

    Ok(())
}

const fn photon_capabilities_support_get_event(_photon: &Photon) -> bool {
    // Broker tiers omit get_event until optional cache lands.
    false
}
