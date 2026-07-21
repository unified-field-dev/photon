//! Encode and decode Photon events on Fluvio records.

use fluvio::RecordKey;
use photon_backend::models::Event;
use photon_backend::{PhotonError, Result};

/// Decoded record with broker offset metadata.
pub struct DecodedRecord {
    /// Parsed Photon event.
    pub event: Event,
}

/// Build record key and JSON payload for a Fluvio produce.
///
/// # Errors
///
/// Returns an error when JSON serialization fails.
pub fn encode_event_record(event: &Event) -> Result<(RecordKey, Vec<u8>)> {
    let key = event
        .topic_key
        .clone()
        .or_else(|| Some(event.event_id.clone()))
        .map_or(RecordKey::NULL, RecordKey::from);

    let body = serde_json::to_vec(event)
        .map_err(|e| PhotonError::caused("fluvio encode event json:", e))?;

    Ok((key, body))
}

/// Decode a Fluvio consumer record into a Photon [`Event`].
///
/// # Errors
///
/// Returns an error when payload cannot be parsed.
pub fn decode_record(payload: &[u8], offset: i64) -> Result<DecodedRecord> {
    let mut event: Event = serde_json::from_slice(payload)
        .map_err(|e| PhotonError::caused("fluvio decode event json:", e))?;

    if event.seq == 0 {
        event.seq = record_sequence(offset);
    }

    Ok(DecodedRecord { event })
}

/// Fluvio offset+1 sequence from record metadata.
#[must_use]
pub const fn record_sequence(offset: i64) -> i64 {
    offset.saturating_add(1)
}
