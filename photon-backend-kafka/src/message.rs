//! Encode and decode Photon events on Kafka records.

use std::collections::BTreeMap;

use chrono::{DateTime, Utc};
use photon_backend::models::Event;
use photon_backend::{PhotonError, Result};
use rskafka::record::Record;

use crate::subject::{HEADER_EVENT_ID, HEADER_SEQ, HEADER_TOPIC_KEY};

/// Decoded record with broker offset metadata.
pub struct DecodedRecord {
    /// Parsed Photon event.
    pub event: Event,
}

/// Build headers and JSON payload for a Kafka produce.
///
/// # Errors
///
/// Returns an error when JSON serialization fails.
pub fn encode_event_record(
    event: &Event,
    timestamp: DateTime<Utc>,
) -> Result<(Record, BTreeMap<String, Vec<u8>>)> {
    let mut headers = BTreeMap::new();
    headers.insert(
        HEADER_EVENT_ID.to_string(),
        event.event_id.as_bytes().to_vec(),
    );
    headers.insert(HEADER_SEQ.to_string(), event.seq.to_string().into_bytes());
    if let Some(key) = event.topic_key.as_deref() {
        headers.insert(HEADER_TOPIC_KEY.to_string(), key.as_bytes().to_vec());
    }

    let body = serde_json::to_vec(event)
        .map_err(|e| PhotonError::caused("kafka encode event json:", e))?;

    let record = Record {
        key: event
            .topic_key
            .clone()
            .or_else(|| Some(event.event_id.clone()))
            .map(std::string::String::into_bytes),
        value: Some(body),
        headers: headers.clone(),
        timestamp,
    };
    Ok((record, headers))
}

/// Decode a Kafka record into a Photon [`Event`].
///
/// # Errors
///
/// Returns an error when headers or payload cannot be parsed.
pub fn decode_record(record: &Record, offset: i64) -> Result<DecodedRecord> {
    let payload = record
        .value
        .as_deref()
        .ok_or_else(|| PhotonError::Internal("kafka record missing payload".into()))?;
    let mut event: Event = serde_json::from_slice(payload)
        .map_err(|e| PhotonError::caused("kafka decode event json:", e))?;

    for (key, value) in &record.headers {
        match key.as_str() {
            HEADER_EVENT_ID => {
                let header_id = String::from_utf8_lossy(value);
                if !header_id.is_empty() && header_id != event.event_id {
                    return Err(PhotonError::Internal(format!(
                        "kafka event_id header mismatch: header={header_id} body={}",
                        event.event_id
                    )));
                }
            }
            HEADER_SEQ => {
                if let Ok(header_seq) = String::from_utf8_lossy(value).parse::<i64>() {
                    if header_seq != event.seq && !(header_seq == 0 && event.seq == 0) {
                        return Err(PhotonError::Internal(format!(
                            "kafka seq header mismatch: header={header_seq} body={}",
                            event.seq
                        )));
                    }
                }
            }
            _ => {}
        }
    }

    if event.seq == 0 {
        event.seq = offset.saturating_add(1);
    }

    Ok(DecodedRecord { event })
}

/// Kafka offset+1 sequence from record metadata.
#[must_use]
pub const fn record_sequence(offset: i64) -> i64 {
    offset.saturating_add(1)
}
