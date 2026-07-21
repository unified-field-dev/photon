//! Encode and decode Photon events on `JetStream` messages.

use async_nats::header::{self, HeaderValue};
use async_nats::jetstream::Message;
use async_nats::HeaderMap;
use bytes::Bytes;
use photon_backend::models::Event;
use photon_backend::{PhotonError, Result};

use crate::subject::{HEADER_EVENT_ID, HEADER_SEQ, HEADER_TOPIC_KEY};

/// Build headers and JSON payload for a `JetStream` publish.
///
/// # Errors
///
/// Returns an error when JSON serialization fails.
pub fn encode_event(event: &Event) -> Result<(HeaderMap, Bytes)> {
    let mut headers = HeaderMap::new();
    headers.insert(HEADER_EVENT_ID, HeaderValue::from(event.event_id.as_str()));
    headers.insert(HEADER_SEQ, HeaderValue::from(event.seq.to_string()));
    if let Some(key) = event.topic_key.as_deref() {
        headers.insert(HEADER_TOPIC_KEY, HeaderValue::from(key));
    }

    let body =
        serde_json::to_vec(event).map_err(|e| PhotonError::caused("nats encode event json:", e))?;
    Ok((headers, Bytes::from(body)))
}

/// Decode a `JetStream` message into a Photon [`Event`].
///
/// # Errors
///
/// Returns an error when headers or payload cannot be parsed.
pub fn decode_event(message: &Message) -> Result<Event> {
    let mut event: Event = serde_json::from_slice(&message.payload)
        .map_err(|e| PhotonError::caused("nats decode event json:", e))?;

    if let Some(headers) = message.headers.as_ref() {
        if let Some(header_id) = headers
            .get(HEADER_EVENT_ID)
            .map(async_nats::HeaderValue::as_str)
        {
            if header_id != event.event_id {
                return Err(PhotonError::Internal(format!(
                    "nats event_id header mismatch: header={header_id} body={}",
                    event.event_id
                )));
            }
        }

        if let Some(header_seq) = headers
            .get(HEADER_SEQ)
            .map(async_nats::HeaderValue::as_str)
            .and_then(|s| s.parse::<i64>().ok())
        {
            if header_seq != event.seq && !(header_seq == 0 && event.seq == 0) {
                return Err(PhotonError::Internal(format!(
                    "nats seq header mismatch: header={header_seq} body={}",
                    event.seq
                )));
            }
        }
    }

    if event.seq == 0 {
        if let Some(stream_seq) = stream_sequence(message) {
            event.seq = i64::try_from(stream_seq).unwrap_or(i64::MAX);
        }
    }

    Ok(event)
}

/// `JetStream` stream sequence from message metadata.
#[must_use]
pub fn stream_sequence(message: &Message) -> Option<u64> {
    if let Some(headers) = message.headers.as_ref() {
        if let Some(seq) = headers
            .get(header::NATS_SEQUENCE)
            .and_then(|v| v.as_str().parse().ok())
        {
            return Some(seq);
        }
    }
    message.info().ok().map(|info| info.stream_sequence)
}
