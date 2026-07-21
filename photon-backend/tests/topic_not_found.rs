//! Integration test: topic registry returns `TopicNotFound` for unknown topics.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use photon_backend::{PhotonError, TopicRegistry};

#[test]
fn unknown_topic_returns_topic_not_found() {
    let registry = TopicRegistry::new();
    let err = registry.get_or_err("nonexistent.topic").unwrap_err();
    assert!(matches!(err, PhotonError::TopicNotFound(name) if name == "nonexistent.topic"));
}
