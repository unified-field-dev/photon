//! Error types for Photon.

use std::fmt;
use std::sync::Arc;

use thiserror::Error;

/// Shared error source that keeps [`PhotonError`] [`Clone`].
pub type SharedError = Arc<dyn std::error::Error + Send + Sync + 'static>;

/// Opaque display-based error source for types that do not implement [`std::error::Error`].
#[derive(Debug)]
struct DisplayError(String);

impl fmt::Display for DisplayError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for DisplayError {}

/// Result type alias for Photon operations.
pub type Result<T> = std::result::Result<T, PhotonError>;

/// Errors that can occur in Photon operations.
#[derive(Debug, Clone, Error)]
pub enum PhotonError {
    /// Topic not found in registry.
    #[error("topic not found: {0}")]
    TopicNotFound(String),

    /// Subscription not found.
    #[error("subscription not found: {0}")]
    SubscriptionNotFound(String),

    /// Event not found.
    #[error("event not found: {0}")]
    EventNotFound(String),

    /// Invalid topic name.
    #[error("invalid topic name: {0}")]
    InvalidTopicName(String),

    /// Payload serialization/deserialization error.
    #[error("payload error: {0}")]
    PayloadError(String),

    /// Schema mismatch at publish time.
    #[error("schema mismatch: {0}")]
    SchemaMismatch(String),

    /// Topic already registered with different schema.
    #[error("topic already exists: {0}")]
    TopicAlreadyExists(String),

    /// Subscription name required for durable subscriptions.
    #[error("subscription name required for durable subscriptions")]
    SubscriptionNameRequired,

    /// Persistence / store error (ops metadata adapters).
    #[error("persistence error: {0}")]
    PersistenceError(String),

    /// Persistence failure with preserved source chain.
    #[error("persistence error: {context}")]
    Persistence {
        /// Human-readable context for the failure.
        context: String,
        /// Underlying store / I/O error.
        #[source]
        source: SharedError,
    },

    /// Identity reconstruction failed at the handler boundary.
    ///
    /// Produced when [`photon_core::IdentityFactory::reconstruct`] rejects actor JSON
    /// (or a typed-actor downcast fails). Executor maps this to
    /// [`crate::instrumentation::FailureReason::IdentityBuild`].
    #[error("identity error: {0}")]
    Identity(String),

    /// Internal error (opaque message, no source chain).
    #[error("internal error: {0}")]
    Internal(String),

    /// Internal failure with preserved source chain.
    #[error("internal error: {context}")]
    Caused {
        /// Human-readable context for the failure.
        context: String,
        /// Underlying error.
        #[source]
        source: SharedError,
    },
}

impl PhotonError {
    /// Internal error with a source chain (broker I/O, crypto, etc.).
    ///
    /// Accepts any [`std::fmt::Display`] value so callers can wrap SDK errors that do not
    /// implement [`std::error::Error`] (e.g. some AEAD/crypto error types).
    pub fn caused(
        context: impl Into<String>,
        err: impl fmt::Display + Send + Sync + 'static,
    ) -> Self {
        Self::Caused {
            context: context.into(),
            source: Arc::new(DisplayError(err.to_string())),
        }
    }

    /// Internal error wrapping a real [`std::error::Error`] source chain.
    pub fn caused_error(
        context: impl Into<String>,
        err: impl std::error::Error + Send + Sync + 'static,
    ) -> Self {
        Self::Caused {
            context: context.into(),
            source: Arc::new(err),
        }
    }

    /// Persistence error with a source chain.
    pub fn persistence(
        context: impl Into<String>,
        err: impl std::error::Error + Send + Sync + 'static,
    ) -> Self {
        Self::Persistence {
            context: context.into(),
            source: Arc::new(err),
        }
    }
}

impl From<serde_json::Error> for PhotonError {
    fn from(err: serde_json::Error) -> Self {
        Self::PayloadError(err.to_string())
    }
}

impl From<anyhow::Error> for PhotonError {
    fn from(err: anyhow::Error) -> Self {
        // Prefer the full anyhow chain in the source message (`{#}`).
        Self::Caused {
            context: err.to_string(),
            source: Arc::new(DisplayError(format!("{err:#}"))),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::error::Error as _;

    use super::*;

    #[derive(Debug, thiserror::Error)]
    #[error("disk offline")]
    struct DiskError;

    /// Display-only error type (does not implement `std::error::Error`),
    /// mimicking SDK types like AEAD crypto errors.
    struct BadTag;

    impl fmt::Display for BadTag {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.write_str("bad auth tag")
        }
    }

    #[test]
    fn caused_wraps_display_only_source() {
        let err = PhotonError::caused("decrypt failed", BadTag);
        assert_eq!(err.to_string(), "internal error: decrypt failed");
        let source = err.source().expect("caused keeps a source");
        assert_eq!(source.to_string(), "bad auth tag");
    }

    #[test]
    fn caused_error_preserves_source_chain() {
        let err = PhotonError::caused_error("flush failed", DiskError);
        assert_eq!(err.to_string(), "internal error: flush failed");
        // Source is shared behind an Arc (keeps PhotonError Clone), so verify the
        // chain by display rather than downcast.
        let source = err.source().expect("caused_error keeps a source");
        assert_eq!(source.to_string(), "disk offline");
    }

    #[test]
    fn persistence_reports_context_and_source() {
        let err = PhotonError::persistence("sqlite decode", DiskError);
        assert_eq!(err.to_string(), "persistence error: sqlite decode");
        let source = err.source().expect("persistence keeps a source");
        assert_eq!(source.to_string(), "disk offline");
    }

    #[test]
    fn anyhow_conversion_keeps_full_chain() {
        let err = anyhow::Error::new(DiskError).context("flush checkpoint");
        let err = PhotonError::from(err);
        assert_eq!(err.to_string(), "internal error: flush checkpoint");
        let source = err.source().expect("anyhow conversion keeps a source");
        let chain = source.to_string();
        assert!(chain.contains("flush checkpoint"), "chain: {chain}");
        assert!(chain.contains("disk offline"), "chain: {chain}");
    }

    #[test]
    fn serde_json_conversion_maps_to_payload_error() {
        let err = serde_json::from_str::<serde_json::Value>("{").unwrap_err();
        let err = PhotonError::from(err);
        assert!(matches!(err, PhotonError::PayloadError(_)));
    }

    #[test]
    fn caused_errors_stay_clone() {
        let err = PhotonError::caused("original", BadTag);
        let clone = err.clone();
        assert_eq!(err.to_string(), clone.to_string());
        assert_eq!(
            err.source().map(ToString::to_string),
            clone.source().map(ToString::to_string)
        );
    }
}
