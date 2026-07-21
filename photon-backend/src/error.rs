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
    /// Accepts any [`Display`] value so callers can wrap SDK errors that do not
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
