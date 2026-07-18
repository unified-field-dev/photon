//! Options and handle for typed subscribe.

use super::SubscriptionMode;

/// Options specific to consumer-group subscriptions.
///
/// Used with [`SubscribeOpts::consumer_group`]. Checkpoints are owned by `group_id` × shard,
/// not per-instance subscription names.
#[derive(Debug, Clone)]
pub struct GroupOpts {
    /// Stable consumer group identifier (shared across replicas).
    pub group_id: String,
    /// Override virtual shard count (defaults to topic `#[photon::topic(shards = ...)]`).
    pub shard_count: Option<u32>,
    /// Fleet member id for lease coordination (static env assignment when unset).
    pub instance_id: Option<String>,
}

impl GroupOpts {
    /// Create group options with the given `group_id`.
    pub fn new(group_id: impl Into<String>) -> Self {
        Self {
            group_id: group_id.into(),
            shard_count: None,
            instance_id: None,
        }
    }

    /// Override virtual shard count for this subscription.
    #[must_use]
    pub const fn shards(mut self, count: u32) -> Self {
        self.shard_count = Some(count);
        self
    }

    /// Set fleet member instance id for lease-based assignment.
    #[must_use]
    pub fn instance_id(mut self, id: impl Into<String>) -> Self {
        self.instance_id = Some(id.into());
        self
    }
}

/// Options for subscribing to a topic.
///
/// Pass to the typed API from [`topic`](https://docs.rs/uf-photon/latest/photon/attr.topic.html):
/// `EventType::subscribe_on(&photon, opts)`.
///
/// Runnable: `keyed_topic`, `consumer_group`. Getting started:
/// [publish and subscribe](https://docs.rs/uf-photon/latest/photon/#4-publish-and-subscribe).
///
/// # Examples
///
/// Ephemeral stream with partition filter (see `keyed_topic` example):
///
/// ```rust,ignore
/// use futures::StreamExt;
/// use photon::{SubscribeOpts, topic};
///
/// #[topic(name = "examples.orders", keyed_by = "customer_id")]
/// struct OrderPlaced { customer_id: String, amount_cents: u64 }
///
/// # async fn demo(photon: &photon::Photon) -> photon::Result<()> {
/// let opts = SubscribeOpts::default_ephemeral().topic_key_filter("alice");
/// let mut stream = OrderPlaced::subscribe_on(photon, opts).await?;
/// let _ = stream.next().await;
/// # Ok(())
/// # }
/// ```
///
/// Durable broadcast by subscription name:
///
/// ```rust,no_run
/// use photon_backend::SubscribeOpts;
///
/// let opts = SubscribeOpts::broadcast().subscription_name("my-worker");
/// ```
#[derive(Debug, Clone, Default)]
pub struct SubscribeOpts {
    /// Subscription name (required for durable broadcast mode).
    pub subscription_name: Option<String>,
    /// Filter by topic key (for keyed topics).
    pub topic_key_filter: Option<String>,
    /// Durable or ephemeral mode.
    pub mode: SubscriptionMode,
    /// Consumer group (load-balanced); mutually exclusive with durable subscription name.
    pub consumer_group: Option<GroupOpts>,
}

impl SubscribeOpts {
    /// Create default options (ephemeral, no key filter).
    #[must_use]
    pub const fn default_ephemeral() -> Self {
        Self {
            subscription_name: None,
            topic_key_filter: None,
            mode: SubscriptionMode::Ephemeral,
            consumer_group: None,
        }
    }

    /// Broadcast durable subscription (today's default).
    #[must_use]
    pub fn broadcast() -> Self {
        Self {
            mode: SubscriptionMode::Durable,
            ..Self::default_ephemeral()
        }
    }

    /// Load-balanced consumer group subscription.
    #[must_use]
    pub fn consumer_group(mut self, opts: GroupOpts) -> Self {
        self.subscription_name = None;
        self.topic_key_filter = None;
        self.mode = SubscriptionMode::Durable;
        self.consumer_group = Some(opts);
        self
    }

    /// Set subscription name (for durable mode).
    #[must_use]
    pub fn subscription_name(mut self, name: impl Into<String>) -> Self {
        self.subscription_name = Some(name.into());
        self
    }

    /// Set topic key filter.
    #[must_use]
    pub fn topic_key_filter(mut self, key: impl Into<String>) -> Self {
        self.topic_key_filter = Some(key.into());
        self
    }

    /// Set mode.
    #[must_use]
    pub const fn mode(mut self, mode: SubscriptionMode) -> Self {
        self.mode = mode;
        self
    }
}

/// Placeholder for future subscription lifecycle control (cancel, pause).
///
/// v0.1 returns a no-op handle; dropping the subscribe stream task is the supported way to
/// stop an ephemeral subscription today.
#[derive(Debug, Default)]
pub struct SubscriptionHandle {
    _private: (),
}

impl SubscriptionHandle {
    /// Create a new handle (reserved for future cancel/pause APIs).
    #[must_use]
    pub const fn new() -> Self {
        Self { _private: () }
    }
}
