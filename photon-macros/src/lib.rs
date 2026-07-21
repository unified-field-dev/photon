//! Proc macros for Photon pub/sub.
//!
//! ## Entry points
//!
//! - [`topic`] — typed publish/subscribe on a struct; submits a topic descriptor to inventory
//! - [`subscribe`] — registers a handler; requires [`Photon::start_executor`](https://docs.rs/uf-photon/latest/photon/struct.Photon.html#method.start_executor) at boot
//!
//! Attribute tables: [`photon::config`](https://docs.rs/uf-photon/latest/photon/config/).
//! Getting started: [declare topics](https://docs.rs/uf-photon/latest/photon/#3-declare-topics-and-handlers).

#![cfg_attr(test, allow(clippy::unwrap_used, clippy::expect_used))]
use proc_macro::TokenStream;

mod subscribe;
mod topic;

/// Marks a struct as a Photon topic, generating typed publish/subscribe APIs.
///
/// Registers a topic descriptor in Quark inventory. With
/// [`PhotonBuilder::auto_registry`](https://docs.rs/uf-photon/latest/photon/struct.PhotonBuilder.html#method.auto_registry),
/// the host discovers it at boot. Prefer `EventType { … }.publish_on(&photon)` with an explicit
/// [`Photon`](https://docs.rs/uf-photon/latest/photon/struct.Photon.html) handle.
///
/// | Attribute | Purpose |
/// |-----------|---------|
/// | `name = "…"` | Topic stream name (required) |
/// | `keyed_by = "field"` | Partition key field on the struct |
/// | `shards = N` | Virtual shard count for consumer groups |
///
/// Full attribute reference: [`photon::config`](https://docs.rs/uf-photon/latest/photon/config/#photon-topic).
/// Getting started: [Mode 1](https://docs.rs/uf-photon/latest/photon/#mode-1--embedded-one-binary).
///
/// # Usage
///
/// ```ignore
/// use futures::StreamExt;
/// use photon::{topic, Photon, SubscribeOpts};
///
/// #[topic(name = "user.notifications", keyed_by = "user_id")]
/// pub struct NotificationPushed {
///     pub user_id: String,
/// }
///
/// # async fn demo(photon: &Photon) -> photon::Result<()> {
/// NotificationPushed { user_id: "u1".into() }
///     .publish_on(photon)
///     .await?;
///
/// let mut stream = NotificationPushed::subscribe_on(
///     photon,
///     SubscribeOpts::default_ephemeral(),
/// )
/// .await?;
/// if let Some(Ok(envelope)) = stream.next().await {
///     let _ = envelope.payload.user_id;
/// }
/// # Ok(())
/// # }
/// ```
#[proc_macro_attribute]
pub fn topic(attr: TokenStream, item: TokenStream) -> TokenStream {
    topic::topic_impl(attr, item)
}

/// Marks a function as a subscription handler registered via inventory.
///
/// The host must call
/// [`Photon::start_executor`](https://docs.rs/uf-photon/latest/photon/struct.Photon.html#method.start_executor)
/// (Mode 1 hosts and Mode 2 **workers**) so inventory-registered handlers run. Publishers that
/// only emit events can omit the executor.
///
/// | Attribute | Purpose |
/// |-----------|---------|
/// | `topic = "…"` | Topic name to consume (required) |
/// | `durable = "name"` | Checkpointed subscription name |
/// | `group = "id"` | Consumer-group load balancing |
///
/// Full attribute reference: [`photon::config`](https://docs.rs/uf-photon/latest/photon/config/#photon-subscribe).
/// Getting started: [Mode 2 worker](https://docs.rs/uf-photon/latest/photon/#worker-binary).
///
/// # Usage (v1 — `Box<dyn Actor>`)
///
/// ```ignore
/// use photon::{topic, subscribe, Actor, Result};
///
/// #[topic(name = "user.notifications")]
/// pub struct NotificationPushed {
///     pub user_id: String,
/// }
///
/// #[subscribe(topic = "user.notifications", durable = "push-worker")]
/// async fn on_notification(
///     _actor: Box<dyn Actor>,
///     _event: NotificationPushed,
/// ) -> Result<()> {
///     Ok(())
/// }
/// ```
///
/// # Actor bindings (v2)
///
/// The first parameter must be a simple identifier typed as one of:
///
/// - `Box<dyn Actor>` — reconstruct as-is (v1)
/// - `Arc<dyn Actor>` — `Arc::from(reconstruct()?)`
/// - `Box<Concrete>` / `Arc<Concrete>` — downcast via `Actor::into_any`; failure maps to
///   `PhotonError::Identity`
///
/// # Optional injectables (v2)
///
/// After `(actor, payload)` you may add trailing parameters detected by type path:
///
/// - `&Event` — transport event (metadata + raw JSON)
/// - `HandlerCtx` — delivery metadata (`event_id`, `topic_name`, `topic_key`, `seq`)
///
/// Unknown trailing types are rejected at compile time.
///
/// The handler must be `async` and return `photon::Result<()>`. Runnable: `subscribe_v2`.
#[proc_macro_attribute]
pub fn subscribe(attr: TokenStream, item: TokenStream) -> TokenStream {
    subscribe::subscribe_impl(attr, item)
}
