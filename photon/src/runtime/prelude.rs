//! Common imports for application code using typed topics and the runtime handle.
//!
//! Prefer this prelude when you want `Event`, `SubscribeOpts`, macros, and `Photon` in scope
//! without importing the full runtime module tree. For backend wiring and transport types, import
//! from the crate root explicitly.
//!
//! Getting started: [declare topics](https://docs.rs/uf-photon/latest/photon/#3-declare-topics-and-handlers).
//!
//! # Example
//!
//! ```rust,no_run
//! use photon::prelude::*;
//!
//! #[topic(name = "app.events.demo")]
//! pub struct Demo {
//!     pub n: u32,
//! }
//!
//! # async fn demo() -> photon::Result<()> {
//! let photon = Photon::builder().auto_registry().build()?;
//! Demo { n: 1 }.publish_on(&photon).await?;
//! # Ok(())
//! # }
//! ```

pub use photon_macros::{subscribe, topic};

pub use photon_backend::{
    Event, HandlerCtx, Result, SubscribeOpts, SubscriptionHandle, TopicDescriptor,
};

pub use photon_runtime::{configure, Photon, PhotonBuilder};

pub use photon_core::{
    actor_downcast_methods, Actor, IdentityFactory, JsonActor, JsonIdentityFactory,
};
