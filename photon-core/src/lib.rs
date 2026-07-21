//! Core identity and shared types for Photon (no delivery topology).
//!
//! Portable actor reconstruction used by handlers and hosts. Delivery backends and the runtime
//! live in other crates.
//!
//! ## Entry points
//!
//! - [`IdentityFactory`] / [`Actor`] — reconstruct actors from captured JSON at handler dispatch
//! - [`JsonIdentityFactory`] / [`JsonActor`] — JSON stubs for tests and examples
//! - [`IdentityError`] — identity port failures

#![cfg_attr(test, allow(clippy::unwrap_used, clippy::expect_used))]

pub mod error;
pub mod identity;
pub mod stub_identity;

pub use error::IdentityError;
pub use identity::{Actor, IdentityFactory};
// `actor_downcast_methods` is exported via `#[macro_export]` from `identity`.
pub use stub_identity::{JsonActor, JsonIdentityFactory};
