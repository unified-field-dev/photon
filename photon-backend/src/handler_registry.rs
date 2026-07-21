//! Registry of `#[photon::subscribe]` handlers discovered via inventory.

#![allow(missing_docs)]

use crate::handler_descriptor::HandlerDescriptor;

quark::define_registry! {
    /// Registry of inventory-submitted subscription handlers.
    pub struct HandlerRegistry for HandlerDescriptor;
}

impl HandlerRegistry {
    /// Handlers for a topic in stable registry-key order.
    #[must_use]
    pub fn for_topic(&self, topic_name: &str) -> Vec<&'static HandlerDescriptor> {
        let mut handlers: Vec<&'static HandlerDescriptor> =
            self.iter().filter(|h| h.topic_name == topic_name).collect();
        handlers.sort_by_key(|h| h.registry_key);
        handlers
    }
}
