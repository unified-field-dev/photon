//! Identity reconstruction port (injected at integration boundary).
//!
//! Publish captures `actor_json` on each event. When `#[photon::subscribe]` handlers run, the
//! executor calls [`IdentityFactory::reconstruct`] so handlers execute with the same actor
//! identity that triggered the publish — mirroring Chronon/Boson-style permission boundaries.

use std::any::Any;

use crate::error::IdentityError;

/// Opaque actor handle for handler execution.
///
/// Bound as [`Send`] + [`Sync`] so handlers may take `Arc<dyn Actor>` across await
/// points in the subscribe invoker future.
///
/// Implementors must provide the [`Any`] downcast helpers (standard bodies):
///
/// ```ignore
/// fn as_any(&self) -> &dyn Any { self }
/// fn as_any_mut(&mut self) -> &mut dyn Any { self }
/// fn into_any(self: Box<Self>) -> Box<dyn Any> { self }
/// ```
///
/// Or expand `actor_downcast_methods!` inside the `impl Actor` block.
pub trait Actor: Send + Sync + Any {
    /// Debug label for logs/tests.
    fn label(&self) -> &str;

    /// Downcast to [`Any`] (immutable).
    fn as_any(&self) -> &dyn Any;

    /// Downcast to [`Any`] (mutable).
    fn as_any_mut(&mut self) -> &mut dyn Any;

    /// Consume into a type-erased [`Any`] box for concrete downcasts in subscribe v2.
    fn into_any(self: Box<Self>) -> Box<dyn Any>;
}

/// Expands the standard [`Actor`] [`Any`] downcast method bodies.
///
/// Place inside an `impl Actor for T { ... }` block alongside [`Actor::label`].
#[macro_export]
macro_rules! actor_downcast_methods {
    () => {
        fn as_any(&self) -> &dyn ::std::any::Any {
            self
        }
        fn as_any_mut(&mut self) -> &mut dyn ::std::any::Any {
            self
        }
        fn into_any(self: ::std::boxed::Box<Self>) -> ::std::boxed::Box<dyn ::std::any::Any> {
            self
        }
    };
}

/// Reconstruct handler identity from captured actor JSON (handler boundary only).
pub trait IdentityFactory: Send + Sync + 'static {
    /// Build an actor for `#[photon::subscribe]` dispatch.
    ///
    /// # Errors
    ///
    /// Returns an error if the operation fails.
    fn reconstruct(&self, actor_json: &str) -> Result<Box<dyn Actor>, IdentityError>;
}

#[cfg(test)]
mod tests {
    use super::*;

    struct TestActor {
        label: String,
    }

    impl Actor for TestActor {
        fn label(&self) -> &str {
            &self.label
        }
        actor_downcast_methods!();
    }

    struct TestFactory;

    impl IdentityFactory for TestFactory {
        fn reconstruct(&self, actor_json: &str) -> Result<Box<dyn Actor>, IdentityError> {
            if actor_json.contains("System") {
                Ok(Box::new(TestActor { label: "ok".into() }))
            } else {
                Err(IdentityError::InvalidActor("missing System".into()))
            }
        }
    }

    #[test]
    fn identity_factory_reconstructs() {
        let factory = TestFactory;
        let actor = factory
            .reconstruct(r#"{"System":{"operation":"t"}}"#)
            .expect("ok");
        assert_eq!(actor.label(), "ok");
    }

    #[test]
    fn actor_downcast_to_concrete() {
        let factory = TestFactory;
        let actor = factory
            .reconstruct(r#"{"System":{"operation":"t"}}"#)
            .expect("ok");
        let concrete = actor.into_any().downcast::<TestActor>().expect("TestActor");
        assert_eq!(concrete.label(), "ok");
    }
}
