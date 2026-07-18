//! Backend implementations and assembly context for Photon.

mod capabilities;
mod context;
mod generic;
mod photon_backend;

pub use capabilities::BackendCapabilities;
pub use context::BackendContext;
pub use generic::GenericPhotonBackend;
pub use photon_backend::PhotonBackend;

/// Back-compat alias for the default in-process `mem` tier [`GenericPhotonBackend`].
///
/// Prefer [`PhotonBuilder::storage_port`](https://docs.rs/uf-photon/latest/photon/struct.PhotonBuilder.html#method.storage_port)
/// (or the default mem path) for normal hosts. Use
/// [`GenericPhotonBackend::install_mem`](crate::GenericPhotonBackend::install_mem) /
/// [`PhotonBuilder::mem_backend`](https://docs.rs/uf-photon/latest/photon/struct.PhotonBuilder.html#method.mem_backend)
/// when you need an explicit install fn.
///
/// # Example
///
/// ```rust,ignore
/// use photon_backend::{BackendContext, EmbeddedBackend};
/// use photon_runtime::Photon;
///
/// let _photon = Photon::builder()
///     .backend_with_context(|ctx: BackendContext| EmbeddedBackend::install_mem(ctx))
///     .auto_registry()
///     .build()?;
/// ```
pub type EmbeddedBackend = GenericPhotonBackend;
