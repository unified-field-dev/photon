//! Build a [`Photon`](photon_runtime::Photon) with in-process storage + custom backend.

use std::sync::Arc;

use photon_backend::backend::{BackendContext, GenericPhotonBackend, PhotonBackend};
use photon_backend::{InProcStoragePort, Result, TransportCrypto};
use photon_runtime::Photon;

/// Test harness for custom backend install functions.
pub struct BackendAuthorHarness;

impl BackendAuthorHarness {
    /// Build a Photon with in-memory storage and the given backend install fn.
    ///
    /// # Errors
    ///
    /// Returns an error if the operation fails.
    pub fn build(
        install: impl FnOnce(BackendContext) -> Result<Arc<dyn PhotonBackend>> + Send + 'static,
    ) -> Result<Photon> {
        let port: Arc<dyn photon_backend::StoragePort> = Arc::new(InProcStoragePort::new(
            TransportCrypto::from_bytes(*b"photon-dev-transport-key-32bytes"),
        ));
        Photon::builder()
            .storage_port(Arc::clone(&port))
            .backend_with_context(install)
            .build()
    }

    /// Build default `mem` tier [`GenericPhotonBackend`].
    ///
    /// # Errors
    ///
    /// Returns an error if the operation fails.
    pub fn build_mem() -> Result<Photon> {
        Self::build(GenericPhotonBackend::install_mem)
    }
}
