//! Storage adapter install + Photon bootstrap for a [`crate::MatrixSpec`].

mod telemetry;
mod topology;

use std::sync::Arc;

use anyhow::{bail, Result};
use photon_backend::StoragePort;
use photon_runtime::Photon;

use crate::matrix::StorageAdapter;
use crate::MatrixSpec;

/// Holds bootstrap state for one matrix row.
pub struct BootstrapSession {
    matrix: MatrixSpec,
    ready: bool,
    storage_port: Option<Arc<dyn StoragePort>>,
}

impl BootstrapSession {
    /// Start a session for the given matrix dimensions.
    #[must_use]
    pub const fn new(matrix: MatrixSpec) -> Self {
        Self {
            matrix,
            ready: false,
            storage_port: None,
        }
    }

    /// Matrix dimensions for this session.
    #[must_use]
    pub const fn matrix(&self) -> &MatrixSpec {
        &self.matrix
    }

    /// Install storage adapter (sync — mem only).
    ///
    /// # Errors
    ///
    /// Returns an error if the operation fails.
    pub fn install(&mut self) -> Result<()> {
        telemetry::apply_telemetry(self.matrix.telemetry);
        match self.matrix.storage {
            StorageAdapter::Mem => {
                self.storage_port =
                    Some(crate::backends::install_storage_port(StorageAdapter::Mem)?);
                self.ready = true;
                Ok(())
            }
            StorageAdapter::Nats | StorageAdapter::Fluvio | StorageAdapter::Kafka => {
                bail!("storage {:?} requires install_async()", self.matrix.storage);
            }
            StorageAdapter::Sqlite => {
                bail!("storage Sqlite requires install_async()");
            }
        }
    }

    /// Async install for broker storage adapters.
    ///
    /// # Errors
    ///
    /// Returns an error if the operation fails.
    pub async fn install_async(&mut self) -> Result<()> {
        telemetry::apply_telemetry(self.matrix.telemetry);
        if self.matrix.storage == StorageAdapter::Mem {
            return self.install();
        }
        self.storage_port =
            Some(crate::backends::install_storage_port_async(self.matrix.storage).await?);
        self.ready = true;
        Ok(())
    }

    /// Build a [`Photon`] for the session matrix after [`install`](Self::install).
    ///
    /// # Errors
    ///
    /// Returns an error if the operation fails.
    pub fn build_photon(&self) -> Result<Photon> {
        if !self.ready {
            bail!("BootstrapSession::install must succeed before build_photon");
        }
        let port = self
            .storage_port
            .clone()
            .ok_or_else(|| anyhow::anyhow!("storage port missing after install"))?;
        let photon = Photon::builder()
            .storage_port(port)
            .auto_registry()
            .build()
            .map_err(anyhow::Error::from)?;
        Ok(topology::finish_photon_for_topology(
            photon,
            self.matrix.topology,
        ))
    }

    /// Whether [`install`](Self::install) completed successfully.
    #[must_use]
    pub const fn is_ready(&self) -> bool {
        self.ready
    }
}
