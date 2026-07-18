//! [`Photon`] builder — storage port + backend assembly.
//!
//! See the crate [Getting started](https://docs.rs/uf-photon/latest/photon/#getting-started)
//! for Mode 1 (embedded) and Mode 2 (brokered) walkthroughs.

use std::sync::Arc;

use photon_telemetry::{install_ops_log, OpsLog};

use photon_backend::{
    instrumentation, BackendContext, EmbeddedBackend, ExecutorServices, GenericPhotonBackend,
    InProcStoragePort, PhotonBackend, PhotonError, Result, RetentionHook, RetentionPolicy,
    StoragePort, TopicRegistry, TransportCrypto,
};

use crate::executor::ExecutorController;
use crate::{Photon, PhotonRuntimeState};

type BackendInstallFn =
    Box<dyn FnOnce(BackendContext) -> Result<Arc<dyn PhotonBackend>> + Send>;

/// Builder for constructing [`Photon`] runtimes.
///
/// Wire a [`StoragePort`] (or accept the default in-process port), optionally install inventory
/// discovery and ops telemetry, then [`build`](Self::build). Keep the returned [`Photon`] handle
/// for `publish_on` / `subscribe_on`.
///
/// | Mode | Typical wiring |
/// |------|----------------|
/// | **Mode 1 — Embedded** | Default builder, or [`storage_port`](Self::storage_port) with `SQLite` |
/// | **Mode 2 — Brokered** | [`storage_port`](Self::storage_port) with NATS/Kafka/Fluvio on **every** binary |
///
/// Getting started: [Mode 1](https://docs.rs/uf-photon/latest/photon/#mode-1--embedded-one-binary),
/// [Mode 2](https://docs.rs/uf-photon/latest/photon/#mode-2--brokered-publisher--worker-binaries).
///
/// # Examples
///
/// Default mem path (loads `PHOTON_TRANSPORT_KEY` via [`TransportCrypto::from_env`]):
///
/// ```rust,no_run
/// use photon_runtime::Photon;
///
/// # fn main() -> photon_backend::Result<()> {
/// let _photon = Photon::builder().auto_registry().build()?;
/// # Ok(())
/// # }
/// ```
///
/// Explicit storage port:
///
/// ```rust,no_run
/// use std::sync::Arc;
///
/// use photon_backend::{InProcStoragePort, StoragePort, TransportCrypto};
/// use photon_runtime::Photon;
///
/// # fn main() -> photon_backend::Result<()> {
/// let port: Arc<dyn StoragePort> = Arc::new(InProcStoragePort::new(
///     TransportCrypto::from_env()?,
/// ));
/// let _photon = Photon::builder().storage_port(port).auto_registry().build()?;
/// # Ok(())
/// # }
/// ```
#[derive(Default)]
pub struct PhotonBuilder {
    storage_port: Option<Arc<dyn StoragePort>>,
    backend: Option<Arc<dyn PhotonBackend>>,
    backend_install: Option<BackendInstallFn>,
    use_auto_registry: bool,
    ops_log: Option<Arc<dyn OpsLog>>,
    retention_policy: Option<RetentionPolicy>,
    retention_hook: Option<Arc<dyn RetentionHook>>,
}

impl PhotonBuilder {
    /// Explicit storage port (defaults to in-process `mem` via [`InProcStoragePort`]).
    ///
    /// Use this for `SQLite` (Mode 1 durable) and for broker adapters (Mode 2 — same port config on
    /// publisher and worker). See
    /// [Getting started → Mode 2](https://docs.rs/uf-photon/latest/photon/#mode-2--brokered-publisher--worker-binaries).
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// use std::sync::Arc;
    ///
    /// use photon_backend::{InProcStoragePort, StoragePort, TransportCrypto};
    /// use photon_runtime::Photon;
    ///
    /// # fn main() -> photon_backend::Result<()> {
    /// let port: Arc<dyn StoragePort> = Arc::new(InProcStoragePort::new(
    ///     TransportCrypto::from_env()?,
    /// ));
    /// let _photon = Photon::builder().storage_port(port).auto_registry().build()?;
    /// # Ok(())
    /// # }
    /// ```
    #[must_use]
    pub fn storage_port(mut self, port: Arc<dyn StoragePort>) -> Self {
        self.storage_port = Some(port);
        self
    }

    /// Pre-built backend instance.
    ///
    /// Prefer [`storage_port`](Self::storage_port) for normal hosts. Use this when you already
    /// constructed a [`PhotonBackend`] (tests, custom delivery stacks).
    ///
    /// # Errors
    ///
    /// [`build`](Self::build) returns an error if both this and [`backend_with_context`](Self::backend_with_context)
    /// are set.
    #[must_use]
    pub fn backend(mut self, backend: Arc<dyn PhotonBackend>) -> Self {
        self.backend = Some(backend);
        self.backend_install = None;
        self
    }

    /// Build backend from shared [`BackendContext`] (custom install closures).
    ///
    /// Prefer [`storage_port`](Self::storage_port) for adapter wiring. This is for hosts that
    /// install a custom [`PhotonBackend`] from the registry context.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// use std::sync::Arc;
    ///
    /// use photon_backend::{BackendContext, EmbeddedBackend};
    /// use photon_runtime::Photon;
    ///
    /// # fn main() -> photon_backend::Result<()> {
    /// let _photon = Photon::builder()
    ///     .backend_with_context(|ctx: BackendContext| EmbeddedBackend::install_mem(ctx))
    ///     .auto_registry()
    ///     .build()?;
    /// # Ok(())
    /// # }
    /// ```
    #[must_use]
    pub fn backend_with_context<F>(mut self, install: F) -> Self
    where
        F: FnOnce(BackendContext) -> Result<Arc<dyn PhotonBackend>> + Send + 'static,
    {
        self.backend = None;
        self.backend_install = Some(Box::new(install));
        self
    }

    /// Shorthand for [`backend_with_context`](Self::backend_with_context) with
    /// [`EmbeddedBackend::install_mem`](photon_backend::GenericPhotonBackend::install_mem).
    ///
    /// Equivalent to the default path when you also omit [`storage_port`](Self::storage_port)
    /// (in-process mem). Prefer the default [`build`](Self::build) unless you need an explicit
    /// install fn.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// use photon_runtime::Photon;
    ///
    /// # fn main() -> photon_backend::Result<()> {
    /// let _photon = Photon::builder().mem_backend().auto_registry().build()?;
    /// # Ok(())
    /// # }
    /// ```
    #[must_use]
    pub fn mem_backend(mut self) -> Self {
        self.backend = None;
        self.backend_install = Some(Box::new(EmbeddedBackend::install_mem));
        self
    }

    /// Install a concrete [`OpsLog`] adapter before build.
    ///
    /// Runnable: `cargo run -p uf-photon --example telemetry_ops_log --features runtime,mem`.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// use photon_runtime::Photon;
    /// use photon_telemetry::ConsoleOpsLog;
    ///
    /// # fn main() -> photon_backend::Result<()> {
    /// let _photon = Photon::builder()
    ///     .ops_log(ConsoleOpsLog)
    ///     .auto_registry()
    ///     .build()?;
    /// # Ok(())
    /// # }
    /// ```
    #[must_use]
    pub fn ops_log(mut self, log: impl OpsLog + 'static) -> Self {
        self.ops_log = Some(Arc::new(log));
        self
    }

    /// Install a shared [`OpsLog`] trait object before build.
    #[must_use]
    pub fn ops_log_arc(mut self, log: Arc<dyn OpsLog>) -> Self {
        self.ops_log = Some(log);
        self
    }

    /// Discover `#[photon::topic]` / `#[photon::subscribe]` descriptors via Quark inventory.
    ///
    /// Required when using the macros in the same crate graph as the host. Without this, the
    /// topic registry stays empty and the executor has nothing to dispatch.
    ///
    /// Runnable: `cargo run -p uf-photon --example embedded_mem --features runtime,mem`.
    #[must_use]
    pub const fn auto_registry(mut self) -> Self {
        self.use_auto_registry = true;
        self
    }

    /// Override default retention policy (env fallbacks apply for unset fields).
    #[must_use]
    pub fn retention_policy(mut self, policy: RetentionPolicy) -> Self {
        self.retention_policy = Some(policy);
        self
    }

    /// Host hook for extra subscriptions and legal-hold floors.
    #[must_use]
    pub fn retention_hook(mut self, hook: Arc<dyn RetentionHook>) -> Self {
        self.retention_hook = Some(hook);
        self
    }

    /// Assemble the [`Photon`] runtime.
    ///
    /// # Defaults
    ///
    /// - **Storage port:** [`InProcStoragePort`] with [`TransportCrypto::from_env`] when
    ///   [`storage_port`](Self::storage_port) was not set — requires `PHOTON_TRANSPORT_KEY`
    /// - **Backend:** generic backend over that port when no custom [`backend`](Self::backend) /
    ///   [`backend_with_context`](Self::backend_with_context) is set
    /// - **Registry:** empty unless [`auto_registry`](Self::auto_registry) was called
    ///
    /// # Errors
    ///
    /// Returns an error if transport crypto cannot load from the environment, a custom install
    /// fn fails, or both `backend` and `backend_with_context` were set.
    pub fn build(self) -> Result<Photon> {
        if let Some(log) = self.ops_log {
            install_ops_log(log);
        }

        let registry = if self.use_auto_registry {
            TopicRegistry::auto_discover()
        } else {
            TopicRegistry::new()
        };

        let port = match self.storage_port {
            Some(port) => port,
            None => Arc::new(InProcStoragePort::new(TransportCrypto::from_env()?)),
        };

        let ctx = BackendContext {
            registry: registry.clone(),
        };

        let backend = match (self.backend, self.backend_install) {
            (Some(b), None) => b,
            (None, Some(install)) => install(ctx)?,
            (None, None) => GenericPhotonBackend::install_with_port(
                BackendContext { registry },
                Arc::clone(&port),
            )?,
            (Some(_), Some(_)) => {
                return Err(PhotonError::Internal(
                    "PhotonBuilder: set backend() or backend_with_context(), not both".into(),
                ));
            }
        };

        let retention_policy = self.retention_policy.unwrap_or_default();
        let runtime = PhotonRuntimeState {
            storage_port: Arc::clone(&port),
            executor_services: Arc::new(ExecutorServices::new(
                port,
                retention_policy,
                self.retention_hook,
            )),
            executor: Arc::new(ExecutorController::default()),
        };

        let backend = instrumentation::wrap_backend(backend);
        Ok(Photon::new(backend, runtime))
    }
}
