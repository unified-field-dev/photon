//! Process-wide [`OpsLog`] install.

use std::sync::{Arc, OnceLock, RwLock};

use super::{ConsoleOpsLog, NoOpsLog, OpsLog};

static GLOBAL: OnceLock<RwLock<Option<Arc<dyn OpsLog>>>> = OnceLock::new();

fn slot() -> &'static RwLock<Option<Arc<dyn OpsLog>>> {
    GLOBAL.get_or_init(|| RwLock::new(None))
}

/// Install the process-wide ops log (typically at server boot before Photon runtime).
///
/// Recovers from a poisoned lock so a prior panicking holder cannot brick install.
pub fn install_ops_log(log: Arc<dyn OpsLog>) {
    let mut guard = slot()
        .write()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    *guard = Some(log);
}

/// Resolved ops log — [`NoOpsLog`] until [`install_ops_log`].
#[must_use]
pub fn ops_log() -> Arc<dyn OpsLog> {
    slot()
        .read()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .clone()
        .unwrap_or_else(|| Arc::new(NoOpsLog))
}

/// Resolve from `PHOTON_TELEMETRY` (`off` | `console`; default `console`).
///
/// External persisted ops-log adapters are installed by the host at boot.
#[must_use]
pub fn ops_log_from_env() -> Arc<dyn OpsLog> {
    match std::env::var("PHOTON_TELEMETRY")
        .ok()
        .map(|v| v.trim().to_ascii_lowercase())
        .as_deref()
    {
        Some("off" | "0" | "false" | "none") => Arc::new(NoOpsLog),
        _ => Arc::new(ConsoleOpsLog),
    }
}
