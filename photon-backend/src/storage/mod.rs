//! Storage adapter port — one implementation per `--storage` tier (`mem`, `sqlite`, `nats`, `fluvio`, `kafka`).
//!
//! [`StoragePort`] is the primary extension point. [`GenericPhotonBackend`](crate::backend::GenericPhotonBackend)
//! maps [`PhotonBackend`](crate::backend::PhotonBackend) calls onto port methods.
//!
//! ## Contract summary
//!
//! | Method | Role |
//! |--------|------|
//! | `append` | Persist one event; assign monotonic `seq` per partition |
//! | `subscribe` | Stream events; optional replay after `after_seq` |
//! | `get_checkpoint_seq` / `set_checkpoint` | Durable subscription cursors |
//! | `get_event` | Optional point lookup (`mem` / `sqlite`; brokers set `supports_get_event: false`) |
//!
//! ## Capabilities
//!
//! | Field | `mem` / `sqlite` | Brokers |
//! |-------|------------------|---------|
//! | `supports_get_event` | `true` | `false` |
//! | `max_replay_window` | unbounded / file-backed | ~15 min broker retention |
//!
//! See also: [`crate::checkpoint`], [`crate::retention`], [`crate::backend`].

mod in_proc;
mod partition;
mod port;

pub use in_proc::InProcStoragePort;
pub use partition::topic_filter_matches;
pub use port::{StorageCapabilities, StoragePort};
