//! `SQLite` embedded [`StoragePort`](photon_backend::StoragePort) for durable single-process Photon.
//!
//! Wire with [`PhotonBuilder::storage_port`](https://docs.rs/uf-photon/latest/photon/struct.PhotonBuilder.html#method.storage_port)
//! after [`SqliteStoragePort::from_env`](SqliteStoragePort::from_env) or [`SqliteStoragePort::open`](SqliteStoragePort::open).

#![cfg_attr(test, allow(clippy::unwrap_used, clippy::expect_used))]
mod config;
mod port;

pub use config::{sqlite_path_from_env, PATH_ENV};
pub use port::SqliteStoragePort;
