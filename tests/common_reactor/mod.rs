//! Shared helpers for reactor-mode integration tests.
//!
//! Each `tests/integration_reactor_s3_*.rs` binary imports both
//! `common/` (for `TestFile`, env config, interceptors) and this
//! module for the reactor-specific bits:
//!
//! ```ignore
//! #[allow(dead_code)]
//! mod common;
//! #[allow(dead_code)]
//! mod common_reactor;
//! use common::*;
//! use common_reactor::*;
//! ```
//!
//! The whole module is gated on `feature = "reactor"` because none of
//! the reactor APIs (`HyperFileHandler`, `HyperFileTokio`,
//! `LocalSpawner`) exist without it.

#![cfg(feature = "reactor")]
#![allow(dead_code)]

use hyperfile::file::hyper::Hyper;
use hyperfile::file::handler::FileContext;
use hyperfile_reactor::LocalSpawner;

/// Type alias for the reactor spawner used by hyperfile tests. The
/// spawner starts an OS thread running a current-thread tokio runtime
/// that owns the `Hyper` instances and their handler loops.
pub type HyperSpawner = LocalSpawner<FileContext<'static>, Hyper<'static>>;

/// Create a fresh spawner using `new_current` (current-thread runtime).
///
/// When the returned spawner (and every `HyperFileHandler` / handler
/// clone derived from it) is dropped, the backing OS thread winds down
/// once its handler loops exit and all tasks complete. Tests do not
/// need to join the thread explicitly; the process will reap it.
pub fn make_spawner() -> HyperSpawner {
    LocalSpawner::new_current()
}
