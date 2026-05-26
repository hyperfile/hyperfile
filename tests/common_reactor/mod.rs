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
//! The whole module is gated on `feature = "reactor"` because none
//! of the reactor APIs (`HyperFileHandler`, `HyperFileTokio`,
//! `Reactor`) exist without it.

#![cfg(feature = "reactor")]
#![allow(dead_code)]

use hyperfile::file::hyper::Hyper;
use hyperfile::file::handler::FileContext;
use hyperfile_reactor::Reactor;

/// Type alias for the reactor used by hyperfile tests. The reactor
/// owns an OS thread running a current-thread tokio runtime that
/// hosts the `Hyper` instance and its handler loop.
pub type HyperReactor = Reactor<FileContext<'static>, Hyper<'static>>;

/// Create a fresh reactor using `new_current` (current-thread
/// runtime).
///
/// When the returned reactor is dropped (along with every
/// `HyperFileHandler` clone derived from it), the backing OS
/// thread winds down once its handler loops exit and all tasks
/// complete. Tests do not need to join the thread explicitly; the
/// process will reap it.
pub fn make_reactor() -> HyperReactor {
    Reactor::new_current().expect("failed to create reactor")
}
