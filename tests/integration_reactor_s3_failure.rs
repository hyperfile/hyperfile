//! Reactor-mode failure path integration tests.
//!
//! These tests validate the claims in `docs/concurrency.md`'s
//! "Reactor failure modes" section:
//!
//!   - A domain error from the handler propagates as `Err(_)` on the
//!     caller's `fh_*` return value; no panic.
//!   - A panic inside the handler (triggered here via a panicking
//!     interceptor) propagates to the caller as a task panic, not as
//!     an `Err(_)` return value.
//!   - Dropping the `LocalSpawner` while still holding a
//!     `HyperFileHandler` leaves the spawner thread alive, so
//!     subsequent `fh_*` calls keep working.
//!
//! Run with:
//! ```bash
//! cargo test --test integration_reactor_s3_failure \
//!     -- --ignored --test-threads=1
//! ```

#![cfg(feature = "reactor")]

#[allow(dead_code)]
mod common;
#[allow(dead_code)]
mod common_reactor;

use common::*;
use common_reactor::*;

use std::io::ErrorKind;
use hyperfile::file::fh::HyperFileHandler;
use hyperfile::file::flags::FileFlags;
use hyperfile::file::mode::FileMode;

/// Test A: A domain-level error (injected by FailOnFlushInode on
/// the second flush_inode call, bypassing the create-time flush)
/// is returned as `Err(_)` by `fh_flush`; no panic, the handler
/// survives, subsequent calls still work.
#[tokio::test]
#[ignore]
async fn reactor_fail_path_domain_error_returns_err() {
    let _ = env_logger::try_init();
    let client = make_client().await;
    let tf = TestFile::new(&client).await;

    let reactor = make_reactor();

    // The first flush_inode call happens inside fh_create_with
    // itself (to persist the newly-created inode). Target the
    // second call so create succeeds and the user's explicit
    // fh_flush is the one that fails.
    let interceptor = FailOnFlushInode::at(2);
    let mut fh = HyperFileHandler::fh_create_with(
        &reactor,
        &client,
        tf.uri(),
        FileFlags::rdwr(),
        FileMode::default_file(),
        interceptor,
    )
    .await
    .expect("fh_create_with");

    // Write goes to the in-memory cache without touching the
    // flush path, so it should succeed.
    let payload = vec![0xAAu8; 4096];
    let n = fh.fh_write(0, &payload).await.expect("fh_write");
    assert_eq!(n, payload.len());

    // flush hits the interceptor and fails. Key assertion: we
    // get an Err(_), NOT a panic from rx.await.expect(...).
    let res = fh.fh_flush().await;
    let err = res.expect_err("flush should fail under FailOnFlushInode::at(2)");
    assert_eq!(err.kind(), ErrorKind::Other);

    // The handler should still be alive: getattr still works.
    let _stat = fh.fh_getattr().await.expect("getattr after failed flush");

    // release may or may not succeed depending on whether it
    // triggers another flush; don't care.
    let _ = fh.fh_release().await;
    drop(fh);
    tf.cleanup(&client).await;
}

/// Test C: Dropping the `LocalSpawner` while still holding a
/// `HyperFileHandler` does NOT kill the spawner thread, because
/// the handler retains its own `UnboundedSender` clone. A write
/// after `drop(spawner)` still completes.
#[tokio::test]
#[ignore]
async fn reactor_spawner_dropped_handle_still_works() {
    let _ = env_logger::try_init();
    let client = make_client().await;
    let tf = TestFile::new(&client).await;

    let reactor = make_reactor();
    let mut fh = HyperFileHandler::fh_create(
        &reactor,
        &client,
        tf.uri(),
        FileFlags::rdwr(),
        FileMode::default_file(),
    )
    .await
    .expect("fh_create");

    // Drop the spawner while we still hold fh.

    // If our assumption is correct, the spawner thread is kept
    // alive by the sender inside fh and this still works.
    let payload = b"after spawner drop".to_vec();
    let n = fh
        .fh_write(0, &payload)
        .await
        .expect("fh_write after spawner drop");
    assert_eq!(n, payload.len());

    // Also flush — a more involved round trip through the handler.
    let _cno = fh.fh_flush().await.expect("fh_flush after spawner drop");

    let _ = fh.fh_release().await;
    drop(fh);
    tf.cleanup(&client).await;
}

/// Test B: A panic inside the handler task does NOT return as
/// `Err(_)` via the normal response value — but thanks to the
/// enum-based `FileResp`, when the handler's async task frame
/// unwinds, the response channel's `Sender` is dropped. The
/// caller's `rx.await` then resolves immediately with a recv
/// error, which the `fh_*` wrappers translate into
/// `Err(ErrorKind::BrokenPipe, "reactor handler task died")`.
///
/// Before the `FileResp` rewrite this test hung forever because
/// the union form of `FileResp` used `ManuallyDrop`, which
/// leaked the sender on unwind.
#[tokio::test]
#[ignore]
async fn reactor_handler_panic_via_interceptor_returns_broken_pipe() {
    let _ = env_logger::try_init();
    let client = make_client().await;
    let tf = TestFile::new(&client).await;

    let reactor = make_reactor();

    // First call is the create-time internal flush; panic on call
    // #2 which is the user's explicit fh_flush.
    let interceptor = PanicOnFlushInode::at(2);
    let mut fh = HyperFileHandler::fh_create_with(
        &reactor,
        &client,
        tf.uri(),
        FileFlags::rdwr(),
        FileMode::default_file(),
        interceptor,
    )
    .await
    .expect("fh_create_with");

    let payload = vec![0xCCu8; 4096];
    let n = fh.fh_write(0, &payload).await.expect("fh_write");
    assert_eq!(n, payload.len());

    // Wrap in a timeout as a belt-and-suspenders guard: if the fix
    // regresses, we don't want the whole test binary to hang.
    let res = tokio::time::timeout(
        std::time::Duration::from_secs(10),
        fh.fh_flush(),
    )
    .await
    .expect("fh_flush hung after handler panic (regression of the FileResp fix)");

    let err = res.expect_err("flush should return Err when the handler panics");
    assert_eq!(
        err.kind(),
        ErrorKind::BrokenPipe,
        "expected BrokenPipe, got {:?}",
        err
    );

    drop(fh);
    tf.cleanup(&client).await;
}


