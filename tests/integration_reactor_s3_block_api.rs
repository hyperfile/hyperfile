//! Closure-scoped block access on the reactor surface
//! (`fh_with_block` / `fh_with_block_mut`).
//!
//! The direct API hands out borrow guards (`Hyper::fs_block` returns a
//! `BlockRef`); the reactor surface cannot, because the `Hyper` lives
//! inside the reactor task and a borrow escaping to the caller would
//! outlive the message that produced it. So the caller's action
//! travels to the block instead, runs inside the reactor task while it
//! holds the real borrow, and only owned values cross the channel.
//!
//! These tests check that the closure form is semantically equivalent
//! to the direct form, that values move in and out of the closure, and
//! that a hole still reports as `None` without running the closure.
//!
//! ```bash
//! HYPERFILE_TEST_BUCKET=<your-bucket> HYPERFILE_TEST_REGION=<your-region> \
//!     cargo test --test integration_reactor_s3_block_api -- --ignored --test-threads=1
//! ```

#![cfg(feature = "reactor")]

#[allow(dead_code)]
mod common;
#[allow(dead_code)]
mod common_reactor;

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use common::*;
use common_reactor::*;

use hyperfile::file::fh::HyperFileHandler;
use hyperfile::file::flags::FileFlags;
use hyperfile::file::mode::FileMode;

const BLK: usize = 4096;

/// Read access: the closure sees the block's bytes and its return
/// value comes back to the caller.
#[tokio::test]
#[ignore]
async fn with_block_returns_the_closure_value() {
    let _ = env_logger::try_init();
    let client = make_client().await;
    let tf = TestFile::new(&client).await;
    let reactor = make_reactor();

    let mut fh = HyperFileHandler::fh_open_or_create_with_default_opt(
        &reactor, &client, tf.uri(), FileFlags::rdwr(), FileMode::default_file(),
    ).await.expect("fh create");

    let mut payload = vec![0u8; 2 * BLK];
    payload[0..BLK].fill(0xA1);
    payload[BLK..].fill(0xB2);
    payload[7] = 0x5A;
    let _ = fh.fh_write(0, &payload).await.expect("fh_write");
    let _ = fh.fh_flush().await.expect("fh_flush");

    // Return a scalar computed from the block.
    let byte = fh.fh_with_block(0, |buf| buf[7]).await
        .expect("fh_with_block")
        .expect("block 0 is mapped");
    assert_eq!(byte, 0x5A);

    // Return an owned aggregate, and check the slice length.
    let (len, first, last) = fh.fh_with_block(1, |buf| (buf.len(), buf[0], buf[buf.len() - 1])).await
        .expect("fh_with_block")
        .expect("block 1 is mapped");
    assert_eq!(len, BLK, "the borrow must be exactly one block long");
    assert_eq!(first, 0xB2);
    assert_eq!(last, 0xB2);

    // Return a heap value moved out of the closure.
    let copy = fh.fh_with_block(0, |buf| buf[0..4].to_vec()).await
        .expect("fh_with_block")
        .expect("mapped");
    assert_eq!(copy, vec![0xA1, 0xA1, 0xA1, 0xA1]);

    let _ = fh.fh_release().await.expect("fh_release");
    tf.cleanup(&client).await;
}

/// A hole reports `None` and must not run the closure.
#[tokio::test]
#[ignore]
async fn with_block_skips_the_closure_on_a_hole() {
    let _ = env_logger::try_init();
    let client = make_client().await;
    let tf = TestFile::new(&client).await;
    let reactor = make_reactor();

    let mut fh = HyperFileHandler::fh_open_or_create_with_default_opt(
        &reactor, &client, tf.uri(), FileFlags::rdwr(), FileMode::default_file(),
    ).await.expect("fh create");

    // Write only block 2, leaving 0 and 1 as holes.
    let _ = fh.fh_write(2 * BLK, &vec![0xDDu8; BLK]).await.expect("fh_write");
    let _ = fh.fh_flush().await.expect("fh_flush");

    let calls = Arc::new(AtomicUsize::new(0));

    let c = calls.clone();
    let got = fh.fh_with_block(1, move |_| { c.fetch_add(1, Ordering::SeqCst); 1u8 }).await
        .expect("fh_with_block on a hole");
    assert!(got.is_none(), "a hole must report None");
    assert_eq!(calls.load(Ordering::SeqCst), 0, "the closure must not run for a hole");

    // create=false on the write side behaves the same.
    let c = calls.clone();
    let got = fh.fh_with_block_mut(1, false, move |_| { c.fetch_add(1, Ordering::SeqCst); 1u8 }).await
        .expect("fh_with_block_mut on a hole");
    assert!(got.is_none());
    assert_eq!(calls.load(Ordering::SeqCst), 0);

    // And the mapped block does run it.
    let c = calls.clone();
    let got = fh.fh_with_block(2, move |buf| { c.fetch_add(1, Ordering::SeqCst); buf[0] }).await
        .expect("fh_with_block")
        .expect("block 2 is mapped");
    assert_eq!(got, 0xDD);
    assert_eq!(calls.load(Ordering::SeqCst), 1);

    let _ = fh.fh_release().await.expect("fh_release");
    tf.cleanup(&client).await;
}

/// In-place modification through the closure is persisted by the next
/// flush, with no write-back call, and is visible after a reopen.
#[tokio::test]
#[ignore]
async fn with_block_mut_modifies_in_place_and_persists() {
    let _ = env_logger::try_init();
    let client = make_client().await;
    let tf = TestFile::new(&client).await;
    let reactor = make_reactor();

    {
        let mut fh = HyperFileHandler::fh_open_or_create_with_default_opt(
            &reactor, &client, tf.uri(), FileFlags::rdwr(), FileMode::default_file(),
        ).await.expect("fh create");
        let _ = fh.fh_write(0, &vec![0x11u8; 2 * BLK]).await.expect("fh_write");
        let _ = fh.fh_flush().await.expect("fh_flush");
        let _ = fh.fh_release().await.expect("fh_release");
    }

    // Reopen so the block is loaded from staging, then edit it. Values
    // are moved into the closure, which is what the Send + 'static
    // bounds require.
    {
        let mut fh = HyperFileHandler::fh_open(
            &reactor, &client, tf.uri(), FileFlags::rdwr(),
        ).await.expect("fh open");

        let marker: Vec<u8> = vec![0xDE, 0xAD, 0xBE, 0xEF];
        let at = 16usize;
        let overwritten = fh.fh_with_block_mut(1, false, move |buf| {
            // Existing content must be there to modify.
            assert!(buf.iter().all(|b| *b == 0x11), "block_mut got a blank buffer");
            let old = buf[at];
            buf[at..at + marker.len()].copy_from_slice(&marker);
            buf[BLK - 1] = 0x99;
            old
        }).await.expect("fh_with_block_mut").expect("block 1 is mapped");
        assert_eq!(overwritten, 0x11, "closure should observe and return the old byte");

        // No write-back call.
        let _ = fh.fh_flush().await.expect("fh_flush");
        let _ = fh.fh_release().await.expect("fh_release");
    }

    // Verify through the byte API.
    {
        let mut fh = HyperFileHandler::fh_open(
            &reactor, &client, tf.uri(), FileFlags::rdonly(),
        ).await.expect("fh open");
        let mut buf = vec![0u8; BLK];
        let n = fh.fh_read(BLK, &mut buf).await.expect("fh_read");
        assert_eq!(n, BLK);
        assert_eq!(&buf[16..20], &[0xDE, 0xAD, 0xBE, 0xEF], "edit not persisted");
        assert_eq!(buf[BLK - 1], 0x99);
        assert!(buf[20..BLK - 1].iter().all(|b| *b == 0x11), "remainder damaged");
        let _ = fh.fh_release().await.expect("fh_release");
    }

    tf.cleanup(&client).await;
}

/// `create` materializes a zero-filled block, and repeated edits in
/// one flush window produce a single version.
#[tokio::test]
#[ignore]
async fn with_block_mut_create_and_repeated_edits() {
    let _ = env_logger::try_init();
    let client = make_client().await;
    let tf = TestFile::new(&client).await;
    let reactor = make_reactor();

    let mut fh = HyperFileHandler::fh_open_or_create_with_default_opt(
        &reactor, &client, tf.uri(), FileFlags::rdwr(), FileMode::default_file(),
    ).await.expect("fh create");

    let _ = fh.fh_write(0, &vec![0x77u8; BLK]).await.expect("fh_write");
    let _ = fh.fh_flush().await.expect("fh_flush");
    let before = fh.fh_last_cno().await.expect("fh_last_cno");

    // Create a block in a hole; it must be zeros.
    let was_zero = fh.fh_with_block_mut(9, true, |buf| {
        let z = buf.iter().all(|b| *b == 0);
        buf[0] = 0x01;
        z
    }).await.expect("create").expect("create=true yields a block");
    assert!(was_zero, "a created block must be zero-filled");

    // Eight more edits to the same block, each its own round trip.
    for i in 1..9u8 {
        let _ = fh.fh_with_block_mut(9, false, move |buf| { buf[i as usize] = i + 1; }).await
            .expect("edit").expect("mapped");
    }

    let _ = fh.fh_flush().await.expect("fh_flush");
    let after = fh.fh_last_cno().await.expect("fh_last_cno");
    assert_eq!(after, before + 1,
        "nine edits in one flush window produced {} checkpoints, expected 1", after - before);

    // Every edit survived.
    let bytes = fh.fh_with_block(9, |buf| buf[0..9].to_vec()).await
        .expect("read back").expect("mapped");
    assert_eq!(bytes, vec![0x01, 2, 3, 4, 5, 6, 7, 8, 9]);
    // And the rest is still zeros.
    let rest_zero = fh.fh_with_block(9, |buf| buf[9..].iter().all(|b| *b == 0)).await
        .expect("read back").expect("mapped");
    assert!(rest_zero, "untouched remainder of a created block must be zeros");

    let _ = fh.fh_release().await.expect("fh_release");
    tf.cleanup(&client).await;
}

/// Access mode is enforced, and the error surfaces through the channel
/// rather than killing the reactor task — the handle stays usable.
#[tokio::test]
#[ignore]
async fn with_block_enforces_access_mode() {
    let _ = env_logger::try_init();
    let client = make_client().await;
    let tf = TestFile::new(&client).await;
    let reactor = make_reactor();

    {
        let mut fh = HyperFileHandler::fh_open_or_create_with_default_opt(
            &reactor, &client, tf.uri(), FileFlags::rdwr(), FileMode::default_file(),
        ).await.expect("fh create");
        let _ = fh.fh_write(0, &vec![0x33u8; BLK]).await.expect("fh_write");
        let _ = fh.fh_flush().await.expect("fh_flush");
        let _ = fh.fh_release().await.expect("fh_release");
    }

    {
        let mut fh = HyperFileHandler::fh_open(
            &reactor, &client, tf.uri(), FileFlags::rdonly(),
        ).await.expect("fh open rdonly");

        let e = fh.fh_with_block_mut(0, false, |_| ()).await
            .expect_err("write access on a read-only handle must fail");
        assert_eq!(e.raw_os_error(), Some(libc::EBADF), "expected EBADF, got {e}");

        // The reactor task survived the rejected request.
        let b = fh.fh_with_block(0, |buf| buf[0]).await
            .expect("read access still works").expect("mapped");
        assert_eq!(b, 0x33);
        let _ = fh.fh_release().await.expect("fh_release");
    }

    {
        let mut fh = HyperFileHandler::fh_open(
            &reactor, &client, tf.uri(), FileFlags::wronly(),
        ).await.expect("fh open wronly");

        let e = fh.fh_with_block(0, |buf| buf[0]).await
            .expect_err("read access on a write-only handle must fail");
        assert_eq!(e.raw_os_error(), Some(libc::EBADF), "expected EBADF, got {e}");

        let _ = fh.fh_with_block_mut(0, false, |buf| { buf[1] = 0x44; }).await
            .expect("write access works").expect("mapped");
        let _ = fh.fh_flush().await.expect("fh_flush");
        let _ = fh.fh_release().await.expect("fh_release");
    }

    tf.cleanup(&client).await;
}

/// The closure form is equivalent to the direct guard form: the same
/// sequence of operations through `Hyper::fs_block*` and through
/// `fh_with_block*` must produce the same bytes on S3.
#[tokio::test]
#[ignore]
async fn closure_form_matches_the_direct_guard_form() {
    let _ = env_logger::try_init();
    let client = make_client().await;
    let reactor = make_reactor();

    // Same edits applied through both surfaces.
    let edits: Vec<(u64, usize, u8)> = vec![(0, 0, 0xA), (0, 4095, 0xB), (3, 100, 0xC), (7, 7, 0xD)];

    // Direct surface.
    let tf_direct = TestFile::new(&client).await;
    {
        use hyperfile::file::hyper::Hyper;
        let mut h = Hyper::fs_open_or_create_with_default_opt(
            &client, tf_direct.uri(), FileFlags::rdwr(), FileMode::default_file(),
        ).await.expect("direct create");
        let _ = h.fs_write(0, &vec![0x20u8; 8 * BLK]).await.expect("write");
        let _ = h.fs_flush().await.expect("flush");
        for (idx, off, val) in &edits {
            let mut blk = h.fs_block_mut(*idx, false).await.expect("fs_block_mut").expect("mapped");
            blk.as_mut_slice()[*off] = *val;
        }
        let _ = h.fs_flush().await.expect("flush");
        let _ = h.fs_release().await.expect("release");
    }

    // Reactor surface.
    let tf_reactor = TestFile::new(&client).await;
    {
        let mut fh = HyperFileHandler::fh_open_or_create_with_default_opt(
            &reactor, &client, tf_reactor.uri(), FileFlags::rdwr(), FileMode::default_file(),
        ).await.expect("fh create");
        let _ = fh.fh_write(0, &vec![0x20u8; 8 * BLK]).await.expect("write");
        let _ = fh.fh_flush().await.expect("flush");
        for (idx, off, val) in &edits {
            let (off, val) = (*off, *val);
            let _ = fh.fh_with_block_mut(*idx, false, move |buf| { buf[off] = val; }).await
                .expect("fh_with_block_mut").expect("mapped");
        }
        let _ = fh.fh_flush().await.expect("flush");
        let _ = fh.fh_release().await.expect("release");
    }

    // Compare the two files block for block.
    {
        use hyperfile::file::hyper::Hyper;
        let mut a = Hyper::fs_open(&client, tf_direct.uri(), FileFlags::rdonly()).await.expect("open a");
        let mut b = Hyper::fs_open(&client, tf_reactor.uri(), FileFlags::rdonly()).await.expect("open b");
        assert_eq!(a.fs_getattr().expect("stat a").st_size, b.fs_getattr().expect("stat b").st_size);
        for i in 0..8usize {
            let mut ba = vec![0u8; BLK];
            let mut bb = vec![0u8; BLK];
            let _ = a.fs_read(i * BLK, &mut ba).await.expect("read a");
            let _ = b.fs_read(i * BLK, &mut bb).await.expect("read b");
            assert_eq!(ba, bb, "block {i} differs between the direct and closure forms");
        }
        let _ = a.fs_release().await.expect("release a");
        let _ = b.fs_release().await.expect("release b");
    }

    tf_direct.cleanup(&client).await;
    tf_reactor.cleanup(&client).await;
}
