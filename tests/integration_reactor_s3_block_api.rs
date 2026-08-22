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

// --- counters on the reactor surface ---
//
// `fh_read_timing` / `fh_flush_timing` mirror the direct API's
// accessors, returning owned snapshots because a reference to the live
// counters cannot leave the reactor task. Having them makes the
// reactor surface's cache behaviour testable, which it was not before.

/// The cache rule documented for the byte and block APIs holds on the
/// reactor surface too: `fh_read` does not populate the data cache, so
/// reading the same block twice fetches twice; `fh_with_block` does, so
/// borrowing twice fetches once and a later `fh_read` is a hit.
#[tokio::test]
#[ignore]
async fn reactor_cache_population_matches_the_direct_api() {
    let _ = env_logger::try_init();
    let client = make_client().await;
    let tf = TestFile::new(&client).await;
    let reactor = make_reactor();

    {
        let mut fh = HyperFileHandler::fh_open_or_create_with_default_opt(
            &reactor, &client, tf.uri(), FileFlags::rdwr(), FileMode::default_file(),
        ).await.expect("fh create");
        let _ = fh.fh_write(0, &vec![0x5Au8; 8 * BLK]).await.expect("write");
        let _ = fh.fh_flush().await.expect("flush");
        let _ = fh.fh_release().await.expect("release");
    }

    // Byte reads: two reads of the same block, two fetches.
    {
        let mut fh = HyperFileHandler::fh_open(
            &reactor, &client, tf.uri(), FileFlags::rdonly(),
        ).await.expect("fh open");
        fh.fh_read_timing_reset().await.expect("reset");

        let mut buf = vec![0u8; BLK];
        let _ = fh.fh_read(3 * BLK, &mut buf).await.expect("first read");
        let first = fh.fh_read_timing().await.expect("timing");
        assert_eq!(first.data_gets, 1);
        assert_eq!(first.cache_hits, 0);

        let _ = fh.fh_read(3 * BLK, &mut buf).await.expect("second read");
        let second = fh.fh_read_timing().await.expect("timing");
        assert_eq!(second.data_gets, 2,
            "fh_read does not populate the cache, so the second read must fetch again");
        assert_eq!(second.cache_hits, 0);

        let _ = fh.fh_release().await.expect("release");
    }

    // Block access: two borrows of the same block, one fetch.
    {
        let mut fh = HyperFileHandler::fh_open(
            &reactor, &client, tf.uri(), FileFlags::rdonly(),
        ).await.expect("fh open");
        fh.fh_read_timing_reset().await.expect("reset");

        let b = fh.fh_with_block(3, |buf| buf[0]).await.expect("borrow").expect("mapped");
        assert_eq!(b, 0x5A);
        let first = fh.fh_read_timing().await.expect("timing");
        assert_eq!(first.data_gets, 1);

        let _ = fh.fh_with_block(3, |buf| buf[0]).await.expect("borrow again").expect("mapped");
        let second = fh.fh_read_timing().await.expect("timing");
        assert_eq!(second.data_gets, 1,
            "fh_with_block populates the cache, so a second borrow must not fetch");
        assert_eq!(second.cache_hits, 1);

        // And the byte path sees it.
        let mut buf = vec![0u8; BLK];
        let _ = fh.fh_read(3 * BLK, &mut buf).await.expect("read");
        let third = fh.fh_read_timing().await.expect("timing");
        assert_eq!(third.data_gets, 1, "a byte read of a cached block must not fetch");
        assert_eq!(third.cache_hits, 2);

        let _ = fh.fh_release().await.expect("release");
    }

    tf.cleanup(&client).await;
}

/// A read spanning contiguous blocks in one segment costs one request
/// on the reactor surface as well.
#[tokio::test]
#[ignore]
async fn reactor_contiguous_read_costs_one_request() {
    let _ = env_logger::try_init();
    let client = make_client().await;
    let tf = TestFile::new(&client).await;
    let reactor = make_reactor();

    const N: usize = 64;
    {
        let mut fh = HyperFileHandler::fh_open_or_create_with_default_opt(
            &reactor, &client, tf.uri(), FileFlags::rdwr(), FileMode::default_file(),
        ).await.expect("fh create");
        let _ = fh.fh_write(0, &vec![0x11u8; N * BLK]).await.expect("write");
        let _ = fh.fh_flush().await.expect("flush");
        let _ = fh.fh_release().await.expect("release");
    }

    let mut fh = HyperFileHandler::fh_open(
        &reactor, &client, tf.uri(), FileFlags::rdonly(),
    ).await.expect("fh open");
    fh.fh_read_timing_reset().await.expect("reset");

    let mut buf = vec![0u8; N * BLK];
    let n = fh.fh_read(0, &mut buf).await.expect("read");
    assert_eq!(n, N * BLK);
    assert!(buf.iter().all(|b| *b == 0x11));

    let t = fh.fh_read_timing().await.expect("timing");
    assert_eq!(t.data_gets, 1,
        "{N} contiguous blocks should coalesce into 1 request, got {}", t.data_gets);
    assert_eq!(t.data_bytes, (N * BLK) as u64);

    let _ = fh.fh_release().await.expect("release");
    tf.cleanup(&client).await;
}

/// Flush timings are reachable too, and both resets work.
#[tokio::test]
#[ignore]
async fn reactor_flush_timing_and_resets() {
    let _ = env_logger::try_init();
    let client = make_client().await;
    let tf = TestFile::new(&client).await;
    let reactor = make_reactor();

    let mut fh = HyperFileHandler::fh_open_or_create_with_default_opt(
        &reactor, &client, tf.uri(), FileFlags::rdwr(), FileMode::default_file(),
    ).await.expect("fh create");

    let _ = fh.fh_write(0, &vec![0x22u8; BLK]).await.expect("write");
    let _ = fh.fh_flush().await.expect("flush");

    let f = fh.fh_flush_timing().await.expect("flush timing");
    assert!(f.flush_count >= 1, "a flush should have been counted, got {}", f.flush_count);
    assert!(f.build_segment_ns > 0, "segment build time should be recorded");

    let mut buf = vec![0u8; BLK];
    let _ = fh.fh_read(0, &mut buf).await.expect("read");
    let r = fh.fh_read_timing().await.expect("read timing");
    assert!(r.cache_hits >= 1 || r.data_gets >= 1, "the read should have been counted");

    fh.fh_flush_timing_reset().await.expect("flush reset");
    fh.fh_read_timing_reset().await.expect("read reset");
    let f = fh.fh_flush_timing().await.expect("flush timing");
    let r = fh.fh_read_timing().await.expect("read timing");
    assert_eq!(f.flush_count, 0);
    assert_eq!(f.build_segment_ns, 0);
    assert_eq!(r.total_gets(), 0);
    assert_eq!(r.cache_hits, 0);

    let _ = fh.fh_release().await.expect("release");
    tf.cleanup(&client).await;
}

// --- borrowing closures ---
//
// `fh_with_block*` requires `Send` but not `'static`, so a closure may
// borrow the caller's locals. That is what lets a caller copy straight
// out of a block into a buffer it already owns, instead of returning an
// owned buffer and copying a second time.

/// The read direction: copy out of the block into a caller-owned
/// buffer, by mutable reference. Under a `'static` bound this could
/// not be expressed.
#[tokio::test]
#[ignore]
async fn with_block_closure_may_borrow_caller_locals() {
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
    let _ = fh.fh_write(0, &payload).await.expect("write");
    let _ = fh.fh_flush().await.expect("flush");

    // Borrow a local buffer mutably from inside the closure.
    let mut out = vec![0u8; BLK];
    let n = fh.fh_with_block(0, |blk| {
        out.copy_from_slice(blk);
        blk.len()
    }).await.expect("with_block").expect("mapped");
    assert_eq!(n, BLK);
    assert!(out.iter().all(|b| *b == 0xA1), "the closure should have filled the caller's buffer");

    // Borrow immutably too, and mix with a mutable borrow of something
    // else, to be sure the bound is genuinely just `Send`.
    let expect = 0xB2u8;
    let mut hits = 0usize;
    let all_match = fh.fh_with_block(1, |blk| {
        hits += 1;
        blk.iter().all(|b| *b == expect)
    }).await.expect("with_block").expect("mapped");
    assert!(all_match);
    assert_eq!(hits, 1, "the closure should have run exactly once");

    // The write direction: copy the caller's bytes in without an
    // intervening owned copy.
    let src: Vec<u8> = (0..64u8).collect();
    let written = fh.fh_with_block_mut(0, false, |blk| {
        blk[0..src.len()].copy_from_slice(&src);
        src.len()
    }).await.expect("with_block_mut").expect("mapped");
    assert_eq!(written, src.len());
    let _ = fh.fh_flush().await.expect("flush");

    let mut check = vec![0u8; 64];
    let _ = fh.fh_read(0, &mut check).await.expect("read back");
    assert_eq!(check, src, "the in-place write should have landed");

    let _ = fh.fh_release().await.expect("release");
    tf.cleanup(&client).await;
}

/// A closure that borrows a local must still be able to report an
/// error out, and a hole must leave the borrowed state untouched.
#[tokio::test]
#[ignore]
async fn borrowing_closure_on_a_hole_does_not_run() {
    let _ = env_logger::try_init();
    let client = make_client().await;
    let tf = TestFile::new(&client).await;
    let reactor = make_reactor();

    let mut fh = HyperFileHandler::fh_open_or_create_with_default_opt(
        &reactor, &client, tf.uri(), FileFlags::rdwr(), FileMode::default_file(),
    ).await.expect("fh create");
    let _ = fh.fh_write(2 * BLK, &vec![0xDDu8; BLK]).await.expect("write");
    let _ = fh.fh_flush().await.expect("flush");

    let mut touched = false;
    let got = fh.fh_with_block(1, |_| { touched = true; }).await.expect("with_block");
    assert!(got.is_none(), "a hole must report None");
    assert!(!touched, "the closure must not run for a hole, so the borrow is untouched");

    // The same local is still usable afterwards.
    let got = fh.fh_with_block(2, |blk| { touched = true; blk[0] }).await
        .expect("with_block").expect("mapped");
    assert_eq!(got, 0xDD);
    assert!(touched);

    let _ = fh.fh_release().await.expect("release");
    tf.cleanup(&client).await;
}

/// Cancelling the future must not leave the reactor running a closure
/// that points into a freed frame. `Drop` waits for the reactor to be
/// done, so the handle stays usable and nothing is corrupted.
///
/// This cannot observe the unsound version failing — that would be
/// undefined behavior, not a test failure — but it does exercise the
/// wait, and asserts the handle and the data survive.
#[tokio::test]
#[ignore]
async fn cancelling_a_borrowing_closure_is_safe() {
    let _ = env_logger::try_init();
    let client = make_client().await;
    let tf = TestFile::new(&client).await;
    let reactor = make_reactor();

    let mut fh = HyperFileHandler::fh_open_or_create_with_default_opt(
        &reactor, &client, tf.uri(), FileFlags::rdwr(), FileMode::default_file(),
    ).await.expect("fh create");
    let _ = fh.fh_write(0, &vec![0x33u8; 4 * BLK]).await.expect("write");
    let _ = fh.fh_flush().await.expect("flush");

    // Cancel repeatedly with a timeout short enough to often fire
    // before the reactor answers.
    for i in 0..32u64 {
        let mut scratch = vec![0u8; BLK];
        let fut = fh.fh_with_block(i % 4, |blk| {
            scratch.copy_from_slice(blk);
            scratch[0]
        });
        match tokio::time::timeout(std::time::Duration::from_micros(1), fut).await {
            Ok(Ok(Some(b))) => assert_eq!(b, 0x33),
            Ok(Ok(None)) => panic!("block {} should be mapped", i % 4),
            Ok(Err(e)) => panic!("with_block failed: {e}"),
            Err(_) => { /* cancelled; the guard waited for the reactor */ },
        }
        // `scratch` drops here. Under the unsound version the reactor
        // could still be writing into it.
    }

    // The handle is still usable and the data is intact.
    let b = fh.fh_with_block(0, |blk| blk[0]).await.expect("with_block").expect("mapped");
    assert_eq!(b, 0x33);
    let mut buf = vec![0u8; 4 * BLK];
    let n = fh.fh_read(0, &mut buf).await.expect("read");
    assert_eq!(n, 4 * BLK);
    assert!(buf.iter().all(|b| *b == 0x33), "data must be intact after cancellations");

    let _ = fh.fh_release().await.expect("release");
    tf.cleanup(&client).await;
}

// --- cancel-safe owned buffers ---
//
// `fh_read` and `fh_write` point the reactor at the caller's memory,
// which is only valid while the caller stays parked on the response, so
// they must not be cancelled. `fh_read_owned` and `fh_write_owned`
// borrow nothing of the caller's: the buffer belongs to the reactor for
// a read, and to the request for a write.

/// `fh_read_owned` returns what was read, truncated to the count, and
/// agrees with `fh_read` byte for byte.
#[tokio::test]
#[ignore]
async fn read_owned_matches_the_borrowing_read() {
    let _ = env_logger::try_init();
    let client = make_client().await;
    let tf = TestFile::new(&client).await;
    let reactor = make_reactor();

    let mut fh = HyperFileHandler::fh_open_or_create_with_default_opt(
        &reactor, &client, tf.uri(), FileFlags::rdwr(), FileMode::default_file(),
    ).await.expect("fh create");

    let mut payload = vec![0u8; 3 * BLK];
    for (i, b) in payload.iter_mut().enumerate() {
        *b = (i % 251) as u8;
    }
    let _ = fh.fh_write(0, &payload).await.expect("write");
    let _ = fh.fh_flush().await.expect("flush");

    // Whole file, and an unaligned window, both ways.
    for (off, len) in [(0usize, 3 * BLK), (100, BLK), (BLK + 7, 2 * BLK - 9)] {
        let owned = fh.fh_read_owned(off, len).await.expect("read_owned");
        let mut borrowed = vec![0u8; len];
        let n = fh.fh_read(off, &mut borrowed).await.expect("read");
        assert_eq!(owned.len(), n, "off={off} len={len}: owned length should be the count");
        assert_eq!(&owned[..], &borrowed[..n], "off={off} len={len}: contents differ");
        assert_eq!(&owned[..], &payload[off..off + n]);
    }

    // At and past EOF the result is empty rather than an error.
    let past = fh.fh_read_owned(3 * BLK, BLK).await.expect("read_owned past eof");
    assert!(past.is_empty(), "reading at EOF should yield no bytes, got {}", past.len());
    let past = fh.fh_read_owned(10 * BLK, BLK).await.expect("read_owned past eof");
    assert!(past.is_empty());

    let _ = fh.fh_release().await.expect("release");
    tf.cleanup(&client).await;
}

/// `fh_write_owned` goes through the same pipeline and persists the
/// same bytes.
#[tokio::test]
#[ignore]
async fn write_owned_matches_the_borrowing_write() {
    let _ = env_logger::try_init();
    let client = make_client().await;
    let tf = TestFile::new(&client).await;
    let reactor = make_reactor();

    let mut fh = HyperFileHandler::fh_open_or_create_with_default_opt(
        &reactor, &client, tf.uri(), FileFlags::rdwr(), FileMode::default_file(),
    ).await.expect("fh create");

    // A full block, then an unaligned partial write over it, so the
    // read-modify-write path is exercised too.
    let a = bytes::Bytes::from(vec![0xA1u8; 2 * BLK]);
    let n = fh.fh_write_owned(0, a.clone()).await.expect("write_owned");
    assert_eq!(n, 2 * BLK);

    let b = bytes::Bytes::from((0..64u8).collect::<Vec<u8>>());
    let n = fh.fh_write_owned(BLK + 11, b.clone()).await.expect("write_owned partial");
    assert_eq!(n, 64);

    let _ = fh.fh_flush().await.expect("flush");
    let _ = fh.fh_release().await.expect("release");

    // Verify from a fresh open.
    let mut fh = HyperFileHandler::fh_open(
        &reactor, &client, tf.uri(), FileFlags::rdonly(),
    ).await.expect("fh open");
    let got = fh.fh_read_owned(0, 2 * BLK).await.expect("read_owned");
    assert_eq!(got.len(), 2 * BLK);
    assert!(got[0..BLK].iter().all(|x| *x == 0xA1), "first block damaged");
    assert_eq!(&got[BLK + 11..BLK + 11 + 64], &b[..], "partial write not persisted");
    assert!(got[BLK..BLK + 11].iter().all(|x| *x == 0xA1), "gap before the partial write damaged");

    // The caller's handle to the bytes is still usable — `Bytes` is
    // shared, not consumed.
    assert_eq!(a.len(), 2 * BLK);
    assert_eq!(b.len(), 64);

    let _ = fh.fh_release().await.expect("release");
    tf.cleanup(&client).await;
}

/// Cancelling the owned variants is safe by construction: the caller
/// has nothing borrowed. Cancel both repeatedly and confirm the handle
/// and the data survive.
#[tokio::test]
#[ignore]
async fn cancelling_the_owned_variants_is_safe() {
    let _ = env_logger::try_init();
    let client = make_client().await;
    let tf = TestFile::new(&client).await;
    let reactor = make_reactor();

    {
        let mut fh = HyperFileHandler::fh_open_or_create_with_default_opt(
            &reactor, &client, tf.uri(), FileFlags::rdwr(), FileMode::default_file(),
        ).await.expect("fh create");
        let _ = fh.fh_write(0, &vec![0x77u8; 8 * BLK]).await.expect("write");
        let _ = fh.fh_flush().await.expect("flush");
        let _ = fh.fh_release().await.expect("release");
    }

    // Cold handle so the reads take real I/O and the timeout fires.
    let mut fh = HyperFileHandler::fh_open(
        &reactor, &client, tf.uri(), FileFlags::rdwr(),
    ).await.expect("fh open");

    let mut cancelled_reads = 0;
    for i in 0..24usize {
        let fut = fh.fh_read_owned(i % 8 * BLK, BLK);
        match tokio::time::timeout(std::time::Duration::from_micros(1), fut).await {
            Ok(Ok(b)) => assert!(b.iter().all(|x| *x == 0x77), "round {i}: wrong bytes"),
            Ok(Err(e)) => panic!("round {i}: read_owned failed: {e}"),
            Err(_) => cancelled_reads += 1,
        }
    }

    let mut cancelled_writes = 0;
    for i in 0..24usize {
        let payload = bytes::Bytes::from(vec![0x88u8; BLK]);
        let fut = fh.fh_write_owned(i % 8 * BLK, payload);
        match tokio::time::timeout(std::time::Duration::from_micros(1), fut).await {
            Ok(Ok(n)) => assert_eq!(n, BLK),
            Ok(Err(e)) => panic!("round {i}: write_owned failed: {e}"),
            Err(_) => cancelled_writes += 1,
        }
    }
    eprintln!("cancelled {cancelled_reads} reads and {cancelled_writes} writes");

    // The handle still works, and the file is readable and consistent:
    // every block is entirely one value or the other, never a mix.
    let _ = fh.fh_flush().await.expect("flush");
    for i in 0..8usize {
        let b = fh.fh_read_owned(i * BLK, BLK).await.expect("read after cancellations");
        assert_eq!(b.len(), BLK);
        let first = b[0];
        assert!(first == 0x77 || first == 0x88, "block {i} has unexpected content {first:#04x}");
        assert!(b.iter().all(|x| *x == first),
            "block {i} is a mix, so a cancelled write was partially applied");
    }

    let _ = fh.fh_release().await.expect("release");
    tf.cleanup(&client).await;
}

/// Concurrent readers must actually overlap.
///
/// The owned read used to run to completion inside the handler task,
/// which processes one request at a time, so every concurrent reader
/// queued behind the one before it — concurrency was 1 no matter how
/// many tasks the caller spawned. It now shares the `Read` op with the
/// borrowed form and takes the same spawning path.
///
/// Wall time is the only way to see this, so the assertion is a loose
/// one: eight concurrent readers must not take as long as doing the
/// same eight reads one after another. Serialized, they would.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn concurrent_owned_reads_overlap() {
    let _ = env_logger::try_init();
    let client = make_client().await;
    let tf = TestFile::new(&client).await;
    let reactor = make_reactor();

    const N: usize = 8;
    const CHUNK: usize = 256 * 1024;

    {
        let mut fh = HyperFileHandler::fh_open_or_create_with_default_opt(
            &reactor, &client, tf.uri(), FileFlags::rdwr(), FileMode::default_file(),
        ).await.expect("fh create");
        let _ = fh.fh_write(0, &vec![0x5Au8; N * CHUNK]).await.expect("write");
        let _ = fh.fh_flush().await.expect("flush");
        let _ = fh.fh_release().await.expect("release");
    }

    // Serial baseline, cold handle.
    let serial = {
        let mut fh = HyperFileHandler::fh_open(
            &reactor, &client, tf.uri(), FileFlags::rdonly(),
        ).await.expect("fh open");
        let t = std::time::Instant::now();
        for i in 0..N {
            let b = fh.fh_read_owned(i * CHUNK, CHUNK).await.expect("read_owned");
            assert_eq!(b.len(), CHUNK);
        }
        let d = t.elapsed();
        let _ = fh.fh_release().await.expect("release");
        d
    };

    // Concurrent, cold handle. Clones share one reactor task, which is
    // the point: the requests have to be in flight together.
    let concurrent = {
        let mut fh = HyperFileHandler::fh_open(
            &reactor, &client, tf.uri(), FileFlags::rdonly(),
        ).await.expect("fh open");
        let t = std::time::Instant::now();
        let mut set = Vec::new();
        for i in 0..N {
            let mut c = fh.clone();
            set.push(tokio::spawn(async move {
                let b = c.fh_read_owned(i * CHUNK, CHUNK).await.expect("read_owned");
                assert_eq!(b.len(), CHUNK);
                assert!(b.iter().all(|x| *x == 0x5A));
            }));
        }
        for j in set {
            j.await.expect("task");
        }
        let d = t.elapsed();
        let _ = fh.fh_release().await.expect("release");
        d
    };

    eprintln!("owned reads: {N} serial {:?}, {N} concurrent {:?}", serial, concurrent);
    // Serialized, the concurrent run would match the serial one. Ask
    // only for a clear improvement, so the test does not turn flaky on a
    // slow or contended network.
    // Fully serialized, the concurrent run would match the serial one.
    // The margin is deliberately loose: the handler still plans and
    // dispatches requests one at a time, so the overlap is partial, and
    // network variance is on the same order as the difference.
    assert!(concurrent * 4 < serial * 3,
        "{N} concurrent owned reads took {:?} against {:?} serially, so they are \
         not overlapping — the request is being run to completion inside the handler",
        concurrent, serial);

    tf.cleanup(&client).await;
}

/// `fh_dirty_block_count` lets a caller skip a flush that would have
/// nothing to write.
#[tokio::test]
#[ignore]
async fn dirty_block_count_tracks_pending_writes() {
    let _ = env_logger::try_init();
    let client = make_client().await;
    let tf = TestFile::new(&client).await;
    let reactor = make_reactor();

    let mut fh = HyperFileHandler::fh_open_or_create_with_default_opt(
        &reactor, &client, tf.uri(), FileFlags::rdwr(), FileMode::default_file(),
    ).await.expect("fh create");

    assert_eq!(fh.fh_dirty_block_count().await.expect("count"), 0,
        "a fresh file has nothing to write");

    let _ = fh.fh_write(0, &vec![0x11u8; 3 * BLK]).await.expect("write");
    assert_eq!(fh.fh_dirty_block_count().await.expect("count"), 3,
        "three blocks written, three dirty");

    let _ = fh.fh_flush().await.expect("flush");
    assert_eq!(fh.fh_dirty_block_count().await.expect("count"), 0,
        "a flush clears the dirty set");

    // A block borrow dirties too.
    let _ = fh.fh_with_block_mut(1, false, |blk| { blk[0] = 0x22; }).await
        .expect("with_block_mut").expect("mapped");
    assert_eq!(fh.fh_dirty_block_count().await.expect("count"), 1,
        "an in-place block edit is a pending write");

    let _ = fh.fh_flush().await.expect("flush");
    assert_eq!(fh.fh_dirty_block_count().await.expect("count"), 0);

    // A read leaves nothing to write.
    let _ = fh.fh_read_owned(0, BLK).await.expect("read_owned");
    assert_eq!(fh.fh_dirty_block_count().await.expect("count"), 0,
        "reading must not dirty anything");

    let _ = fh.fh_release().await.expect("release");
    tf.cleanup(&client).await;
}

/// Concurrent read-only block access must overlap.
///
/// `BlockAction::Ref` used to fetch the block inside the handler arm,
/// and the handler takes `&mut self` and handles one context at a time,
/// so every concurrent reader queued behind the one before it —
/// concurrency was 1 whatever the caller spawned. Measured at 0.99:
/// 289.6 ms serial against 292.1 ms for the same reads concurrently.
///
/// The fix mirrors what the write path's retrieve already does: the
/// block is owned rather than borrowed from the file, so the load moves
/// into a spawned task along with the closure, and the arm returns.
///
/// Same shape as `concurrent_owned_reads_overlap`, and the same loose
/// threshold for the same reason: cache hits and planning still run one
/// at a time in the handler, so the overlap is partial.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore]
async fn concurrent_block_reads_overlap() {
    let _ = env_logger::try_init();
    let client = make_client().await;
    let tf = TestFile::new(&client).await;
    let reactor = make_reactor();

    const N: u64 = 48;

    {
        let mut fh = HyperFileHandler::fh_open_or_create_with_default_opt(
            &reactor, &client, tf.uri(), FileFlags::rdwr(), FileMode::default_file(),
        ).await.expect("fh create");
        let _ = fh.fh_write(0, &vec![0x5Au8; N as usize * BLK]).await.expect("write");
        let _ = fh.fh_flush().await.expect("flush");
        let _ = fh.fh_release().await.expect("release");
    }

    // Serial baseline, every block cold.
    let serial = {
        let mut fh = HyperFileHandler::fh_open(
            &reactor, &client, tf.uri(), FileFlags::rdonly(),
        ).await.expect("fh open");
        let t = std::time::Instant::now();
        for i in 0..N {
            let b = fh.fh_with_block(i, |blk| blk[0]).await
                .expect("with_block").expect("mapped");
            assert_eq!(b, 0x5A);
        }
        let d = t.elapsed();
        let s = fh.fh_read_timing().await.expect("timing");
        assert_eq!(s.data_gets, N, "each cold block should cost one request");
        let _ = fh.fh_release().await.expect("release");
        d
    };

    // Concurrent, every block cold again. Clones share one reactor
    // task, which is the point.
    let concurrent = {
        let mut fh = HyperFileHandler::fh_open(
            &reactor, &client, tf.uri(), FileFlags::rdonly(),
        ).await.expect("fh open");
        fh.fh_read_timing_reset().await.expect("reset");
        let t = std::time::Instant::now();
        let mut set = Vec::new();
        for i in 0..N {
            let mut c = fh.clone();
            set.push(tokio::spawn(async move {
                let b = c.fh_with_block(i, |blk| blk[0]).await
                    .expect("with_block").expect("mapped");
                assert_eq!(b, 0x5A);
            }));
        }
        for j in set {
            j.await.expect("task");
        }
        let d = t.elapsed();
        let s = fh.fh_read_timing().await.expect("timing");
        // The efficiency of the block path is the other half of the
        // point: concurrency must not cost extra requests.
        assert!(s.data_gets <= N + 4,
            "concurrent block reads should not fetch more than the {N} blocks, got {}",
            s.data_gets);
        let _ = fh.fh_release().await.expect("release");
        d
    };

    eprintln!("block reads: {N} serial {:?}, {N} concurrent {:?}", serial, concurrent);
    assert!(concurrent * 4 < serial * 3,
        "{N} concurrent block reads took {:?} against {:?} serially, so they are not \
         overlapping — the fetch is being run to completion inside the handler",
        concurrent, serial);

    tf.cleanup(&client).await;
}

/// `fh_with_blocks` visits many blocks in one crossing.
///
/// The cost of block access on this surface is the channel crossing, not
/// the copy — tens of microseconds against a fraction of one — so a caller
/// touching hundreds of blocks spends nearly all its time in the channel.
/// A directory listing reading inode records is exactly that shape.
///
/// Paired with `fh_read_ahead`, a whole region costs two crossings however
/// many blocks it holds: warm it, then visit it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn reactor_with_blocks_visits_a_batch_in_one_crossing() {
    let _ = env_logger::try_init();
    let client = make_client().await;
    let tf = TestFile::new(&client).await;
    let reactor = make_reactor();

    const NB: usize = 64;

    {
        let mut fh = HyperFileHandler::fh_open_or_create_with_default_opt(
            &reactor, &client, tf.uri(), FileFlags::rdwr(), FileMode::default_file()).await.unwrap();
        for b in 0..NB {
            fh.fh_write(b * BLK, &vec![(b % 251) as u8; BLK]).await.expect("write");
        }
        fh.fh_flush().await.expect("flush");
        let _ = fh.fh_release().await.expect("release");
    }

    let mut fh = HyperFileHandler::fh_open(&reactor, &client, tf.uri(), FileFlags::rdonly()).await.unwrap();

    // Warm the region, then visit all of it in one message.
    let warmed = fh.fh_read_ahead(0, NB * BLK).await.expect("read_ahead");
    assert_eq!(warmed, NB);

    let indices: Vec<u64> = (0..NB as u64).collect();
    let mut seen = Vec::new();
    let mut absent = 0usize;
    let resident = fh.fh_with_blocks(&indices, |idx, bytes| {
        match bytes {
            Some(b) => seen.push((idx, b[0])),
            None => absent += 1,
        }
    }).await.expect("with_blocks");

    assert_eq!(resident, NB, "every warmed block should have been resident");
    assert_eq!(absent, 0, "nothing should have been reported absent");
    assert_eq!(seen.len(), NB, "the closure should have run once per index");
    for (idx, first) in seen {
        assert_eq!(first, (idx as usize % 251) as u8,
            "block {} handed the closure the wrong bytes", idx);
    }

    // A block that was never warmed is reported absent rather than fetched,
    // which is what keeps the batch free of object requests.
    let before = fh.fh_read_timing().await.expect("timing");
    let far: Vec<u64> = vec![100_000, 100_001];
    let mut nones = 0usize;
    let resident = fh.fh_with_blocks(&far, |_, bytes| {
        if bytes.is_none() { nones += 1; }
    }).await.expect("with_blocks far");
    let after = fh.fh_read_timing().await.expect("timing");
    assert_eq!(resident, 0);
    assert_eq!(nones, 2, "both uncached indices should be reported absent");
    assert_eq!(after.data_gets, before.data_gets,
        "a batch must not fetch; it reports what is not cached");

    let _ = fh.fh_release().await;
    tf.cleanup(&client).await;
}
