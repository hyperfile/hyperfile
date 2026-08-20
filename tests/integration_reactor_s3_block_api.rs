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
