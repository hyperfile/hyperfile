//! Rollback integration tests.
//!
//! Two groups:
//!  1. Rollback EXPOSURE tests (AlwaysFailFlushInode) — document that
//!     when flush permanently fails, the persisted state remains unchanged
//!     and in-memory state is recoverable on reopen.
//!  2. Rollback CORRECTNESS tests (FailOnFlushInode) — verify that a
//!     single transient flush failure is not silently committed by a
//!     subsequent retry or release flush, for every mutation API
//!     (write / truncate extend / truncate shrink / write_zero /
//!     write_aligned_batch / write_batch).
//!
//! Run with:
//! ```bash
//! cargo test --test integration_s3_rollback -- --ignored --test-threads=1
//! ```

mod common;
use common::*;

use hyperfile::file::hyper::Hyper;
use hyperfile::file::flags::FileFlags;
use hyperfile::file::mode::FileMode;
use hyperfile::buffer::{AlignedDataBlockWrapper, BatchDataBlockWrapper};

// --------------------------------------------------------------------
// Rollback exposure tests
// --------------------------------------------------------------------
//
// These tests document the CURRENT behavior when `flush_inode` fails
// during a truncate/write operation. They are documenting (not testing
// for correctness yet) the divergence between in-memory inode and
// persisted inode.
//
// Observations discovered by the initial version of these tests:
//   1. hyperfile has a built-in flush retry (DEFAULT_FLUSH_RETRIES=3,
//      `HyperTrait::flush()` in file/mod.rs). When a single flush_inode
//      fails, the retry loop may succeed on a subsequent attempt,
//      masking the failure from the caller's perspective after release.
//   2. `fs_release()` also performs a flush as part of teardown. Even
//      if the operation under test returns Err, a subsequent release
//      can still successfully persist the (already-mutated) in-memory
//      state, causing the "failed" operation to effectively commit.
//   3. For this reason we use `AlwaysFailFlushInode` below — it ensures
//      every flush_inode attempt fails, so retries and release flushes
//      cannot paper over the injected failure.
//
// When the rollback issue is eventually fixed, these tests will need
// to be updated to reflect the corrected behavior.

/// Truncate-extend on an already-persisted file, with flush_inode ALWAYS failing.
/// Documents that:
///   1. `fs_truncate()` returns Err.
///   2. `fs_release()` also returns Err (cannot clean up).
///   3. The PERSISTED size remains the original size — the inode write
///      really never succeeded on disk.
#[tokio::test]
#[ignore]
async fn rollback_exposure_truncate_extend_flush_fails() {
    let _ = env_logger::try_init();
    let client = make_client().await;
    let tf = TestFile::new(&client).await;

    let original_size = 1024usize;
    let payload: Vec<u8> = (0..original_size).map(|i| (i & 0xFF) as u8).collect();

    // 1. Create a file and flush some data.
    {
        let mut hyper = Hyper::fs_open_or_create_with_default_opt(
            &client,
            tf.uri(),
            FileFlags::rdwr(),
            FileMode::default_file(),
        )
        .await
        .expect("create");
        let _ = hyper.fs_write(0, &payload).await.expect("write");
        let _ = hyper.fs_release().await.expect("release");
    }

    // 2. Reopen with an interceptor that fails ALL flush_inode calls.
    let interceptor = AlwaysFailFlushInode::new();
    let truncate_result = {
        let mut hyper = Hyper::fs_open(&client, tf.uri(), FileFlags::rdwr())
            .await
            .expect("open");
        hyper.with_staging_interceptor(interceptor.clone());

        let result = hyper.fs_truncate(8192).await;
        // Best-effort release — expected to also fail because flush will retry.
        let _release_result = hyper.fs_release().await;
        result
    };

    assert!(
        truncate_result.is_err(),
        "expected truncate to fail due to injected flush_inode failure, got {:?}",
        truncate_result
    );
    assert!(
        interceptor.call_count() >= 1,
        "interceptor was never invoked"
    );
    println!(
        "[truncate_extend_flush_fails] truncate_result={:?} interceptor_calls={}",
        truncate_result,
        interceptor.call_count()
    );

    // 3. Reopen and inspect what was actually persisted.
    let persisted_size = {
        let hyper = Hyper::fs_open(&client, tf.uri(), FileFlags::rdonly())
            .await
            .expect("reopen");
        let stat = hyper.fs_getattr().expect("getattr");
        stat.st_size as usize
    };
    println!(
        "[truncate_extend_flush_fails] persisted_size={}",
        persisted_size
    );

    // Document current behavior: persisted size should still be the original,
    // since all flush attempts failed.
    assert_eq!(
        persisted_size, original_size,
        "persisted size diverged: expected original {}, got {}",
        original_size, persisted_size
    );

    tf.cleanup(&client).await;
}

/// Truncate-shrink with flush ALWAYS failing. Most dangerous case: bmap.truncate()
/// has already discarded mappings for blocks beyond the new size before the
/// flush is attempted. This test exposes whether reopening the file still
/// has access to the original data (bmap is reloaded from the persisted
/// inode, so theoretically the data should still be readable).
#[tokio::test]
#[ignore]
async fn rollback_exposure_truncate_shrink_flush_fails() {
    let _ = env_logger::try_init();
    let client = make_client().await;
    let tf = TestFile::new(&client).await;

    // 1. Seed a 16 KiB file (4 data blocks of 4 KiB).
    let original_size = 16 * 1024usize;
    let payload: Vec<u8> = (0..original_size).map(|i| (i & 0xFF) as u8).collect();
    {
        let mut hyper = Hyper::fs_open_or_create_with_default_opt(
            &client,
            tf.uri(),
            FileFlags::rdwr(),
            FileMode::default_file(),
        )
        .await
        .expect("create");
        let _ = hyper.fs_write(0, &payload).await.expect("write");
        let _ = hyper.fs_release().await.expect("release");
    }

    // 2. Reopen, install always-failing interceptor.
    let interceptor = AlwaysFailFlushInode::new();
    let truncate_result = {
        let mut hyper = Hyper::fs_open(&client, tf.uri(), FileFlags::rdwr())
            .await
            .expect("open");
        hyper.with_staging_interceptor(interceptor.clone());

        // Shrink to 1 block worth of data.
        let result = hyper.fs_truncate(4096).await;
        let _release = hyper.fs_release().await;
        result
    };

    assert!(
        truncate_result.is_err(),
        "expected truncate to fail, got {:?}",
        truncate_result
    );
    println!(
        "[truncate_shrink_flush_fails] truncate_result={:?} interceptor_calls={}",
        truncate_result,
        interceptor.call_count()
    );

    // 3. Reopen and read everything to see what was preserved.
    let (persisted_size, first_block_ok, last_block_ok) = {
        let mut hyper = Hyper::fs_open(&client, tf.uri(), FileFlags::rdonly())
            .await
            .expect("reopen");
        let stat = hyper.fs_getattr().expect("getattr");
        let size = stat.st_size as usize;

        // Read the first block (offset 0, 4KiB) — should match payload[..4096]
        let mut buf0 = vec![0u8; 4096];
        let r0 = hyper.fs_read(0, &mut buf0).await;
        let first_block_ok = r0.is_ok() && buf0 == payload[..4096];

        // If size is still original, try reading the last block too.
        let last_block_ok = if size >= 16 * 1024 {
            let mut buf3 = vec![0u8; 4096];
            let r3 = hyper.fs_read(12 * 1024, &mut buf3).await;
            Some(r3.is_ok() && buf3 == payload[12 * 1024..16 * 1024])
        } else {
            None
        };

        (size, first_block_ok, last_block_ok)
    };

    println!(
        "[truncate_shrink_flush_fails] persisted_size={} first_block_ok={} last_block_ok={:?}",
        persisted_size, first_block_ok, last_block_ok
    );

    // Document current behavior: persisted size should still be 16 KiB
    // because all flush attempts failed.
    assert_eq!(
        persisted_size, original_size,
        "persisted size diverged after failed shrink: expected {}, got {}",
        original_size, persisted_size
    );
    // First block must always be readable — it's unaffected by shrink.
    assert!(first_block_ok, "first block corrupted after failed shrink");
    // Last block readability after a failed shrink: because bmap is reloaded
    // from the persisted inode on reopen, the in-memory bmap truncation
    // should be undone.
    if let Some(ok) = last_block_ok {
        assert!(ok, "last block unreadable after failed shrink — data lost");
    }

    tf.cleanup(&client).await;
}

/// Write followed by ALL flush attempts failing. Documents whether the
/// written data is visible after reopening the file.
#[tokio::test]
#[ignore]
async fn rollback_exposure_write_flush_fails() {
    let _ = env_logger::try_init();
    let client = make_client().await;
    let tf = TestFile::new(&client).await;

    // 1. Create an empty file, flush to get initial inode persisted.
    {
        let mut hyper = Hyper::fs_open_or_create_with_default_opt(
            &client,
            tf.uri(),
            FileFlags::rdwr(),
            FileMode::default_file(),
        )
        .await
        .expect("create");
        // Release without writing — creates an empty persisted file.
        let _ = hyper.fs_release().await.expect("release");
    }

    // 2. Reopen, install always-failing interceptor, write and try to flush.
    let interceptor = AlwaysFailFlushInode::new();
    let (write_result, flush_result) = {
        let mut hyper = Hyper::fs_open(&client, tf.uri(), FileFlags::rdwr())
            .await
            .expect("open");
        hyper.with_staging_interceptor(interceptor.clone());

        let data = b"hello world";
        let wr = hyper.fs_write(0, data).await;
        let fl = hyper.fs_flush().await;
        let _release = hyper.fs_release().await;
        (wr, fl)
    };

    // write() returns Ok even when subsequent flush fails, because write
    // only modifies in-memory state. flush() is where the failure surfaces.
    println!(
        "[write_flush_fails] write_result={:?} flush_result={:?} interceptor_calls={}",
        write_result,
        flush_result,
        interceptor.call_count()
    );
    assert!(
        flush_result.is_err(),
        "expected flush to fail, got write={:?} flush={:?}",
        write_result,
        flush_result
    );

    // 3. Reopen and check persisted state.
    let persisted_size = {
        let hyper = Hyper::fs_open(&client, tf.uri(), FileFlags::rdonly())
            .await
            .expect("reopen");
        let stat = hyper.fs_getattr().expect("getattr");
        stat.st_size as usize
    };
    println!(
        "[write_flush_fails] persisted_size={}",
        persisted_size
    );

    // The persisted file should still be empty because all flush attempts failed.
    assert_eq!(
        persisted_size, 0,
        "persisted size diverged after failed write+flush: expected 0, got {}",
        persisted_size
    );

    tf.cleanup(&client).await;
}

// --------------------------------------------------------------------
// Rollback CORRECTNESS tests (FailOnce — expose silent commits)
// --------------------------------------------------------------------
//
// These tests use FailOnFlushInode::at(1), so the FIRST flush_inode
// call fails but subsequent attempts succeed. This simulates the
// realistic case where a transient failure happens mid-operation.
//
// Without a proper rollback in write/truncate/write_zero, the
// sequence of events is:
//   1. Operation mutates in-memory inode (e.g. set_size).
//   2. Operation calls flush() → flush_inode #1 fails → returns Err.
//   3. Caller sees Err and assumes the operation didn't commit.
//   4. Caller eventually calls fs_release() → flush() → flush_inode #2
//      succeeds → the mutated in-memory state is committed.
//
// That is the silent-commit bug these tests are designed to expose.

/// Truncate-extend + a single transient flush failure. After release,
/// the persisted size MUST remain the original — otherwise we have
/// committed a "failed" operation.
#[tokio::test]
#[ignore]
async fn rollback_correctness_truncate_extend_single_flush_fails() {
    let _ = env_logger::try_init();
    let client = make_client().await;
    let tf = TestFile::new(&client).await;

    let original_size = 1024usize;
    let payload: Vec<u8> = (0..original_size).map(|i| (i & 0xFF) as u8).collect();

    {
        let mut hyper = Hyper::fs_open_or_create_with_default_opt(
            &client,
            tf.uri(),
            FileFlags::rdwr(),
            FileMode::default_file(),
        )
        .await
        .expect("create");
        let _ = hyper.fs_write(0, &payload).await.expect("write");
        let _ = hyper.fs_release().await.expect("release");
    }

    // Fail exactly the first flush_inode; subsequent attempts succeed.
    let interceptor = FailOnFlushInode::at(1);
    let truncate_result = {
        let mut hyper = Hyper::fs_open(&client, tf.uri(), FileFlags::rdwr())
            .await
            .expect("open");
        hyper.with_staging_interceptor(interceptor.clone());
        let r = hyper.fs_truncate(8192).await;
        let _rel = hyper.fs_release().await;
        r
    };

    assert!(
        truncate_result.is_err(),
        "expected truncate to fail, got {:?}",
        truncate_result
    );
    println!(
        "[correctness_truncate_extend] truncate={:?} calls={}",
        truncate_result,
        interceptor.call_count()
    );

    let persisted_size = {
        let hyper = Hyper::fs_open(&client, tf.uri(), FileFlags::rdonly())
            .await
            .expect("reopen");
        hyper.fs_getattr().expect("getattr").st_size as usize
    };
    println!(
        "[correctness_truncate_extend] persisted_size={}",
        persisted_size
    );

    // INVARIANT: a failed truncate must NOT change the persisted size.
    assert_eq!(
        persisted_size, original_size,
        "failed truncate silently committed: expected size {}, got {}",
        original_size, persisted_size
    );

    tf.cleanup(&client).await;
}

/// Truncate-shrink + single flush failure. Besides size, we also verify
/// that all original blocks are still readable — bmap.truncate() destroys
/// in-memory mappings before flush, so without rollback the first block
/// can appear corrupted.
#[tokio::test]
#[ignore]
async fn rollback_correctness_truncate_shrink_single_flush_fails() {
    let _ = env_logger::try_init();
    let client = make_client().await;
    let tf = TestFile::new(&client).await;

    let original_size = 16 * 1024usize;
    let payload: Vec<u8> = (0..original_size).map(|i| (i & 0xFF) as u8).collect();
    {
        let mut hyper = Hyper::fs_open_or_create_with_default_opt(
            &client,
            tf.uri(),
            FileFlags::rdwr(),
            FileMode::default_file(),
        )
        .await
        .expect("create");
        let _ = hyper.fs_write(0, &payload).await.expect("write");
        let _ = hyper.fs_release().await.expect("release");
    }

    let interceptor = FailOnFlushInode::at(1);
    let truncate_result = {
        let mut hyper = Hyper::fs_open(&client, tf.uri(), FileFlags::rdwr())
            .await
            .expect("open");
        hyper.with_staging_interceptor(interceptor.clone());
        let r = hyper.fs_truncate(4096).await;
        let _rel = hyper.fs_release().await;
        r
    };

    assert!(
        truncate_result.is_err(),
        "expected truncate to fail, got {:?}",
        truncate_result
    );
    println!(
        "[correctness_truncate_shrink] truncate={:?} calls={}",
        truncate_result,
        interceptor.call_count()
    );

    let (persisted_size, first_ok, last_ok) = {
        let mut hyper = Hyper::fs_open(&client, tf.uri(), FileFlags::rdonly())
            .await
            .expect("reopen");
        let size = hyper.fs_getattr().expect("getattr").st_size as usize;

        let mut b0 = vec![0u8; 4096];
        let r0 = hyper.fs_read(0, &mut b0).await;
        let first_ok = r0.is_ok() && b0 == payload[..4096];

        let last_ok = if size >= 16 * 1024 {
            let mut b3 = vec![0u8; 4096];
            let r3 = hyper.fs_read(12 * 1024, &mut b3).await;
            Some(r3.is_ok() && b3 == payload[12 * 1024..16 * 1024])
        } else {
            None
        };
        (size, first_ok, last_ok)
    };
    println!(
        "[correctness_truncate_shrink] size={} first_ok={} last_ok={:?}",
        persisted_size, first_ok, last_ok
    );

    // INVARIANTS after a failed shrink:
    //   1. persisted size unchanged.
    //   2. all blocks of the original file still readable.
    assert_eq!(
        persisted_size, original_size,
        "failed shrink silently committed: size expected {}, got {}",
        original_size, persisted_size
    );
    assert!(first_ok, "first block corrupted after failed shrink");
    assert_eq!(
        last_ok,
        Some(true),
        "last block unreadable after failed shrink — data lost"
    );

    tf.cleanup(&client).await;
}

/// Write + single flush failure. After release, the persisted file MUST
/// still be empty (0 bytes), not the 11 bytes we attempted to write.
#[tokio::test]
#[ignore]
async fn rollback_correctness_write_single_flush_fails() {
    let _ = env_logger::try_init();
    let client = make_client().await;
    let tf = TestFile::new(&client).await;

    {
        let mut hyper = Hyper::fs_open_or_create_with_default_opt(
            &client,
            tf.uri(),
            FileFlags::rdwr(),
            FileMode::default_file(),
        )
        .await
        .expect("create");
        let _ = hyper.fs_release().await.expect("release");
    }

    let interceptor = FailOnFlushInode::at(1);
    let (write_result, flush_result) = {
        let mut hyper = Hyper::fs_open(&client, tf.uri(), FileFlags::rdwr())
            .await
            .expect("open");
        hyper.with_staging_interceptor(interceptor.clone());
        let wr = hyper.fs_write(0, b"hello world").await;
        let fl = hyper.fs_flush().await;
        let _rel = hyper.fs_release().await;
        (wr, fl)
    };

    println!(
        "[correctness_write] write={:?} flush={:?} calls={}",
        write_result,
        flush_result,
        interceptor.call_count()
    );
    assert!(
        flush_result.is_err(),
        "expected flush to fail, got write={:?} flush={:?}",
        write_result,
        flush_result
    );

    let persisted_size = {
        let hyper = Hyper::fs_open(&client, tf.uri(), FileFlags::rdonly())
            .await
            .expect("reopen");
        hyper.fs_getattr().expect("getattr").st_size as usize
    };
    println!("[correctness_write] persisted_size={}", persisted_size);

    // INVARIANT: a failed write+flush must not commit the data.
    assert_eq!(
        persisted_size, 0,
        "failed write silently committed: expected 0, got {}",
        persisted_size
    );

    tf.cleanup(&client).await;
}


// --------------------------------------------------------------------
// Rollback correctness tests for the remaining write variants
// --------------------------------------------------------------------
//
// rollback_from_persisted is wired into write/write_zero/truncate/
// write_aligned_batch/write_batch, but integration tests only exercised
// write and truncate so far. The three tests below close the gap.

/// fs_write_zero extends the file with a sparse region of zeros. With a
/// single transient flush_inode failure, the operation must return Err
/// and leave the persisted file unchanged.
#[tokio::test]
#[ignore]
async fn rollback_correctness_write_zero_single_flush_fails() {
    let _ = env_logger::try_init();
    let client = make_client().await;
    let tf = TestFile::new(&client).await;

    let original_size = 4096usize;
    let payload: Vec<u8> = (0..original_size).map(|i| (i & 0xFF) as u8).collect();
    {
        let mut hyper = Hyper::fs_open_or_create_with_default_opt(
            &client,
            tf.uri(),
            FileFlags::rdwr(),
            FileMode::default_file(),
        )
        .await
        .expect("create");
        let _ = hyper.fs_write(0, &payload).await.expect("write");
        let _ = hyper.fs_release().await.expect("release");
    }

    // Fail only the first flush_inode; if rollback works, the zero-fill
    // extension must not be visible after reopen.
    //
    // Note: fs_write_zero returns Ok for the buffered write; the actual
    // flush happens later. We call fs_flush explicitly so the injected
    // failure surfaces at a well-defined point.
    let interceptor = FailOnFlushInode::at(1);
    let (write_zero_result, flush_result) = {
        let mut hyper = Hyper::fs_open(&client, tf.uri(), FileFlags::rdwr())
            .await
            .expect("open");
        hyper.with_staging_interceptor(interceptor.clone());
        // Extend from 4 KiB to 12 KiB with zeros.
        let wr = hyper.fs_write_zero(original_size, 8192).await;
        let fr = hyper.fs_flush().await;
        let _rel = hyper.fs_release().await;
        (wr, fr)
    };

    // write_zero itself may return Ok (data buffered); flush must fail.
    assert!(
        flush_result.is_err(),
        "expected flush after write_zero to fail, got wr={:?} flush={:?}",
        write_zero_result, flush_result,
    );
    println!(
        "[correctness_write_zero] write_zero={:?} flush={:?} calls={}",
        write_zero_result,
        flush_result,
        interceptor.call_count(),
    );

    // Reopen, verify size and original bytes.
    let (persisted_size, body_ok) = {
        let mut hyper = Hyper::fs_open(&client, tf.uri(), FileFlags::rdonly())
            .await
            .expect("reopen");
        let size = hyper.fs_getattr().expect("getattr").st_size as usize;
        let mut buf = vec![0u8; original_size];
        let _ = hyper.fs_read(0, &mut buf).await.expect("read");
        (size, buf == payload)
    };

    assert_eq!(
        persisted_size, original_size,
        "write_zero silently committed: expected size {}, got {}",
        original_size, persisted_size,
    );
    assert!(body_ok, "original bytes corrupted after failed write_zero");

    tf.cleanup(&client).await;
}

/// fs_write_aligned_batch writes a mix of data and zero blocks at 4 KiB
/// alignment. A single flush_inode failure must leave the file empty.
#[tokio::test]
#[ignore]
async fn rollback_correctness_write_aligned_batch_single_flush_fails() {
    let _ = env_logger::try_init();
    let client = make_client().await;
    let tf = TestFile::new(&client).await;

    // Start from an empty persisted file.
    {
        let mut hyper = Hyper::fs_open_or_create_with_default_opt(
            &client,
            tf.uri(),
            FileFlags::rdwr(),
            FileMode::default_file(),
        )
        .await
        .expect("create");
        let _ = hyper.fs_release().await.expect("release");
    }

    let interceptor = FailOnFlushInode::at(1);
    let (batch_result, flush_result) = {
        let mut hyper = Hyper::fs_open(&client, tf.uri(), FileFlags::rdwr())
            .await
            .expect("open");
        hyper.with_staging_interceptor(interceptor.clone());

        // Three 4 KiB blocks: data, zero, data. Exercises both code paths.
        let block0 = AlignedDataBlockWrapper::new(0, 4096, false);
        block0.as_mut_slice().fill(0xAA);
        let block1 = AlignedDataBlockWrapper::new(1, 4096, true); // zero
        let block2 = AlignedDataBlockWrapper::new(2, 4096, false);
        block2.as_mut_slice().fill(0xCC);
        let blocks = vec![block0, block1, block2];

        let br = hyper.fs_write_aligned_batch(blocks).await;
        let fr = hyper.fs_flush().await;
        let _rel = hyper.fs_release().await;
        (br, fr)
    };

    assert!(
        flush_result.is_err(),
        "expected flush after write_aligned_batch to fail, got batch={:?} flush={:?}",
        batch_result, flush_result,
    );
    println!(
        "[correctness_aligned_batch] batch={:?} flush={:?} calls={}",
        batch_result,
        flush_result,
        interceptor.call_count(),
    );

    // Reopen, file must still be empty.
    let persisted_size = {
        let hyper = Hyper::fs_open(&client, tf.uri(), FileFlags::rdonly())
            .await
            .expect("reopen");
        hyper.fs_getattr().expect("getattr").st_size as usize
    };
    assert_eq!(
        persisted_size, 0,
        "write_aligned_batch silently committed: expected 0, got {}",
        persisted_size,
    );

    tf.cleanup(&client).await;
}

/// fs_write_batch with a partial block over an existing file. A single
/// flush_inode failure must leave the original data unchanged, and the
/// size must not shift.
#[tokio::test]
#[ignore]
async fn rollback_correctness_write_batch_single_flush_fails() {
    let _ = env_logger::try_init();
    let client = make_client().await;
    let tf = TestFile::new(&client).await;

    let original_size = 4096usize;
    let payload: Vec<u8> = (0..original_size).map(|i| (i & 0xFF) as u8).collect();
    {
        let mut hyper = Hyper::fs_open_or_create_with_default_opt(
            &client,
            tf.uri(),
            FileFlags::rdwr(),
            FileMode::default_file(),
        )
        .await
        .expect("create");
        let _ = hyper.fs_write(0, &payload).await.expect("write");
        let _ = hyper.fs_release().await.expect("release");
    }

    let interceptor = FailOnFlushInode::at(1);
    let (batch_result, flush_result) = {
        let mut hyper = Hyper::fs_open(&client, tf.uri(), FileFlags::rdwr())
            .await
            .expect("open");
        hyper.with_staging_interceptor(interceptor.clone());

        // Partial block: block 0, offset 100, len 200 — overwrites 200
        // bytes in the middle of the already-persisted block.
        let part = BatchDataBlockWrapper::new_partial_block(
            0, 4096, 100, 200, false,
        );
        part.as_mut_slice().fill(0xFF);
        let blocks = vec![part];

        let br = hyper.fs_write_batch(blocks).await;
        let fr = hyper.fs_flush().await;
        let _rel = hyper.fs_release().await;
        (br, fr)
    };

    assert!(
        flush_result.is_err(),
        "expected flush after write_batch to fail, got batch={:?} flush={:?}",
        batch_result, flush_result,
    );
    println!(
        "[correctness_batch] batch={:?} flush={:?} calls={}",
        batch_result,
        flush_result,
        interceptor.call_count(),
    );

    // Reopen, verify size and bytes intact.
    let (persisted_size, body_ok) = {
        let mut hyper = Hyper::fs_open(&client, tf.uri(), FileFlags::rdonly())
            .await
            .expect("reopen");
        let size = hyper.fs_getattr().expect("getattr").st_size as usize;
        let mut buf = vec![0u8; original_size];
        let _ = hyper.fs_read(0, &mut buf).await.expect("read");
        (size, buf == payload)
    };

    assert_eq!(
        persisted_size, original_size,
        "write_batch silently changed size: expected {}, got {}",
        original_size, persisted_size,
    );
    assert!(
        body_ok,
        "write_batch silently modified original bytes",
    );

    tf.cleanup(&client).await;
}

