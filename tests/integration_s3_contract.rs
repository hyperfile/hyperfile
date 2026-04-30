//! Flush contract (invariant) integration tests.
//!
//! These tests use #[doc(hidden)] getters on Hyper (dirty_block_count,
//! is_attr_dirty, is_bmap_dirty, in_memory_last_cno /
//! in_memory_last_ondisk_cno) to assert internal state after various
//! flush outcomes. Organized into four groups:
//!   A: invariants on success and on fail+rollback
//!   B: segment-done failure path
//!   C: attr-only flush early-return path
//!   D: retry policy (retries on ResourceBusy, not on Other)
//!
//! Run with:
//! ```bash
//! cargo test --test integration_s3_contract -- --ignored --test-threads=1
//! ```

mod common;
use common::*;

use hyperfile::file::hyper::Hyper;
use hyperfile::file::flags::FileFlags;
use hyperfile::file::mode::FileMode;

// --------------------------------------------------------------------
// A: flush contract invariants (success + rollback)
// --------------------------------------------------------------------

/// A: after a successful flush, all dirty indicators must be cleared and
/// the cno bookkeeping must be in sync with persisted state.
#[tokio::test]
#[ignore]
async fn contract_flush_success_clears_all_dirty_flags() {
    let _ = env_logger::try_init();
    let client = make_client().await;
    let tf = TestFile::new(&client).await;

    let mut hyper = Hyper::fs_open_or_create_with_default_opt(
        &client,
        tf.uri(),
        FileFlags::rdwr(),
        FileMode::default_file(),
    )
    .await
    .expect("create");

    // Write data → cache is dirty, bmap is dirty.
    let data = vec![0xABu8; 4096];
    let _ = hyper.fs_write(0, &data).await.expect("write");
    assert!(hyper.dirty_block_count() > 0, "expected dirty blocks before flush");
    assert!(hyper.is_bmap_dirty(), "expected bmap dirty before flush");
    assert!(hyper.is_attr_dirty(), "expected attr dirty before flush (mtime set)");

    // Flush.
    let cno = hyper.fs_flush().await.expect("flush");

    // Invariants after successful flush:
    assert_eq!(hyper.dirty_block_count(), 0, "dirty blocks not cleared after flush");
    assert!(!hyper.is_bmap_dirty(), "bmap still dirty after flush");
    assert!(!hyper.is_attr_dirty(), "attr still dirty after flush");
    assert_eq!(
        hyper.in_memory_last_cno(),
        hyper.in_memory_last_ondisk_cno(),
        "last_cno and last_ondisk_cno diverged after successful flush",
    );
    assert!(cno > 0, "flush should return a non-zero cno on success");

    let _ = hyper.fs_release().await;
    tf.cleanup(&client).await;
}

/// A: after flush fails and fs_flush rolls back, the in-memory state must
/// match what was persisted (which is the pre-write state).
#[tokio::test]
#[ignore]
async fn contract_flush_fail_rollback_restores_clean_state() {
    let _ = env_logger::try_init();
    let client = make_client().await;
    let tf = TestFile::new(&client).await;

    // Prepare an empty persisted file.
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

    // Reopen, install always-fail interceptor, write, try to flush.
    let mut hyper = Hyper::fs_open(&client, tf.uri(), FileFlags::rdwr())
        .await
        .expect("open");
    hyper.with_staging_interceptor(AlwaysFailFlushInode::new());

    let baseline_cno = hyper.in_memory_last_cno();
    let baseline_ondisk = hyper.in_memory_last_ondisk_cno();
    assert_eq!(baseline_cno, baseline_ondisk, "baseline cno mismatch after open");

    let _ = hyper.fs_write(0, &[0xAAu8; 100]).await.expect("write");
    assert!(hyper.dirty_block_count() > 0, "expected dirty after write");

    // Flush MUST fail.
    let flush_res = hyper.fs_flush().await;
    assert!(flush_res.is_err(), "flush should have failed");

    // Invariants after rollback:
    //   dirty flags all cleared (rolled back to persisted state)
    //   last_cno and last_ondisk_cno aligned with each other and with baseline
    assert_eq!(hyper.dirty_block_count(), 0, "dirty blocks not cleared by rollback");
    assert!(!hyper.is_bmap_dirty(), "bmap still dirty after rollback");
    assert!(!hyper.is_attr_dirty(), "attr still dirty after rollback");
    assert_eq!(
        hyper.in_memory_last_cno(),
        hyper.in_memory_last_ondisk_cno(),
        "cno tracking still diverged after rollback",
    );
    assert_eq!(
        hyper.in_memory_last_cno(),
        baseline_cno,
        "cno moved despite rollback",
    );

    let _ = hyper.fs_release().await;
    tf.cleanup(&client).await;
}

// --------------------------------------------------------------------
// B: segment-done failure path
// --------------------------------------------------------------------

/// B: when `segwr.done()` (segment upload) always fails, flush must return
/// Err and the persisted file must be unchanged. Rollback clears the
/// in-memory mutations.
#[tokio::test]
#[ignore]
async fn contract_segment_done_fail_persisted_unchanged() {
    let _ = env_logger::try_init();
    let client = make_client().await;
    let tf = TestFile::new(&client).await;

    let original_size = 512usize;
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

    let interceptor = AlwaysFailSegmentDone::new();
    let write_res = {
        let mut hyper = Hyper::fs_open(&client, tf.uri(), FileFlags::rdwr())
            .await
            .expect("open");
        hyper.with_staging_interceptor(interceptor.clone());

        let _ = hyper.fs_write(1024, &[0xFFu8; 128]).await.expect("write buffered");
        let res = hyper.fs_flush().await;
        let _ = hyper.fs_release().await;
        res
    };

    assert!(
        write_res.is_err(),
        "expected flush to fail when segment done fails, got {:?}",
        write_res,
    );
    assert!(interceptor.call_count() >= 1, "segment done interceptor never invoked");
    println!(
        "[segment_done_fail] flush={:?} interceptor_calls={}",
        write_res, interceptor.call_count()
    );

    // Reopen and confirm persisted file hasn't grown.
    let persisted_size = {
        let hyper = Hyper::fs_open(&client, tf.uri(), FileFlags::rdonly())
            .await
            .expect("reopen");
        hyper.fs_getattr().expect("getattr").st_size as usize
    };
    assert_eq!(
        persisted_size, original_size,
        "persisted size changed after failed segment upload",
    );

    tf.cleanup(&client).await;
}

// --------------------------------------------------------------------
// C: attr-only flush takes the early-return path
// --------------------------------------------------------------------

/// C: flushing when only inode attrs are dirty (no data / bmap changes)
/// should NOT allocate a new segment — the flush updates inode only.
#[tokio::test]
#[ignore]
async fn contract_attr_only_flush_does_not_produce_new_segment() {
    let _ = env_logger::try_init();
    let client = make_client().await;
    let tf = TestFile::new(&client).await;

    // Create file, write data, flush — this creates segment 1.
    let cno_after_write = {
        let mut hyper = Hyper::fs_open_or_create_with_default_opt(
            &client,
            tf.uri(),
            FileFlags::rdwr(),
            FileMode::default_file(),
        )
        .await
        .expect("create");
        let _ = hyper.fs_write(0, &[0xABu8; 1024]).await.expect("write");
        let cno = hyper.fs_flush().await.expect("flush");
        let _ = hyper.fs_release().await;
        cno
    };
    assert!(cno_after_write > 0, "first flush should produce a segment");

    // Reopen, chmod only (no data/bmap changes). `fs_chmod` already calls
    // flush() internally, so we can observe the post-flush state directly.
    let cno_after_chmod = {
        let mut hyper = Hyper::fs_open(&client, tf.uri(), FileFlags::rdwr())
            .await
            .expect("open");
        assert_eq!(hyper.in_memory_last_cno(), cno_after_write);

        let _ = hyper.fs_chmod(0o600).await.expect("chmod");

        // After chmod's internal flush: no dirty state, cno unchanged because
        // the attr-only early-return path did not allocate a new segment.
        assert!(!hyper.is_attr_dirty(), "attr_dirty should be cleared by chmod's flush");
        assert!(!hyper.is_bmap_dirty(), "bmap should not be dirty after attr-only flush");
        assert_eq!(hyper.dirty_block_count(), 0, "no dirty blocks after attr-only flush");

        let cno = hyper.in_memory_last_cno();
        let _ = hyper.fs_release().await;
        cno
    };

    // attr-only flush takes the early-return branch: cno unchanged.
    assert_eq!(
        cno_after_chmod, cno_after_write,
        "attr-only flush should not advance cno",
    );

    // Reopen and verify the mode was persisted despite no new segment.
    let mode = {
        let hyper = Hyper::fs_open(&client, tf.uri(), FileFlags::rdonly())
            .await
            .expect("reopen");
        hyper.fs_getattr().expect("getattr").st_mode
    };
    assert_eq!(mode & 0o777, 0o600, "mode change did not persist");

    tf.cleanup(&client).await;
}

// --------------------------------------------------------------------
// D: retry policy
// --------------------------------------------------------------------

/// D: a single ResourceBusy failure should be retried. The flush should
/// eventually succeed and commit the mutations.
#[tokio::test]
#[ignore]
async fn contract_flush_retries_on_resource_busy() {
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

    let interceptor = ResourceBusyOnceFlushInode::new();
    let flush_res = {
        let mut hyper = Hyper::fs_open(&client, tf.uri(), FileFlags::rdwr())
            .await
            .expect("open");
        hyper.with_staging_interceptor(interceptor.clone());
        let _ = hyper.fs_write(0, b"retry me").await.expect("write");
        let res = hyper.fs_flush().await;
        let _ = hyper.fs_release().await;
        res
    };

    assert!(
        flush_res.is_ok(),
        "flush should succeed after ResourceBusy retry, got {:?}",
        flush_res,
    );
    assert!(
        interceptor.call_count() >= 2,
        "interceptor should have been invoked at least twice (1 fail + 1 retry), got {}",
        interceptor.call_count(),
    );
    println!(
        "[retry_resource_busy] flush={:?} interceptor_calls={}",
        flush_res, interceptor.call_count()
    );

    // Verify the data was committed.
    let persisted_size = {
        let hyper = Hyper::fs_open(&client, tf.uri(), FileFlags::rdonly())
            .await
            .expect("reopen");
        hyper.fs_getattr().expect("getattr").st_size as usize
    };
    assert_eq!(persisted_size, 8, "retried flush should have committed");

    tf.cleanup(&client).await;
}

/// D: a non-retryable error (ErrorKind::Other) must NOT trigger retries.
/// The interceptor should be invoked exactly once per flush attempt.
#[tokio::test]
#[ignore]
async fn contract_flush_does_not_retry_on_other_error() {
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

    let interceptor = FailOtherNoRetry::new();
    let flush_res = {
        let mut hyper = Hyper::fs_open(&client, tf.uri(), FileFlags::rdwr())
            .await
            .expect("open");
        hyper.with_staging_interceptor(interceptor.clone());
        let _ = hyper.fs_write(0, b"fail me").await.expect("write");
        let res = hyper.fs_flush().await;
        // Release also calls flush; count it to confirm no retry loop.
        let _ = hyper.fs_release().await;
        res
    };

    assert!(flush_res.is_err(), "flush should fail");
    // fs_flush runs flush once, fs_release runs flush once. With NO retry
    // on Other, the interceptor is invoked exactly twice total (never
    // 3x from the retry loop). Allow <= 2 in case release skips flush
    // because rollback cleared dirty state.
    assert!(
        interceptor.call_count() <= 2,
        "expected <= 2 interceptor calls (1 from fs_flush, up to 1 from fs_release), got {}",
        interceptor.call_count(),
    );
    println!(
        "[no_retry_on_other] flush={:?} interceptor_calls={}",
        flush_res, interceptor.call_count()
    );

    tf.cleanup(&client).await;
}

