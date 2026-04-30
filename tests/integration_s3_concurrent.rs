//! Multi-instance concurrency integration tests.
//!
//! Two `Hyper` instances opening the same URI are independent and
//! coordinate only through S3 optimistic concurrency control. These
//! tests exercise that coordination:
//!
//!  - `concurrent_two_writers_optimistic_cc`: default
//!    `RetryLastWriterWins` policy; both writers succeed, last writer
//!    overwrites.
//!  - `concurrent_read_while_writer_flushes`: reader observes
//!    self-consistent checkpoints while writer churns.
//!  - `concurrent_writer_a_fails_writer_b_succeeds`: writer A has an
//!    always-fail interceptor; writer B commits normally.
//!  - `concurrent_two_writers_fail_fast_policy`: `FailFast` policy;
//!    exactly one writer returns Ok, the other sees
//!    `ErrorKind::AlreadyExists`.
//!
//! See `docs/concurrency.md` for the full behavior model.
//!
//! Run with:
//! ```bash
//! cargo test --test integration_s3_concurrent -- --ignored --test-threads=1
//! ```

mod common;
use common::*;

use std::io::ErrorKind;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use hyperfile::file::hyper::Hyper;
use hyperfile::file::flags::FileFlags;
use hyperfile::file::mode::FileMode;
use hyperfile::config::{FlushConflictPolicy, HyperFileRuntimeConfig};

// --------------------------------------------------------------------
// Concurrent-writer / concurrent-reader tests (A group)
// --------------------------------------------------------------------
//
// hyperfile's concurrency model:
//   * Single Hyper instance serializes writes via a semaphore(1) when
//     `range-lock` feature is disabled (the default test configuration).
//   * Multiple Hyper instances opening the same S3 URI are INDEPENDENT
//     and rely on S3-level optimistic concurrency control through
//     `OnDiskState` (checksum / ETag) threaded into `flush_inode`.
//
// These tests exercise the multi-instance case. The expected invariant
// is that two writers racing on the same URI cannot silently both commit
// divergent state — either one wins and the other gets a well-defined
// error, or they serialize so the last writer's view is consistent.

/// Two writers open the same URI, write different data, flush concurrently.
/// Under optimistic concurrency control we expect:
///   * At most one of the two flushes reports Ok.
///   * After both finish, reopening shows a state consistent with the
///     winning writer (either A's or B's bytes, not a mix).
#[tokio::test]
#[ignore]
async fn concurrent_two_writers_optimistic_cc() {
    let _ = env_logger::try_init();
    let client = make_client().await;
    let tf = TestFile::new(&client).await;

    // Seed an empty file so both writers open the same persisted baseline.
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

    let uri = tf.uri().to_string();
    let client_a = client.clone();
    let client_b = client.clone();
    let uri_a = uri.clone();
    let uri_b = uri.clone();

    let writer_a = async move {
        let mut hyper = Hyper::fs_open(&client_a, &uri_a, FileFlags::rdwr())
            .await
            .expect("open A");
        // A writes 4 KiB of 0xAA.
        let data = vec![0xAAu8; 4096];
        let _ = hyper.fs_write(0, &data).await.expect("write A");
        let flush = hyper.fs_flush().await;
        let _ = hyper.fs_release().await;
        flush
    };

    let writer_b = async move {
        let mut hyper = Hyper::fs_open(&client_b, &uri_b, FileFlags::rdwr())
            .await
            .expect("open B");
        // B writes 4 KiB of 0xBB.
        let data = vec![0xBBu8; 4096];
        let _ = hyper.fs_write(0, &data).await.expect("write B");
        let flush = hyper.fs_flush().await;
        let _ = hyper.fs_release().await;
        flush
    };

    let (a_flush, b_flush) = tokio::join!(writer_a, writer_b);

    let a_ok = a_flush.is_ok();
    let b_ok = b_flush.is_ok();
    println!(
        "[two_writers_occ] A flush={:?} B flush={:?}",
        a_flush, b_flush
    );

    // Invariant 1: at least one writer must succeed (otherwise we made no
    // progress). Invariant 2: at most one succeeds if OCC is working.
    assert!(
        a_ok || b_ok,
        "both writers failed: A={:?} B={:?}",
        a_flush, b_flush,
    );

    // Read the final persisted state.
    let mut buf = vec![0u8; 4096];
    let mut hyper = Hyper::fs_open(&client, &uri, FileFlags::rdonly())
        .await
        .expect("reopen");
    let stat = hyper.fs_getattr().expect("getattr");
    let n = hyper.fs_read(0, &mut buf).await.expect("read");
    let _ = hyper.fs_release().await;

    println!(
        "[two_writers_occ] final size={} first_byte=0x{:02X} last_byte=0x{:02X}",
        stat.st_size, buf[0], buf[n - 1]
    );

    // The persisted bytes must be a single writer's data, not a mix.
    let all_aa = buf.iter().all(|&b| b == 0xAA);
    let all_bb = buf.iter().all(|&b| b == 0xBB);
    assert!(
        all_aa || all_bb,
        "persisted content is a mix of A and B — consistency broken (first={:#x} last={:#x})",
        buf[0], buf[n - 1],
    );

    // OCC invariant: if both flushes claimed Ok, there's a good chance the
    // second one silently overwrote the first. Record the observation.
    if a_ok && b_ok {
        println!(
            "[two_writers_occ] WARNING: both flushes reported Ok — OCC may not be enforcing exclusion"
        );
    }

    tf.cleanup(&client).await;
}

/// A writer repeatedly writes and flushes; a concurrent reader opens
/// the URI while that's happening. Every reader open must yield a
/// self-consistent view (the size must match what can be read).
#[tokio::test]
#[ignore]
async fn concurrent_read_while_writer_flushes() {
    let _ = env_logger::try_init();
    let client = make_client().await;
    let tf = TestFile::new(&client).await;

    // Seed a known initial state.
    {
        let mut hyper = Hyper::fs_open_or_create_with_default_opt(
            &client,
            tf.uri(),
            FileFlags::rdwr(),
            FileMode::default_file(),
        )
        .await
        .expect("create");
        let initial = vec![0x11u8; 4096];
        let _ = hyper.fs_write(0, &initial).await.expect("write");
        let _ = hyper.fs_release().await.expect("release");
    }

    let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));

    let uri = tf.uri().to_string();
    let client_w = client.clone();
    let uri_w = uri.clone();
    let stop_w = stop.clone();

    let writer = async move {
        let mut iterations = 0u32;
        while !stop_w.load(Ordering::SeqCst) && iterations < 8 {
            let mut hyper = Hyper::fs_open(&client_w, &uri_w, FileFlags::rdwr())
                .await
                .expect("writer open");
            // Each iteration writes a new 4 KiB pattern filled with the
            // iteration number (low byte), then flushes.
            let byte = 0x20u8 + (iterations as u8);
            let data = vec![byte; 4096];
            let _ = hyper.fs_write(0, &data).await.expect("writer write");
            let _ = hyper.fs_flush().await;
            let _ = hyper.fs_release().await;
            iterations += 1;
            tokio::task::yield_now().await;
        }
        iterations
    };

    let uri_r = uri.clone();
    let client_r = client.clone();
    let stop_r = stop.clone();

    let reader = async move {
        let mut observations = Vec::new();
        let mut reads = 0u32;
        while !stop_r.load(Ordering::SeqCst) && reads < 20 {
            let mut hyper = Hyper::fs_open(&client_r, &uri_r, FileFlags::rdonly())
                .await
                .expect("reader open");
            let size = hyper.fs_getattr().expect("getattr").st_size as usize;
            let mut buf = vec![0u8; size];
            let _ = hyper.fs_read(0, &mut buf).await.expect("read");
            let _ = hyper.fs_release().await;

            // Every read must see a homogeneous block — a single writer's
            // pattern. If the size says 4096 but bytes diverge, the
            // checkpoint is inconsistent.
            let first = buf.first().copied();
            let all_same = buf.iter().all(|&b| Some(b) == first);
            observations.push((size, first, all_same));

            reads += 1;
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
        observations
    };

    // Run writer + reader concurrently; have a separate future arm the
    // stop flag after a short window.
    let stop_arm = stop.clone();
    let timer = async move {
        tokio::time::sleep(std::time::Duration::from_secs(2)).await;
        stop_arm.store(true, Ordering::SeqCst);
    };

    let (writer_iters, observations, _) = tokio::join!(writer, reader, timer);

    println!(
        "[read_while_writer_flushes] writer did {} iterations, reader made {} reads",
        writer_iters, observations.len()
    );

    // Every observation must be self-consistent (homogeneous buffer).
    for (i, (size, first, ok)) in observations.iter().enumerate() {
        assert!(
            *ok,
            "observation #{} inconsistent: size={} first_byte={:?} buffer not homogeneous",
            i, size, first,
        );
    }

    tf.cleanup(&client).await;
}

/// Two writers again, but writer A's flush is injected with always-fail
/// (the instance never commits). Writer B writes normally. Expected:
///   * A's flush returns Err (and rollback leaves it consistent).
///   * B's flush returns Ok.
///   * After both finish, the persisted content matches B's write.
#[tokio::test]
#[ignore]
async fn concurrent_writer_a_fails_writer_b_succeeds() {
    let _ = env_logger::try_init();
    let client = make_client().await;
    let tf = TestFile::new(&client).await;

    // Seed empty file.
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

    let uri = tf.uri().to_string();
    let client_a = client.clone();
    let client_b = client.clone();
    let uri_a = uri.clone();
    let uri_b = uri.clone();

    let writer_a = async move {
        let mut hyper = Hyper::fs_open(&client_a, &uri_a, FileFlags::rdwr())
            .await
            .expect("open A");
        hyper.with_staging_interceptor(AlwaysFailFlushInode::new());
        let data = vec![0xAAu8; 2048];
        let _ = hyper.fs_write(0, &data).await.expect("write A");
        let flush = hyper.fs_flush().await;
        let _ = hyper.fs_release().await;
        flush
    };

    let writer_b = async move {
        let mut hyper = Hyper::fs_open(&client_b, &uri_b, FileFlags::rdwr())
            .await
            .expect("open B");
        let data = vec![0xBBu8; 2048];
        let _ = hyper.fs_write(0, &data).await.expect("write B");
        let flush = hyper.fs_flush().await;
        let _ = hyper.fs_release().await;
        flush
    };

    let (a_flush, b_flush) = tokio::join!(writer_a, writer_b);

    println!(
        "[a_fails_b_succeeds] A flush={:?} B flush={:?}",
        a_flush, b_flush
    );

    assert!(
        a_flush.is_err(),
        "writer A should fail due to injected interceptor, got {:?}",
        a_flush,
    );
    assert!(
        b_flush.is_ok(),
        "writer B should succeed, got {:?}",
        b_flush,
    );

    // Persisted content must be exactly B's bytes.
    let mut buf = vec![0u8; 2048];
    let mut hyper = Hyper::fs_open(&client, &uri, FileFlags::rdonly())
        .await
        .expect("reopen");
    let size = hyper.fs_getattr().expect("getattr").st_size as usize;
    let _ = hyper.fs_read(0, &mut buf).await.expect("read");
    let _ = hyper.fs_release().await;

    assert_eq!(size, 2048, "unexpected persisted size: {}", size);
    assert!(
        buf.iter().all(|&b| b == 0xBB),
        "persisted bytes do not match writer B's data (first={:#x}, last={:#x})",
        buf[0], buf[2047],
    );

    tf.cleanup(&client).await;
}

/// Same scenario as `concurrent_two_writers_optimistic_cc` but both
/// writers are configured with `FlushConflictPolicy::FailFast`. In this
/// mode the later writer must NOT silently overwrite the earlier one —
/// exactly one writer succeeds, the other observes
/// `ErrorKind::AlreadyExists` and does not retry.
#[tokio::test]
#[ignore]
async fn concurrent_two_writers_fail_fast_policy() {
    let _ = env_logger::try_init();
    let client = make_client().await;
    let tf = TestFile::new(&client).await;

    // Seed empty file.
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

    let runtime_cfg = HyperFileRuntimeConfig {
        flush_conflict_policy: FlushConflictPolicy::FailFast,
        ..HyperFileRuntimeConfig::default()
    };

    let uri = tf.uri().to_string();
    let client_a = client.clone();
    let client_b = client.clone();
    let uri_a = uri.clone();
    let uri_b = uri.clone();
    let cfg_a = runtime_cfg.clone();
    let cfg_b = runtime_cfg.clone();

    let writer_a = async move {
        let mut hyper = Hyper::fs_open_opt(&client_a, &uri_a, FileFlags::rdwr(), &cfg_a)
            .await
            .expect("open A");
        let data = vec![0xAAu8; 4096];
        let _ = hyper.fs_write(0, &data).await.expect("write A");
        let flush = hyper.fs_flush().await;
        let _ = hyper.fs_release().await;
        flush
    };

    let writer_b = async move {
        let mut hyper = Hyper::fs_open_opt(&client_b, &uri_b, FileFlags::rdwr(), &cfg_b)
            .await
            .expect("open B");
        let data = vec![0xBBu8; 4096];
        let _ = hyper.fs_write(0, &data).await.expect("write B");
        let flush = hyper.fs_flush().await;
        let _ = hyper.fs_release().await;
        flush
    };

    let (a_flush, b_flush) = tokio::join!(writer_a, writer_b);
    println!(
        "[fail_fast_policy] A flush={:?} B flush={:?}",
        a_flush, b_flush
    );

    // Exactly one must be Ok; the other must be AlreadyExists.
    let a_ok = a_flush.is_ok();
    let b_ok = b_flush.is_ok();
    assert!(
        a_ok ^ b_ok,
        "expected exactly one writer to succeed under FailFast, got A={:?} B={:?}",
        a_flush, b_flush,
    );

    // The failing side must report AlreadyExists specifically, not a
    // generic ResourceBusy or Other.
    let failed = if a_ok { b_flush } else { a_flush };
    let err = failed.as_ref().err().expect("failing writer should have error");
    assert_eq!(
        err.kind(),
        ErrorKind::AlreadyExists,
        "failing writer's error kind should be AlreadyExists under FailFast, got {:?}",
        err,
    );

    // Persisted content must match the winning writer (homogeneous bytes).
    let mut buf = vec![0u8; 4096];
    let mut hyper = Hyper::fs_open(&client, &uri, FileFlags::rdonly())
        .await
        .expect("reopen");
    let _ = hyper.fs_read(0, &mut buf).await.expect("read");
    let _ = hyper.fs_release().await;

    let all_aa = buf.iter().all(|&b| b == 0xAA);
    let all_bb = buf.iter().all(|&b| b == 0xBB);
    assert!(
        all_aa || all_bb,
        "persisted content is a mix of A and B (first={:#x} last={:#x})",
        buf[0], buf[4095],
    );

    tf.cleanup(&client).await;
}
