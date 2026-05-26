//! Reactor + range-lock integration tests.
//!
//! Requires `--features range-lock`. Under this feature, `reactor.rs`
//! acquires a byte-range lock (aligned to data_block_size) at
//! `spawn_read` / `spawn_write` / `spawn_write_zero` entry. The
//! per-file semaphore is loosened to MAX_PERMITS so concurrency is
//! bounded purely by range overlap.
//!
//! Test goals:
//!
//!  - Concurrent writes to DISJOINT ranges must both succeed.
//!  - Concurrent writes to OVERLAPPING ranges must serialize — the
//!    persisted content must match one writer's payload (not a torn
//!    mix of both).
//!  - Flush must wait for in-flight writes rather than racing them.
//!
//! Run with:
//! ```bash
//! cargo test --features range-lock --test integration_reactor_s3_range_lock \
//!     -- --ignored --test-threads=1
//! ```

#![cfg(all(feature = "reactor", feature = "range-lock"))]

#[allow(dead_code)]
mod common;
#[allow(dead_code)]
mod common_reactor;

use common::*;
use common_reactor::*;

use hyperfile::file::fh::HyperFileHandler;
use hyperfile::file::flags::FileFlags;
use hyperfile::file::mode::FileMode;

/// Two disjoint (non-overlapping, block-aligned) writes run concurrently
/// through clones of the same handler. Both must succeed; the final
/// file must contain both payloads side by side.
#[tokio::test]
#[ignore]
async fn reactor_rl_concurrent_disjoint_writes_succeed() {
    let _ = env_logger::try_init();
    let client = make_client().await;
    let tf = TestFile::new(&client).await;

    let reactor = make_reactor();

    // Pre-create so both writers open the same file.
    {
        let mut fh = HyperFileHandler::fh_open_or_create_with_default_opt(
            &reactor, &client, tf.uri(), FileFlags::rdwr(), FileMode::default_file(),
        )
        .await
        .expect("create");
        let _ = fh.fh_release().await;
    }

    // Open once, clone for the two concurrent writers.
    let fh = HyperFileHandler::fh_open(
        &reactor, &client, tf.uri(), FileFlags::rdwr(),
    )
    .await
    .expect("open");

    let mut fh_a = fh.clone();
    let mut fh_b = fh.clone();
    // Drop the original so only the two workers hold references; the
    // handler loop finishes when both workers release their handles.
    drop(fh);

    let writer_a = async move {
        let data = vec![0xAAu8; 4096];
        fh_a.fh_write(0, &data).await.expect("A write");
        fh_a.fh_flush().await.expect("A flush");
        fh_a.fh_release().await.expect("A release")
    };

    let writer_b = async move {
        let data = vec![0xBBu8; 4096];
        // Non-overlapping with A's range [0, 4096).
        fh_b.fh_write(8192, &data).await.expect("B write");
        fh_b.fh_flush().await.expect("B flush");
        fh_b.fh_release().await.expect("B release")
    };

    let (a_cno, b_cno) = tokio::join!(writer_a, writer_b);
    println!(
        "[rl_disjoint] A cno={} B cno={}",
        a_cno, b_cno
    );

    // Verify persisted state contains both payloads.
    let mut fh = HyperFileHandler::fh_open(
        &reactor, &client, tf.uri(), FileFlags::rdonly(),
    )
    .await
    .expect("reopen");
    let stat = fh.fh_getattr().await.expect("getattr");
    assert!(stat.st_size as usize >= 8192 + 4096);

    let mut buf_a = vec![0u8; 4096];
    fh.fh_read(0, &mut buf_a).await.expect("read A");
    assert!(buf_a.iter().all(|&b| b == 0xAA), "A's range was not preserved");

    let mut buf_b = vec![0u8; 4096];
    fh.fh_read(8192, &mut buf_b).await.expect("read B");
    assert!(buf_b.iter().all(|&b| b == 0xBB), "B's range was not preserved");

    let _ = fh.fh_release().await;
    tf.cleanup(&client).await;
}

/// Two writes to the SAME 4 KiB block from two clones. With range-lock
/// they serialize at the reactor layer. The persisted block must be
/// homogeneous — either all A's bytes or all B's, not a torn mix.
#[tokio::test]
#[ignore]
async fn reactor_rl_concurrent_overlapping_writes_serialize() {
    let _ = env_logger::try_init();
    let client = make_client().await;
    let tf = TestFile::new(&client).await;

    let reactor = make_reactor();

    {
        let mut fh = HyperFileHandler::fh_open_or_create_with_default_opt(
            &reactor, &client, tf.uri(), FileFlags::rdwr(), FileMode::default_file(),
        )
        .await
        .expect("create");
        let _ = fh.fh_release().await;
    }

    let fh = HyperFileHandler::fh_open(
        &reactor, &client, tf.uri(), FileFlags::rdwr(),
    )
    .await
    .expect("open");

    let mut fh_a = fh.clone();
    let mut fh_b = fh.clone();
    drop(fh);

    let writer_a = async move {
        // Same block [0, 4096) as B.
        let data = vec![0xAAu8; 4096];
        let _ = fh_a.fh_write(0, &data).await.expect("A write");
        let _ = fh_a.fh_flush().await;
        let _ = fh_a.fh_release().await;
    };

    let writer_b = async move {
        let data = vec![0xBBu8; 4096];
        let _ = fh_b.fh_write(0, &data).await.expect("B write");
        let _ = fh_b.fh_flush().await;
        let _ = fh_b.fh_release().await;
    };

    tokio::join!(writer_a, writer_b);

    // Verify the 4 KiB block is homogeneous — either all A (0xAA) or
    // all B (0xBB). Anything else means the writes overlapped.
    let mut fh = HyperFileHandler::fh_open(
        &reactor, &client, tf.uri(), FileFlags::rdonly(),
    )
    .await
    .expect("reopen");
    let mut buf = vec![0u8; 4096];
    fh.fh_read(0, &mut buf).await.expect("read");
    let _ = fh.fh_release().await;

    let all_aa = buf.iter().all(|&b| b == 0xAA);
    let all_bb = buf.iter().all(|&b| b == 0xBB);
    assert!(
        all_aa || all_bb,
        "block is torn between two concurrent writers (first={:#x} last={:#x})",
        buf[0], buf[4095],
    );

    tf.cleanup(&client).await;
}

/// A flush issued while a write is in flight must not race it. After
/// both complete, the file must reflect the write (flush either
/// committed the dirty state or was a no-op and the subsequent write
/// is still buffered → the next read on reopen sees the write).
#[tokio::test]
#[ignore]
async fn reactor_rl_flush_waits_for_inflight_writes() {
    let _ = env_logger::try_init();
    let client = make_client().await;
    let tf = TestFile::new(&client).await;

    let reactor = make_reactor();
    {
        let mut fh = HyperFileHandler::fh_open_or_create_with_default_opt(
            &reactor, &client, tf.uri(), FileFlags::rdwr(), FileMode::default_file(),
        )
        .await
        .expect("create");
        let _ = fh.fh_release().await;
    }

    let fh = HyperFileHandler::fh_open(
        &reactor, &client, tf.uri(), FileFlags::rdwr(),
    )
    .await
    .expect("open");

    let mut fh_w = fh.clone();
    let mut fh_f = fh.clone();
    drop(fh);

    // Writer does a large-ish write so the flush has something to
    // observe (and race with, if the lock didn't hold).
    let payload = vec![0xCDu8; 64 * 1024];
    let payload_expected = payload.clone();

    let writer = async move {
        fh_w.fh_write(0, &payload).await.expect("write");
        fh_w.fh_flush().await.expect("writer flush");
        fh_w.fh_release().await.expect("writer release")
    };

    let flusher = async move {
        fh_f.fh_flush().await.expect("flusher flush");
        fh_f.fh_release().await.expect("flusher release")
    };

    let (w_cno, f_cno) = tokio::join!(writer, flusher);
    println!("[rl_flush_wait] writer_cno={} flusher_cno={}", w_cno, f_cno);

    // Regardless of flush-vs-write interleaving, the final persisted
    // state must reflect the write in full.
    let mut fh = HyperFileHandler::fh_open(
        &reactor, &client, tf.uri(), FileFlags::rdonly(),
    )
    .await
    .expect("reopen");
    let stat = fh.fh_getattr().await.expect("getattr");
    assert_eq!(stat.st_size as usize, payload_expected.len());

    let mut buf = vec![0u8; payload_expected.len()];
    fh.fh_read(0, &mut buf).await.expect("read");
    assert_eq!(buf, payload_expected, "persisted data mismatch after race");

    let _ = fh.fh_release().await;
    tf.cleanup(&client).await;
}
