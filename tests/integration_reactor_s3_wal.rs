//! Reactor + WAL integration tests.
//!
//! Requires `--features wal`. With WAL enabled, writes are durable to
//! a separate S3 prefix (the "WAL root") before flush; recovery on
//! reopen replays unflushed data.
//!
//! Each test configures a WAL prefix as a sibling to the main file
//! staging (`<uri>/wal/`) and verifies:
//!
//!  - Buffered writes produce WAL chunks.
//!  - A successful flush clears the WAL for the committed segid.
//!  - An unflushed write is replayed on reopen (recovery path).
//!
//! Run with:
//! ```bash
//! cargo test --features wal --test integration_reactor_s3_wal \
//!     -- --ignored --test-threads=1
//! ```

#![cfg(all(feature = "reactor", feature = "wal"))]

#[allow(dead_code)]
mod common;
#[allow(dead_code)]
mod common_reactor;

use common::*;
use common_reactor::*;

use hyperfile::file::fh::HyperFileHandler;
use hyperfile::file::flags::FileFlags;
use hyperfile::file::mode::FileMode;
use hyperfile::file::hyper::Hyper;
use hyperfile::file::flags::HyperFileFlags;
use hyperfile::file::mode::HyperFileMode;
use hyperfile::config::{HyperFileConfig, HyperFileConfigBuilder};
use hyperfile::staging::config::StagingConfig;
use hyperfile::wal::config::{HyperFileWalConfig, WalRecoveryMode};
use hyperfile::buffer::{AlignedDataBlockWrapper, BatchDataBlockWrapper};

/// Build a HyperFileConfig with WAL pointing at `<uri>/wal/`.
fn build_wal_config(uri: &str) -> HyperFileConfig {
    let staging_config = StagingConfig::new_s3_uri(uri, None);
    let wal_uri = format!("{}/wal", uri);
    let wal_config = HyperFileWalConfig::new(&wal_uri);
    HyperFileConfigBuilder::new()
        .with_staging_config(&staging_config)
        .with_wal_config(&wal_config)
        .build()
}

/// Unlink both the file and the WAL prefix.
async fn cleanup_with_wal(client: &aws_sdk_s3::Client, uri: &str) {
    // fs_unlink deletes everything under <uri>/ which includes the WAL
    // subdirectory, so a single call is enough.
    let _ = Hyper::fs_unlink(client, uri).await;
}

/// Basic smoke test: create a file with WAL enabled, write + flush +
/// reopen + read. Verifies the WAL path doesn't break normal
/// round trips.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn reactor_wal_smoke_write_flush_reopen() {
    let _ = env_logger::try_init();
    let client = make_client().await;
    let tf = TestFile::new(&client).await;

    let reactor = make_reactor();
    let config = build_wal_config(tf.uri());
    let payload = b"hello wal world".to_vec();

    // Create with WAL config, write, flush, release.
    {
        let hyper = Hyper::create(
            client.clone(),
            config.clone(),
            HyperFileFlags::from_flags(FileFlags::rdwr()),
            HyperFileMode::from_mode(FileMode::default_file()),
        )
        .await
        .expect("create hyper with wal");

        let mut fh = HyperFileHandler::fh_from_hyper(&reactor, hyper)
            .await
            .expect("spawn handler");
        fh.fh_write(0, &payload).await.expect("write");
        fh.fh_flush().await.expect("flush");
        let _ = fh.fh_release().await.expect("release");
    }

    // Reopen with WAL config, verify content.
    {
        let hyper = Hyper::open(
            client.clone(),
            config.clone(),
            HyperFileFlags::from_flags(FileFlags::rdonly()),
        )
        .await
        .expect("open hyper with wal");

        let mut fh = HyperFileHandler::fh_from_hyper(&reactor, hyper)
            .await
            .expect("spawn handler");
        let mut buf = vec![0u8; payload.len()];
        fh.fh_read(0, &mut buf).await.expect("read");
        assert_eq!(buf, payload);
        let _ = fh.fh_release().await;
    }

    cleanup_with_wal(&client, tf.uri()).await;
}

/// After a successful flush the WAL prefix for that segid should be
/// cleared. We verify this indirectly by reopening: if WAL still held
/// stale data, the recovery path would overwrite the flushed state.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn reactor_wal_flush_and_reopen_is_idempotent() {
    let _ = env_logger::try_init();
    let client = make_client().await;
    let tf = TestFile::new(&client).await;

    let reactor = make_reactor();
    let config = build_wal_config(tf.uri());

    // First pass: write A, flush, release.
    let payload_a = vec![0xAAu8; 8192];
    {
        let hyper = Hyper::create(
            client.clone(),
            config.clone(),
            HyperFileFlags::from_flags(FileFlags::rdwr()),
            HyperFileMode::from_mode(FileMode::default_file()),
        )
        .await
        .expect("create");
        let mut fh = HyperFileHandler::fh_from_hyper(&reactor, hyper)
            .await
            .expect("spawn");
        fh.fh_write(0, &payload_a).await.expect("write A");
        fh.fh_flush().await.expect("flush A");
        let _ = fh.fh_release().await;
    }

    // Second pass: open the existing file, overwrite with B, flush.
    let payload_b = vec![0xBBu8; 8192];
    {
        let hyper = Hyper::open(
            client.clone(),
            config.clone(),
            HyperFileFlags::from_flags(FileFlags::rdwr()),
        )
        .await
        .expect("open B");
        let mut fh = HyperFileHandler::fh_from_hyper(&reactor, hyper)
            .await
            .expect("spawn B");
        fh.fh_write(0, &payload_b).await.expect("write B");
        fh.fh_flush().await.expect("flush B");
        let _ = fh.fh_release().await;
    }

    // Third pass: reopen read-only, verify we see B (not A, and not a
    // replayed mix from an uncleared WAL).
    {
        let hyper = Hyper::open(
            client.clone(),
            config.clone(),
            HyperFileFlags::from_flags(FileFlags::rdonly()),
        )
        .await
        .expect("reopen ro");
        let mut fh = HyperFileHandler::fh_from_hyper(&reactor, hyper)
            .await
            .expect("spawn ro");
        let mut buf = vec![0u8; payload_b.len()];
        fh.fh_read(0, &mut buf).await.expect("read");
        assert_eq!(buf, payload_b, "reopen after flush shows stale data");
        let _ = fh.fh_release().await;
    }

    cleanup_with_wal(&client, tf.uri()).await;
}

/// After a successful flush, the WAL prefix for the flushed segid
/// should be cleaned up. The cleanup runs as a fire-and-forget
/// `tokio::spawn` inside the handler, so we give it a moment
/// before listing.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn reactor_wal_delete_after_flush() {
    let _ = env_logger::try_init();
    let client = make_client().await;
    let tf = TestFile::new(&client).await;

    let reactor = make_reactor();
    let config = build_wal_config(tf.uri());
    let payload = vec![0xCDu8; 8192];

    // Create with WAL, write, flush.
    {
        let hyper = Hyper::create(
            client.clone(),
            config.clone(),
            HyperFileFlags::from_flags(FileFlags::rdwr()),
            HyperFileMode::from_mode(FileMode::default_file()),
        )
        .await
        .expect("create hyper with wal");

        let mut fh = HyperFileHandler::fh_from_hyper(&reactor, hyper)
            .await
            .expect("spawn handler");
        fh.fh_write(0, &payload).await.expect("write");
        let _segid = fh.fh_flush().await.expect("flush");

        // Let the fire-and-forget delete task catch up. 1s is plenty
        // over the ~20-100ms ListObjects + DeleteObjects round trip
        // on S3 Express One Zone.
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;

        let _ = fh.fh_release().await;
    }

    // List the WAL prefix directly via the S3 client. It should
    // contain zero objects: every chunk from the flushed segid
    // has been deleted.
    let wal_prefix = format!("{}/wal/", tf.uri().trim_start_matches("s3://")
        .splitn(2, '/')
        .nth(1)
        .expect("uri has no key part"));
    let bucket = test_bucket();
    let list = client
        .list_objects_v2()
        .bucket(&bucket)
        .prefix(&wal_prefix)
        .send()
        .await
        .expect("list_objects_v2");
    let keys: Vec<String> = list
        .contents()
        .iter()
        .filter_map(|o| o.key().map(|s| s.to_string()))
        .collect();
    assert!(
        keys.is_empty(),
        "WAL prefix still holds {} objects after flush: {:?}",
        keys.len(),
        keys,
    );

    cleanup_with_wal(&client, tf.uri()).await;
}

/// Crash recovery: write, do NOT flush, drop the handler + spawner
/// (simulates a process crash), reopen, verify the written data
/// comes back via WAL replay on open.
///
/// `fh_write` returning `Ok` already implies the WAL PUT has
/// completed (the handler only wakes the caller after the spawned
/// WAL PUT and the cache-update callback have both finished), so
/// no extra synchronization is needed here.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn reactor_wal_crash_recovery_replays_unflushed_write() {
    let _ = env_logger::try_init();
    let client = make_client().await;
    let tf = TestFile::new(&client).await;

    let config = build_wal_config(tf.uri());
    let payload = vec![0xE1u8; 8192];

    // Phase 1: create + release cleanly so the file exists on S3
    //          and the WAL prefix is empty.
    {
        let reactor = make_reactor();
        let hyper = Hyper::create(
            client.clone(),
            config.clone(),
            HyperFileFlags::from_flags(FileFlags::rdwr()),
            HyperFileMode::from_mode(FileMode::default_file()),
        )
        .await
        .expect("create");
        let mut fh = HyperFileHandler::fh_from_hyper(&reactor, hyper)
            .await
            .expect("spawn");
        let _ = fh.fh_release().await;
        drop(fh);
    }

    // Phase 2: reopen, write, crash (drop without flush/release).
    {
        let reactor = make_reactor();
        let hyper = Hyper::open(
            client.clone(),
            config.clone(),
            HyperFileFlags::from_flags(FileFlags::rdwr()),
        )
        .await
        .expect("reopen");
        let mut fh = HyperFileHandler::fh_from_hyper(&reactor, hyper)
            .await
            .expect("spawn");

        fh.fh_write(0, &payload).await.expect("write");
        // At this point WAL PUT has completed (write's Ok is
        // only delivered after the spawned WAL PUT task reports
        // back), so dropping here is a safe crash simulation.
        drop(fh);
    }

    // Phase 3: reopen. wal_flush_recovery should fire inside
    //          do_open and replay the WAL chunks, then force a
    //          flush so reading succeeds.
    {
        let reactor = make_reactor();
        let hyper = Hyper::open(
            client.clone(),
            config.clone(),
            HyperFileFlags::from_flags(FileFlags::rdonly()),
        )
        .await
        .expect("reopen after crash");
        let mut fh = HyperFileHandler::fh_from_hyper(&reactor, hyper)
            .await
            .expect("spawn");

        let mut buf = vec![0u8; payload.len()];
        fh.fh_read(0, &mut buf).await.expect("read");
        assert_eq!(
            buf, payload,
            "data did not survive crash + reopen via WAL replay",
        );

        let _ = fh.fh_release().await;
        drop(fh);
    }

    cleanup_with_wal(&client, tf.uri()).await;
}

/// Crash recovery with multiple writes. Writes at two disjoint
/// offsets, both should survive crash-and-recover via WAL replay.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn reactor_wal_crash_recovery_multiple_disjoint_writes() {
    let _ = env_logger::try_init();
    let client = make_client().await;
    let tf = TestFile::new(&client).await;

    let config = build_wal_config(tf.uri());
    let payload_a = vec![0xAAu8; 4096];
    let payload_b = vec![0xBBu8; 4096];
    let off_b = 16384;

    // Phase 1: create + release.
    {
        let reactor = make_reactor();
        let hyper = Hyper::create(
            client.clone(),
            config.clone(),
            HyperFileFlags::from_flags(FileFlags::rdwr()),
            HyperFileMode::from_mode(FileMode::default_file()),
        )
        .await
        .expect("create");
        let mut fh = HyperFileHandler::fh_from_hyper(&reactor, hyper)
            .await
            .expect("spawn");
        let _ = fh.fh_release().await;
        drop(fh);
    }

    // Phase 2: reopen, write two chunks, crash.
    {
        let reactor = make_reactor();
        let hyper = Hyper::open(
            client.clone(),
            config.clone(),
            HyperFileFlags::from_flags(FileFlags::rdwr()),
        )
        .await
        .expect("reopen");
        let mut fh = HyperFileHandler::fh_from_hyper(&reactor, hyper)
            .await
            .expect("spawn");

        fh.fh_write(0, &payload_a).await.expect("write A");
        fh.fh_write(off_b, &payload_b).await.expect("write B");
        // Both writes' WAL PUTs are complete by the time fh_write
        // returns Ok; safe to drop here.
        drop(fh);
    }

    // Phase 3: reopen, verify both payloads survive.
    {
        let reactor = make_reactor();
        let hyper = Hyper::open(
            client.clone(),
            config.clone(),
            HyperFileFlags::from_flags(FileFlags::rdonly()),
        )
        .await
        .expect("reopen after crash");
        let mut fh = HyperFileHandler::fh_from_hyper(&reactor, hyper)
            .await
            .expect("spawn");

        let mut buf_a = vec![0u8; payload_a.len()];
        fh.fh_read(0, &mut buf_a).await.expect("read A");
        assert_eq!(buf_a, payload_a, "A did not survive");

        let mut buf_b = vec![0u8; payload_b.len()];
        fh.fh_read(off_b, &mut buf_b).await.expect("read B");
        assert_eq!(buf_b, payload_b, "B did not survive");

        let _ = fh.fh_release().await;
        drop(fh);
    }

    cleanup_with_wal(&client, tf.uri()).await;
}

/// Crash recovery under concurrent writers via multiple handler
/// clones: two clones issue writes to disjoint byte ranges
/// concurrently through `tokio::join!`, then the handler +
/// spawner are dropped without a flush. Reopening should replay
/// both writes' WAL chunks and both payloads should be present.
///
/// The fh_write pipeline under WAL spans multiple handler hops
/// (Write -> WriteWal -> WriteAbsorbBh) and the per-file
/// semaphore permit is held across those hops. An earlier naive
/// implementation used a blocking acquire_owned().await, which
/// deadlocked here: handler is serial, so handler A couldn't
/// progress through its remaining hops while handler B was
/// blocked inside acquire_owned(). spawn_write/spawn_write_zero
/// now use a non-blocking try_acquire_owned() and re-queue via
/// send_highprio when the permit is unavailable.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn reactor_wal_crash_recovery_multiple_handles() {
    let _ = env_logger::try_init();
    let client = make_client().await;
    let tf = TestFile::new(&client).await;

    let config = build_wal_config(tf.uri());
    let payload_a = vec![0xA1u8; 4096];
    let payload_b = vec![0xB2u8; 4096];
    let off_b = 16384;

    // Phase 1: clean create.
    {
        let reactor = make_reactor();
        let hyper = Hyper::create(
            client.clone(),
            config.clone(),
            HyperFileFlags::from_flags(FileFlags::rdwr()),
            HyperFileMode::from_mode(FileMode::default_file()),
        )
        .await
        .expect("create");
        let mut fh = HyperFileHandler::fh_from_hyper(&reactor, hyper)
            .await
            .expect("spawn");
        let _ = fh.fh_release().await;
        drop(fh);
    }

    // Phase 2: reopen, two concurrent writes via cloned handlers,
    //          then crash.
    {
        let reactor = make_reactor();
        let hyper = Hyper::open(
            client.clone(),
            config.clone(),
            HyperFileFlags::from_flags(FileFlags::rdwr()),
        )
        .await
        .expect("reopen");
        let fh = HyperFileHandler::fh_from_hyper(&reactor, hyper)
            .await
            .expect("spawn");

        let mut fh_a = fh.clone();
        let mut fh_b = fh.clone();
        drop(fh);

        let data_a = payload_a.clone();
        let data_b = payload_b.clone();
        let writer_a = async move {
            fh_a.fh_write(0, &data_a).await.expect("A write");
            fh_a
        };
        let writer_b = async move {
            fh_b.fh_write(off_b, &data_b).await.expect("B write");
            fh_b
        };
        let (fh_a, fh_b) = tokio::join!(writer_a, writer_b);

        // Both fh_write calls have returned Ok, so both WAL PUTs
        // have completed. Crash without flush/release.
        drop(fh_a);
        drop(fh_b);
    }

    // Phase 3: reopen, both payloads should be there.
    {
        let reactor = make_reactor();
        let hyper = Hyper::open(
            client.clone(),
            config.clone(),
            HyperFileFlags::from_flags(FileFlags::rdonly()),
        )
        .await
        .expect("reopen after crash");
        let mut fh = HyperFileHandler::fh_from_hyper(&reactor, hyper)
            .await
            .expect("spawn");

        let mut buf_a = vec![0u8; payload_a.len()];
        fh.fh_read(0, &mut buf_a).await.expect("read A");
        assert_eq!(buf_a, payload_a, "A did not survive concurrent crash");

        let mut buf_b = vec![0u8; payload_b.len()];
        fh.fh_read(off_b, &mut buf_b).await.expect("read B");
        assert_eq!(buf_b, payload_b, "B did not survive concurrent crash");

        let _ = fh.fh_release().await;
        drop(fh);
    }

    cleanup_with_wal(&client, tf.uri()).await;
}

/// Verify the direct API flush path also cleans the WAL prefix.
/// Mirrors reactor_wal_delete_after_flush but goes through
/// `Hyper::fs_*` directly rather than a handler. The delete is
/// still fire-and-forget, so we sleep briefly before listing.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn direct_api_wal_delete_after_flush() {
    let _ = env_logger::try_init();
    let client = make_client().await;
    let tf = TestFile::new(&client).await;

    let payload = vec![0xDDu8; 8192];

    // Build direct-API-friendly runtime config, then hand the
    // config into HyperFile via Hyper::create (the direct API
    // shortcut that composes HyperFileConfig internally is only
    // exposed through fs_create variants, which don't take WAL
    // config — so we use Hyper::create here).
    let config = build_wal_config(tf.uri());

    {
        let mut hyper = Hyper::create(
            client.clone(),
            config.clone(),
            HyperFileFlags::from_flags(FileFlags::rdwr()),
            HyperFileMode::from_mode(FileMode::default_file()),
        )
        .await
        .expect("create");

        hyper.fs_write(0, &payload).await.expect("fs_write");
        let _segid = hyper.fs_flush().await.expect("fs_flush");

        // Give the fire-and-forget delete task time to complete.
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;

        let _ = hyper.fs_release().await;
    }

    // List the WAL prefix directly. It should be empty after
    // flush + delete.
    let wal_prefix = format!(
        "{}/wal/",
        tf.uri()
            .trim_start_matches("s3://")
            .splitn(2, '/')
            .nth(1)
            .expect("uri has no key part"),
    );
    let bucket = test_bucket();
    let list = client
        .list_objects_v2()
        .bucket(&bucket)
        .prefix(&wal_prefix)
        .send()
        .await
        .expect("list_objects_v2");
    let keys: Vec<String> = list
        .contents()
        .iter()
        .filter_map(|o| o.key().map(|s| s.to_string()))
        .collect();
    assert!(
        keys.is_empty(),
        "direct API: WAL prefix still holds {} objects after flush: {:?}",
        keys.len(),
        keys,
    );

    cleanup_with_wal(&client, tf.uri()).await;
}

/// Read a block whose segment is still being uploaded.
///
/// A WAL-protected flush hands the segment upload to a background task,
/// so for a short window the newest data lives only in a memory-pinned
/// buffer registered in `flushing_segments`. Reads of those blocks are
/// planned as `Inmem` and served from that buffer rather than staging.
///
/// It takes a *concurrent* reader to see that window: `fh_flush` does not
/// answer until the upload is done, so one task writing and flushing in
/// sequence never observes it. Here one task writes and flushes in a loop
/// while another reads the same blocks. The data cache is disabled, so
/// those reads cannot be answered from cache and have to go through the
/// planner.
///
/// The reader also has to survive the flush landing mid-flight: the
/// planner decides `Inmem` from `last_ondisk_cno`, and by the time the
/// spawned read runs the upload may have finished, the entry been removed
/// and the pinned buffer dropped. The same bytes are on staging at that
/// point, so the read falls back to reading them from there. Either way it
/// must return data — this used to panic in the spawned task.
///
/// Reads race writes, so the check is that a block is *uniform* and holds
/// one of the patterns written so far. A read served from a half-released
/// buffer, or from a buffer at the wrong offset, shows up as a mixture.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn reactor_wal_read_block_in_segment_still_uploading() {
    let _ = env_logger::try_init();
    let client = make_client().await;
    let tf = TestFile::new(&client).await;

    let reactor = make_reactor();

    // Data cache off, so a read cannot come from cache.
    let staging_config = StagingConfig::new_s3_uri(tf.uri(), None);
    let wal_uri = format!("{}/wal", tf.uri());
    let wal_config = HyperFileWalConfig::new(&wal_uri);
    let mut runtime = hyperfile::config::HyperFileRuntimeConfig::default();
    runtime.data_cache_blocks = 0;
    let config = HyperFileConfigBuilder::new()
        .with_staging_config(&staging_config)
        .with_wal_config(&wal_config)
        .with_runtime_config(&runtime)
        .build();

    const BLK: usize = 4096;
    const NB: usize = 8;
    const ROUNDS: u8 = 20;
    const FIRST: u8 = 0x40;

    let hyper = Hyper::create(
        client.clone(),
        config.clone(),
        HyperFileFlags::from_flags(FileFlags::rdwr()),
        HyperFileMode::from_mode(FileMode::default_file()),
    )
    .await
    .expect("create hyper with wal");
    let fh = HyperFileHandler::fh_from_hyper(&reactor, hyper).await.expect("spawn handler");

    // Seed, so the reader never looks at a hole.
    {
        let mut w = fh.clone();
        for b in 0..NB {
            w.fh_write(b * BLK, &vec![FIRST; BLK]).await.expect("seed write");
        }
        w.fh_flush().await.expect("seed flush");
    }

    let done = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));

    let writer = {
        let (mut w, done) = (fh.clone(), done.clone());
        tokio::spawn(async move {
            for round in 1..ROUNDS {
                let byte = FIRST + round;
                for b in 0..NB {
                    w.fh_write(b * BLK, &vec![byte; BLK]).await.expect("write");
                }
                w.fh_flush().await.expect("flush");
            }
            done.store(true, std::sync::atomic::Ordering::SeqCst);
        })
    };

    let reader = {
        let (r, done) = (fh.clone(), done.clone());
        tokio::spawn(async move {
            let mut reads = 0usize;
            while !done.load(std::sync::atomic::Ordering::SeqCst) {
                for b in 0..NB {
                    let got = r.fh_read_owned(b * BLK, BLK).await.expect("read");
                    assert_eq!(got.len(), BLK, "block {} short read", b);
                    let first = got[0];
                    assert!((FIRST..FIRST + ROUNDS).contains(&first),
                        "block {} starts with {:#x}, which was never written", b, first);
                    assert!(got.iter().all(|&v| v == first),
                        "block {} is not uniform: starts {:#x}, mismatch at {:?} — \
                         a read of an in-flight segment returned the wrong bytes",
                        b, first, got.iter().position(|&v| v != first));
                    reads += 1;
                }
            }
            reads
        })
    };

    writer.await.expect("writer");
    let reads = reader.await.expect("reader");

    // Correctness alone cannot tell a read served from the pinned segment
    // from one that waited for the flush and then read staging, so assert
    // on the counter that distinguishes them. Throughput would not do:
    // writes also overlap a flush now, and their contention brings the
    // read count down to roughly what deferring produced.
    let timing = fh.fh_read_timing().await.expect("read timing");
    assert!(timing.inflight_reads > 0,
        "no read was served from a segment still being written out, so the \
         WAL read path did not run: {} reads completed across {} flushes",
        reads, ROUNDS - 1);
    eprintln!("verified {} concurrent reads across {} flushes, {} of them served \
        from a segment still being written out",
        reads, ROUNDS - 1, timing.inflight_reads);

    let mut fh = fh;
    let _ = fh.fh_release().await.expect("release");
    cleanup_with_wal(&client, tf.uri()).await;
}

/// An unaligned write landing while a flush is in flight has to
/// read-modify-write a block that lives in the segment being uploaded.
///
/// The block is not in the cache, and the bmap points at a segment that is
/// not on staging yet, so the retrieve copies it out of the pinned buffer
/// and the write modifies that copy. Getting the copy wrong is invisible
/// to the write itself and shows up only in the bytes the write did not
/// cover, which is what this checks.
///
/// It also covers the gap that made this unsafe: a WAL write is a
/// multi-hop pipeline, and a flush interleaving between the hop that
/// fetches a block and the hop that modifies it used to sweep that block
/// away, leaving the write to rebuild it from zeroes. The marker survived
/// and everything else in the block was lost.
///
/// Each round starts a flush without waiting for it, so the marker writes
/// that follow are the first touch of a block that now lives only in the
/// segment being uploaded.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn reactor_wal_write_during_flush_keeps_the_untouched_bytes() {
    let _ = env_logger::try_init();
    let client = make_client().await;
    let tf = TestFile::new(&client).await;
    let reactor = make_reactor();

    let staging_config = StagingConfig::new_s3_uri(tf.uri(), None);
    let wal_uri = format!("{}/wal", tf.uri());
    let wal_config = HyperFileWalConfig::new(&wal_uri);
    let mut runtime = hyperfile::config::HyperFileRuntimeConfig::default();
    runtime.data_cache_blocks = 0;
    let config = HyperFileConfigBuilder::new()
        .with_staging_config(&staging_config)
        .with_wal_config(&wal_config)
        .with_runtime_config(&runtime)
        .build();

    const BLK: usize = 4096;
    const NB: usize = 8;
    const ROUNDS: u8 = 16;
    const BASE: u8 = 0x50;
    const MARK: u8 = 0xC7;
    const MARK_OFF: usize = 1000;
    const MARK_LEN: usize = 64;

    let hyper = Hyper::create(
        client.clone(),
        config.clone(),
        HyperFileFlags::from_flags(FileFlags::rdwr()),
        HyperFileMode::from_mode(FileMode::default_file()),
    ).await.expect("create hyper with wal");
    let fh = HyperFileHandler::fh_from_hyper(&reactor, hyper).await.expect("spawn handler");

    let mut markers = 0usize;
    for round in 1..ROUNDS {
        for b in 0..NB {
            let mut w = fh.clone();
            w.fh_write(b * BLK, &vec![BASE + round; BLK]).await.expect("bulk write");
        }
        let flush = {
            let mut w = fh.clone();
            tokio::spawn(async move { w.fh_flush().await })
        };
        for b in 0..NB {
            let mut w = fh.clone();
            w.fh_write(b * BLK + MARK_OFF, &vec![MARK; MARK_LEN]).await.expect("marker write");
            markers += 1;
        }
        flush.await.expect("flush task").expect("flush");
    }

    let mut fh = fh;
    fh.fh_flush().await.expect("final flush");
    let _ = fh.fh_release().await.expect("release");
    assert_eq!(markers, (ROUNDS as usize - 1) * NB, "every marker write should have completed");

    let hyper = Hyper::open(client.clone(), config.clone(),
        HyperFileFlags::from_flags(FileFlags::rdonly())).await.expect("reopen");
    let mut fh = HyperFileHandler::fh_from_hyper(&reactor, hyper).await.expect("spawn handler");
    for b in 0..NB {
        let got = fh.fh_read_owned(b * BLK, BLK).await.expect("read back");
        assert_eq!(got.len(), BLK, "block {} short read", b);
        let base = got[0];
        assert!((BASE..BASE + ROUNDS).contains(&base),
            "block {} starts with {:#x}, which no write produced — the block was \
             rebuilt from nothing instead of from its previous contents", b, base);
        for (i, &v) in got.iter().enumerate() {
            let ok = if (MARK_OFF..MARK_OFF + MARK_LEN).contains(&i) {
                v == MARK || v == base
            } else {
                v == base
            };
            assert!(ok,
                "block {} byte {}: got {:#x} with base {:#x} — a write concurrent \
                 with a flush lost the bytes it did not cover",
                b, i, v, base);
        }
    }
    let _ = fh.fh_release().await;
    cleanup_with_wal(&client, tf.uri()).await;
}

/// When publishing fails and replaying the log cannot clear it either, the
/// file stops accepting writes and goes on serving reads. It does not panic.
///
/// A WAL flush answers as soon as the log holds the data and uploads detached,
/// so by the time that upload fails the caller has already been told — truly —
/// that the data is durable. There is nobody to return an error to. Replaying
/// is the remedy, and it publishes through the same path, so a store that is
/// refusing writes defeats it too.
///
/// That case used to end in `panic!("please fix wal with offline tools")`,
/// which for a server built on this crate is the whole process, including the
/// reads it was still serving correctly.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn reactor_wal_publish_failure_turns_read_only_without_panicking() {
    let _ = env_logger::try_init();
    let client = make_client().await;
    let tf = TestFile::new(&client).await;

    let reactor = make_reactor();
    let config = build_wal_config(tf.uri());

    // Call 1 is create's own inode publish, so fail from call 2 on: the
    // explicit flush and every recovery attempt after it.
    let hyper = Hyper::create_with_interceptor(
        client.clone(),
        config.clone(),
        HyperFileFlags::from_flags(FileFlags::rdwr()),
        HyperFileMode::from_mode(FileMode::default_file()),
        FailOnFlushInode::from(2),
    ).await.expect("create with interceptor");
    let mut fh = HyperFileHandler::fh_from_hyper(&reactor, hyper).await.expect("fh_from_hyper");

    let payload = vec![0x9Cu8; 4096];
    let n = fh.fh_write(0, &payload).await.expect("write must succeed: the wal takes it");
    assert_eq!(n, payload.len());

    // Reading back what was written keeps working throughout.
    let mut buf = vec![0u8; payload.len()];
    let _ = fh.fh_read(0, &mut buf).await.expect("read");
    assert_eq!(buf, payload, "the wal-held data must still read back");

    // Ask for a flush. Whether this reports an error or not is not the point
    // — the publish it triggers runs detached — so drive it and then wait for
    // the bounded retries to be spent.
    let _ = fh.fh_flush().await;
    tokio::time::sleep(std::time::Duration::from_secs(8)).await;

    // Writes are refused now, and say why.
    let err = fh.fh_write(0, &[0xFF; 4096]).await
        .expect_err("writes must be refused once publishing has failed for good");
    assert_eq!(err.kind(), std::io::ErrorKind::ReadOnlyFilesystem, "got {:?}: {}", err.kind(), err);
    assert!(format!("{}", err).contains("wal"), "the message should point at the wal: {}", err);

    let e = fh.fh_write_zero(0, 4096).await.expect_err("write_zero must be refused");
    assert_eq!(e.kind(), std::io::ErrorKind::ReadOnlyFilesystem, "write_zero gave {:?}: {}", e.kind(), e);
    let e = fh.fh_truncate(0).await.expect_err("truncate must be refused");
    assert_eq!(e.kind(), std::io::ErrorKind::ReadOnlyFilesystem, "truncate gave {:?}: {}", e.kind(), e);

    // Reads are still served rather than the process being gone, which is the
    // reason not to panic. What they show is a separate matter: the flush had
    // already repointed the map at the segment it could not publish, so this
    // handle no longer has the newest data to hand. Durability is unaffected —
    // the log holds it — and the assertion that matters is further down.
    let mut buf2 = vec![0u8; payload.len()];
    let _ = fh.fh_read(0, &mut buf2).await.expect("reads must keep being served");
    let _ = fh.fh_getattr().await.expect("getattr must keep working");

    let _ = fh.fh_release().await;
    drop(fh);

    // The point of answering a wal flush early is that the log makes the data
    // durable whether or not the publish lands. So open again without the
    // failure injected: recovery replays the log and the write is there.
    {
        let mut hyper = Hyper::open(
            client.clone(),
            config.clone(),
            HyperFileFlags::from_flags(FileFlags::rdonly()),
        ).await.expect("reopen after a failed publish");
        let mut buf3 = vec![0u8; payload.len()];
        let n = hyper.fs_read(0, &mut buf3).await.expect("read after recovery");
        assert_eq!(n, payload.len());
        assert_eq!(buf3, payload,
            "the write was acknowledged, so recovery must produce it");
        let _ = hyper.fs_release().await;
    }

    cleanup_with_wal(&client, tf.uri()).await;
}

/// A write that returned `Ok` survives without any flush, and
/// `writes_durable_on_ack` is what says so.
///
/// This pins the property a caller relies on when it decides it may skip
/// flushing: the bytes are in the log the moment the write returns, no publish
/// need have happened, and reopening replays them. The publish thresholds are
/// set out of reach so that nothing incidental can produce a segment — if one
/// did, the test would pass for the wrong reason.
///
/// The no-log half is not decoration. Without it, a test that writes, drops
/// and reads the data back proves nothing: the same result would follow from
/// the write never having been lost in the first place. The control shows the
/// bytes really do go missing when the log is not there.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn reactor_wal_writes_are_durable_on_ack_without_any_flush() {
    let _ = env_logger::try_init();
    let client = make_client().await;
    let payload = vec![0x6Du8; 8192];

    // Thresholds no workload of this size can reach, so a publish cannot
    // happen behind the test's back.
    let mut runtime = hyperfile::config::HyperFileRuntimeConfig::default();
    runtime.data_cache_dirty_max_bytes_threshold = usize::MAX / 2;
    runtime.data_cache_dirty_max_blocks_threshold = usize::MAX / 2;
    runtime.data_cache_dirty_max_flush_interval = u64::MAX / 2;

    let with_log = |uri: &str| {
        let staging_config = StagingConfig::new_s3_uri(uri, None);
        let wal_config = HyperFileWalConfig::new(&format!("{}/wal", uri));
        HyperFileConfigBuilder::new()
            .with_staging_config(&staging_config)
            .with_wal_config(&wal_config)
            .with_runtime_config(&runtime)
            .build()
    };
    let without_log = |uri: &str| {
        let staging_config = StagingConfig::new_s3_uri(uri, None);
        HyperFileConfigBuilder::new()
            .with_staging_config(&staging_config)
            .with_runtime_config(&runtime)
            .build()
    };

    // --- with a log: the write survives, and the query says it will ---
    let tf = TestFile::new(&client).await;
    {
        let reactor = make_reactor();
        let hyper = Hyper::create(
            client.clone(), with_log(tf.uri()),
            HyperFileFlags::from_flags(FileFlags::rdwr()),
            HyperFileMode::from_mode(FileMode::default_file()),
        ).await.expect("create");
        assert!(hyper.writes_durable_on_ack(),
            "a container with a log must report the guarantee");
        let mut fh = HyperFileHandler::fh_from_hyper(&reactor, hyper).await.expect("spawn");
        let _ = fh.fh_release().await;
    }
    {
        let reactor = make_reactor();
        let hyper = Hyper::open(
            client.clone(), with_log(tf.uri()),
            HyperFileFlags::from_flags(FileFlags::rdwr()),
        ).await.expect("reopen");
        assert!(hyper.writes_durable_on_ack());
        let mut fh = HyperFileHandler::fh_from_hyper(&reactor, hyper).await.expect("spawn");
        let n = fh.fh_write(0, &payload).await.expect("write");
        assert_eq!(n, payload.len());
        // No flush, no release: the process is gone as far as the container
        // is concerned.
        drop(fh);
        drop(reactor);
    }
    {
        let mut hyper = Hyper::open(
            client.clone(), with_log(tf.uri()),
            HyperFileFlags::from_flags(FileFlags::rdonly()),
        ).await.expect("reopen after crash");
        let mut buf = vec![0u8; payload.len()];
        let _ = hyper.fs_read(0, &mut buf).await.expect("read");
        assert_eq!(buf, payload,
            "the write returned Ok, so it must survive with no flush of any kind");
        let _ = hyper.fs_release().await;
    }
    cleanup_with_wal(&client, tf.uri()).await;

    // --- the control: no log, so the same sequence loses the write ---
    let tf2 = TestFile::new(&client).await;
    {
        let reactor = make_reactor();
        let hyper = Hyper::create(
            client.clone(), without_log(tf2.uri()),
            HyperFileFlags::from_flags(FileFlags::rdwr()),
            HyperFileMode::from_mode(FileMode::default_file()),
        ).await.expect("create");
        assert!(!hyper.writes_durable_on_ack(),
            "without a log there is no such guarantee to report");
        let mut fh = HyperFileHandler::fh_from_hyper(&reactor, hyper).await.expect("spawn");
        let _ = fh.fh_release().await;
    }
    {
        let reactor = make_reactor();
        let hyper = Hyper::open(
            client.clone(), without_log(tf2.uri()),
            HyperFileFlags::from_flags(FileFlags::rdwr()),
        ).await.expect("reopen");
        let mut fh = HyperFileHandler::fh_from_hyper(&reactor, hyper).await.expect("spawn");
        let _ = fh.fh_write(0, &payload).await.expect("write");
        drop(fh);
        drop(reactor);
    }
    {
        let mut hyper = Hyper::open(
            client.clone(), without_log(tf2.uri()),
            HyperFileFlags::from_flags(FileFlags::rdonly()),
        ).await.expect("reopen");
        // Asserted on the size, not on the bytes read: the write left no size
        // behind, so a read returns nothing at all — and "every byte of the
        // zero bytes I read was zero" would be true of any outcome.
        let st = hyper.fs_getattr().expect("getattr");
        assert_eq!(st.st_size, 0,
            "without a log an unflushed write must leave no size behind, else \
             the half above proves nothing");
        let mut buf = vec![0xFFu8; payload.len()];
        let n = hyper.fs_read(0, &mut buf).await.expect("read");
        assert_eq!(n, 0, "and nothing to read");
        let _ = hyper.fs_release().await;
    }
    tf2.cleanup(&client).await;
}

/// Count objects under the WAL prefix.
async fn wal_object_count(client: &aws_sdk_s3::Client, uri: &str) -> usize {
    let bucket = test_bucket();
    let prefix = format!("{}/wal/", uri.strip_prefix(&format!("s3://{}/", bucket)).expect("uri"));
    let mut n = 0;
    let mut token = None;
    loop {
        let mut req = client.list_objects_v2().bucket(&bucket).prefix(&prefix);
        if let Some(t) = token { req = req.continuation_token(t); }
        let r = req.send().await.expect("list wal prefix");
        n += r.contents().len();
        match r.next_continuation_token() {
            Some(t) => token = Some(t.to_string()),
            None => break,
        }
    }
    n
}

/// A batch write reaches the log, like any other write.
///
/// `write_inner` logs; `write_aligned_batch_locked` did not, so a caller using
/// the batch API got a log that stayed empty and a recovery that had nothing to
/// replay. That is not merely a missing feature: the flush path answers early
/// *because* the log holds the data, so writes it never saw are acknowledged
/// while living only in memory.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn reactor_wal_batch_write_reaches_the_log() {
    let _ = env_logger::try_init();
    let client = make_client().await;
    let tf = TestFile::new(&client).await;
    const NB: usize = 24;
    const BLK: usize = 4096;

    let reactor = make_reactor();
    let hyper = Hyper::create(
        client.clone(), build_wal_config(tf.uri()),
        HyperFileFlags::from_flags(FileFlags::rdwr()),
        HyperFileMode::from_mode(FileMode::default_file()),
    ).await.expect("create");
    let mut fh = HyperFileHandler::fh_from_hyper(&reactor, hyper).await.expect("spawn");

    assert_eq!(wal_object_count(&client, tf.uri()).await, 0, "nothing written yet");

    // Data blocks and a hole, so both kinds are covered.
    let mut blocks = Vec::new();
    for i in 0..NB {
        let b = AlignedDataBlockWrapper::new(i as u64, BLK, i == 7);
        if i != 7 {
            b.as_mut_slice().fill((i % 251) as u8);
        }
        blocks.push(b);
    }
    let n = fh.fh_write_aligned_batch(blocks).await.expect("batch write");
    assert_eq!(n, NB * BLK);

    let logged = wal_object_count(&client, tf.uri()).await;
    assert!(logged > 0, "a batch write must reach the log, found {} objects", logged);
    // Adjacent blocks share a record. This layout is two data runs split by a
    // hole, so three records — not one per block, which is the cost the batch
    // API exists to avoid.
    assert_eq!(logged, 3,
        "expected one record per run of adjacent same-kind blocks, got {}", logged);

    let _ = fh.fh_release().await;
    cleanup_with_wal(&client, tf.uri()).await;
}

/// And the guarantee `writes_durable_on_ack` reports holds for it: a batch
/// write that returned `Ok` survives with no flush at all.
///
/// Same shape as `reactor_wal_writes_are_durable_on_ack_without_any_flush`,
/// through the batch API, because that is the API a caller reaches for once it
/// has measured the difference — and the guarantee cannot be true of one write
/// path and false of another while a single method reports it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn reactor_wal_batch_write_is_durable_on_ack() {
    let _ = env_logger::try_init();
    let client = make_client().await;
    let tf = TestFile::new(&client).await;
    const NB: usize = 8;
    const BLK: usize = 4096;

    let mut runtime = hyperfile::config::HyperFileRuntimeConfig::default();
    runtime.data_cache_dirty_max_bytes_threshold = usize::MAX / 2;
    runtime.data_cache_dirty_max_blocks_threshold = usize::MAX / 2;
    runtime.data_cache_dirty_max_flush_interval = u64::MAX / 2;
    let config = {
        let staging_config = StagingConfig::new_s3_uri(tf.uri(), None);
        let wal_config = HyperFileWalConfig::new(&format!("{}/wal", tf.uri()));
        HyperFileConfigBuilder::new()
            .with_staging_config(&staging_config)
            .with_wal_config(&wal_config)
            .with_runtime_config(&runtime)
            .build()
    };

    // Exists, with an empty log.
    {
        let reactor = make_reactor();
        let hyper = Hyper::create(
            client.clone(), config.clone(),
            HyperFileFlags::from_flags(FileFlags::rdwr()),
            HyperFileMode::from_mode(FileMode::default_file()),
        ).await.expect("create");
        let mut fh = HyperFileHandler::fh_from_hyper(&reactor, hyper).await.expect("spawn");
        let _ = fh.fh_release().await;
    }

    // Batch write, then die: no flush, no release.
    {
        let reactor = make_reactor();
        let hyper = Hyper::open(
            client.clone(), config.clone(),
            HyperFileFlags::from_flags(FileFlags::rdwr()),
        ).await.expect("reopen");
        assert!(hyper.writes_durable_on_ack(), "the log is configured");
        let mut fh = HyperFileHandler::fh_from_hyper(&reactor, hyper).await.expect("spawn");
        let mut blocks = Vec::new();
        for i in 0..NB {
            let b = AlignedDataBlockWrapper::new(i as u64, BLK, false);
            b.as_mut_slice().fill(0x40 + i as u8);
            blocks.push(b);
        }
        let _ = fh.fh_write_aligned_batch(blocks).await.expect("batch write");
        drop(fh);
        drop(reactor);
    }

    // Reopen: recovery must produce it.
    {
        let mut hyper = Hyper::open(
            client.clone(), config.clone(),
            HyperFileFlags::from_flags(FileFlags::rdonly()),
        ).await.expect("reopen after crash");
        let st = hyper.fs_getattr().expect("getattr");
        assert_eq!(st.st_size as usize, NB * BLK,
            "the batch write returned Ok, so its size must survive");
        for i in 0..NB {
            let mut buf = vec![0u8; BLK];
            let n = hyper.fs_read(i * BLK, &mut buf).await.expect("read");
            assert_eq!(n, BLK);
            assert!(buf.iter().all(|b| *b == 0x40 + i as u8),
                "block {} did not survive: first byte {:#x}", i, buf[0]);
        }
        let _ = hyper.fs_release().await;
    }

    cleanup_with_wal(&client, tf.uri()).await;
}

/// Crash, recover, crash again with no successful publish in between — and the
/// container stays exactly what was acknowledged, and stays writable.
///
/// The ordering matters and is easy to lose: a consumer reported that a
/// successful flush anywhere between the recovery and the crash hides the
/// problem. So no round flushes. Recovery itself publishes, which is what makes
/// "no publish *after* recovery" the interesting window.
///
/// Batch writes throughout, since that is the API the report came from.
/// Deliberately small — a handful of blocks per round is enough to establish
/// the invariant; volume belongs in a performance measurement, not here.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn reactor_wal_repeated_crash_without_publish_keeps_the_container_sound() {
    let _ = env_logger::try_init();
    let client = make_client().await;
    let tf = TestFile::new(&client).await;
    const BLK: usize = 4096;
    const PER_ROUND: usize = 4;

    // Nothing may publish on its own, so every publish in this test is either
    // recovery's or an explicit flush.
    let mut runtime = hyperfile::config::HyperFileRuntimeConfig::default();
    runtime.data_cache_dirty_max_bytes_threshold = usize::MAX / 2;
    runtime.data_cache_dirty_max_blocks_threshold = usize::MAX / 2;
    runtime.data_cache_dirty_max_flush_interval = u64::MAX / 2;
    let config = {
        let staging_config = StagingConfig::new_s3_uri(tf.uri(), None);
        let wal_config = HyperFileWalConfig::new(&format!("{}/wal", tf.uri()));
        HyperFileConfigBuilder::new()
            .with_staging_config(&staging_config)
            .with_wal_config(&wal_config)
            .with_runtime_config(&runtime)
            .build()
    };

    let batch = |round: usize| {
        let mut blocks = Vec::new();
        for i in 0..PER_ROUND {
            let idx = (round * PER_ROUND + i) as u64;
            let b = AlignedDataBlockWrapper::new(idx, BLK, false);
            b.as_mut_slice().fill((0x10 * (round + 1) + i) as u8);
            blocks.push(b);
        }
        blocks
    };
    let expected = |round: usize, i: usize| (0x10 * (round + 1) + i) as u8;

    // Round 0: create, batch write, die. No flush.
    {
        let reactor = make_reactor();
        let hyper = Hyper::create(
            client.clone(), config.clone(),
            HyperFileFlags::from_flags(FileFlags::rdwr()),
            HyperFileMode::from_mode(FileMode::default_file()),
        ).await.expect("create");
        let mut fh = HyperFileHandler::fh_from_hyper(&reactor, hyper).await.expect("spawn");
        let _ = fh.fh_write_aligned_batch(batch(0)).await.expect("batch 0");
        drop(fh);
        drop(reactor);
    }

    // Rounds 1..3: open (which recovers and publishes), write, die. Nothing
    // between the recovery and the death publishes anything.
    for round in 1..4usize {
        let reactor = make_reactor();
        let hyper = Hyper::open(
            client.clone(), config.clone(),
            HyperFileFlags::from_flags(FileFlags::rdwr()),
        ).await.unwrap_or_else(|e| panic!("round {} reopen: {}", round, e));
        let mut fh = HyperFileHandler::fh_from_hyper(&reactor, hyper).await.expect("spawn");

        // Everything acknowledged so far must be readable right after recovery.
        for r in 0..round {
            for i in 0..PER_ROUND {
                let off = (r * PER_ROUND + i) * BLK;
                let mut buf = vec![0u8; BLK];
                let n = fh.fh_read(off, &mut buf).await
                    .unwrap_or_else(|e| panic!("round {} read of round {} block {}: {}", round, r, i, e));
                assert_eq!(n, BLK, "round {}: short read of round {} block {}", round, r, i);
                assert!(buf.iter().all(|b| *b == expected(r, i)),
                    "round {}: round {} block {} came back {:#x}, expected {:#x}",
                    round, r, i, buf[0], expected(r, i));
            }
        }

        let _ = fh.fh_write_aligned_batch(batch(round)).await
            .unwrap_or_else(|e| panic!("round {} batch write: {}", round, e));
        drop(fh);
        drop(reactor);
    }

    // Final: everything is there, and the container still takes a write and a
    // flush — a container that recovers but cannot be written to is the shape
    // of the report this covers.
    {
        let reactor = make_reactor();
        let hyper = Hyper::open(
            client.clone(), config.clone(),
            HyperFileFlags::from_flags(FileFlags::rdwr()),
        ).await.expect("final reopen");
        let mut fh = HyperFileHandler::fh_from_hyper(&reactor, hyper).await.expect("spawn");

        for r in 0..4usize {
            for i in 0..PER_ROUND {
                let off = (r * PER_ROUND + i) * BLK;
                let mut buf = vec![0u8; BLK];
                let n = fh.fh_read(off, &mut buf).await.expect("final read");
                assert_eq!(n, BLK);
                assert!(buf.iter().all(|b| *b == expected(r, i)),
                    "round {} block {} lost: {:#x} != {:#x}", r, i, buf[0], expected(r, i));
            }
        }

        let tail = AlignedDataBlockWrapper::new(64, BLK, false);
        tail.as_mut_slice().fill(0xFE);
        let _ = fh.fh_write_aligned_batch(vec![tail]).await
            .expect("a recovered container must still be writable");
        let _ = fh.fh_flush().await.expect("and flushable");

        let mut buf = vec![0u8; BLK];
        let _ = fh.fh_read(64 * BLK, &mut buf).await.expect("read tail");
        assert!(buf.iter().all(|b| *b == 0xFE));

        let _ = fh.fh_release().await;
    }

    cleanup_with_wal(&client, tf.uri()).await;
}

/// The partial-block batch path logs too.
///
/// `write_batch` takes writes that do not reach a block boundary and is a
/// separate implementation from the aligned one, so it needed the same fix and
/// needs its own coverage: nothing under `wal` exercised it before.
///
/// A partial record cannot be joined to an adjacent one — its range stops
/// mid-block — so this also checks the mixed case, where full blocks coalesce
/// around a partial one that does not.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn reactor_wal_partial_batch_write_is_durable_on_ack() {
    let _ = env_logger::try_init();
    let client = make_client().await;
    let tf = TestFile::new(&client).await;
    const BLK: usize = 4096;

    let mut runtime = hyperfile::config::HyperFileRuntimeConfig::default();
    runtime.data_cache_dirty_max_bytes_threshold = usize::MAX / 2;
    runtime.data_cache_dirty_max_blocks_threshold = usize::MAX / 2;
    runtime.data_cache_dirty_max_flush_interval = u64::MAX / 2;
    let config = {
        let staging_config = StagingConfig::new_s3_uri(tf.uri(), None);
        let wal_config = HyperFileWalConfig::new(&format!("{}/wal", tf.uri()));
        HyperFileConfigBuilder::new()
            .with_staging_config(&staging_config)
            .with_wal_config(&wal_config)
            .with_runtime_config(&runtime)
            .build()
    };

    // A base the partial write lands inside, published so the partial write has
    // something to modify rather than create.
    {
        let reactor = make_reactor();
        let hyper = Hyper::create(
            client.clone(), config.clone(),
            HyperFileFlags::from_flags(FileFlags::rdwr()),
            HyperFileMode::from_mode(FileMode::default_file()),
        ).await.expect("create");
        let mut fh = HyperFileHandler::fh_from_hyper(&reactor, hyper).await.expect("spawn");
        let _ = fh.fh_write(0, &vec![0x11u8; 4 * BLK]).await.expect("base write");
        let _ = fh.fh_flush().await.expect("publish the base");
        let _ = fh.fh_release().await;
    }

    // Full, partial, full — then die with no flush.
    {
        let reactor = make_reactor();
        let hyper = Hyper::open(
            client.clone(), config.clone(),
            HyperFileFlags::from_flags(FileFlags::rdwr()),
        ).await.expect("reopen");
        let mut fh = HyperFileHandler::fh_from_hyper(&reactor, hyper).await.expect("spawn");

        let full0 = BatchDataBlockWrapper::new(0, BLK, false);
        full0.as_mut_slice().fill(0xA0);
        let full1 = BatchDataBlockWrapper::new(1, BLK, false);
        full1.as_mut_slice().fill(0xA1);
        let part = BatchDataBlockWrapper::new_partial_block(2, BLK, 100, 200, false);
        part.as_mut_slice().fill(0xBB);
        let full3 = BatchDataBlockWrapper::new(3, BLK, false);
        full3.as_mut_slice().fill(0xA3);

        let _ = fh.fh_write_batch(vec![full0, full1, part, full3]).await.expect("batch write");
        drop(fh);
        drop(reactor);
    }

    // Reopen: recovery must reproduce all of it, the partial write included and
    // the bytes around it untouched.
    {
        let mut hyper = Hyper::open(
            client.clone(), config.clone(),
            HyperFileFlags::from_flags(FileFlags::rdonly()),
        ).await.expect("reopen after crash");

        for (blk, want) in [(0usize, 0xA0u8), (1, 0xA1), (3, 0xA3)] {
            let mut buf = vec![0u8; BLK];
            let n = hyper.fs_read(blk * BLK, &mut buf).await.expect("read");
            assert_eq!(n, BLK);
            assert!(buf.iter().all(|b| *b == want),
                "block {} came back {:#x}, expected {:#x}", blk, buf[0], want);
        }

        let mut buf = vec![0u8; BLK];
        let n = hyper.fs_read(2 * BLK, &mut buf).await.expect("read block 2");
        assert_eq!(n, BLK);
        assert!(buf[..100].iter().all(|b| *b == 0x11), "bytes before the partial write changed");
        assert!(buf[100..300].iter().all(|b| *b == 0xBB), "the partial write did not survive");
        assert!(buf[300..].iter().all(|b| *b == 0x11), "bytes after the partial write changed");

        let _ = hyper.fs_release().await;
    }

    cleanup_with_wal(&client, tf.uri()).await;
}

/// List the record names and whether a barrier exists, for one checkpoint
/// prefix. Returns `(record_names, barrier_body)`.
async fn wal_segid_contents(
    client: &aws_sdk_s3::Client, uri: &str, segid: u64,
) -> (Vec<String>, Option<Vec<u8>>) {
    let bucket = test_bucket();
    let root = uri.strip_prefix(&format!("s3://{}/", bucket)).expect("uri");
    let prefix = format!("{}/wal/{:010}/", root, segid);
    let mut names = Vec::new();
    let mut token = None;
    loop {
        let mut req = client.list_objects_v2().bucket(&bucket).prefix(&prefix);
        if let Some(t) = token { req = req.continuation_token(t); }
        let r = req.send().await.expect("list segid prefix");
        for o in r.contents() {
            if let Some(k) = o.key() {
                names.push(k.trim_start_matches(&prefix).to_string());
            }
        }
        match r.next_continuation_token() {
            Some(t) => token = Some(t.to_string()),
            None => break,
        }
    }
    let barrier = match client.get_object().bucket(&bucket)
        .key(format!("{}barrier", prefix)).send().await
    {
        Ok(o) => Some(o.body.collect().await.expect("collect").to_vec()),
        Err(_) => None,
    };
    names.retain(|n| n != "barrier");
    (names, barrier)
}

/// A flush seals the log group it is about to publish, and the barrier's
/// manifest names exactly the records that are there.
///
/// The barrier answers the one question the records cannot: is this group
/// whole? A crash mid-flush leaves a partial set that is indistinguishable from
/// a complete one by looking at the objects, and applying it hands the layer
/// above half a transaction.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn reactor_wal_flush_seals_the_group_it_publishes() {
    let _ = env_logger::try_init();
    let client = make_client().await;
    let tf = TestFile::new(&client).await;
    const BLK: usize = 4096;

    let reactor = make_reactor();
    let hyper = Hyper::create(
        client.clone(), build_wal_config(tf.uri()),
        HyperFileFlags::from_flags(FileFlags::rdwr()),
        HyperFileMode::from_mode(FileMode::default_file()),
    ).await.expect("create");
    let mut fh = HyperFileHandler::fh_from_hyper(&reactor, hyper).await.expect("spawn");

    // Records land under the checkpoint the flush is about to leave behind.
    let segid = fh.fh_last_cno().await.expect("last cno");

    let mut blocks = Vec::new();
    for i in 0..6usize {
        let b = AlignedDataBlockWrapper::new(i as u64, BLK, i == 3);
        if i != 3 { b.as_mut_slice().fill(0x50 + i as u8); }
        blocks.push(b);
    }
    let _ = fh.fh_write_aligned_batch(blocks).await.expect("batch write");

    let (records_before, barrier_before) = wal_segid_contents(&client, tf.uri(), segid).await;
    assert!(!records_before.is_empty(), "records must be there before the flush");
    assert!(barrier_before.is_none(), "nothing is sealed until a flush says so");

    let _ = fh.fh_flush().await.expect("flush");

    let (records, barrier) = wal_segid_contents(&client, tf.uri(), segid).await;
    let body = barrier.expect("a flush must seal the group it publishes");
    let manifest = hyperfile::wal::WalBarrier::decode(&body)
        .expect("the barrier must be readable by this build");

    assert_eq!(manifest.entries.len(), records.len(),
        "manifest lists {} entries, {} records are stored",
        manifest.entries.len(), records.len());
    for (seq, off, len) in manifest.entries.iter() {
        let want = format!("{}_{}_{}", seq, off, len);
        assert!(records.contains(&want),
            "manifest names {} but no such record is stored; stored: {:?}", want, records);
    }

    let _ = fh.fh_release().await;
    cleanup_with_wal(&client, tf.uri()).await;
}

/// The two recovery modes land in different places, and the durability
/// guarantee reports which one it is.
///
/// `Latest` keeps every acknowledged write, which is what
/// `writes_durable_on_ack` promises, so it is the default. `Barrier` stops at
/// the last sealed group, so a write with no flush behind it is discarded by
/// design — and the guarantee has to say so rather than keep claiming a
/// property the mode has given up. A caller that skipped flushing on the
/// strength of it would otherwise lose data silently, which is the whole reason
/// that method is named for the guarantee and not for the log.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn reactor_wal_recovery_mode_decides_the_landing_point() {
    let _ = env_logger::try_init();
    let client = make_client().await;
    const BLK: usize = 4096;

    let mut runtime = hyperfile::config::HyperFileRuntimeConfig::default();
    runtime.data_cache_dirty_max_bytes_threshold = usize::MAX / 2;
    runtime.data_cache_dirty_max_blocks_threshold = usize::MAX / 2;
    runtime.data_cache_dirty_max_flush_interval = u64::MAX / 2;

    let config_for = |uri: &str, mode: WalRecoveryMode| {
        let staging_config = StagingConfig::new_s3_uri(uri, None);
        let wal_config = HyperFileWalConfig::new(&format!("{}/wal", uri))
            .with_recovery_mode(mode);
        HyperFileConfigBuilder::new()
            .with_staging_config(&staging_config)
            .with_wal_config(&wal_config)
            .with_runtime_config(&runtime)
            .build()
    };

    // A container each, because opening recovers: whichever mode went first
    // would publish and leave the other with nothing to decide about.
    for mode in [WalRecoveryMode::Latest, WalRecoveryMode::Barrier] {
        let tf = TestFile::new(&client).await;

        // Sealed work, then unsealed work on top of it, then die.
        {
            let reactor = make_reactor();
            let hyper = Hyper::create(
                client.clone(), config_for(tf.uri(), mode),
                HyperFileFlags::from_flags(FileFlags::rdwr()),
                HyperFileMode::from_mode(FileMode::default_file()),
            ).await.expect("create");
            let mut fh = HyperFileHandler::fh_from_hyper(&reactor, hyper).await.expect("spawn");

            let sealed = AlignedDataBlockWrapper::new(0, BLK, false);
            sealed.as_mut_slice().fill(0xE1);
            let _ = fh.fh_write_aligned_batch(vec![sealed]).await.expect("sealed write");
            let _ = fh.fh_flush().await.expect("flush seals it");

            let unsealed = AlignedDataBlockWrapper::new(1, BLK, false);
            unsealed.as_mut_slice().fill(0xE2);
            let _ = fh.fh_write_aligned_batch(vec![unsealed]).await.expect("unsealed write");
            drop(fh);
            drop(reactor);
        }

        let mut hyper = Hyper::open(
            client.clone(), config_for(tf.uri(), mode),
            HyperFileFlags::from_flags(FileFlags::rdonly()),
        ).await.unwrap_or_else(|e| panic!("open {:?}: {}", mode, e));

        // Sealed work survives either way.
        let mut buf0 = vec![0u8; BLK];
        let n = hyper.fs_read(0, &mut buf0).await.expect("read the sealed block");
        assert_eq!(n, BLK, "{:?}: sealed block missing", mode);
        assert!(buf0.iter().all(|b| *b == 0xE1),
            "{:?}: sealed block came back {:#x}", mode, buf0[0]);

        let st = hyper.fs_getattr().expect("getattr");
        match mode {
            WalRecoveryMode::Latest => {
                assert!(hyper.writes_durable_on_ack(),
                    "Latest keeps acknowledged writes, so the guarantee holds");
                assert_eq!(st.st_size as usize, 2 * BLK,
                    "Latest must keep the unsealed write");
                let mut buf1 = vec![0u8; BLK];
                let _ = hyper.fs_read(BLK, &mut buf1).await.expect("read the unsealed block");
                assert!(buf1.iter().all(|b| *b == 0xE2),
                    "Latest must keep the write that had no flush behind it");
            },
            WalRecoveryMode::Barrier => {
                assert!(!hyper.writes_durable_on_ack(),
                    "Barrier discards unsealed work, so the guarantee must not be claimed");
                assert_eq!(st.st_size as usize, BLK,
                    "Barrier must stop at the seal, leaving the unsealed write out");
            },
        }

        let _ = hyper.fs_release().await;
        cleanup_with_wal(&client, tf.uri()).await;
    }
}

/// Opening says what recovery did, which is what lets a caller skip work that
/// costs the whole container.
///
/// The field that decides it is `landed_on_barrier`: true means the contents are
/// a state whoever wrote them declared consistent, so a repair pass or a full
/// verification can be skipped. Three cases, and they must be distinguishable —
/// a report that said the same thing every time would be worse than none, since
/// it would look like information.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn reactor_wal_open_reports_what_recovery_did() {
    let _ = env_logger::try_init();
    let client = make_client().await;
    const BLK: usize = 4096;

    let mut runtime = hyperfile::config::HyperFileRuntimeConfig::default();
    runtime.data_cache_dirty_max_bytes_threshold = usize::MAX / 2;
    runtime.data_cache_dirty_max_blocks_threshold = usize::MAX / 2;
    runtime.data_cache_dirty_max_flush_interval = u64::MAX / 2;
    let config_for = |uri: &str, mode: WalRecoveryMode| {
        let staging_config = StagingConfig::new_s3_uri(uri, None);
        let wal_config = HyperFileWalConfig::new(&format!("{}/wal", uri))
            .with_recovery_mode(mode);
        HyperFileConfigBuilder::new()
            .with_staging_config(&staging_config)
            .with_wal_config(&wal_config)
            .with_runtime_config(&runtime)
            .build()
    };

    // Case 1: clean stop. Nothing to replay, and the container sits at a
    // published checkpoint, which is a seal.
    {
        let tf = TestFile::new(&client).await;
        {
            let reactor = make_reactor();
            let hyper = Hyper::create(
                client.clone(), config_for(tf.uri(), WalRecoveryMode::Latest),
                HyperFileFlags::from_flags(FileFlags::rdwr()),
                HyperFileMode::from_mode(FileMode::default_file()),
            ).await.expect("create");
            let mut fh = HyperFileHandler::fh_from_hyper(&reactor, hyper).await.expect("spawn");
            let b = AlignedDataBlockWrapper::new(0, BLK, false);
            b.as_mut_slice().fill(0xC1);
            let _ = fh.fh_write_aligned_batch(vec![b]).await.expect("write");
            let _ = fh.fh_flush().await.expect("flush");
            let _ = fh.fh_release().await;
        }
        let mut hyper = Hyper::open(
            client.clone(), config_for(tf.uri(), WalRecoveryMode::Latest),
            HyperFileFlags::from_flags(FileFlags::rdonly()),
        ).await.expect("open");
        let r = hyper.wal_recovery_report();
        assert!(!r.replayed, "a clean stop leaves nothing to replay: {:?}", r);
        assert!(r.landed_on_barrier, "a published checkpoint is a seal: {:?}", r);
        assert_eq!(r.records_dropped, 0, "{:?}", r);
        let _ = hyper.fs_release().await;
        cleanup_with_wal(&client, tf.uri()).await;
    }

    // Case 2: Latest over unsealed work. It replays, and says the landing point
    // is not a declared-consistent one — which is the caller's cue to repair.
    // Case 3: the same container shape under Barrier, which stops at the seal
    // and counts what it set aside.
    for mode in [WalRecoveryMode::Latest, WalRecoveryMode::Barrier] {
        let tf = TestFile::new(&client).await;
        {
            let reactor = make_reactor();
            let hyper = Hyper::create(
                client.clone(), config_for(tf.uri(), mode),
                HyperFileFlags::from_flags(FileFlags::rdwr()),
                HyperFileMode::from_mode(FileMode::default_file()),
            ).await.expect("create");
            let mut fh = HyperFileHandler::fh_from_hyper(&reactor, hyper).await.expect("spawn");
            let sealed = AlignedDataBlockWrapper::new(0, BLK, false);
            sealed.as_mut_slice().fill(0xD1);
            let _ = fh.fh_write_aligned_batch(vec![sealed]).await.expect("write");
            let _ = fh.fh_flush().await.expect("flush");
            let unsealed = AlignedDataBlockWrapper::new(1, BLK, false);
            unsealed.as_mut_slice().fill(0xD2);
            let _ = fh.fh_write_aligned_batch(vec![unsealed]).await.expect("write");
            drop(fh);
            drop(reactor);
        }
        let mut hyper = Hyper::open(
            client.clone(), config_for(tf.uri(), mode),
            HyperFileFlags::from_flags(FileFlags::rdonly()),
        ).await.expect("open");
        let r = hyper.wal_recovery_report();
        match mode {
            WalRecoveryMode::Latest => {
                assert!(r.replayed, "Latest had unsealed work to apply: {:?}", r);
                assert!(!r.landed_on_barrier,
                    "the last group applied was unsealed, so this is not a declared \
                     state: {:?}", r);
                assert_eq!(r.records_dropped, 0, "Latest drops nothing: {:?}", r);
            },
            WalRecoveryMode::Barrier => {
                assert!(r.landed_on_barrier, "Barrier stops at a seal: {:?}", r);
                assert!(r.records_dropped > 0,
                    "Barrier set the unsealed work aside and must say so: {:?}", r);
            },
        }
        let _ = hyper.fs_release().await;
        cleanup_with_wal(&client, tf.uri()).await;
    }
}

/// With a log, sitting idle does not publish. Without one, it does.
///
/// A timer publish is pointless once a log makes the write durable on return,
/// and it is worse than pointless for `WalRecoveryMode::Barrier`: the newest
/// published checkpoint is what recovery has to reach, so if a timer can put one
/// in the middle of the caller's own unit of work, then "reach the newest
/// checkpoint" and "stop at a declared state" cannot both be satisfied.
///
/// The control half matters as much as the first: without a log the timer is the
/// only thing bounding how long an acknowledged write sits in memory, so this
/// also checks it was not switched off for everyone.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn reactor_wal_time_alone_does_not_publish() {
    let _ = env_logger::try_init();
    let client = make_client().await;
    const BLK: usize = 4096;

    // A short timer, and dirty thresholds far out of reach so that only time
    // could trigger a publish.
    let mut runtime = hyperfile::config::HyperFileRuntimeConfig::default();
    runtime.data_cache_dirty_max_bytes_threshold = usize::MAX / 2;
    runtime.data_cache_dirty_max_blocks_threshold = usize::MAX / 2;
    runtime.data_cache_dirty_max_flush_interval = 200;

    for with_log in [true, false] {
        let tf = TestFile::new(&client).await;
        let staging_config = StagingConfig::new_s3_uri(tf.uri(), None);
        let mut builder = HyperFileConfigBuilder::new()
            .with_staging_config(&staging_config)
            .with_runtime_config(&runtime);
        let wal_config = HyperFileWalConfig::new(&format!("{}/wal", tf.uri()));
        if with_log {
            builder = builder.with_wal_config(&wal_config);
        }
        let config = builder.build();

        let reactor = make_reactor();
        let hyper = Hyper::create(
            client.clone(), config,
            HyperFileFlags::from_flags(FileFlags::rdwr()),
            HyperFileMode::from_mode(FileMode::default_file()),
        ).await.expect("create");
        let mut fh = HyperFileHandler::fh_from_hyper(&reactor, hyper).await.expect("spawn");

        let b = AlignedDataBlockWrapper::new(0, BLK, false);
        b.as_mut_slice().fill(0xF1);
        let _ = fh.fh_write_aligned_batch(vec![b]).await.expect("write");
        let before = fh.fh_last_cno().await.expect("last cno");

        // Well past the timer, with a write arriving after it to force the
        // check — `need_flush` is consulted on the write path, not by a clock.
        tokio::time::sleep(std::time::Duration::from_millis(600)).await;
        let b2 = AlignedDataBlockWrapper::new(1, BLK, false);
        b2.as_mut_slice().fill(0xF2);
        let _ = fh.fh_write_aligned_batch(vec![b2]).await.expect("second write");
        let after = fh.fh_last_cno().await.expect("last cno");

        if with_log {
            assert_eq!(after, before,
                "with a log, time alone must not publish: {} -> {}", before, after);
        } else {
            assert!(after > before,
                "without a log the timer is the only bound on how long a write \
                 sits in memory, and it must still fire: {} -> {}", before, after);
        }

        let _ = fh.fh_release().await;
        if with_log {
            cleanup_with_wal(&client, tf.uri()).await;
        } else {
            tf.cleanup(&client).await;
        }
    }
}

/// An transaction publishes nothing until it closes, and an interval left
/// open is not applied.
///
/// The case this exists for is work whose midpoints are not states anyone should
/// come up in — a repair pass over the container's own contents. Such a pass can
/// write past the dirty-data thresholds, and a threshold crossing partway through
/// would make a half-fixed container the newest checkpoint, which the next open
/// would take as its baseline.
///
/// Both halves matter. That nothing publishes is what protects the container
/// while the work runs; that an unfinished transaction is not applied is what makes
/// an interrupted pass cost redoing it rather than repairing the result of half
/// of it. The second is honoured in `Latest` too, since the caller declared it
/// rather than it being an accident of where the crash fell.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn direct_wal_atomic_interval_publishes_nothing_until_it_closes() {
    let _ = env_logger::try_init();
    let client = make_client().await;
    let tf = TestFile::new(&client).await;
    const BLK: usize = 4096;

    // Thresholds low enough that the interval's writes would cross them.
    let mut runtime = hyperfile::config::HyperFileRuntimeConfig::default();
    runtime.data_cache_dirty_max_bytes_threshold = 64 * BLK;
    runtime.data_cache_dirty_max_blocks_threshold = 64;
    runtime.data_cache_dirty_max_flush_interval = 200;
    let staging_config = StagingConfig::new_s3_uri(tf.uri(), None);
    let wal_config = HyperFileWalConfig::new(&format!("{}/wal", tf.uri()));
    let config = HyperFileConfigBuilder::new()
        .with_staging_config(&staging_config)
        .with_wal_config(&wal_config)
        .with_runtime_config(&runtime)
        .build();

    let mut h = Hyper::create(
        client.clone(), config.clone(),
        HyperFileFlags::from_flags(FileFlags::rdwr()),
        HyperFileMode::from_mode(FileMode::default_file()),
    ).await.expect("create");

    // Ordinary work first, published, so there is a baseline to come back to.
    let _ = h.fs_write(0, &vec![0xB0u8; BLK]).await.expect("baseline write");
    let baseline_cno = h.fs_flush().await.expect("baseline flush");

    // Inside the interval nothing may publish, and asking to is refused.
    h.fs_begin_txn().await.expect("begin");
    assert!(h.fs_in_txn());
    assert!(h.fs_begin_txn().await.is_err(), "a second begin must be refused");

    for i in 1..40usize {
        let _ = h.fs_write(i * BLK, &vec![0xA5u8; BLK]).await
            .unwrap_or_else(|e| panic!("interval write {}: {}", i, e));
    }
    assert_eq!(h.fs_last_cno(), baseline_cno,
        "nothing may publish while the interval is open");

    let before = h.fs_getattr().expect("getattr").st_size;
    let err = h.fs_flush().await.expect_err("an explicit flush inside must be refused");
    assert_eq!(err.kind(), std::io::ErrorKind::ResourceBusy, "got {:?}: {}", err.kind(), err);
    // A refusal must not discard anything. `fs_flush` rolls back when a flush
    // fails, and a refusal reaching that path threw away every dirty block —
    // acknowledged writes lost to a request that was merely not allowed.
    assert_eq!(h.fs_getattr().expect("getattr").st_size, before,
        "the refused flush discarded dirty data");

    // Closing publishes it as one checkpoint.
    let closed_cno = h.fs_commit_txn().await.expect("end");
    assert!(closed_cno > baseline_cno, "closing must publish: {} -> {}", baseline_cno, closed_cno);
    assert!(!h.fs_in_txn());
    assert!(h.fs_commit_txn().await.is_err(), "closing twice must be refused");
    let _ = h.fs_release().await.expect("release");

    // And the whole unit is there afterwards.
    {
        let mut h = Hyper::open(
            client.clone(), config.clone(),
            HyperFileFlags::from_flags(FileFlags::rdonly()),
        ).await.expect("reopen");
        let mut buf = vec![0u8; BLK];
        let _ = h.fs_read(39 * BLK, &mut buf).await.expect("read the last block of the unit");
        assert!(buf.iter().all(|b| *b == 0xA5), "the closed unit must be complete");
        let _ = h.fs_release().await;
    }

    cleanup_with_wal(&client, tf.uri()).await;
}

/// An interval left open is not applied, and what came before it is.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn direct_wal_unfinished_atomic_interval_is_not_applied() {
    let _ = env_logger::try_init();
    let client = make_client().await;
    const BLK: usize = 4096;

    let mut runtime = hyperfile::config::HyperFileRuntimeConfig::default();
    runtime.data_cache_dirty_max_bytes_threshold = usize::MAX / 2;
    runtime.data_cache_dirty_max_blocks_threshold = usize::MAX / 2;
    runtime.data_cache_dirty_max_flush_interval = u64::MAX / 2;

    // Both modes: the interval is the caller's declaration, so `Latest` must
    // honour it too even though it otherwise applies whatever it finds.
    for mode in [WalRecoveryMode::Latest, WalRecoveryMode::Barrier] {
        let tf = TestFile::new(&client).await;
        let staging_config = StagingConfig::new_s3_uri(tf.uri(), None);
        let wal_config = HyperFileWalConfig::new(&format!("{}/wal", tf.uri()))
            .with_recovery_mode(mode);
        let config = HyperFileConfigBuilder::new()
            .with_staging_config(&staging_config)
            .with_wal_config(&wal_config)
            .with_runtime_config(&runtime)
            .build();

        {
            let mut h = Hyper::create(
                client.clone(), config.clone(),
                HyperFileFlags::from_flags(FileFlags::rdwr()),
                HyperFileMode::from_mode(FileMode::default_file()),
            ).await.expect("create");
            // Published baseline.
            let _ = h.fs_write(0, &vec![0xB0u8; BLK]).await.expect("baseline");
            let _ = h.fs_flush().await.expect("flush");
            // An ordinary unflushed write, which is not part of the unit.
            let _ = h.fs_write(BLK, &vec![0xB1u8; BLK]).await.expect("ordinary write");
            // Then a unit, left open.
            h.fs_begin_txn().await.expect("begin");
            let _ = h.fs_write(2 * BLK, &vec![0xA5u8; BLK]).await.expect("unit write");
            let _ = h.fs_write(3 * BLK, &vec![0xA6u8; BLK]).await.expect("unit write");
            // Die without closing it.
            std::mem::forget(h);
        }

        let mut h = Hyper::open(
            client.clone(), config.clone(),
            HyperFileFlags::from_flags(FileFlags::rdonly()),
        ).await.unwrap_or_else(|e| panic!("{:?} reopen: {}", mode, e));

        let mut buf = vec![0u8; BLK];
        let _ = h.fs_read(0, &mut buf).await.expect("read baseline");
        assert!(buf.iter().all(|b| *b == 0xB0), "{:?}: baseline lost", mode);

        let st = h.fs_getattr().expect("getattr");
        assert_eq!(st.st_size as usize, 2 * BLK,
            "{:?}: the unit must be left out and the ordinary write kept — size says {}",
            mode, st.st_size);

        let mut buf1 = vec![0u8; BLK];
        let _ = h.fs_read(BLK, &mut buf1).await.expect("read the ordinary write");
        assert!(buf1.iter().all(|b| *b == 0xB1),
            "{:?}: the write before the interval is ordinary and must survive", mode);

        let r = h.wal_recovery_report();
        assert!(r.records_dropped > 0,
            "{:?}: the unit's records were set aside and must be counted: {:?}", mode, r);

        let _ = h.fs_release().await;
        cleanup_with_wal(&client, tf.uri()).await;
    }
}

/// Aborting undoes the transaction on this handle, which is what separates it
/// from never committing.
///
/// Never committing leaves the writes in memory until the file is closed, and
/// only a reopen discards them — so a caller that gave up on a repair pass and
/// carried on reading would still see its half-finished work. Aborting rolls the
/// file back to what is published, so it does not.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn direct_wal_aborting_a_transaction_undoes_it_here_and_now() {
    let _ = env_logger::try_init();
    let client = make_client().await;
    let tf = TestFile::new(&client).await;
    const BLK: usize = 4096;

    let mut runtime = hyperfile::config::HyperFileRuntimeConfig::default();
    runtime.data_cache_dirty_max_bytes_threshold = usize::MAX / 2;
    runtime.data_cache_dirty_max_blocks_threshold = usize::MAX / 2;
    runtime.data_cache_dirty_max_flush_interval = u64::MAX / 2;
    let staging_config = StagingConfig::new_s3_uri(tf.uri(), None);
    let wal_config = HyperFileWalConfig::new(&format!("{}/wal", tf.uri()));
    let config = HyperFileConfigBuilder::new()
        .with_staging_config(&staging_config)
        .with_wal_config(&wal_config)
        .with_runtime_config(&runtime)
        .build();

    let mut h = Hyper::create(
        client.clone(), config.clone(),
        HyperFileFlags::from_flags(FileFlags::rdwr()),
        HyperFileMode::from_mode(FileMode::default_file()),
    ).await.expect("create");

    let _ = h.fs_write(0, &vec![0xB0u8; BLK]).await.expect("baseline");
    let published = h.fs_flush().await.expect("flush");

    assert!(h.fs_abort_txn().await.is_err(), "aborting with none open must be refused");

    h.fs_begin_txn().await.expect("begin");
    let _ = h.fs_write(BLK, &vec![0xA5u8; BLK]).await.expect("write inside");
    assert_eq!(h.fs_getattr().expect("getattr").st_size as usize, 2 * BLK,
        "the write is visible while the transaction is open — there is no isolation");

    h.fs_abort_txn().await.expect("abort");
    assert!(!h.fs_in_txn());
    assert_eq!(h.fs_getattr().expect("getattr").st_size as usize, BLK,
        "aborting must undo the transaction on this handle, not just on a later open");
    assert_eq!(h.fs_last_cno(), published, "aborting must not publish");

    // And the file still works afterwards.
    let _ = h.fs_write(BLK, &vec![0xC7u8; BLK]).await.expect("write after abort");
    let _ = h.fs_flush().await.expect("flush after abort");
    let mut buf = vec![0u8; BLK];
    let _ = h.fs_read(BLK, &mut buf).await.expect("read");
    assert!(buf.iter().all(|b| *b == 0xC7));
    let _ = h.fs_release().await.expect("release");

    cleanup_with_wal(&client, tf.uri()).await;
}

/// Barrier recovery that has nothing to apply must not leave the container
/// read-only.
///
/// "Stopped at an unsealed group, so nothing was applied" is `Barrier`'s most
/// ordinary outcome, and it produced cno 0. The retry loop read 0 as failure,
/// spent its attempts, and then set the fail-stop flag — after which every write
/// fails, on a container that opened successfully. `do_open` discards the error,
/// so nothing says why.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn reactor_wal_barrier_recovery_with_nothing_to_apply_stays_writable() {
    let _ = env_logger::try_init();
    let client = make_client().await;
    let tf = TestFile::new(&client).await;
    const BLK: usize = 4096;

    let mut runtime = hyperfile::config::HyperFileRuntimeConfig::default();
    runtime.data_cache_dirty_max_bytes_threshold = usize::MAX / 2;
    runtime.data_cache_dirty_max_blocks_threshold = usize::MAX / 2;
    runtime.data_cache_dirty_max_flush_interval = u64::MAX / 2;
    let staging_config = StagingConfig::new_s3_uri(tf.uri(), None);
    let wal_config = HyperFileWalConfig::new(&format!("{}/wal", tf.uri()))
        .with_recovery_mode(WalRecoveryMode::Barrier);
    let config = HyperFileConfigBuilder::new()
        .with_staging_config(&staging_config)
        .with_wal_config(&wal_config)
        .with_runtime_config(&runtime)
        .build();

    // Records with no barrier behind them: the whole log is one unsealed group,
    // so Barrier recovery applies nothing at all.
    {
        let reactor = make_reactor();
        let hyper = Hyper::create(
            client.clone(), config.clone(),
            HyperFileFlags::from_flags(FileFlags::rdwr()),
            HyperFileMode::from_mode(FileMode::default_file()),
        ).await.expect("create");
        let mut fh = HyperFileHandler::fh_from_hyper(&reactor, hyper).await.expect("spawn");
        let b = AlignedDataBlockWrapper::new(0, BLK, false);
        b.as_mut_slice().fill(0x71);
        let _ = fh.fh_write_aligned_batch(vec![b]).await.expect("write");
        drop(fh);
        drop(reactor);
    }

    let reactor = make_reactor();
    let hyper = Hyper::open(
        client.clone(), config.clone(),
        HyperFileFlags::from_flags(FileFlags::rdwr()),
    ).await.expect("reopen");

    // Nothing was applied, so nothing should have been retried either: the
    // report is where that outcome belongs, and it must not read as a failure.
    // Asked before handing the file to the reactor, which is where a caller in
    // this mode would ask it too.
    let r = hyper.wal_recovery_report();
    assert!(!r.replayed, "Barrier applied nothing: {:?}", r);
    assert!(r.landed_on_barrier, "stopping at a seal is landing on one: {:?}", r);
    assert!(r.records_dropped > 0, "and what it set aside is counted: {:?}", r);

    let mut fh = HyperFileHandler::fh_from_hyper(&reactor, hyper).await.expect("spawn");

    // The container must still take writes. Nothing failed — there was simply
    // nothing this mode was willing to apply.
    let b = AlignedDataBlockWrapper::new(0, BLK, false);
    b.as_mut_slice().fill(0x72);
    let n = fh.fh_write_aligned_batch(vec![b]).await
        .expect("a container that recovered to a seal must still be writable");
    assert_eq!(n, BLK);
    let _ = fh.fh_flush().await.expect("and flushable");

    let mut buf = vec![0u8; BLK];
    let _ = fh.fh_read(0, &mut buf).await.expect("read");
    assert!(buf.iter().all(|b| *b == 0x72));

    let _ = fh.fh_release().await;
    cleanup_with_wal(&client, tf.uri()).await;
}

/// In `Barrier` mode nothing publishes unless the caller asks, and a write that
/// would need a publish to make room is refused instead.
///
/// The mode sells one thing: the state recovery lands on is one the caller
/// declared consistent. Recovery cannot land earlier than the newest published
/// checkpoint, so anything else that publishes puts a checkpoint nobody declared
/// beneath the floor and the guarantee is gone. Whether that publish also writes
/// a barrier is beside the point — publishing at all is what does it.
///
/// The control is `Latest`, where the threshold must still publish: the memory
/// bound is the reason it exists, and this is a change of who may publish, not
/// the removal of a bound.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn reactor_wal_barrier_mode_publishes_only_when_asked() {
    let _ = env_logger::try_init();
    let client = make_client().await;
    const BLK: usize = 4096;

    // A threshold a handful of blocks crosses.
    let mut runtime = hyperfile::config::HyperFileRuntimeConfig::default();
    runtime.data_cache_dirty_max_bytes_threshold = 8 * BLK;
    runtime.data_cache_dirty_max_blocks_threshold = 8;
    runtime.data_cache_dirty_max_flush_interval = u64::MAX / 2;
    runtime.segment_buffer_size = 8 * BLK;

    for mode in [WalRecoveryMode::Barrier, WalRecoveryMode::Latest] {
        let tf = TestFile::new(&client).await;
        let staging_config = StagingConfig::new_s3_uri(tf.uri(), None);
        let wal_config = HyperFileWalConfig::new(&format!("{}/wal", tf.uri()))
            .with_recovery_mode(mode);
        let config = HyperFileConfigBuilder::new()
            .with_staging_config(&staging_config)
            .with_wal_config(&wal_config)
            .with_runtime_config(&runtime)
            .build();

        let reactor = make_reactor();
        let hyper = Hyper::create(
            client.clone(), config,
            HyperFileFlags::from_flags(FileFlags::rdwr()),
            HyperFileMode::from_mode(FileMode::default_file()),
        ).await.expect("create");
        let mut fh = HyperFileHandler::fh_from_hyper(&reactor, hyper).await.expect("spawn");

        let before = fh.fh_last_cno().await.expect("last cno");
        let mut refused = None;
        for i in 0..40usize {
            let b = AlignedDataBlockWrapper::new(i as u64, BLK, false);
            b.as_mut_slice().fill(0x60 + (i % 16) as u8);
            match fh.fh_write_aligned_batch(vec![b]).await {
                Ok(_) => {},
                Err(e) => { refused = Some(e); break; },
            }
        }
        let after = fh.fh_last_cno().await.expect("last cno");

        match mode {
            WalRecoveryMode::Barrier => {
                let e = refused.expect(
                    "Barrier must refuse a write it cannot make room for rather than \
                     publishing a checkpoint the caller never declared");
                assert_eq!(e.kind(), std::io::ErrorKind::OutOfMemory, "got {:?}: {}", e.kind(), e);
                assert_eq!(after, before,
                    "and nothing may have published on its own: {} -> {}", before, after);
                // Asking explicitly still works, and makes room.
                let _ = fh.fh_flush().await.expect("an explicit flush is the way out");
                assert!(fh.fh_last_cno().await.expect("cno") > before,
                    "the caller's own flush must publish");
            },
            WalRecoveryMode::Latest => {
                assert!(refused.is_none(),
                    "Latest keeps its memory bound by publishing: {:?}", refused);
                assert!(after > before,
                    "so the threshold must still have published: {} -> {}", before, after);
            },
        }

        let _ = fh.fh_release().await;
        cleanup_with_wal(&client, tf.uri()).await;
    }
}
