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
use hyperfile::wal::config::HyperFileWalConfig;

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
