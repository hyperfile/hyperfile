//! Reactor-mode smoke integration tests.
//!
//! Exercises the two reactor APIs built on top of `Hyper`:
//!
//!  - `HyperFileHandler` (fh.rs) — direct Handler-style API where the
//!    caller explicitly owns a `LocalSpawner`.
//!  - `HyperFileTokio` (tokio_wrapper.rs) — higher-level wrapper that
//!    spawns its own spawner and exposes tokio AsyncRead / AsyncWrite /
//!    AsyncSeek. Each method goes through a request/response round
//!    trip with the internal handler loop.
//!
//! Both APIs should behave functionally equivalent to the direct
//! `Hyper::fs_*` API; these tests are here to catch regressions in the
//! request serialization, channel plumbing, and spawner lifecycle.
//!
//! Runs under default features (reactor + meta_loader_batch) only —
//! additional feature combinations are covered by separate binaries.
//!
//! ```bash
//! cargo test --test integration_reactor_s3_smoke -- --ignored --test-threads=1
//! ```

#![cfg(feature = "reactor")]

#[allow(dead_code)]
mod common;
#[allow(dead_code)]
mod common_reactor;

use std::io::SeekFrom;
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};
use common::*;
use common_reactor::*;
use hyperfile::file::hyper::Hyper;
use hyperfile::file::flags::HyperFileFlags;
use hyperfile::file::mode::HyperFileMode;
use hyperfile::config::HyperFileConfigBuilder;
use hyperfile::staging::config::StagingConfig;

use hyperfile::file::fh::HyperFileHandler;
use hyperfile::file::tokio_wrapper::HyperFileTokio;
use hyperfile::file::flags::FileFlags;
use hyperfile::file::mode::FileMode;

// ---------------------------------------------------------------------
// HyperFileHandler (fh_*) tests — 1 spawner per test, explicit control
// ---------------------------------------------------------------------

/// Handler: create → write → release → reopen → read round-trip.
#[tokio::test]
#[ignore]
async fn reactor_handler_write_read_round_trip() {
    let _ = env_logger::try_init();
    let client = make_client().await;
    let tf = TestFile::new(&client).await;

    let reactor = make_reactor();
    let payload: Vec<u8> = (0u8..=127u8).collect();

    // Create + write + release.
    {
        let mut fh = HyperFileHandler::fh_open_or_create_with_default_opt(
            &reactor, &client, tf.uri(), FileFlags::rdwr(), FileMode::default_file(),
        )
        .await
        .expect("fh create");
        let n = fh.fh_write(0, &payload).await.expect("fh_write");
        assert_eq!(n, payload.len());
        let _ = fh.fh_release().await.expect("fh_release");
    }

    // Reopen read-only, verify.
    {
        let mut fh = HyperFileHandler::fh_open(
            &reactor, &client, tf.uri(), FileFlags::rdonly(),
        )
        .await
        .expect("fh open");
        let stat = fh.fh_getattr().await.expect("fh_getattr");
        assert_eq!(stat.st_size as usize, payload.len());

        let mut buf = vec![0u8; payload.len()];
        let n = fh.fh_read(0, &mut buf).await.expect("fh_read");
        assert_eq!(n, payload.len());
        assert_eq!(buf, payload);
        let _ = fh.fh_release().await;
    }

    tf.cleanup(&client).await;
}

/// Handler: truncate to extend preserves data, fills zeros.
#[tokio::test]
#[ignore]
async fn reactor_handler_truncate_extend() {
    let _ = env_logger::try_init();
    let client = make_client().await;
    let tf = TestFile::new(&client).await;

    let reactor = make_reactor();
    let payload: Vec<u8> = (0..1024u16).map(|v| (v & 0xFF) as u8).collect();

    {
        let mut fh = HyperFileHandler::fh_open_or_create_with_default_opt(
            &reactor, &client, tf.uri(), FileFlags::rdwr(), FileMode::default_file(),
        )
        .await
        .expect("create");
        let _ = fh.fh_write(0, &payload).await.expect("write");
        let _ = fh.fh_release().await;
    }

    let new_size = 8 * 1024;
    {
        let mut fh = HyperFileHandler::fh_open(
            &reactor, &client, tf.uri(), FileFlags::rdwr(),
        )
        .await
        .expect("open");
        fh.fh_truncate(new_size).await.expect("truncate extend");
        let _ = fh.fh_release().await;
    }

    {
        let mut fh = HyperFileHandler::fh_open(
            &reactor, &client, tf.uri(), FileFlags::rdonly(),
        )
        .await
        .expect("open");
        let stat = fh.fh_getattr().await.expect("getattr");
        assert_eq!(stat.st_size as usize, new_size);

        let mut buf = vec![0u8; new_size];
        let _ = fh.fh_read(0, &mut buf).await.expect("read");
        assert_eq!(&buf[..payload.len()], &payload[..]);
        assert!(buf[payload.len()..].iter().all(|&b| b == 0));
        let _ = fh.fh_release().await;
    }

    tf.cleanup(&client).await;
}

/// Handler: truncate to shrink preserves the prefix, drops the tail.
#[tokio::test]
#[ignore]
async fn reactor_handler_truncate_shrink() {
    let _ = env_logger::try_init();
    let client = make_client().await;
    let tf = TestFile::new(&client).await;

    let reactor = make_reactor();
    let payload: Vec<u8> = (0..16 * 1024).map(|i| (i & 0xFF) as u8).collect();

    {
        let mut fh = HyperFileHandler::fh_open_or_create_with_default_opt(
            &reactor, &client, tf.uri(), FileFlags::rdwr(), FileMode::default_file(),
        )
        .await
        .expect("create");
        let _ = fh.fh_write(0, &payload).await.expect("write");
        let _ = fh.fh_release().await;
    }

    let new_size = 5000;
    {
        let mut fh = HyperFileHandler::fh_open(
            &reactor, &client, tf.uri(), FileFlags::rdwr(),
        )
        .await
        .expect("open");
        fh.fh_truncate(new_size).await.expect("truncate shrink");
        let _ = fh.fh_release().await;
    }

    {
        let mut fh = HyperFileHandler::fh_open(
            &reactor, &client, tf.uri(), FileFlags::rdonly(),
        )
        .await
        .expect("open");
        let stat = fh.fh_getattr().await.expect("getattr");
        assert_eq!(stat.st_size as usize, new_size);

        let mut buf = vec![0u8; new_size];
        let _ = fh.fh_read(0, &mut buf).await.expect("read");
        assert_eq!(&buf[..], &payload[..new_size]);
        let _ = fh.fh_release().await;
    }

    tf.cleanup(&client).await;
}

/// Handler: explicit fh_flush after write returns a monotonic segid.
#[tokio::test]
#[ignore]
async fn reactor_handler_flush_returns_cno() {
    let _ = env_logger::try_init();
    let client = make_client().await;
    let tf = TestFile::new(&client).await;

    let reactor = make_reactor();
    let mut fh = HyperFileHandler::fh_open_or_create_with_default_opt(
        &reactor, &client, tf.uri(), FileFlags::rdwr(), FileMode::default_file(),
    )
    .await
    .expect("create");

    let _ = fh.fh_write(0, &[0xAAu8; 4096]).await.expect("write");
    let cno1 = fh.fh_flush().await.expect("first flush");

    let _ = fh.fh_write(0, &[0xBBu8; 4096]).await.expect("write 2");
    let cno2 = fh.fh_flush().await.expect("second flush");

    // Each flush that persists dirty state must advance the checkpoint.
    assert!(cno2 > cno1, "cno did not advance: {} -> {}", cno1, cno2);

    let _ = fh.fh_release().await;
    tf.cleanup(&client).await;
}

/// Handler: getattr and setattr round-trip.
#[tokio::test]
#[ignore]
async fn reactor_handler_getattr_setattr() {
    let _ = env_logger::try_init();
    let client = make_client().await;
    let tf = TestFile::new(&client).await;

    let reactor = make_reactor();
    let mut fh = HyperFileHandler::fh_open_or_create_with_default_opt(
        &reactor, &client, tf.uri(), FileFlags::rdwr(), FileMode::default_file(),
    )
    .await
    .expect("create");

    let stat1 = fh.fh_getattr().await.expect("getattr");
    assert!(stat1.st_mode & libc::S_IFREG != 0);

    // Mutate uid/gid via setattr.
    let mut stat2 = stat1;
    stat2.st_uid = 7777;
    stat2.st_gid = 8888;
    let out = fh.fh_setattr(stat2).await.expect("setattr");
    assert_eq!(out.st_uid, 7777);
    assert_eq!(out.st_gid, 8888);

    let _ = fh.fh_release().await;
    tf.cleanup(&client).await;
}

// ---------------------------------------------------------------------
// HyperFileTokio tests — AsyncRead / AsyncWrite / AsyncSeek surface
// ---------------------------------------------------------------------

/// Tokio: write → seek → read verifies the tokio AsyncRead/Write/Seek
/// surface wraps the reactor correctly.
#[tokio::test]
#[ignore]
async fn reactor_tokio_read_write_seek_round_trip() {
    let _ = env_logger::try_init();
    let client = make_client().await;
    let tf = TestFile::new(&client).await;

    let payload = b"hello reactor tokio world";
    {
        let mut file = HyperFileTokio::open_or_create_with_default_opt(
            &client, tf.uri(), FileFlags::rdwr(), FileMode::default_file(),
        )
        .await
        .expect("create");

        // AsyncWrite::write
        let n = file.write(payload).await.expect("write");
        assert_eq!(n, payload.len());

        file.flush().await.expect("flush");
        file.shutdown().await.expect("shutdown");
    }

    {
        let mut file = HyperFileTokio::open(
            &client, tf.uri(), FileFlags::rdonly(),
        )
        .await
        .expect("open");

        // Seek past the beginning, then read the tail.
        file.seek(SeekFrom::Start(6)).await.expect("seek");
        let mut buf = vec![0u8; payload.len() - 6];
        file.read_exact(&mut buf).await.expect("read_exact");
        assert_eq!(buf, &payload[6..]);

        file.shutdown().await.expect("shutdown");
    }

    tf.cleanup(&client).await;
}

/// Tokio: flush returns Ok on an empty buffered state (no dirty data).
#[tokio::test]
#[ignore]
async fn reactor_tokio_flush_shutdown_empty() {
    let _ = env_logger::try_init();
    let client = make_client().await;
    let tf = TestFile::new(&client).await;

    {
        let mut file = HyperFileTokio::open_or_create_with_default_opt(
            &client, tf.uri(), FileFlags::rdwr(), FileMode::default_file(),
        )
        .await
        .expect("create");
        file.flush().await.expect("flush on empty file");
        file.shutdown().await.expect("shutdown");
    }

    tf.cleanup(&client).await;
}

/// O_APPEND on the reactor handler API: writes through fh_write
/// land at end-of-file regardless of the offset argument.
#[tokio::test]
#[ignore]
async fn reactor_o_append_single_handle() {
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

    let part_a = b"alpha-".to_vec();
    let part_b = b"beta\n".to_vec();
    {
        let flags = FileFlags::from(libc::O_RDWR | libc::O_APPEND);
        let mut fh = HyperFileHandler::fh_open(&reactor, &client, tf.uri(), flags)
            .await
            .expect("open append");
        // Misleading offsets in both calls.
        let _ = fh.fh_write(999, &part_a).await.expect("write A");
        let _ = fh.fh_write(0, &part_b).await.expect("write B");
        let _ = fh.fh_flush().await.expect("flush");
        let _ = fh.fh_release().await;
    }

    let mut fh = HyperFileHandler::fh_open(&reactor, &client, tf.uri(), FileFlags::rdonly())
        .await
        .expect("reopen");
    let total = part_a.len() + part_b.len();
    let mut buf = vec![0u8; total];
    let n = fh.fh_read(0, &mut buf).await.expect("read");
    assert_eq!(n, total);
    let mut want = part_a.clone();
    want.extend_from_slice(&part_b);
    assert_eq!(buf, want);
    let _ = fh.fh_release().await;

    tf.cleanup(&client).await;
}

/// fh_fdatasync exercises the FlushData reactor path: data must
/// be persisted (matches fh_flush behavior); attr-only changes
/// must be skipped.
#[tokio::test]
#[ignore]
async fn reactor_fh_fdatasync_round_trip() {
    let _ = env_logger::try_init();
    let client = make_client().await;
    let tf = TestFile::new(&client).await;

    let reactor = make_reactor();

    // Create + write + flush. Note baseline cno.
    let payload = vec![0xAAu8; 4096];
    let baseline_cno = {
        let mut fh = HyperFileHandler::fh_open_or_create_with_default_opt(
            &reactor, &client, tf.uri(), FileFlags::rdwr(), FileMode::default_file(),
        ).await.expect("create");
        let _ = fh.fh_write(0, &payload).await.expect("write");
        let cno = fh.fh_flush().await.expect("flush baseline");
        let _ = fh.fh_release().await;
        cno
    };

    // Reopen, read (atime dirty), fh_fdatasync. cno unchanged.
    {
        let mut fh = HyperFileHandler::fh_open(&reactor, &client, tf.uri(), FileFlags::rdonly())
            .await.expect("open ro");
        let mut buf = vec![0u8; payload.len()];
        let _ = fh.fh_read(0, &mut buf).await.expect("read");
        let cno = fh.fh_fdatasync().await.expect("fdatasync attr-only");
        assert_eq!(cno, baseline_cno,
            "fh_fdatasync must not bump cno on attr-only dirt");
        let _ = fh.fh_release().await;
    }

    // Reopen, write, fh_fdatasync. Must persist.
    let new_payload = vec![0xCCu8; 8192];
    let after_cno = {
        let mut fh = HyperFileHandler::fh_open(&reactor, &client, tf.uri(), FileFlags::rdwr())
            .await.expect("open rdwr");
        let _ = fh.fh_write(0, &new_payload).await.expect("write");
        let cno = fh.fh_fdatasync().await.expect("fdatasync data-dirty");
        let _ = fh.fh_release().await;
        cno
    };
    assert!(after_cno > baseline_cno,
        "fh_fdatasync with dirty data must bump cno");

    // Verify content survived.
    let mut fh = HyperFileHandler::fh_open(&reactor, &client, tf.uri(), FileFlags::rdonly())
        .await.expect("reopen");
    let mut buf = vec![0u8; new_payload.len()];
    let _ = fh.fh_read(0, &mut buf).await.expect("read all");
    assert_eq!(buf, new_payload);
    let _ = fh.fh_release().await;
    tf.cleanup(&client).await;
}

/// Handler: write-side operations on a read-only handle must fail
/// with EBADF (POSIX). The reactor has its own write paths
/// (`spawn_write` / `spawn_write_zero`) separate from the direct
/// API, so it needs its own coverage; `fh_truncate` and the batch
/// writes share `HyperFile::*` with the direct API.
#[tokio::test]
#[ignore]
async fn reactor_handler_rdonly_write_ops_are_ebadf() {
    let _ = env_logger::try_init();
    let client = make_client().await;
    let tf = TestFile::new(&client).await;

    let reactor = make_reactor();
    let payload = vec![0xAAu8; 4096];

    // Create with content, then reopen read-only.
    {
        let mut fh = HyperFileHandler::fh_open_or_create_with_default_opt(
            &reactor, &client, tf.uri(), FileFlags::rdwr(), FileMode::default_file(),
        )
        .await
        .expect("fh create");
        let _ = fh.fh_write(0, &payload).await.expect("fh_write");
        let _ = fh.fh_release().await.expect("fh_release");
    }

    let mut fh = HyperFileHandler::fh_open(
        &reactor, &client, tf.uri(), FileFlags::rdonly(),
    )
    .await
    .expect("fh open rdonly");

    let expect_ebadf = |res: std::io::Result<usize>, what: &str| {
        let err = res.err().unwrap_or_else(|| panic!("{what} on a read-only handle must fail"));
        assert_eq!(err.raw_os_error(), Some(libc::EBADF),
            "{what} must fail with EBADF, got {:?} (kind {:?})", err.raw_os_error(), err.kind());
    };

    expect_ebadf(fh.fh_write(0, &[0xBBu8; 8]).await, "fh_write");
    expect_ebadf(fh.fh_write_zero(0, 8).await, "fh_write_zero");
    expect_ebadf(fh.fh_truncate(0).await.map(|_| 0usize), "fh_truncate");

    // The handler is still alive and reads are unaffected.
    let mut buf = vec![0u8; payload.len()];
    let n = fh.fh_read(0, &mut buf).await.expect("fh_read after rejected writes");
    assert_eq!(n, payload.len());
    assert_eq!(buf, payload, "content must be untouched");
    let stat = fh.fh_getattr().await.expect("fh_getattr");
    assert_eq!(stat.st_size as usize, payload.len(),
        "size must be untouched by the rejected truncate");

    let _ = fh.fh_release().await;
    tf.cleanup(&client).await;
}

/// Handler: a read on a write-only handle must fail with EBADF, while
/// writes — including a partial write that needs an internal
/// read-modify-write — keep working. The reactor has its own read
/// path (`spawn_read`), so it needs its own coverage.
#[tokio::test]
#[ignore]
async fn reactor_handler_wronly_read_is_ebadf() {
    let _ = env_logger::try_init();
    let client = make_client().await;
    let tf = TestFile::new(&client).await;

    let reactor = make_reactor();
    const BLOCK: usize = 4096;

    // Seed and flush so the read-modify-write below must fetch from
    // staging rather than this handle's cache.
    {
        let mut fh = HyperFileHandler::fh_open_or_create_with_default_opt(
            &reactor, &client, tf.uri(), FileFlags::rdwr(), FileMode::default_file(),
        )
        .await
        .expect("fh create");
        let _ = fh.fh_write(0, &vec![0xAAu8; BLOCK]).await.expect("seed write");
        let _ = fh.fh_release().await.expect("fh_release");
    }

    {
        let mut fh = HyperFileHandler::fh_open(
            &reactor, &client, tf.uri(), FileFlags::wronly(),
        )
        .await
        .expect("fh open wronly");

        let mut buf = vec![0u8; BLOCK];
        let err = fh.fh_read(0, &mut buf).await
            .expect_err("fh_read on a write-only handle must fail");
        assert_eq!(err.raw_os_error(), Some(libc::EBADF),
            "fh_read must fail with EBADF, got {:?} (kind {:?})",
            err.raw_os_error(), err.kind());

        // Handler is still alive; writes still work, including a
        // partial write needing a read-modify-write.
        let n = fh.fh_write(50, &vec![0xBBu8; 100]).await
            .expect("partial fh_write on wronly handle must work");
        assert_eq!(n, 100);
        let _ = fh.fh_release().await.expect("fh_release");
    }

    // Verify content from a read-only handle.
    {
        let mut fh = HyperFileHandler::fh_open(
            &reactor, &client, tf.uri(), FileFlags::rdonly(),
        )
        .await
        .expect("fh open rdonly");
        let mut buf = vec![0u8; BLOCK];
        let n = fh.fh_read(0, &mut buf).await.expect("fh_read");
        assert_eq!(n, BLOCK);
        assert!(buf[..50].iter().all(|&b| b == 0xAA));
        assert!(buf[50..150].iter().all(|&b| b == 0xBB));
        assert!(buf[150..].iter().all(|&b| b == 0xAA));
        let _ = fh.fh_release().await;
    }

    tf.cleanup(&client).await;
}

/// A staging read that fails must fail the read, not report success over
/// a buffer nothing was written into.
///
/// The reactor plans a cold read as a spawned ranged GET. That task used
/// to discard the result of the load and report the full byte count
/// regardless, so an unreadable segment produced a successful read whose
/// buffer still held whatever the caller's allocation happened to
/// contain — a wrong answer rather than an error.
///
/// Deleting the segment object while keeping the inode is a stand-in for
/// any failure of that GET. The data cache is disabled so the read cannot
/// be answered from memory and has to go to staging.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn reactor_read_reports_a_failed_staging_load() {
    let _ = env_logger::try_init();
    let client = make_client().await;
    let tf = TestFile::new(&client).await;
    let reactor = make_reactor();

    const BLK: usize = 4096;

    let staging_config = StagingConfig::new_s3_uri(tf.uri(), None);
    let mut runtime = hyperfile::config::HyperFileRuntimeConfig::default();
    runtime.data_cache_blocks = 0;
    let config = HyperFileConfigBuilder::new()
        .with_staging_config(&staging_config)
        .with_runtime_config(&runtime)
        .build();

    // Write something and get it onto staging.
    {
        let hyper = Hyper::create(
            client.clone(),
            config.clone(),
            HyperFileFlags::from_flags(FileFlags::rdwr()),
            HyperFileMode::from_mode(FileMode::default_file()),
        ).await.expect("create");
        let mut fh = HyperFileHandler::fh_from_hyper(&reactor, hyper).await.expect("spawn handler");
        fh.fh_write(0, &vec![0x7Eu8; 4 * BLK]).await.expect("write");
        fh.fh_flush().await.expect("flush");
        let _ = fh.fh_release().await.expect("release");
    }

    // Sanity check: it reads back before we break it.
    let bucket = test_bucket();
    let prefix = tf.uri().trim_start_matches("s3://")
        .trim_start_matches(&bucket)
        .trim_start_matches('/')
        .to_string() + "/";
    {
        let hyper = Hyper::open(client.clone(), config.clone(),
            HyperFileFlags::from_flags(FileFlags::rdonly())).await.expect("open");
        let mut fh = HyperFileHandler::fh_from_hyper(&reactor, hyper).await.expect("spawn handler");
        let got = fh.fh_read_owned(0, BLK).await.expect("read before deletion");
        assert!(got.iter().all(|&v| v == 0x7E), "precondition: content should read back");
        let _ = fh.fh_release().await;
    }

    // Delete the segment objects, keep the inode so open still works.
    let listed = client.list_objects_v2().bucket(&bucket).prefix(&prefix).send().await.expect("list");
    let mut deleted = 0;
    for obj in listed.contents() {
        let key = obj.key().unwrap_or_default();
        let name = key.rsplit('/').next().unwrap_or_default();
        if !name.is_empty() && name.len() == 10 && name.chars().all(|c| c.is_ascii_digit()) {
            client.delete_object().bucket(&bucket).key(key).send().await.expect("delete segment");
            deleted += 1;
        }
    }
    assert!(deleted > 0, "no segment object found to delete under {}", prefix);

    // The read must now fail rather than hand back an unfilled buffer.
    {
        let hyper = Hyper::open(client.clone(), config.clone(),
            HyperFileFlags::from_flags(FileFlags::rdonly())).await.expect("open");
        let mut fh = HyperFileHandler::fh_from_hyper(&reactor, hyper).await.expect("spawn handler");
        let res = fh.fh_read_owned(0, BLK).await;
        match res {
            Err(e) => eprintln!("read failed as it should: {}", e),
            Ok(buf) => panic!(
                "read reported success over {} bytes with the segment deleted; \
                 first bytes {:?} — a failed staging load was swallowed",
                buf.len(), &buf[..8.min(buf.len())]),
        }
        let _ = fh.fh_release().await;
    }

    tf.cleanup(&client).await;
}

/// A write whose read-modify-write cannot read the block must fail, not
/// apply itself over a block that was never filled.
///
/// A partial write needs the block's current contents before it can put
/// its own bytes in. That fetch is a spawned staging read, and its result
/// used to be discarded: the task reported the full byte count whatever
/// happened, so a failed read left the block as the freshly allocated,
/// zeroed buffer it started as. The write then applied its bytes to that,
/// marked it dirty and reported success, and the next flush persisted
/// zeroes over everything the block used to hold. Silent, and permanent —
/// worse than the read-side version of the same mistake, which only gave
/// one caller a wrong answer.
///
/// Deleting the segment object stands in for any failure of that read.
/// The write must report it, and the handle must remain usable
/// afterwards: the failing request holds the per-file permit and, under
/// `range-lock`, a range, and neither is released by the path that
/// completes a write.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn reactor_write_reports_a_failed_read_modify_write() {
    let _ = env_logger::try_init();
    let client = make_client().await;
    let tf = TestFile::new(&client).await;
    let reactor = make_reactor();

    const BLK: usize = 4096;
    const PAT: u8 = 0x7E;

    let staging_config = StagingConfig::new_s3_uri(tf.uri(), None);
    let mut runtime = hyperfile::config::HyperFileRuntimeConfig::default();
    runtime.data_cache_blocks = 0;
    let config = HyperFileConfigBuilder::new()
        .with_staging_config(&staging_config)
        .with_runtime_config(&runtime)
        .build();

    {
        let hyper = Hyper::create(client.clone(), config.clone(),
            HyperFileFlags::from_flags(FileFlags::rdwr()),
            HyperFileMode::from_mode(FileMode::default_file())).await.expect("create");
        let mut fh = HyperFileHandler::fh_from_hyper(&reactor, hyper).await.expect("spawn handler");
        fh.fh_write(0, &vec![PAT; 4 * BLK]).await.expect("seed write");
        fh.fh_flush().await.expect("seed flush");
        let _ = fh.fh_release().await.expect("release");
    }

    // Break the staging read the retrieve will need.
    let bucket = test_bucket();
    let prefix = tf.uri().trim_start_matches("s3://")
        .trim_start_matches(&bucket).trim_start_matches('/').to_string() + "/";
    let listed = client.list_objects_v2().bucket(&bucket).prefix(&prefix).send().await.expect("list");
    let mut deleted = 0;
    for obj in listed.contents() {
        let key = obj.key().unwrap_or_default();
        let name = key.rsplit('/').next().unwrap_or_default();
        if name.len() == 10 && name.chars().all(|c| c.is_ascii_digit()) {
            client.delete_object().bucket(&bucket).key(key).send().await.expect("delete segment");
            deleted += 1;
        }
    }
    assert!(deleted > 0, "no segment object found under {}", prefix);

    let hyper = Hyper::open(client.clone(), config.clone(),
        HyperFileFlags::from_flags(FileFlags::rdwr())).await.expect("open");
    let mut fh = HyperFileHandler::fh_from_hyper(&reactor, hyper).await.expect("spawn handler");

    // 64 bytes inside block 0, so the block has to be read first.
    match fh.fh_write(1000, &vec![0xC7u8; 64]).await {
        Err(e) => eprintln!("partial write failed as it should: {}", e),
        Ok(n) => panic!(
            "partial write reported {} bytes written although the block could not \
             be read; the block was rebuilt from zeroes and would be persisted", n),
    }

    // The handle has to still work, which it will not if the failing write
    // kept the permit or its range. A whole-block write needs no retrieve,
    // so this is about the locks rather than about staging.
    let res = tokio::time::timeout(
        std::time::Duration::from_secs(10),
        fh.fh_write(0, &vec![0x33u8; BLK]),
    ).await;
    match res {
        Err(_) => panic!("a whole-block write hung after the failed one, so the \
                          failed write did not give back the permit or its range"),
        Ok(r) => { r.expect("whole-block write after a failed one"); },
    }

    let _ = fh.fh_flush().await.expect("flush");
    let _ = fh.fh_release().await;
    tf.cleanup(&client).await;
}

/// `fh_read_ahead` warms a range so the reads that follow find it.
///
/// The byte read path queries the data cache and does not fill it, so
/// read-ahead through `fh_read` costs requests and buys nothing — the
/// reporter measured object requests doubling with cache hits unchanged.
/// This checks what the entry point is for: the range is warmed in one
/// crossing with the requests coalesced, and the reads after it reach
/// staging not at all.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn reactor_read_ahead_warms_the_cache() {
    let _ = env_logger::try_init();
    let client = make_client().await;
    let tf = TestFile::new(&client).await;
    let reactor = make_reactor();

    const BLK: usize = 4096;
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

    let before = fh.fh_read_timing().await.expect("timing");
    let cached = fh.fh_read_ahead(0, NB * BLK).await.expect("read_ahead");
    let after_warm = fh.fh_read_timing().await.expect("timing");
    assert_eq!(cached, NB, "every block in the range should have been installed");

    let warm_gets = after_warm.data_gets - before.data_gets;
    assert!(warm_gets < NB as u64,
        "warming {} blocks took {} requests, so nothing was coalesced", NB, warm_gets);

    for b in 0..NB {
        let got = fh.fh_read_owned(b * BLK, BLK).await.expect("read");
        assert!(got.iter().all(|&v| v == (b % 251) as u8),
            "block {} came back wrong after read_ahead", b);
    }
    let after_reads = fh.fh_read_timing().await.expect("timing");
    assert_eq!(after_reads.data_gets, after_warm.data_gets,
        "reads over a warmed range must not issue object requests");

    eprintln!("warmed {} blocks with {} requests; {} following reads cost 0",
        cached, warm_gets, NB);

    let _ = fh.fh_release().await;
    tf.cleanup(&client).await;
}
