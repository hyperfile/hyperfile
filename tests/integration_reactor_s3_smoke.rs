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
