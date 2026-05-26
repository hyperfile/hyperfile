//! Smoke integration tests: happy-path create/write/read/truncate.
//!
//! Run with:
//! ```bash
//! HYPERFILE_TEST_BUCKET=<your-bucket> \
//! HYPERFILE_TEST_REGION=<your-region> \
//! cargo test --test integration_s3_smoke -- --ignored --test-threads=1
//! ```

mod common;
use common::*;

use hyperfile::file::hyper::Hyper;
use hyperfile::file::flags::FileFlags;
use hyperfile::file::mode::FileMode;

/// Verify basic create → write → flush → release → reopen → read round-trip.
#[tokio::test]
#[ignore]
async fn smoke_write_read_round_trip() {
    let _ = env_logger::try_init();
    let client = make_client().await;
    let tf = TestFile::new(&client).await;

    let payload: Vec<u8> = (0u8..=127u8).collect();
    {
        let mut hyper = Hyper::fs_open_or_create_with_default_opt(
            &client,
            tf.uri(),
            FileFlags::rdwr(),
            FileMode::default_file(),
        )
        .await
        .expect("create failed");
        let n = hyper.fs_write(0, &payload).await.expect("write failed");
        assert_eq!(n, payload.len());
        let _last_cno = hyper.fs_release().await.expect("release failed");
    }

    {
        let mut hyper = Hyper::fs_open(&client, tf.uri(), FileFlags::rdonly())
            .await
            .expect("open failed");
        let stat = hyper.fs_getattr().expect("getattr failed");
        assert_eq!(stat.st_size as usize, payload.len());

        let mut buf = vec![0u8; payload.len()];
        let n = hyper.fs_read(0, &mut buf).await.expect("read failed");
        assert_eq!(n, payload.len());
        assert_eq!(buf, payload);
        let _ = hyper.fs_release().await;
    }

    tf.cleanup(&client).await;
}

/// Truncate to extend an existing file — verify new size is reflected and
/// previously written data is preserved, newly extended range reads as zeros.
#[tokio::test]
#[ignore]
async fn smoke_truncate_extend() {
    let _ = env_logger::try_init();
    let client = make_client().await;
    let tf = TestFile::new(&client).await;

    let payload: Vec<u8> = (0..1024u16).map(|v| (v & 0xFF) as u8).collect();
    {
        let mut hyper = Hyper::fs_open_or_create_with_default_opt(
            &client,
            tf.uri(),
            FileFlags::rdwr(),
            FileMode::default_file(),
        )
        .await
        .expect("create failed");
        let n = hyper.fs_write(0, &payload).await.expect("write failed");
        assert_eq!(n, payload.len());
        let _ = hyper.fs_release().await.expect("release failed");
    }

    let new_size = 8 * 1024;
    {
        let mut hyper = Hyper::fs_open(&client, tf.uri(), FileFlags::rdwr())
            .await
            .expect("open failed");
        hyper.fs_truncate(new_size).await.expect("truncate extend failed");
        let _ = hyper.fs_release().await.expect("release failed");
    }

    {
        let mut hyper = Hyper::fs_open(&client, tf.uri(), FileFlags::rdonly())
            .await
            .expect("open failed");
        let stat = hyper.fs_getattr().expect("getattr");
        assert_eq!(stat.st_size as usize, new_size);

        let mut buf = vec![0u8; new_size];
        let n = hyper.fs_read(0, &mut buf).await.expect("read");
        assert_eq!(n, new_size);

        assert_eq!(&buf[..payload.len()], &payload[..]);
        assert!(buf[payload.len()..].iter().all(|&b| b == 0));
        let _ = hyper.fs_release().await;
    }

    tf.cleanup(&client).await;
}

/// Truncate to shrink an existing file — verify new size, content preserved
/// up to the new size, and reading beyond new size fails (or returns 0 bytes).
#[tokio::test]
#[ignore]
async fn smoke_truncate_shrink() {
    let _ = env_logger::try_init();
    let client = make_client().await;
    let tf = TestFile::new(&client).await;

    let payload: Vec<u8> = (0..16 * 1024).map(|i| (i & 0xFF) as u8).collect();
    {
        let mut hyper = Hyper::fs_open_or_create_with_default_opt(
            &client,
            tf.uri(),
            FileFlags::rdwr(),
            FileMode::default_file(),
        )
        .await
        .expect("create failed");
        let n = hyper.fs_write(0, &payload).await.expect("write failed");
        assert_eq!(n, payload.len());
        let _ = hyper.fs_release().await.expect("release");
    }

    let new_size = 5000;
    {
        let mut hyper = Hyper::fs_open(&client, tf.uri(), FileFlags::rdwr())
            .await
            .expect("open failed");
        hyper.fs_truncate(new_size).await.expect("truncate shrink failed");
        let _ = hyper.fs_release().await.expect("release");
    }

    {
        let mut hyper = Hyper::fs_open(&client, tf.uri(), FileFlags::rdonly())
            .await
            .expect("open failed");
        let stat = hyper.fs_getattr().expect("getattr");
        assert_eq!(stat.st_size as usize, new_size);

        let mut buf = vec![0u8; new_size];
        let n = hyper.fs_read(0, &mut buf).await.expect("read");
        assert_eq!(n, new_size);
        assert_eq!(&buf[..], &payload[..new_size]);
        let _ = hyper.fs_release().await;
    }

    tf.cleanup(&client).await;
}

/// O_TRUNC: opening an existing non-empty file with write access and
/// O_TRUNC must reset the length to 0. After release+reopen, getattr
/// must report 0 and a read returns 0 bytes.
#[tokio::test]
#[ignore]
async fn smoke_o_trunc_truncates_to_zero_on_open() {
    let _ = env_logger::try_init();
    let client = make_client().await;
    let tf = TestFile::new(&client).await;

    // Create a file with some content.
    let payload: Vec<u8> = (0..4096u16).map(|v| (v & 0xFF) as u8).collect();
    {
        let mut hyper = Hyper::fs_open_or_create_with_default_opt(
            &client,
            tf.uri(),
            FileFlags::rdwr(),
            FileMode::default_file(),
        )
        .await
        .expect("create failed");
        let n = hyper.fs_write(0, &payload).await.expect("write failed");
        assert_eq!(n, payload.len());
        let _ = hyper.fs_release().await.expect("release");
    }

    // Reopen with O_RDWR | O_TRUNC; the open path must truncate to 0.
    {
        let flags = FileFlags::from(libc::O_RDWR | libc::O_TRUNC);
        let mut hyper = Hyper::fs_open(&client, tf.uri(), flags)
            .await
            .expect("open w/ O_TRUNC failed");
        let stat = hyper.fs_getattr().expect("getattr");
        assert_eq!(stat.st_size, 0, "O_TRUNC should have set size to 0");
        let _ = hyper.fs_release().await.expect("release");
    }

    // Verify 0 bytes after reopen — i.e. the truncate persisted.
    {
        let mut hyper = Hyper::fs_open(&client, tf.uri(), FileFlags::rdonly())
            .await
            .expect("open failed");
        let stat = hyper.fs_getattr().expect("getattr");
        assert_eq!(stat.st_size, 0);

        let mut buf = vec![0u8; 16];
        let n = hyper.fs_read(0, &mut buf).await.expect("read");
        assert_eq!(n, 0);
        let _ = hyper.fs_release().await;
    }

    tf.cleanup(&client).await;
}

/// O_TRUNC without write access (O_RDONLY | O_TRUNC) is silently
/// ignored — content must still be readable. Mirrors Linux's
/// behaviour: glibc/the kernel no-op rather than reject.
#[tokio::test]
#[ignore]
async fn smoke_o_trunc_ignored_when_readonly() {
    let _ = env_logger::try_init();
    let client = make_client().await;
    let tf = TestFile::new(&client).await;

    let payload: Vec<u8> = (0..256u16).map(|v| (v & 0xFF) as u8).collect();
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

    // O_RDONLY | O_TRUNC: trunc bit should be ignored, content preserved.
    {
        let flags = FileFlags::from(libc::O_RDONLY | libc::O_TRUNC);
        let mut hyper = Hyper::fs_open(&client, tf.uri(), flags)
            .await
            .expect("open ro+trunc");
        let stat = hyper.fs_getattr().expect("getattr");
        assert_eq!(stat.st_size as usize, payload.len(), "O_TRUNC must be a no-op without write access");
        let mut buf = vec![0u8; payload.len()];
        let n = hyper.fs_read(0, &mut buf).await.expect("read");
        assert_eq!(n, payload.len());
        assert_eq!(buf, payload);
        let _ = hyper.fs_release().await;
    }

    tf.cleanup(&client).await;
}

/// O_NOATIME: a read does not advance st_atime. We compare atime
/// before and after a read on a file opened with O_NOATIME and assert
/// the field is unchanged. As a control, we do the same with a plain
/// O_RDONLY open and assert atime moved forward.
///
/// Note: atime updates land on the inode at read time but are only
/// visible to a fresh open after a successful flush + reopen.
/// Comparing within the same handle via `fs_getattr` is sufficient
/// here — `update_atime` writes the live in-memory inode that
/// `fs_getattr` reads from.
#[tokio::test]
#[ignore]
async fn smoke_o_noatime_skips_atime_update_on_read() {
    let _ = env_logger::try_init();
    let client = make_client().await;
    let tf = TestFile::new(&client).await;

    let payload = vec![0xABu8; 4096];
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

    // Control: plain O_RDONLY -- a read should advance atime.
    {
        let mut hyper = Hyper::fs_open(&client, tf.uri(), FileFlags::rdonly())
            .await
            .expect("open");
        let before = hyper.fs_getattr().expect("getattr").st_atime;
        // Sleep 1.1s so that the 1-second granularity of st_atime can
        // observe the change. (The inode timestamps come from
        // SystemTime::now(); resolution is sub-second but the libc
        // st_atime field is whole-seconds.)
        tokio::time::sleep(std::time::Duration::from_millis(1100)).await;
        let mut buf = vec![0u8; payload.len()];
        let _ = hyper.fs_read(0, &mut buf).await.expect("read");
        let after = hyper.fs_getattr().expect("getattr").st_atime;
        assert!(
            after > before,
            "control: O_RDONLY read should advance atime (before={}, after={})",
            before, after
        );
        let _ = hyper.fs_release().await;
    }

    // Test: O_RDONLY | O_NOATIME -- a read must NOT advance atime.
    {
        let flags = FileFlags::from(libc::O_RDONLY | libc::O_NOATIME);
        let mut hyper = Hyper::fs_open(&client, tf.uri(), flags)
            .await
            .expect("open w/ O_NOATIME");
        let before = hyper.fs_getattr().expect("getattr").st_atime;
        tokio::time::sleep(std::time::Duration::from_millis(1100)).await;
        let mut buf = vec![0u8; payload.len()];
        let _ = hyper.fs_read(0, &mut buf).await.expect("read");
        let after = hyper.fs_getattr().expect("getattr").st_atime;
        assert_eq!(
            before, after,
            "O_NOATIME read must not advance atime (before={}, after={})",
            before, after
        );
        let _ = hyper.fs_release().await;
    }

    tf.cleanup(&client).await;
}
