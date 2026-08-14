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

/// O_APPEND (single handle, direct API): every fs_write goes to
/// end of file regardless of the offset argument. Verifies:
///   - first append on empty file lands at offset 0
///   - second append lands at end of first
///   - explicit offset arg is ignored under O_APPEND
#[tokio::test]
#[ignore]
async fn smoke_o_append_writes_at_end() {
    let _ = env_logger::try_init();
    let client = make_client().await;
    let tf = TestFile::new(&client).await;

    // Create the file empty.
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

    // Open with O_APPEND, write twice, both at deliberately-wrong
    // offsets. POSIX requires the offsets to be ignored.
    let part_a = b"hello-".to_vec();
    let part_b = b"world\n".to_vec();
    {
        let flags = FileFlags::from(libc::O_RDWR | libc::O_APPEND);
        let mut hyper = Hyper::fs_open(&client, tf.uri(), flags)
            .await
            .expect("open w/ O_APPEND");
        // Misleading offset: 999 — should be ignored.
        let n = hyper.fs_write(999, &part_a).await.expect("write A");
        assert_eq!(n, part_a.len());
        // Same again — different misleading offset.
        let n = hyper.fs_write(0, &part_b).await.expect("write B");
        assert_eq!(n, part_b.len());

        let stat = hyper.fs_getattr().expect("getattr");
        assert_eq!(stat.st_size as usize, part_a.len() + part_b.len());
        let _ = hyper.fs_release().await.expect("release");
    }

    // Verify on a fresh open: appended in order.
    {
        let mut hyper = Hyper::fs_open(&client, tf.uri(), FileFlags::rdonly())
            .await
            .expect("reopen");
        let total = part_a.len() + part_b.len();
        let mut buf = vec![0u8; total];
        let n = hyper.fs_read(0, &mut buf).await.expect("read");
        assert_eq!(n, total);
        let mut want = part_a.clone();
        want.extend_from_slice(&part_b);
        assert_eq!(buf, want);
        let _ = hyper.fs_release().await;
    }

    tf.cleanup(&client).await;
}

/// O_APPEND with write_zero: each write_zero appends the requested
/// number of zero bytes, ignoring the offset argument.
#[tokio::test]
#[ignore]
async fn smoke_o_append_write_zero() {
    let _ = env_logger::try_init();
    let client = make_client().await;
    let tf = TestFile::new(&client).await;

    // Create with a small payload.
    let head = vec![0xCDu8; 100];
    {
        let mut hyper = Hyper::fs_open_or_create_with_default_opt(
            &client,
            tf.uri(),
            FileFlags::rdwr(),
            FileMode::default_file(),
        )
        .await
        .expect("create");
        let _ = hyper.fs_write(0, &head).await.expect("write head");
        let _ = hyper.fs_release().await.expect("release");
    }

    // Append 200 zeros via write_zero under O_APPEND. The 7777
    // offset must be ignored.
    {
        let flags = FileFlags::from(libc::O_RDWR | libc::O_APPEND);
        let mut hyper = Hyper::fs_open(&client, tf.uri(), flags)
            .await
            .expect("open");
        let n = hyper.fs_write_zero(7777, 200).await.expect("write_zero");
        assert_eq!(n, 200);
        let stat = hyper.fs_getattr().expect("getattr");
        assert_eq!(stat.st_size as usize, head.len() + 200);
        let _ = hyper.fs_release().await.expect("release");
    }

    // Verify: head intact, 200 zeros after.
    {
        let mut hyper = Hyper::fs_open(&client, tf.uri(), FileFlags::rdonly())
            .await
            .expect("reopen");
        let total = head.len() + 200;
        let mut buf = vec![0u8; total];
        let n = hyper.fs_read(0, &mut buf).await.expect("read");
        assert_eq!(n, total);
        assert_eq!(&buf[..head.len()], &head[..]);
        assert!(buf[head.len()..].iter().all(|&b| b == 0));
        let _ = hyper.fs_release().await;
    }

    tf.cleanup(&client).await;
}

/// O_EXCL with O_CREAT on a missing file: must create successfully.
/// (fs_open_or_create + the EXCL bit set.)
#[tokio::test]
#[ignore]
async fn smoke_o_excl_creates_when_missing() {
    let _ = env_logger::try_init();
    let client = make_client().await;
    let tf = TestFile::new(&client).await;

    let flags = FileFlags::from(libc::O_RDWR | libc::O_CREAT | libc::O_EXCL);
    let mut hyper = Hyper::fs_open_or_create_with_default_opt(
        &client,
        tf.uri(),
        flags,
        FileMode::default_file(),
    )
    .await
    .expect("O_CREAT|O_EXCL on missing file must succeed");

    let stat = hyper.fs_getattr().expect("getattr");
    assert_eq!(stat.st_size, 0);
    let _ = hyper.fs_release().await;

    tf.cleanup(&client).await;
}

/// O_EXCL with O_CREAT on an existing file: must error AlreadyExists.
#[tokio::test]
#[ignore]
async fn smoke_o_excl_errors_when_exists() {
    let _ = env_logger::try_init();
    let client = make_client().await;
    let tf = TestFile::new(&client).await;

    // Pre-create.
    {
        let mut hyper = Hyper::fs_open_or_create_with_default_opt(
            &client,
            tf.uri(),
            FileFlags::rdwr(),
            FileMode::default_file(),
        )
        .await
        .expect("pre-create");
        let _ = hyper.fs_release().await.expect("release");
    }

    // Attempt with O_EXCL: must fail.
    let flags = FileFlags::from(libc::O_RDWR | libc::O_CREAT | libc::O_EXCL);
    let res = Hyper::fs_open_or_create_with_default_opt(
        &client,
        tf.uri(),
        flags,
        FileMode::default_file(),
    )
    .await;
    match res {
        Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {}
        Err(e) => panic!("expected AlreadyExists, got {:?}", e),
        Ok(_) => panic!("O_CREAT|O_EXCL on existing file must fail"),
    }

    tf.cleanup(&client).await;
}

/// O_CREAT alone (no O_EXCL) on an existing file: must open it,
/// not error. Verifies the existing open-or-create path still
/// works after the EXCL change.
#[tokio::test]
#[ignore]
async fn smoke_o_creat_without_excl_opens_existing() {
    let _ = env_logger::try_init();
    let client = make_client().await;
    let tf = TestFile::new(&client).await;

    // Pre-create with content.
    let payload = vec![0xEEu8; 256];
    {
        let mut hyper = Hyper::fs_open_or_create_with_default_opt(
            &client,
            tf.uri(),
            FileFlags::rdwr(),
            FileMode::default_file(),
        )
        .await
        .expect("pre-create");
        let _ = hyper.fs_write(0, &payload).await.expect("write");
        let _ = hyper.fs_release().await.expect("release");
    }

    // Reopen with O_CREAT (no O_EXCL): must open existing, content preserved.
    let flags = FileFlags::from(libc::O_RDWR | libc::O_CREAT);
    let mut hyper = Hyper::fs_open_or_create_with_default_opt(
        &client,
        tf.uri(),
        flags,
        FileMode::default_file(),
    )
    .await
    .expect("O_CREAT without O_EXCL on existing file must succeed");

    let stat = hyper.fs_getattr().expect("getattr");
    assert_eq!(stat.st_size as usize, payload.len(), "existing content lost");

    let mut buf = vec![0u8; payload.len()];
    let n = hyper.fs_read(0, &mut buf).await.expect("read");
    assert_eq!(n, payload.len());
    assert_eq!(buf, payload);
    let _ = hyper.fs_release().await;

    tf.cleanup(&client).await;
}

/// O_EXCL on the bare fs_open path (no O_CREAT): per POSIX this is
/// undefined and Linux ignores the EXCL bit. Hyperfile follows the
/// same rule — fs_open + O_EXCL on an existing file should still
/// open it, on a missing file should still NotFound.
#[tokio::test]
#[ignore]
async fn smoke_o_excl_without_creat_is_ignored_on_open() {
    let _ = env_logger::try_init();
    let client = make_client().await;
    let tf = TestFile::new(&client).await;

    // Pre-create.
    {
        let mut hyper = Hyper::fs_open_or_create_with_default_opt(
            &client,
            tf.uri(),
            FileFlags::rdwr(),
            FileMode::default_file(),
        )
        .await
        .expect("pre-create");
        let _ = hyper.fs_release().await.expect("release");
    }

    // fs_open + O_EXCL: must succeed (EXCL ignored on bare open).
    let flags = FileFlags::from(libc::O_RDWR | libc::O_EXCL);
    let mut hyper = Hyper::fs_open(&client, tf.uri(), flags)
        .await
        .expect("fs_open + O_EXCL must ignore the EXCL bit");
    let _ = hyper.fs_release().await;

    tf.cleanup(&client).await;
}

/// st_blocks reflects only actually-allocated 512-byte units, not
/// `ceil(st_size / 512)`. Verifies the fix for the previous
/// behaviour that inferred st_blocks from st_size and reported
/// huge inflated values for sparse files.
///
/// Scenario:
///   - Create file
///   - Write 4 KiB at offset 0  (1 block)
///   - Write 4 KiB at offset 1 GiB (1 more block — sparse hole in
///     between)
///   - Reopen and check st_size = 1 GiB + 4 KiB but st_blocks = 16
///     (= 2 blocks * 4 KiB / 512). NOT (1 GiB + 4 KiB) / 512.
#[tokio::test]
#[ignore]
async fn smoke_sparse_st_blocks_does_not_inflate() {
    let _ = env_logger::try_init();
    let client = make_client().await;
    let tf = TestFile::new(&client).await;

    const ONE_GIB: usize = 1024 * 1024 * 1024;
    const BLOCK: usize = 4096;

    {
        let mut hyper = Hyper::fs_open_or_create_with_default_opt(
            &client,
            tf.uri(),
            FileFlags::rdwr(),
            FileMode::default_file(),
        )
        .await
        .expect("create");
        let buf = vec![0xAA; BLOCK];
        hyper.fs_write(0, &buf).await.expect("write head");
        hyper.fs_write(ONE_GIB, &buf).await.expect("write tail");
        let _ = hyper.fs_release().await.expect("release");
    }

    {
        let hyper = Hyper::fs_open(&client, tf.uri(), FileFlags::rdonly())
            .await
            .expect("reopen");
        let stat = hyper.fs_getattr().expect("getattr");
        assert_eq!(
            stat.st_size as usize,
            ONE_GIB + BLOCK,
            "st_size should reflect the virtual file size"
        );
        // 2 data blocks * 4 KiB = 8 KiB allocated; 8 KiB / 512 = 16
        // 512-byte units. The bug we're guarding against would
        // report ~2097160 (= ceil((1 GiB + 4 KiB) / 512)).
        let expected_blocks = (2 * BLOCK / 512) as i64;
        let bug_value = ((ONE_GIB + BLOCK) / 512) as i64;
        assert_eq!(
            stat.st_blocks, expected_blocks,
            "st_blocks should reflect actually-allocated 512-byte units \
             (expected {}, got {}); the old buggy value would be {}",
            expected_blocks, stat.st_blocks, bug_value
        );
        // st_blksize should be the data_block_size (default: 4 KiB)
        assert_eq!(
            stat.st_blksize as usize, BLOCK,
            "st_blksize should equal the data_block_size from meta config"
        );
    }

    tf.cleanup(&client).await;
}

/// st_blocks decreases when a file is truncated to shrink. Write
/// 8 blocks worth of data, truncate to 1 block, verify st_blocks
/// reports 8 (= 4096 / 512), not 64.
#[tokio::test]
#[ignore]
async fn smoke_truncate_shrink_decrements_st_blocks() {
    let _ = env_logger::try_init();
    let client = make_client().await;
    let tf = TestFile::new(&client).await;

    const BLOCK: usize = 4096;
    let payload: Vec<u8> = (0..BLOCK * 8).map(|i| (i & 0xFF) as u8).collect();

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

    {
        let mut hyper = Hyper::fs_open(&client, tf.uri(), FileFlags::rdwr())
            .await
            .expect("reopen rdwr");
        let stat_before = hyper.fs_getattr().expect("getattr before");
        assert_eq!(stat_before.st_blocks, (8 * BLOCK / 512) as i64);

        hyper.fs_truncate(BLOCK).await.expect("truncate to 1 block");
        let _ = hyper.fs_release().await.expect("release");
    }

    {
        let hyper = Hyper::fs_open(&client, tf.uri(), FileFlags::rdonly())
            .await
            .expect("reopen ro");
        let stat = hyper.fs_getattr().expect("getattr");
        assert_eq!(stat.st_size as usize, BLOCK);
        assert_eq!(
            stat.st_blocks,
            (BLOCK / 512) as i64,
            "after shrinking from 8 blocks to 1, st_blocks should be 8 (= 4096/512), not 64"
        );
    }

    tf.cleanup(&client).await;
}

/// **Edge case**: truncate to an exact block boundary must NOT
/// zero the last fully-retained block. With data_block_size = 4096:
///
///   - Pre-condition: file size = 8192 (2 full blocks of 0xAA)
///   - Action: truncate to 4096 (= 1 * data_block_size)
///   - Expected: file contains [0, 4096) of 0xAA
///
/// The bug was that `truncate_last_data_block(0, offset_to_discard=0)`
/// got called, which interprets "offset 0" as "zero from start", and
/// wiped out the entire last block.
#[tokio::test]
#[ignore]
async fn smoke_truncate_shrink_to_block_boundary_preserves_data() {
    let _ = env_logger::try_init();
    let client = make_client().await;
    let tf = TestFile::new(&client).await;

    const BLOCK: usize = 4096;
    let payload = vec![0xAAu8; BLOCK * 2];
    {
        let mut hyper = Hyper::fs_open_or_create_with_default_opt(
            &client, tf.uri(), FileFlags::rdwr(), FileMode::default_file(),
        ).await.expect("create");
        let _ = hyper.fs_write(0, &payload).await.expect("write");
        let _ = hyper.fs_release().await.expect("release");
    }

    {
        let mut hyper = Hyper::fs_open(&client, tf.uri(), FileFlags::rdwr())
            .await.expect("reopen rdwr");
        // Truncate down to one full block.
        hyper.fs_truncate(BLOCK).await.expect("truncate to boundary");
        let _ = hyper.fs_release().await.expect("release");
    }

    {
        let mut hyper = Hyper::fs_open(&client, tf.uri(), FileFlags::rdonly())
            .await.expect("reopen ro");
        let stat = hyper.fs_getattr().expect("getattr");
        assert_eq!(stat.st_size as usize, BLOCK);

        let mut buf = vec![0u8; BLOCK];
        let n = hyper.fs_read(0, &mut buf).await.expect("read");
        assert_eq!(n, BLOCK);
        assert_eq!(
            buf,
            payload[..BLOCK],
            "truncate to a block boundary must keep the last fully-retained \
             block intact; got the block partly or fully zeroed"
        );
    }

    tf.cleanup(&client).await;
}

/// Edge case: shrink from N to N-1 when both fall in the same
/// data block (same-block branch). E.g. 4097 → 4096.
///
/// Both old and new sizes have the same `cur_blk_idx` (= 1), so
/// the truncate code takes the same-block branch. This case used
/// to also call truncate_last_data_block with offset_to_discard=0,
/// which zeroed an out-of-file block — wasteful but not wrong.
/// After truncate, reading the file must return the first 4096
/// bytes of payload intact.
#[tokio::test]
#[ignore]
async fn smoke_truncate_same_block_to_boundary() {
    let _ = env_logger::try_init();
    let client = make_client().await;
    let tf = TestFile::new(&client).await;

    const BLOCK: usize = 4096;
    let payload: Vec<u8> = (0..BLOCK + 1).map(|i| (i & 0xFF) as u8).collect();
    {
        let mut hyper = Hyper::fs_open_or_create_with_default_opt(
            &client, tf.uri(), FileFlags::rdwr(), FileMode::default_file(),
        ).await.expect("create");
        let _ = hyper.fs_write(0, &payload).await.expect("write");
        // truncate to BLOCK before release; same-block (both are in block 1)
        hyper.fs_truncate(BLOCK).await.expect("truncate");
        let _ = hyper.fs_release().await.expect("release");
    }

    let mut hyper = Hyper::fs_open(&client, tf.uri(), FileFlags::rdonly())
        .await.expect("reopen");
    let stat = hyper.fs_getattr().expect("getattr");
    assert_eq!(stat.st_size as usize, BLOCK);
    let mut buf = vec![0u8; BLOCK];
    hyper.fs_read(0, &mut buf).await.expect("read");
    assert_eq!(buf, payload[..BLOCK], "block 0 content must be preserved");
    let _ = hyper.fs_release().await;
    tf.cleanup(&client).await;
}

/// Edge case: shrink within a single mid-block. E.g. size=3000 →
/// new_size=1500. Both in block 0. Same-block branch.
/// truncate_last_data_block(0, 1500) zeros bytes [1500, 4096).
/// Reads of [0, 1500) must return original payload.
#[tokio::test]
#[ignore]
async fn smoke_truncate_same_block_mid_to_mid() {
    let _ = env_logger::try_init();
    let client = make_client().await;
    let tf = TestFile::new(&client).await;

    let payload: Vec<u8> = (0..3000).map(|i| (i & 0xFF) as u8).collect();
    {
        let mut hyper = Hyper::fs_open_or_create_with_default_opt(
            &client, tf.uri(), FileFlags::rdwr(), FileMode::default_file(),
        ).await.expect("create");
        let _ = hyper.fs_write(0, &payload).await.expect("write");
        hyper.fs_truncate(1500).await.expect("truncate");
        let _ = hyper.fs_release().await.expect("release");
    }

    let mut hyper = Hyper::fs_open(&client, tf.uri(), FileFlags::rdonly())
        .await.expect("reopen");
    assert_eq!(hyper.fs_getattr().expect("ga").st_size as usize, 1500);
    let mut buf = vec![0u8; 1500];
    hyper.fs_read(0, &mut buf).await.expect("read");
    assert_eq!(buf, payload[..1500], "content [0,1500) must survive");
    let _ = hyper.fs_release().await;
    tf.cleanup(&client).await;
}

/// Edge case: cross-block shrink ending mid-block. Write 3 full
/// blocks of 0xAA, truncate to mid-block 1 (5000). Block 2 must
/// be dropped, block 1 retained with [0, 904)=0xAA, [904, 4096)=0,
/// block 0 untouched.
#[tokio::test]
#[ignore]
async fn smoke_truncate_cross_block_shrink_to_mid() {
    let _ = env_logger::try_init();
    let client = make_client().await;
    let tf = TestFile::new(&client).await;

    const BLOCK: usize = 4096;
    let payload = vec![0xAAu8; BLOCK * 3];
    {
        let mut hyper = Hyper::fs_open_or_create_with_default_opt(
            &client, tf.uri(), FileFlags::rdwr(), FileMode::default_file(),
        ).await.expect("create");
        let _ = hyper.fs_write(0, &payload).await.expect("write");
        hyper.fs_truncate(BLOCK + 904).await.expect("truncate to 5000");
        let _ = hyper.fs_release().await.expect("release");
    }

    let mut hyper = Hyper::fs_open(&client, tf.uri(), FileFlags::rdonly())
        .await.expect("reopen");
    let stat = hyper.fs_getattr().expect("ga");
    assert_eq!(stat.st_size as usize, BLOCK + 904);
    // 2 blocks allocated: block 0 + block 1 → 8 KiB / 512 = 16
    assert_eq!(stat.st_blocks, (2 * BLOCK / 512) as i64);

    let mut buf = vec![0u8; BLOCK + 904];
    hyper.fs_read(0, &mut buf).await.expect("read");
    assert!(buf.iter().all(|&b| b == 0xAA), "content survives in [0, new_size)");
    let _ = hyper.fs_release().await;
    tf.cleanup(&client).await;
}

/// Edge case: extend file to mid-block. Reads of the new range
/// must return zeros (sparse hole). bmap unchanged on the extend
/// itself.
#[tokio::test]
#[ignore]
async fn smoke_truncate_extend_to_mid_block() {
    let _ = env_logger::try_init();
    let client = make_client().await;
    let tf = TestFile::new(&client).await;

    const BLOCK: usize = 4096;
    let payload = vec![0xCCu8; BLOCK];
    {
        let mut hyper = Hyper::fs_open_or_create_with_default_opt(
            &client, tf.uri(), FileFlags::rdwr(), FileMode::default_file(),
        ).await.expect("create");
        let _ = hyper.fs_write(0, &payload).await.expect("write");
        hyper.fs_truncate(BLOCK + 1234).await.expect("extend to 5330");
        let _ = hyper.fs_release().await.expect("release");
    }

    let mut hyper = Hyper::fs_open(&client, tf.uri(), FileFlags::rdonly())
        .await.expect("reopen");
    let stat = hyper.fs_getattr().expect("ga");
    assert_eq!(stat.st_size as usize, BLOCK + 1234);
    // Only block 0 actually allocated (extension is sparse)
    assert_eq!(stat.st_blocks, (BLOCK / 512) as i64);

    let mut buf = vec![0u8; BLOCK + 1234];
    hyper.fs_read(0, &mut buf).await.expect("read");
    assert_eq!(&buf[..BLOCK], &payload[..]);
    assert!(buf[BLOCK..].iter().all(|&b| b == 0), "extended range reads as zeros");
    let _ = hyper.fs_release().await;
    tf.cleanup(&client).await;
}

/// Edge case: truncate to 0, then write again. After truncate(0)
/// all blocks should be discarded; subsequent writes should
/// produce a fresh file.
#[tokio::test]
#[ignore]
async fn smoke_truncate_to_zero_then_write() {
    let _ = env_logger::try_init();
    let client = make_client().await;
    let tf = TestFile::new(&client).await;

    const BLOCK: usize = 4096;
    let initial = vec![0xAAu8; BLOCK * 2];
    let final_payload = vec![0xBBu8; 1000];

    {
        let mut hyper = Hyper::fs_open_or_create_with_default_opt(
            &client, tf.uri(), FileFlags::rdwr(), FileMode::default_file(),
        ).await.expect("create");
        let _ = hyper.fs_write(0, &initial).await.expect("write 8 KiB");
        let _ = hyper.fs_release().await.expect("release");
    }

    {
        let mut hyper = Hyper::fs_open(&client, tf.uri(), FileFlags::rdwr())
            .await.expect("reopen");
        hyper.fs_truncate(0).await.expect("truncate to zero");
        let _ = hyper.fs_write(0, &final_payload).await.expect("write 1 KiB");
        let _ = hyper.fs_release().await.expect("release");
    }

    let mut hyper = Hyper::fs_open(&client, tf.uri(), FileFlags::rdonly())
        .await.expect("reopen ro");
    let stat = hyper.fs_getattr().expect("ga");
    assert_eq!(stat.st_size as usize, final_payload.len());
    assert_eq!(stat.st_blocks, (BLOCK / 512) as i64,
        "should report 1 block (the new write), not 2 (stale from initial)");

    let mut buf = vec![0u8; final_payload.len()];
    hyper.fs_read(0, &mut buf).await.expect("read");
    assert_eq!(buf, final_payload);
    let _ = hyper.fs_release().await;
    tf.cleanup(&client).await;
}

/// Edge case: extend then shrink back to original size — must
/// behave the same as never extending. The shrink path should
/// drop the (sparse) blocks that came from the extension.
#[tokio::test]
#[ignore]
async fn smoke_truncate_extend_then_shrink_back() {
    let _ = env_logger::try_init();
    let client = make_client().await;
    let tf = TestFile::new(&client).await;

    const BLOCK: usize = 4096;
    let payload = vec![0xCDu8; 100];

    {
        let mut hyper = Hyper::fs_open_or_create_with_default_opt(
            &client, tf.uri(), FileFlags::rdwr(), FileMode::default_file(),
        ).await.expect("create");
        let _ = hyper.fs_write(0, &payload).await.expect("write 100");
        hyper.fs_truncate(BLOCK * 4).await.expect("extend to 16 KiB");
        hyper.fs_truncate(payload.len()).await.expect("shrink back to 100");
        let _ = hyper.fs_release().await.expect("release");
    }

    let mut hyper = Hyper::fs_open(&client, tf.uri(), FileFlags::rdonly())
        .await.expect("reopen");
    let stat = hyper.fs_getattr().expect("ga");
    assert_eq!(stat.st_size as usize, payload.len());
    let mut buf = vec![0u8; payload.len()];
    hyper.fs_read(0, &mut buf).await.expect("read");
    assert_eq!(buf, payload);
    let _ = hyper.fs_release().await;
    tf.cleanup(&client).await;
}

/// Edge case: shrink to 1 byte. The last partial block has a
/// single user byte; bytes [1, block_size) must read as zero.
#[tokio::test]
#[ignore]
async fn smoke_truncate_shrink_to_one_byte() {
    let _ = env_logger::try_init();
    let client = make_client().await;
    let tf = TestFile::new(&client).await;

    const BLOCK: usize = 4096;
    let payload = vec![0xAAu8; BLOCK];
    {
        let mut hyper = Hyper::fs_open_or_create_with_default_opt(
            &client, tf.uri(), FileFlags::rdwr(), FileMode::default_file(),
        ).await.expect("create");
        let _ = hyper.fs_write(0, &payload).await.expect("write");
        let _ = hyper.fs_release().await.expect("release");
    }

    {
        let mut hyper = Hyper::fs_open(&client, tf.uri(), FileFlags::rdwr())
            .await.expect("reopen");
        hyper.fs_truncate(1).await.expect("truncate to 1 byte");
        let _ = hyper.fs_release().await.expect("release");
    }

    let mut hyper = Hyper::fs_open(&client, tf.uri(), FileFlags::rdonly())
        .await.expect("reopen ro");
    let stat = hyper.fs_getattr().expect("ga");
    assert_eq!(stat.st_size, 1);
    let mut buf = vec![0u8; 1];
    hyper.fs_read(0, &mut buf).await.expect("read");
    assert_eq!(buf[0], 0xAA);
    let _ = hyper.fs_release().await;
    tf.cleanup(&client).await;
}

/// Edge case: truncate-shrink across a zero-block (block created by
/// fs_write_zero). truncate_last_data_block has special handling
/// for zero-block blkptrs (returns Ok(false), no zeroing). Verify
/// the path works and content is correct.
#[tokio::test]
#[ignore]
async fn smoke_truncate_shrink_through_zero_block() {
    let _ = env_logger::try_init();
    let client = make_client().await;
    let tf = TestFile::new(&client).await;

    const BLOCK: usize = 4096;

    // Layout: block 0 = data 0xAA (4 KiB), block 1 = zero block (4 KiB),
    //         block 2 = data 0xCC (4 KiB).  size = 12 KiB.
    {
        let mut hyper = Hyper::fs_open_or_create_with_default_opt(
            &client, tf.uri(), FileFlags::rdwr(), FileMode::default_file(),
        ).await.expect("create");
        let _ = hyper.fs_write(0, &vec![0xAAu8; BLOCK]).await.expect("write block 0");
        let _ = hyper.fs_write_zero(BLOCK, BLOCK).await.expect("write_zero block 1");
        let _ = hyper.fs_write(BLOCK * 2, &vec![0xCCu8; BLOCK]).await.expect("write block 2");
        let _ = hyper.fs_release().await.expect("release");
    }

    // Truncate to mid-block 1 (size 5000): block 2 dropped, block 1
    // (zero block) kept partially. The current truncate code calls
    // truncate_last_data_block(1, 904) → bmap.lookup(1) → zero block
    // → returns Ok(false), no data change. set_size(5000).
    {
        let mut hyper = Hyper::fs_open(&client, tf.uri(), FileFlags::rdwr())
            .await.expect("reopen rdwr");
        hyper.fs_truncate(BLOCK + 904).await.expect("truncate to 5000");
        let _ = hyper.fs_release().await.expect("release");
    }

    let mut hyper = Hyper::fs_open(&client, tf.uri(), FileFlags::rdonly())
        .await.expect("reopen ro");
    let stat = hyper.fs_getattr().expect("ga");
    assert_eq!(stat.st_size as usize, BLOCK + 904);

    let mut buf = vec![0xFFu8; BLOCK + 904]; // pre-fill with sentinel
    hyper.fs_read(0, &mut buf).await.expect("read");
    assert!(buf[..BLOCK].iter().all(|&b| b == 0xAA), "block 0 must be 0xAA");
    assert!(buf[BLOCK..].iter().all(|&b| b == 0), "tail of zero-block 1 must read as 0");
    let _ = hyper.fs_release().await;
    tf.cleanup(&client).await;
}

/// Edge case: truncate-shrink to exactly the boundary of a zero
/// block. Should drop the zero block entirely and keep the
/// previous block (whose content was 0xAA) intact.
#[tokio::test]
#[ignore]
async fn smoke_truncate_shrink_to_zero_block_boundary() {
    let _ = env_logger::try_init();
    let client = make_client().await;
    let tf = TestFile::new(&client).await;

    const BLOCK: usize = 4096;

    // Layout: block 0 = 0xAA, block 1 = zero block. size=8 KiB.
    {
        let mut hyper = Hyper::fs_open_or_create_with_default_opt(
            &client, tf.uri(), FileFlags::rdwr(), FileMode::default_file(),
        ).await.expect("create");
        let _ = hyper.fs_write(0, &vec![0xAAu8; BLOCK]).await.expect("write");
        let _ = hyper.fs_write_zero(BLOCK, BLOCK).await.expect("write_zero");
        let _ = hyper.fs_release().await.expect("release");
    }

    // Truncate to BLOCK (exact boundary, drops block 1)
    {
        let mut hyper = Hyper::fs_open(&client, tf.uri(), FileFlags::rdwr())
            .await.expect("reopen rdwr");
        hyper.fs_truncate(BLOCK).await.expect("truncate");
        let _ = hyper.fs_release().await.expect("release");
    }

    let mut hyper = Hyper::fs_open(&client, tf.uri(), FileFlags::rdonly())
        .await.expect("reopen ro");
    let stat = hyper.fs_getattr().expect("ga");
    assert_eq!(stat.st_size as usize, BLOCK);
    let mut buf = vec![0u8; BLOCK];
    hyper.fs_read(0, &mut buf).await.expect("read");
    assert!(buf.iter().all(|&b| b == 0xAA), "block 0 (0xAA) must survive");
    let _ = hyper.fs_release().await;
    tf.cleanup(&client).await;
}

/// Regression test for the "assign key not found in direct node"
/// failure: write multiple blocks, then immediately truncate-shrink
/// across a block boundary WITHOUT calling fs_flush in between.
///
/// Pre-fix: the dirty data block cache still held the truncated-
/// away blocks, so the next flush iterated those blocks and called
/// `bmap.assign(blk_idx, real_ptr)` on a key that bmap.truncate
/// had just dropped — surfacing as `NotFound: assign key not found
/// in direct node`.
///
/// Fix: `truncate_shrink` now removes dirty cache entries whose
/// key would be discarded by the bmap.truncate that follows.
#[tokio::test]
#[ignore]
async fn smoke_truncate_shrink_with_unflushed_writes() {
    let _ = env_logger::try_init();
    let client = make_client().await;
    let tf = TestFile::new(&client).await;

    const BLOCK: usize = 4096;
    let payload = vec![0xAAu8; BLOCK * 3]; // 3 dirty blocks: 0, 1, 2
    {
        let mut hyper = Hyper::fs_open_or_create_with_default_opt(
            &client, tf.uri(), FileFlags::rdwr(), FileMode::default_file(),
        ).await.expect("create");
        let _ = hyper.fs_write(0, &payload).await.expect("write");
        // No fs_flush here — dirty cache has blocks 0, 1, 2.
        // Truncate to mid-block 1: drops bmap entries 2..; the
        // dirty cache entry for block 2 must be evicted by
        // truncate or the subsequent internal flush will fail.
        hyper.fs_truncate(BLOCK + 904).await
            .expect("truncate-shrink with unflushed dirty cache must not fail");
        let _ = hyper.fs_release().await.expect("release");
    }

    let mut hyper = Hyper::fs_open(&client, tf.uri(), FileFlags::rdonly())
        .await.expect("reopen");
    let stat = hyper.fs_getattr().expect("ga");
    assert_eq!(stat.st_size as usize, BLOCK + 904);

    let mut buf = vec![0u8; BLOCK + 904];
    hyper.fs_read(0, &mut buf).await.expect("read");
    assert!(buf.iter().all(|&b| b == 0xAA),
        "first 5000 bytes of 0xAA must survive truncate");
    let _ = hyper.fs_release().await;
    tf.cleanup(&client).await;
}

/// fs_fdatasync: when only attribute fields are dirty (e.g. atime
/// from a read), skip the segment write and leave last_cno
/// unchanged. Compare against fs_flush which DOES bump last_cno.
#[tokio::test]
#[ignore]
async fn smoke_fdatasync_skips_attr_only_flush() {
    let _ = env_logger::try_init();
    let client = make_client().await;
    let tf = TestFile::new(&client).await;

    // Create + write some data, then flush so we have a baseline
    // persisted segment.
    let payload = vec![0xAAu8; 4096];
    let baseline_cno = {
        let mut hyper = Hyper::fs_open_or_create_with_default_opt(
            &client, tf.uri(), FileFlags::rdwr(), FileMode::default_file(),
        ).await.expect("create");
        let _ = hyper.fs_write(0, &payload).await.expect("write");
        hyper.fs_flush().await.expect("flush baseline")
    };

    // fdatasync arm: open ro, read (dirties atime in-memory), then
    // fs_fdatasync. Expect last_cno unchanged AND the attr-dirty
    // bit unchanged (the fdatasync skip is observable: we didn't
    // persist the attr change).
    {
        let mut hyper = Hyper::fs_open(&client, tf.uri(), FileFlags::rdonly())
            .await.expect("open ro");
        let mut buf = vec![0u8; payload.len()];
        let _ = hyper.fs_read(0, &mut buf).await.expect("read");
        // Confirm the inode is in the "attr-only dirty" state.
        assert!(hyper.is_attr_dirty(),
            "read should have dirtied atime in-memory");
        assert_eq!(hyper.dirty_block_count(), 0);
        let cno_after_fdatasync = hyper.fs_fdatasync().await
            .expect("fdatasync attr-only");
        assert_eq!(
            cno_after_fdatasync, baseline_cno,
            "fdatasync must not bump cno"
        );
        assert!(hyper.is_attr_dirty(),
            "fdatasync must leave attr-dirty bit SET (skip = no inode write)");
        let _ = hyper.fs_release().await;
    }

    // Control arm: same setup but use fs_flush — must clear the
    // attr-dirty bit (it issues an inode-only S3 PUT). Hyperfile's
    // attr-only flush does NOT bump cno (the segment id is for new
    // segments; inode-only PUT overwrites the same key).
    {
        let mut hyper = Hyper::fs_open(&client, tf.uri(), FileFlags::rdonly())
            .await.expect("open ro");
        let mut buf = vec![0u8; payload.len()];
        let _ = hyper.fs_read(0, &mut buf).await.expect("read");
        assert!(hyper.is_attr_dirty());
        let _ = hyper.fs_flush().await.expect("flush attr-only");
        assert!(!hyper.is_attr_dirty(),
            "fs_flush must clear attr-dirty bit (inode written to S3)");
        let _ = hyper.fs_release().await;
    }

    tf.cleanup(&client).await;
}

/// fs_fdatasync: when data is dirty, behaves identically to
/// fs_flush — must persist the data + bmap + size and bump cno.
#[tokio::test]
#[ignore]
async fn smoke_fdatasync_persists_data() {
    let _ = env_logger::try_init();
    let client = make_client().await;
    let tf = TestFile::new(&client).await;

    let baseline_cno = {
        let mut hyper = Hyper::fs_open_or_create_with_default_opt(
            &client, tf.uri(), FileFlags::rdwr(), FileMode::default_file(),
        ).await.expect("create");
        hyper.fs_release().await.expect("release")
    };

    let payload = vec![0xBBu8; 4096];
    let after_fdatasync_cno = {
        let mut hyper = Hyper::fs_open(&client, tf.uri(), FileFlags::rdwr())
            .await.expect("open rdwr");
        let _ = hyper.fs_write(0, &payload).await.expect("write");
        let cno = hyper.fs_fdatasync().await.expect("fdatasync data-dirty");
        let _ = hyper.fs_release().await;
        cno
    };
    assert!(
        after_fdatasync_cno > baseline_cno,
        "fdatasync with dirty data must bump cno (got {} <= {})",
        after_fdatasync_cno, baseline_cno
    );

    // Verify on reopen that the data made it.
    {
        let mut hyper = Hyper::fs_open(&client, tf.uri(), FileFlags::rdonly())
            .await.expect("reopen ro");
        let stat = hyper.fs_getattr().expect("ga");
        assert_eq!(stat.st_size as usize, payload.len());
        let mut buf = vec![0u8; payload.len()];
        let _ = hyper.fs_read(0, &mut buf).await.expect("read");
        assert_eq!(buf, payload);
        let _ = hyper.fs_release().await;
    }

    tf.cleanup(&client).await;
}

/// POSIX: ctime advances on every metadata-affecting operation.
/// This test exercises write / truncate / chmod / chown and
/// asserts each strictly bumps st_ctime relative to the prior
/// observation. As a control, a read does NOT bump ctime.
#[tokio::test]
#[ignore]
async fn smoke_ctime_bumps_on_metadata_changes() {
    let _ = env_logger::try_init();
    let client = make_client().await;
    let tf = TestFile::new(&client).await;

    // Helper: sleep slightly more than the 1-second granularity of
    // st_ctime (whole seconds field) so each successive bump is
    // observable. Inode timestamps are sub-second internally but
    // libc::stat exposes only sec + nsec; we don't assume nsec is
    // monotonic-strict between same-second events, so we wait.
    let tick = || async {
        tokio::time::sleep(std::time::Duration::from_millis(1100)).await;
    };

    // --- create empty
    let mut hyper = Hyper::fs_open_or_create_with_default_opt(
        &client, tf.uri(), FileFlags::rdwr(), FileMode::default_file(),
    ).await.expect("create");

    let ctime0 = hyper.fs_getattr().expect("ga0").st_ctime;
    tick().await;

    // --- write: bumps mtime + ctime
    let _ = hyper.fs_write(0, &vec![0xAAu8; 100]).await.expect("write");
    let st1 = hyper.fs_getattr().expect("ga1");
    assert!(st1.st_ctime > ctime0,
        "write must bump ctime ({} > {})", st1.st_ctime, ctime0);
    assert_eq!(st1.st_ctime, st1.st_mtime,
        "write should bump mtime and ctime to the same instant");
    tick().await;

    // --- read: does NOT bump ctime (control)
    let mut buf = vec![0u8; 100];
    let _ = hyper.fs_read(0, &mut buf).await.expect("read");
    let st2 = hyper.fs_getattr().expect("ga2");
    assert_eq!(st2.st_ctime, st1.st_ctime,
        "read must NOT bump ctime (got {} != {})", st2.st_ctime, st1.st_ctime);
    tick().await;

    // --- truncate: bumps ctime
    hyper.fs_truncate(50).await.expect("truncate");
    let st3 = hyper.fs_getattr().expect("ga3");
    assert!(st3.st_ctime > st2.st_ctime,
        "truncate must bump ctime ({} > {})", st3.st_ctime, st2.st_ctime);
    tick().await;

    // --- chmod: bumps ctime
    let _ = hyper.fs_chmod(0o600).await.expect("chmod");
    let st4 = hyper.fs_getattr().expect("ga4");
    assert!(st4.st_ctime > st3.st_ctime,
        "chmod must bump ctime ({} > {})", st4.st_ctime, st3.st_ctime);
    // mtime must NOT have advanced (chmod doesn't change content)
    assert_eq!(st4.st_mtime, st3.st_mtime,
        "chmod must not bump mtime (got {} != {})", st4.st_mtime, st3.st_mtime);
    tick().await;

    // --- chown: bumps ctime
    let _ = hyper.fs_chown(1234, 5678).await.expect("chown");
    let st5 = hyper.fs_getattr().expect("ga5");
    assert!(st5.st_ctime > st4.st_ctime,
        "chown must bump ctime ({} > {})", st5.st_ctime, st4.st_ctime);
    assert_eq!(st5.st_mtime, st4.st_mtime,
        "chown must not bump mtime");

    let _ = hyper.fs_release().await;
    tf.cleanup(&client).await;
}

/// POSIX: ctime is not user-settable. fs_setattr ignores
/// stat.st_ctime and bumps ctime to NOW. Verifies that even a
/// caller who passes a stale or fabricated st_ctime does not
/// rewind the field.
#[tokio::test]
#[ignore]
async fn smoke_setattr_overrides_caller_ctime() {
    let _ = env_logger::try_init();
    let client = make_client().await;
    let tf = TestFile::new(&client).await;

    let mut hyper = Hyper::fs_open_or_create_with_default_opt(
        &client, tf.uri(), FileFlags::rdwr(), FileMode::default_file(),
    ).await.expect("create");

    let ctime0 = hyper.fs_getattr().expect("ga0").st_ctime;
    tokio::time::sleep(std::time::Duration::from_millis(1100)).await;

    let mut stat = hyper.fs_getattr().expect("ga1");
    // Caller submits a way-back-in-time ctime.
    stat.st_ctime = 0;
    stat.st_ctime_nsec = 0;
    // Also flip ownership so update_stat has something to do.
    stat.st_uid = 4242;
    let returned = hyper.fs_setattr(&stat).await.expect("setattr");

    assert!(returned.st_ctime > ctime0,
        "setattr must bump ctime to NOW, ignoring caller's value (got {})",
        returned.st_ctime);
    assert_ne!(returned.st_ctime, 0);
    assert_eq!(returned.st_uid, 4242);

    let _ = hyper.fs_release().await;
    tf.cleanup(&client).await;
}

/// `fs_rename` is currently a placeholder that returns
/// `ErrorKind::Unsupported`. Verify the contract: the call
/// fails with the right error kind AND does not perturb the
/// source file.
#[tokio::test]
#[ignore]
async fn smoke_fs_rename_returns_unsupported() {
    let _ = env_logger::try_init();
    let client = make_client().await;
    let tf = TestFile::new(&client).await;

    // Create the source file.
    let payload = vec![0xCDu8; 128];
    {
        let mut hyper = Hyper::fs_open_or_create_with_default_opt(
            &client, tf.uri(), FileFlags::rdwr(), FileMode::default_file(),
        ).await.expect("create");
        let _ = hyper.fs_write(0, &payload).await.expect("write");
        let _ = hyper.fs_release().await.expect("release");
    }

    // Attempt rename. Should fail with Unsupported.
    let dst = format!("{}-renamed-target", tf.uri());
    let res = Hyper::fs_rename(&client, tf.uri(), &dst).await;
    match res {
        Err(e) if e.kind() == std::io::ErrorKind::Unsupported => {}
        Err(e) => panic!("expected Unsupported, got {:?}", e),
        Ok(()) => panic!("fs_rename returned Ok but is not yet implemented"),
    }

    // Source must still be intact and openable.
    let mut hyper = Hyper::fs_open(&client, tf.uri(), FileFlags::rdonly())
        .await.expect("source still openable");
    let stat = hyper.fs_getattr().expect("ga");
    assert_eq!(stat.st_size as usize, payload.len());
    let mut buf = vec![0u8; payload.len()];
    let _ = hyper.fs_read(0, &mut buf).await.expect("read");
    assert_eq!(buf, payload);
    let _ = hyper.fs_release().await;

    tf.cleanup(&client).await;
}

/// SEEK_DATA / SEEK_HOLE over a flushed sparse file.
/// Layout (4 KiB blocks): block 0 = data, block 1 = hole (never
/// written), block 2 = data, blocks 3.. = absent up to size.
#[tokio::test]
#[ignore]
async fn smoke_seek_data_hole_flushed() {
    let _ = env_logger::try_init();
    let client = make_client().await;
    let tf = TestFile::new(&client).await;

    const BLOCK: usize = 4096;

    {
        let mut hyper = Hyper::fs_open_or_create_with_default_opt(
            &client, tf.uri(), FileFlags::rdwr(), FileMode::default_file(),
        ).await.expect("create");
        hyper.fs_write(0, &vec![0xAAu8; BLOCK]).await.expect("write block 0");
        hyper.fs_write(BLOCK * 2, &vec![0xCCu8; BLOCK]).await.expect("write block 2");
        // Extend to 4 blocks: block 3 is a trailing hole.
        hyper.fs_truncate(BLOCK * 4).await.expect("truncate to 16 KiB");
        let _ = hyper.fs_release().await.expect("release");
    }

    let mut hyper = Hyper::fs_open(&client, tf.uri(), FileFlags::rdonly())
        .await.expect("reopen ro");

    // SEEK_DATA from 0 -> 0 (block 0 is data).
    assert_eq!(hyper.fs_seek_data(0).await.expect("sd0"), Some(0));
    // SEEK_HOLE from 0 -> BLOCK (block 1 is the first hole).
    assert_eq!(hyper.fs_seek_hole(0).await.expect("sh0"), Some(BLOCK));
    // SEEK_DATA from inside the hole (block 1) -> BLOCK*2 (block 2).
    assert_eq!(hyper.fs_seek_data(BLOCK).await.expect("sd1"), Some(BLOCK * 2));
    // SEEK_DATA partway into block 0 -> the offset itself.
    assert_eq!(hyper.fs_seek_data(100).await.expect("sd100"), Some(100));
    // SEEK_HOLE from inside block 2 (data) -> BLOCK*3 (trailing hole).
    assert_eq!(hyper.fs_seek_hole(BLOCK * 2 + 10).await.expect("sh2"), Some(BLOCK * 3));
    // SEEK_DATA from the trailing hole -> None (no data to EOF).
    assert_eq!(hyper.fs_seek_data(BLOCK * 3).await.expect("sd3"), None);
    // off == size -> SEEK_HOLE returns size, SEEK_DATA returns None.
    assert_eq!(hyper.fs_seek_hole(BLOCK * 4).await.expect("shEOF"), Some(BLOCK * 4));
    assert_eq!(hyper.fs_seek_data(BLOCK * 4).await.expect("sdEOF"), None);
    // off > size -> both None.
    assert_eq!(hyper.fs_seek_hole(BLOCK * 5).await.expect("sh>"), None);

    let _ = hyper.fs_release().await;
    tf.cleanup(&client).await;
}

/// Regression: SEEK_DATA / SEEK_HOLE must see unflushed writes that
/// live only in the dirty cache (not yet in the bmap). Before the
/// fix, the seek path consulted only the bmap and reported a block
/// that fs_read would happily return as a hole.
#[tokio::test]
#[ignore]
async fn smoke_seek_data_hole_unflushed() {
    let _ = env_logger::try_init();
    let client = make_client().await;
    let tf = TestFile::new(&client).await;

    const BLOCK: usize = 4096;

    let mut hyper = Hyper::fs_open_or_create_with_default_opt(
        &client, tf.uri(), FileFlags::rdwr(), FileMode::default_file(),
    ).await.expect("create");

    // Write block 1 only, do NOT flush. Extend size to 3 blocks so
    // block 0 is a leading hole and block 2 a trailing hole.
    hyper.fs_write(BLOCK, &vec![0xBBu8; BLOCK]).await.expect("write block 1");
    // truncate-extend updates i_size; block 1 stays dirty in cache.
    if hyper.fs_getattr().expect("ga").st_size < (BLOCK * 3) as i64 {
        hyper.fs_truncate(BLOCK * 3).await.expect("extend");
    }

    // SEEK_DATA from 0 must find block 1 (the unflushed write),
    // not skip past it.
    assert_eq!(hyper.fs_seek_data(0).await.expect("sd0"), Some(BLOCK));
    // SEEK_HOLE from 0 -> 0 (block 0 is a hole).
    assert_eq!(hyper.fs_seek_hole(0).await.expect("sh0"), Some(0));
    // SEEK_HOLE from block 1 (data) -> BLOCK*2 (trailing hole).
    assert_eq!(hyper.fs_seek_hole(BLOCK).await.expect("sh1"), Some(BLOCK * 2));

    let _ = hyper.fs_release().await;
    tf.cleanup(&client).await;
}

/// A freshly created, never-written file must report atime, mtime
/// and ctime all ≈ now (POSIX), not epoch 0. Regression for the
/// create path leaving atime/mtime at 0.
#[tokio::test]
#[ignore]
async fn smoke_create_stamps_all_times() {
    let _ = env_logger::try_init();
    let client = make_client().await;
    let tf = TestFile::new(&client).await;

    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64;

    // Create, no write, reopen read-only and stat.
    {
        let mut hyper = Hyper::fs_open_or_create_with_default_opt(
            &client, tf.uri(), FileFlags::rdwr(), FileMode::default_file(),
        ).await.expect("create");
        let _ = hyper.fs_release().await.expect("release");
    }

    let mut hyper = Hyper::fs_open(&client, tf.uri(), FileFlags::rdonly())
        .await.expect("reopen ro");
    let st = hyper.fs_getattr().expect("getattr");

    assert!(st.st_atime > 0, "atime must not be epoch 0");
    assert!(st.st_mtime > 0, "mtime must not be epoch 0");
    assert!(st.st_ctime > 0, "ctime must not be epoch 0");
    // within a generous window of the wall clock at creation
    for (name, t) in [("atime", st.st_atime), ("mtime", st.st_mtime), ("ctime", st.st_ctime)] {
        assert!((t - now).abs() < 120, "{} ({}) should be ≈ now ({})", name, t, now);
    }
    let _ = hyper.fs_release().await;
    tf.cleanup(&client).await;
}

/// Regression: a shrinking truncate must drop CLEAN cached blocks
/// above the new EOF, not just dirty ones. Sequence (all on one
/// open handle):
///
///   write at X -> flush (makes the block clean) -> shrink below X
///   -> grow back above X -> read at X
///
/// The read must see zeros: the bytes at X were discarded by the
/// shrink, and after the grow the region is a hole. Before the fix
/// the clean cached block survived the truncate and the read path
/// (which consults the clean tier first) served the pre-truncate
/// bytes. The persisted state was already correct, so this only
/// reproduced on the handle that did the truncate.
///
/// Found by fsx (xfstests) against a FUSE fs layered on hyperfile.
#[tokio::test]
#[ignore]
async fn smoke_truncate_shrink_drops_clean_cached_blocks() {
    let _ = env_logger::try_init();
    let client = make_client().await;
    let tf = TestFile::new(&client).await;

    const OFF: usize = 200_000;
    const LEN: usize = 4096;

    let mut hyper = Hyper::fs_open_or_create_with_default_opt(
        &client, tf.uri(), FileFlags::rdwr(), FileMode::default_file(),
    ).await.expect("create");

    // 1. write data at OFF
    let n = hyper.fs_write(OFF, &vec![0xABu8; LEN]).await.expect("write");
    assert_eq!(n, LEN);

    // 2. flush — REQUIRED: this moves the block from the dirty list
    //    into the clean cache tier. Without it the block stays dirty
    //    and the old dirty-only cleanup already handled it.
    let _ = hyper.fs_flush().await.expect("flush");

    // 3. shrink below OFF, discarding those bytes
    hyper.fs_truncate(1000).await.expect("shrink to 1000");
    let st = hyper.fs_getattr().expect("ga after shrink");
    assert_eq!(st.st_size as usize, 1000);

    // 4. grow back past OFF; the region is now a hole
    hyper.fs_truncate(300_000).await.expect("grow to 300000");
    let st = hyper.fs_getattr().expect("ga after grow");
    assert_eq!(st.st_size as usize, 300_000);

    // 5. read at OFF must be all zeros, on THIS handle
    let mut buf = vec![0xFFu8; LEN]; // sentinel: must be overwritten with 0
    let n = hyper.fs_read(OFF, &mut buf).await.expect("read");
    assert_eq!(n, LEN);
    let nonzero = buf.iter().filter(|&&b| b != 0).count();
    assert_eq!(nonzero, 0,
        "grown-back region must read as a hole, found {} stale bytes (first={:#x})",
        nonzero, buf.iter().find(|&&b| b != 0).copied().unwrap_or(0));

    // The persisted state must agree after a reopen too.
    let _ = hyper.fs_release().await.expect("release");
    let mut hyper = Hyper::fs_open(&client, tf.uri(), FileFlags::rdonly())
        .await.expect("reopen ro");
    let mut buf = vec![0xFFu8; LEN];
    let _ = hyper.fs_read(OFF, &mut buf).await.expect("read after reopen");
    assert!(buf.iter().all(|&b| b == 0), "reopened handle must also read zeros");
    let _ = hyper.fs_release().await;
    tf.cleanup(&client).await;
}

/// POSIX: every write-side operation on a handle that was not
/// opened for writing must fail with EBADF.
///
/// - `write()`: "[EBADF] The fildes argument is not a valid file
///   descriptor open for writing" is a mandatory ("shall fail")
///   error.
/// - `ftruncate()`: the spec allows "[EBADF] or [EINVAL]" for the
///   same condition; hyperfile uses EBADF so all write-side
///   operations report one errno.
///
/// The errno is checked via `raw_os_error()`, because
/// `std::io::ErrorKind` has no EBADF variant (its `kind()` is the
/// unmatchable `Uncategorized`).
#[tokio::test]
#[ignore]
async fn smoke_rdonly_handle_write_ops_are_ebadf() {
    let _ = env_logger::try_init();
    let client = make_client().await;
    let tf = TestFile::new(&client).await;

    // Create with real content so the reads below have something to
    // return, then reopen read-only.
    {
        let mut hyper = Hyper::fs_open_or_create_with_default_opt(
            &client, tf.uri(), FileFlags::rdwr(), FileMode::default_file(),
        ).await.expect("create");
        let _ = hyper.fs_write(0, &vec![0xAAu8; 4096]).await.expect("write rdwr");
        let _ = hyper.fs_release().await.expect("release");
    }

    let mut hyper = Hyper::fs_open(&client, tf.uri(), FileFlags::rdonly())
        .await.expect("reopen rdonly");

    let expect_ebadf = |res: std::io::Result<usize>, what: &str| {
        let err = res.err().unwrap_or_else(|| panic!("{what} on a read-only handle must fail"));
        assert_eq!(err.raw_os_error(), Some(libc::EBADF),
            "{what} must fail with EBADF, got {:?} (kind {:?})", err.raw_os_error(), err.kind());
    };

    expect_ebadf(hyper.fs_write(0, &[0xBBu8; 8]).await, "fs_write");
    expect_ebadf(hyper.fs_write_zero(0, 8).await, "fs_write_zero");
    // fs_truncate returns Result<()>; adapt to the same helper.
    expect_ebadf(hyper.fs_truncate(0).await.map(|_| 0usize), "fs_truncate");
    expect_ebadf(hyper.fs_truncate(1 << 20).await.map(|_| 0usize), "fs_truncate (extend)");

    // Reads still work, and the file was not modified by any of the
    // rejected operations.
    let mut buf = vec![0u8; 4096];
    let n = hyper.fs_read(0, &mut buf).await.expect("read on rdonly handle");
    assert_eq!(n, 4096);
    assert!(buf.iter().all(|&b| b == 0xAA), "content must be untouched");
    let st = hyper.fs_getattr().expect("getattr");
    assert_eq!(st.st_size, 4096, "size must be untouched by the rejected truncates");

    // Operations POSIX does not gate on write access must still work
    // on a read-only handle: fsync/fdatasync (no-ops here, nothing is
    // dirty) and chmod/chown (governed by ownership, not by the
    // handle's access mode).
    let _ = hyper.fs_flush().await.expect("fs_flush on rdonly handle");
    let _ = hyper.fs_fdatasync().await.expect("fs_fdatasync on rdonly handle");
    let st = hyper.fs_chmod(0o600).await.expect("fs_chmod on rdonly handle");
    assert_eq!(st.st_mode & 0o777, 0o600, "chmod must take effect");

    let _ = hyper.fs_release().await;
    tf.cleanup(&client).await;
}

/// POSIX: a read on a handle not opened for reading must fail with
/// EBADF ("[EBADF] The fildes argument is not a valid file descriptor
/// open for reading", a mandatory error).
#[tokio::test]
#[ignore]
async fn smoke_wronly_handle_read_is_ebadf() {
    let _ = env_logger::try_init();
    let client = make_client().await;
    let tf = TestFile::new(&client).await;

    {
        let mut hyper = Hyper::fs_open_or_create_with_default_opt(
            &client, tf.uri(), FileFlags::rdwr(), FileMode::default_file(),
        ).await.expect("create");
        let _ = hyper.fs_write(0, &vec![0xAAu8; 4096]).await.expect("seed write");
        let _ = hyper.fs_release().await.expect("release");
    }

    let mut hyper = Hyper::fs_open(&client, tf.uri(), FileFlags::wronly())
        .await.expect("reopen wronly");

    let mut buf = vec![0u8; 4096];
    let err = hyper.fs_read(0, &mut buf).await
        .expect_err("read on a write-only handle must fail");
    assert_eq!(err.raw_os_error(), Some(libc::EBADF),
        "read must fail with EBADF, got {:?} (kind {:?})", err.raw_os_error(), err.kind());

    // Writes still work on the same handle.
    let n = hyper.fs_write(0, &vec![0xBBu8; 4096]).await.expect("write on wronly handle");
    assert_eq!(n, 4096);

    let _ = hyper.fs_release().await.expect("release");
    tf.cleanup(&client).await;
}

/// The read guard must not break write-side operations that read
/// internally. On an `O_WRONLY` handle:
///
///   * a partial (sub-block) write needs a read-modify-write of the
///     existing block,
///   * a shrink to a non-block-aligned size needs to read the tail
///     block to zero its remainder.
///
/// Both go through the lower-level `load_data_block_*` helpers rather
/// than `read()`, so both must still work — and produce correct
/// content, verified from a separate read-only handle.
#[tokio::test]
#[ignore]
async fn smoke_wronly_handle_internal_reads_still_work() {
    let _ = env_logger::try_init();
    let client = make_client().await;
    let tf = TestFile::new(&client).await;

    const BLOCK: usize = 4096;

    // Seed two blocks of 0xAA and flush so the data is on S3 (not in
    // this handle's cache) — the read-modify-write below must fetch
    // it back from staging.
    {
        let mut hyper = Hyper::fs_open_or_create_with_default_opt(
            &client, tf.uri(), FileFlags::rdwr(), FileMode::default_file(),
        ).await.expect("create");
        let _ = hyper.fs_write(0, &vec![0xAAu8; BLOCK * 2]).await.expect("seed write");
        let _ = hyper.fs_release().await.expect("release");
    }

    // Write-only handle: partial write in the middle of block 0.
    {
        let mut hyper = Hyper::fs_open(&client, tf.uri(), FileFlags::wronly())
            .await.expect("reopen wronly");
        let n = hyper.fs_write(50, &vec![0xBBu8; 100]).await
            .expect("partial write on wronly handle must work (read-modify-write)");
        assert_eq!(n, 100);
        let _ = hyper.fs_release().await.expect("release");
    }

    // Verify from a read-only handle: the untouched parts of block 0
    // survived the read-modify-write.
    {
        let mut hyper = Hyper::fs_open(&client, tf.uri(), FileFlags::rdonly())
            .await.expect("reopen rdonly");
        let mut buf = vec![0u8; BLOCK * 2];
        let _ = hyper.fs_read(0, &mut buf).await.expect("read");
        assert!(buf[..50].iter().all(|&b| b == 0xAA), "bytes before the patch must be 0xAA");
        assert!(buf[50..150].iter().all(|&b| b == 0xBB), "patched bytes must be 0xBB");
        assert!(buf[150..].iter().all(|&b| b == 0xAA), "bytes after the patch must be 0xAA");
        let _ = hyper.fs_release().await;
    }

    // Write-only handle: shrink to a non-aligned size. This reads the
    // tail block to zero its remainder.
    {
        let mut hyper = Hyper::fs_open(&client, tf.uri(), FileFlags::wronly())
            .await.expect("reopen wronly for truncate");
        hyper.fs_truncate(BLOCK + 1000).await
            .expect("truncate on wronly handle must work");
        let _ = hyper.fs_release().await.expect("release");
    }

    // Verify the shrink kept the retained bytes and the size.
    {
        let mut hyper = Hyper::fs_open(&client, tf.uri(), FileFlags::rdonly())
            .await.expect("reopen rdonly after truncate");
        let st = hyper.fs_getattr().expect("getattr");
        assert_eq!(st.st_size as usize, BLOCK + 1000);
        let mut buf = vec![0u8; BLOCK + 1000];
        let _ = hyper.fs_read(0, &mut buf).await.expect("read after truncate");
        assert!(buf[50..150].iter().all(|&b| b == 0xBB), "patch must survive the truncate");
        assert!(buf[BLOCK..].iter().all(|&b| b == 0xAA), "retained tail must be 0xAA");
        let _ = hyper.fs_release().await;
    }

    tf.cleanup(&client).await;
}

/// `lseek` requires no particular access mode, and neither do the
/// `SEEK_DATA` / `SEEK_HOLE` extensions. They must keep working on a
/// write-only handle even though `read` is rejected.
#[tokio::test]
#[ignore]
async fn smoke_wronly_handle_seek_data_hole_still_work() {
    let _ = env_logger::try_init();
    let client = make_client().await;
    let tf = TestFile::new(&client).await;

    const BLOCK: usize = 4096;
    {
        let mut hyper = Hyper::fs_open_or_create_with_default_opt(
            &client, tf.uri(), FileFlags::rdwr(), FileMode::default_file(),
        ).await.expect("create");
        let _ = hyper.fs_write(0, &vec![0xAAu8; BLOCK]).await.expect("seed write");
        hyper.fs_truncate(BLOCK * 3).await.expect("extend with a trailing hole");
        let _ = hyper.fs_release().await.expect("release");
    }

    let mut hyper = Hyper::fs_open(&client, tf.uri(), FileFlags::wronly())
        .await.expect("reopen wronly");

    assert_eq!(hyper.fs_seek_data(0).await.expect("seek_data on wronly"), Some(0));
    assert_eq!(hyper.fs_seek_hole(0).await.expect("seek_hole on wronly"), Some(BLOCK));

    // ...while read is still rejected on the same handle.
    let mut buf = vec![0u8; 16];
    let err = hyper.fs_read(0, &mut buf).await.expect_err("read must still fail");
    assert_eq!(err.raw_os_error(), Some(libc::EBADF));

    let _ = hyper.fs_release().await;
    tf.cleanup(&client).await;
}

/// Helper: list every object key under a `s3://bucket/prefix` URI.
async fn list_keys_under(client: &aws_sdk_s3::Client, uri: &str) -> Vec<String> {
    let rest = uri.strip_prefix("s3://").expect("s3:// uri");
    let (bucket, prefix) = rest.split_once('/').expect("uri has a key part");
    let mut out = Vec::new();
    let mut stream = client
        .list_objects_v2()
        .bucket(bucket)
        .prefix(format!("{}/", prefix))
        .into_paginator()
        .send();
    while let Some(page) = stream.next().await {
        let page = page.expect("list_objects_v2");
        for obj in page.contents.unwrap_or_default() {
            if let Some(k) = obj.key {
                out.push(k);
            }
        }
    }
    out
}

/// POSIX unlink(2): removing a file that does not exist fails with
/// ENOENT. Backed by a conditional `DeleteObject` (`If-Match: *`) on
/// the inode, which is the authoritative "file exists" marker.
#[tokio::test]
#[ignore]
async fn smoke_unlink_nonexistent_is_enoent() {
    let _ = env_logger::try_init();
    let client = make_client().await;
    let tf = TestFile::new(&client).await;

    // Never created.
    let err = Hyper::fs_unlink(&client, tf.uri()).await
        .expect_err("unlink of a non-existent file must fail");
    assert_eq!(err.kind(), std::io::ErrorKind::NotFound,
        "expected ENOENT, got {:?}", err.kind());

    // Create, then unlink succeeds.
    {
        let mut hyper = Hyper::fs_open_or_create_with_default_opt(
            &client, tf.uri(), FileFlags::rdwr(), FileMode::default_file(),
        ).await.expect("create");
        let _ = hyper.fs_write(0, &vec![0xAAu8; 4096]).await.expect("write");
        let _ = hyper.fs_release().await.expect("release");
    }
    Hyper::fs_unlink(&client, tf.uri()).await.expect("unlink of an existing file");
    assert!(list_keys_under(&client, tf.uri()).await.is_empty(),
        "unlink must remove every object under the prefix");

    // Unlinking again reports ENOENT.
    let err = Hyper::fs_unlink(&client, tf.uri()).await
        .expect_err("second unlink must fail");
    assert_eq!(err.kind(), std::io::ErrorKind::NotFound,
        "expected ENOENT on the second unlink, got {:?}", err.kind());
}

// Not covered here: once the inode is gone, the remaining objects
// under the prefix cannot be reclaimed through `fs_unlink` (or the
// cleaner). See "Known gap: orphaned objects are not reclaimable" in
// docs/posix.md.
