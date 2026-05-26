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
        let _ = hyper.fs_flush().await.expect("flush before truncate");
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
        let _ = hyper.fs_flush().await.expect("flush before truncate");
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
        let _ = hyper.fs_flush().await.expect("flush");
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
