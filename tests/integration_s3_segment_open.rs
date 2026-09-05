//! Segment-summary read (`SegmentReadWrite::open`) integration tests.
//!
//! `open` speculatively reads `SEGMENT_HEADER_FETCH_SIZE` (512 KiB) and then
//! tops up if the summary is larger. That interacts with the object's actual
//! size in ways that were repeatedly wrong:
//!
//!   * a segment object smaller than the fetch size was rejected outright,
//!     because the exact-length `do_get_object` treats a short read as an
//!     error;
//!   * `remain_bytes` was computed before the guard that is its only
//!     consumer, so it underflowed for any summary below 512 KiB — which is
//!     the normal case, since `s_bytes` covers the summary, not the segment.
//!
//! `open` has no caller inside hyperfile (reads go through the block map and
//! the block loaders, which issue exact-length range reads), so nothing else
//! in the test suite covers it; hyperfile-cleaner is the consumer.
//!
//! **Run these under debug assertions too.** The underflow above was
//! invisible under `--release`, where it wrapped and was discarded.
//!
//! ```bash
//! HYPERFILE_TEST_BUCKET=<your-bucket> HYPERFILE_TEST_REGION=<your-region> \
//!     cargo test --test integration_s3_segment_open -- --ignored --test-threads=1
//! ```

#[allow(dead_code)]
mod common;

use common::*;

use hyperfile::file::hyper::Hyper;
use hyperfile::file::flags::FileFlags;
use hyperfile::file::mode::FileMode;
use hyperfile::config::HyperFileRuntimeConfig;
use hyperfile::staging::{config::StagingConfig, s3::S3Staging};
use hyperfile::segment::SegmentReadWrite;

/// Write `payload_len` bytes as a single flush, then read the resulting
/// segment's summary back via `open`.
async fn write_one_segment_then_open(payload_len: usize) {
    let client = make_client().await;
    let tf = TestFile::new(&client).await;

    {
        let mut hyper = Hyper::fs_open_or_create_with_default_opt(
            &client, tf.uri(), FileFlags::rdwr(), FileMode::default_file(),
        ).await.expect("create");
        let n = hyper.fs_write(0, &vec![0xAAu8; payload_len]).await.expect("write");
        assert_eq!(n, payload_len);
        let _ = hyper.fs_flush().await.expect("flush");
        let _ = hyper.fs_release().await.expect("release");
    }

    let staging = S3Staging::from(
        &client,
        StagingConfig::new_s3_uri(tf.uri(), None),
        HyperFileRuntimeConfig::default(),
    ).await.expect("staging");

    let ss = staging.open(hyperfile::SegmentId::new_from_cno(1)).await
        .unwrap_or_else(|e| panic!("open segid 1 for a {payload_len}-byte payload: {e}"));

    // The summary must describe the data we just wrote.
    let block_size = 4096;
    let expect_blocks = (payload_len / block_size) as u32;
    assert_eq!(ss.hdr.s_ndatablk, expect_blocks,
        "summary should list {expect_blocks} data blocks");
    assert!(ss.hdr.s_bytes as usize >= std::mem::size_of_val(&ss.hdr),
        "s_bytes ({}) must cover at least the header", ss.hdr.s_bytes);
    assert_eq!(ss.hdr.s_cno, 1, "segment should report its own segid");

    tf.cleanup(&client).await;
}

/// Segment object far below the 512 KiB speculative fetch size. The
/// speculative read gets a short response, which must be accepted.
#[tokio::test]
#[ignore]
async fn segment_open_smaller_than_fetch_size() {
    let _ = env_logger::try_init();
    write_one_segment_then_open(4096).await;       // 1 data block
    write_one_segment_then_open(8192).await;       // 2 data blocks
}

/// Segment object at and above the fetch size: the speculative read is
/// satisfied in full, and the summary is still far smaller than 512 KiB, so
/// no top-up read is needed.
#[tokio::test]
#[ignore]
async fn segment_open_at_and_above_fetch_size() {
    let _ = env_logger::try_init();
    write_one_segment_then_open(512 * 1024).await;      // exactly the fetch size
    write_one_segment_then_open(1024 * 1024).await;     // above it
    write_one_segment_then_open(16 * 1024 * 1024).await; // well above it
}

/// A container whose format this build cannot represent is refused, not read.
///
/// Containers written before `i_meta_config` was populated carry zero there,
/// which decoded to a root of 0 bytes and blocks of 1 byte — and since the
/// container's config overwrites the caller's, that 1-byte block size then
/// decided how every read was cut up. A consumer opened such a container with
/// 0.6.x and got a device reporting 33280 bytes where 2 GiB had been written.
///
/// The inode object is patched in place rather than a legacy container being
/// checked in, so the test states exactly which byte matters.
#[tokio::test]
#[ignore]
async fn open_refuses_a_container_format_it_cannot_represent() {
    let _ = env_logger::try_init();
    let client = make_client().await;
    let tf = TestFile::new(&client).await;

    // A perfectly good container first.
    {
        let mut h = Hyper::fs_open_or_create_with_default_opt(
            &client, tf.uri(), FileFlags::rdwr(), FileMode::default_file(),
        ).await.expect("create");
        let _ = h.fs_write(0, &vec![0x5Au8; 8192]).await.expect("write");
        let _ = h.fs_flush().await.expect("flush");
        let _ = h.fs_release().await.expect("release");
    }
    // It opens.
    {
        let mut h = Hyper::fs_open(&client, tf.uri(), FileFlags::rdonly()).await
            .expect("a container this build wrote must open");
        let _ = h.fs_release().await;
    }

    // Zero `i_meta_config`, which is what a pre-0.4 container carries. Offset
    // 44 in `InodeRaw`: four u64 timestamps and ino, then three u32 nsec
    // fields.
    let bucket = test_bucket();
    let key = format!("{}/inode", tf.uri().strip_prefix(&format!("s3://{}/", bucket)).expect("uri prefix"));
    let got = client.get_object().bucket(&bucket).key(&key).send().await.expect("get inode");
    let mut bytes = got.body.collect().await.expect("collect").to_vec();
    assert!(bytes.len() >= 48, "inode object is {} bytes", bytes.len());
    assert_ne!(&bytes[44..48], &[0u8; 4], "i_meta_config should be populated before patching");
    bytes[44..48].copy_from_slice(&0u32.to_ne_bytes());
    client.put_object().bucket(&bucket).key(&key)
        .body(bytes.into()).send().await.expect("put patched inode");

    // Now it must refuse, and say what it saw.
    let err = match Hyper::fs_open(&client, tf.uri(), FileFlags::rdonly()).await {
        Ok(_) => panic!("an unrepresentable container format must not open"),
        Err(e) => e,
    };
    assert_eq!(err.kind(), std::io::ErrorKind::InvalidData, "got {:?}: {}", err.kind(), err);
    let msg = format!("{}", err);
    assert!(msg.contains("unrecognised container format"), "message was: {}", msg);
    assert!(msg.contains("0x00000000"), "the raw value must be reported: {}", msg);

    tf.cleanup(&client).await;
}
