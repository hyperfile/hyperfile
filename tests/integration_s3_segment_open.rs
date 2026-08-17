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

    let ss = staging.open(1).await
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
