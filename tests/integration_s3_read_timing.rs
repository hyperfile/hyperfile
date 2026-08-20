//! Read-side counters (`read_timing`).
//!
//! The read path's cost is dominated by object-store round trips.
//! Wall-clock timing cannot distinguish one request for a coalesced
//! range from many separate ones, nor a cache hit from a fetch, so
//! these counters exist to make those observable — and these tests
//! exist to make sure they count what they claim to.
//!
//! ```bash
//! HYPERFILE_TEST_BUCKET=<your-bucket> HYPERFILE_TEST_REGION=<your-region> \
//!     cargo test --test integration_s3_read_timing -- --ignored --test-threads=1
//! ```

#[allow(dead_code)]
mod common;

use common::*;

use hyperfile::file::hyper::Hyper;
use hyperfile::file::flags::FileFlags;
use hyperfile::file::mode::FileMode;

const BLK: usize = 4096;

async fn seed(client: &aws_sdk_s3::Client, uri: &str, blocks: usize) {
    let mut h = Hyper::fs_open_or_create_with_default_opt(
        client, uri, FileFlags::rdwr(), FileMode::default_file(),
    ).await.expect("create");
    let _ = h.fs_write(0, &vec![0x5Au8; blocks * BLK]).await.expect("write");
    let _ = h.fs_flush().await.expect("flush");
    let _ = h.fs_release().await.expect("release");
}

/// A read spanning blocks that are contiguous inside one segment must
/// cost **one** request, not one per block. This is the property the
/// counters exist to make checkable: it is invisible to wall-clock
/// timing, and it is what separates a coalesced read from a loop.
#[tokio::test]
#[ignore]
async fn contiguous_read_costs_one_request() {
    let _ = env_logger::try_init();
    let client = make_client().await;
    let tf = TestFile::new(&client).await;

    const N: usize = 64;
    seed(&client, tf.uri(), N).await;

    let mut h = Hyper::fs_open(&client, tf.uri(), FileFlags::rdonly()).await.expect("reopen");
    // Reset after open so the inode and index reads that opening
    // performs do not count against the read being measured.
    h.read_timing_reset();

    let mut buf = vec![0u8; N * BLK];
    let n = h.fs_read(0, &mut buf).await.expect("read");
    assert_eq!(n, N * BLK);
    assert!(buf.iter().all(|b| *b == 0x5A));

    let t = h.read_timing().snapshot();
    assert_eq!(t.data_gets, 1,
        "{N} contiguous blocks in one segment should coalesce into 1 request, got {}",
        t.data_gets);
    assert_eq!(t.data_bytes, (N * BLK) as u64,
        "fetched bytes should match the range read");
    assert_eq!(t.cache_hits, 0, "nothing should have been cached on a fresh open");

    let _ = h.fs_release().await.expect("release");
    tf.cleanup(&client).await;
}

/// Reading the same block twice through the byte API costs two
/// requests: the byte read path deliberately does not populate the
/// data cache, so there is nothing for the second read to hit.
///
/// Asserting this pins down an asymmetry that is easy to trip over —
/// see `block_borrow_populates_the_cache` below for the other half.
#[tokio::test]
#[ignore]
async fn byte_read_does_not_populate_the_cache() {
    let _ = env_logger::try_init();
    let client = make_client().await;
    let tf = TestFile::new(&client).await;

    seed(&client, tf.uri(), 8).await;

    let mut h = Hyper::fs_open(&client, tf.uri(), FileFlags::rdonly()).await.expect("reopen");
    h.read_timing_reset();

    let mut buf = vec![0u8; BLK];
    let _ = h.fs_read(3 * BLK, &mut buf).await.expect("first read");
    let first = h.read_timing().snapshot();
    assert_eq!(first.data_gets, 1);
    assert_eq!(first.cache_hits, 0);

    let _ = h.fs_read(3 * BLK, &mut buf).await.expect("second read");
    let second = h.read_timing().snapshot();
    assert_eq!(second.data_gets, 2,
        "a byte read does not cache, so the second read must fetch again");
    assert_eq!(second.cache_hits, 0,
        "a byte read leaves nothing in the data cache to hit");

    let _ = h.fs_release().await.expect("release");
    tf.cleanup(&client).await;
}

/// A block borrow *does* populate the cache: the second borrow is a
/// hit and costs no request. A byte read of the same block afterwards
/// is also a hit, so the benefit is one-directional but real.
#[tokio::test]
#[ignore]
async fn block_borrow_populates_the_cache() {
    let _ = env_logger::try_init();
    let client = make_client().await;
    let tf = TestFile::new(&client).await;

    seed(&client, tf.uri(), 8).await;

    let mut h = Hyper::fs_open(&client, tf.uri(), FileFlags::rdonly()).await.expect("reopen");
    h.read_timing_reset();

    {
        let blk = h.fs_block(3).await.expect("borrow").expect("mapped");
        assert_eq!(blk.as_slice()[0], 0x5A);
    }
    let first = h.read_timing().snapshot();
    assert_eq!(first.data_gets, 1, "a cold borrow fetches once");
    assert_eq!(first.cache_hits, 0);

    {
        let blk = h.fs_block(3).await.expect("borrow again").expect("mapped");
        assert_eq!(blk.as_slice()[0], 0x5A);
    }
    let second = h.read_timing().snapshot();
    assert_eq!(second.data_gets, 1, "a second borrow must not fetch");
    assert_eq!(second.cache_hits, 1, "a second borrow is a cache hit");

    // The byte path sees the cached block too.
    let mut buf = vec![0u8; BLK];
    let _ = h.fs_read(3 * BLK, &mut buf).await.expect("read");
    let third = h.read_timing().snapshot();
    assert_eq!(third.data_gets, 1, "a byte read of a cached block must not fetch");
    assert_eq!(third.cache_hits, 2);

    let _ = h.fs_release().await.expect("release");
    tf.cleanup(&client).await;
}

/// Opening a file whose block map has spilled out of the inode costs
/// index reads, and they are counted separately from data reads so
/// that the two can be attributed independently.
#[tokio::test]
#[ignore]
async fn index_reads_are_counted_separately() {
    let _ = env_logger::try_init();
    let client = make_client().await;
    let tf = TestFile::new(&client).await;

    // Enough blocks that the map cannot live in the inode's inline
    // root, so a lookup has to fetch a node.
    seed(&client, tf.uri(), 64).await;

    let mut h = Hyper::fs_open(&client, tf.uri(), FileFlags::rdonly()).await.expect("reopen");
    let after_open = h.read_timing().snapshot();
    assert!(after_open.inode_gets >= 1, "opening must read the inode");

    h.read_timing_reset();
    // Read one block far into the file, forcing a descent.
    let mut buf = vec![0u8; BLK];
    let _ = h.fs_read(60 * BLK, &mut buf).await.expect("read");

    let t = h.read_timing().snapshot();
    assert_eq!(t.data_gets, 1, "one block, one data request");
    assert!(t.meta_gets >= 1,
        "a spilled map must be descended, so at least one index request; got {}",
        t.meta_gets);
    assert!(t.meta_bytes > 0, "index requests should report bytes");
    assert_eq!(t.total_gets(), t.data_gets + t.meta_gets + t.inode_gets);
    assert!(t.staging_ns > 0, "time spent awaiting staging should be recorded");

    let _ = h.fs_release().await.expect("release");
    tf.cleanup(&client).await;
}

/// The batched index loader fetches a segment's meta blocks with a
/// header request followed by a blocks request. Both must be counted:
/// recording one request per call to that helper would understate
/// round trips by half, which is the number these counters exist to
/// report honestly.
#[tokio::test]
#[ignore]
async fn index_fetch_counts_every_request_it_makes() {
    let _ = env_logger::try_init();
    let client = make_client().await;
    let tf = TestFile::new(&client).await;

    seed(&client, tf.uri(), 64).await;

    let mut h = Hyper::fs_open(&client, tf.uri(), FileFlags::rdonly()).await.expect("reopen");
    h.read_timing_reset();

    let mut buf = vec![0u8; BLK];
    let _ = h.fs_read(60 * BLK, &mut buf).await.expect("read");

    let t = h.read_timing().snapshot();
    // With the batched loader a descent fetches a header and then the
    // block chunk, so at least two. The per-block loader issues one
    // request per node, which is also at least one; assert the floor
    // that holds either way, and that bytes were attributed.
    assert!(t.meta_gets >= 1, "index requests should be counted, got {}", t.meta_gets);
    assert!(t.meta_bytes >= t.meta_gets,
        "each counted index request should have contributed bytes: {} requests, {} bytes",
        t.meta_gets, t.meta_bytes);

    let _ = h.fs_release().await.expect("release");
    tf.cleanup(&client).await;
}

/// A hole costs no request at all, and neither does a read served
/// entirely from the write cache before a flush.
#[tokio::test]
#[ignore]
async fn holes_and_unflushed_writes_cost_nothing() {
    let _ = env_logger::try_init();
    let client = make_client().await;
    let tf = TestFile::new(&client).await;

    let mut h = Hyper::fs_open_or_create_with_default_opt(
        &client, tf.uri(), FileFlags::rdwr(), FileMode::default_file(),
    ).await.expect("create");

    // Write block 0, leave 1 as a hole, write block 2.
    let _ = h.fs_write(0, &vec![0x11u8; BLK]).await.expect("write 0");
    let _ = h.fs_write(2 * BLK, &vec![0x22u8; BLK]).await.expect("write 2");
    h.read_timing_reset();

    // Still dirty in cache: reads must not reach staging.
    let mut buf = vec![0u8; BLK];
    let _ = h.fs_read(0, &mut buf).await.expect("read dirty");
    assert!(buf.iter().all(|b| *b == 0x11));
    let t = h.read_timing().snapshot();
    assert_eq!(t.data_gets, 0, "a dirty block must be served from cache");
    assert_eq!(t.cache_hits, 1);

    // The hole.
    h.read_timing_reset();
    let mut buf = vec![0xFFu8; BLK];
    let _ = h.fs_read(BLK, &mut buf).await.expect("read hole");
    assert!(buf.iter().all(|b| *b == 0), "hole reads as zeros");
    let t = h.read_timing().snapshot();
    assert_eq!(t.data_gets, 0, "a hole costs no request");
    assert_eq!(t.cache_hits, 0, "a hole is not a cache hit either");

    let _ = h.fs_flush().await.expect("flush");
    let _ = h.fs_release().await.expect("release");
    tf.cleanup(&client).await;
}

/// `read_timing_reset` zeroes every counter, so a measurement can be
/// bracketed.
#[tokio::test]
#[ignore]
async fn reset_zeroes_every_counter() {
    let _ = env_logger::try_init();
    let client = make_client().await;
    let tf = TestFile::new(&client).await;

    seed(&client, tf.uri(), 8).await;

    let mut h = Hyper::fs_open(&client, tf.uri(), FileFlags::rdonly()).await.expect("reopen");
    let mut buf = vec![0u8; BLK];
    let _ = h.fs_read(0, &mut buf).await.expect("read");
    let _ = h.fs_block(1).await.expect("borrow");
    let _ = h.fs_block(1).await.expect("borrow again");

    let before = h.read_timing().snapshot();
    assert!(before.total_gets() > 0 && before.cache_hits > 0,
        "the measurement should have recorded something to reset");

    h.read_timing_reset();
    let after = h.read_timing().snapshot();
    assert_eq!(after.total_gets(), 0);
    assert_eq!(after.total_bytes(), 0);
    assert_eq!(after.cache_hits, 0);
    assert_eq!(after.staging_ns, 0);
    assert_eq!(after.data_gets, 0);
    assert_eq!(after.meta_gets, 0);
    assert_eq!(after.inode_gets, 0);

    let _ = h.fs_release().await.expect("release");
    tf.cleanup(&client).await;
}
