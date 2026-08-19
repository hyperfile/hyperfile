//! Block borrow API (`fs_block` / `fs_block_mut` / `fs_block_state`).
//!
//! These cover the semantics a block-storage consumer depends on:
//! borrowing the cache's own buffer rather than copying, telling a
//! hole apart from a block of zeros, and modifying a block in place
//! so that the next flush persists it with no write-back call.
//!
//! **Run under debug assertions as well as release.** The cache
//! tiers carry `debug_assert!`s about block lock and dirty state
//! that only fire in a debug build, and the local-disk tier asserts
//! outright that a clean block handed out by `get` was not already
//! locked — the trap that makes the borrow guards' `Drop` load-
//! bearing.
//!
//! ```bash
//! HYPERFILE_TEST_BUCKET=<your-bucket> HYPERFILE_TEST_REGION=<your-region> \
//!     cargo test --test integration_s3_block_api -- --ignored --test-threads=1
//! ```

#[allow(dead_code)]
mod common;

use common::*;

use hyperfile::file::hyper::Hyper;
use hyperfile::file::flags::FileFlags;
use hyperfile::file::mode::FileMode;
use hyperfile::file::block::BlockState;

const BLK: usize = 4096;

async fn open_rdwr(client: &aws_sdk_s3::Client, uri: &str) -> Hyper<'static> {
    Hyper::fs_open_or_create_with_default_opt(
        client, uri, FileFlags::rdwr(), FileMode::default_file(),
    ).await.expect("open rdwr")
}

/// `fs_block` hands back the bytes that were written, and reports
/// the block as mapped.
#[tokio::test]
#[ignore]
async fn block_read_returns_written_data() {
    let _ = env_logger::try_init();
    let client = make_client().await;
    let tf = TestFile::new(&client).await;

    let mut h = open_rdwr(&client, tf.uri()).await;
    // Three blocks with distinguishable contents.
    let mut payload = vec![0u8; 3 * BLK];
    payload[0..BLK].fill(0xA1);
    payload[BLK..2 * BLK].fill(0xB2);
    payload[2 * BLK..].fill(0xC3);
    let _ = h.fs_write(0, &payload).await.expect("write");

    // Before any flush the blocks are dirty in cache; the borrow
    // must serve them from there.
    for (idx, want) in [(0u64, 0xA1u8), (1, 0xB2), (2, 0xC3)] {
        let blk = h.fs_block(idx).await.expect("fs_block").expect("block should be mapped");
        assert_eq!(blk.len(), BLK, "borrow must be exactly one block long");
        assert_eq!(blk.index(), idx);
        assert!(blk.as_slice().iter().all(|b| *b == want),
            "block {idx} should be all {want:#04x} while dirty");
    }
    assert_eq!(h.fs_block_state(1).await.expect("state"), BlockState::Mapped);

    // Flush, then read again: now served from staging or the clean
    // tier rather than the dirty tier.
    let _ = h.fs_flush().await.expect("flush");
    for (idx, want) in [(0u64, 0xA1u8), (1, 0xB2), (2, 0xC3)] {
        let blk = h.fs_block(idx).await.expect("fs_block").expect("block should be mapped");
        assert!(blk.as_slice().iter().all(|b| *b == want),
            "block {idx} should still be all {want:#04x} after flush");
    }

    // Repeated borrows of the same block must work. On the
    // local-disk tier `get` mlocks a clean block and asserts it was
    // not already locked, so this is what makes `BlockRef::drop`'s
    // unlock load-bearing.
    for _ in 0..3 {
        let blk = h.fs_block(1).await.expect("fs_block").expect("mapped");
        assert_eq!(blk.as_slice()[0], 0xB2);
    }

    let _ = h.fs_release().await.expect("release");
    tf.cleanup(&client).await;
}

/// The distinction the byte API cannot make: a hole, an explicit
/// zero block, and real data all read back as zeros through
/// `fs_read`, but `fs_block` and `fs_block_state` tell them apart.
#[tokio::test]
#[ignore]
async fn block_distinguishes_hole_zero_and_data() {
    let _ = env_logger::try_init();
    let client = make_client().await;
    let tf = TestFile::new(&client).await;

    let mut h = open_rdwr(&client, tf.uri()).await;

    // Block 0: real data that happens to be all zeros.
    let _ = h.fs_write(0, &vec![0u8; BLK]).await.expect("write zeros as data");
    // Block 1: left as a hole by writing only block 2.
    // Block 2: real data.
    let _ = h.fs_write(2 * BLK, &vec![0xDDu8; BLK]).await.expect("write block 2");
    // Block 3: explicit zero block.
    let _ = h.fs_write_zero(3 * BLK, BLK).await.expect("write_zero block 3");

    // Data-that-is-zeros is mapped and borrowable.
    let blk0 = h.fs_block(0).await.expect("fs_block 0");
    assert!(blk0.is_some(), "a block of written zeros is data, not a hole");
    assert!(blk0.unwrap().as_slice().iter().all(|b| *b == 0));
    assert_eq!(h.fs_block_state(0).await.expect("state 0"), BlockState::Mapped);

    // The hole is not borrowable and is Unmapped.
    assert!(h.fs_block(1).await.expect("fs_block 1").is_none(),
        "a hole must not yield a borrow");
    assert_eq!(h.fs_block_state(1).await.expect("state 1"), BlockState::Unmapped);

    // Real data.
    assert_eq!(h.fs_block_state(2).await.expect("state 2"), BlockState::Mapped);
    assert_eq!(h.fs_block(2).await.expect("fs_block 2").expect("mapped").as_slice()[0], 0xDD);

    // The explicit zero block is also not borrowable, but is
    // distinguishable from the hole.
    assert!(h.fs_block(3).await.expect("fs_block 3").is_none(),
        "an explicit zero block has no data to borrow");
    assert_eq!(h.fs_block_state(3).await.expect("state 3"), BlockState::Zero);

    // And through the byte API all four are indistinguishable.
    let mut buf = vec![0xFFu8; BLK];
    let _ = h.fs_read(0, &mut buf).await.expect("read 0");
    assert!(buf.iter().all(|b| *b == 0));
    let mut buf = vec![0xFFu8; BLK];
    let _ = h.fs_read(BLK, &mut buf).await.expect("read 1");
    assert!(buf.iter().all(|b| *b == 0), "hole reads as zeros");
    let mut buf = vec![0xFFu8; BLK];
    let _ = h.fs_read(3 * BLK, &mut buf).await.expect("read 3");
    assert!(buf.iter().all(|b| *b == 0), "zero block reads as zeros");

    let _ = h.fs_release().await.expect("release");
    tf.cleanup(&client).await;
}

/// In-place modification through `fs_block_mut` is persisted by the
/// next flush with no write-back call, and is visible to a fresh
/// open.
#[tokio::test]
#[ignore]
async fn block_mut_modifies_in_place_and_persists() {
    let _ = env_logger::try_init();
    let client = make_client().await;
    let tf = TestFile::new(&client).await;

    {
        let mut h = open_rdwr(&client, tf.uri()).await;
        let _ = h.fs_write(0, &vec![0x11u8; 2 * BLK]).await.expect("write");
        let _ = h.fs_flush().await.expect("flush");
        let _ = h.fs_release().await.expect("release");
    }

    // Re-open so the blocks come from staging, not cache.
    {
        let mut h = open_rdwr(&client, tf.uri()).await;
        {
            let mut blk = h.fs_block_mut(1, false).await.expect("fs_block_mut")
                .expect("block 1 is mapped");
            assert_eq!(blk.len(), BLK);
            // Loaded content must be what was persisted.
            assert!(blk.as_slice().iter().all(|b| *b == 0x11),
                "block_mut must load existing content, not hand back a blank buffer");
            // Modify a slice of it in place.
            blk.as_mut_slice()[0..4].copy_from_slice(&[0xDE, 0xAD, 0xBE, 0xEF]);
            blk.as_mut_slice()[BLK - 1] = 0x99;
        }
        // No write-back call. Just flush.
        let _ = h.fs_flush().await.expect("flush");
        let _ = h.fs_release().await.expect("release");
    }

    // Verify through the byte API from a fresh open.
    {
        let mut h = open_rdwr(&client, tf.uri()).await;
        let mut buf = vec![0u8; BLK];
        let _ = h.fs_read(BLK, &mut buf).await.expect("read block 1");
        assert_eq!(&buf[0..4], &[0xDE, 0xAD, 0xBE, 0xEF],
            "in-place modification was not persisted");
        assert_eq!(buf[BLK - 1], 0x99);
        assert!(buf[4..BLK - 1].iter().all(|b| *b == 0x11),
            "the rest of the block must be untouched");
        // Block 0 must be unaffected.
        let mut buf0 = vec![0u8; BLK];
        let _ = h.fs_read(0, &mut buf0).await.expect("read block 0");
        assert!(buf0.iter().all(|b| *b == 0x11));
        let _ = h.fs_release().await.expect("release");
    }

    tf.cleanup(&client).await;
}

/// `create` controls whether a block with no data is materialized.
/// A created block must be zero-filled — the local-disk tier's own
/// new-dirty-block path hands out an unzeroed view of its backing
/// file, so this asserts the API does not use it.
#[tokio::test]
#[ignore]
async fn block_mut_create_semantics() {
    let _ = env_logger::try_init();
    let client = make_client().await;
    let tf = TestFile::new(&client).await;

    let mut h = open_rdwr(&client, tf.uri()).await;
    // Establish a file with some size so we are not only testing
    // the empty-file case.
    let _ = h.fs_write(0, &vec![0x77u8; BLK]).await.expect("write");
    let _ = h.fs_flush().await.expect("flush");

    // create = false on a hole yields nothing.
    assert!(h.fs_block_mut(5, false).await.expect("block_mut no-create").is_none(),
        "create=false must not materialize a hole");
    assert_eq!(h.fs_block_state(5).await.expect("state"), BlockState::Unmapped,
        "a rejected create must not have touched the bmap");

    // create = true materializes zeros.
    {
        let mut blk = h.fs_block_mut(5, true).await.expect("block_mut create")
            .expect("create=true must yield a block");
        assert!(blk.as_slice().iter().all(|b| *b == 0),
            "a created block must be zero-filled");
        blk.as_mut_slice()[0..3].copy_from_slice(&[1, 2, 3]);
    }
    assert_eq!(h.fs_block_state(5).await.expect("state after create"), BlockState::Mapped);

    let _ = h.fs_flush().await.expect("flush");
    let _ = h.fs_release().await.expect("release");

    // The created block survives a round trip, zeros and all.
    let mut h = open_rdwr(&client, tf.uri()).await;
    let blk = h.fs_block(5).await.expect("fs_block").expect("created block is mapped");
    assert_eq!(&blk.as_slice()[0..3], &[1, 2, 3]);
    assert!(blk.as_slice()[3..].iter().all(|b| *b == 0),
        "the untouched remainder of a created block must be zeros");
    drop(blk);
    let _ = h.fs_release().await.expect("release");

    tf.cleanup(&client).await;
}

/// Borrowing the same block repeatedly inside one flush window must
/// produce exactly one new version, matching `fs_write`.
#[tokio::test]
#[ignore]
async fn block_mut_repeated_borrows_make_one_version() {
    let _ = env_logger::try_init();
    let client = make_client().await;
    let tf = TestFile::new(&client).await;

    let mut h = open_rdwr(&client, tf.uri()).await;
    let _ = h.fs_write(0, &vec![0u8; BLK]).await.expect("write");
    let _ = h.fs_flush().await.expect("flush");
    let before = h.fs_last_cno();

    // Ten separate borrows of the same block, each writing one byte.
    for i in 0..10u8 {
        let mut blk = h.fs_block_mut(0, false).await.expect("block_mut").expect("mapped");
        blk.as_mut_slice()[i as usize] = i + 1;
    }
    let _ = h.fs_flush().await.expect("flush");
    let after = h.fs_last_cno();

    assert_eq!(after, before + 1,
        "ten borrows in one flush window produced {} checkpoints, expected 1",
        after - before);

    // All ten modifications are present.
    let blk = h.fs_block(0).await.expect("fs_block").expect("mapped");
    for i in 0..10u8 {
        assert_eq!(blk.as_slice()[i as usize], i + 1, "byte {i} lost");
    }
    drop(blk);

    let _ = h.fs_release().await.expect("release");
    tf.cleanup(&client).await;
}

/// `fs_block_mut` does not move EOF. The block is still durable, but
/// it is reachable only through the block API — `fs_read` stops at
/// `i_size`. `st_blocks` does account for it.
#[tokio::test]
#[ignore]
async fn block_mut_leaves_i_size_alone() {
    let _ = env_logger::try_init();
    let client = make_client().await;
    let tf = TestFile::new(&client).await;

    let mut h = open_rdwr(&client, tf.uri()).await;
    let _ = h.fs_write(0, &vec![0x55u8; BLK]).await.expect("write");
    let _ = h.fs_flush().await.expect("flush");

    let size_before = h.fs_getattr().expect("getattr").st_size;
    let blocks_before = h.fs_getattr().expect("getattr").st_blocks;
    assert_eq!(size_before, BLK as i64);

    // Dirty a block far above EOF.
    {
        let mut blk = h.fs_block_mut(100, true).await.expect("block_mut").expect("created");
        blk.as_mut_slice().fill(0xEE);
    }
    let _ = h.fs_flush().await.expect("flush");

    let attr = h.fs_getattr().expect("getattr");
    assert_eq!(attr.st_size, size_before,
        "fs_block_mut must not change i_size");
    assert!(attr.st_blocks > blocks_before,
        "i_blocks should account for the new block: {} -> {}", blocks_before, attr.st_blocks);

    // Unreachable through the byte API, because read stops at EOF.
    let mut buf = vec![0u8; BLK];
    let n = h.fs_read(100 * BLK, &mut buf).await.expect("read past EOF");
    assert_eq!(n, 0, "read past i_size must return 0");

    let _ = h.fs_release().await.expect("release");

    // But durable and reachable through the block API after reopen.
    let mut h = open_rdwr(&client, tf.uri()).await;
    let blk = h.fs_block(100).await.expect("fs_block").expect("block above EOF is durable");
    assert!(blk.as_slice().iter().all(|b| *b == 0xEE));
    drop(blk);
    let _ = h.fs_release().await.expect("release");

    tf.cleanup(&client).await;
}

/// Access mode is enforced on both borrows, the same as the byte API.
#[tokio::test]
#[ignore]
async fn block_api_enforces_access_mode() {
    let _ = env_logger::try_init();
    let client = make_client().await;
    let tf = TestFile::new(&client).await;

    {
        let mut h = open_rdwr(&client, tf.uri()).await;
        let _ = h.fs_write(0, &vec![0x33u8; BLK]).await.expect("write");
        let _ = h.fs_flush().await.expect("flush");
        let _ = h.fs_release().await.expect("release");
    }

    // O_RDONLY: reads borrow, writes are EBADF.
    {
        let mut h = Hyper::fs_open(&client, tf.uri(), FileFlags::rdonly()).await.expect("open rdonly");
        assert!(h.fs_block(0).await.expect("read borrow on rdonly").is_some());
        let e = h.fs_block_mut(0, false).await.expect_err("write borrow on rdonly must fail");
        assert_eq!(e.raw_os_error(), Some(libc::EBADF), "expected EBADF, got {e}");
        let _ = h.fs_release().await.expect("release");
    }

    // O_WRONLY: writes borrow, reads are EBADF.
    {
        let mut h = Hyper::fs_open(&client, tf.uri(), FileFlags::wronly()).await.expect("open wronly");
        let e = h.fs_block(0).await.expect_err("read borrow on wronly must fail");
        assert_eq!(e.raw_os_error(), Some(libc::EBADF), "expected EBADF, got {e}");
        let e = h.fs_block_state(0).await.expect_err("state query needs read access");
        assert_eq!(e.raw_os_error(), Some(libc::EBADF), "expected EBADF, got {e}");
        assert!(h.fs_block_mut(0, false).await.expect("write borrow on wronly").is_some());
        let _ = h.fs_flush().await.expect("flush");
        let _ = h.fs_release().await.expect("release");
    }

    tf.cleanup(&client).await;
}

// --- cache tier coverage ---
//
// The tests above run on the default in-memory data cache. The two
// configurations below are where the borrow API does materially
// different work, so they get the same round trip:
//
//   * local-disk cache: clean blocks are views into a backing file
//     rather than heap allocations, `Cache::get` mlocks the block it
//     hands out and asserts it was not already locked, and eviction
//     punches a hole. `BlockRef::drop`'s unlock and `insert_clean`'s
//     copy-into-the-file both only matter here.
//   * disabled data cache (`data_cache_blocks = 0`, which `O_DIRECT`
//     without `wal` forces): there is no cached buffer to borrow at
//     all, so `fs_block` has to fall back to owning the block it
//     loaded.

use hyperfile::config::{HyperFileConfigBuilder, HyperFileRuntimeConfig};
use hyperfile::data_cache::config::HyperFileDataCacheConfig;
use hyperfile::staging::config::StagingConfig;

/// Exercise a read-modify-write round trip under a specific data
/// cache configuration.
///
/// The file is created and populated with the default cache first,
/// then reopened with the configuration under test. That makes the
/// borrowed blocks come from staging rather than from the dirty tier,
/// which is the interesting path, and it avoids an unrelated defect:
/// the local-disk cache mmaps the file's size at open, and
/// `mmap(len=0)` is `EINVAL`, so that tier cannot create an empty
/// file.
///
/// `create_idx` is the index used for the create-in-a-hole case. It
/// must be inside `i_size` for the local-disk tier, which addresses
/// cached blocks as offsets into a mapping sized from `i_size` and
/// cannot grow that mapping in place. Blocks above EOF are covered
/// by `block_mut_leaves_i_size_alone` on the default cache.
async fn round_trip_with_cache(
    cache: HyperFileDataCacheConfig,
    data_cache_blocks: usize,
    create_idx: u64,
    label: &str,
) {
    let client = make_client().await;
    let tf = TestFile::new(&client).await;

    // Seed blocks 0..4 and 12..16, leaving 4..12 as holes, so that
    // `create_idx` can name a hole that still sits inside i_size.
    {
        let mut h = open_rdwr(&client, tf.uri()).await;
        let _ = h.fs_write(0, &vec![0x11u8; 4 * BLK]).await.expect("seed write low");
        let _ = h.fs_write(12 * BLK, &vec![0x22u8; 4 * BLK]).await.expect("seed write high");
        let _ = h.fs_flush().await.expect("seed flush");
        assert_eq!(h.fs_getattr().expect("getattr").st_size, 16 * BLK as i64);
        let _ = h.fs_release().await.expect("seed release");
    }

    let mut runtime = HyperFileRuntimeConfig::default();
    runtime.data_cache_blocks = data_cache_blocks;
    let staging = StagingConfig::new_s3_uri(tf.uri(), None);
    let config = HyperFileConfigBuilder::new()
        .with_staging_config(&staging)
        .with_data_cache_config(&cache)
        .with_runtime_config(&runtime)
        .build();

    {
        let mut h = Hyper::fs_open_or_create_with_config(
            &client, config.clone(), FileFlags::rdwr(), FileMode::default_file(),
        ).await.unwrap_or_else(|e| panic!("{label}: open: {e}"));

        // Read borrow, repeatedly, on blocks that are clean and
        // resident only in staging. Each `fs_block` on the local-disk
        // tier mlocks the block it caches and asserts it was not
        // already locked, so this is what makes `BlockRef::drop`'s
        // unlock load-bearing.
        for _ in 0..3 {
            for idx in 0..4u64 {
                let blk = h.fs_block(idx).await
                    .unwrap_or_else(|e| panic!("{label}: fs_block({idx}): {e}"))
                    .unwrap_or_else(|| panic!("{label}: block {idx} should be mapped"));
                assert!(blk.as_slice().iter().all(|b| *b == 0x11),
                    "{label}: block {idx} content wrong");
            }
        }

        // Holes stay holes.
        assert!(h.fs_block(6).await.expect("fs_block hole").is_none(),
            "{label}: block 6 should be a hole");

        // Write borrow on a clean block: promotes out of the clean
        // tier into the dirty tier.
        {
            let mut blk = h.fs_block_mut(2, false).await
                .unwrap_or_else(|e| panic!("{label}: fs_block_mut: {e}"))
                .unwrap_or_else(|| panic!("{label}: block 2 should be mapped"));
            assert!(blk.as_slice().iter().all(|b| *b == 0x11),
                "{label}: block_mut loaded wrong content");
            blk.as_mut_slice()[0..2].copy_from_slice(&[0xAB, 0xCD]);
        }

        // Create a block in a hole; must be zeros on every tier.
        {
            let mut blk = h.fs_block_mut(create_idx, true).await
                .unwrap_or_else(|e| panic!("{label}: fs_block_mut create: {e}"))
                .unwrap_or_else(|| panic!("{label}: create=true must yield a block"));
            assert!(blk.as_slice().iter().all(|b| *b == 0),
                "{label}: created block is not zero-filled");
            blk.as_mut_slice()[0] = 0x5A;
        }

        let _ = h.fs_flush().await.expect("flush");
        let _ = h.fs_release().await.expect("release");
    }

    // Verify from a fresh open with the default cache.
    {
        let mut h = open_rdwr(&client, tf.uri()).await;
        let mut buf = vec![0u8; BLK];
        let _ = h.fs_read(2 * BLK, &mut buf).await.expect("read");
        assert_eq!(&buf[0..2], &[0xAB, 0xCD], "{label}: modification not persisted");
        assert!(buf[2..].iter().all(|b| *b == 0x11), "{label}: block 2 remainder damaged");

        let blk = h.fs_block(create_idx).await.expect("fs_block created").expect("created block persisted");
        assert_eq!(blk.as_slice()[0], 0x5A, "{label}: created block byte lost");
        assert!(blk.as_slice()[1..].iter().all(|b| *b == 0),
            "{label}: created block remainder is not zeros");
        drop(blk);
        let _ = h.fs_release().await.expect("release");
    }

    tf.cleanup(&client).await;
}

#[tokio::test]
#[ignore]
async fn block_api_on_local_disk_cache() {
    let _ = env_logger::try_init();
    let dir = format!("/tmp/hyperfile-block-api-test-{}", std::process::id());
    std::fs::create_dir_all(&dir).expect("create cache dir");
    round_trip_with_cache(
        HyperFileDataCacheConfig::new_local_disk(Some(&dir), None),
        64,
        // Inside i_size: this tier cannot address a block past the
        // end of its mapping, and cannot grow the mapping in place.
        6,
        "local-disk cache",
    ).await;
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
#[ignore]
async fn block_api_with_data_cache_disabled() {
    let _ = env_logger::try_init();
    // data_cache_blocks = 0 is what O_DIRECT without wal forces.
    // `fs_block` cannot borrow from a cache that holds nothing, so
    // this covers the owned fallback.
    round_trip_with_cache(HyperFileDataCacheConfig::new_mem(), 0, 20, "disabled cache").await;
}
