//! Local-disk **data** cache (`HyperFileDataCacheConfig::LocalDisk`).
//!
//! This tier keeps blocks in a memory-mapped file. It used to map the
//! file's own address space and address a cached block as
//! `addr + blk_idx * data_block_size`, which failed in four distinct
//! ways, none of them covered by any other suite because no other
//! suite selects this tier:
//!
//!   * a newly created file has size zero, and `mmap` rejects a zero
//!     length, so the tier could not be used to create a file at all;
//!   * a write past the current EOF minted a view outside the
//!     mapping and wrote through it — memory corruption, observed as
//!     `SIGSEGV` and as loader assertion failures;
//!   * the mapping could not grow, since `mremap` was called without
//!     `MREMAP_MAYMOVE` and the result asserted to be unmoved;
//!   * a sparse file with a large address space could not be mapped,
//!     because the mapping had to span the address space rather than
//!     the resident blocks.
//!
//! It is now a fixed pool of block-sized slots, so a block index is
//! never used as an offset.
//!
//! Separately, `close` used to `libc::close` a descriptor owned by a
//! `std::fs::File`, from both `shutdown` and `Drop`, which aborted
//! the process on release.
//!
//! ```bash
//! HYPERFILE_TEST_BUCKET=<your-bucket> HYPERFILE_TEST_REGION=<your-region> \
//!     cargo test --test integration_s3_local_disk_cache -- --ignored --test-threads=1
//! ```

#[allow(dead_code)]
mod common;

use common::*;

use hyperfile::file::hyper::Hyper;
use hyperfile::file::flags::FileFlags;
use hyperfile::file::mode::FileMode;
use hyperfile::config::{HyperFileConfig, HyperFileConfigBuilder, HyperFileRuntimeConfig};
use hyperfile::data_cache::config::HyperFileDataCacheConfig;
use hyperfile::staging::config::StagingConfig;

const BLK: usize = 4096;

/// A scratch cache directory, removed on drop.
struct CacheDir(String);

impl CacheDir {
    fn new(tag: &str) -> Self {
        let path = format!("/tmp/hyperfile-ldc-test-{}-{}", std::process::id(), tag);
        std::fs::create_dir_all(&path).expect("create cache dir");
        Self(path)
    }
}

impl Drop for CacheDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn local_disk_config(uri: &str, dir: &CacheDir, data_cache_blocks: usize) -> HyperFileConfig {
    let mut runtime = HyperFileRuntimeConfig::default();
    runtime.data_cache_blocks = data_cache_blocks;
    let staging = StagingConfig::new_s3_uri(uri, None);
    HyperFileConfigBuilder::new()
        .with_staging_config(&staging)
        .with_data_cache_config(&HyperFileDataCacheConfig::new_local_disk(Some(&dir.0), None))
        .with_runtime_config(&runtime)
        .build()
}

/// Creating a file on this tier used to fail: the cache was sized
/// from `i_size`, which is zero for a new file, and `mmap` rejects a
/// zero length (`EINVAL`).
#[tokio::test]
#[ignore]
async fn create_empty_file_on_local_disk_cache() {
    let _ = env_logger::try_init();
    let client = make_client().await;
    let dir = CacheDir::new("create");
    let tf = TestFile::new(&client).await;

    let mut h = Hyper::fs_open_or_create_with_config(
        &client, local_disk_config(tf.uri(), &dir, 64), FileFlags::rdwr(), FileMode::default_file(),
    ).await.expect("create an empty file with the local-disk data cache");

    assert_eq!(h.fs_getattr().expect("getattr").st_size, 0);
    let _ = h.fs_flush().await.expect("flush");
    let _ = h.fs_release().await.expect("release");

    tf.cleanup(&client).await;
}

/// Releasing a file on this tier used to abort the process:
/// `close` closed a descriptor owned by `File`, and both `shutdown`
/// and `Drop` called it.
///
/// The abort happened after `fs_release` returned, in `Drop`, so this
/// only proves anything because the process has to survive long
/// enough to run the rest of the suite.
#[tokio::test]
#[ignore]
async fn release_does_not_abort() {
    let _ = env_logger::try_init();
    let client = make_client().await;
    let dir = CacheDir::new("release");
    let tf = TestFile::new(&client).await;

    for round in 0..3 {
        let mut h = Hyper::fs_open_or_create_with_config(
            &client, local_disk_config(tf.uri(), &dir, 32), FileFlags::rdwr(), FileMode::default_file(),
        ).await.unwrap_or_else(|e| panic!("round {round}: open: {e}"));
        let _ = h.fs_write(0, &vec![round as u8; BLK]).await.expect("write");
        let _ = h.fs_flush().await.expect("flush");
        let _ = h.fs_release().await.expect("release");
        // The cache is dropped here.
    }

    tf.cleanup(&client).await;
}

/// An extending write used to address a block past the end of the
/// mapping and write through the resulting pointer. Block 40 of a
/// one-block file is ~150 KiB outside a 4 KiB mapping.
#[tokio::test]
#[ignore]
async fn extending_write_far_past_eof() {
    let _ = env_logger::try_init();
    let client = make_client().await;
    let dir = CacheDir::new("extend");
    let tf = TestFile::new(&client).await;

    let mut h = Hyper::fs_open_or_create_with_config(
        &client, local_disk_config(tf.uri(), &dir, 64), FileFlags::rdwr(), FileMode::default_file(),
    ).await.expect("create");

    // One block, so the old mapping would have been 4096 bytes.
    let _ = h.fs_write(0, &vec![0x11u8; BLK]).await.expect("write block 0");
    let _ = h.fs_flush().await.expect("flush");

    // Far outside it.
    let _ = h.fs_write(40 * BLK, &vec![0xEEu8; BLK]).await.expect("write block 40");
    // And further, without an intervening flush.
    let _ = h.fs_write(500 * BLK, &vec![0xCCu8; BLK]).await.expect("write block 500");
    let _ = h.fs_flush().await.expect("flush");

    // Read everything back.
    let mut buf = vec![0u8; BLK];
    let _ = h.fs_read(0, &mut buf).await.expect("read 0");
    assert!(buf.iter().all(|b| *b == 0x11), "block 0 damaged");
    let mut buf = vec![0u8; BLK];
    let _ = h.fs_read(40 * BLK, &mut buf).await.expect("read 40");
    assert!(buf.iter().all(|b| *b == 0xEE), "block 40 wrong");
    let mut buf = vec![0u8; BLK];
    let _ = h.fs_read(500 * BLK, &mut buf).await.expect("read 500");
    assert!(buf.iter().all(|b| *b == 0xCC), "block 500 wrong");
    // A hole between them still reads as zeros.
    let mut buf = vec![0xFFu8; BLK];
    let _ = h.fs_read(100 * BLK, &mut buf).await.expect("read hole");
    assert!(buf.iter().all(|b| *b == 0), "hole should read as zeros");

    let _ = h.fs_release().await.expect("release");
    tf.cleanup(&client).await;
}

/// A sparse address space far larger than the file's resident data.
/// The mapping used to have to span the whole space; now it covers
/// only the slot pool.
#[tokio::test]
#[ignore]
async fn sparse_address_space() {
    let _ = env_logger::try_init();
    let client = make_client().await;
    let dir = CacheDir::new("sparse");
    let tf = TestFile::new(&client).await;

    let mut h = Hyper::fs_open_or_create_with_config(
        &client, local_disk_config(tf.uri(), &dir, 64), FileFlags::rdwr(), FileMode::default_file(),
    ).await.expect("create");

    // 4 TiB apart. Mapping the address space would need 4 TiB of
    // virtual address space and a 4 TiB cache file.
    let far = 1usize << 42;
    let _ = h.fs_write(0, &vec![0xA0u8; BLK]).await.expect("write low");
    let _ = h.fs_write(far, &vec![0xB0u8; BLK]).await.expect("write far");
    let _ = h.fs_flush().await.expect("flush");

    assert_eq!(h.fs_getattr().expect("getattr").st_size, (far + BLK) as i64);
    // Only two blocks are resident, so st_blocks stays small.
    assert_eq!(h.fs_getattr().expect("getattr").st_blocks, 2 * (BLK / 512) as i64);

    let mut buf = vec![0u8; BLK];
    let _ = h.fs_read(0, &mut buf).await.expect("read low");
    assert!(buf.iter().all(|b| *b == 0xA0));
    let mut buf = vec![0u8; BLK];
    let _ = h.fs_read(far, &mut buf).await.expect("read far");
    assert!(buf.iter().all(|b| *b == 0xB0));

    let _ = h.fs_release().await.expect("release");
    tf.cleanup(&client).await;
}

/// Slots must be recycled. Touch far more distinct blocks than the
/// pool has slots, with flushes in between so blocks move through the
/// clean tier and get evicted, and verify the data stays correct.
#[tokio::test]
#[ignore]
async fn slot_recycling_across_many_blocks() {
    let _ = env_logger::try_init();
    let client = make_client().await;
    let dir = CacheDir::new("recycle");
    let tf = TestFile::new(&client).await;

    // A deliberately tiny clean tier so eviction runs constantly.
    let mut h = Hyper::fs_open_or_create_with_config(
        &client, local_disk_config(tf.uri(), &dir, 4), FileFlags::rdwr(), FileMode::default_file(),
    ).await.expect("create");

    const N: u64 = 64;
    for i in 0..N {
        let _ = h.fs_write(i as usize * BLK, &vec![(i + 1) as u8; BLK]).await
            .unwrap_or_else(|e| panic!("write block {i}: {e}"));
        if i % 8 == 7 {
            let _ = h.fs_flush().await.expect("flush");
        }
    }
    let _ = h.fs_flush().await.expect("flush");

    // Every block must still hold its own marker. A slot handed out
    // twice, or a hole punched under a live block, shows up here.
    for i in 0..N {
        let mut buf = vec![0u8; BLK];
        let _ = h.fs_read(i as usize * BLK, &mut buf).await.expect("read");
        let want = (i + 1) as u8;
        assert!(buf.iter().all(|b| *b == want),
            "block {i} should be all {want:#04x}, got first byte {:#04x}", buf[0]);
    }

    // Read them again, now mostly from staging, to exercise the clean
    // tier's insert/evict cycle.
    for i in (0..N).rev() {
        let mut buf = vec![0u8; BLK];
        let _ = h.fs_read(i as usize * BLK, &mut buf).await.expect("reread");
        let want = (i + 1) as u8;
        assert!(buf.iter().all(|b| *b == want), "block {i} wrong on reread");
    }

    let _ = h.fs_release().await.expect("release");
    tf.cleanup(&client).await;
}

/// Truncate has to release the slots of the blocks it drops, and a
/// file grown back past the old EOF must read as zeros rather than
/// serving pre-truncate bytes out of a recycled slot.
#[tokio::test]
#[ignore]
async fn truncate_releases_slots_and_leaves_no_stale_bytes() {
    let _ = env_logger::try_init();
    let client = make_client().await;
    let dir = CacheDir::new("truncate");
    let tf = TestFile::new(&client).await;

    let mut h = Hyper::fs_open_or_create_with_config(
        &client, local_disk_config(tf.uri(), &dir, 64), FileFlags::rdwr(), FileMode::default_file(),
    ).await.expect("create");

    let _ = h.fs_write(0, &vec![0xABu8; 8 * BLK]).await.expect("write");
    let _ = h.fs_flush().await.expect("flush");

    // Shrink to one block, then grow back.
    let _ = h.fs_truncate(BLK).await.expect("shrink");
    let _ = h.fs_flush().await.expect("flush");
    let _ = h.fs_truncate(8 * BLK).await.expect("grow");
    let _ = h.fs_flush().await.expect("flush");

    let mut buf = vec![0xFFu8; BLK];
    let _ = h.fs_read(0, &mut buf).await.expect("read surviving block");
    assert!(buf.iter().all(|b| *b == 0xAB), "block 0 should survive the truncate");

    for i in 1..8u64 {
        let mut buf = vec![0xFFu8; BLK];
        let _ = h.fs_read(i as usize * BLK, &mut buf).await.expect("read regrown");
        assert!(buf.iter().all(|b| *b == 0),
            "block {i} above the old EOF must read as zeros, found {:#04x}", buf[0]);
    }

    let _ = h.fs_release().await.expect("release");
    tf.cleanup(&client).await;
}

/// The pool is finite. Dirty far more blocks in one flush window than
/// it has slots and confirm the fallback to heap blocks is correct
/// rather than merely non-crashing.
#[tokio::test]
#[ignore]
async fn pool_exhaustion_falls_back_correctly() {
    let _ = env_logger::try_init();
    let client = make_client().await;
    let dir = CacheDir::new("exhaust");
    let tf = TestFile::new(&client).await;

    let mut h = Hyper::fs_open_or_create_with_config(
        &client, local_disk_config(tf.uri(), &dir, 8), FileFlags::rdwr(), FileMode::default_file(),
    ).await.expect("create");

    // One write covering many blocks: every block is dirtied before
    // any flush check runs.
    const N: usize = 3000;
    let mut payload = vec![0u8; N * BLK];
    for i in 0..N {
        payload[i * BLK..(i + 1) * BLK].fill((i % 251) as u8);
    }
    let _ = h.fs_write(0, &payload).await.expect("one big write");
    let _ = h.fs_flush().await.expect("flush");

    // Spot-check across the range, including past where a modest pool
    // would have run out.
    for i in [0usize, 1, 500, 1500, 2999] {
        let mut buf = vec![0u8; BLK];
        let _ = h.fs_read(i * BLK, &mut buf).await.expect("read");
        let want = (i % 251) as u8;
        assert!(buf.iter().all(|b| *b == want),
            "block {i} should be all {want:#04x}, got {:#04x}", buf[0]);
    }

    let _ = h.fs_release().await.expect("release");
    tf.cleanup(&client).await;
}
