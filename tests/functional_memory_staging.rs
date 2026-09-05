#![cfg(not(feature = "blocking"))]
//! Functional tests on in-memory staging — no S3, no credentials, no
//! network.
//!
//! These drive `HyperFile` directly rather than through `Hyper`, because
//! `Hyper` is bound to S3 staging and takes an `aws_sdk_s3::Client` in
//! every constructor. Everything below `Hyper` is generic, so the format,
//! the bmap, the block pointers, the caches, flush and reopen all run
//! exactly as they do against a bucket.
//!
//! What is deliberately *not* covered here, because it belongs to S3
//! rather than to hyperfile: conditional writes and the OCC built on
//! them, multipart upload, and the error kinds a real service returns.
//! Those stay in the `integration_s3_*` suites.
//!
//! Run with no environment at all:
//!
//! ```bash
//! cargo test --test functional_memory_staging
//! ```

use std::io::ErrorKind;

use btree_ondisk::NullNodeCache;

use hyperfile::config::{HyperFileConfigBuilder, HyperFileRuntimeConfig};
use hyperfile::file::HyperTrait;
use hyperfile::file::file::HyperFile;
use hyperfile::file::flags::{FileFlags, HyperFileFlags};
use hyperfile::file::mode::{FileMode, HyperFileMode};
use hyperfile::staging::Staging;
use hyperfile::staging::config::StagingConfig;
use btree_ondisk::MemoryBlockLoader;
use hyperfile::staging::memory::MemoryStaging;

const BLK: usize = 4096;

type MemFile<'a> = HyperFile<'a, MemoryStaging, MemoryBlockLoader<hyperfile::BlockPtr>, NullNodeCache>;

/// A staging handle and a file created on it. Clones of the handle share
/// the storage, so the returned handle can reopen the file later.
async fn create(name: &str) -> (MemoryStaging, MemFile<'static>) {
    create_with(name, HyperFileRuntimeConfig::default()).await
}

async fn create_with(name: &str, runtime: HyperFileRuntimeConfig) -> (MemoryStaging, MemFile<'static>) {
    let staging_config = StagingConfig::new_memory(name);
    let config = HyperFileConfigBuilder::new()
        .with_staging_config(&staging_config)
        .with_runtime_config(&runtime)
        .build();
    let staging = MemoryStaging::new(staging_config, runtime);
    let file = MemFile::new(
        staging.clone(),
        staging.to_block_loader(),
        NullNodeCache,
        config,
        HyperFileFlags::from_flags(FileFlags::rdwr()),
        HyperFileMode::from_mode(FileMode::default_file()),
    ).await.expect("create on memory staging");
    (staging, file)
}

/// Reopen the file that lives on `staging`.
async fn reopen(staging: &MemoryStaging, flags: FileFlags) -> MemFile<'static> {
    let config = HyperFileConfigBuilder::new()
        .with_staging_config(staging.config())
        .build();
    MemFile::open(
        staging.clone(),
        staging.to_block_loader(),
        NullNodeCache,
        config,
        HyperFileFlags::from_flags(flags),
    ).await.expect("reopen from memory staging")
}

async fn read_at(file: &mut MemFile<'static>, off: usize, len: usize) -> Vec<u8> {
    let mut buf = vec![0u8; len];
    let n = file.read(off, &mut buf).await.expect("read");
    buf.truncate(n);
    buf
}

#[tokio::test]
async fn write_flush_reopen_round_trip() {
    let _ = env_logger::try_init();
    let (staging, mut file) = create("round-trip").await;

    let payload = b"hyperfile on memory staging".to_vec();
    let n = file.write(0, &payload).await.expect("write");
    assert_eq!(n, payload.len());
    // Creating a file writes its inode straight away — `HyperFile::new`
    // ends in `flush_inode(Create)` — but no segment exists until data is
    // flushed.
    assert!(staging.has_inode(), "create should have written an inode");
    assert_eq!(staging.segment_count(), 0, "no segment until the first flush");

    let segid = file.flush().await.expect("flush");
    assert!(segid.as_cno() > 0, "flush should produce a segment id");
    assert!(staging.has_inode(), "the flush should have written an inode");
    assert_eq!(staging.segment_count(), 1, "one flush, one segment");

    drop(file);

    let mut file = reopen(&staging, FileFlags::rdonly()).await;
    assert_eq!(read_at(&mut file, 0, payload.len()).await, payload);
}

#[tokio::test]
async fn data_survives_across_several_flushes() {
    let _ = env_logger::try_init();
    let (staging, mut file) = create("many-flushes").await;

    // Each round writes its own block and flushes, so every block ends up
    // in a different segment and reading them back exercises the bmap
    // resolving pointers across segments.
    for round in 0..8u8 {
        let _ = file.write(round as usize * BLK, &vec![0x40 + round; BLK]).await.expect("write");
        let _ = file.flush().await.expect("flush");
    }
    assert_eq!(staging.segment_count(), 8, "one segment per flush");

    drop(file);
    let mut file = reopen(&staging, FileFlags::rdonly()).await;
    for round in 0..8u8 {
        let got = read_at(&mut file, round as usize * BLK, BLK).await;
        assert_eq!(got.len(), BLK, "block {} short read", round);
        assert!(got.iter().all(|&v| v == 0x40 + round),
            "block {} came back wrong, starts {:#x}", round, got[0]);
    }
}

#[tokio::test]
async fn unaligned_write_keeps_the_bytes_around_it() {
    let _ = env_logger::try_init();
    let (staging, mut file) = create("unaligned").await;

    // Lay down a pattern and get it onto staging, so the partial write
    // that follows has to read the block back before modifying it.
    let _ = file.write(0, &vec![0xAA; 4 * BLK]).await.expect("seed write");
    let _ = file.flush().await.expect("seed flush");
    drop(file);

    let mut file = reopen(&staging, FileFlags::rdwr()).await;
    let _ = file.write(BLK + 100, &vec![0xC7; 64]).await.expect("unaligned write");
    let _ = file.flush().await.expect("flush");
    drop(file);

    let mut file = reopen(&staging, FileFlags::rdonly()).await;
    let got = read_at(&mut file, BLK, BLK).await;
    for (i, &v) in got.iter().enumerate() {
        let want = if (100..164).contains(&i) { 0xC7 } else { 0xAA };
        assert_eq!(v, want, "byte {} of block 1: expected {:#x}, got {:#x}", i, want, v);
    }
}

#[tokio::test]
async fn holes_read_as_zeroes() {
    let _ = env_logger::try_init();
    let (staging, mut file) = create("sparse").await;

    // Blocks 0 and 3 written, 1 and 2 never touched.
    let _ = file.write(0, &vec![0x11; BLK]).await.expect("write low");
    let _ = file.write(3 * BLK, &vec![0x22; BLK]).await.expect("write high");
    let _ = file.flush().await.expect("flush");
    drop(file);

    let mut file = reopen(&staging, FileFlags::rdonly()).await;
    assert!(read_at(&mut file, 0, BLK).await.iter().all(|&v| v == 0x11));
    assert!(read_at(&mut file, BLK, 2 * BLK).await.iter().all(|&v| v == 0),
        "a hole has to read as zeroes");
    assert!(read_at(&mut file, 3 * BLK, BLK).await.iter().all(|&v| v == 0x22));
}

#[tokio::test]
async fn truncate_shrinks_and_grows() {
    let _ = env_logger::try_init();
    let (staging, mut file) = create("truncate").await;

    let _ = file.write(0, &vec![0x5A; 4 * BLK]).await.expect("write");
    let _ = file.flush().await.expect("flush");

    // Shrink to a block boundary.
    file.truncate(BLK).await.expect("shrink");
    let _ = file.flush().await.expect("flush after shrink");
    assert_eq!(file.stat().st_size, BLK as i64, "size should follow the shrink");

    // Grow again: the new range is a hole and must read as zeroes.
    file.truncate(3 * BLK).await.expect("grow");
    let _ = file.flush().await.expect("flush after grow");
    drop(file);

    let mut file = reopen(&staging, FileFlags::rdonly()).await;
    assert_eq!(file.stat().st_size, (3 * BLK) as i64);
    assert!(read_at(&mut file, 0, BLK).await.iter().all(|&v| v == 0x5A),
        "the surviving block should keep its contents");
    assert!(read_at(&mut file, BLK, 2 * BLK).await.iter().all(|&v| v == 0),
        "the range added by growing should read as zeroes");
}

#[tokio::test]
async fn write_zero_punches_a_hole() {
    let _ = env_logger::try_init();
    let (staging, mut file) = create("write-zero").await;

    let _ = file.write(0, &vec![0x33; 4 * BLK]).await.expect("write");
    let _ = file.flush().await.expect("flush");

    let _ = file.write_zero(BLK, 2 * BLK).await.expect("write_zero");
    let _ = file.flush().await.expect("flush after write_zero");
    drop(file);

    let mut file = reopen(&staging, FileFlags::rdonly()).await;
    assert!(read_at(&mut file, 0, BLK).await.iter().all(|&v| v == 0x33));
    assert!(read_at(&mut file, BLK, 2 * BLK).await.iter().all(|&v| v == 0),
        "the zeroed range should read as zeroes");
    assert!(read_at(&mut file, 3 * BLK, BLK).await.iter().all(|&v| v == 0x33));
}

#[tokio::test]
async fn many_blocks_grow_the_bmap_past_one_meta_node() {
    let _ = env_logger::try_init();
    let (staging, mut file) = create("bmap-growth").await;

    // Enough blocks that the bmap needs more than its root node, so
    // reopening has to fetch meta nodes out of the segment through the
    // block loader rather than reading them from the inode.
    const NB: usize = 600;
    for b in 0..NB {
        let _ = file.write(b * BLK, &vec![(b % 251) as u8; BLK]).await.expect("write");
    }
    let _ = file.flush().await.expect("flush");
    drop(file);

    let mut file = reopen(&staging, FileFlags::rdonly()).await;
    assert_eq!(file.stat().st_size, (NB * BLK) as i64);
    for b in (0..NB).step_by(37) {
        let got = read_at(&mut file, b * BLK, BLK).await;
        let want = (b % 251) as u8;
        assert!(got.iter().all(|&v| v == want),
            "block {} came back wrong, starts {:#x} wanted {:#x}", b, got[0], want);
    }
}

/// The read counters work the same way here as against a bucket, which
/// makes this the cheapest place to pin the rule that byte reads do not
/// populate the data cache — see docs/posix.md, "Byte reads do not
/// populate the data cache; block access does".
#[tokio::test]
async fn byte_reads_reach_staging_every_time() {
    let _ = env_logger::try_init();
    let (staging, mut file) = create("counters").await;

    let _ = file.write(0, &vec![0x77; BLK]).await.expect("write");
    let _ = file.flush().await.expect("flush");
    drop(file);

    let mut file = reopen(&staging, FileFlags::rdonly()).await;
    let before = staging.read_timing().snapshot();
    let _ = read_at(&mut file, 0, BLK).await;
    let after_first = staging.read_timing().snapshot();
    assert!(after_first.data_gets > before.data_gets,
        "a cold block read has to reach staging");

    let _ = read_at(&mut file, 0, BLK).await;
    let after_second = staging.read_timing().snapshot();
    assert_eq!(after_second.data_gets, after_first.data_gets + 1,
        "a second byte read of the same block reaches staging again, because \
         the byte path does not populate the data cache");
}

#[tokio::test]
async fn unlink_removes_everything() {
    let _ = env_logger::try_init();
    let (staging, mut file) = create("unlink").await;

    let _ = file.write(0, &vec![0x99; 2 * BLK]).await.expect("write");
    let _ = file.flush().await.expect("flush");
    drop(file);
    assert!(staging.segment_count() > 0);
    assert!(staging.has_inode());

    staging.unlink().await.expect("unlink");
    assert_eq!(staging.segment_count(), 0, "unlink should drop the segments");
    assert!(!staging.has_inode(), "unlink should drop the inode");

    // Opening what is no longer there has to fail rather than hand back an
    // empty file.
    let config = HyperFileConfigBuilder::new()
        .with_staging_config(staging.config())
        .build();
    let res = MemFile::open(
        staging.clone(),
        staging.to_block_loader(),
        NullNodeCache,
        config,
        HyperFileFlags::from_flags(FileFlags::rdonly()),
    ).await;
    match res {
        Err(e) => assert_eq!(e.kind(), ErrorKind::NotFound,
            "opening an unlinked file should be NotFound, got {:?}", e.kind()),
        Ok(_) => panic!("opening an unlinked file should have failed"),
    }
}

#[tokio::test]
async fn separate_storage_does_not_leak_between_handles() {
    let _ = env_logger::try_init();
    let (staging_a, mut file_a) = create("handle-a").await;
    let (staging_b, _file_b) = create("handle-b").await;

    let _ = file_a.write(0, &vec![0x01; BLK]).await.expect("write");
    let _ = file_a.flush().await.expect("flush");

    assert_eq!(staging_a.segment_count(), 1);
    assert_eq!(staging_b.segment_count(), 0,
        "a separately constructed MemoryStaging must not see another's segments");
}
/// `HyperFileMetaConfig::block_ptr_format` decides the format a new file
/// uses. It used to be ignored — `HyperFile::new` hardcoded `MicroGroup` —
/// so this pins that the config is what is honoured, and that its default
/// still names the format files were already getting.
#[tokio::test]
async fn block_ptr_format_comes_from_the_config() {
    use hyperfile::meta_format::BlockPtrFormat;

    let _ = env_logger::try_init();
    assert_eq!(hyperfile::config::HyperFileMetaConfig::default().block_ptr_format,
        BlockPtrFormat::MicroGroup,
        "the default has to stay the format created files already used");

    // Ask for Flat explicitly and check a round trip works on it, which it
    // cannot if the setting is being ignored.
    for fmt in [BlockPtrFormat::Flat, BlockPtrFormat::MicroGroup] {
        let staging_config = StagingConfig::new_memory(&format!("fmt-{:?}", fmt));
        let mut meta = hyperfile::config::HyperFileMetaConfig::default();
        meta.block_ptr_format = fmt;
        let config = HyperFileConfigBuilder::new()
            .with_staging_config(&staging_config)
            .with_meta_config(&meta)
            .build();
        let staging = MemoryStaging::new(staging_config, HyperFileRuntimeConfig::default())
            .with_block_ptr_format(fmt);

        let mut file = MemFile::new(
            staging.clone(), staging.to_block_loader(), NullNodeCache, config.clone(),
            HyperFileFlags::from_flags(FileFlags::rdwr()),
            HyperFileMode::from_mode(FileMode::default_file()),
        ).await.expect("create");

        // Enough blocks that reopening has to resolve pointers through the
        // loader, which only works if both sides agree on the format.
        for b in 0..300usize {
            let _ = file.write(b * BLK, &vec![(b % 251) as u8; BLK]).await.expect("write");
        }
        let _ = file.flush().await.expect("flush");
        drop(file);

        let mut file = MemFile::open(
            staging.clone(), staging.to_block_loader(), NullNodeCache, config,
            HyperFileFlags::from_flags(FileFlags::rdonly()),
        ).await.expect("reopen");
        for b in (0..300usize).step_by(29) {
            let got = read_at(&mut file, b * BLK, BLK).await;
            assert!(got.iter().all(|&v| v == (b % 251) as u8),
                "{:?}: block {} came back wrong", fmt, b);
        }
    }
}

/// `read_ahead` puts a range in the data cache so a later read finds it.
///
/// A byte read queries the cache but does not fill it, so bytes fetched
/// speculatively through `read` have nowhere to live and the next read
/// fetches them again — which makes read-ahead through the byte path a
/// pure loss. This checks the two things a caller needs: the reads that
/// follow cost no object requests, and warming a wide range costs far
/// fewer requests than one per block.
#[tokio::test]
async fn read_ahead_warms_the_cache_for_later_reads() {
    let _ = env_logger::try_init();
    let (staging, mut file) = create("read-ahead").await;

    const NB: usize = 64;
    for b in 0..NB {
        let _ = file.write(b * BLK, &vec![(b % 251) as u8; BLK]).await.expect("write");
    }
    let _ = file.flush().await.expect("flush");
    drop(file);

    let mut file = reopen(&staging, FileFlags::rdonly()).await;

    // Warm the whole file in one call.
    let before = staging.read_timing().snapshot();
    let cached = file.read_ahead(0, NB * BLK).await.expect("read_ahead");
    let after_warm = staging.read_timing().snapshot();
    assert_eq!(cached, NB, "every block in the range should have been installed");

    let warm_gets = after_warm.data_gets - before.data_gets;
    assert!(warm_gets < NB as u64,
        "warming {} blocks took {} requests, so nothing was coalesced", NB, warm_gets);

    // The reads that follow must not reach staging at all.
    for b in 0..NB {
        let got = read_at(&mut file, b * BLK, BLK).await;
        assert!(got.iter().all(|&v| v == (b % 251) as u8),
            "block {} came back wrong after read_ahead", b);
    }
    let after_reads = staging.read_timing().snapshot();
    assert_eq!(after_reads.data_gets, after_warm.data_gets,
        "reads over a warmed range must not issue object requests");

    eprintln!("warmed {} blocks with {} requests; {} following reads cost 0",
        cached, warm_gets, NB);
}

/// Read-ahead skips what it does not need: blocks already cached, and
/// holes, which read as zeroes without an object request.
#[tokio::test]
async fn read_ahead_skips_resident_blocks_and_holes() {
    let _ = env_logger::try_init();
    let (staging, mut file) = create("read-ahead-skips").await;

    // Blocks 0..4 written, 4..8 left as a hole, 8..12 written.
    let _ = file.write(0, &vec![0x11; 4 * BLK]).await.expect("write low");
    let _ = file.write(8 * BLK, &vec![0x22; 4 * BLK]).await.expect("write high");
    let _ = file.flush().await.expect("flush");
    drop(file);

    let mut file = reopen(&staging, FileFlags::rdonly()).await;

    // Holes are not installed, so only the eight backed blocks are.
    let cached = file.read_ahead(0, 12 * BLK).await.expect("read_ahead");
    assert_eq!(cached, 8, "only blocks backed by data should be installed");

    // A second warm of the same range finds everything resident and
    // fetches nothing.
    let before = staging.read_timing().snapshot();
    let again = file.read_ahead(0, 12 * BLK).await.expect("read_ahead again");
    let after = staging.read_timing().snapshot();
    assert_eq!(again, 0, "a warmed range should install nothing the second time");
    assert_eq!(after.data_gets, before.data_gets,
        "a warmed range should not be fetched again");

    // And the data still reads correctly, holes included.
    assert!(read_at(&mut file, 0, 4 * BLK).await.iter().all(|&v| v == 0x11));
    assert!(read_at(&mut file, 4 * BLK, 4 * BLK).await.iter().all(|&v| v == 0));
    assert!(read_at(&mut file, 8 * BLK, 4 * BLK).await.iter().all(|&v| v == 0x22));
}
