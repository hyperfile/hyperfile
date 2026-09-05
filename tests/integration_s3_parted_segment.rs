//! Partial segments: what a container writes when memory pressure asks for it and
//! nobody asked for a checkpoint.
//!
//! Needs two things that are off by default — `BlockPtrFormat::PartedSegment`,
//! fixed when the container is created, and `parted_segment_enabled` — so every
//! other suite exercises the single-object path and this is the only place a
//! partial is produced.

#[allow(dead_code)]
mod common;

use common::*;

use hyperfile::file::hyper::Hyper;
use hyperfile::file::flags::{FileFlags, HyperFileFlags};
use hyperfile::file::mode::{FileMode, HyperFileMode};
use hyperfile::config::{HyperFileConfig, HyperFileConfigBuilder, HyperFileMetaConfig,
    HyperFileRuntimeConfig};
use hyperfile::meta_format::BlockPtrFormat;
use hyperfile::staging::config::StagingConfig;

const BLK: usize = 4096;

/// A container that answers memory pressure with a partial segment.
///
/// `trigger_blocks` is where the dirty threshold fires and `min_bytes` is the floor
/// below which a partial is not worth its object. Both small, so a test can cross
/// them without writing much.
fn parted_config(uri: &str, trigger_blocks: usize, min_bytes: usize, enabled: bool) -> HyperFileConfig {
    let d = HyperFileMetaConfig::default();
    let meta = HyperFileMetaConfig::new(
        d.root_size, d.meta_block_size, BLK, BlockPtrFormat::PartedSegment);
    let mut runtime = HyperFileRuntimeConfig::default();
    runtime.parted_segment_enabled = enabled;
    runtime.data_cache_dirty_min_bytes_to_part = min_bytes;
    // The block count is what fires; the byte ceiling and the timer are kept out of
    // it so the test's own boundary is the only one.
    runtime.data_cache_dirty_max_blocks_threshold = trigger_blocks;
    runtime.data_cache_dirty_max_bytes_threshold = trigger_blocks * BLK;
    // `segment_buffer_size` is left alone: the WAL path preallocates it, and its
    // default is far above what these tests write, so it never fires.
    runtime.data_cache_dirty_max_flush_interval = u64::MAX / 2;
    HyperFileConfigBuilder::new()
        .with_staging_config(&StagingConfig::new_s3_uri(uri, None))
        .with_meta_config(&meta)
        .with_runtime_config(&runtime)
        .build()
}

async fn object_names(client: &aws_sdk_s3::Client, uri: &str) -> Vec<String> {
    let bucket = test_bucket();
    let root = uri.strip_prefix(&format!("s3://{}/", bucket)).expect("uri");
    let mut out = Vec::new();
    let mut token = None;
    loop {
        let mut req = client.list_objects_v2().bucket(&bucket).prefix(format!("{}/", root));
        if let Some(t) = token { req = req.continuation_token(t); }
        let r = req.send().await.expect("list");
        for o in r.contents() {
            if let Some(k) = o.key() {
                out.push(k.rsplit('/').next().unwrap().to_string());
            }
        }
        match r.next_continuation_token() {
            Some(t) => token = Some(t.to_string()),
            None => break,
        }
    }
    out.sort();
    out
}

fn parts_of(names: &[String]) -> Vec<&String> {
    names.iter().filter(|n| n.contains('.')).collect()
}

/// Crossing the dirty threshold writes a partial segment and publishes nothing.
///
/// This is the whole point. That crossing publishes a checkpoint the caller never
/// asked for, which is what ties memory pressure to the checkpoint history — and
/// under a transaction or `Barrier`, where nothing may publish, it is refused
/// instead, so work larger than the threshold cannot be done at all.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn crossing_the_threshold_writes_a_partial_and_publishes_nothing() {
    let _ = env_logger::try_init();
    let client = make_client().await;
    let tf = TestFile::new(&client).await;
    let config = parted_config(tf.uri(), 4, 2 * BLK, true);

    {
        let mut hyper = Hyper::create(
            client.clone(), config.clone(),
            HyperFileFlags::from_flags(FileFlags::rdwr()),
            HyperFileMode::from_mode(FileMode::default_file()),
        ).await.expect("create");
        let cno_before = hyper.fs_last_cno();
        for i in 0..20 {
            let _ = hyper.fs_write(i * BLK, &vec![0x40 + i as u8; BLK]).await.expect("write");
        }
        let cno_after = hyper.fs_last_cno();
        assert_eq!(cno_after, cno_before,
            "a threshold crossing published a checkpoint: {} -> {}", cno_before, cno_after);

        let names = object_names(&client, tf.uri()).await;
        assert!(!parts_of(&names).is_empty(),
            "the threshold produced no partial: {:?}", names);
        assert!(!names.iter().any(|n| n.ends_with(".0")),
            "part 0 belongs to the consistency point, and none was asked for: {:?}", names);

        let cno = hyper.fs_flush().await.expect("flush");
        assert!(cno > cno_after, "the flush published nothing");
        let _ = hyper.fs_release().await;
    }

    let names = object_names(&client, tf.uri()).await;
    assert!(names.iter().any(|n| n.ends_with(".0")),
        "the consistency point wrote no part 0: {:?}", names);

    let mut hyper = Hyper::open(
        client.clone(), config.clone(),
        HyperFileFlags::from_flags(FileFlags::rdonly()),
    ).await.expect("reopen");
    for i in 0..20 {
        let mut buf = vec![0u8; BLK];
        let n = hyper.fs_read(i * BLK, &mut buf).await.expect("read");
        assert_eq!(n, BLK, "short read at block {}", i);
        assert!(buf.iter().all(|b| *b == 0x40 + i as u8),
            "block {} came back {:#x}, so a partial lost or misplaced it", i, buf[0]);
    }
    let _ = hyper.fs_release().await;
    tf.cleanup(&client).await;
}

/// Every partial belongs to the checkpoint that completes it: one number, one
/// part 0.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn partials_and_their_consistency_point_share_one_checkpoint() {
    let _ = env_logger::try_init();
    let client = make_client().await;
    let tf = TestFile::new(&client).await;
    let config = parted_config(tf.uri(), 4, 2 * BLK, true);

    {
        let mut hyper = Hyper::create(
            client.clone(), config.clone(),
            HyperFileFlags::from_flags(FileFlags::rdwr()),
            HyperFileMode::from_mode(FileMode::default_file()),
        ).await.expect("create");
        for i in 0..20 {
            let _ = hyper.fs_write(i * BLK, &vec![0x60 + i as u8; BLK]).await.expect("write");
        }
        let _ = hyper.fs_flush().await.expect("flush");
        let _ = hyper.fs_release().await;
    }

    let names = object_names(&client, tf.uri()).await;
    let checkpoints: std::collections::HashSet<&str> = parts_of(&names).iter()
        .map(|n| n.split_once('.').unwrap().0).collect();
    assert_eq!(checkpoints.len(), 1,
        "one flush should be one checkpoint, got {:?} from {:?}", checkpoints, names);
    assert_eq!(names.iter().filter(|n| n.ends_with(".0")).count(), 1,
        "exactly one part 0 completes the stream: {:?}", names);
    assert!(parts_of(&names).len() >= 3,
        "expected partials plus a part 0, got {:?}", names);

    tf.cleanup(&client).await;
}

/// With the switch off the same workload publishes on every crossing, which is
/// what every other container does.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn with_the_switch_off_a_threshold_still_publishes() {
    let _ = env_logger::try_init();
    let client = make_client().await;
    let tf = TestFile::new(&client).await;
    let config = parted_config(tf.uri(), 4, 2 * BLK, false);

    let mut hyper = Hyper::create(
        client.clone(), config.clone(),
        HyperFileFlags::from_flags(FileFlags::rdwr()),
        HyperFileMode::from_mode(FileMode::default_file()),
    ).await.expect("create");
    let cno_before = hyper.fs_last_cno();
    for i in 0..20 {
        let _ = hyper.fs_write(i * BLK, &vec![0x80 + i as u8; BLK]).await.expect("write");
    }
    assert!(hyper.fs_last_cno() > cno_before,
        "with the switch off a crossing must publish, {} -> {}",
        cno_before, hyper.fs_last_cno());

    let names = object_names(&client, tf.uri()).await;
    assert!(!names.iter().any(|n| {
            n.split_once('.').and_then(|(_, p)| p.parse::<u16>().ok()).map(|p| p > 0).unwrap_or(false)
        }),
        "the switch is off, so nothing beyond part 0 should exist: {:?}", names);

    for i in 0..20 {
        let mut buf = vec![0u8; BLK];
        let _ = hyper.fs_read(i * BLK, &mut buf).await.expect("read");
        assert!(buf.iter().all(|b| *b == 0x80 + i as u8), "block {} came back {:#x}", i, buf[0]);
    }
    let _ = hyper.fs_release().await;
    tf.cleanup(&client).await;
}

/// A floor above the trigger suppresses partials: a crossing not worth an object
/// behaves as it always did.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn a_crossing_below_the_floor_publishes_instead() {
    let _ = env_logger::try_init();
    let client = make_client().await;
    let tf = TestFile::new(&client).await;
    let config = parted_config(tf.uri(), 4, 64 * BLK, true);

    let mut hyper = Hyper::create(
        client.clone(), config.clone(),
        HyperFileFlags::from_flags(FileFlags::rdwr()),
        HyperFileMode::from_mode(FileMode::default_file()),
    ).await.expect("create");
    let cno_before = hyper.fs_last_cno();
    for i in 0..20 {
        let _ = hyper.fs_write(i * BLK, &vec![0x90 + i as u8; BLK]).await.expect("write");
    }
    assert!(hyper.fs_last_cno() > cno_before,
        "below the floor a crossing must publish as before, {} -> {}",
        cno_before, hyper.fs_last_cno());
    let _ = hyper.fs_release().await;
    tf.cleanup(&client).await;
}

/// A partial says so in its header, the consistency point does not, and every
/// partial carries a populated inode.
///
/// The flag is for a reader that arrives with an object name rather than a
/// checkpoint number — a cleaner, or a tool walking a listing. Resolving a
/// checkpoint number never lands on a partial, because it only ever looks for the
/// bare name or part 0; the flag is what protects everything that does not go
/// through that resolution.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn a_partial_is_marked_and_carries_its_own_inode() {
    use hyperfile::segment::SegmentReadWrite;
    use hyperfile::staging::s3::S3Staging;
    use hyperfile::SegmentId;

    let _ = env_logger::try_init();
    let client = make_client().await;
    let tf = TestFile::new(&client).await;
    let config = parted_config(tf.uri(), 4, 2 * BLK, true);

    {
        let mut hyper = Hyper::create(
            client.clone(), config.clone(),
            HyperFileFlags::from_flags(FileFlags::rdwr()),
            HyperFileMode::from_mode(FileMode::default_file()),
        ).await.expect("create");
        for i in 0..20 {
            let _ = hyper.fs_write(i * BLK, &vec![0xA0 + i as u8; BLK]).await.expect("write");
        }
        let _ = hyper.fs_flush().await.expect("flush");
        let _ = hyper.fs_release().await;
    }

    let staging: S3Staging = S3Staging::from(
        &client,
        StagingConfig::new_s3_uri(tf.uri(), None),
        HyperFileRuntimeConfig::default(),
    ).await.expect("staging");

    let names = object_names(&client, tf.uri()).await;
    let mut partials = 0;
    for n in &names {
        let Some((_, part)) = n.split_once('.') else { continue };
        let part: u16 = part.parse().expect("part index");
        let ss = staging.open(SegmentId::with_part(1, part)).await
            .unwrap_or_else(|e| panic!("open part {}: {}", part, e));
        let marked = ss.hdr.s_flags & hyperfile::ondisk::SEGMENT_FLAG_PARTIAL != 0;
        if part == 0 {
            assert!(!marked, "part 0 is the checkpoint and must not be marked partial");
        } else {
            assert!(marked, "part {} is a partial and is not marked", part);
            partials += 1;
        }
        // Every part carries the inode, so any of them can be parsed on its own.
        assert_eq!(ss.hdr.s_cno, 1, "part {} names the wrong checkpoint", part);
        assert!(ss.hdr.s_inode.i_size > 0,
            "part {} carries an empty inode, so it cannot be parsed alone", part);
        // And describes its own blocks, not the checkpoint's.
        assert_eq!(ss.blocks.len(), ss.hdr.s_ndatablk as usize);
    }
    assert!(partials >= 2, "expected several partials among {:?}", names);

    tf.cleanup(&client).await;
}

/// A checkpoint whose partials were written but whose consistency point never
/// arrived is not openable, and does not become openable by having partials.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn an_abandoned_checkpoint_is_not_openable() {
    let _ = env_logger::try_init();
    let client = make_client().await;
    let tf = TestFile::new(&client).await;
    let config = parted_config(tf.uri(), 4, 2 * BLK, true);

    {
        let mut hyper = Hyper::create(
            client.clone(), config.clone(),
            HyperFileFlags::from_flags(FileFlags::rdwr()),
            HyperFileMode::from_mode(FileMode::default_file()),
        ).await.expect("create");
        // Publish one checkpoint so the container has a state to fall back to.
        let _ = hyper.fs_write(0, &vec![0xC0; BLK]).await.expect("write");
        let published = hyper.fs_flush().await.expect("flush");

        // Now accumulate partials and walk away without asking for a checkpoint.
        for i in 1..20 {
            let _ = hyper.fs_write(i * BLK, &vec![0xD0 + i as u8; BLK]).await.expect("write");
        }
        let names = object_names(&client, tf.uri()).await;
        let abandoned: Vec<&String> = names.iter()
            .filter(|n| n.starts_with(&format!("{:0>10}", published + 1))).collect();
        assert!(!abandoned.is_empty(), "no partials to abandon among {:?}", names);
        assert!(!abandoned.iter().any(|n| n.ends_with(".0")),
            "the abandoned checkpoint has a part 0: {:?}", abandoned);
        std::mem::forget(hyper);
    }

    // The container opens at the checkpoint that was asked for, and the one that
    // was not is absent rather than partly there.
    let mut hyper = Hyper::open(
        client.clone(), config.clone(),
        HyperFileFlags::from_flags(FileFlags::rdonly()),
    ).await.expect("reopen");
    let mut buf = vec![0u8; BLK];
    let _ = hyper.fs_read(0, &mut buf).await.expect("read");
    assert!(buf.iter().all(|b| *b == 0xC0), "the published checkpoint did not survive");
    let mut buf = vec![0u8; BLK];
    let n = hyper.fs_read(BLK, &mut buf).await.expect("read");
    assert!(n == 0 || buf.iter().all(|b| *b != 0xD1),
        "an abandoned partial's data came back, so something referenced it");
    let _ = hyper.fs_release().await;

    // Opening the abandoned number by cno fails: resolution looks for part 0 and
    // the bare name, and neither is there.
    let res = Hyper::open_cno(
        client.clone(), config.clone(),
        HyperFileFlags::from_flags(FileFlags::rdonly()), 2).await;
    assert!(res.is_err(), "an abandoned checkpoint opened as if it were complete");

    tf.cleanup(&client).await;
}

/// Listing a container's checkpoints counts each one once, however many objects it
/// took to write.
///
/// The list is what a tool walking a container's history reads. Recognising a
/// checkpoint by parsing the whole object name as a number stopped working the moment
/// a name could carry a part suffix, and the failure is quiet: no error, an empty
/// list, a container that looks like it has no history at all.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn listing_counts_each_checkpoint_once() {
    use hyperfile::segment::SegmentReadWrite;
    use hyperfile::staging::s3::S3Staging;
    use hyperfile::SegmentId;

    let _ = env_logger::try_init();
    let client = make_client().await;
    let tf = TestFile::new(&client).await;
    // Small threshold so each round is written as partials plus a part 0.
    let config = parted_config(tf.uri(), 4, 2 * BLK, true);
    const ROUNDS: usize = 3;

    {
        let mut hyper = Hyper::create(
            client.clone(), config.clone(),
            HyperFileFlags::from_flags(FileFlags::rdwr()),
            HyperFileMode::from_mode(FileMode::default_file()),
        ).await.expect("create");
        for r in 0..ROUNDS {
            for i in 0..10 {
                let at = (r * 10 + i) * BLK;
                let _ = hyper.fs_write(at, &vec![0xE0 + (r * 10 + i) as u8; BLK]).await.expect("write");
            }
            let _ = hyper.fs_flush().await.expect("flush");
        }
        let _ = hyper.fs_release().await;
    }

    let staging: S3Staging = S3Staging::from(
        &client,
        StagingConfig::new_s3_uri(tf.uri(), None),
        HyperFileRuntimeConfig::default(),
    ).await.expect("staging");

    // Everything, which is what a history walk asks for.
    let all = staging.list(SegmentId::new(0)).await.expect("list");
    assert!(!all.is_empty(),
        "listing a container written by this version returned nothing");
    assert_eq!(all.len(), ROUNDS,
        "{} flushes should list as {} checkpoints, got {:?} — a checkpoint written as \
         several objects must still count once", ROUNDS, ROUNDS, all);
    assert!(all.iter().all(|s| s.part_id().is_none()),
        "a checkpoint list should name checkpoints, not their objects: {:?}", all);
    let mut sorted = all.clone();
    sorted.sort();
    assert_eq!(all, sorted, "the list should be ascending: {:?}", all);

    // Bounded: at or below the given checkpoint.
    let upto = staging.list(all[1]).await.expect("list upto");
    assert_eq!(upto.len(), 2, "listing up to {} gave {:?}", all[1], upto);

    tf.cleanup(&client).await;
}
