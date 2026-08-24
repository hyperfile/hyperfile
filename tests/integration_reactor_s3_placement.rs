//! Read-only placement queries (`fh_read_plan` / `fh_block_placement`).
//!
//! Reads are merged only for blocks that adjoin within one segment, so
//! where a file's blocks landed sets the cost of reading it — and that is
//! decided here, below any caller. These two queries expose the decision
//! without influencing it: the plan says what a read would cost, the
//! placement says what the cost is made of.
//!
//! The test that carries the most weight is
//! `plan_agrees_with_what_the_read_does`. The reason for exposing the
//! planner rather than raw placement is that a caller computing the merging
//! rule for itself would be writing a second implementation of it, which
//! can disagree with the first silently. That is only true while the query
//! really is the planner, so it is asserted against a real read's request
//! count, on a layout built to make the two easy to disagree about.
//!
//! ```bash
//! HYPERFILE_TEST_BUCKET=<your-bucket> HYPERFILE_TEST_REGION=<your-region> \
//!     cargo test --test integration_reactor_s3_placement -- --ignored --test-threads=1
//! ```

#![cfg(feature = "reactor")]

#[allow(dead_code)]
mod common;
#[allow(dead_code)]
mod common_reactor;

use common::*;
use common_reactor::*;

use hyperfile::file::PlannedRead;
use hyperfile::file::fh::HyperFileHandler;
use hyperfile::file::flags::FileFlags;
use hyperfile::file::mode::FileMode;

const BLK: usize = 4096;

/// Total bytes the entries account for, and how many are requests.
fn tally(plan: &[PlannedRead]) -> (u64, usize) {
    let bytes = plan.iter().map(|e| e.range().1).sum();
    let gets = plan.iter().filter(|e| e.is_get()).count();
    (bytes, gets)
}

/// Write `nblk` blocks, flush, and close.
async fn seed(client: &aws_sdk_s3::Client, reactor: &HyperReactor, uri: &str, nblk: usize) {
    let mut fh = HyperFileHandler::fh_open_or_create_with_default_opt(
        reactor, client, uri, FileFlags::rdwr(), FileMode::default_file(),
    ).await.expect("fh create");
    for b in 0..nblk {
        let _ = fh.fh_write(b * BLK, &vec![(b % 251) as u8; BLK]).await.expect("write");
    }
    let _ = fh.fh_flush().await.expect("flush");
    let _ = fh.fh_release().await;
}

/// Write the even blocks, flush, then the odd ones, flush. Consecutive file
/// blocks then alternate between two segments, which is what a read cannot
/// merge across.
async fn seed_alternating(client: &aws_sdk_s3::Client, reactor: &HyperReactor, uri: &str, nblk: usize) {
    let mut fh = HyperFileHandler::fh_open_or_create_with_default_opt(
        reactor, client, uri, FileFlags::rdwr(), FileMode::default_file(),
    ).await.expect("fh create");
    for b in (0..nblk).filter(|b| b % 2 == 0) {
        let _ = fh.fh_write(b * BLK, &vec![(b % 251) as u8; BLK]).await.expect("write");
    }
    let _ = fh.fh_flush().await.expect("flush");
    for b in (0..nblk).filter(|b| b % 2 == 1) {
        let _ = fh.fh_write(b * BLK, &vec![(b % 251) as u8; BLK]).await.expect("write");
    }
    let _ = fh.fh_flush().await.expect("flush");
    let _ = fh.fh_release().await;
}

/// The plan's request count is what the read actually issues, on a layout
/// where merging genuinely has to make decisions.
///
/// This is the property the whole query rests on. If it ever fails, callers
/// selecting files by predicted cost are being told a layout is fine when it
/// is not.
#[tokio::test]
#[ignore]
async fn plan_agrees_with_what_the_read_does() {
    let _ = env_logger::try_init();
    let client = make_client().await;
    let tf = TestFile::new(&client).await;
    let reactor = make_reactor();
    const NB: usize = 64;

    seed_alternating(&client, &reactor, tf.uri(), NB).await;

    let mut fh = HyperFileHandler::fh_open(&reactor, &client, tf.uri(), FileFlags::rdonly())
        .await.expect("fh open");
    let _ = fh.fh_read_timing_reset().await;

    let plan = fh.fh_read_plan(0, (NB * BLK) as u64).await.expect("read plan");
    let (bytes, gets) = tally(&plan);
    assert_eq!(bytes, (NB * BLK) as u64, "entries must account for the whole range");

    // Asking must not fetch data.
    let after_ask = fh.fh_read_timing().await.expect("timing");
    assert_eq!(after_ask.data_gets, 0, "asking about a read must issue no data request");

    // Now do the read the plan described.
    let mut buf = vec![0u8; NB * BLK];
    let n = fh.fh_read(0, &mut buf).await.expect("read");
    assert_eq!(n, NB * BLK);

    let after_read = fh.fh_read_timing().await.expect("timing");
    assert_eq!(
        after_read.data_gets as usize, gets,
        "the plan predicted {} requests and the read issued {}",
        gets, after_read.data_gets,
    );
    // The layout was built to be unmergeable, so this is not the trivial
    // case of one request for everything.
    assert!(gets > 1, "alternating segments should not merge into one request, got {}", gets);

    let _ = fh.fh_release().await;
    tf.cleanup(&client).await;
}

/// A file written in one pass merges into few requests, and the plan says
/// so — the other end of the range from the test above.
#[tokio::test]
#[ignore]
async fn a_file_written_in_one_pass_merges() {
    let _ = env_logger::try_init();
    let client = make_client().await;
    let tf = TestFile::new(&client).await;
    let reactor = make_reactor();
    const NB: usize = 64;

    seed(&client, &reactor, tf.uri(), NB).await;

    let mut fh = HyperFileHandler::fh_open(&reactor, &client, tf.uri(), FileFlags::rdonly())
        .await.expect("fh open");
    let _ = fh.fh_read_timing_reset().await;

    let plan = fh.fh_read_plan(0, (NB * BLK) as u64).await.expect("read plan");
    let (bytes, gets) = tally(&plan);
    assert_eq!(bytes, (NB * BLK) as u64);
    assert_eq!(gets, 1, "one pass should lay the blocks down adjacently, got {} requests", gets);

    let mut buf = vec![0u8; NB * BLK];
    let _ = fh.fh_read(0, &mut buf).await.expect("read");
    let t = fh.fh_read_timing().await.expect("timing");
    assert_eq!(t.data_gets, 1, "and the read should issue that one request");

    let _ = fh.fh_release().await;
    tf.cleanup(&client).await;
}

/// A run longer than `read_get_max_bytes` splits, and the plan splits it the
/// same way the read does.
///
/// This is the only way two consecutive requests land in the same segment: a
/// flush writes its blocks sorted and packed, so two file-consecutive blocks
/// in one segment are always adjacent within it. Which makes the budget the
/// one thing a hand-rolled cost model would have no reason to guess at, and
/// the reason this case is worth its own test.
#[tokio::test]
#[ignore]
async fn a_run_longer_than_the_budget_splits() {
    let _ = env_logger::try_init();
    let client = make_client().await;
    let tf = TestFile::new(&client).await;
    let reactor = make_reactor();
    // Comfortably over the 16 MiB default, in one contiguous run.
    const SZ: usize = 20 * 1024 * 1024;

    {
        let mut fh = HyperFileHandler::fh_open_or_create_with_default_opt(
            &reactor, &client, tf.uri(), FileFlags::rdwr(), FileMode::default_file(),
        ).await.expect("fh create");
        let _ = fh.fh_write(0, &vec![0x7E; SZ]).await.expect("write");
        let _ = fh.fh_flush().await.expect("flush");
        let _ = fh.fh_release().await;
    }

    let mut fh = HyperFileHandler::fh_open(&reactor, &client, tf.uri(), FileFlags::rdonly())
        .await.expect("fh open");
    let _ = fh.fh_read_timing_reset().await;

    let plan = fh.fh_read_plan(0, SZ as u64).await.expect("read plan");
    let (bytes, gets) = tally(&plan);
    assert_eq!(bytes, SZ as u64, "entries must account for the whole range");
    assert!(gets > 1, "a 20 MiB run cannot be one request under a 16 MiB budget, got {}", gets);

    // Consecutive requests here are in one segment and adjacent within it —
    // split only by the budget, which is what makes this case distinct.
    let gets_only: Vec<_> = plan.iter().filter(|e| e.is_get()).collect();
    if let [PlannedRead::Get { segid: s0, .. }, PlannedRead::Get { segid: s1, .. }] = gets_only[..2] {
        assert_eq!(s0, s1, "one flush, so one segment");
    }

    let mut buf = vec![0u8; SZ];
    let n = fh.fh_read(0, &mut buf).await.expect("read");
    assert_eq!(n, SZ);
    let t = fh.fh_read_timing().await.expect("timing");
    assert_eq!(
        t.data_gets as usize, gets,
        "the plan predicted {} requests and the read issued {}", gets, t.data_gets,
    );

    let _ = fh.fh_release().await;
    tf.cleanup(&client).await;
}

/// The entries are contiguous and in file order, so a caller can walk them
/// without reconstructing offsets.
#[tokio::test]
#[ignore]
async fn entries_tile_the_range_in_order() {
    let _ = env_logger::try_init();
    let client = make_client().await;
    let tf = TestFile::new(&client).await;
    let reactor = make_reactor();
    const NB: usize = 32;

    seed_alternating(&client, &reactor, tf.uri(), NB).await;

    let mut fh = HyperFileHandler::fh_open(&reactor, &client, tf.uri(), FileFlags::rdonly())
        .await.expect("fh open");

    // An unaligned window, to catch an entry that starts on a block boundary
    // when it should start where the caller asked.
    let off = (3 * BLK + 1000) as u64;
    let len = (5 * BLK + 77) as u64;
    let plan = fh.fh_read_plan(off, len).await.expect("read plan");
    assert!(!plan.is_empty());

    let mut at = off;
    for e in &plan {
        let (e_off, e_len) = e.range();
        assert_eq!(e_off, at, "entries must tile the range without gap or overlap");
        assert!(e_len > 0, "an entry covering nothing should not be reported");
        at += e_len;
    }
    assert_eq!(at, off + len, "entries must reach the end of the range");

    let _ = fh.fh_release().await;
    tf.cleanup(&client).await;
}

/// Placement of a file written in one pass: every block in one segment, at
/// ascending offsets.
#[tokio::test]
#[ignore]
async fn placement_of_one_pass_is_one_segment_ascending() {
    let _ = env_logger::try_init();
    let client = make_client().await;
    let tf = TestFile::new(&client).await;
    let reactor = make_reactor();
    const NB: usize = 32;

    seed(&client, &reactor, tf.uri(), NB).await;

    let mut fh = HyperFileHandler::fh_open(&reactor, &client, tf.uri(), FileFlags::rdonly())
        .await.expect("fh open");
    let places = fh.fh_block_placement(0, NB).await.expect("placement");
    assert_eq!(places.len(), NB, "one answer per block asked about");

    let mut segs = std::collections::BTreeSet::new();
    let mut last_off = None;
    for (i, p) in places.iter().enumerate() {
        let (segid, at) = p.expect("a flushed block must have a place");
        segs.insert(segid);
        if let Some(prev) = last_off {
            assert!(at > prev, "block {} went backwards in the segment: {} after {}", i, at, prev);
        }
        last_off = Some(at);
    }
    assert_eq!(segs.len(), 1, "one flush should put them all in one segment, got {:?}", segs);

    let _ = fh.fh_release().await;
    tf.cleanup(&client).await;
}

/// Placement explains a cost the plan only reports: alternating flushes put
/// consecutive blocks in different segments.
#[tokio::test]
#[ignore]
async fn placement_shows_why_a_read_does_not_merge() {
    let _ = env_logger::try_init();
    let client = make_client().await;
    let tf = TestFile::new(&client).await;
    let reactor = make_reactor();
    const NB: usize = 32;

    seed_alternating(&client, &reactor, tf.uri(), NB).await;

    let mut fh = HyperFileHandler::fh_open(&reactor, &client, tf.uri(), FileFlags::rdonly())
        .await.expect("fh open");
    let places = fh.fh_block_placement(0, NB).await.expect("placement");

    let segs: std::collections::BTreeSet<_> = places.iter()
        .filter_map(|p| p.map(|(s, _)| s)).collect();
    assert_eq!(segs.len(), 2, "two flushes, two segments, got {:?}", segs);

    // Neighbouring blocks are in different segments, which is exactly what
    // the read path cannot merge across.
    let seg_of: Vec<_> = places.iter().map(|p| p.expect("mapped").0).collect();
    let switches = seg_of.windows(2).filter(|w| w[0] != w[1]).count();
    assert!(switches > NB / 2, "expected the segment to change at almost every block, got {} changes", switches);

    let _ = fh.fh_release().await;
    tf.cleanup(&client).await;
}

/// A hole and a never-written tail have no place. Neither is an error.
#[tokio::test]
#[ignore]
async fn a_hole_has_no_place() {
    let _ = env_logger::try_init();
    let client = make_client().await;
    let tf = TestFile::new(&client).await;
    let reactor = make_reactor();

    let mut fh = HyperFileHandler::fh_open_or_create_with_default_opt(
        &reactor, &client, tf.uri(), FileFlags::rdwr(), FileMode::default_file(),
    ).await.expect("fh create");
    // Blocks 0 and 3 written; 1 and 2 left as a hole.
    let _ = fh.fh_write(0, &vec![0xA1; BLK]).await.expect("write");
    let _ = fh.fh_write(3 * BLK, &vec![0xB2; BLK]).await.expect("write");
    let _ = fh.fh_flush().await.expect("flush");

    let places = fh.fh_block_placement(0, 6).await.expect("placement");
    assert!(places[0].is_some(), "block 0 was written");
    assert!(places[1].is_none(), "block 1 is a hole");
    assert!(places[2].is_none(), "block 2 is a hole");
    assert!(places[3].is_some(), "block 3 was written");
    assert!(places[4].is_none(), "block 4 is past the end");
    assert!(places[5].is_none(), "block 5 is past the end");

    // And the plan reports the hole as needing no request.
    let plan = fh.fh_read_plan(BLK as u64, (2 * BLK) as u64).await.expect("read plan");
    let (bytes, gets) = tally(&plan);
    assert_eq!(bytes, (2 * BLK) as u64);
    assert_eq!(gets, 0, "reading a hole needs no object request");

    let _ = fh.fh_release().await;
    tf.cleanup(&client).await;
}

/// A block written and not yet flushed has no place on staging, and the
/// plan reports it as needing no request because it is in memory.
#[tokio::test]
#[ignore]
async fn an_unflushed_block_has_no_place_and_needs_no_request() {
    let _ = env_logger::try_init();
    let client = make_client().await;
    let tf = TestFile::new(&client).await;
    let reactor = make_reactor();

    let mut fh = HyperFileHandler::fh_open_or_create_with_default_opt(
        &reactor, &client, tf.uri(), FileFlags::rdwr(), FileMode::default_file(),
    ).await.expect("fh create");
    let _ = fh.fh_write(0, &vec![0xC3; 4 * BLK]).await.expect("write");
    // No flush.

    let places = fh.fh_block_placement(0, 4).await.expect("placement");
    for (i, p) in places.iter().enumerate() {
        assert!(p.is_none(), "block {} is dirty and unflushed, so it has no place yet", i);
    }

    let plan = fh.fh_read_plan(0, (4 * BLK) as u64).await.expect("read plan");
    let (bytes, gets) = tally(&plan);
    assert_eq!(bytes, (4 * BLK) as u64);
    assert_eq!(gets, 0, "unflushed blocks are in memory, so no request");

    let _ = fh.fh_release().await;
    tf.cleanup(&client).await;
}

/// The plan follows the cache, which is the documented catch: the same file
/// costs requests cold and nothing warm. Placement does not move.
#[tokio::test]
#[ignore]
async fn the_plan_follows_the_cache_but_placement_does_not() {
    let _ = env_logger::try_init();
    let client = make_client().await;
    let tf = TestFile::new(&client).await;
    let reactor = make_reactor();
    const NB: usize = 16;

    seed(&client, &reactor, tf.uri(), NB).await;

    let mut fh = HyperFileHandler::fh_open(&reactor, &client, tf.uri(), FileFlags::rdonly())
        .await.expect("fh open");

    let cold = fh.fh_read_plan(0, (NB * BLK) as u64).await.expect("read plan");
    let (_, cold_gets) = tally(&cold);
    assert!(cold_gets >= 1, "a cold file must cost at least one request");
    let cold_places = fh.fh_block_placement(0, NB).await.expect("placement");

    // Warm the range into the data cache.
    let _ = fh.fh_read_ahead(0, NB * BLK).await.expect("read ahead");

    let warm = fh.fh_read_plan(0, (NB * BLK) as u64).await.expect("read plan");
    let (warm_bytes, warm_gets) = tally(&warm);
    assert_eq!(warm_bytes, (NB * BLK) as u64, "still accounts for the whole range");
    assert_eq!(warm_gets, 0, "every block is resident, so nothing would be fetched");

    let warm_places = fh.fh_block_placement(0, NB).await.expect("placement");
    assert_eq!(warm_places, cold_places, "warming must not change where the blocks are");

    let _ = fh.fh_release().await;
    tf.cleanup(&client).await;
}

/// The batch forms answer per range, in order, with the same answers the
/// single-range forms give.
#[tokio::test]
#[ignore]
async fn the_batch_forms_answer_per_range_in_order() {
    let _ = env_logger::try_init();
    let client = make_client().await;
    let tf = TestFile::new(&client).await;
    let reactor = make_reactor();
    const NB: usize = 48;

    seed_alternating(&client, &reactor, tf.uri(), NB).await;

    let mut fh = HyperFileHandler::fh_open(&reactor, &client, tf.uri(), FileFlags::rdonly())
        .await.expect("fh open");

    let ranges: Vec<(u64, u64)> = vec![
        (0, (4 * BLK) as u64),
        ((20 * BLK) as u64, (3 * BLK) as u64),
        ((7 * BLK + 100) as u64, (2 * BLK) as u64),
    ];
    let batch = fh.fh_read_plan_many(&ranges).await.expect("read plan many");
    assert_eq!(batch.len(), ranges.len(), "one answer per range");
    for (i, (off, len)) in ranges.iter().copied().enumerate() {
        let one = fh.fh_read_plan(off, len).await.expect("read plan");
        assert_eq!(batch[i], one, "range {} answered differently in a batch", i);
        assert_eq!(tally(&batch[i]).0, len, "range {} must be accounted for", i);
    }

    let b_ranges: Vec<(u64, usize)> = vec![(0, 4), (20, 3), (7, 9)];
    let b_batch = fh.fh_block_placement_many(&b_ranges).await.expect("placement many");
    assert_eq!(b_batch.len(), b_ranges.len());
    for (i, (start, n)) in b_ranges.iter().copied().enumerate() {
        let one = fh.fh_block_placement(start, n).await.expect("placement");
        assert_eq!(b_batch[i], one, "block range {} answered differently in a batch", i);
        assert_eq!(b_batch[i].len(), n);
    }

    let _ = fh.fh_release().await;
    tf.cleanup(&client).await;
}

/// An empty batch is not an error, and asks nothing.
#[tokio::test]
#[ignore]
async fn an_empty_batch_asks_nothing() {
    let _ = env_logger::try_init();
    let client = make_client().await;
    let tf = TestFile::new(&client).await;
    let reactor = make_reactor();

    seed(&client, &reactor, tf.uri(), 4).await;

    let mut fh = HyperFileHandler::fh_open(&reactor, &client, tf.uri(), FileFlags::rdonly())
        .await.expect("fh open");
    assert!(fh.fh_read_plan_many(&[]).await.expect("empty plan batch").is_empty());
    assert!(fh.fh_block_placement_many(&[]).await.expect("empty placement batch").is_empty());

    // A zero-length range has nothing to account for.
    let plan = fh.fh_read_plan(0, 0).await.expect("zero length");
    assert!(plan.is_empty(), "a zero-length range has no entries");
    assert!(fh.fh_block_placement(0, 0).await.expect("zero blocks").is_empty());

    let _ = fh.fh_release().await;
    tf.cleanup(&client).await;
}
