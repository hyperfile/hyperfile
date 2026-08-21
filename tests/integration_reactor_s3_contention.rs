//! Concurrent use of one reactor handle must not deadlock.
//!
//! The per-file semaphore has a single permit for a writable handle
//! without `range-lock`, and a write's retrieve carries that permit from
//! `spawn_write` until `absorb_write` — across a hop that only the
//! handler task can run. Any operation that *waited* for the permit on
//! the handler task therefore deadlocked against the one thing that
//! could end the wait: the handler blocked, the callback that would
//! release the permit never ran, and the process went idle for good.
//!
//! Reported from a FUSE filesystem doing concurrent `fh_read_owned`
//! while another thread wrote and flushed. It reproduced here on the
//! first iteration.
//!
//! Every operation that takes the permit is now non-blocking on the
//! handler task and puts its request back on a contention miss, and `cb`
//! outranks `highprio` so the retry can never overtake the hop it waits
//! on. These tests drive all of them at once.
//!
//! A failure here is a hang, not an assertion, so each test carries its
//! own watchdog and reports which operation was outstanding.
//!
//! ```bash
//! HYPERFILE_TEST_BUCKET=<your-bucket> HYPERFILE_TEST_REGION=<your-region> \
//!     cargo test --test integration_reactor_s3_contention -- --ignored --test-threads=1
//! ```

#![cfg(feature = "reactor")]

#[allow(dead_code)]
mod common;
#[allow(dead_code)]
mod common_reactor;

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use common::*;
use common_reactor::*;

use hyperfile::file::fh::HyperFileHandler;
use hyperfile::file::flags::FileFlags;
use hyperfile::file::mode::FileMode;

const BLK: usize = 4096;
const NBLK: u64 = 128;

/// Fails the test if the owning task stops making progress.
struct Watchdog {
    stop: Arc<AtomicBool>,
    progress: Arc<AtomicUsize>,
    inflight: Arc<AtomicUsize>,
    marker: Arc<AtomicUsize>,
}

const MARKS: [&str; 7] = ["-", "write", "flush", "block_mut", "block", "truncate", "batch"];

impl Watchdog {
    fn new() -> Self {
        Self {
            stop: Arc::new(AtomicBool::new(false)),
            progress: Arc::new(AtomicUsize::new(0)),
            inflight: Arc::new(AtomicUsize::new(0)),
            marker: Arc::new(AtomicUsize::new(0)),
        }
    }

    /// Watches for a stall and returns true if one happened.
    fn spawn(&self, limit_secs: usize) -> tokio::task::JoinHandle<bool> {
        let (stop, progress, inflight, marker) =
            (self.stop.clone(), self.progress.clone(), self.inflight.clone(), self.marker.clone());
        tokio::spawn(async move {
            // `last` starts at usize::MAX so the first sample always
            // counts as movement; otherwise a stall on the very first
            // iteration — which is how this reproduced — would leave
            // progress at 0 and look like no change from the initial 0.
            let mut last = usize::MAX;
            let mut idle = 0usize;
            while !stop.load(Ordering::Relaxed) {
                tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                let p = progress.load(Ordering::SeqCst);
                if p == last {
                    idle += 1;
                    if idle >= limit_secs {
                        eprintln!("STALLED {}s: progress={} inflight={} owner_in={}",
                            idle, p, inflight.load(Ordering::SeqCst),
                            MARKS[marker.load(Ordering::SeqCst).min(6)]);
                        stop.store(true, Ordering::SeqCst);
                        return true;
                    }
                } else {
                    idle = 0;
                    last = p;
                }
            }
            false
        })
    }
}

async fn seed(client: &aws_sdk_s3::Client, reactor: &HyperReactor, uri: &str) {
    let mut fh = HyperFileHandler::fh_open_or_create_with_default_opt(
        reactor, client, uri, FileFlags::rdwr(), FileMode::default_file(),
    ).await.expect("create");
    let _ = fh.fh_write(0, &vec![0x5Au8; NBLK as usize * BLK]).await.expect("write");
    let _ = fh.fh_flush().await.expect("flush");
    let _ = fh.fh_release().await.expect("release");
}

/// The reported shape: concurrent `fh_read_owned` from spawned tasks
/// while the owning task writes, edits blocks and flushes.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore]
async fn concurrent_reads_do_not_stall_writes() {
    let _ = env_logger::try_init();
    let client = make_client().await;
    let tf = TestFile::new(&client).await;
    let reactor = make_reactor();
    seed(&client, &reactor, tf.uri()).await;

    // Writable, no range-lock: exactly one permit for the whole file.
    let fh = HyperFileHandler::fh_open(&reactor, &client, tf.uri(), FileFlags::rdwr())
        .await.expect("open");

    let wd = Watchdog::new();
    let watch = wd.spawn(30);

    let mut readers = Vec::new();
    for r in 0..6u64 {
        let (c, stop, inflight) = (fh.clone(), wd.stop.clone(), wd.inflight.clone());
        readers.push(tokio::spawn(async move {
            let mut i = r;
            while !stop.load(Ordering::Relaxed) {
                inflight.fetch_add(1, Ordering::SeqCst);
                let res = c.fh_read_owned((i % NBLK) as usize * BLK, BLK).await;
                inflight.fetch_sub(1, Ordering::SeqCst);
                if res.is_err() { break; }
                i = i.wrapping_add(7);
                tokio::task::yield_now().await;
            }
        }));
    }

    let owner = {
        let (mut c, stop, marker, progress) =
            (fh.clone(), wd.stop.clone(), wd.marker.clone(), wd.progress.clone());
        tokio::spawn(async move {
            for n in 0..300u64 {
                if stop.load(Ordering::Relaxed) { break; }

                marker.store(1, Ordering::SeqCst);
                let off = ((n * 13) % NBLK) as usize * BLK + 11;
                c.fh_write(off, &vec![(n & 0xff) as u8; 900]).await.expect("write");

                marker.store(3, Ordering::SeqCst);
                c.fh_with_block_mut((n * 5) % NBLK, false, move |b| { b[0] = (n & 0xff) as u8; })
                    .await.expect("block_mut").expect("mapped");

                marker.store(4, Ordering::SeqCst);
                c.fh_with_block((n * 3) % NBLK, |b| b[0]).await.expect("block").expect("mapped");

                if n % 3 == 0 {
                    marker.store(2, Ordering::SeqCst);
                    c.fh_flush().await.expect("flush");
                }
                marker.store(0, Ordering::SeqCst);
                progress.fetch_add(1, Ordering::SeqCst);
            }
        })
    };

    // Race the owner against the watchdog. On a stall the owner never
    // returns, so waiting for it would hang the test instead of failing
    // it — which is how this bug presents in the first place.
    let mut owner_res = None;
    let stalled = tokio::select! {
        r = owner => { owner_res = Some(r); false },
        s = watch => s.unwrap_or(true),
    };
    wd.stop.store(true, Ordering::SeqCst);
    assert!(!stalled, "the handler stalled: {} iterations completed",
        wd.progress.load(Ordering::SeqCst));
    for r in readers { let _ = r.await; }
    owner_res.expect("owner did not finish").expect("owner task panicked");
    assert_eq!(wd.progress.load(Ordering::SeqCst), 300);

    let mut fh = fh;
    let _ = fh.fh_flush().await.expect("final flush");
    let _ = fh.fh_release().await.expect("release");
    tf.cleanup(&client).await;
}

/// The same contention, but the owning task also drives the other
/// operations that take the permit: truncate and the batch writes.
/// Each of those had the same blocking acquire.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore]
async fn concurrent_reads_do_not_stall_truncate_or_batch() {
    let _ = env_logger::try_init();
    let client = make_client().await;
    let tf = TestFile::new(&client).await;
    let reactor = make_reactor();
    seed(&client, &reactor, tf.uri()).await;

    let fh = HyperFileHandler::fh_open(&reactor, &client, tf.uri(), FileFlags::rdwr())
        .await.expect("open");

    let wd = Watchdog::new();
    let watch = wd.spawn(30);

    let mut readers = Vec::new();
    for r in 0..6u64 {
        let (c, stop, inflight) = (fh.clone(), wd.stop.clone(), wd.inflight.clone());
        readers.push(tokio::spawn(async move {
            let mut i = r;
            while !stop.load(Ordering::Relaxed) {
                inflight.fetch_add(1, Ordering::SeqCst);
                // Read inside the surviving region so a shrink cannot
                // turn this into a short read the loop misreads.
                let res = c.fh_read_owned((i % 16) as usize * BLK, BLK).await;
                inflight.fetch_sub(1, Ordering::SeqCst);
                if res.is_err() { break; }
                i = i.wrapping_add(3);
                tokio::task::yield_now().await;
            }
        }));
    }

    let owner = {
        let (mut c, stop, marker, progress) =
            (fh.clone(), wd.stop.clone(), wd.marker.clone(), wd.progress.clone());
        tokio::spawn(async move {
            for n in 0..120u64 {
                if stop.load(Ordering::Relaxed) { break; }

                marker.store(1, Ordering::SeqCst);
                c.fh_write(((n * 7) % 16) as usize * BLK, &vec![(n & 0xff) as u8; BLK])
                    .await.expect("write");

                marker.store(5, Ordering::SeqCst);
                // Grow and shrink above the region the readers use.
                let size = (32 + (n % 32) as usize) * BLK;
                c.fh_truncate(size).await.expect("truncate");

                marker.store(3, Ordering::SeqCst);
                c.fh_with_block_mut(n % 16, false, move |b| { b[1] = (n & 0xff) as u8; })
                    .await.expect("block_mut").expect("mapped");

                if n % 4 == 0 {
                    marker.store(2, Ordering::SeqCst);
                    c.fh_flush().await.expect("flush");
                }
                marker.store(0, Ordering::SeqCst);
                progress.fetch_add(1, Ordering::SeqCst);
            }
        })
    };

    // Race the owner against the watchdog. On a stall the owner never
    // returns, so waiting for it would hang the test instead of failing
    // it — which is how this bug presents in the first place.
    let mut owner_res = None;
    let stalled = tokio::select! {
        r = owner => { owner_res = Some(r); false },
        s = watch => s.unwrap_or(true),
    };
    wd.stop.store(true, Ordering::SeqCst);
    assert!(!stalled, "the handler stalled: {} iterations completed",
        wd.progress.load(Ordering::SeqCst));
    for r in readers { let _ = r.await; }
    owner_res.expect("owner did not finish").expect("owner task panicked");
    assert_eq!(wd.progress.load(Ordering::SeqCst), 120);

    let mut fh = fh;
    let _ = fh.fh_flush().await.expect("final flush");
    let _ = fh.fh_release().await.expect("release");
    tf.cleanup(&client).await;
}

/// Many writers on one handle, which is the case the permit exists for:
/// each write's retrieve holds it across a hop, so writers contend with
/// each other as well as with readers. Data must still be consistent
/// afterwards, not merely un-stalled.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore]
async fn concurrent_writers_and_readers_stay_consistent() {
    let _ = env_logger::try_init();
    let client = make_client().await;
    let tf = TestFile::new(&client).await;
    let reactor = make_reactor();
    seed(&client, &reactor, tf.uri()).await;

    let fh = HyperFileHandler::fh_open(&reactor, &client, tf.uri(), FileFlags::rdwr())
        .await.expect("open");

    let wd = Watchdog::new();
    let watch = wd.spawn(40);

    // Each writer owns a disjoint block and stamps it with its own id,
    // so the final contents are checkable.
    const WRITERS: u64 = 4;
    const ROUNDS: u64 = 40;
    let mut writers = Vec::new();
    for w in 0..WRITERS {
        let (mut c, stop, progress) = (fh.clone(), wd.stop.clone(), wd.progress.clone());
        writers.push(tokio::spawn(async move {
            for n in 0..ROUNDS {
                if stop.load(Ordering::Relaxed) { break; }
                let blk = w; // one block each
                let byte = (w * 16 + (n & 0x0f)) as u8;
                c.fh_write(blk as usize * BLK, &vec![byte; BLK]).await.expect("write");
                c.fh_with_block_mut(blk, false, move |b| { b[0] = byte; })
                    .await.expect("block_mut").expect("mapped");
                progress.fetch_add(1, Ordering::SeqCst);
            }
            // Leave a known final value.
            let final_byte = (w * 16 + 0xf) as u8;
            c.fh_write(w as usize * BLK, &vec![final_byte; BLK]).await.expect("final write");
            final_byte
        }));
    }

    let mut readers = Vec::new();
    for r in 0..4u64 {
        let (c, stop, inflight) = (fh.clone(), wd.stop.clone(), wd.inflight.clone());
        readers.push(tokio::spawn(async move {
            let mut i = r + WRITERS;
            while !stop.load(Ordering::Relaxed) {
                inflight.fetch_add(1, Ordering::SeqCst);
                let res = c.fh_read_owned((i % NBLK) as usize * BLK, BLK).await;
                inflight.fetch_sub(1, Ordering::SeqCst);
                if res.is_err() { break; }
                i = i.wrapping_add(5);
                tokio::task::yield_now().await;
            }
        }));
    }

    let flusher = {
        let (mut c, stop) = (fh.clone(), wd.stop.clone());
        tokio::spawn(async move {
            while !stop.load(Ordering::Relaxed) {
                tokio::time::sleep(std::time::Duration::from_millis(20)).await;
                if c.fh_flush().await.is_err() { break; }
            }
        })
    };

    // Same race as above: a stalled writer never returns.
    let all_writers = async move {
        let mut out = Vec::new();
        for w in writers {
            out.push(w.await.expect("writer task panicked"));
        }
        out
    };
    let mut expected = Vec::new();
    let stalled = tokio::select! {
        v = all_writers => { expected = v; false },
        s = watch => s.unwrap_or(true),
    };
    wd.stop.store(true, Ordering::SeqCst);
    assert!(!stalled, "the handler stalled: {} writes completed",
        wd.progress.load(Ordering::SeqCst));
    for r in readers { let _ = r.await; }
    let _ = flusher.await;
    assert_eq!(wd.progress.load(Ordering::SeqCst) as u64, WRITERS * ROUNDS);

    let mut fh = fh;
    let _ = fh.fh_flush().await.expect("final flush");
    let _ = fh.fh_release().await.expect("release");

    // Each writer's block must hold that writer's final value: no write
    // was lost or applied out of order under contention.
    let mut check = HyperFileHandler::fh_open(&reactor, &client, tf.uri(), FileFlags::rdonly())
        .await.expect("reopen");
    for (w, want) in expected.iter().enumerate() {
        let got = check.fh_read_owned(w * BLK, BLK).await.expect("read back");
        assert_eq!(got.len(), BLK);
        assert!(got.iter().all(|b| b == want),
            "block {w} should be all {want:#04x}, first byte {:#04x}", got[0]);
    }
    let _ = check.fh_release().await.expect("release");

    tf.cleanup(&client).await;
}

/// Maximal contention: every operation that takes the per-file permit,
/// issued concurrently from clones of one handle.
///
/// The permit is only held across a hop when a write has to fetch a
/// block first, which happens for an **unaligned** write — a
/// block-aligned one needs no retrieve and returns the permit within the
/// arm. So the writers here write across block boundaries deliberately;
/// with aligned writes the window barely exists and none of this is
/// exercised.
///
/// Each of `spawn_read`, `block_mut`, `truncate` and the batch writes
/// used to wait for the permit on the handler task. This drives them all
/// at once against a permit that is regularly held across a fetch.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore]
async fn every_permit_taking_operation_under_contention() {
    let _ = env_logger::try_init();
    let client = make_client().await;
    let tf = TestFile::new(&client).await;
    let reactor = make_reactor();
    seed(&client, &reactor, tf.uri()).await;

    let fh = HyperFileHandler::fh_open(&reactor, &client, tf.uri(), FileFlags::rdwr())
        .await.expect("open");

    let wd = Watchdog::new();
    let watch = wd.spawn(40);
    // The writers walk distinct blocks so each first touch is a cold
    // retrieve, which is when the permit is held across a fetch. The
    // fast operations get many more attempts, because each has to land
    // inside one of those few-millisecond windows to contend at all.
    const WRITES: u64 = 100;
    const ATTEMPTS: u64 = 600;

    let mut tasks: Vec<tokio::task::JoinHandle<()>> = Vec::new();

    // Unaligned writers: these are what hold the permit across a fetch.
    for w in 0..3u64 {
        let (mut c, stop, progress) = (fh.clone(), wd.stop.clone(), wd.progress.clone());
        tasks.push(tokio::spawn(async move {
            for n in 0..WRITES {
                if stop.load(Ordering::Relaxed) { break; }
                // Straddle a block boundary so a retrieve is needed, and
                // pick a block this task has not touched yet so it is
                // still cold.
                let off = (w * WRITES + n) as usize % 120 * BLK + BLK - 64;
                c.fh_write(off, &vec![(n & 0xff) as u8; 256]).await.expect("unaligned write");
                progress.fetch_add(1, Ordering::SeqCst);
            }
        }));
    }

    // Block edits.
    {
        let (mut c, stop, progress) = (fh.clone(), wd.stop.clone(), wd.progress.clone());
        tasks.push(tokio::spawn(async move {
            for n in 0..ATTEMPTS {
                if stop.load(Ordering::Relaxed) { break; }
                c.fh_with_block_mut(n % 120, false, move |b| { b[2] = (n & 0xff) as u8; })
                    .await.expect("block_mut").expect("mapped");
                progress.fetch_add(1, Ordering::SeqCst);
            }
        }));
    }

    // Read-only block access.
    {
        let (mut c, stop, progress) = (fh.clone(), wd.stop.clone(), wd.progress.clone());
        tasks.push(tokio::spawn(async move {
            for n in 0..ATTEMPTS {
                if stop.load(Ordering::Relaxed) { break; }
                c.fh_with_block(n % 120, |b| b[0]).await.expect("block").expect("mapped");
                progress.fetch_add(1, Ordering::SeqCst);
            }
        }));
    }

    // Truncate, kept strictly above every block the other tasks use, so
    // a shrink cannot unmap one out from under them. The others work
    // within the first 121 blocks; the seed is NBLK.
    {
        let (mut c, stop, progress) = (fh.clone(), wd.stop.clone(), wd.progress.clone());
        tasks.push(tokio::spawn(async move {
            for n in 0..ATTEMPTS {
                if stop.load(Ordering::Relaxed) { break; }
                c.fh_truncate((NBLK as usize + (n % 32) as usize) * BLK).await.expect("truncate");
                progress.fetch_add(1, Ordering::SeqCst);
            }
        }));
    }

    // Byte reads.
    for r in 0..4u64 {
        let (c, stop, inflight) = (fh.clone(), wd.stop.clone(), wd.inflight.clone());
        tasks.push(tokio::spawn(async move {
            let mut c = c;
            let mut i = r;
            while !stop.load(Ordering::Relaxed) {
                inflight.fetch_add(1, Ordering::SeqCst);
                let res = c.fh_read_owned((i % 40) as usize * BLK, BLK).await;
                inflight.fetch_sub(1, Ordering::SeqCst);
                if res.is_err() { break; }
                i = i.wrapping_add(3);
                tokio::task::yield_now().await;
            }
        }));
    }

    // Flushes throughout, so pipelines and flush windows overlap.
    let flusher = {
        let (mut c, stop) = (fh.clone(), wd.stop.clone());
        tokio::spawn(async move {
            while !stop.load(Ordering::Relaxed) {
                tokio::time::sleep(std::time::Duration::from_millis(15)).await;
                if c.fh_flush().await.is_err() { break; }
            }
        })
    };

    // The first six tasks are bounded; readers run until told to stop.
    let bounded: Vec<_> = tasks.drain(0..6).collect();
    let all_bounded = async move {
        for t in bounded { t.await.expect("task panicked"); }
    };
    let stalled = tokio::select! {
        _ = all_bounded => false,
        s = watch => s.unwrap_or(true),
    };
    wd.stop.store(true, Ordering::SeqCst);
    assert!(!stalled, "the handler stalled: {} operations completed",
        wd.progress.load(Ordering::SeqCst));
    for t in tasks { let _ = t.await; }
    let _ = flusher.await;

    assert_eq!(wd.progress.load(Ordering::SeqCst) as u64, WRITES * 3 + ATTEMPTS * 3);

    let mut fh = fh;
    let _ = fh.fh_flush().await.expect("final flush");
    let _ = fh.fh_release().await.expect("release");
    tf.cleanup(&client).await;
}
