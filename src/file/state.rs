use std::time::Instant;
use std::sync::atomic::{Ordering, AtomicBool};
#[cfg(feature = "reactor")]
use std::sync::atomic::AtomicU64;

pub(crate) struct State {
    last_flush: Instant,
    flushing: AtomicBool,
    // A flush is waiting for in-flight ranges to drain. Distinct from
    // `flushing`, which means one is already running: this covers the
    // window before it starts, and exists to stop new operations taking
    // a range lock during it. Without that, the drain never completes
    // under a steady stream of readers and the flush is starved.
    #[cfg(all(feature = "reactor", feature = "range-lock"))]
    flush_pending: AtomicBool,
    /// Bumped by anything that changes what a block should contain.
    ///
    /// A read-ahead fetches against the bmap as it was when it planned,
    /// then installs the blocks later. Between those two points the file
    /// can change, and the block it fetched can leave the cache — a
    /// truncate drops every cached entry above the new size — so
    /// "install only if the block is not already resident" is not enough
    /// to keep a stale copy out. Whoever installs a fetched block compares
    /// this against the value it recorded when planning, and discards the
    /// block if it moved.
    ///
    /// Coarse on purpose: any change anywhere invalidates every read-ahead
    /// in flight. Read-ahead is speculative, so discarding it costs a
    /// missed optimization rather than correctness, and a counter cannot
    /// be wrong about the blocks it does not know about.
    ///
    /// Shared so a spawned fetch can read it after the handler has moved
    /// on to other work.
    #[cfg(feature = "reactor")]
    mutation_gen: std::sync::Arc<AtomicU64>,
    /// Set when publishing has failed in a way this file cannot resolve,
    /// after which it accepts no further modification.
    ///
    /// Reached only by the WAL flush path, whose upload runs detached: by
    /// the time it fails the caller has already been told the data is
    /// durable, which it is — the WAL holds it — so there is nobody left to
    /// return an error to. Replaying the WAL is the remedy and it is tried
    /// a bounded number of times first. When even that will not go through,
    /// the honest end state is to stop accepting writes and keep serving
    /// reads, leaving the WAL intact for offline repair.
    ///
    /// Previously this case panicked, which for a server built on this
    /// crate means the whole process, and takes down reads that were still
    /// being served correctly.
    #[cfg(feature = "wal")]
    publish_failed: AtomicBool,
}

impl Default for State {
    fn default() -> Self {
        Self::new()
    }
}

impl State {
    pub(crate) fn new() -> Self {
        Self {
            last_flush: Instant::now(),
            flushing: AtomicBool::new(false),
            #[cfg(all(feature = "reactor", feature = "range-lock"))]
            flush_pending: AtomicBool::new(false),
            #[cfg(feature = "reactor")]
            mutation_gen: std::sync::Arc::new(AtomicU64::new(0)),
            #[cfg(feature = "wal")]
            publish_failed: AtomicBool::new(false),
        }
    }

    pub(crate) fn get_last_flush(&self) -> &Instant {
        &self.last_flush
    }

    pub(crate) fn set_last_flush(&mut self) {
        self.last_flush = Instant::now();
    }

    /// Whether a flush holds the flush lock right now.
    ///
    /// Not feature-gated, because the flag is set and cleared unconditionally and
    /// the question it answers arises without a reactor: recovery replays through
    /// the ordinary write path while holding that lock, so a threshold crossing
    /// during a replay would ask for it again.
    pub(crate) fn is_flushing(&self) -> bool {
        self.flushing.load(Ordering::SeqCst)
    }

    pub(crate) fn set_flushing(&self) {
        self.flushing.store(true, Ordering::SeqCst);
    }

    pub(crate) fn clear_flushing(&self) {
        self.flushing.store(false, Ordering::SeqCst);
    }

    /// Whether publishing has failed unrecoverably. See the field.
    #[cfg(feature = "wal")]
    pub(crate) fn is_publish_failed(&self) -> bool {
        self.publish_failed.load(Ordering::SeqCst)
    }

    /// One-way: there is no path back without offline repair, and pretending
    /// otherwise would let a write land on a file whose newest data is only
    /// in the log.
    #[cfg(feature = "wal")]
    pub(crate) fn set_publish_failed(&self) {
        self.publish_failed.store(true, Ordering::SeqCst);
    }

    // Set while a flush waits for in-flight ranges to drain, so that new
    // operations defer instead of taking a fresh range lock and pushing
    // the drain out of reach. Must be cleared on every path that stops
    // waiting, or those operations defer for good.
    #[cfg(all(feature = "reactor", feature = "range-lock"))]
    pub(crate) fn is_flush_pending(&self) -> bool {
        self.flush_pending.load(Ordering::SeqCst)
    }

    #[cfg(all(feature = "reactor", feature = "range-lock"))]
    pub(crate) fn set_flush_pending(&self) {
        self.flush_pending.store(true, Ordering::SeqCst);
    }

    #[cfg(all(feature = "reactor", feature = "range-lock"))]
    pub(crate) fn clear_flush_pending(&self) {
        self.flush_pending.store(false, Ordering::SeqCst);
    }

    /// See the field docs on `mutation_gen`.
    #[cfg(feature = "reactor")]
    pub(crate) fn mutation_gen(&self) -> u64 {
        self.mutation_gen.load(Ordering::SeqCst)
    }

    /// A handle a spawned fetch can read after the file has moved on.
    #[cfg(feature = "reactor")]
    pub(crate) fn clone_mutation_gen_handle(&self) -> std::sync::Arc<AtomicU64> {
        self.mutation_gen.clone()
    }

    #[cfg(feature = "reactor")]
    pub(crate) fn bump_mutation_gen(&self) {
        self.mutation_gen.fetch_add(1, Ordering::SeqCst);
    }
}
