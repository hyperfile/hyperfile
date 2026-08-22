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
        }
    }

    pub(crate) fn get_last_flush(&self) -> &Instant {
        &self.last_flush
    }

    pub(crate) fn set_last_flush(&mut self) {
        self.last_flush = Instant::now();
    }

    #[cfg(feature = "reactor")]
    pub(crate) fn is_flushing(&self) -> bool {
        self.flushing.load(Ordering::SeqCst)
    }

    pub(crate) fn set_flushing(&self) {
        self.flushing.store(true, Ordering::SeqCst);
    }

    pub(crate) fn clear_flushing(&self) {
        self.flushing.store(false, Ordering::SeqCst);
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
