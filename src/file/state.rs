use std::time::Instant;
use std::sync::atomic::{Ordering, AtomicBool};

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
}
