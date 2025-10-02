use std::time::Instant;
use std::sync::atomic::{Ordering, AtomicBool};

pub(crate) struct State {
    last_flush: Instant,
    flushing: AtomicBool,
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
        }
    }

    pub(crate) fn get_last_flush(&self) -> &Instant {
        &self.last_flush
    }

    pub(crate) fn set_last_flush(&mut self) {
        self.last_flush = Instant::now();
    }

    pub(crate) fn is_flushing(&self) -> bool {
        self.flushing.load(Ordering::SeqCst)
    }

    pub(crate) fn set_flushing(&self) {
        self.flushing.store(true, Ordering::SeqCst);
    }

    pub(crate) fn clear_flushing(&self) {
        self.flushing.store(false, Ordering::SeqCst);
    }
}
