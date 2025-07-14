use std::sync::Arc;
use std::time::Duration;
use core::ops::Range;
use tokio::sync::Mutex;
use rangemap::map::RangeMap;

const TRY_WAIT_MICROS: u64 = 10;

#[derive(Clone)]
pub struct RangeLock {
    inner: Arc<Mutex<RangeMap<u64, bool>>>,
    aligned_size: u64,
    aligned_shift: u32,
}

impl RangeLock {
    pub fn new(aligned_size: u64) -> Self {
        Self {
            inner: Arc::new(Mutex::new(RangeMap::new())),
            aligned_size: aligned_size,
            aligned_shift: aligned_size.checked_ilog2().expect("failed to get aligned shift from aligned size"),
        }
    }

    // align start and end to aligned size
    fn aligned_range(&self, range: &Range<u64>) -> Range<u64> {
        Range {
            start: range.start >> self.aligned_shift << self.aligned_shift,
            end: (range.end + self.aligned_size - 1) >> self.aligned_shift << self.aligned_shift,
        }
    }

    // for reactor mode, lock always call in handler loop,
    // actively retry if unable to lock
    //
    // return:
    //   true - locked
    //   false - not able to lock
    pub fn try_lock(&self, range: Range<u64>) -> bool {
        let range = self.aligned_range(&range);
        loop {
            let Ok(mut lock) = self.inner.try_lock() else {
                std::thread::sleep(Duration::from_micros(TRY_WAIT_MICROS));
                continue;
            };
            let overlapped = lock.overlaps(&range);
            if overlapped == true {
                return false;
            }
            lock.insert(range, true);
            return true;
        }
    }

    pub fn try_unlock(&mut self, range: Range<u64>) {
        let range = self.aligned_range(&range);
        loop {
            let Ok(mut lock) = self.inner.try_lock() else {
                std::thread::sleep(Duration::from_micros(TRY_WAIT_MICROS));
                continue;
            };
            lock.remove(range);
            break;
        }
    }

    pub async fn unlock(&mut self, range: Range<u64>) {
        let mut lock = self.inner.lock().await;
        let range = self.aligned_range(&range);
        lock.remove(range);
    }

    // test if any write op is processing (any range locked)
    pub fn is_locked(&self) -> bool {
        match self.inner.try_lock() {
            Ok(lock) => {
                // test if range map is empty
                return !lock.is_empty();
            },
            Err(_) => {
                // if mutex is locked, someone holding the lock
                return true;
            },
        }
    }
}
