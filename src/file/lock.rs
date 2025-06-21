use std::sync::Arc;
use std::time::Duration;
use core::ops::Range;
use tokio::sync::Mutex;
use rangemap::map::RangeMap;

const TRY_WAIT_MICROS: u64 = 10;

#[derive(Clone)]
pub struct RangeLock {
    inner: Arc<Mutex<RangeMap<u64, bool>>>,
}

impl RangeLock {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(RangeMap::new())),
        }
    }

    // for reactor mode, lock always call in handler loop,
    // actively retry if unable to lock
    //
    // return:
    //   true - locked
    //   false - not able to lock
    pub fn try_lock(&self, range: Range<u64>) -> bool {
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
        lock.remove(range);
    }
}
