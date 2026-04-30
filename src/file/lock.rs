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

#[cfg(test)]
mod tests {
    use super::*;

    const BLOCK: u64 = 4096;

    #[test]
    fn new_lock_is_unlocked() {
        let lock = RangeLock::new(BLOCK);
        assert!(!lock.is_locked());
    }

    #[test]
    fn lock_then_unlock() {
        let mut lock = RangeLock::new(BLOCK);
        assert!(lock.try_lock(0..BLOCK));
        assert!(lock.is_locked());
        lock.try_unlock(0..BLOCK);
        assert!(!lock.is_locked());
    }

    #[test]
    fn overlapping_lock_fails() {
        let lock = RangeLock::new(BLOCK);
        assert!(lock.try_lock(0..BLOCK));
        // Exact same range must fail.
        assert!(!lock.try_lock(0..BLOCK));
    }

    #[test]
    fn partial_overlap_fails() {
        let lock = RangeLock::new(BLOCK);
        // Lock [0, 2*BLOCK).
        assert!(lock.try_lock(0..2 * BLOCK));
        // [BLOCK, 3*BLOCK) overlaps second half of the existing lock.
        assert!(!lock.try_lock(BLOCK..3 * BLOCK));
    }

    #[test]
    fn disjoint_ranges_both_succeed() {
        let lock = RangeLock::new(BLOCK);
        assert!(lock.try_lock(0..BLOCK));
        // [2*BLOCK, 3*BLOCK) is strictly above, not overlapping.
        assert!(lock.try_lock(2 * BLOCK..3 * BLOCK));
    }

    #[test]
    fn abutting_ranges_both_succeed() {
        let lock = RangeLock::new(BLOCK);
        // End of first == start of second; half-open ranges do not overlap.
        assert!(lock.try_lock(0..BLOCK));
        assert!(lock.try_lock(BLOCK..2 * BLOCK));
    }

    #[test]
    fn unlock_allows_relock() {
        let mut lock = RangeLock::new(BLOCK);
        assert!(lock.try_lock(0..BLOCK));
        assert!(!lock.try_lock(0..BLOCK));
        lock.try_unlock(0..BLOCK);
        assert!(lock.try_lock(0..BLOCK));
    }

    #[test]
    fn aligned_range_rounds_out() {
        let lock = RangeLock::new(BLOCK);
        // Unaligned [100, 200) should expand to [0, BLOCK).
        let expanded = lock.aligned_range(&(100..200));
        assert_eq!(expanded, 0..BLOCK);

        // [0, BLOCK-1) should expand to [0, BLOCK).
        let expanded = lock.aligned_range(&(0..(BLOCK - 1)));
        assert_eq!(expanded, 0..BLOCK);

        // Cross-block [BLOCK-1, BLOCK+1) should expand to [0, 2*BLOCK).
        let expanded = lock.aligned_range(&((BLOCK - 1)..(BLOCK + 1)));
        assert_eq!(expanded, 0..2 * BLOCK);

        // Already aligned range stays the same.
        let expanded = lock.aligned_range(&(BLOCK..3 * BLOCK));
        assert_eq!(expanded, BLOCK..3 * BLOCK);
    }

    #[test]
    fn lock_unaligned_conflicts_with_aligned() {
        let lock = RangeLock::new(BLOCK);
        // Locking [100, 200) aligns to [0, BLOCK).
        assert!(lock.try_lock(100..200));
        // Another lock fully inside that aligned span must fail.
        assert!(!lock.try_lock(300..400));
        // But a range in the next aligned block succeeds.
        assert!(lock.try_lock((BLOCK + 100)..(BLOCK + 200)));
    }

    #[test]
    fn unlock_nonexistent_range_is_noop() {
        let mut lock = RangeLock::new(BLOCK);
        // Unlocking a range that was never locked should not panic or
        // interfere with subsequent locks.
        lock.try_unlock(0..BLOCK);
        assert!(!lock.is_locked());
        assert!(lock.try_lock(0..BLOCK));
    }

    #[tokio::test]
    async fn async_unlock_also_releases() {
        let mut lock = RangeLock::new(BLOCK);
        assert!(lock.try_lock(0..BLOCK));
        lock.unlock(0..BLOCK).await;
        assert!(!lock.is_locked());
        assert!(lock.try_lock(0..BLOCK));
    }

    #[test]
    fn is_locked_reflects_state_across_clones() {
        // Clone shares the same inner Mutex/RangeMap.
        let lock = RangeLock::new(BLOCK);
        let clone = lock.clone();
        assert!(!lock.is_locked());
        assert!(!clone.is_locked());

        assert!(lock.try_lock(0..BLOCK));
        // Clone sees the lock.
        assert!(clone.is_locked());
    }
}
