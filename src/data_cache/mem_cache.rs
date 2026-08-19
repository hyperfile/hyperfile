use std::fmt;
use std::num::NonZeroUsize;
use std::collections::BTreeMap;
use log::{debug, warn};
use lru::LruCache;
use crate::{BlockIndex, BlockIndexIter};
use crate::buffer::DataBlock;
use crate::file::DirtyDataBlocks;
use super::Cache;

pub(crate) struct MemCache {
    // NOTE:
    //   1) dirty list is higher priority than cache list
    //   2) data cache only intend to cache incomplete block access
    pub(crate) data_blocks_cache: LruCache<BlockIndex, DataBlock>,
    pub(crate) data_blocks_dirty: BTreeMap<BlockIndex, DataBlock>, // index by block uid
    pub(crate) data_cache_blocks: usize,
    pub(crate) data_block_size: usize,

}

impl fmt::Display for MemCache {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "  data cache blocks limit: {}, data lru cache size: {}, data dirty size: {}",
            self.data_cache_blocks, self.data_blocks_cache.len(), self.data_blocks_dirty.len())
    }
}

impl MemCache {
    pub(crate) fn new(data_cache_blocks: usize, data_block_size: usize) -> Self {
        Self {
            data_blocks_cache: LruCache::new(
                // fail back to 1 if data_cache_blocks is set to zero
                NonZeroUsize::new(data_cache_blocks).or(NonZeroUsize::new(1)).unwrap()
            ),
            data_blocks_dirty: BTreeMap::new(),
            data_cache_blocks,
            data_block_size,
        }
    }
}

impl Cache for MemCache {
    fn set_size(&self, _: usize) {
        /* do nothing */
    }


    fn set_unlimited(&mut self) {
        self.data_blocks_cache.resize(NonZeroUsize::new(usize::MAX).unwrap());
    }

    fn restore_limit(&mut self) {
        self.data_blocks_cache.resize(
            NonZeroUsize::new(self.data_cache_blocks).or(NonZeroUsize::new(1)).unwrap()
        );
    }

    fn new_block(&self, blk_idx: BlockIndex) -> DataBlock {
        DataBlock::new(blk_idx, self.data_block_size)
    }

    fn get(&mut self, blk_idx: &BlockIndex) -> Option<&DataBlock> {
        // check dirty cache
        if let Some(block) = self.data_blocks_dirty.get(blk_idx) {
            // cache hit
            debug!("Cache Hit on dirty list for block index: {}", blk_idx);
            return Some(block);
        }
        // check data cache
        if let Some(block) = (self.data_cache_blocks > 0).then(|| self.data_blocks_cache.get(blk_idx)).flatten() {
            // cache hit
            debug!("Cache Hit on cache list for block index: {}", blk_idx);
            return Some(block);
        }
        None
    }

    // force insert a block
    fn insert(&mut self, blk_idx: BlockIndex, block: DataBlock) -> Option<DataBlock> {
        // be sure block is not in cache list
        let _ = self.data_blocks_cache.pop(&blk_idx);
        self.data_blocks_dirty.insert(blk_idx, block)
    }

    /// Side-effect-free probe; see the trait docs. `LruCache::contains`
    /// does not disturb recency, which is what "side-effect-free"
    /// requires here.
    fn has(&self, blk_idx: &BlockIndex) -> bool {
        if self.data_blocks_dirty.contains_key(blk_idx) {
            return true;
        }
        self.data_cache_blocks > 0 && self.data_blocks_cache.contains(blk_idx)
    }

    /// Install a clean, freshly-loaded block in the LRU. See the
    /// trait docs.
    fn insert_clean(&mut self, blk_idx: BlockIndex, block: DataBlock) -> Option<DataBlock> {
        if self.data_cache_blocks == 0 {
            // Nothing to keep it in. Hand it back so the caller can
            // still use the bytes it just paid to load.
            return Some(block);
        }
        debug_assert!(!block.is_dirty(), "insert_clean given a dirty block");
        // A block being installed clean must not already be dirty
        // under the same index; the caller probed the cache first.
        debug_assert!(!self.data_blocks_dirty.contains_key(&blk_idx));
        block.set_should_cache();
        let _ = self.data_blocks_cache.put(blk_idx, block);
        None
    }

    // remove a block
    fn remove(&mut self, blk_idx: &BlockIndex) -> bool {
        // be sure block is not in cache list
        let in_clean = self.data_blocks_cache.pop(&blk_idx).is_some();
        let in_dirty = self.data_blocks_dirty.remove(blk_idx).is_some();
        in_clean || in_dirty
    }

    // test if block of index need to be retrieve
    #[inline]
    fn contains(&mut self, blk_idx: &BlockIndex) -> bool {
        if self.data_blocks_dirty.contains_key(blk_idx) {
            return true;
        }
        if let Some(block) = (self.data_cache_blocks > 0).then(|| self.data_blocks_cache.pop(blk_idx)).flatten() {
            self.data_blocks_dirty.insert(*blk_idx, block);
            return true;
        }
        false
    }

    fn get_mut(&mut self, blk_idx: &BlockIndex) -> Option<&mut DataBlock> {
        if self.data_blocks_dirty.contains_key(blk_idx) {
            return self.data_blocks_dirty.get_mut(blk_idx);
        }
        if let Some(block) = (self.data_cache_blocks > 0).then(|| self.data_blocks_cache.pop(blk_idx)).flatten() {
            self.data_blocks_dirty.insert(*blk_idx, block);
            return self.data_blocks_dirty.get_mut(blk_idx);
        }
        None
    }

    // we only care about incomplete blocks and not in dirty list
    // return:
    //   - vec of data block ptr we need to retrieve
    fn write_prepare(&mut self, off: usize, len: usize) -> Vec<BlockIndex> {
        let mut output = Vec::new();
        let blk_iter = BlockIndexIter::new(off, len, self.data_block_size);
        debug!("start to write prepare for write offset {}, len {}", off, len);
        for (blk_idx, start_off, data_len) in blk_iter {
            // for a complete block, we don't need to retrieve
            if start_off == 0 && data_len == self.data_block_size {
                // discard data blocks cached if we have
                let _ = self.data_blocks_cache.pop(&blk_idx);
                continue;
            }
            // for incomplete block
            if self.data_blocks_dirty.contains_key(&blk_idx) {
                // incomplete block but already in dirty list
                continue;
            }
            if let Some(block) = (self.data_cache_blocks > 0).then(|| self.data_blocks_cache.pop(&blk_idx)).flatten() {
                // incomplete block found in data blocks cache
                self.data_blocks_dirty.insert(blk_idx, block);
                continue;
            }
            // incomplete block and not in both dirty and cache list
            output.push(blk_idx);
        }
        debug!("end of write prepare {} of blocks need to be retrieve", output.len());
        output
    }

    fn update_cache(&mut self, blk_idx: &BlockIndex, off: usize, buf: &[u8]) {
        if let Some(block) = self.data_blocks_dirty.get_mut(blk_idx) {
            // found in dirty list, just update it's content
            block.copy(off, buf);
        } else if let Some(mut block) = self.data_blocks_cache.pop(blk_idx) {
            // not found in dirty list but on cache list,
            // let's update block content and move it to dirty list
            // NOTE: this not intend to happen in currently design, kick warning
            block.copy(off, buf);
            self.data_blocks_dirty.insert(*blk_idx, block);
            warn!("update_cache - block index: {blk_idx} not in dirty list but in cache list, this is not by design");
        } else {
            // can't found in dirty list, create a new one
            let mut block = DataBlock::new(*blk_idx, self.data_block_size);
            block.copy(off, buf);
            self.data_blocks_dirty.insert(*blk_idx, block);
        }
    }

    // return:
    //   true - block truncated and is on dirty list
    //   false - block not found in the cache
    fn truncate_data_block(&mut self, blk_idx: &BlockIndex, offset_to_discard: usize) -> bool {
        if let Some(block) = self.data_blocks_dirty.get_mut(&blk_idx) {
            let buf = block.as_mut_slice();
            let (_, to_clear) = buf.split_at_mut(offset_to_discard);
            to_clear.fill(0);
            debug!("data block in dirty list, data cleared");
            return true;
        }
        if let Some(block) = (self.data_cache_blocks > 0).then(|| self.data_blocks_cache.pop(&blk_idx)).flatten() {
            let buf = block.as_mut_slice();
            let (_, to_clear) = buf.split_at_mut(offset_to_discard);
            to_clear.fill(0);
            debug!("data block in cache list, data cleared");
            // move data block into dirty list
            self.data_blocks_dirty.insert(*blk_idx, block);
            return true;
        }
        false
    }

    fn dirty_count(&self) -> usize {
        self.data_blocks_dirty.len()
    }

    fn truncate_blocks_above(&mut self, boundary: BlockIndex) -> usize {
        let mut removed = 0;
        // BTreeMap doesn't expose a `split_off_keys_above` so we
        // collect the keys first and then drop. The dirty map is
        // bounded by the dirty-blocks threshold (defaults from
        // lib.rs are in the tens to thousands range), so the
        // intermediate Vec is fine.
        let dirty: Vec<BlockIndex> = self.data_blocks_dirty
            .range(boundary..)
            .map(|(k, _)| *k)
            .collect();
        for k in dirty {
            if self.data_blocks_dirty.remove(&k).is_some() {
                removed += 1;
            }
        }
        // The clean tier must be swept independently: a block that
        // was already flushed lives only here, and enumerating
        // candidates from the dirty map alone would leave it
        // behind for the read path to serve after the file grows
        // back past the old EOF. LruCache has no range API, so
        // collect the keys to evict first.
        if self.data_cache_blocks > 0 {
            let clean: Vec<BlockIndex> = self.data_blocks_cache
                .iter()
                .map(|(k, _)| *k)
                .filter(|k| *k >= boundary)
                .collect();
            for k in clean {
                let _ = self.data_blocks_cache.pop(&k);
            }
        }
        removed
    }

    fn get_dirty(&self) -> DirtyDataBlocks<'_> {
        let b: BTreeMap<BlockIndex, &DataBlock> = self.data_blocks_dirty.iter()
                        .map(|(idx, blk)| (*idx, blk))
                        .collect();
        DirtyDataBlocks { inner: Some(b), owned: None }
    }

    fn clear_dirty(&mut self) {
        while let Some((blk_idx, block)) = self.data_blocks_dirty.pop_first() {
            if !block.is_should_cache() {
                continue;
            }
            if self.data_cache_blocks == 0 {
                // Cache disabled: nothing to keep the block in, so drop
                // it. The data is already persisted (clear_dirty runs
                // after a flush) and the buffer is a plain heap
                // allocation, so dropping it here is the whole cleanup.
                continue;
            }
            // keep block that should cache into cache list
            if let Some(_) = self.data_blocks_cache.put(blk_idx, block) {
                panic!("block already exists, failed to put back block index {} into data blocks cache", blk_idx);
            }
        }
    }

    fn clear_data_blocks_cache(&mut self) {
        if self.data_cache_blocks > 0 {
            self.data_blocks_cache.clear();
        }
    }

    fn shutdown(&self) {
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn new_cache() -> MemCache {
        MemCache::new(4, 4096) // 4 cache slots, 4KiB blocks
    }

    /// `data_cache_blocks == 0` means "clean-block caching disabled".
    /// It is a representable, reachable configuration, and every path
    /// that consults the clean tier used to guard it with
    /// `(enabled).then(|| ...).unwrap()` — which panics precisely when
    /// the cache is *disabled*, because `bool::then` yields `None` for
    /// a false condition. Exercise every such path with the cache off.
    #[test]
    fn disabled_cache_does_not_panic() {
        let mut c = MemCache::new(0, 4096);
        assert_eq!(c.dirty_count(), 0);

        // get / contains / get_mut / truncate_data_block: all consult
        // the clean tier after missing the dirty map.
        assert!(c.get(&0).is_none());
        assert!(!c.contains(&0));
        assert!(c.get_mut(&0).is_none());
        assert!(!c.truncate_data_block(&0, 100));

        // remove and insert pop the clean tier unconditionally.
        assert!(!c.remove(&0), "nothing cached, so nothing to remove");
        assert!(c.insert(0, DataBlock::new(0, 4096)).is_none());
        assert_eq!(c.dirty_count(), 1);

        // write_prepare consults the clean tier for partial blocks.
        let v = c.write_prepare(4096 + 10, 20);
        assert_eq!(v, vec![1], "block 1 is partial and not cached");

        // clear_dirty has nowhere to put the block; it must drop it
        // rather than panic, and must not leave it dirty.
        c.clear_dirty();
        assert_eq!(c.dirty_count(), 0);
        assert!(c.get(&0).is_none(), "a disabled cache must not retain the block");

        // truncate_blocks_above sweeps both tiers.
        assert_eq!(c.truncate_blocks_above(0), 0);
        c.clear_data_blocks_cache();
    }

    /// A block marked should_cache still must not be retained when the
    /// cache is disabled.
    #[test]
    fn disabled_cache_clear_dirty_drops_should_cache_block() {
        let mut c = MemCache::new(0, 4096);
        let mut blk = DataBlock::new(7, 4096);
        blk.set_should_cache();
        blk.copy(0, &[0xAB; 4096]);
        c.insert(7, blk);
        assert_eq!(c.dirty_count(), 1);

        c.clear_dirty();
        assert_eq!(c.dirty_count(), 0);
        assert!(c.get(&7).is_none(),
            "with the cache disabled the block must be dropped, not cached");
    }

    // --- insert / get ---

    #[test]
    fn insert_and_get() {
        let mut cache = new_cache();
        let mut blk = DataBlock::new(0, 4096);
        blk.copy(0, &[0xAA]);
        cache.insert(0, blk);
        let got = cache.get(&0).unwrap();
        assert_eq!(got.as_slice()[0], 0xAA);
    }

    #[test]
    fn get_missing_returns_none() {
        let mut cache = new_cache();
        assert!(cache.get(&99).is_none());
    }

    // --- dirty tracking ---

    #[test]
    fn insert_goes_to_dirty() {
        let mut cache = new_cache();
        cache.insert(0, DataBlock::new(0, 4096));
        assert_eq!(cache.dirty_count(), 1);
    }

    #[test]
    fn dirty_count_and_get_dirty() {
        let mut cache = new_cache();
        cache.insert(0, DataBlock::new(0, 4096));
        cache.insert(5, DataBlock::new(5, 4096));
        assert_eq!(cache.dirty_count(), 2);
        let dirty = cache.get_dirty();
        assert_eq!(dirty.len(), 2);
    }

    #[test]
    fn clear_dirty_moves_should_cache_to_lru() {
        let mut cache = new_cache();
        let blk = DataBlock::new(0, 4096);
        blk.set_should_cache();
        cache.insert(0, blk);
        assert_eq!(cache.dirty_count(), 1);

        cache.clear_dirty();
        assert_eq!(cache.dirty_count(), 0);
        // block should now be in LRU cache, accessible via get
        assert!(cache.get(&0).is_some());
    }

    #[test]
    fn clear_dirty_discards_non_cacheable() {
        let mut cache = new_cache();
        cache.insert(0, DataBlock::new(0, 4096)); // no set_should_cache
        cache.clear_dirty();
        assert_eq!(cache.dirty_count(), 0);
        assert!(cache.get(&0).is_none());
    }

    // --- write_prepare ---

    #[test]
    fn write_prepare_full_block_no_retrieve() {
        let mut cache = new_cache();
        // writing a full block at offset 0 should not need retrieval
        let need = cache.write_prepare(0, 4096);
        assert!(need.is_empty());
    }

    #[test]
    fn write_prepare_partial_block_needs_retrieve() {
        let mut cache = new_cache();
        // writing 100 bytes at offset 10 — partial block 0
        let need = cache.write_prepare(10, 100);
        assert_eq!(need, vec![0]);
    }

    #[test]
    fn write_prepare_partial_block_in_dirty_no_retrieve() {
        let mut cache = new_cache();
        cache.insert(0, DataBlock::new(0, 4096));
        // block 0 already dirty, partial write should not need retrieve
        let need = cache.write_prepare(10, 100);
        assert!(need.is_empty());
    }

    #[test]
    fn write_prepare_cross_block() {
        let mut cache = new_cache();
        // write 100 bytes crossing block 0→1 boundary at offset 4090
        let need = cache.write_prepare(4090, 100);
        // both block 0 and block 1 are partial and not cached
        assert_eq!(need, vec![0, 1]);
    }

    // --- update_cache ---

    #[test]
    fn update_cache_creates_block_if_missing() {
        let mut cache = new_cache();
        cache.update_cache(&0, 0, &[1, 2, 3]);
        assert_eq!(cache.dirty_count(), 1);
        let blk = cache.get(&0).unwrap();
        assert_eq!(&blk.as_slice()[0..3], &[1, 2, 3]);
    }

    #[test]
    fn update_cache_updates_existing_dirty() {
        let mut cache = new_cache();
        cache.insert(0, DataBlock::new(0, 4096));
        cache.update_cache(&0, 10, &[0xFF; 4]);
        let blk = cache.get(&0).unwrap();
        assert_eq!(&blk.as_slice()[10..14], &[0xFF; 4]);
    }

    // --- truncate ---

    #[test]
    fn truncate_dirty_block() {
        let mut cache = new_cache();
        let mut blk = DataBlock::new(0, 4096);
        blk.copy(0, &[0xFF; 4096]);
        cache.insert(0, blk);

        let found = cache.truncate_data_block(&0, 100);
        assert!(found);
        let blk = cache.get(&0).unwrap();
        // first 100 bytes preserved
        assert_eq!(blk.as_slice()[99], 0xFF);
        // rest zeroed
        assert!(blk.as_slice()[100..].iter().all(|&b| b == 0));
    }

    #[test]
    fn truncate_missing_block() {
        let mut cache = new_cache();
        assert!(!cache.truncate_data_block(&99, 0));
    }

    // --- truncate_blocks_above ---

    #[test]
    fn truncate_blocks_above_drops_dirty() {
        let mut cache = new_cache();
        cache.insert(1, DataBlock::new(1, 4096));
        cache.insert(9, DataBlock::new(9, 4096));

        let removed = cache.truncate_blocks_above(5);
        assert_eq!(removed, 1, "only the dirty block at 9 is above the boundary");
        assert!(cache.get(&9).is_none(), "dirty block above boundary must be dropped");
        assert!(cache.get(&1).is_some(), "block below boundary must be kept");
    }

    /// Regression: a block that was already flushed lives only in
    /// the clean tier. Enumerating truncate candidates from the
    /// dirty map alone left it behind, and the read path (which
    /// consults the clean tier) then served pre-truncate bytes
    /// after the file grew back past the old EOF.
    #[test]
    fn truncate_blocks_above_drops_clean() {
        let mut cache = new_cache();
        let mut blk = DataBlock::new(9, 4096);
        blk.set_should_cache();
        blk.copy(0, &[0xAB; 4096]);
        cache.insert(9, blk);
        // Simulate a flush: dirty -> clean tier.
        cache.clear_dirty();
        assert_eq!(cache.dirty_count(), 0);
        assert!(cache.get(&9).is_some(), "block should now be cached clean");

        let removed = cache.truncate_blocks_above(5);
        assert_eq!(removed, 0, "clean evictions must not be counted as dirty removals");
        assert!(cache.get(&9).is_none(),
            "clean cached block above the boundary must be dropped");
    }

    #[test]
    fn truncate_blocks_above_keeps_clean_below_boundary() {
        let mut cache = new_cache();
        let mut blk = DataBlock::new(2, 4096);
        blk.set_should_cache();
        blk.copy(0, &[0xCD; 4096]);
        cache.insert(2, blk);
        cache.clear_dirty();

        let _ = cache.truncate_blocks_above(5);
        let got = cache.get(&2).expect("clean block below boundary must survive");
        assert_eq!(got.as_slice()[0], 0xCD);
    }

    #[test]
    fn truncate_blocks_above_at_boundary_is_inclusive() {
        let mut cache = new_cache();
        let blk = DataBlock::new(5, 4096);
        blk.set_should_cache();
        cache.insert(5, blk);
        cache.clear_dirty();

        let _ = cache.truncate_blocks_above(5);
        assert!(cache.get(&5).is_none(), "boundary key itself must be dropped");
    }

    // --- contains / get_mut ---

    #[test]
    fn contains_promotes_from_cache_to_dirty() {
        let mut cache = new_cache();
        // put block in LRU cache via clear_dirty path
        let blk = DataBlock::new(0, 4096);
        blk.set_should_cache();
        cache.insert(0, blk);
        cache.clear_dirty();
        assert_eq!(cache.dirty_count(), 0);

        // contains should promote it back to dirty
        assert!(cache.contains(&0));
        assert_eq!(cache.dirty_count(), 1);
    }

    #[test]
    fn get_mut_promotes_from_cache_to_dirty() {
        let mut cache = new_cache();
        let blk = DataBlock::new(0, 4096);
        blk.set_should_cache();
        cache.insert(0, blk);
        cache.clear_dirty();

        let blk = cache.get_mut(&0).unwrap();
        blk.copy(0, &[0xBB]);
        assert_eq!(cache.dirty_count(), 1);
    }

    // --- remove ---

    #[test]
    fn remove_from_dirty() {
        let mut cache = new_cache();
        cache.insert(0, DataBlock::new(0, 4096));
        assert!(cache.remove(&0), "remove should report the block was there");
        assert_eq!(cache.dirty_count(), 0);
    }

    // --- eviction ---

    #[test]
    fn lru_eviction() {
        let mut cache = MemCache::new(2, 4096); // only 2 LRU slots
        // fill LRU via clear_dirty
        for i in 0..3 {
            let blk = DataBlock::new(i, 4096);
            blk.set_should_cache();
            cache.insert(i, blk);
        }
        cache.clear_dirty();
        // LRU has capacity 2, so block 0 should have been evicted
        assert!(cache.get(&0).is_none());
        assert!(cache.get(&2).is_some());
    }

    // --- new_block ---

    #[test]
    fn new_block_correct_size() {
        let cache = new_cache();
        let blk = cache.new_block(7);
        assert_eq!(blk.index(), 7);
        assert_eq!(blk.size(), 4096);
    }

    // --- set_unlimited / restore_limit ---

    #[test]
    fn set_unlimited_and_restore() {
        let mut cache = MemCache::new(2, 4096);
        cache.set_unlimited();
        // should be able to insert many without eviction
        for i in 0..100 {
            let blk = DataBlock::new(i, 4096);
            blk.set_should_cache();
            cache.insert(i, blk);
        }
        cache.clear_dirty();
        assert!(cache.get(&0).is_some());

        cache.restore_limit();
        // after restore, capacity is back to 2 — next operations may evict
    }

    // --- has / insert_clean / ensure_capacity ---
    //
    // These back the block borrow API. `has` exists because neither
    // `get` nor `contains` can be used to probe: on the local-disk
    // tier `get` mlocks the block it returns and asserts it was not
    // already locked, and `contains` promotes a clean block into the
    // dirty tier.

    #[test]
    fn has_is_side_effect_free() {
        let mut cache = new_cache();
        assert!(!cache.has(&7), "empty cache must not claim to have a block");

        // A dirty block is visible to `has`.
        let blk = DataBlock::new(7, 4096);
        blk.set_should_cache();
        cache.insert(7, blk);
        assert!(cache.has(&7));
        assert_eq!(cache.dirty_count(), 1);

        // Move it to the clean tier.
        cache.clear_dirty();
        assert_eq!(cache.dirty_count(), 0);

        // Still visible, and *repeated* probes must not promote it
        // back into the dirty tier the way `contains` would.
        for _ in 0..3 {
            assert!(cache.has(&7), "clean block must be visible to has()");
        }
        assert_eq!(cache.dirty_count(), 0,
            "has() must not promote a clean block into the dirty tier");

        // Contrast: `contains` does promote.
        assert!(cache.contains(&7));
        assert_eq!(cache.dirty_count(), 1, "contains() is expected to promote");
    }

    #[test]
    fn insert_clean_populates_the_clean_tier() {
        let mut cache = new_cache();
        let mut block = cache.new_block(3);
        block.copy(0, &[0xC1u8; 4096]);

        assert!(cache.insert_clean(3, block).is_none(),
            "an enabled cache must take the block");
        assert!(cache.has(&3));
        assert_eq!(cache.dirty_count(), 0,
            "insert_clean must not make the block dirty");

        // Readable, and still clean afterwards.
        assert_eq!(cache.get(&3).expect("clean hit").as_slice()[0], 0xC1);
        assert_eq!(cache.dirty_count(), 0);

        // A later write borrow promotes it.
        assert!(cache.get_mut(&3).is_some());
        assert_eq!(cache.dirty_count(), 1);
    }

    #[test]
    fn insert_clean_hands_the_block_back_when_disabled() {
        // data_cache_blocks == 0 is what O_DIRECT without wal forces.
        // There is nowhere to keep a clean block, so the caller must
        // get it back rather than silently lose the bytes it loaded.
        let mut cache = MemCache::new(0, 4096);
        let mut block = cache.new_block(1);
        block.copy(0, &[0xD2u8; 4096]);

        let returned = cache.insert_clean(1, block);
        let returned = returned.expect("a disabled cache must return the block");
        assert_eq!(returned.index(), 1);
        assert_eq!(returned.as_slice()[0], 0xD2, "returned block must keep its contents");
        assert!(!cache.has(&1), "nothing should have been cached");
    }

    #[test]
    fn insert_clean_marks_should_cache() {
        // Without should_cache, clear_dirty drops the block instead of
        // keeping it, so a block that arrived via insert_clean and was
        // later dirtied would not survive a flush.
        let mut cache = new_cache();
        let block = cache.new_block(5);
        assert!(cache.insert_clean(5, block).is_none());
        assert!(cache.get(&5).expect("clean hit").is_should_cache());

        let _ = cache.get_mut(&5).expect("promote");
        cache.clear_dirty();
        assert!(cache.has(&5), "block should survive clear_dirty");
    }
}