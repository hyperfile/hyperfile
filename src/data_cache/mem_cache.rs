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
        if let Some(block) = (self.data_cache_blocks > 0).then(|| self.data_blocks_cache.get(blk_idx)).unwrap() {
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

    // remove a block
    fn remove(&mut self, blk_idx: &BlockIndex) -> Option<DataBlock> {
        // be sure block is not in cache list
        let _ = self.data_blocks_cache.pop(&blk_idx);
        self.data_blocks_dirty.remove(blk_idx)
    }

    // test if block of index need to be retrieve
    #[inline]
    fn contains(&mut self, blk_idx: &BlockIndex) -> bool {
        if self.data_blocks_dirty.contains_key(blk_idx) {
            return true;
        }
        if let Some(block) = (self.data_cache_blocks > 0).then(|| self.data_blocks_cache.pop(blk_idx)).unwrap() {
            self.data_blocks_dirty.insert(*blk_idx, block);
            return true;
        }
        false
    }

    fn get_mut(&mut self, blk_idx: &BlockIndex) -> Option<&mut DataBlock> {
        if self.data_blocks_dirty.contains_key(blk_idx) {
            return self.data_blocks_dirty.get_mut(blk_idx);
        }
        if let Some(block) = (self.data_cache_blocks > 0).then(|| self.data_blocks_cache.pop(blk_idx)).unwrap() {
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
            if let Some(block) = (self.data_cache_blocks > 0).then(|| self.data_blocks_cache.pop(&blk_idx)).unwrap() {
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
        if let Some(block) = (self.data_cache_blocks > 0).then(|| self.data_blocks_cache.pop(&blk_idx)).unwrap() {
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
            // keep block that should cache into cache list
            if let Some(_) = (self.data_cache_blocks > 0).then(|| self.data_blocks_cache.put(blk_idx, block)).unwrap() {
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
        let removed = cache.remove(&0);
        assert!(removed.is_some());
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
}