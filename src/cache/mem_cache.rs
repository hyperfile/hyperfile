use std::fmt;
use std::num::NonZeroUsize;
use std::collections::BTreeMap;
use log::{debug, warn};
use lru::LruCache;
use crate::{BlockIndex, BlockIndexIter};
use crate::buffer::DataBlock;
use crate::file::DirtyDataBlocks;

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
        writeln!(f, "  data dirty size: {}", self.data_blocks_dirty.len())?;
        writeln!(f, "  data cache blocks: {}, data cache size: {}", self.data_cache_blocks, self.data_blocks_cache.len())
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

    pub(crate) fn set_size(&self, _: usize) {
        /* do nothing */
    }

    pub(crate) fn set_unlimited(&mut self) {
        self.data_blocks_cache.resize(NonZeroUsize::new(usize::MAX).unwrap());
    }

    pub(crate) fn restore_limit(&mut self) {
        self.data_blocks_cache.resize(
            NonZeroUsize::new(self.data_cache_blocks).or(NonZeroUsize::new(1)).unwrap()
        );
    }

    pub(crate) fn new_block(&self, blk_idx: BlockIndex) -> DataBlock {
        DataBlock::new(blk_idx, self.data_block_size)
    }

    pub(crate) fn get(&mut self, blk_idx: &BlockIndex) -> Option<&DataBlock> {
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
    pub(crate) fn insert(&mut self, blk_idx: BlockIndex, block: DataBlock) -> Option<DataBlock> {
        // be sure block is not in cache list
        let _ = self.data_blocks_cache.pop(&blk_idx);
        self.data_blocks_dirty.insert(blk_idx, block)
    }

    // remove a block
    pub(crate) fn remove(&mut self, blk_idx: &BlockIndex) -> Option<DataBlock> {
        // be sure block is not in cache list
        let _ = self.data_blocks_cache.pop(&blk_idx);
        self.data_blocks_dirty.remove(blk_idx)
    }

    // test if block of index need to be retrieve
    #[inline]
    pub(crate) fn contains(&mut self, blk_idx: &BlockIndex) -> bool {
        if self.data_blocks_dirty.contains_key(blk_idx) {
            return true;
        }
        if let Some(block) = (self.data_cache_blocks > 0).then(|| self.data_blocks_cache.pop(blk_idx)).unwrap() {
            self.data_blocks_dirty.insert(*blk_idx, block);
            return true;
        }
        false
    }

    pub(crate) fn get_mut(&mut self, blk_idx: &BlockIndex) -> Option<&mut DataBlock> {
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
    pub(crate) fn write_prepare(&mut self, off: usize, len: usize) -> Vec<BlockIndex> {
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

    pub(crate) fn update_cache(&mut self, blk_idx: &BlockIndex, off: usize, buf: &[u8]) {
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
    pub(crate) fn truncate_data_block(&mut self, blk_idx: &BlockIndex, offset_to_discard: usize) -> bool {
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

    pub(crate) fn dirty_count(&self) -> usize {
        self.data_blocks_dirty.len()
    }

    pub(crate) fn get_dirty(&self) -> DirtyDataBlocks<'_> {
        let b: BTreeMap<BlockIndex, &DataBlock> = self.data_blocks_dirty.iter()
                        .map(|(idx, blk)| (*idx, blk))
                        .collect();
        DirtyDataBlocks { inner: Some(b), owned: None }
    }

    pub(crate) fn clear_dirty(&mut self) {
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

    pub(crate) fn clear_data_blocks_cache(&mut self) {
        if self.data_cache_blocks > 0 {
            self.data_blocks_cache.clear();
        }
    }
}
