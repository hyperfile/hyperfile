pub mod config;
pub(crate) mod mem_cache;
pub(crate) mod local_disk_cache;

use std::fmt;
use std::io::Result;
use crate::BlockIndex;
use crate::buffer::DataBlock;
use crate::file::DirtyDataBlocks;
use self::config::HyperFileDataCacheConfig;
use self::{mem_cache::MemCache, local_disk_cache::LocalDiskCache};

pub(crate) trait Cache {
    fn set_size(&self, size: usize);
    fn set_unlimited(&mut self);
    fn restore_limit(&mut self);
    fn new_block(&self, blk_idx: BlockIndex) -> DataBlock;
    fn get(&mut self, blk_idx: &BlockIndex) -> Option<&DataBlock>;
    /// Side-effect-free membership test across both tiers.
    ///
    /// Unlike `contains`, which promotes a clean block into the
    /// dirty tier, and unlike `get`, which on the local-disk tier
    /// mlocks the block it returns, this only answers the question.
    /// Needed by the read-borrow path, which must decide whether to
    /// load *before* taking the borrow it intends to hand out,
    /// because `get` cannot be called twice on the same clean block
    /// without tripping the lock assertion.
    fn has(&self, blk_idx: &BlockIndex) -> bool;
    fn insert(&mut self, blk_idx: BlockIndex, block: DataBlock) -> Option<DataBlock>;
    /// Install a freshly-loaded, **clean** block in the read cache.
    ///
    /// `insert` places a block in the dirty tier, so it is the wrong
    /// entry point for a block that was just read from staging and
    /// must not be written back. Until the block borrow API
    /// (`HyperFile::block`) there was no such caller: the byte read
    /// path loads straight into the caller's buffer and never
    /// caches, so the clean tier was populated only by `clear_dirty`
    /// handing over blocks that had just been flushed.
    ///
    /// Each tier stores the block in its own representation — the
    /// local-disk tier copies the bytes into its backing file so
    /// that eviction can reclaim them with a hole punch — so
    /// ownership is taken.
    ///
    /// Returns the block back as `Some` when it could **not** be
    /// cached, which happens when the data cache is disabled
    /// (`data_cache_blocks == 0`, as `O_DIRECT` without `wal`
    /// forces). Callers that need the bytes regardless must use the
    /// returned block; there is nothing in the cache to borrow.
    fn insert_clean(&mut self, blk_idx: BlockIndex, block: DataBlock) -> Option<DataBlock>;
    /// Drop a block from both tiers.
    ///
    /// Returns whether anything was removed. Deliberately does not
    /// hand the block back: the local-disk tier's blocks are views
    /// into its backing file, so an owned block escaping the cache
    /// would leak the slot it occupies. No caller wanted the value.
    fn remove(&mut self, blk_idx: &BlockIndex) -> bool;
    fn contains(&mut self, blk_idx: &BlockIndex) -> bool;
    fn get_mut(&mut self, blk_idx: &BlockIndex) -> Option<&mut DataBlock>;
    fn write_prepare(&mut self, off: usize, len: usize) -> Vec<BlockIndex>;
    fn update_cache(&mut self, blk_idx: &BlockIndex, off: usize, buf: &[u8]);
    fn truncate_data_block(&mut self, blk_idx: &BlockIndex, offset_to_discard: usize) -> bool;
    /// Remove every cached entry whose key is `>= boundary`,
    /// **dirty and clean alike**. Used by `truncate_shrink`, where
    /// the bmap is about to drop the same range of keys.
    ///
    /// Both tiers must be dropped, for two different reasons:
    ///
    /// * dirty entries: a stale dirty entry would surface during
    ///   the next flush as `bmap.assign(blk_idx, ...) ->
    ///   NotFound("assign key not found in direct node")`.
    /// * clean entries: their contents are no longer part of the
    ///   file. If the file is later grown past the old EOF again,
    ///   the read path consults the clean tier first and would
    ///   serve the pre-truncate bytes where POSIX requires a hole
    ///   (zeros).
    ///
    /// Returns the number of *dirty* entries removed. Clean
    /// evictions are deliberately not counted: callers use the
    /// return value for `i_blocks` accounting, and clean blocks
    /// were already accounted for by the bmap walk that computes
    /// `removed_blocks`.
    fn truncate_blocks_above(&mut self, boundary: BlockIndex) -> usize;
    fn dirty_count(&self) -> usize;
    fn get_dirty(&self) -> DirtyDataBlocks<'_>;
    fn clear_dirty(&mut self);

    /// Move just these blocks out of the dirty tier, leaving the rest dirty.
    ///
    /// For a partial segment, which writes out part of the dirty set and keeps
    /// accumulating. [`Self::clear_dirty`] cannot serve: it empties the tier, and
    /// blocks written after the partial was built have to stay dirty.
    ///
    /// A block not in the dirty tier is skipped rather than reported, because a
    /// caller cannot hold the tier's borrow while asking for this and so cannot
    /// know the set is still exactly what it saw.
    fn demote_dirty(&mut self, indexes: &[BlockIndex]);
    fn clear_data_blocks_cache(&mut self);
    fn shutdown(&self);
}

impl fmt::Display for Box<dyn Cache + Send> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", *self)
    }
}

/// Build the configured data cache.
///
/// `max_dirty_blocks` is the flush threshold, used by the local-disk
/// tier to size its slot pool: dirty blocks share the mapping with
/// clean ones, so the pool must cover both. The in-memory tier
/// ignores it.
pub(crate) fn cache_from_config(config: &HyperFileDataCacheConfig, max_dirty_blocks: usize, data_cache_blocks: usize, data_block_size: usize) -> Result<Box<dyn Cache + Send>> {
    match config {
        HyperFileDataCacheConfig::Memory(_) => {
            Ok(Box::new(MemCache::new(data_cache_blocks, data_block_size)))
        },
        HyperFileDataCacheConfig::LocalDisk(local) => {
            let cache = LocalDiskCache::open_or_create(local.full_file_path()?, max_dirty_blocks, data_cache_blocks, data_block_size)?;
            Ok(Box::new(cache))
        },
    }
}
