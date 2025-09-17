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
    fn insert(&mut self, blk_idx: BlockIndex, block: DataBlock) -> Option<DataBlock>;
    fn remove(&mut self, blk_idx: &BlockIndex) -> Option<DataBlock>;
    fn contains(&mut self, blk_idx: &BlockIndex) -> bool;
    fn get_mut(&mut self, blk_idx: &BlockIndex) -> Option<&mut DataBlock>;
    fn write_prepare(&mut self, off: usize, len: usize) -> Vec<BlockIndex>;
    fn update_cache(&mut self, blk_idx: &BlockIndex, off: usize, buf: &[u8]);
    fn truncate_data_block(&mut self, blk_idx: &BlockIndex, offset_to_discard: usize) -> bool;
    fn dirty_count(&self) -> usize;
    fn get_dirty(&self) -> DirtyDataBlocks<'_>;
    fn clear_dirty(&mut self);
    fn clear_data_blocks_cache(&mut self);
    fn shutdown(&self);
}

impl fmt::Display for Box<dyn Cache + Send> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", *self)
    }
}

pub(crate) fn cache_from_config(config: &HyperFileDataCacheConfig, size: usize, data_cache_blocks: usize, data_block_size: usize) -> Result<Box<dyn Cache + Send>> {
    match config {
        HyperFileDataCacheConfig::Memory(_) => {
            Ok(Box::new(MemCache::new(data_cache_blocks, data_block_size)))
        },
        HyperFileDataCacheConfig::LocalDisk(local) => {
            let cache = LocalDiskCache::open_or_create(local.full_file_path()?, size, data_cache_blocks, data_block_size)?;
            Ok(Box::new(cache))
        },
    }
}
