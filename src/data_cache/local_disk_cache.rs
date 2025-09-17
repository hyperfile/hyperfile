use std::fmt;
use std::io::{Error, ErrorKind, Result};
use std::path::Path;
use std::os::fd::AsRawFd;
use std::num::NonZeroUsize;
use std::collections::BTreeMap;
use log::{debug, warn};
use lru::LruCache;
use crate::{BlockIndex, BlockIndexIter};
use crate::buffer::DataBlock;
use crate::file::DirtyDataBlocks;
use super::Cache;

pub(crate) struct LocalDiskCache {
    addr: u64, // acturallly *mut libc::c_void
    size: usize,
    file: std::fs::File,
    pub(crate) data_blocks_cache: LruCache<BlockIndex, DataBlock>,
    pub(crate) data_blocks_dirty: BTreeMap<BlockIndex, DataBlock>, // index by block uid
    pub(crate) data_cache_blocks: usize,
    pub(crate) data_block_size: usize,
}

impl fmt::Display for LocalDiskCache {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "  data cache blocks limit: {}, data lru cache size: {}, data dirty size: {}",
            self.data_cache_blocks, self.data_blocks_cache.len(), self.data_blocks_dirty.len())
    }
}

impl Drop for LocalDiskCache {
    fn drop(&mut self) {
        self.sync().expect("local disk cache - failed to sync data");
        self.close().expect("local disk cache - failed to close cache");
    }
}

impl LocalDiskCache {
    pub(crate) fn new(file_path: impl AsRef<Path>, size: usize, data_cache_blocks: usize, data_block_size: usize) -> Result<Self> {
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(file_path)?;
        let fd = file.as_raw_fd();

        // init backend file ondisk
        unsafe {
            let ret = libc::ftruncate(fd, size as libc::off_t);
            if ret != 0 {
                return Err(Error::last_os_error());
            }
        }

        // create memory map
        let addr = unsafe {
            libc::mmap(
                std::ptr::null_mut::<libc::c_void>(),
                size as libc::size_t,
                libc::PROT_READ | libc::PROT_WRITE,
                libc::MAP_SHARED,
                fd,
                0,
            )
        };

        if addr == libc::MAP_FAILED {
            return Err(Error::last_os_error());
        }

        let data_blocks_cache = lru::LruCache::new(
            NonZeroUsize::new(data_cache_blocks).or(NonZeroUsize::new(1)).unwrap()
        );

        let data_blocks_dirty = BTreeMap::new();

        Ok(Self {
            addr: addr as u64,
            size,
            file,
            data_blocks_cache,
            data_blocks_dirty,
            data_cache_blocks,
            data_block_size,
        })
    }

    pub(crate) fn open(file_path: impl AsRef<Path>, size: usize, data_cache_blocks: usize, data_block_size: usize) -> Result<Self> {
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(file_path)?;
        let metadata = file.metadata()?;
        let cache_file_size = metadata.len() as usize;
        let fd = file.as_raw_fd();
        assert!(cache_file_size == size);

        // create memory map
        let addr = unsafe {
            libc::mmap(
                std::ptr::null_mut::<libc::c_void>(),
                size as libc::size_t,
                libc::PROT_READ | libc::PROT_WRITE,
                libc::MAP_SHARED,
                fd,
                0,
            )
        };

        if addr == libc::MAP_FAILED {
            return Err(Error::last_os_error());
        }

        let data_blocks_cache = lru::LruCache::new(
            NonZeroUsize::new(data_cache_blocks).or(NonZeroUsize::new(1)).unwrap()
        );

        let data_blocks_dirty = BTreeMap::new();

        Ok(Self {
            addr: addr as u64,
            size,
            file,
            data_blocks_cache,
            data_blocks_dirty,
            data_cache_blocks,
            data_block_size,
        })
    }

    pub(crate) fn open_or_create(file_path: impl AsRef<Path>, size: usize, data_cache_blocks: usize, data_block_size: usize) -> Result<Self> {
        Self::open(&file_path, size, data_cache_blocks, data_block_size)
            .or_else(|e| {
                if e.kind() == ErrorKind::NotFound {
                    Self::new(file_path, size, data_cache_blocks, data_block_size)
                } else {
                    Err(e)
                }
            })
    }

    fn new_dirty_block(&self, blk_idx: BlockIndex) -> DataBlock {
        let addr = self.addr as *mut u8;
        let ptr = unsafe {
            addr.add(blk_idx as usize * self.data_block_size)
        };
        let block = DataBlock::new_mmap(blk_idx, ptr, self.data_block_size);
        block.set_dirty();
        block.lock();
        block
    }

    fn discard(&self, blk_idx: BlockIndex) {
        let fd = self.file.as_raw_fd();
        let offset = (blk_idx as usize * self.data_block_size) as libc::off_t;
        let len = self.data_block_size as libc::off_t;
        let ret = unsafe {
            libc::fallocate(fd, libc::FALLOC_FL_PUNCH_HOLE | libc::FALLOC_FL_KEEP_SIZE, offset, len)
        };
        if ret == -1 {
            panic!("fallocate failed to punch hole at offset: {}, len: {}, error: {}",
                offset, len, Error::last_os_error());
        }
    }

    pub(crate) fn sync(&self) -> Result<()> {
        let ret = unsafe {
            libc::msync(self.addr as *mut libc::c_void, self.size, libc::MS_SYNC | libc::MS_INVALIDATE)
        };
        if ret != 0 {
            return Err(Error::last_os_error());
        }
        Ok(())
    }

    pub(crate) fn close(&self) -> Result<()> {
        let ret = unsafe {
            libc::munmap(self.addr as *mut libc::c_void, self.size)
        };
        if ret != 0 {
            return Err(Error::last_os_error());
        }
        let fd = self.file.as_raw_fd();
        let ret = unsafe {
            libc::close(fd)
        };
        if ret != 0 {
            return Err(Error::last_os_error());
        }
        Ok(())
    }
}

impl Cache for LocalDiskCache {
    fn set_size(&self, new_size: usize) {
        let fd = self.file.as_raw_fd();
        let addr = self.addr as *mut libc::c_void;
        let ret = unsafe {
            libc::ftruncate(fd, new_size as libc::off_t)
        };
        if ret == -1 {
            panic!("ftruncate failed to change cache file size from {} to {}, error: {}",
                self.size, new_size, Error::last_os_error());
        }
        let ret = unsafe {
            libc::mremap(addr, self.size, new_size, 0)
        };
        if ret == libc::MAP_FAILED {
            panic!("mremap failed to change cache space size from {} to {}, error: {}",
                self.size, new_size, Error::last_os_error());
        }
        assert!(ret == addr);
        unsafe {
            let ptr = std::ptr::addr_of!(self.size) as *mut usize;
            std::ptr::write_volatile(ptr, new_size);
        }
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
        DataBlock::new_alloc(blk_idx, self.data_block_size)
    }

    fn get(&mut self, blk_idx: &BlockIndex) -> Option<&DataBlock> {
        // check dirty cache
        if let Some(block) = self.data_blocks_dirty.get(blk_idx) {
            // cache hit
            debug!("Cache Hit on dirty list for block index: {}", blk_idx);
            assert!(block.is_locked());
            return Some(block);
        }
        // check data cache
        if let Some(block) = (self.data_cache_blocks > 0).then(|| self.data_blocks_cache.get(blk_idx)).unwrap() {
            // cache hit
            debug!("Cache Hit on cache list for block index: {}", blk_idx);
            assert!(!block.is_locked());
            block.lock();
            return Some(block);
        }
        None
    }

    fn insert(&mut self, blk_idx: BlockIndex, block: DataBlock) -> Option<DataBlock> {
        // be sure block is not in cache list
        let _ = self.data_blocks_cache.pop(&blk_idx);
        assert!(!block.is_dirty());
        let mut dirty_block = self.new_dirty_block(blk_idx);
        dirty_block.copy(0, block.as_slice());
        self.data_blocks_dirty.insert(blk_idx, dirty_block)
    }

    fn remove(&mut self, blk_idx: &BlockIndex) -> Option<DataBlock> {
        // be sure block is not in cache list
        let _ = self.data_blocks_cache.pop(&blk_idx);
        self.data_blocks_dirty.remove(blk_idx)
            .and_then(|block| {
                block.clear_dirty();
                block.unlock();
                Some(block)
            })
    }

    fn contains(&mut self, blk_idx: &BlockIndex) -> bool {
        if self.data_blocks_dirty.contains_key(blk_idx) {
            return true;
        }
        if let Some(block) = (self.data_cache_blocks > 0).then(|| self.data_blocks_cache.pop(blk_idx)).unwrap() {
            block.lock();
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
            block.lock();
            block.set_dirty();
            self.data_blocks_dirty.insert(*blk_idx, block);
            return self.data_blocks_dirty.get_mut(blk_idx);
        }
        None
    }

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
                block.lock();
                block.set_dirty();
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
            block.lock();
            // not found in dirty list but on cache list,
            // let's update block content and move it to dirty list
            // NOTE: this not intend to happen in currently design, kick warning
            block.copy(off, buf);
            block.set_dirty();
            self.data_blocks_dirty.insert(*blk_idx, block);
            warn!("update_cache - block index: {blk_idx} not in dirty list but in cache list, this is not by design");
        } else {
            // can't found in dirty list, create a new one
            let mut block = self.new_dirty_block(*blk_idx);
            block.copy(off, buf);
            self.data_blocks_dirty.insert(*blk_idx, block);
        }
    }

    fn truncate_data_block(&mut self, blk_idx: &BlockIndex, offset_to_discard: usize) -> bool {
        if let Some(block) = self.data_blocks_dirty.get_mut(&blk_idx) {
            let buf = block.as_mut_slice();
            let (_, to_clear) = buf.split_at_mut(offset_to_discard);
            to_clear.fill(0);
            debug!("data block in dirty list, data cleared");
            return true;
        }
        if let Some(block) = (self.data_cache_blocks > 0).then(|| self.data_blocks_cache.pop(&blk_idx)).unwrap() {
            block.lock();
            let buf = block.as_mut_slice();
            let (_, to_clear) = buf.split_at_mut(offset_to_discard);
            to_clear.fill(0);
            debug!("data block in cache list, data cleared");
            block.set_dirty();
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
            block.clear_dirty();
            block.unlock();
            // push into cache list
            if let Some((old_blk_idx, _)) = (self.data_cache_blocks > 0).then(|| self.data_blocks_cache.push(blk_idx, block)).unwrap() {
                if old_blk_idx == blk_idx {
                    panic!("block already exists, failed to put back block index {} into data blocks cache", blk_idx);
                } else {
                    self.discard(old_blk_idx);
                }
            }
        }
    }

    fn clear_data_blocks_cache(&mut self) {
        if self.data_cache_blocks > 0 {
            self.data_blocks_cache.clear();
        }
    }

    fn shutdown(&self) {
        self.sync().expect("local disk cache - failed to sync data");
        self.close().expect("local disk cache - failed to close cache");
    }
}
