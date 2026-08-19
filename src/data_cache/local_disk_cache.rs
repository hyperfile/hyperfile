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
    /// Whether the mapping has already been torn down.
    ///
    /// `Cache::shutdown` and `Drop` both tear the cache down, and
    /// `shutdown` is the normal path on release, so `Drop` almost
    /// always runs second. Without this the second teardown would
    /// `msync`/`munmap` an address that is no longer mapped.
    closed: std::sync::atomic::AtomicBool,
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
        // Best-effort: a failure here cannot be reported, and
        // panicking in `Drop` would abort if we are already
        // unwinding. `shutdown` is the path that surfaces errors.
        if let Err(e) = self.teardown() {
            warn!("local disk cache - teardown during drop failed: {}", e);
        }
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
            closed: std::sync::atomic::AtomicBool::new(false),
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
            closed: std::sync::atomic::AtomicBool::new(false),
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

    /// Sync and unmap, exactly once.
    ///
    /// The file descriptor is deliberately *not* closed here. It is
    /// owned by `self.file`, whose own `Drop` closes it; closing it
    /// by hand left the `File` holding a descriptor that had already
    /// been handed back to the OS, which Rust's I/O safety checks
    /// abort on ("owned file descriptor already closed"). Combined
    /// with `shutdown` and `Drop` both tearing down, releasing a file
    /// that used this cache aborted the process.
    fn teardown(&self) -> Result<()> {
        use std::sync::atomic::Ordering;
        if self.closed.swap(true, Ordering::AcqRel) {
            return Ok(());
        }
        let ret = unsafe {
            libc::msync(self.addr as *mut libc::c_void, self.size, libc::MS_SYNC | libc::MS_INVALIDATE)
        };
        if ret != 0 {
            return Err(Error::last_os_error());
        }
        let ret = unsafe {
            libc::munmap(self.addr as *mut libc::c_void, self.size)
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
        if let Some(block) = (self.data_cache_blocks > 0).then(|| self.data_blocks_cache.get(blk_idx)).flatten() {
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
        if let Some(block) = (self.data_cache_blocks > 0).then(|| self.data_blocks_cache.pop(blk_idx)).flatten() {
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
        if let Some(block) = (self.data_cache_blocks > 0).then(|| self.data_blocks_cache.pop(blk_idx)).flatten() {
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
            if let Some(block) = (self.data_cache_blocks > 0).then(|| self.data_blocks_cache.pop(&blk_idx)).flatten() {
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
        if let Some(block) = (self.data_cache_blocks > 0).then(|| self.data_blocks_cache.pop(&blk_idx)).flatten() {
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

    fn truncate_blocks_above(&mut self, boundary: BlockIndex) -> usize {
        let mut removed = 0;
        let dirty: Vec<BlockIndex> = self.data_blocks_dirty
            .range(boundary..)
            .map(|(k, _)| *k)
            .collect();
        for k in dirty {
            if self.data_blocks_dirty.remove(&k).is_some() {
                removed += 1;
            }
            // Punch the backing hole: `new_dirty_block` hands out a
            // raw mmap view without zeroing it, so a later write to
            // this index would otherwise observe the pre-truncate
            // bytes still sitting in the cache file.
            self.discard(k);
        }
        // Sweep the clean tier independently. A block that was
        // already flushed lives only here; enumerating candidates
        // from the dirty map alone would leave it behind for the
        // read path to serve after the file grows back past the old
        // EOF. LruCache has no range API, so collect first.
        if self.data_cache_blocks > 0 {
            let clean: Vec<BlockIndex> = self.data_blocks_cache
                .iter()
                .map(|(k, _)| *k)
                .filter(|k| *k >= boundary)
                .collect();
            for k in clean {
                let _ = self.data_blocks_cache.pop(&k);
                self.discard(k);
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
            block.clear_dirty();
            block.unlock();
            if self.data_cache_blocks == 0 {
                // Cache disabled: there is nowhere to keep the block,
                // and unlike the in-memory cache its bytes live in the
                // backing cache file rather than on the heap. Punch the
                // hole so that a later `new_dirty_block` for this index
                // — which hands out an unzeroed mmap view — cannot
                // observe the bytes being dropped here. Same reasoning
                // as the eviction discard below.
                self.discard(blk_idx);
                continue;
            }
            // push into cache list
            if let Some((old_blk_idx, _)) = self.data_blocks_cache.push(blk_idx, block) {
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
        self.teardown().expect("local disk cache - failed to tear down cache");
    }
}
