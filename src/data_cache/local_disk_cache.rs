use std::fmt;
use std::io::{Error, Result};
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

/// Block cache backed by a memory-mapped file on local disk.
///
/// # Addressing
///
/// The mapping is a fixed pool of block-sized **slots**, and a block
/// occupies whichever slot was free when it entered the cache. It is
/// deliberately *not* a 1:1 map of the file's address space.
///
/// Addressing cached blocks as `addr + blk_idx * data_block_size`,
/// as this tier used to, ties the mapping's size to the file's size
/// and breaks in four ways: a new file has size zero and `mmap`
/// rejects a zero length; a write past the current EOF mints a view
/// outside the mapping, because the cache is told the new size only
/// after the blocks are created; growing the mapping in place is not
/// something `mremap` can promise, and letting it move would dangle
/// every view already handed out; and a sparse file with a large
/// address space cannot be mapped at all, since the mapping would
/// have to span the whole space rather than the resident blocks.
///
/// With a slot pool the mapping's size depends only on how many
/// blocks the cache may hold, so none of those apply, and a block
/// index is never used as an offset.
///
/// When the pool is exhausted — the dirty set can transiently exceed
/// its threshold, since one large write dirties every block it
/// touches before the flush check runs — blocks fall back to heap
/// allocations. That costs memory rather than correctness; such
/// blocks simply have no slot to release.
pub(crate) struct LocalDiskCache {
    addr: u64, // acturallly *mut libc::c_void
    /// Length of the mapping, i.e. `nslots * data_block_size`. Fixed
    /// at construction.
    size: usize,
    /// Number of block-sized slots in the mapping.
    nslots: usize,
    /// Free slot indices, used as a stack.
    free_slots: Vec<u32>,
    /// The backing file, held to own the descriptor: its `Drop` closes the fd once the
    /// mapping is torn down.
    ///
    /// Read only by `punch`, which is Linux-only, so elsewhere the field is carried and
    /// never looked at -- which is its job here.
    #[cfg_attr(not(target_os = "linux"), allow(dead_code))]
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
        write!(f, "  data cache blocks limit: {}, data lru cache size: {}, data dirty size: {}, free slots: {}/{}",
            self.data_cache_blocks, self.data_blocks_cache.len(), self.data_blocks_dirty.len(),
            self.free_slots.len(), self.nslots)
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
    /// Number of slots to provide beyond the clean-tier capacity.
    ///
    /// Dirty blocks live in the same mapping as clean ones, and the
    /// dirty set is bounded by the flush threshold rather than by
    /// `data_cache_blocks`, so the pool needs room for both. Anything
    /// past this falls back to heap blocks.
    const DIRTY_SLOT_HEADROOM: usize = 1024;

    fn pool_slots(data_cache_blocks: usize, max_dirty_blocks: usize) -> usize {
        // At least one slot: `mmap` rejects a zero length, which is
        // what a freshly created (empty) file used to produce.
        (data_cache_blocks + max_dirty_blocks + Self::DIRTY_SLOT_HEADROOM).max(1)
    }

    pub(crate) fn open_or_create(
        file_path: impl AsRef<Path>,
        max_dirty_blocks: usize,
        data_cache_blocks: usize,
        data_block_size: usize,
    ) -> Result<Self> {
        let nslots = Self::pool_slots(data_cache_blocks, max_dirty_blocks);
        let size = nslots * data_block_size;

        // The cache is not persistent: nothing repopulates the LRU at
        // open, so a pre-existing file's contents are unreachable
        // either way. Reuse the path if it is there, size it to the
        // pool, and treat the contents as scratch.
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(file_path)?;
        let fd = file.as_raw_fd();

        unsafe {
            let ret = libc::ftruncate(fd, size as libc::off_t);
            if ret != 0 {
                return Err(Error::last_os_error());
            }
        }

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

        debug!("local disk cache - {} slots of {} bytes, {} bytes mapped",
            nslots, data_block_size, size);

        Ok(Self {
            addr: addr as u64,
            size,
            nslots,
            // Hand out low slots first; reverse so popping ascends.
            free_slots: (0..nslots as u32).rev().collect(),
            file,
            closed: std::sync::atomic::AtomicBool::new(false),
            data_blocks_cache,
            data_blocks_dirty: BTreeMap::new(),
            data_cache_blocks,
            data_block_size,
        })
    }

    /// Byte offset of a slot within the mapping.
    #[inline]
    fn slot_offset(&self, slot: u32) -> usize {
        slot as usize * self.data_block_size
    }

    /// Which slot a block occupies, or `None` if it is a heap block
    /// handed out because the pool was exhausted.
    ///
    /// Derived from the block's address rather than tracked in a
    /// side table, so it cannot fall out of sync with reality.
    fn slot_of(&self, block: &DataBlock) -> Option<u32> {
        let p = block.as_slice().as_ptr() as usize;
        let base = self.addr as usize;
        if p < base || p >= base + self.size {
            return None;
        }
        Some(((p - base) / self.data_block_size) as u32)
    }

    /// Take a slot-backed, zeroed, dirty, locked block, falling back
    /// to a heap block when the pool is empty.
    fn new_dirty_block(&mut self, blk_idx: BlockIndex) -> DataBlock {
        let Some(slot) = self.free_slots.pop() else {
            warn!("local disk cache - slot pool of {} exhausted, falling back to a heap block for index {}",
                self.nslots, blk_idx);
            let block = DataBlock::new_alloc(blk_idx, self.data_block_size);
            block.set_dirty();
            block.lock();
            return block;
        };
        let ptr = unsafe {
            (self.addr as *mut u8).add(self.slot_offset(slot))
        };
        let block = DataBlock::new_mmap(blk_idx, ptr, self.data_block_size);
        // Zero explicitly. Releasing a slot punches its hole, so a
        // recycled slot normally reads as zeros already, but relying
        // on that makes every future release path load-bearing for
        // correctness rather than only for space. Callers that only
        // partially fill a new block have been a repeated source of
        // stale-data bugs.
        block.as_mut_slice().fill(0);
        block.set_dirty();
        block.lock();
        block
    }

    /// Return a block's slot to the pool and reclaim its disk space.
    ///
    /// Punching the hole is what frees the space; the slot reads as
    /// zeros afterwards, though `new_dirty_block` does not depend on
    /// that.
    fn release(&mut self, block: &DataBlock) {
        let Some(slot) = self.slot_of(block) else {
            return; // heap fallback block, nothing to reclaim
        };
        self.punch(slot);
        self.free_slots.push(slot);
    }

    /// Hand the slot's space back to the filesystem, keeping the file's length.
    ///
    /// Space reclamation and nothing else: the slot goes straight onto `free_slots` and
    /// is written in full before it is read again, so a host that skips this pays in
    /// footprint. The cache file is created at its full length and is sparse until
    /// written, so skipping means it ends up fully allocated once every slot has been
    /// used once.
    ///
    /// It does change what a bug in slot accounting would look like. After a punch the
    /// region reads as zeros; without one it reads as whatever the previous tenant left.
    /// Neither is correct, but they fail differently.
    #[cfg(target_os = "linux")]
    fn punch(&self, slot: u32) {
        let fd = self.file.as_raw_fd();
        let offset = self.slot_offset(slot) as libc::off_t;
        let len = self.data_block_size as libc::off_t;
        let ret = unsafe {
            libc::fallocate(fd, libc::FALLOC_FL_PUNCH_HOLE | libc::FALLOC_FL_KEEP_SIZE, offset, len)
        };
        if ret == -1 {
            panic!("fallocate failed to punch hole at offset: {}, len: {}, error: {}",
                offset, len, Error::last_os_error());
        }
    }

    /// `fallocate` is Linux's. darwin has `fcntl(F_PUNCHHOLE)` and this could use it;
    /// what it would buy is the footprint of a local cache file, which is why it does
    /// not. See the Linux one above for what going without costs.
    #[cfg(not(target_os = "linux"))]
    fn punch(&self, slot: u32) {
        let _ = slot;
    }

    /// Sync and unmap, exactly once.
    ///
    /// The file descriptor is deliberately *not* closed here. It is
    /// owned by `self.file`, whose own `Drop` closes it; closing it
    /// by hand left the `File` holding a descriptor that had already
    /// been handed back to the OS, which Rust's I/O safety checks
    /// abort on ("owned file descriptor already closed"). Combined
    /// with `shutdown` and `Drop` both tearing down, releasing a
    /// file that used this cache aborted the process.
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
    /// No-op. The mapping is a fixed slot pool, so it does not track
    /// the file's size; see the type-level docs.
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
        let displaced = self.data_blocks_dirty.insert(blk_idx, dirty_block);
        if let Some(old) = &displaced {
            self.release(old);
        }
        displaced
    }

    /// Install a clean, freshly-loaded block in the LRU. See the
    /// trait docs.
    ///
    /// The incoming block is a heap allocation, but this tier's
    /// clean entries must be views into the backing file so that
    /// eviction can reclaim their space with a hole punch. So the
    /// bytes are copied into a file-backed view for the block's
    /// index and the heap block is dropped.
    ///
    /// When the cache is disabled nothing is written to the file at
    /// all, so unlike `clear_dirty` there is no hole to punch on the
    /// way out.
    /// Side-effect-free probe; see the trait docs. Notably this does
    /// *not* mlock the block the way `get` does, which is the whole
    /// reason it exists.
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
            return Some(block);
        }
        debug_assert!(!block.is_dirty(), "insert_clean given a dirty block");
        debug_assert!(!self.data_blocks_dirty.contains_key(&blk_idx));

        // `new_dirty_block` is the only way to mint a view into the
        // backing file; it hands back a dirty, locked block, so undo
        // both once the bytes are in place. Order matters: `unlock`
        // refuses to act on a block still marked dirty.
        let mut cached = self.new_dirty_block(blk_idx);
        cached.copy(0, block.as_slice());
        cached.set_should_cache();
        cached.clear_dirty();
        cached.unlock();

        if let Some((old_blk_idx, old)) = self.data_blocks_cache.push(blk_idx, cached) {
            if old_blk_idx == blk_idx {
                panic!("block already exists, failed to insert clean block index {} into data blocks cache", blk_idx);
            }
            self.release(&old);
        }
        None
    }

    fn remove(&mut self, blk_idx: &BlockIndex) -> bool {
        let mut removed = false;
        if let Some(block) = self.data_blocks_cache.pop(&blk_idx) {
            self.release(&block);
            removed = true;
        }
        if let Some(block) = self.data_blocks_dirty.remove(blk_idx) {
            block.clear_dirty();
            block.unlock();
            self.release(&block);
            removed = true;
        }
        removed
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
            if let Some(block) = self.data_blocks_dirty.remove(&k) {
                removed += 1;
                self.release(&block);
            }
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
                if let Some(block) = self.data_blocks_cache.pop(&k) {
                    self.release(&block);
                }
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
                // backing file rather than on the heap, so its slot
                // has to go back to the pool.
                self.release(&block);
                continue;
            }
            // push into cache list
            if let Some((old_blk_idx, old)) = self.data_blocks_cache.push(blk_idx, block) {
                if old_blk_idx == blk_idx {
                    panic!("block already exists, failed to put back block index {} into data blocks cache", blk_idx);
                }
                self.release(&old);
            }
        }
    }

    fn demote_dirty(&mut self, indexes: &[BlockIndex]) {
        for blk_idx in indexes {
            let Some(block) = self.data_blocks_dirty.remove(blk_idx) else {
                // Dirtied again since the partial was built, or evicted. Either
                // way it is not this call's business — see the trait's note.
                continue;
            };
            block.clear_dirty();
            block.unlock();
            if self.data_cache_blocks == 0 {
                // Same reasoning as `clear_dirty`: these bytes live in the backing
                // file, so the slot has to go back to the pool.
                self.release(&block);
                continue;
            }
            if let Some((old_blk_idx, old)) = self.data_blocks_cache.push(*blk_idx, block) {
                if old_blk_idx == *blk_idx {
                    panic!("block already exists, failed to put back block index {} into data blocks cache", blk_idx);
                }
                self.release(&old);
            }
        }
    }

    fn clear_data_blocks_cache(&mut self) {
        if self.data_cache_blocks > 0 {
            while let Some((_, block)) = self.data_blocks_cache.pop_lru() {
                self.release(&block);
            }
        }
    }

    fn shutdown(&self) {
        self.teardown().expect("local disk cache - failed to tear down cache");
    }
}
