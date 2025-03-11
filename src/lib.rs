pub mod config;
pub mod meta_format;
pub mod inode;
pub mod ondisk;
pub mod segment;
pub mod file;
pub mod s3uri;
pub(crate) mod meta_loader;
pub(crate) mod buffer;
pub(crate) mod staging;
pub(crate) mod s3commons;

pub type BlockIndex = u64;
pub type BlockPtr = u64;
pub type BlockOffset = usize;

pub type SegmentId = u64;
pub type SegmentOffset = usize;

struct BlockIndexIter {
    remain: usize,
    block_size: usize,
    current: usize, // current offset
}

impl BlockIndexIter {
    fn new(off: usize, len: usize, block_size: usize) -> Self {
        Self {
            remain: len,
            block_size: block_size,
            current: off,
        }
    }
}

impl Iterator for BlockIndexIter {
    // (block index, start offset with in block, length)
    type Item = (BlockIndex, BlockOffset, usize);

    fn next(&mut self) -> Option<Self::Item> {
        if self.remain > 0 {
            let blk_idx = (self.current / self.block_size) as BlockIndex;
            let start = self.current;
            let next_blk_idx = ((self.current + self.remain) / self.block_size) as BlockIndex;
            let len = if next_blk_idx > blk_idx {
                self.block_size - self.current % self.block_size
            } else {
                self.remain
            };
            // calc next current
            self.current = start + len;
            self.remain -= len;
            Some((blk_idx, start % self.block_size, len))
        } else {
            None
        }
    }
}

#[repr(C)]
pub(crate) struct BMapUserData {
    pub(crate) blk_ptr_format: meta_format::BlockPtrFormat,
    pub(crate) pad1: u8,
    pub(crate) pad2: u8,
    pub(crate) pad3: u8,
}

impl BMapUserData {
    pub(crate) fn new(blk_ptr_format: meta_format::BlockPtrFormat) -> Self {
        Self {
            blk_ptr_format,
            pad1: 0,
            pad2: 0,
            pad3: 0,
        }
    }

    pub(crate) fn from_u32(user_data: u32) -> Self {
        let mut bmap_user_data = unsafe { std::mem::MaybeUninit::zeroed().assume_init() };
        let ptr = std::ptr::addr_of_mut!(bmap_user_data) as *mut u32;
        unsafe {
            std::ptr::write_volatile(ptr, user_data);
        }
        bmap_user_data
    }

    pub(crate) fn as_u32(&self) -> u32 {
        let ptr = std::ptr::addr_of!(*self) as *const u32;
        unsafe {
            *ptr
        }
    }
}

pub(crate) const DEFAULT_FORWARD_ORIGIN_CONCURRENCY: usize = 10;
pub(crate) const DEFAULT_FORWARD_ORIGIN_THRESHOLD: usize = 8 * 1024 * 1024;
pub(crate) const DEFAULT_FORWARD_ORIGIN_CHUNK_SIZE: usize = 8 * 1024 * 1024;

pub(crate) const DEFAULT_SEGMENT_BUFFER_SIZE: usize = 100 * 1024 * 1024;
pub(crate) const DEFAULT_LARGE_SEGMENT_BUFFER_SIZE: usize = 1024 * 1024 * 1024;
pub(crate) const DEFAULT_SEGMENT_MPU_CHUNK_SIZE: usize = 16 * 1024 * 1024;
// count of data blocks kept by each opened hyper file in LRU
// CAUTION: this value should be large enough to hold the largest stripe of a single FUSE write
//   default continues write is 128K, max could be 1M ?
pub(crate) const DEFAULT_DATA_CACHE_BLOCKS: usize = 1024;
pub(crate) const DEFAULT_LARGE_DATA_CACHE_BLOCKS: usize = 256000;
/// Dirty data in bytes threshold to force a flush
///
/// This value combine with `DEFAULT_DIRTY_DATA_BLOCKS_THRESHOLD` will determine max data/blocks
/// each HyperFile can hold before a force flush
pub(crate) const DEFAULT_MAX_DIRTY_DATA_BYTES_THRESHOLD: usize = 8_388_608;
pub(crate) const DEFAULT_LARGE_MAX_DIRTY_DATA_BYTES_THRESHOLD: usize = 1024 * 1024 * 1024;
/// Dirty data in blocks threshold to force a flush
pub(crate) const DEFAULT_MAX_DIRTY_DATA_BLOCKS_THRESHOLD: usize = 32;
pub(crate) const DEFAULT_LARGE_MAX_DIRTY_DATA_BLOCKS_THRESHOLD: usize = 256000;
/// Max interval threshold in milliseconds to force a flush,
///
/// normally time based force flush should be triggered by external,
/// this value set a protection for dirty blocks in case external trigger missed,
/// but we still relying on external timer to trigger force flush,
/// this value CAN NOT guarantee all dirty blocks been flushed when no incoming write op.
pub(crate) const DEFAULT_MAX_DIRTY_DATA_FLUSH_INTERVAL: u64 = 5000;

pub(crate) const DEFAULT_FLUSH_RETRIES: usize = 3;
pub(crate) const DEFAULT_FLUSH_BACKOFF_SECS: u64 = 1;
pub(crate) const DEFAULT_PARTIAL_FLUSH_TIMEOUT: u64 = 5;
pub(crate) const DEFAULT_PARTIAL_FLUSH_CHECK_INTERVAL_SECS: u64 = 1;
