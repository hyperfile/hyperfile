// Feature `reactor` and `blocking` are mutually exclusive. They pick
// conflicting configurations of `btree-ondisk` (Arc vs Rc for the
// internal nodes). btree-ondisk has its own `compile_error!` for
// this, which in practice fires first; this guard documents the
// contract as part of this crate's own API and gives the reader a
// one-line explanation of why.
#[cfg(all(feature = "reactor", feature = "blocking"))]
compile_error!(
    "features `reactor` and `blocking` are mutually exclusive: \
     `reactor` enables btree-ondisk/arc, `blocking` enables \
     btree-ondisk/rc. Pick one."
);

pub mod config;
pub mod meta_format;
pub mod inode;
pub mod ondisk;
pub mod segment;
pub mod file;
pub mod s3uri;
pub mod buffer;
pub mod utils;
pub mod staging;
pub mod meta_loader;
pub mod data_cache;
pub(crate) mod s3commons;
pub(crate) mod segment_body;
#[cfg(feature = "wal")]
pub mod wal;
pub mod node_cache;

pub type BlockIndex = u64;
pub type BlockPtr = u64;
pub type BlockOffset = usize;

pub type SegmentId = u64;
pub type SegmentOffset = usize;

pub struct BlockIndexIter {
    remain: usize,
    block_size: usize,
    current: usize, // current offset
}

impl BlockIndexIter {
    pub fn new(off: usize, len: usize, block_size: usize) -> Self {
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
pub struct BMapUserData {
    pub blk_ptr_format: meta_format::BlockPtrFormat,
    pad1: u8,
    pad2: u8,
    pad3: u8,
}

impl BMapUserData {
    pub fn new(blk_ptr_format: meta_format::BlockPtrFormat) -> Self {
        Self {
            blk_ptr_format,
            pad1: 0,
            pad2: 0,
            pad3: 0,
        }
    }

    pub fn from_u32(user_data: u32) -> Self {
        let mut bmap_user_data = unsafe { std::mem::MaybeUninit::zeroed().assume_init() };
        let ptr = std::ptr::addr_of_mut!(bmap_user_data) as *mut u32;
        unsafe {
            std::ptr::write_volatile(ptr, user_data);
        }
        bmap_user_data
    }

    pub fn as_u32(&self) -> u32 {
        let ptr = std::ptr::addr_of!(*self) as *const u32;
        unsafe {
            *ptr
        }
    }
}

pub(crate) const DEFAULT_FORWARD_ORIGIN_CONCURRENCY: usize = 10;
pub(crate) const DEFAULT_FORWARD_ORIGIN_THRESHOLD: usize = 8 * 1024 * 1024;
pub(crate) const DEFAULT_FORWARD_ORIGIN_CHUNK_SIZE: usize = 8 * 1024 * 1024;

// Read-coalescing (Level A read-side perf): when a single read
// spans multiple consecutive blocks that map to a contiguous range
// in the same segment, the read path issues ONE byte-range S3 GET
// instead of one per block. The caps below bound a single
// coalesced GET (so we don't issue a 1-GiB request) and the number
// of concurrent in-flight coalesced GETs spawned per single read
// (backpressure for the SDK connection pool / bandwidth).
pub(crate) const DEFAULT_READ_GET_MAX_BYTES: usize = 16 * 1024 * 1024;
pub(crate) const DEFAULT_READ_MAX_CONCURRENCY: usize = 10;

pub(crate) const DEFAULT_SEGMENT_BUFFER_SIZE: usize = 100 * 1024 * 1024;
pub(crate) const DEFAULT_MIDDLE_SEGMENT_BUFFER_SIZE: usize = 256 * 1024 * 1024;
pub(crate) const DEFAULT_LARGE_SEGMENT_BUFFER_SIZE: usize = 1024 * 1024 * 1024;
pub(crate) const DEFAULT_SEGMENT_MPU_CHUNK_SIZE: usize = 16 * 1024 * 1024;
// count of data blocks kept by each opened hyper file in LRU
// CAUTION: this value should be large enough to hold the largest stripe of a single FUSE write
//   default continues write is 128K, max could be 1M ?
pub(crate) const DEFAULT_DATA_CACHE_BLOCKS: usize = 1024;
pub(crate) const DEFAULT_MIDDLE_DATA_CACHE_BLOCKS: usize = 65536;
pub(crate) const DEFAULT_LARGE_DATA_CACHE_BLOCKS: usize = 256000;
/// Dirty data in bytes threshold to force a flush
///
/// This value combine with `DEFAULT_DIRTY_DATA_BLOCKS_THRESHOLD` will determine max data/blocks
/// each HyperFile can hold before a force flush
pub(crate) const DEFAULT_MAX_DIRTY_DATA_BYTES_THRESHOLD: usize = 8_388_608;
pub(crate) const DEFAULT_MIDDLE_MAX_DIRTY_DATA_BYTES_THRESHOLD: usize = 256 * 1024 * 1024;
pub(crate) const DEFAULT_LARGE_MAX_DIRTY_DATA_BYTES_THRESHOLD: usize = 1024 * 1024 * 1024;
/// Dirty data in blocks threshold to force a flush
pub(crate) const DEFAULT_MAX_DIRTY_DATA_BLOCKS_THRESHOLD: usize = 32;
pub(crate) const DEFAULT_MIDDLE_MAX_DIRTY_DATA_BLOCKS_THRESHOLD: usize = 65536;
pub(crate) const DEFAULT_LARGE_MAX_DIRTY_DATA_BLOCKS_THRESHOLD: usize = 256000;
/// Max interval threshold in milliseconds to force a flush,
///
/// normally time based force flush should be triggered by external,
/// this value set a protection for dirty blocks in case external trigger missed,
/// but we still relying on external timer to trigger force flush,
/// this value CAN NOT guarantee all dirty blocks been flushed when no incoming write op.
pub(crate) const DEFAULT_MAX_DIRTY_DATA_FLUSH_INTERVAL: u64 = 5000;

pub(crate)  const DEFAULT_FLUSH_RETRIES: usize = 3;
pub(crate)  const DEFAULT_FLUSH_BACKOFF_SECS: u64 = 1;
pub(crate)  const DEFAULT_PARTIAL_FLUSH_TIMEOUT: u64 = 5;
pub(crate)  const DEFAULT_PARTIAL_FLUSH_CHECK_INTERVAL_SECS: u64 = 1;
// count of bmap in memory node cache
pub(crate) const DEFAULT_NODE_CACHE_BLOCKS: usize = 1024;
pub(crate) const DEFAULT_MAX_NODE_CACHE_BLOCKS: usize = btree_ondisk::DEFAULT_CACHE_UNLIMITED;

#[cfg(test)]
mod tests {
    use super::*;

    // --- BlockIndexIter ---

    #[test]
    fn block_index_iter_zero_length() {
        let iter = BlockIndexIter::new(0, 0, 4096);
        let items: Vec<_> = iter.collect();
        assert!(items.is_empty());
    }

    #[test]
    fn block_index_iter_within_single_block() {
        // read 100 bytes at offset 10 within block 0
        let items: Vec<_> = BlockIndexIter::new(10, 100, 4096).collect();
        assert_eq!(items, vec![(0, 10, 100)]);
    }

    #[test]
    fn block_index_iter_exact_block_aligned() {
        // read exactly one full block from offset 0
        let items: Vec<_> = BlockIndexIter::new(0, 4096, 4096).collect();
        assert_eq!(items, vec![(0, 0, 4096)]);
    }

    #[test]
    fn block_index_iter_cross_two_blocks() {
        // read 100 bytes starting at offset 4090 (6 bytes in block 0, 94 bytes in block 1)
        let items: Vec<_> = BlockIndexIter::new(4090, 100, 4096).collect();
        assert_eq!(items, vec![(0, 4090, 6), (1, 0, 94)]);
    }

    #[test]
    fn block_index_iter_cross_three_blocks() {
        // offset 4000, len 8192+96 = spans block 0 (tail), block 1 (full), block 2 (head)
        let items: Vec<_> = BlockIndexIter::new(4000, 4096 + 96 + 96, 4096).collect();
        // block 0: offset 4000, len 96 (4096-4000)
        // block 1: offset 0, len 4096
        // block 2: offset 0, len 96
        assert_eq!(items, vec![(0, 4000, 96), (1, 0, 4096), (2, 0, 96)]);
    }

    #[test]
    fn block_index_iter_starts_mid_block() {
        // offset 8192 is start of block 2 with 4096 block size
        let items: Vec<_> = BlockIndexIter::new(8192, 4096, 4096).collect();
        assert_eq!(items, vec![(2, 0, 4096)]);
    }

    #[test]
    fn block_index_iter_total_length_preserved() {
        let off = 1234;
        let len = 9999;
        let total: usize = BlockIndexIter::new(off, len, 4096).map(|(_, _, l)| l).sum();
        assert_eq!(total, len);
    }

    // --- BMapUserData ---

    #[test]
    fn bmap_user_data_round_trip() {
        let ud = BMapUserData::new(meta_format::BlockPtrFormat::MicroGroup);
        let val = ud.as_u32();
        let ud2 = BMapUserData::from_u32(val);
        assert_eq!(ud2.blk_ptr_format, meta_format::BlockPtrFormat::MicroGroup);
        assert_eq!(ud2.as_u32(), val);
    }

    #[test]
    fn bmap_user_data_flat() {
        let ud = BMapUserData::new(meta_format::BlockPtrFormat::Flat);
        let val = ud.as_u32();
        let ud2 = BMapUserData::from_u32(val);
        assert_eq!(ud2.blk_ptr_format, meta_format::BlockPtrFormat::Flat);
    }

    #[test]
    fn bmap_user_data_nop() {
        let ud = BMapUserData::new(meta_format::BlockPtrFormat::Nop);
        assert_eq!(ud.as_u32() & 0xFF, 0); // Nop = 0
    }
}
