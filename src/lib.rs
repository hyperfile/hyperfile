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

/// Which stored piece of the container a block lives in.
///
/// `seq_id` is the checkpoint: ordered, incremented, and persisted as
/// `i_last_cno` / `i_last_seq` / `s_cno`. It is a `u32` because that is what a
/// block pointer can address — 30 bits of it — and a wider type would promise
/// range the pointer cannot name.
///
/// `part_id` is which object of that checkpoint, for a container whose format
/// streams a checkpoint out as several — see
/// [`meta_format::BlockPtrFormat::PartedSegment`]. `None` is a checkpoint written
/// as a single object, which is every format before parting, and it keeps the
/// object name it has always had.
///
/// `None` and `Some(0)` are deliberately different: they name different keys, and
/// treating them as one would mean a parted container and an unparted one
/// disagreeing about what a bare name holds.
///
/// Ordering is by checkpoint first, then unparted before parted, then part. That
/// matters because recovery filters the log by this order, and a part index must
/// never outrank a checkpoint.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug, Default)]
pub struct SegmentId {
    seq_id: u32,
    part_id: Option<u16>,
}

pub type SegmentOffset = usize;

impl SegmentId {
    /// A checkpoint written as a single object.
    #[inline]
    pub const fn new(seq_id: u32) -> Self {
        Self { seq_id, part_id: None }
    }

    /// One object of a checkpoint streamed out in pieces.
    #[inline]
    pub const fn with_part(seq_id: u32, part_id: u16) -> Self {
        Self { seq_id, part_id: Some(part_id) }
    }

    /// From what a block pointer decoded to.
    #[inline]
    pub const fn from_parts(seq_id: u32, part_id: Option<u16>) -> Self {
        Self { seq_id, part_id }
    }

    /// The checkpoint this belongs to.
    #[inline]
    pub const fn seq_id(&self) -> u32 {
        self.seq_id
    }

    /// Which object of the checkpoint, or `None` when it is the only one.
    #[inline]
    pub const fn part_id(&self) -> Option<u16> {
        self.part_id
    }

    /// Whether the checkpoint was streamed as several objects.
    #[inline]
    pub const fn is_parted(&self) -> bool {
        self.part_id.is_some()
    }

    /// The checkpoint number as the inode and the segment header store it. Drops
    /// the part, which those fields have no room for and no use for: they name a
    /// checkpoint, not one of its pieces.
    #[inline]
    pub const fn as_cno(&self) -> u64 {
        self.seq_id as u64
    }

    /// The same checkpoint without a part — its identity as a checkpoint rather
    /// than as a stored object.
    #[inline]
    pub const fn whole(&self) -> Self {
        Self::new(self.seq_id)
    }

    /// Where this checkpoint's summary, metadata and inode live: part 0 when it
    /// was streamed, and the one object otherwise.
    #[inline]
    pub const fn summary(&self) -> Self {
        match self.part_id {
            Some(_) => Self::with_part(self.seq_id, 0),
            None => *self,
        }
    }

    /// From a checkpoint number that came from a persisted field or a u64 API.
    #[inline]
    pub fn new_from_cno(cno: u64) -> Self {
        Self::from(cno)
    }

    /// The same checkpoint, naming one of its objects.
    #[inline]
    pub const fn at_part(&self, part_id: u16) -> Self {
        Self::with_part(self.seq_id, part_id)
    }

    /// The next checkpoint. Never carries a part: a part is a property of how one
    /// checkpoint was written out, not something to count through.
    #[inline]
    pub const fn next(&self) -> Self {
        Self::new(self.seq_id + 1)
    }

    /// The previous checkpoint, saturating at zero. The log prefix feeding a
    /// checkpoint is the one below it.
    #[inline]
    pub const fn prev(&self) -> Self {
        Self::new(self.seq_id.saturating_sub(1))
    }
}

impl From<u64> for SegmentId {
    /// From a persisted checkpoint number. Refuses in debug what a pointer could
    /// not have named anyway.
    #[inline]
    fn from(cno: u64) -> Self {
        debug_assert!(cno <= u32::MAX as u64, "checkpoint {} is past what a pointer can name", cno);
        Self::new(cno as u32)
    }
}

impl From<u32> for SegmentId {
    #[inline]
    fn from(seq_id: u32) -> Self {
        Self::new(seq_id)
    }
}

impl std::fmt::Display for SegmentId {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self.part_id {
            None => write!(f, "{}", self.seq_id),
            Some(p) => write!(f, "{}.{}", self.seq_id, p),
        }
    }
}

#[cfg(test)]
mod segment_id_tests {
    use super::*;

    #[test]
    fn whole_and_part_round_trip() {
        let w = SegmentId::new(12345);
        assert_eq!(w.seq_id(), 12345);
        assert_eq!(w.part_id(), None);
        assert!(!w.is_parted());

        for part in [0u16, 1, 255, 16383] {
            let o = SegmentId::with_part(12345, part);
            assert_eq!(o.seq_id(), 12345);
            assert_eq!(o.part_id(), Some(part));
            assert!(o.is_parted());
        }
    }

    /// The whole reason this is not a packing of `SegmentId`: an object written
    /// as one piece and part 0 of a streamed one are different objects, and a
    /// representation that conflated them would name the wrong key.
    #[test]
    fn unparted_is_not_part_zero() {
        assert_ne!(SegmentId::new(7), SegmentId::with_part(7, 0));
    }

    #[test]
    fn new_follows_what_a_pointer_decoded_to() {
        assert_eq!(SegmentId::from_parts(9, None), SegmentId::new(9));
        assert_eq!(SegmentId::from_parts(9, Some(4)), SegmentId::with_part(9, 4));
    }

    /// Ordered by checkpoint first. Recovery and listing order by checkpoint, so
    /// a part index must never outrank one.
    #[test]
    fn ordering_is_checkpoint_major() {
        assert!(SegmentId::with_part(5, 16383) < SegmentId::new(6));
        assert!(SegmentId::new(5) < SegmentId::with_part(5, 0));
        assert!(SegmentId::with_part(5, 1) < SegmentId::with_part(5, 2));
    }

    #[test]
    fn summary_of_names_part_zero_or_itself() {
        assert_eq!(SegmentId::with_part(3, 9).summary(), SegmentId::with_part(3, 0));
        assert_eq!(SegmentId::new(3).summary(), SegmentId::new(3));
    }

    #[test]
    fn display_matches_the_object_naming() {
        assert_eq!(SegmentId::new(3).to_string(), "3");
        assert_eq!(SegmentId::with_part(3, 0).to_string(), "3.0");
        assert_eq!(SegmentId::with_part(3, 12).to_string(), "3.12");
    }
}

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
