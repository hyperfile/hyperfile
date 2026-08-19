//! Block-level borrow API.
//!
//! The byte API (`fs_read` / `fs_write`) copies: a read copies out
//! of the cached block into the caller's buffer, a write copies the
//! caller's buffer into the cached block. Callers that are really
//! block-storage consumers — they only ever read block N, modify
//! block N, and write block N back — pay for a buffer they do not
//! want, and typically end up keeping a shadow copy of every block
//! they intend to modify so that they can write it out later.
//!
//! This module lets such a caller *borrow* the cached block
//! instead. [`BlockRef`] hands out `&[u8]` for reading;
//! [`BlockMut`] hands out `&mut [u8]` for modification in the very
//! buffer the next flush will write, so no shadow copy and no
//! write-back step is needed.
//!
//! # Holes
//!
//! The byte API cannot distinguish a hole from a block of zeros:
//! reading either returns zeros. Block borrows can, because
//! acquiring a block consults the bmap directly. See
//! [`BlockState`], and note that [`HyperFile::block`] returns
//! `Ok(None)` for anything not backed by real data.
//!
//! [`HyperFile::block`]: super::file::HyperFile::block
//!
//! # Guards and flush
//!
//! A guard borrows the file, so the borrow checker alone prevents
//! any other operation — including flush and eviction — while a
//! guard is alive. No runtime pinning is involved. The flip side is
//! that a guard cannot be held across a flush or across an `await`
//! on the same file; acquire it, use it, drop it.
//!
//! # `i_size` is not touched
//!
//! Neither guard changes `i_size`. Callers of this API manage their
//! own address-space layout and do not want a block write to move
//! EOF. A consequence worth understanding: a block dirtied above
//! `i_size` *is* flushed durably (the dirty set drives segment
//! build), but `fs_read` will not return it, because `read` stops
//! at `i_size`. Such blocks are reachable only through this API.
//! `i_blocks` *is* updated, so `st_blocks` still accounts for the
//! storage.

use std::fmt;
use std::io::Result;
use crate::BlockIndex;
use crate::buffer::DataBlock;

/// How a block index is mapped, as recorded in the bmap.
///
/// Distinguishing these is the point of [`HyperFile::block_state`]:
/// through the byte API all three read back as zeros, so a caller
/// that keeps its own index cannot tell "this block was never
/// written" from "this block holds real data that happens to be
/// zeros" — which is the difference between a consistent index and
/// a lost block.
///
/// [`HyperFile::block_state`]: super::file::HyperFile::block_state
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BlockState {
    /// No bmap entry for this index: never written. Reads as zeros.
    Unmapped,
    /// Mapped to an explicit zero block, from `write_zero`, a
    /// truncate, or an aligned batch with `is_zero`. Reads as
    /// zeros and occupies no staging space, but unlike
    /// [`Self::Unmapped`] the index *is* present in the bmap.
    Zero,
    /// Mapped to real data, either dirty in cache or persisted in
    /// staging.
    Mapped,
}

impl BlockState {
    /// Whether reading this block yields data rather than zeros.
    #[inline]
    pub fn is_mapped(&self) -> bool {
        matches!(self, Self::Mapped)
    }

    /// Whether this block reads back as zeros — either because it
    /// has no bmap entry or because it is an explicit zero block.
    #[inline]
    pub fn is_hole(&self) -> bool {
        matches!(self, Self::Unmapped | Self::Zero)
    }
}

/// A read-only borrow of one data block.
///
/// Obtained from [`HyperFile::block`]. The slice from
/// [`Self::as_slice`] is the cache's own buffer, not a copy, and is
/// always exactly one `data_block_size` long.
///
/// When the data cache is disabled — which `O_DIRECT` without `wal`
/// forces — there is no cached buffer to borrow, so the guard owns
/// the block it loaded instead. That is invisible through this API
/// except that the bytes are not retained for a subsequent call.
///
/// [`HyperFile::block`]: super::file::HyperFile::block
pub struct BlockRef<'a> {
    inner: BlockRefInner<'a>,
}

enum BlockRefInner<'a> {
    /// Borrowed from the cache. Must be unlocked on drop.
    Cached(&'a DataBlock),
    /// Loaded but not cacheable; owned by this guard and freed with
    /// it.
    Owned(DataBlock),
}

impl<'a> BlockRef<'a> {
    pub(crate) fn cached(block: &'a DataBlock) -> Self {
        Self { inner: BlockRefInner::Cached(block) }
    }

    pub(crate) fn owned(block: DataBlock) -> Self {
        Self { inner: BlockRefInner::Owned(block) }
    }

    #[inline]
    fn block(&self) -> &DataBlock {
        match &self.inner {
            BlockRefInner::Cached(b) => b,
            BlockRefInner::Owned(b) => b,
        }
    }

    /// The block's contents. Always `data_block_size` bytes.
    #[inline]
    pub fn as_slice(&self) -> &[u8] {
        self.block().as_slice()
    }

    /// The block index this guard was acquired for.
    #[inline]
    pub fn index(&self) -> BlockIndex {
        self.block().index()
    }

    /// Length in bytes, i.e. `data_block_size`.
    #[inline]
    pub fn len(&self) -> usize {
        self.block().size()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl Drop for BlockRef<'_> {
    fn drop(&mut self) {
        if let BlockRefInner::Cached(block) = &self.inner {
            // Mirrors the byte read path, which unlocks after
            // copying out of a cache hit. On the local-disk tier
            // `Cache::get` mlocks a block it hands out of the clean
            // tier and asserts the block is *not* already locked on
            // the next `get`, so failing to unlock here would turn a
            // second read of the same clean block into a panic. A
            // no-op for in-memory blocks, and for dirty blocks,
            // which stay locked until flush clears the dirty flag.
            block.unlock();
        }
        // The owned variant needs nothing: `DataBlock`'s own `Drop`
        // releases the allocation.
    }
}

/// Deliberately does not print the block contents: these are
/// `data_block_size` bytes, and a `{:?}` in a log line should not
/// dump a whole block.
impl fmt::Debug for BlockRef<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BlockRef")
            .field("index", &self.index())
            .field("len", &self.len())
            .field("cached", &matches!(self.inner, BlockRefInner::Cached(_)))
            .finish()
    }
}

/// A mutable borrow of one cached data block.
///
/// Obtained from [`HyperFile::block_mut`]. Writes through
/// [`Self::as_mut_slice`] land in the buffer the next flush will
/// write out; no write-back call is needed. The block is marked
/// dirty at acquisition, not at drop, so an early return or a panic
/// between acquire and drop cannot lose the modification.
///
/// Because the block is dirty from acquisition, acquiring a
/// `BlockMut` and *not* modifying it still produces a new version
/// at the next flush — the same as writing identical bytes through
/// `fs_write`. Use [`HyperFile::block`] when a read-only borrow
/// will do.
///
/// [`HyperFile::block_mut`]: super::file::HyperFile::block_mut
/// [`HyperFile::block`]: super::file::HyperFile::block
pub struct BlockMut<'a> {
    block: &'a mut DataBlock,
}

impl<'a> BlockMut<'a> {
    pub(crate) fn new(block: &'a mut DataBlock) -> Self {
        Self { block }
    }

    /// The block's current contents. Always `data_block_size`
    /// bytes.
    #[inline]
    pub fn as_slice(&self) -> &[u8] {
        self.block.as_slice()
    }

    /// The block's contents, mutably. Always `data_block_size`
    /// bytes.
    ///
    /// Takes `&mut self` deliberately. `DataBlock`'s own accessor
    /// takes `&self` and hands out `&mut [u8]` through interior
    /// mutability, which is sound only for hyperfile's serialized
    /// internal callers; exposing that shape publicly would let a
    /// caller mint two aliasing `&mut [u8]` for one block. Routing
    /// mutation through the guard keeps the aliasing rules with
    /// the borrow checker.
    #[inline]
    pub fn as_mut_slice(&mut self) -> &mut [u8] {
        self.block.as_mut_slice()
    }

    /// The block index this guard was acquired for.
    #[inline]
    pub fn index(&self) -> BlockIndex {
        self.block.index()
    }

    /// Length in bytes, i.e. `data_block_size`.
    #[inline]
    pub fn len(&self) -> usize {
        self.block.size()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Fill the whole block with zeros.
    ///
    /// A convenience for callers initializing a freshly created
    /// block; equivalent to `self.as_mut_slice().fill(0)`.
    #[inline]
    pub fn zero(&mut self) {
        self.as_mut_slice().fill(0);
    }
}

/// Deliberately does not print the block contents; see
/// [`BlockRef`]'s implementation.
impl fmt::Debug for BlockMut<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BlockMut")
            .field("index", &self.index())
            .field("len", &self.len())
            .finish()
    }
}

/// Result of asking for a block that may not be backed by data.
///
/// Kept as a type alias rather than an enum so that the common
/// case reads as an `Option`: `None` means "no data here", which
/// for a caller checking its own index against hyperfile's is the
/// interesting signal. Call
/// [`HyperFile::block_state`](super::file::HyperFile::block_state)
/// when the `Unmapped` / `Zero` distinction matters.
pub type MaybeBlockRef<'a> = Result<Option<BlockRef<'a>>>;

/// Mutable counterpart of [`MaybeBlockRef`].
pub type MaybeBlockMut<'a> = Result<Option<BlockMut<'a>>>;
