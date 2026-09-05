pub mod file;
pub mod flags;
pub mod mode;
#[cfg(feature = "reactor")]
pub mod handler;
#[cfg(feature = "reactor")]
pub mod reactor;
pub mod hyper;
pub mod fs;
#[cfg(feature = "reactor")]
pub mod fh;
#[cfg(feature = "reactor")]
pub mod tokio_wrapper;
pub mod block;
#[cfg(feature = "range-lock")]
pub mod lock;
mod state;

use std::io::{Error, ErrorKind, Result};
use std::time::{Instant, Duration};
use std::sync::atomic::{AtomicU64, Ordering};
use std::collections::BTreeMap;
#[cfg(feature = "wal")]
use std::sync::Weak;
#[cfg(feature = "wal")]
use std::pin::Pin;
use log::{info, debug, warn};
use tokio::sync::{OwnedSemaphorePermit, OwnedMutexGuard};
use btree_ondisk::{bmap::BMap, BlockLoader, NodeValue, NodeCache};
use btree_ondisk::btree::BtreeNodeDirty;
#[cfg(all(feature = "wal", feature = "reactor"))]
use crate::file::handler::ChannelGroup;
#[cfg(all(feature = "wal", feature = "reactor"))]
use crate::file::handler::FileContext;
use crate::*;
use crate::buffer::DataBlock;
use crate::{BlockIndex, BlockPtr};
use crate::{SegmentId, SegmentOffset};
use crate::config::HyperFileConfig;
use crate::ondisk::{BMapRawType, InodeRaw};
use crate::inode::{Inode, OnDiskState, FlushInodeFlag};
use crate::staging::Staging;

/// Per-phase cumulative nanosecond counters for the flush path.
///
/// Instrumented from inside `flush_process`. Each phase's elapsed
/// time is added to the matching counter on every flush; the
/// `flush_count` counter is incremented once per completed flush.
///
/// Read externally via `HyperFile::flush_timing()`; reset via
/// `HyperFile::flush_timing_reset()`. Both accessors are
/// `#[doc(hidden)]` — the instrumentation is intended for
/// benchmarks and regression profiling, not part of the public
/// behavioral contract.
///
/// The WAL flush path is not yet instrumented; running this
/// against a WAL-enabled file will return zero counters.
#[derive(Default, Debug)]
pub struct FlushTiming {
    pub pre_build_ns: AtomicU64,
    pub build_segment_ns: AtomicU64,
    pub segment_done_ns: AtomicU64,
    pub flush_inode_ns: AtomicU64,
    pub cleanup_ns: AtomicU64,
    pub flush_count: AtomicU64,
}

/// Snapshot of `FlushTiming` values at a single point in time.
#[derive(Clone, Copy, Debug, Default)]
pub struct FlushTimingSnapshot {
    pub pre_build_ns: u64,
    pub build_segment_ns: u64,
    pub segment_done_ns: u64,
    pub flush_inode_ns: u64,
    pub cleanup_ns: u64,
    pub flush_count: u64,
}

impl FlushTiming {
    pub fn snapshot(&self) -> FlushTimingSnapshot {
        FlushTimingSnapshot {
            pre_build_ns: self.pre_build_ns.load(Ordering::Relaxed),
            build_segment_ns: self.build_segment_ns.load(Ordering::Relaxed),
            segment_done_ns: self.segment_done_ns.load(Ordering::Relaxed),
            flush_inode_ns: self.flush_inode_ns.load(Ordering::Relaxed),
            cleanup_ns: self.cleanup_ns.load(Ordering::Relaxed),
            flush_count: self.flush_count.load(Ordering::Relaxed),
        }
    }

    pub fn reset(&self) {
        self.pre_build_ns.store(0, Ordering::Relaxed);
        self.build_segment_ns.store(0, Ordering::Relaxed);
        self.segment_done_ns.store(0, Ordering::Relaxed);
        self.flush_inode_ns.store(0, Ordering::Relaxed);
        self.cleanup_ns.store(0, Ordering::Relaxed);
        self.flush_count.store(0, Ordering::Relaxed);
    }
}

/// One entry of a read plan: what reading part of a range would cost.
///
/// See [`HyperFile::read_plan`](crate::file::file::HyperFile::read_plan).
/// Entries account for the whole range asked about, in the order the
/// requests would be made, so a range that needs nothing fetched still
/// appears — as [`Self::Local`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PlannedRead {
    /// One object request, covering `[off, off + len)` of the file, served
    /// from `len` bytes at `at` in segment `segid`.
    ///
    /// May cover many consecutive blocks: a request is extended while the
    /// next block's location is adjacent in the same object and the
    /// request stays under `read_get_max_bytes`.
    Get {
        off: u64,
        len: u64,
        /// Which object this request hits — the checkpoint, and which of its
        /// pieces when it was streamed as several.
        ///
        /// The whole identity, not just the checkpoint: a streamed checkpoint has
        /// several places for a run of blocks to break, and two requests that look
        /// adjacent by `at` alone are two requests when their objects differ.
        segid: crate::SegmentId,
        at: u64,
    },
    /// `[off, off + len)` needs no object request: a hole, a block in the
    /// data cache, or — with `wal` — a block in a segment still pinned in
    /// memory.
    Local { off: u64, len: u64 },
}

impl PlannedRead {
    /// The file range this entry covers.
    pub fn range(&self) -> (u64, u64) {
        match self {
            Self::Get { off, len, .. } => (*off, *len),
            Self::Local { off, len } => (*off, *len),
        }
    }

    /// Whether this entry is an object request.
    pub fn is_get(&self) -> bool {
        matches!(self, Self::Get { .. })
    }
}

/// Read-side counters, the counterpart of [`FlushTiming`].
///
/// The read path's cost is dominated by object-store round trips, and
/// there was previously no way to count them: a caller can time a read
/// but cannot tell one request for a coalesced range from many, or a
/// cache hit from a fetch. That matters most for callers that assert on
/// round-trip counts rather than on wall time, since wall time varies
/// with the network.
///
/// Requests are split by what they fetch, because the three answer
/// different questions:
///
/// * data — how well block reads coalesce into ranged requests;
/// * meta — how many index nodes a lookup had to descend through;
/// * inode — one per cold open, so mostly a constant.
///
/// Counters are shared by every clone of a file's staging handle, so
/// they cover one file. They are never reset implicitly; call
/// [`Self::reset`] to bracket a measurement.
#[derive(Default, Debug)]
pub struct ReadTiming {
    /// Object requests issued to fetch data blocks. One coalesced
    /// range counts once, which is the point.
    pub data_gets: AtomicU64,
    /// Bytes returned by those requests, including any read past what
    /// the caller asked for as a result of coalescing.
    pub data_bytes: AtomicU64,
    /// Of `data_gets`, those issued by read-ahead rather than by a read.
    ///
    /// A subset, not a separate total: a read-ahead's request is counted in
    /// `data_gets` as well, so a read's own requests are
    /// `data_gets - read_ahead_gets`.
    ///
    /// Without this the two cannot be told apart, since the counter is
    /// incremented where the request is made and staging does not know who
    /// asked. A consumer comparing two files found one costing twice the
    /// requests of the other with both laid out identically, and could not
    /// establish whether they were measuring the layout or how much
    /// read-ahead each file attracted — the alternative being to turn
    /// read-ahead off, which measures a different system.
    pub read_ahead_gets: AtomicU64,
    /// Bytes returned by those requests. A subset of `data_bytes`, for the
    /// same reason.
    pub read_ahead_bytes: AtomicU64,
    /// Object requests issued to fetch meta (index) blocks.
    pub meta_gets: AtomicU64,
    /// Bytes returned by those requests.
    pub meta_bytes: AtomicU64,
    /// Object requests issued to fetch the inode.
    pub inode_gets: AtomicU64,
    /// Block reads served from the data cache, without reaching
    /// staging. Together with `data_gets` this separates "the cache
    /// worked" from "the read coalesced well".
    pub cache_hits: AtomicU64,
    /// Nanoseconds spent awaiting staging reads, so that a share of
    /// wall time can be attributed to the object store rather than
    /// inferred.
    pub staging_ns: AtomicU64,
    /// Block reads answered from a segment that is still being written
    /// out, which only happens with `wal`. These cost no object request
    /// and no wait for the flush, which is the point of doing them —
    /// counting them separately is the only way to tell that the WAL read
    /// path is working rather than merely correct.
    pub inflight_reads: AtomicU64,
}

/// Snapshot of [`ReadTiming`] values at a single point in time.
#[derive(Clone, Copy, Debug, Default)]
pub struct ReadTimingSnapshot {
    pub data_gets: u64,
    pub data_bytes: u64,
    /// Of `data_gets` / `data_bytes`, what read-ahead issued. See
    /// [`ReadTiming::read_ahead_gets`].
    pub read_ahead_gets: u64,
    pub read_ahead_bytes: u64,
    pub meta_gets: u64,
    pub meta_bytes: u64,
    pub inode_gets: u64,
    pub cache_hits: u64,
    pub staging_ns: u64,
    pub inflight_reads: u64,
}

impl ReadTimingSnapshot {
    /// Every object request, whatever it fetched.
    pub fn total_gets(&self) -> u64 {
        self.data_gets + self.meta_gets + self.inode_gets
    }

    /// Every byte fetched. Excludes the inode, whose size is not
    /// tracked separately.
    pub fn total_bytes(&self) -> u64 {
        self.data_bytes + self.meta_bytes
    }
}

impl ReadTiming {
    pub fn snapshot(&self) -> ReadTimingSnapshot {
        ReadTimingSnapshot {
            data_gets: self.data_gets.load(Ordering::Relaxed),
            data_bytes: self.data_bytes.load(Ordering::Relaxed),
            read_ahead_gets: self.read_ahead_gets.load(Ordering::Relaxed),
            read_ahead_bytes: self.read_ahead_bytes.load(Ordering::Relaxed),
            meta_gets: self.meta_gets.load(Ordering::Relaxed),
            meta_bytes: self.meta_bytes.load(Ordering::Relaxed),
            inode_gets: self.inode_gets.load(Ordering::Relaxed),
            cache_hits: self.cache_hits.load(Ordering::Relaxed),
            staging_ns: self.staging_ns.load(Ordering::Relaxed),
            inflight_reads: self.inflight_reads.load(Ordering::Relaxed),
        }
    }

    pub fn reset(&self) {
        self.data_gets.store(0, Ordering::Relaxed);
        self.read_ahead_gets.store(0, Ordering::Relaxed);
        self.read_ahead_bytes.store(0, Ordering::Relaxed);
        self.data_bytes.store(0, Ordering::Relaxed);
        self.meta_gets.store(0, Ordering::Relaxed);
        self.meta_bytes.store(0, Ordering::Relaxed);
        self.inode_gets.store(0, Ordering::Relaxed);
        self.cache_hits.store(0, Ordering::Relaxed);
        self.staging_ns.store(0, Ordering::Relaxed);
        self.inflight_reads.store(0, Ordering::Relaxed);
    }

    #[inline]
    pub(crate) fn add_data_get(&self, bytes: usize, elapsed_ns: u64) {
        self.data_gets.fetch_add(1, Ordering::Relaxed);
        self.data_bytes.fetch_add(bytes as u64, Ordering::Relaxed);
        self.staging_ns.fetch_add(elapsed_ns, Ordering::Relaxed);
    }

    /// Attribute a request already counted in `data_gets` to read-ahead.
    ///
    /// Called by the read-ahead path itself rather than by staging, which
    /// has no way to know: it sees a ranged load and nothing about its
    /// purpose. Called once per successful load, so a fetch that failed
    /// after staging counted it is not attributed here — the two counters
    /// disagree by that request, which is the honest answer.
    pub(crate) fn add_read_ahead_get(&self, bytes: usize) {
        self.read_ahead_gets.fetch_add(1, Ordering::Relaxed);
        self.read_ahead_bytes.fetch_add(bytes as u64, Ordering::Relaxed);
    }

    #[inline]
    pub(crate) fn add_meta_get(&self, bytes: usize, elapsed_ns: u64) {
        self.meta_gets.fetch_add(1, Ordering::Relaxed);
        self.meta_bytes.fetch_add(bytes as u64, Ordering::Relaxed);
        self.staging_ns.fetch_add(elapsed_ns, Ordering::Relaxed);
    }

    #[inline]
    pub(crate) fn add_inode_get(&self, elapsed_ns: u64) {
        self.inode_gets.fetch_add(1, Ordering::Relaxed);
        self.staging_ns.fetch_add(elapsed_ns, Ordering::Relaxed);
    }

    #[inline]
    pub(crate) fn add_cache_hit(&self) {
        self.cache_hits.fetch_add(1, Ordering::Relaxed);
    }

    #[cfg(feature = "wal")]
    #[inline]
    pub(crate) fn add_inflight_read(&self) {
        self.inflight_reads.fetch_add(1, Ordering::Relaxed);
    }
}

pub struct DirtyDataBlocks<'a> {
    pub inner: Option<BTreeMap<BlockIndex, &'a DataBlock>>,
    // if we need to owned (clone) the data
    pub owned: Option<BTreeMap<BlockIndex, DataBlock>>,
}

impl<'a> DirtyDataBlocks<'a> {
    pub fn len(&self) -> usize {
        if let Some(inner) = &self.inner {
            return inner.len();
        } else if let Some(owned) = &self.owned {
            return owned.len();
        }
        // if both inner/owned are None, return 0
        return 0;
    }

    pub fn data(&'a self) -> BTreeMap<BlockIndex, &'a DataBlock> {
        if let Some(inner) = &self.inner {
            return inner.clone();
        } else if let Some(owned) = &self.owned {
            let inner: BTreeMap<BlockIndex, &'a DataBlock> = owned
                                .iter()
                                .map(|(idx, blk)| (*idx, blk))
                                .collect();
            return inner;
        }
        panic!("invalid DirtyDataBlocks");
    }
}

pub trait HyperTrait<T: Staging<L> + segment::SegmentReadWrite + Send + Clone + 'static, L: BlockLoader<BlockPtr> + Clone, C: NodeCache<BlockPtr> + Clone, V: Copy + Default + std::fmt::Display + NodeValue + 'static> {
    // block ptr
    fn blk_ptr_encode(&self, segid: SegmentId, offset: SegmentOffset, seq: usize) -> BlockPtr;
    fn blk_ptr_decode(&self, blk_ptr: &BlockPtr) -> (SegmentId, SegmentOffset);
    fn blk_ptr_decode_display(&self, blk_ptr: &BlockPtr) -> String;
    // data cache
    fn clear_data_blocks_cache(&mut self);
    fn set_data_blocks_cache_unlimited(&mut self);
    fn restore_data_blocks_cache_limit(&mut self);
    // dirty data
    fn get_data_blocks_dirty(&self) -> DirtyDataBlocks<'_>;
    fn clear_data_blocks_dirty(&mut self);
    // lock
    fn lock(&self) -> impl Future<Output = OwnedSemaphorePermit>;
    fn unlock(&self, permit: OwnedSemaphorePermit);
    fn flush_lock(&self) -> impl Future<Output = OwnedMutexGuard<()>>;
    fn flush_unlock(&self, lock: OwnedMutexGuard<()>);
    // bmap
    fn bmap_as_slice(&self) -> &[u8];
    fn bmap_get_block_loader(&self) -> L;
    fn bmap_get_node_cache(&self) -> C;
    fn bmap_dirty(&self) -> bool;
    fn bmap_lookup_dirty(&self) -> Vec<BtreeNodeDirty<'_, BlockIndex, V, BlockPtr>>;
    fn bmap_assign_meta_node(&self, blk_ptr: BlockPtr, node: BtreeNodeDirty<'_, BlockIndex, V, BlockPtr>) -> impl Future<Output = Result<()>>;
    fn bmap_assign_data_node(&self, blk_idx: &BlockIndex, blk_ptr: BlockPtr) -> impl Future<Output = Result<()>>;
    fn bmap_clear_dirty(&mut self);
    fn bmap_update<'a>(&mut self, bmap: BMap<'a, BlockIndex, V, BlockPtr, L, C>);
    fn bmap_insert_dummy_value(bmap: &mut BMap<'_, BlockIndex, V, BlockPtr, L, C>, blk_idx: &BlockIndex) -> impl Future<Output = Result<Option<V>>>;
    fn bmap_set_cache_unlimited(&self) -> usize;
    fn bmap_set_cache_limit(&self, liimt: usize);
    // inode
    fn inode(&self) -> &Inode;
    fn inode_mut(&self) -> &mut Inode;
    // others
    fn staging(&self) -> &T;
    fn config(&self) -> &HyperFileConfig;
    fn set_last_flush(&mut self);
    fn sleep(dur: Duration) -> impl Future<Output = ()>;
    fn flush_timing(&self) -> &FlushTiming;

    /// `Err` once publishing has failed unrecoverably. See
    /// `State::publish_failed`.
    fn check_writable(&self) -> Result<()>;

    /// The write-ahead log, if one is configured.
    #[cfg(feature = "wal")]
    fn wal_mut(&mut self) -> Option<&mut Box<dyn crate::wal::WalReadWrite + Send>>;

    /// Whether an transaction is open. See `HyperFile::begin_txn`.
    #[cfg(feature = "wal")]
    fn in_txn_trait(&self) -> bool;

    /// Whether this flush may be satisfied by the log without publishing. See
    /// `HyperFileWalConfig::publish_every`.
    #[cfg(feature = "wal")]
    fn wal_may_defer_publish(&self) -> bool;

    /// Record that a flush was satisfied by the log.
    #[cfg(feature = "wal")]
    fn wal_count_deferred_barrier(&mut self);

    // wal
    #[cfg(feature = "wal")]
    fn wal_set_mem_segment(&self, mem_segid: SegmentId, mem_segdata: Weak<Pin<Box<Vec<u8>>>>) -> impl Future<Output = ()>;
    #[cfg(feature = "wal")]
    fn wal_clear_mem_segment(&self, mem_segid: SegmentId) -> impl Future<Output = ()>;
    /// Fire-and-forget delete of the WAL objects for the given
    /// segid. Spawned so the caller (typically the flush path)
    /// doesn't block on the round trip. If the delete fails or
    /// the spawned task is cancelled, the WAL entries remain on
    /// storage but recovery filters them out by last_ondisk_cno
    /// so correctness is unaffected.
    #[cfg(feature = "wal")]
    fn wal_spawn_delete_segment(&self, segid: SegmentId);

    /// Delete every log prefix this publish supersedes, and reset the deferred
    /// count.
    ///
    /// One publish can stand for several groups: each deferred flush sealed its
    /// own, and all of them are contained in the segment just written. Deleting
    /// only the newest would leave the rest behind, so the log would grow in
    /// proportion to how often publishing is deferred — which is the cost that
    /// deferral is trying to avoid paying elsewhere.
    #[cfg(feature = "wal")]
    fn wal_drop_superseded_prefixes(&mut self, published: SegmentId) {
        let deferred = self.wal_deferred_count();
        for back in 1..=(deferred + 1) {
            self.wal_spawn_delete_segment(SegmentId::new_from_cno(published.as_cno().saturating_sub(back as u64)));
        }
        self.wal_reset_deferred_count();
    }

    /// Flushes satisfied by the log since the last publish.
    #[cfg(feature = "wal")]
    fn wal_deferred_count(&self) -> usize;

    #[cfg(feature = "wal")]
    fn wal_reset_deferred_count(&mut self);

    // provided method
    fn bmap_get_raw(&self) -> BMapRawType {
        let mut b: BMapRawType = unsafe { std::mem::MaybeUninit::zeroed().assume_init() };
        b.copy_from_slice(self.bmap_as_slice());
        b
    }

    // recover inode from segment
    fn recover_partial_flush(&mut self, segid: u64, od_state: &Option<OnDiskState>) -> impl Future<Output = Result<()>> {async move {
        let mut raw_inode: InodeRaw = unsafe { std::mem::MaybeUninit::zeroed().assume_init() };
        let _ = self.staging().load_inode_from_segment(&mut raw_inode.as_mut_u8_slice(), SegmentId::new_from_cno(segid)).await?;
        let od_state = self.staging().flush_inode(raw_inode.as_u8_slice(), od_state, FlushInodeFlag::Update).await?;
        self.inode_mut().clear_attr_dirty();
        self.inode_mut().set_ondisk_state(od_state);
        let last_cno = self.inode().get_last_cno();
        self.inode_mut().set_last_ondisk_cno(last_cno);
        Ok(())
    }}

    /// try recover partial flush (inode ahead segment)
    fn try_recover_partial_flush(&mut self) -> impl Future<Output = Result<(InodeRaw, Option<OnDiskState>)>> {async {
        let mut raw_inode: InodeRaw = unsafe { std::mem::MaybeUninit::zeroed().assume_init() };
        let mut inode_state;
        loop {
            debug!("try_recover_partial_flush - loop entry");
            match self.staging().load_inode(&mut raw_inode.as_mut_u8_slice()).await {
                Ok(od_state) => { inode_state = od_state; },
                Err(e) => { return Err(e); },
            }
            debug!("try_recover_partial_flush - inode ondisk state: {:?}", inode_state);

            // test next segid
            let next_segid = SegmentId::new_from_cno(raw_inode.i_last_seq + 1);
            match self.staging().load_segment_timestamp(next_segid).await {
                Ok((server_time, segment_lm)) => {
                    if server_time - segment_lm > DEFAULT_PARTIAL_FLUSH_TIMEOUT as i64 {
                        self.recover_partial_flush(next_segid.as_cno(), &inode_state).await?;
                    } else {
                        // sleep for a while for next check
                        Self::sleep(Duration::from_secs(DEFAULT_PARTIAL_FLUSH_CHECK_INTERVAL_SECS)).await;
                    }
                    continue;
                },
                Err(e) => {
                    if e.kind() != ErrorKind::NotFound { return Err(e); }
                    // if we have inode but last segment not exists, let's break
                    break;
                },
            }
        }

        Ok((raw_inode, inode_state))
    }}

    /// refresh bmap by starting from reload inode
    /// and detect potential dead flush
    fn refresh_bmap(&mut self) -> impl Future<Output = Result<SegmentId>> {async {
        // save current in memory state
        let curr_segid = self.inode().get_last_seq();
        let (raw_inode, inode_state) = self.try_recover_partial_flush().await?;
        let od_last_seq = raw_inode.i_last_seq;

        if curr_segid.as_cno() == od_last_seq {
            debug!("REFRESH_BMAP - quit due: current segid {} == on disk segid {}", curr_segid, od_last_seq);
            return Ok(curr_segid);
        } else if curr_segid.as_cno() > od_last_seq {
            warn!("REFRESH_BMAP - current segid {} is ahead of on disk segid {}", curr_segid, od_last_seq);
            return Ok(curr_segid);
        }

        // get back block loader
        let meta_block_loader = self.bmap_get_block_loader();
        // get back node cache
        let node_cache = self.bmap_get_node_cache();

        // Borrowed out of the inode rather than copied to a local: `BMap::read`
        // wants an 8-byte-aligned buffer, and `BMapRawType` is `[u8; N]`, whose
        // own alignment is 1. `InodeRaw` is `align(8)` and `i_bmap` sits at an
        // 8-aligned offset within it, so this reference is aligned by
        // construction — a copy would be wherever the compiler put it.
        let mut bmap = BMap::<BlockIndex, V, BlockPtr, L, C>::read(&raw_inode.i_bmap, self.config().meta.meta_block_size, meta_block_loader, node_cache)?;

        let _permit = self.lock().await;

        // clear cached data
        let _ = self.clear_data_blocks_cache();

        let dirty_data_blocks = self.get_data_blocks_dirty();

        // no dirty blocks, just return
        if dirty_data_blocks.len() == 0 {
            return Ok(curr_segid);
        }

        // rebuild dirty bmap
        for (blk_idx, _) in dirty_data_blocks.data().iter() {
            // if blk index is already exists, overwrite it
            let _ = Self::bmap_insert_dummy_value(&mut bmap, blk_idx).await
                .expect("failed to insert dummy value to bmap for dirty blocks during rebuild bmap");
        }
        assert!(bmap.dirty() == true);

        info!("REFRESH_BMAP - update in-memory bmap with inode from ondisk");
        // refresh inner bmap and inode but don't touch inode attr
        self.bmap_update(bmap);
        (*self.inode_mut()).i_last_seq = raw_inode.i_last_seq;
        (*self.inode_mut()).i_last_cno = raw_inode.i_last_cno;
        (*self.inode_mut()).i_last_ondisk_cno = raw_inode.i_last_cno;
        (*self.inode_mut()).i_ondisk_state = inode_state;

        Ok(self.inode().get_last_seq())
    }}

    fn flush_process_pre_build_segment(&self) -> impl Future<Output = Result<(u64, DirtyDataBlocks<'_>)>> {async {
        let dirty_data_blocks = self.get_data_blocks_dirty();

        if dirty_data_blocks.len() == 0 && !self.bmap_dirty() {
            if self.inode().is_attr_dirty() {
                debug!("inode attr is dirty, flush inode ONLY");
                let b = self.bmap_get_raw();
                let raw_inode = self.inode().to_raw(b);
                let od_state = self.staging().flush_inode(raw_inode.as_u8_slice(), self.inode().get_ondisk_state(), FlushInodeFlag::Update).await?;
                self.inode_mut().clear_attr_dirty();
                self.inode_mut().set_ondisk_state(od_state);
                let last_cno = self.inode().get_last_cno();
                self.inode_mut().set_last_ondisk_cno(last_cno);
            }
            debug!("flush quit, NO dirty data blocks amd bmap is NOT dirty");
            let segid = self.inode().get_last_seq();
            return Ok((segid.as_cno(), DirtyDataBlocks { inner: None, owned: None }));
        }
        Ok((0, dirty_data_blocks))
    }}

    fn flush_process_build_segment(&self, dirty_data_blocks: DirtyDataBlocks<'_>)
            -> impl Future<Output = Result<(segment::Writer<T>, SegmentId, InodeRaw, Vec<BtreeNodeDirty<'_, BlockIndex, V, BlockPtr>>)>>
    {async move {
        // prepare for a segment
        let _start = Instant::now();

        // 1. collect all dirty meta data
        //
        // Note the ordering constraint this imposes on everything that
        // dirties a data block: this set is fixed *here*, before the
        // loop below assigns pointers to data blocks. A bmap node that
        // is not already dirty at this point is never written, so the
        // pointer `bmap_assign_data_node` stores into it exists only in
        // memory, and a cold reader keeps following the old one.
        //
        // So every path that puts a block into the dirty tier must also
        // insert into the bmap for that index in the same flush window,
        // which is what marks the containing node dirty. The write
        // paths do it for every block they touch; see `block_mut` and
        // `truncate_last_data_block` for two places where omitting it
        // caused silent loss, visible only once the map had spilled out
        // of the inode's inline root — below that the root travels with
        // the inode, which every flush writes.
        let dirty_meta_vec = self.bmap_lookup_dirty();
        debug!("start to create a new segemtnt: dirty meta nodes {}, dirty data blocks {}",
            dirty_meta_vec.len(), dirty_data_blocks.len());

        let segid = self.inode_mut().get_next_seq();
        let mut file_off = 0;
        let mut segwr = self.staging().new_segwr(segid, &self.config().meta);

        let dirty_data_blocks = self.get_data_blocks_dirty();
        let ndatadirty = dirty_data_blocks.len();
        file_off += segment::Writer::<T>::calc_ss_aligned_bytes(ndatadirty);

        let mut block_seq = 0;
        // assign real blk ptr to meta data
        for n in &dirty_meta_vec {
            let blk_ptr = self.blk_ptr_encode(segid, file_off, block_seq);
            let node_size = n.size();
            // use 0 as key, but it's useless
            debug!("assign block ptr for meta node: block ptr {}", self.blk_ptr_decode_display(&blk_ptr));
            self.bmap_assign_meta_node(blk_ptr, n.clone()).await?;
            segwr.inc_metablk();
            file_off += node_size;
            block_seq += 1;
        }

        // 2. collect all dirty data block

        // assign real blk ptr to data block
        for (blk_idx, n) in dirty_data_blocks.data().iter() {
            let blk_ptr = self.blk_ptr_encode(segid, file_off, block_seq);
            let block_size = n.size();
            debug!("assign block ptr for data node: block ptr {}, block index {}", self.blk_ptr_decode_display(&blk_ptr), blk_idx);
            self.bmap_assign_data_node(blk_idx, blk_ptr).await?;
            segwr.inc_datablk(blk_idx, &blk_ptr);
            file_off += block_size;
            block_seq += 1;
        }

        let _ = _start.elapsed();

        // prepare inode
        let b = self.bmap_get_raw();
        let mut raw_inode = self.inode().to_raw(b);
        raw_inode.i_last_cno = segid.as_cno();
        // TODO: calc segment checksum
        segwr.realize_ss(0, &raw_inode);

        // write out root node to staging file
        let _start = Instant::now();
        for n in &dirty_meta_vec {
            let _ = segwr.append(n.as_slice())?;
        }
        #[cfg(any(not(feature = "concurrent-segment-build"), not(feature = "wal")))]
        for (_, n) in dirty_data_blocks.data().iter() {
            let _ = segwr.append_data_block(n)?;
        }
        #[cfg(all(feature = "concurrent-segment-build", feature = "wal"))]
        {

        const TARGET_CHUNKS: usize = 50;
        let data_blocks = dirty_data_blocks.data()
                        .into_iter()
                        .map(|(_, block)| block)
                        .collect::<Vec<&DataBlock>>();
        // Split `data_blocks` into up to TARGET_CHUNKS chunks.
        //
        // Using ceil-div here is deliberate: floor-div of
        // `len / (TARGET_CHUNKS - 1)` yields 0 whenever
        // `len < TARGET_CHUNKS - 1`, and a `split_off(0)` loop
        // with chunk_size=0 never terminates. Ceil-div also
        // guarantees chunk_size >= 1 for any non-empty input,
        // so the number of chunks is at most TARGET_CHUNKS.
        let mut joins = Vec::new();
        if !data_blocks.is_empty() {
            let chunk_size = data_blocks.len().div_ceil(TARGET_CHUNKS);
            for chunk in data_blocks.chunks(chunk_size) {
                let j = segwr.spawn_append(chunk.to_vec());
                joins.push(j);
            }
        }

        // NOTE: the wait loop below is a CPU-bound busy-wait, not
        // an async wait. It keeps the caller's runtime pinned on
        // one core while the spawn_blocking workers make progress
        // on the blocking pool. That's tolerable on a multi-threaded
        // runtime (other workers remain free), and happens to work
        // on the reactor's current-thread runtime only because we
        // spawn onto the blocking pool (decoupled from the runtime's
        // worker).
        //
        // Known limitation: a panic inside the spawn_blocking
        // closure is silently swallowed. `is_finished()` returns
        // true for both "completed" and "panicked" handles, and we
        // drop the JoinHandle without calling `.await` or inspecting
        // its JoinError. For the current workload (memcpy into a
        // pre-sized buffer) a panic shouldn't happen, but this is
        // load-bearing only by accident.
        //
        // Replace with `tokio::task::JoinSet` when any of the
        // following becomes true:
        //   - panics in the copy closure need to surface as flush
        //     errors (today they're swallowed);
        //   - flush latency matters and the spinning core becomes a
        //     measurable cost;
        //   - we move the flush path to a single-threaded runtime
        //     with no blocking pool.
        //
        // Sketch of the replacement:
        //   let mut set = JoinSet::new();
        //   for chunk in data_blocks.chunks(chunk_size) {
        //       set.spawn_blocking(move || { /* copy */ });
        //   }
        //   while let Some(res) = set.join_next().await {
        //       res.map_err(|e| /* JoinError -> our Error */)?;
        //   }

        // wait all spawn append completed
        while let Some(res) = joins.pop() {
            let join = res?;
            if !join.is_finished() {
                joins.push(Ok(join));
            }
        }

        }
        let _ = _start.elapsed();
        Ok((segwr, segid, raw_inode, dirty_meta_vec))
    }}

    /// Seal the records written since the last barrier, naming the checkpoint
    /// they belong to.
    ///
    /// Ordered deliberately: after every record is confirmed written, because a
    /// barrier claims the group is whole; and before the segment is built,
    /// because building it advances `last_seq` and the records are under the
    /// old one. Together those two make "a barrier exists for this segid" mean
    /// "the group is complete and was never published".
    ///
    /// Nothing to seal is not an error. A flush with no records behind it —
    /// metadata only, or a replay, which takes the log out while it runs —
    /// writes a barrier over an empty manifest, which recovery reads as a
    /// complete group of nothing.
    #[cfg(feature = "wal")]
    fn wal_seal_barrier(&mut self) -> impl Future<Output = Result<()>> {async {
        let segid = self.inode().get_last_seq();
        let fut = match self.wal_mut() {
            Some(wal) => wal.write_barrier(segid),
            None => return Ok(()),
        };
        fut.await
    }}

    fn flush_process(&mut self) -> impl Future<Output = Result<SegmentId>> {async move {
        let fn_start = Instant::now();
        debug!("flush started");

        // Seal the log group before anything else: after every record is
        // confirmed written, and before the segment build advances `last_seq`
        // past the checkpoint those records are under. Sealing nothing is a
        // no-op, so the case where this flush turns out to have nothing to
        // publish costs nothing and writes no barrier.
        #[cfg(feature = "wal")]
        self.wal_seal_barrier().await?;
        let _start = Instant::now();
        let (segid, dirty_data_blocks) = self.flush_process_pre_build_segment().await?;
        self.flush_timing().pre_build_ns.fetch_add(
            _start.elapsed().as_nanos() as u64, Ordering::Relaxed);
        if segid > 0 {
            self.flush_timing().flush_count.fetch_add(1, Ordering::Relaxed);
            return Ok(SegmentId::new_from_cno(segid));
        }
        // Seal the log group before the segment exists, so a crash between the
        // two leaves a complete group to replay rather than an unpublished
        // segment with no way to tell whether its records were all there.
        let _start = Instant::now();
        let (segwr, segid, raw_inode, dirty_meta_vec) = self.flush_process_build_segment(dirty_data_blocks).await?;
        self.flush_timing().build_segment_ns.fetch_add(
            _start.elapsed().as_nanos() as u64, Ordering::Relaxed);

        let _start = Instant::now();
        segwr.done().await?;
        // update last cno in memory after segment write out
        self.inode_mut().set_last_cno(segid.as_cno());
        self.flush_timing().segment_done_ns.fetch_add(
            _start.elapsed().as_nanos() as u64, Ordering::Relaxed);

        // flush inode after writeout segment
        let _start = Instant::now();
        let od_state = self.staging().flush_inode(raw_inode.as_u8_slice(), self.inode().get_ondisk_state(), FlushInodeFlag::Update).await?;
        self.flush_timing().flush_inode_ns.fetch_add(
            _start.elapsed().as_nanos() as u64, Ordering::Relaxed);
        self.inode_mut().clear_attr_dirty();
        self.inode_mut().set_ondisk_state(od_state);
        let last_cno = self.inode().get_last_cno();
        self.inode_mut().set_last_ondisk_cno(last_cno);

        // start to cleanup
        let _start = Instant::now();

        // clear dirty for all dirty meta node
        for n in dirty_meta_vec {
            n.clear_dirty();
        }

        self.clear_data_blocks_dirty();

        // clear dirty for bmap
        self.bmap_clear_dirty();
        // reset last flush
        self.set_last_flush();

        self.flush_timing().cleanup_ns.fetch_add(
            _start.elapsed().as_nanos() as u64, Ordering::Relaxed);
        self.flush_timing().flush_count.fetch_add(1, Ordering::Relaxed);
        let _ = fn_start.elapsed();

        // WAL cleanup: the chunks feeding into this segment live
        // under (segid - 1) (flush called get_next_seq() when
        // allocating segid). Fire-and-forget; see the twin
        // wal_flush_done path for the same pattern on the reactor
        // WAL flush.
        #[cfg(feature = "wal")]
        {
            self.wal_drop_superseded_prefixes(segid);
        }

        Ok(SegmentId::new_from_cno(self.inode().get_last_ondisk_cno()))
    }}

    #[cfg(all(feature = "wal", feature = "reactor"))]
    fn wal_flush_process_reactor<'a: 'static>(&mut self, fh: ChannelGroup<FileContext<'a>>, lock: OwnedMutexGuard<()>)
            -> impl Future<Output = std::result::Result<SegmentId, (OwnedMutexGuard<()>, Error)>>
    {async move {
        let fn_start = Instant::now();
        debug!("flush started");

        // See `wal_seal_barrier`: after the records, before the segment.
        if let Err(e) = self.wal_seal_barrier().await {
            return Err((lock, e));
        }

        // A flush may be satisfied by the log alone. The data is durable — each
        // write made it so before returning — so what is deferred is the
        // checkpoint, not the data.
        //
        // The sequence still has to move, or the next group's records would land
        // under this checkpoint and the next barrier would overwrite this one's,
        // claiming two groups as one.
        //
        // The dirty-data thresholds stay on, and they are what bounds how much
        // can pile up here. Recovery replays each deferred group in turn, so a
        // mount after a crash pays what these flushes did not.
        if self.wal_may_defer_publish() {
            let segid = self.inode().get_last_seq();
            self.inode_mut().set_last_seq(segid.next());
            self.wal_count_deferred_barrier();
            self.set_last_flush();
            let last_cno = self.inode().get_last_cno();
            self.flush_unlock(lock);
            debug!("flush satisfied by the log, checkpoint deferred; last published {}", last_cno);
            return Ok(SegmentId::new_from_cno(last_cno));
        }

        let (segid, dirty_data_blocks) = match self.flush_process_pre_build_segment().await {
            Ok((segid, dirty_data_blocks)) => (segid, dirty_data_blocks),
            Err(e) => return Err((lock, e)),
        };
        if segid > 0 {
            // manually unlock
            self.set_last_flush();
            self.flush_unlock(lock);
            return Ok(SegmentId::new_from_cno(segid));
        }
        let (segwr, segid, raw_inode, dirty_meta_vec) = match self.flush_process_build_segment(dirty_data_blocks).await {
            Ok((segwr, segid, raw_inode, dirty_meta_vec)) => (segwr, segid, raw_inode, dirty_meta_vec),
            Err(e) => return Err((lock, e)),
        };

        let staging = self.staging().clone();
        let od_state = self.inode().get_ondisk_state().clone();
        // set bmap cache to unlimit, restore back until flush done
        let bmap_cache_limit = self.bmap_set_cache_unlimited();
        let (mem_segid, mem_segdata) = segwr.get_weak_data();
        self.wal_set_mem_segment(mem_segid, mem_segdata).await;
        tokio::task::spawn(async move {
            let _start = Instant::now();
            match segwr.done().await {
                Ok(_) => {},
                Err(e) => {
                    warn!("segment write failed in wal flush process: {:?}", e);
                    let ctx = FileContext::new_wal_flush_recovery(lock);
                    let _ = fh.send_cb(ctx);
                    return;
                },
            }
            match staging.flush_inode(raw_inode.as_u8_slice(), &od_state, FlushInodeFlag::Update).await {
                Ok(od_state) => {
                    let ctx = FileContext::new_wal_flush_done(lock, segid, od_state.unwrap().clone(), bmap_cache_limit);
                    let _ = fh.send_cb(ctx);
                },
                Err(e) => {
                    warn!("flush inode failed in wal flush process: {:?}", e);
                    let ctx = FileContext::new_wal_flush_recovery(lock);
                    let _ = fh.send_cb(ctx);
                },
            }
        });
        self.inode_mut().set_last_cno(segid.as_cno());
        self.inode_mut().clear_attr_dirty();

        // start to cleanup
        let _start = Instant::now();

        // clear dirty for all dirty meta node
        for n in dirty_meta_vec {
            n.clear_dirty();
        }

        // set data blocks cache unlimited
        self.set_data_blocks_cache_unlimited();

        self.clear_data_blocks_dirty();

        // clear dirty for bmap
        self.bmap_clear_dirty();
        // reset last flush
        // defer set last flash in wal_flush_done

        let _ = _start.elapsed();
        let _ = fn_start.elapsed();
        // return cno in inode memory instead cno on disk
        Ok(SegmentId::new_from_cno(self.inode().get_last_cno()))
    }}

    #[cfg(all(feature = "wal", feature = "blocking"))]
    fn wal_flush_process_blocking<'a>(&mut self) -> impl Future<Output = Result<SegmentId>> {async move {
        let fn_start = Instant::now();
        debug!("flush started");

        let (segid, dirty_data_blocks) = self.flush_process_pre_build_segment().await?;
        if segid > 0 {
            return Ok(segid);
        }
        let (segwr, segid, raw_inode, dirty_meta_vec) = self.flush_process_build_segment(dirty_data_blocks).await?;

        let staging = self.staging().clone();
        let od_state = self.inode().get_ondisk_state().clone();
        let join = tokio::task::spawn(async move {
            match segwr.done().await {
                Ok(_) => {},
                Err(e) => {
                    warn!("segment write failed in wal flush process: {:?}", e);
                    return Err(e);
                },
            }
            match staging.flush_inode(raw_inode.as_u8_slice(), &od_state, FlushInodeFlag::Update).await {
                Ok(od_state) => { return Ok(od_state); },
                Err(e) => {
                    warn!("flush inode failed in wal flush process: {:?}", e);
                    return Err(e);
                },
            }
        });

        let od_state = join.await??;

        self.inode_mut().set_last_cno(segid.as_cno());
        self.inode_mut().clear_attr_dirty();
        self.inode_mut().set_ondisk_state(od_state);
        let last_cno = self.inode().get_last_cno();
        self.inode_mut().set_last_ondisk_cno(last_cno);

        // start to cleanup
        let _start = Instant::now();

        // clear dirty for all dirty meta node
        for n in dirty_meta_vec {
            n.clear_dirty();
        }

        self.clear_data_blocks_dirty();

        // clear dirty for bmap
        self.bmap_clear_dirty();
        // reset last flush
        self.set_last_flush();

        let _ = _start.elapsed();
        let _ = fn_start.elapsed();
        Ok(SegmentId::new_from_cno(self.inode().get_last_ondisk_cno()))
    }}

    // flush out dirty data
    // original flush process
    fn _flush_process(&mut self) -> impl Future<Output = Result<SegmentId>> {async move {

        let fn_start = Instant::now();
        debug!("flush started");

        let dirty_data_blocks = self.get_data_blocks_dirty();

        if dirty_data_blocks.len() == 0 && !self.bmap_dirty() {
            if self.inode().is_attr_dirty() {
                debug!("inode attr is dirty, flush inode ONLY");
                let b = self.bmap_get_raw();
                let raw_inode = self.inode().to_raw(b);
                let od_state = self.staging().flush_inode(raw_inode.as_u8_slice(), self.inode().get_ondisk_state(), FlushInodeFlag::Update).await?;
                self.inode_mut().clear_attr_dirty();
                self.inode_mut().set_ondisk_state(od_state);
                let last_cno = self.inode().get_last_cno();
                self.inode_mut().set_last_ondisk_cno(last_cno);
            }
            debug!("flush quit, NO dirty data blocks amd bmap is NOT dirty");
            return Ok(self.inode().get_last_seq());
        }

        // prepare for a segment
        let _start = Instant::now();

        // 1. collect all dirty meta data
        //
        // Note the ordering constraint this imposes on everything that
        // dirties a data block: this set is fixed *here*, before the
        // loop below assigns pointers to data blocks. A bmap node that
        // is not already dirty at this point is never written, so the
        // pointer `bmap_assign_data_node` stores into it exists only in
        // memory, and a cold reader keeps following the old one.
        //
        // So every path that puts a block into the dirty tier must also
        // insert into the bmap for that index in the same flush window,
        // which is what marks the containing node dirty. The write
        // paths do it for every block they touch; see `block_mut` and
        // `truncate_last_data_block` for two places where omitting it
        // caused silent loss, visible only once the map had spilled out
        // of the inode's inline root — below that the root travels with
        // the inode, which every flush writes.
        let dirty_meta_vec = self.bmap_lookup_dirty();
        debug!("start to create a new segemtnt: dirty meta nodes {}, dirty data blocks {}",
            dirty_meta_vec.len(), dirty_data_blocks.len());

        let segid = self.inode_mut().get_next_seq();
        let mut file_off = 0;
        let mut segwr = self.staging().new_segwr(segid, &self.config().meta);

        let dirty_data_blocks = self.get_data_blocks_dirty();
        let ndatadirty = dirty_data_blocks.len();
        file_off += segment::Writer::<T>::calc_ss_aligned_bytes(ndatadirty);

        let mut block_seq = 0;
        // assign real blk ptr to meta data
        for n in &dirty_meta_vec {
            let blk_ptr = self.blk_ptr_encode(segid, file_off, block_seq);
            let node_size = n.size();
            // use 0 as key, but it's useless
            debug!("assign block ptr for meta node: block ptr {}", self.blk_ptr_decode_display(&blk_ptr));
            self.bmap_assign_meta_node(blk_ptr, n.clone()).await?;
            segwr.inc_metablk();
            file_off += node_size;
            block_seq += 1;
        }

        // 2. collect all dirty data block

        // assign real blk ptr to data block
        for (blk_idx, n) in dirty_data_blocks.data().iter() {
            let blk_ptr = self.blk_ptr_encode(segid, file_off, block_seq);
            let block_size = n.size();
            debug!("assign block ptr for data node: block ptr {}, block index {}", self.blk_ptr_decode_display(&blk_ptr), blk_idx);
            self.bmap_assign_data_node(blk_idx, blk_ptr).await?;
            segwr.inc_datablk(blk_idx, &blk_ptr);
            file_off += block_size;
            block_seq += 1;
        }

        let _ = _start.elapsed();

        // prepare inode
        let b = self.bmap_get_raw();
        let mut raw_inode = self.inode().to_raw(b);
        raw_inode.i_last_cno = segid.as_cno();
        // TODO: calc segment checksum
        segwr.realize_ss(0, &raw_inode);

        // write out root node to staging file
        let _start = Instant::now();
        for n in &dirty_meta_vec {
            let _ = segwr.append(n.as_slice())?;
        }
        for (_, n) in dirty_data_blocks.data().iter() {
            let _ = segwr.append_data_block(n)?;
        }
        let _ = _start.elapsed();

        let _start = Instant::now();
        segwr.done().await?;
        // update last cno in memory after segment write out
        self.inode_mut().set_last_cno(segid.as_cno());
        let _ = _start.elapsed();

        // flush inode after writeout segment
        let _start = Instant::now();
        let od_state = self.staging().flush_inode(raw_inode.as_u8_slice(), self.inode().get_ondisk_state(), FlushInodeFlag::Update).await?;
        let _ = _start.elapsed();
        self.inode_mut().clear_attr_dirty();
        self.inode_mut().set_ondisk_state(od_state);
        let last_cno = self.inode().get_last_cno();
        self.inode_mut().set_last_ondisk_cno(last_cno);

        // start to cleanup
        let _start = Instant::now();

        // clear dirty for all dirty meta node
        for n in dirty_meta_vec {
            n.clear_dirty();
        }

        self.clear_data_blocks_dirty();

        // clear dirty for bmap
        self.bmap_clear_dirty();
        // reset last flush
        self.set_last_flush();

        let _ = _start.elapsed();
        let _ = fn_start.elapsed();
        Ok(SegmentId::new_from_cno(self.inode().get_last_ondisk_cno()))
    }}

    fn flush(&mut self) -> impl Future<Output = Result<SegmentId>> {async {
        use crate::config::FlushConflictPolicy;
        // A file that cannot publish must stop trying. Without this every
        // operation that flushes first — truncate among them — would reattempt
        // the failed publish and report its error rather than the read-only
        // state, and a steady stream of them would keep the retry cycle
        // running for good.
        self.check_writable()?;
        // Asking to publish inside an interval that asked for no publishing is
        // a contradiction, and answering `Ok` to it would tell the caller its
        // data is at a checkpoint when it is not.
        #[cfg(feature = "wal")]
        if self.in_txn_trait() {
            return Err(Error::new(ErrorKind::ResourceBusy,
                "a transaction is open, so publishing was asked not to happen; commit it to publish"));
        }
        let policy = self.config().runtime.flush_conflict_policy;

        let mut retries = 0;
        let mut backoff = DEFAULT_FLUSH_BACKOFF_SECS;
        while retries < DEFAULT_FLUSH_RETRIES {
            let lock = self.flush_lock().await;
            match self.flush_process().await {
                Ok(segid) => {
                    self.flush_unlock(lock);
                    return Ok(segid);
                },
                Err(err) => {
                    let kind = err.kind();

                    // AlreadyExists signals an OCC conflict from the storage
                    // layer (S3 412/409). Behavior is policy-controlled.
                    if kind == ErrorKind::AlreadyExists {
                        match policy {
                            FlushConflictPolicy::FailFast => {
                                // Do not retry: surface the conflict to the
                                // caller so it can re-read state and decide.
                                self.flush_unlock(lock);
                                return Err(err);
                            },
                            FlushConflictPolicy::RetryLastWriterWins => {
                                // Fall through to the retry path below.
                                warn!("{err}");
                            },
                        }
                    } else if kind != ErrorKind::ResourceBusy {
                        self.flush_unlock(lock);
                        return Err(err);
                    } else {
                        warn!("{err}");
                    }
                },
            }
            self.flush_unlock(lock);
            // back off sleep
            Self::sleep(Duration::from_secs(backoff)).await;
            let _ = self.refresh_bmap().await?;
            retries += 1;
            backoff += DEFAULT_FLUSH_BACKOFF_SECS;
        }
        let err_str = format!("FLUSH - reached max retries {}", retries);
        return Err(Error::new(ErrorKind::ResourceBusy, err_str));
    }}
}
