use std::fmt;
use std::collections::{HashMap, BTreeMap, BTreeSet};
use std::sync::Arc;
#[cfg(feature = "wal")]
use std::sync::Weak;
#[cfg(feature = "wal")]
use std::pin::Pin;
use std::time::{Instant, Duration};
use std::io::{Error, ErrorKind, Result};
use log::{debug, warn};
#[cfg(all(feature = "wal", feature = "reactor"))]
use crate::file::handler::ChannelGroup;
use btree_ondisk::{bmap::BMap, BlockLoader, NodeCache};
use btree_ondisk::btree::BtreeNodeDirty;
use btree_ondisk::DEFAULT_CACHE_UNLIMITED;
#[cfg(feature = "wal")]
use tokio::sync::RwLock;
use tokio::sync::{
    Semaphore, OwnedSemaphorePermit,
    Mutex, OwnedMutexGuard,
};
use crate::{BlockIndex, BlockPtr, BlockIndexIter, SegmentId, SegmentOffset, BMapUserData};
use crate::meta_format::BlockPtrFormat;
use crate::buffer::{DataBlock, AlignedDataBlockWrapper, BatchDataBlockWrapper};
use crate::staging::{StagingIntercept, Staging, config::StagingConfig};
use crate::segment::SegmentReadWrite;
use crate::ondisk::{InodeRaw, BMapRawType};
use crate::inode::{Inode, FlushInodeFlag};
use crate::config::{HyperFileConfig, HyperFileMetaConfig};
#[cfg(all(feature = "wal", feature = "reactor"))]
use crate::file::handler::FileContext;
#[cfg(feature = "wal")]
use crate::wal::{WalReadWrite, WalChunkDesc};
#[cfg(all(feature = "wal", feature = "reactor"))]
use crate::inode::OnDiskState;
use crate::data_cache::Cache;
use super::flags::HyperFileFlags;
use super::mode::HyperFileMode;
use super::{HyperTrait, DirtyDataBlocks, FlushTiming};
#[cfg(feature = "range-lock")]
use super::lock::RangeLock;
use super::state::State;

/// One step of a coalesced read plan. Each variant carries a
/// `dst_len` — the number of bytes it consumes from the user's
/// destination buffer in iteration order. The executor walks the
/// ops, repeatedly `split_at_mut(dst_len)` on the user buffer,
/// and dispatches the slice to the right source.
///
/// `Range` is the coalesced variant: it covers a contiguous
/// byte range of one segment, possibly spanning multiple data
/// blocks. The Level-A planner merges adjacent
/// `is_on_staging` blocks whose decoded `(segid, staging_off)`
/// are contiguous, capped by `runtime.read_get_max_bytes`.
#[derive(Debug)]
pub(crate) enum ReadOp {
    /// Block is in the in-memory data cache (dirty or clean).
    /// Copy bytes out of the cached block.
    Cache {
        blk_idx: BlockIndex,
        src_off_in_block: usize,
        dst_len: usize,
    },
    /// Block has no backing storage (zero-block ptr or sparse
    /// hole). Fill destination with zero.
    Zero { dst_len: usize },
    /// Block lives in an in-flight (not-yet-flushed-to-S3) WAL
    /// segment whose contents are still pinned in memory. Copy
    /// from the in-memory segment buffer.
    #[cfg(feature = "wal")]
    Inmem {
        segid: SegmentId,
        s3_off: usize,
        dst_len: usize,
    },
    /// Coalesced ranged GET against the staging segment. May
    /// cover multiple consecutive blocks.
    Range {
        segid: SegmentId,
        s3_off: usize,
        dst_len: usize,
    },
}

impl ReadOp {
    pub(crate) fn dst_len(&self) -> usize {
        match self {
            Self::Cache { dst_len, .. } => *dst_len,
            Self::Zero { dst_len } => *dst_len,
            #[cfg(feature = "wal")]
            Self::Inmem { dst_len, .. } => *dst_len,
            Self::Range { dst_len, .. } => *dst_len,
        }
    }
}

pub struct HyperFile<'a, T: Send + Clone, L: BlockLoader<BlockPtr>, C: NodeCache<BlockPtr>> {
    pub(crate) staging: T,
    pub(crate) bmap: BMap<'a, BlockIndex, BlockPtr, BlockPtr, L, C>,
    pub(crate) bmap_ud: BMapUserData,
    pub(crate) cache: Box<dyn Cache + Send>,
    pub(crate) inode: Inode,
    pub(crate) config: HyperFileConfig,
    pub(crate) max_dirty_blocks: usize,
    pub(crate) flags: HyperFileFlags,
    pub(crate) state: State,
    pub(crate) sema: Arc<Semaphore>,
    pub(crate) flush_lock: Arc<Mutex<()>>,
    pub(crate) flush_timing: FlushTiming,
    #[cfg(feature = "range-lock")]
    pub(crate) range_lock: RangeLock,
    #[cfg(feature = "reactor")]
    pub(crate) rt: Option<tokio::runtime::Runtime>,
    #[cfg(feature = "wal")]
    pub(crate) wal: Option<Box<dyn WalReadWrite + Send>>,
    #[cfg(feature = "wal")]
    pub(crate) flushing_segments: Arc<RwLock<HashMap<SegmentId, Weak<Pin<Box<Vec<u8>>>>>>>,
}

impl<T: Send + Clone, L: BlockLoader<BlockPtr>, C: NodeCache<BlockPtr>> fmt::Display for HyperFile<'_, T, L, C> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(f, "==== dump HyperFile ====")?;
        writeln!(f, "  {:?}", self.config)?;
        writeln!(f, "  max dirty blocks: {}", self.max_dirty_blocks)?;
        writeln!(f, "  {}", self.cache)?;
        writeln!(f, "  {}", self.inode)
    }
}

impl<T: Send + Clone, L: BlockLoader<BlockPtr>, C: NodeCache<BlockPtr>> Drop for HyperFile<'_, T, L, C> {
    fn drop(&mut self) {
        #[cfg(feature = "reactor")]
        if let Some(rt) = self.rt.take() {
            rt.shutdown_background();
        }
    }
}

impl<'a, T, L, C> HyperFile<'a, T, L, C>
    where
        'a: 'static,
        T: Staging<L> + SegmentReadWrite + Send + Clone + 'static,
        L: BlockLoader<BlockPtr> + Clone + 'static,
        C: NodeCache<BlockPtr> + Clone,
{
    pub async fn new(staging: T, meta_block_loader: L, node_cache: C, config: HyperFileConfig, flags: HyperFileFlags, mode: HyperFileMode) -> Result<Self>
    {
        let meta_config = config.meta.clone();

        let bmap = BMap::<BlockIndex, BlockPtr, BlockPtr, L, C>::new(meta_config.root_size, meta_config.meta_block_size, meta_block_loader, node_cache)?;
        let bmap_ud = BMapUserData::new(BlockPtrFormat::MicroGroup);
        bmap.set_userdata(bmap_ud.as_u32());
        bmap.set_cache_limit(config.runtime.node_cache_blocks);

        let inode = Inode::default_file()
            .with_mode(&mode)
            .with_meta_config(&meta_config);
        let max_dirty_blocks = Self::calc_max_dirty_blocks(meta_config.data_block_size,
            config.runtime.data_cache_dirty_max_bytes_threshold,
            config.runtime.data_cache_dirty_max_blocks_threshold);

        let permits = if flags.is_rdonly() {
            Semaphore::MAX_PERMITS
        } else {
            #[cfg(feature = "range-lock")]
            { Semaphore::MAX_PERMITS }
            #[cfg(not(feature = "range-lock"))]
            { 1 }
        };

        let data_cache_blocks = if flags.is_direct() {
            #[cfg(not(feature = "wal"))]
            { 0 }
            // reset to data_cache_blocks if we are in wal mode
            #[cfg(feature = "wal")]
            { config.runtime.data_cache_blocks }
        } else {
            config.runtime.data_cache_blocks
        };

        #[cfg(feature = "wal")]
        let wal = config.wal.to_wal(config.meta.data_block_size, inode.get_last_seq())?;

        #[cfg(feature = "wal")]
        if let Some(ref wal) = wal {
            let v = wal.list_segments().await?;
            if v.len() > 0 {
                warn!("wal {} is not empty, please clear before create new file", config.wal.root_uri);
                return Err(Error::new(ErrorKind::ResourceBusy, "wal directory is not empty"));
            }
        }

        #[cfg(feature = "range-lock")]
        let range_lock = RangeLock::new(config.meta.data_block_size as u64);

        let mut file = Self {
            staging: staging,
            bmap: bmap,
            bmap_ud: bmap_ud,
            cache: crate::data_cache::cache_from_config(
                &config.data_cache,
                0,
                data_cache_blocks,
                config.meta.data_block_size,
            )?,
            inode: inode,
            config: config,
            max_dirty_blocks: max_dirty_blocks,
            flags: flags,
            state: State::default(),
            sema: Arc::new(Semaphore::new(permits)),
            flush_lock: Arc::new(Mutex::new(())),
            flush_timing: FlushTiming::default(),
            #[cfg(feature = "reactor")]
            rt: Some(tokio::runtime::Runtime::new().unwrap()),
            #[cfg(feature = "wal")]
            wal: wal,
            #[cfg(feature = "wal")]
            flushing_segments: Arc::new(RwLock::new(HashMap::new())),
            #[cfg(feature = "range-lock")]
            range_lock: range_lock,
        };
        // flush inode for hyper file new created
        let _ = file.flush_inode(FlushInodeFlag::Create).await?;
        Ok(file)
    }

    /// open a hyper file
    /// open by loading inode from staging,
    /// if inode is not found in staging, create hyper file from scratch
    pub async fn open(staging: T, meta_block_loader: L, node_cache: C, config: HyperFileConfig, flags: HyperFileFlags) -> Result<Self>
    {
        Self::do_open(staging, meta_block_loader, node_cache, config, flags, 0).await
    }

    /// open a hyper file with cno for read-only
    pub async fn open_cno(staging: T, meta_block_loader: L, node_cache: C, config: HyperFileConfig, flags: HyperFileFlags, cno: u64) -> Result<Self>
    {
        if !flags.is_rdonly() {
            return Err(Error::new(ErrorKind::ReadOnlyFilesystem, "write access is not allowed for open specific cno"));
        }
        Self::do_open(staging, meta_block_loader, node_cache, config, flags, cno).await
    }

    async fn do_open(staging: T, meta_block_loader: L, node_cache: C, mut config: HyperFileConfig, flags: HyperFileFlags, cno: u64) -> Result<Self>
    {
        let mut raw_inode: InodeRaw = unsafe { std::mem::MaybeUninit::zeroed().assume_init() };
        let inode_state;
        let res_inode = if cno == 0 {
            staging.load_inode(&mut raw_inode.as_mut_u8_slice()).await
        } else {
            staging.load_inode_from_segment(&mut raw_inode.as_mut_u8_slice(), cno as SegmentId).await
        };
        match res_inode {
            Ok(od_state) => {
                /* if we load inode without error, we use inode as truth of metadata */
                inode_state = od_state;
            },
            Err(e) => {
                return Err(e);
            },
        }
        // get back meta config from inode raw
        let meta_config = HyperFileMetaConfig::from_u32(raw_inode.i_meta_config);
        let b = raw_inode.i_bmap;
        let bmap = BMap::<BlockIndex, BlockPtr, BlockPtr, L, C>::read(&b, meta_config.meta_block_size, meta_block_loader, node_cache)?;
        let bmap_ud = BMapUserData::from_u32(bmap.get_userdata());
        bmap.set_cache_limit(config.runtime.node_cache_blocks);

        // if inode exists, we trust it

        let max_dirty_blocks = Self::calc_max_dirty_blocks(meta_config.data_block_size,
            config.runtime.data_cache_dirty_max_bytes_threshold,
            config.runtime.data_cache_dirty_max_blocks_threshold);

        let permits = if flags.is_rdonly() {
            Semaphore::MAX_PERMITS
        } else {
            #[cfg(feature = "range-lock")]
            { Semaphore::MAX_PERMITS }
            #[cfg(not(feature = "range-lock"))]
            { 1 }
        };

        let data_cache_blocks = if flags.is_direct() {
            #[cfg(not(feature = "wal"))]
            { 0 }
            // reset to data_cache_blocks if we are in wal mode
            #[cfg(feature = "wal")]
            { config.runtime.data_cache_blocks }
        } else {
            config.runtime.data_cache_blocks
        };

        // overwrite the default meta config with the one we get from inode
        config.meta = meta_config;

        let inode = Inode::from_raw(&raw_inode, inode_state);
        #[cfg(feature = "wal")]
        let wal = config.wal.to_wal(config.meta.data_block_size, inode.get_last_seq())?;

        #[cfg(feature = "range-lock")]
        let range_lock = RangeLock::new(config.meta.data_block_size as u64);

        let mut file = Self {
            staging: staging,
            bmap: bmap,
            bmap_ud: bmap_ud,
            cache: crate::data_cache::cache_from_config(
                &config.data_cache,
                inode.size(),
                data_cache_blocks,
                config.meta.data_block_size,
            )?,
            inode: inode,
            config: config,
            max_dirty_blocks: max_dirty_blocks,
            flags: flags,
            state: State::default(),
            sema: Arc::new(Semaphore::new(permits)),
            flush_lock: Arc::new(Mutex::new(())),
            flush_timing: FlushTiming::default(),
            #[cfg(feature = "reactor")]
            rt: Some(tokio::runtime::Runtime::new().unwrap()),
            #[cfg(feature = "wal")]
            wal: wal,
            #[cfg(feature = "wal")]
            flushing_segments: Arc::new(RwLock::new(HashMap::new())),
            #[cfg(feature = "range-lock")]
            range_lock: range_lock,
        };
        // refresh bmap if need to do recovery
        let _ = file.refresh_bmap().await?;

        #[cfg(feature = "wal")]
        if let Some(ref wal) = file.wal {
            let v = wal.list_segments().await?;
            if let Some(wal_max_segid) = v.iter().max() {
                let last_seq = file.inode().get_last_seq();
                if *wal_max_segid >= last_seq {
                    warn!("inconsistent wal data - max segid on wal: {}, seq in inode: {}", wal_max_segid, last_seq);
                    let lock = file.flush_lock().await;
                    let _ = file.wal_flush_recovery(lock).await;
                }
            }
        }
        Ok(file)
    }

    pub async fn release(&mut self) -> Result<SegmentId> {
        #[cfg(feature = "reactor")]
        if self.state.is_flushing() {
            return Err(Error::new(ErrorKind::ResourceBusy, "flush is in-progress"));
        };
        #[cfg(feature = "reactor")]
        if let Some(rt) = self.rt.take() {
            rt.shutdown_background();
        }
        let segid = self.flush().await?;
        self.cache.shutdown();
        self.bmap.get_node_cache().shutdown();
        Ok(segid)
    }

    pub fn stat(&self) -> libc::stat {
        // TODO: set dev and rdev here
        let dev = 0;
        let rdev = 0;
        self.inode.to_stat(dev, rdev)
    }

    // fast stat by read inode without open file
    pub async fn stat_fast(staging: T) -> Result<libc::stat> {
        let mut raw_inode: InodeRaw = unsafe { std::mem::MaybeUninit::zeroed().assume_init() };
        staging.load_inode(&mut raw_inode.as_mut_u8_slice()).await?;
        let inode = Inode::from_raw(&raw_inode, None);
        Ok(inode.to_stat(0, 0))
    }

    // fast update stat by load inode and flush inode
    pub async fn update_stat_fast(staging: T, stat: &libc::stat) -> Result<libc::stat> {
        let mut raw_inode: InodeRaw = unsafe { std::mem::MaybeUninit::zeroed().assume_init() };
        let od_state = staging.load_inode(&mut raw_inode.as_mut_u8_slice()).await?;
        let mut inode = Inode::from_raw(&raw_inode, od_state);
        inode.update_stat(stat);
        let raw = inode.to_raw(raw_inode.i_bmap);
        let od_state = inode.get_ondisk_state();
        let _ = staging.flush_inode(raw.as_u8_slice(), od_state, FlushInodeFlag::Update).await?;
        Ok(inode.to_stat(stat.st_dev, stat.st_rdev))
    }

    pub async fn update_stat(&mut self, stat: &libc::stat) -> Result<libc::stat> {
        let stat = self.inode.update_stat(stat);
        let _ = self.flush().await?;
        Ok(stat)
    }

    /// Opt this file in/out of cross-mount open-but-unlinked
    /// ([`InodeRaw::FLAG_KEEP_OPEN`]) and persist the change.
    pub async fn set_keep_open(&mut self, on: bool) -> Result<()> {
        self.inode.set_keep_open(on);
        let _ = self.flush().await?;
        Ok(())
    }

    /// True if this file opts in to cross-mount open-but-unlinked.
    pub fn is_keep_open(&self) -> bool { self.inode.is_keep_open() }

    /// Sorted set of dirty (cache-only, not-yet-flushed) data block
    /// indices that are `>= from`. These are unflushed writes that
    /// live only in the data cache and are not yet in the bmap, but
    /// the read path serves them, so for SEEK_DATA / SEEK_HOLE they
    /// count as data. The dirty set is bounded by the flush
    /// threshold, so collecting it is cheap.
    fn dirty_blocks_from(&self, from: BlockIndex) -> BTreeSet<BlockIndex> {
        self.cache.get_dirty().data().into_keys().filter(|k| *k >= from).collect()
    }

    /// `SEEK_DATA`: the smallest offset >= `off` that holds data, or
    /// `None` (the caller maps to `ENXIO`) if there is no data
    /// between `off` and EOF.
    ///
    /// Data lives in two places: unflushed writes in the dirty cache
    /// and flushed blocks in the bmap. We take the minimum of the
    /// first data block from each source. The bmap scan uses
    /// `seek_key` to skip runs of absent (hole) keys in O(log n) per
    /// jump, so a fully-sparse flushed file is cheap; only runs of
    /// zero-block keys are stepped through one block at a time.
    pub async fn seek_data(&self, off: usize) -> Result<Option<usize>> {
        let size = self.inode.size();
        if off >= size {
            return Ok(None);
        }
        let bsize = self.config.meta.data_block_size;
        let last = ((size - 1) / bsize) as BlockIndex;
        let start = (off / bsize) as BlockIndex;

        // Source 1: unflushed writes (always data).
        let dirty_next = self.dirty_blocks_from(start).into_iter().next();

        // Source 2: flushed, non-zero blocks in the bmap.
        let mut bmap_next = None;
        let mut blk = start;
        loop {
            match self.bmap.seek_key(&blk).await {
                Ok(k) => {
                    if k > last {
                        break; // no present key within the file
                    }
                    if !BlockPtrFormat::is_zero_block(&self.bmap.lookup(&k).await?) {
                        bmap_next = Some(k);
                        break;
                    }
                    if k == last {
                        break; // trailing zero block: no data to EOF
                    }
                    blk = k + 1; // skip this zero block
                }
                Err(e) if e.kind() == ErrorKind::NotFound => break,
                Err(e) => return Err(e),
            }
        }

        let cand = match (dirty_next, bmap_next) {
            (Some(a), Some(b)) => Some(a.min(b)),
            (Some(a), None) => Some(a),
            (None, Some(b)) => Some(b),
            (None, None) => None,
        };
        // Within the starting block, data begins at `off` itself.
        Ok(cand.map(|b| ((b as usize) * bsize).max(off)))
    }

    /// `SEEK_HOLE`: the smallest offset >= `off` that is in a hole.
    /// EOF is an implicit hole, so a file with no internal hole
    /// returns its size. `off == size` returns `size`; `off > size`
    /// returns `None` (the caller maps to `ENXIO`).
    ///
    /// Complexity note: runs of holes are skipped in O(log n) via
    /// `seek_key`, but a run of data blocks must be checked one block
    /// at a time (zero blocks are present bmap keys and are
    /// indistinguishable from data without a per-key lookup), so
    /// SEEK_HOLE over a large fully-dense region is O(n).
    pub async fn seek_hole(&self, off: usize) -> Result<Option<usize>> {
        let size = self.inode.size();
        if off > size {
            return Ok(None);
        }
        if off == size {
            return Ok(Some(size)); // EOF is an implicit hole
        }
        let bsize = self.config.meta.data_block_size;
        let last = ((size - 1) / bsize) as BlockIndex;
        let start = (off / bsize) as BlockIndex;

        let dirty = self.dirty_blocks_from(start);

        let mut blk = start;
        loop {
            if blk > last {
                return Ok(Some(size)); // data through EOF: hole is at EOF
            }
            if dirty.contains(&blk) {
                blk += 1; // unflushed write: data
                continue;
            }
            match self.bmap.seek_key(&blk).await {
                // No present key >= blk, and blk is not dirty:
                // blk and everything after it is a hole.
                Err(e) if e.kind() == ErrorKind::NotFound => {
                    return Ok(Some(((blk as usize) * bsize).max(off)));
                }
                Err(e) => return Err(e),
                // blk is absent in the bmap (next present key is
                // beyond it) and not dirty: blk is a hole.
                Ok(k) if k > blk => {
                    return Ok(Some(((blk as usize) * bsize).max(off)));
                }
                // blk is present; a zero block is a hole.
                Ok(_) => {
                    if BlockPtrFormat::is_zero_block(&self.bmap.lookup(&blk).await?) {
                        return Ok(Some(((blk as usize) * bsize).max(off)));
                    }
                    blk += 1; // data, keep scanning
                }
            }
        }
    }

    pub async fn read(&mut self, off: usize, mut buf: &mut [u8]) -> Result<usize> {
        let _permit = self.sema.clone().acquire_owned().await.unwrap();
        let fn_start = Instant::now();
        debug!("READ - off: {}, buf len: {}", off, buf.len());
        if off >= self.inode.size() {
            return Ok(0);
        }
        // if requested buffer exceed file size, cut off tailing buffer
        if off + buf.len() > self.inode.size() {
            let exceeded_len = off + buf.len() - self.inode.size();
            let mid = buf.len() - exceeded_len;
            debug!("READ - buf len shrink to: {}, due to file size {}", mid, self.inode.size());
            (buf, _) = buf.split_at_mut(mid);
        }
        if buf.is_empty() {
            let _ = fn_start;
            if !self.flags.is_noatime() {
                self.inode.update_atime();
            }
            return Ok(0);
        }

        let buf_len = buf.len();

        // Stage 1: walk blocks and build a coalesced plan.
        let plan = self.plan_read(off, buf_len).await?;
        debug!("READ - planned {} ops for {} bytes", plan.len(), buf_len);

        // Stage 2: execute. Walk ops, splitting `buf` as we go.
        let mut bytes_read = 0;
        let mut remaining = buf;
        for op in plan {
            let dst_len = op.dst_len();
            let (this, next) = remaining.split_at_mut(dst_len);
            match op {
                ReadOp::Cache { blk_idx, src_off_in_block, dst_len: _ } => {
                    let block = self.cache.get(&blk_idx)
                        .expect("planner classified as cache hit but block is gone");
                    block.copy_out(src_off_in_block, this);
                    block.unlock();
                }
                ReadOp::Zero { dst_len: _ } => {
                    this.fill(0);
                }
                #[cfg(feature = "wal")]
                ReadOp::Inmem { segid, s3_off, dst_len: _ } => {
                    let lock = self.flushing_segments.read().await;
                    let weak = lock.get(&segid)
                        .unwrap_or_else(|| panic!("inflight segid {segid} not registered"));
                    let data = weak.upgrade()
                        .unwrap_or_else(|| panic!("inflight data for segid {segid} dropped"));
                    let end = s3_off + this.len();
                    this.copy_from_slice(&data[s3_off..end]);
                }
                ReadOp::Range { segid, s3_off, dst_len: _ } => {
                    self.staging.load_range(segid, s3_off, this).await?;
                }
            }
            bytes_read += dst_len;
            remaining = next;
        }

        let _ = fn_start;

        if !self.flags.is_noatime() {
            self.inode.update_atime();
        }
        Ok(bytes_read)
    }

    /// Walk the read range and produce a coalesced op list. See
    /// `ReadOp` for the coalescing rules; in short, contiguous
    /// blocks that all map to one segment with consecutive
    /// staging offsets get merged into one `ReadOp::Range`,
    /// capped at `runtime.read_get_max_bytes`. Cache hits, zero
    /// blocks, and in-flight WAL blocks each break the run and
    /// produce their own per-block op.
    pub(crate) async fn plan_read(&mut self, off: usize, buf_len: usize) -> Result<Vec<ReadOp>> {
        let data_block_size = self.config.meta.data_block_size;
        let max_get = self.config.runtime.read_get_max_bytes;

        let mut ops: Vec<ReadOp> = Vec::new();
        // (segid, s3_off_start, accumulated_len)
        let mut current_range: Option<(SegmentId, usize, usize)> = None;
        let flush_range = |ops: &mut Vec<ReadOp>,
                           current_range: &mut Option<(SegmentId, usize, usize)>| {
            if let Some((seg, off, len)) = current_range.take() {
                ops.push(ReadOp::Range { segid: seg, s3_off: off, dst_len: len });
            }
        };

        let mut consumed = 0usize;
        let mut blk_idx = (off / data_block_size) as BlockIndex;
        let mut block_off = off % data_block_size;

        while consumed < buf_len {
            let block_remaining = data_block_size - block_off;
            let dst_len = block_remaining.min(buf_len - consumed);

            // Cache check first — `cache.get` does NOT promote
            // clean→dirty (only `contains` does), so this is a
            // safe peek with a side-effect of bumping LRU on
            // clean hits, which mirrors the pre-coalescing read.
            let cache_hit = self.cache.get(&blk_idx).is_some();
            if cache_hit {
                flush_range(&mut ops, &mut current_range);
                ops.push(ReadOp::Cache {
                    blk_idx,
                    src_off_in_block: block_off,
                    dst_len,
                });
                consumed += dst_len;
                blk_idx += 1;
                block_off = 0;
                continue;
            }

            // bmap lookup; NotFound → treat as zero block.
            let blk_ptr = match self.bmap.lookup(&blk_idx).await {
                Ok(p) => p,
                Err(e) if e.kind() == ErrorKind::NotFound => {
                    BlockPtrFormat::new_zero_block()
                }
                Err(e) => {
                    warn!("plan_read - lookup bmap for block index {blk_idx} error: {}", e);
                    return Err(e);
                }
            };

            if BlockPtrFormat::is_zero_block(&blk_ptr) {
                flush_range(&mut ops, &mut current_range);
                ops.push(ReadOp::Zero { dst_len });
            } else if BlockPtrFormat::is_on_staging(&blk_ptr) {
                let (segid, staging_off) = self.blk_ptr_decode(&blk_ptr);

                // WAL feature: a staging-pointer for a segid
                // greater than the on-disk last_cno means the
                // segment is in flight (memory-pinned, not yet
                // on S3). Read from memory, no S3 GET, no
                // coalescing.
                #[cfg(feature = "wal")]
                let is_inflight = self.wal.is_some()
                    && self.inode().get_last_cno() > self.inode().get_last_ondisk_cno()
                    && segid > self.inode().get_last_ondisk_cno();
                #[cfg(not(feature = "wal"))]
                let is_inflight = false;

                if is_inflight {
                    flush_range(&mut ops, &mut current_range);
                    #[cfg(feature = "wal")]
                    ops.push(ReadOp::Inmem {
                        segid,
                        s3_off: staging_off + block_off,
                        dst_len,
                    });
                    #[cfg(not(feature = "wal"))]
                    {
                        let _ = staging_off;
                        unreachable!()
                    }
                } else {
                    let s3_off = staging_off + block_off;
                    let extended = match &current_range {
                        Some((cur_seg, cur_off, cur_len)) => {
                            *cur_seg == segid
                                && cur_off + cur_len == s3_off
                                && cur_len + dst_len <= max_get
                        }
                        None => false,
                    };
                    if extended {
                        let (seg, off, len) = current_range.take().unwrap();
                        current_range = Some((seg, off, len + dst_len));
                    } else {
                        flush_range(&mut ops, &mut current_range);
                        current_range = Some((segid, s3_off, dst_len));
                    }
                }
            } else if BlockPtrFormat::is_dummy_value(&blk_ptr) {
                panic!(
                    "plan_read - dummy block ptr at blk_idx {} (write path leaked into read?)",
                    blk_idx,
                );
            } else {
                panic!(
                    "plan_read - unknown block ptr {} at blk_idx {}",
                    self.blk_ptr_decode_display(&blk_ptr),
                    blk_idx,
                );
            }

            consumed += dst_len;
            blk_idx += 1;
            block_off = 0;
        }

        flush_range(&mut ops, &mut current_range);
        Ok(ops)
    }

    /// The error a write-side operation must return when the handle
    /// was not opened for writing.
    ///
    /// POSIX `write()` lists `[EBADF] The fildes argument is not a
    /// valid file descriptor open for writing` as a mandatory
    /// ("shall fail") error. `ftruncate()` permits `[EBADF] or
    /// [EINVAL]` for the same condition; we use `EBADF` there too so
    /// every write-side operation reports one errno.
    ///
    /// Built with `from_raw_os_error` because `std::io::ErrorKind`
    /// has no `EBADF` variant: `PermissionDenied` would surface as
    /// `EACCES` (which POSIX reserves for permission-bit failures at
    /// `open` time) and `InvalidInput` would surface as `EINVAL`
    /// (conformant for `ftruncate` but not for `write`). Callers get
    /// the exact errno via `Error::raw_os_error()`; note that
    /// `Error::kind()` is `Uncategorized` for EBADF and so cannot be
    /// matched on. This is also why the error carries no custom
    /// message: an `io::Error` can have a raw errno or a custom
    /// message, not both.
    #[inline]
    pub(crate) fn ebadf_not_writable() -> Error {
        Error::from_raw_os_error(libc::EBADF)
    }

    pub async fn write(&mut self, off: usize, buf: &[u8]) -> Result<usize> {
        if !self.flags.is_writable() {
            return Err(Self::ebadf_not_writable());
        }
        self.write_inner(off, buf).await
    }

    /// `write` without the write-access check.
    ///
    /// Used by WAL crash recovery, which replays previously
    /// acknowledged writes while opening the file and must therefore
    /// run regardless of the access mode the caller opened with —
    /// otherwise a read-only open of a file that crashed mid-flush
    /// could not be served correctly.
    async fn write_inner(&mut self, off: usize, buf: &[u8]) -> Result<usize> {
        let permit = self.sema.clone().acquire_owned().await.unwrap();
        let fn_start = Instant::now();
        let len = buf.len();
        // O_APPEND: write at end-of-file, ignoring the caller-
        // supplied offset. Direct-API writes take &mut self, so
        // i_size cannot change between this read and the size update
        // at the end of the function — the borrow checker provides
        // the atomic-append serialization that POSIX requires.
        let off = if self.flags.is_append() {
            self.inode.size()
        } else {
            off
        };
        debug!("WRITE - off: {}, buf len: {}", off, len);

        let v = self.write_prepare(off, len);
        let fetched = self.write_retrieve(v).await?;
        for block in fetched.into_iter() {
            let blk_idx = block.index();
            let None = self.cache.insert(blk_idx, block) else {
                panic!("BlockIndex {} already on data_blocks_dirty list", blk_idx);
            };
        }

        #[cfg(feature = "wal")]
        if let Some(wal) = &mut self.wal {
            let _ = wal.write(self.inode.get_last_seq(), off, buf).await?;
        }

        let mut bytes_write = 0;

        let data_block_size = self.config.meta.data_block_size;
        let blk_iter = BlockIndexIter::new(off, len, data_block_size);
        let mut next_slice = buf;
        for (blk_idx, off, len) in blk_iter {
            let (this, next) = next_slice.split_at(len);
            debug!("      - update cache block index {}, offset {}, len {}", blk_idx, off, len);
            self.update_cache(blk_idx, off, this);
            bytes_write += this.len();
            next_slice = next;
        }

        // bulk update bmap
        let blk_iter = BlockIndexIter::new(off, len, data_block_size);
        let mut new_blocks: usize = 0;
        for (blk_idx, _, _) in blk_iter {
            // force bmap update for dirty blocks
            let prev = self.bmap.insert(blk_idx, BlockPtrFormat::dummy_value()).await.expect("failed to insert dummy value to bmap for dirty blocks");
            if prev.is_none() {
                new_blocks += 1;
            }
        }

        let oldsize = self.inode.size();
        if off + len > oldsize {
            self.inode.set_size(off + len);
            self.cache.set_size(off + len);
        }
        if new_blocks > 0 {
            // Each newly-allocated bmap entry backs `data_block_size`
            // bytes; convert to the 512-byte units that st_blocks
            // reports.
            self.inode.update_blocks((new_blocks * data_block_size) as isize);
        }
        self.inode.update_mtime();
        drop(permit);

        if let Err(e) = self.try_flush().await {
            let _ = self.rollback_from_persisted().await;
            return Err(e);
        }
        let _ = fn_start;
        Ok(bytes_write)
    }

    pub async fn write_zero(&mut self, off: usize, len: usize) -> Result<usize> {
        if !self.flags.is_writable() {
            return Err(Self::ebadf_not_writable());
        }
        self.write_zero_inner(off, len).await
    }

    /// `write_zero` without the write-access check. See
    /// [`Self::write_inner`] for why WAL recovery needs this.
    async fn write_zero_inner(&mut self, off: usize, len: usize) -> Result<usize> {
        let permit = self.sema.clone().acquire_owned().await.unwrap();
        let fn_start = Instant::now();
        // O_APPEND: same rule as write(). See the corresponding
        // comment there.
        let off = if self.flags.is_append() {
            self.inode.size()
        } else {
            off
        };
        debug!("WRITE ZERO - off: {}, len: {}", off, len);

        let v = self.write_prepare(off, len);
        let fetched = self.write_retrieve(v).await?;
        for block in fetched.into_iter() {
            let blk_idx = block.index();
            let None = self.cache.insert(blk_idx, block) else {
                panic!("BlockIndex {} already on data_blocks_dirty list", blk_idx);
            };
        }

        #[cfg(feature = "wal")]
        if let Some(wal) = &mut self.wal {
            let _ = wal.write_zero(self.inode.get_last_seq(), off, len).await?;
        }

        let mut bytes_write = 0;
        let mut new_blocks: usize = 0;

        let data_block_size = self.config.meta.data_block_size;
        let oldsize = self.inode.size();
        let blk_iter = BlockIndexIter::new(off, len, data_block_size);
        for (blk_idx, start_off, data_len) in blk_iter {
            // for a complete block,
            // no need to update data in cache, because is's already all zero
            // and insert zero block into block map
            if start_off == 0 && data_len == data_block_size {
                // insert or update
                let prev = self.bmap.insert(blk_idx, BlockPtrFormat::new_zero_block()).await.expect("failed to insert new zero to bmap");
                if prev.is_none() {
                    new_blocks += 1;
                }
                bytes_write += data_len;
                let _ = self.cache.remove(&blk_idx);
                continue;
            }
            // for a incomplete block
            // last block execption which start off from block start and len exceed current file
            // TODO: merge this with new cache impl
            if start_off == 0 && (blk_idx as usize * data_block_size) + start_off + data_len > oldsize {
                // insert or update
                let prev = self.bmap.insert(blk_idx, BlockPtrFormat::new_zero_block()).await.expect("failed to insert new zero to bmap");
                if prev.is_none() {
                    new_blocks += 1;
                }
                bytes_write += data_len;
                let _ = self.cache.remove(&blk_idx);
                continue;
            }
            // update cache data with zero
            debug!("      - update cache block index {}, offset {}, len {}", blk_idx, start_off, data_len);
            let mut zero = Vec::with_capacity(data_len);
            zero.resize(data_len, 0);
            self.update_cache(blk_idx, start_off, &zero);
            // force bmap update for dirty blocks
            let prev = self.bmap.insert(blk_idx, BlockPtrFormat::dummy_value()).await.expect("failed to insert dummy value to bmap for dirty blocks");
            if prev.is_none() {
                new_blocks += 1;
            }
            bytes_write += data_len;
        }

        let oldsize = self.inode.size();
        if off + len > oldsize {
            self.inode.set_size(off + len);
            self.cache.set_size(off + len);
        }
        if new_blocks > 0 {
            self.inode.update_blocks((new_blocks * data_block_size) as isize);
        }
        self.inode.update_mtime();
        drop(permit);

        if let Err(e) = self.try_flush().await {
            let _ = self.rollback_from_persisted().await;
            return Err(e);
        }
        let _ = fn_start;
        Ok(bytes_write)
    }

    // write in batch style, all blocks in input vec should be full block
    pub(crate) async fn write_aligned_batch(&mut self, mut blocks: Vec<AlignedDataBlockWrapper>) -> Result<usize> {
        if !self.flags.is_writable() {
            return Err(Self::ebadf_not_writable());
        }
        if blocks.len() == 0 {
            return Ok(0);
        }

        // sort and dedup
        blocks.sort_by_key(|b| b.index());
        blocks.reverse();
        blocks.dedup_by_key(|b| b.index());
        blocks.reverse();

        let permit = self.sema.clone().acquire_owned().await.unwrap();
        let data_block_size = self.config.meta.data_block_size;

        let mut bytes_write = 0;
        let mut new_blocks: usize = 0;
        for block_wrapper in blocks.iter() {
            let blk_idx = block_wrapper.index();
            let blk_sz = block_wrapper.size();
            assert!(blk_sz == data_block_size);
            if block_wrapper.is_zero() {
                let _ = self.cache.remove(&blk_idx);
                bytes_write += blk_sz;
                let prev = self.bmap.insert(blk_idx, BlockPtrFormat::new_zero_block()).await.expect("failed to insert new zero to bmap");
                if prev.is_none() {
                    new_blocks += 1;
                }
                continue;
            }
            self.update_cache(blk_idx, 0, block_wrapper.as_slice());
            bytes_write += blk_sz;
            // force bmap update for dirty blocks
            let prev = self.bmap.insert(blk_idx, BlockPtrFormat::dummy_value()).await.expect("failed to insert dummy value to bmap for dirty blocks");
            if prev.is_none() {
                new_blocks += 1;
            }
        }
        // try update file size by offset and len from last block
        let last_block_wrapper = blocks.last().expect("unable to get last block, input blocks is empty");
        let oldsize = self.inode.size();
        let off = (last_block_wrapper.index() as usize) * data_block_size;
        let len = last_block_wrapper.size();
        if off + len > oldsize {
            self.inode.set_size(off + len);
            self.cache.set_size(off + len);
        }
        if new_blocks > 0 {
            self.inode.update_blocks((new_blocks * data_block_size) as isize);
        }
        self.inode.update_mtime();
        drop(permit);
        if let Err(e) = self.try_flush().await {
            let _ = self.rollback_from_persisted().await;
            return Err(e);
        }
        Ok(bytes_write)
    }

    pub(crate) fn need_flush(&self) -> bool {
        // check if dirty data bytes exceed segment buffer threshold
        let ndatadirty = self.cache.dirty_count();
        let data_block_size = self.config.meta.data_block_size;
        // trigger flush because we meet memory threshold
        let threshold_flush = ndatadirty > self.max_dirty_blocks
            || (ndatadirty * data_block_size) > self.config.runtime.segment_buffer_size;
        // trigger immediate flush if file opened in O_DIRECT or O_SYNC or O_DSYNC
        let immediate_flush = if self.flags.is_sync_flush_mode() {
            #[cfg(not(feature = "wal"))]
            { true }
            // in wal mode, we dont's need immediately flush
            // unless wal is not configured
            #[cfg(feature = "wal")]
            { self.wal.is_none() }
        } else {
            false
        };

        let max_flush_interval = self.config.runtime.data_cache_dirty_max_flush_interval;
        let last_flush_expired = self.state.get_last_flush().elapsed() >= Duration::from_millis(max_flush_interval);
        if last_flush_expired || threshold_flush || immediate_flush {
            return true;
        }
        false
    }

    // try flush out dirty data if all threshold condition meet
    pub(crate) async fn try_flush(&mut self) -> Result<bool> {
        if self.need_flush() {
            let _ = self.flush().await?;
            return Ok(true);
        }
        Ok(false)
    }

    /// Explicit flush with rollback-on-failure semantics.
    ///
    /// `HyperTrait::flush()` commits all pending in-memory mutations to
    /// staging. If it fails, in-memory state still reflects the mutations
    /// that never made it to disk, diverging from the persisted state. This
    /// wrapper rolls the in-memory state back to what's persisted on
    /// failure, so callers that observe `Err` also see an in-memory state
    /// that matches reality.
    ///
    /// Users exercise this via `fs_flush` / `fh_flush`.
    pub async fn flush_with_rollback(&mut self) -> Result<SegmentId> {
        match self.flush().await {
            Ok(segid) => Ok(segid),
            Err(e) => {
                let _ = self.rollback_from_persisted().await;
                Err(e)
            }
        }
    }

    /// POSIX-`fdatasync` flavoured flush.
    ///
    /// Acts as `flush_with_rollback` but skips work that
    /// `fdatasync(2)` is allowed to skip per POSIX: flushing
    /// metadata that is not required to read the file's data
    /// correctly. In hyperfile terms, that means skipping the
    /// inode write when only attribute fields (`atime` / `mtime` /
    /// `ctime` / `mode` / `uid` / `gid`) are dirty and there is
    /// no dirty data block and no dirty bmap.
    ///
    /// If data or bmap is dirty, this delegates to
    /// `flush_with_rollback` — those updates are necessary for
    /// data correctness (`i_size` lives in the inode) and
    /// `fdatasync` must persist them too.
    ///
    /// Returns the last cno currently persisted on staging. When
    /// the call short-circuits (no data/bmap dirt), no new
    /// segment is written and the returned cno is the same as
    /// `last_cno()`.
    pub async fn flush_data(&mut self) -> Result<SegmentId> {
        if self.dirty_block_count() == 0 && !self.bmap_dirty() {
            // Inode may still be attr-dirty, but `fdatasync` is
            // explicitly allowed to skip persisting attr-only
            // changes. Return the last persisted cno unchanged.
            return Ok(self.inode.get_last_ondisk_cno());
        }
        self.flush_with_rollback().await
    }

    /// Roll back the in-memory state of this file to match what's persisted
    /// on staging. Used by the failure path of write/truncate/write_zero/flush
    /// to undo in-memory mutations when the flush fails. Steps:
    ///   1. Reload persisted inode and rebuild bmap from it.
    ///   2. Replace self.inode fields with the persisted values, preserving
    ///      read-only attrs like ino/uid/gid/mode/nlink which aren't mutated
    ///      by these code paths.
    ///   3. Reset bookkeeping (last_seq, ondisk_state) to the reloaded values
    ///      so subsequent operations see a consistent state.
    ///   4. Discard dirty data blocks and cached read blocks.
    ///
    /// Best-effort: if reloading the persisted inode itself fails, this
    /// function returns the error without attempting further recovery. The
    /// caller should propagate the original flush error regardless.
    pub(crate) async fn rollback_from_persisted(&mut self) -> Result<()> {
        // 1. reload persisted inode + rebuild bmap
        let mut raw_inode: InodeRaw = unsafe { std::mem::MaybeUninit::zeroed().assume_init() };
        let inode_state = self.staging.load_inode(&mut raw_inode.as_mut_u8_slice()).await?;
        let b = raw_inode.i_bmap;
        let meta_block_loader = self.staging.to_block_loader();
        let node_cache = self.bmap.get_node_cache();
        let new_bmap = BMap::<BlockIndex, BlockPtr, BlockPtr, L, C>::read(
            &b,
            self.config.meta.meta_block_size,
            meta_block_loader,
            node_cache,
        )?;
        new_bmap.set_cache_limit(self.config.runtime.node_cache_blocks);

        // 2. commit fresh bmap + inode fields from persisted state
        self.bmap = new_bmap;
        self.inode.restore_attr_from_raw(&raw_inode);
        self.inode.i_last_seq = raw_inode.i_last_seq;
        self.inode.i_last_cno = raw_inode.i_last_cno;
        self.inode.set_last_ondisk_cno(raw_inode.i_last_cno);
        self.inode.set_ondisk_state(inode_state);
        self.cache.set_size(self.inode.size());

        // 3. discard in-memory dirty / cached data
        self.cache.clear_dirty();
        self.cache.clear_data_blocks_cache();

        Ok(())
    }

    #[allow(dead_code)]
    #[cfg(all(feature = "wal", feature = "blocking"))]
    pub(crate) async fn kick_wal_protected_flush_blocking(&mut self) -> Result<SegmentId> {
        let lock = self.flush_lock().await;
        match self.wal_flush_process_blocking().await {
            Ok(segid) => {
                self.flush_unlock(lock);
                return Ok(segid);
            },
            Err(e) => {
                warn!("kick_wal_protected_flush_blocking failed: {:?}", e);
                return self.wal_flush_recovery(lock).await;
            },
        }
    }

    #[cfg(all(feature = "wal", feature = "reactor"))]
    pub(crate) async fn kick_wal_protected_flush_reactor(&mut self, fh: ChannelGroup<FileContext<'a>>) -> Result<SegmentId> {
        let Ok(lock) = self.flush_lock.clone().try_lock_owned() else {
            // FIXME: skip this flush by return ResourceBusy for now,
            // should this flush be re-queue?
            return Err(Error::new(ErrorKind::ResourceBusy, "another flush is in-progress"));
        };
        self.state.set_flushing();
        match self.wal_flush_process_reactor(fh, lock).await {
            Ok(segid) => {
                return Ok(segid);
            },
            Err((lock, e)) => {
                warn!("kick_wal_protected_flush_reactor failed: {:?}", e);
                return self.wal_flush_recovery(lock).await;
            },
        }
    }

    #[cfg(all(feature = "wal", feature = "reactor"))]
    pub(crate) async fn wal_flush_done(&mut self, lock: OwnedMutexGuard<()>, segid: SegmentId, od_state: OnDiskState, bmap_cache_limit: usize) {
        self.inode_mut().set_ondisk_state(Some(od_state));
        let last_cno = self.inode().get_last_cno();
        assert!(last_cno == segid);
        self.inode_mut().set_last_ondisk_cno(last_cno);
        self.wal_clear_mem_segment(segid).await;
        // Fire-and-forget delete of the persisted WAL objects.
        //
        // WAL chunks are written under the inode's last_seq at the
        // time of each write. The flush path calls get_next_seq(),
        // allocates the NEW segid for the segment, and the WAL
        // chunks stay under (segid - 1). Delete that prefix.
        //
        // A lost delete here leaves storage slightly bloated —
        // recovery filters by last_ondisk_cno, so old entries are
        // still correct — and is not worth blocking the flush ack.
        self.wal_spawn_delete_segment(segid.saturating_sub(1));
        // restore cache limit
        self.restore_data_blocks_cache_limit();
        self.bmap_set_cache_limit(bmap_cache_limit);
        self.set_last_flush();
        self.flush_unlock(lock);
    }

    // starting wal flush recovery process by reloading inode from backend storage
    // everything should be clean or give a panic if unrecoverable
    #[cfg(feature = "wal")]
    pub(crate) async fn wal_flush_recovery(&mut self, lock: OwnedMutexGuard<()>) -> Result<SegmentId> {
        debug!("wal_flush_recovery - started");
        match self.do_wal_flush_recovery().await {
            Ok(cno) => {
                self.flush_unlock(lock);
                if cno != 0 { return Ok(cno); }
                warn!("wal_flush_recovery - return with cno 0");
            },
            Err(e) => {
                self.flush_unlock(lock);
                warn!("wal_flush_recovery - return with err {}", e);
            },
        }
        panic!("wal_flush_recovery - failed, please fix wal with offline tools");
    }

    // handler flush lock in caller
    #[cfg(feature = "wal")]
    async fn do_wal_flush_recovery(&mut self) -> Result<SegmentId> {
        let v = self.wal_list_segments().await?;
        let last_ondisk = self.inode().get_last_ondisk_cno();
        // filter out candidate segment id to playback
        let mut segids: Vec<_> = v.into_iter().filter(|id| *id >= last_ondisk).collect();
        segids.sort();
        debug!("do_wal_flush_recovery - replay segments {:?}", segids);

        let mut cno = 0;
        for segid in segids {
            cno = self.wal_replay_chunks(segid).await?;
            assert!(cno == segid + 1);
        }

        Ok(cno)
    }

    #[cfg(feature = "wal")]
    pub async fn wal_list_segments(&self) -> Result<Vec<SegmentId>> {
        let Some(ref wal) = self.wal else {
            return Err(Error::new(ErrorKind::Unsupported, "wal is not configured"));
        };
        wal.list_segments().await
    }

    #[cfg(feature = "wal")]
    pub async fn wal_list_chunks(&self, segid: SegmentId) -> Result<BTreeMap<usize, WalChunkDesc>> {
        let Some(ref wal) = self.wal else {
            return Err(Error::new(ErrorKind::Unsupported, "wal is not configured"));
        };
        wal.list_chunks(segid).await
    }

    #[cfg(feature = "wal")]
    pub async fn wal_replay_chunks(&mut self, segid: SegmentId) -> Result<SegmentId> {
        debug!("wal_replay_chunks - start to process {}", segid);
        let map = self.wal_list_chunks(segid).await?;

        // take out wal to avoid write path exec into wal again
        let wal = self.wal.take();

        // retrieve all chunks and write to file
        // TODO: currently one by one, make it concurrent in future
        //
        // The `_inner` variants bypass the write-access check on
        // purpose: recovery replays writes that were already
        // acknowledged before the crash, and it runs during `open`
        // regardless of the access mode the caller asked for. A
        // read-only open of a file that crashed mid-flush still has
        // to replay the WAL to present correct contents.
        for (_, chunk) in map.iter() {
            if chunk.is_zero {
                let _ = self.write_zero_inner(chunk.offset, chunk.len).await?;
            } else {
                let data = wal.as_ref().unwrap().read(chunk.seq, chunk.segid, chunk.offset, chunk.len).await?;
                let _ = self.write_inner(chunk.offset, &data).await?;
            }
        }

        // install wal back
        self.wal = wal;

        // force flush
        let res = self.flush_process().await;

        // Fire-and-forget cleanup of the replayed WAL objects. If
        // this fails, recovery on the next open will just skip them
        // (list_segments is filtered by last_ondisk_cno), so the
        // only cost of a lost delete is a small bit of storage
        // bloat; don't block on it.
        if res.is_ok() {
            self.wal_spawn_delete_segment(segid);
        }

        res
    }

    pub async fn flush_inode(&mut self, flag: FlushInodeFlag) -> Result<()> {
        // TODO update necessary inode fields
        let mut b: BMapRawType = unsafe { std::mem::MaybeUninit::zeroed().assume_init() };
        b.copy_from_slice(self.bmap.as_slice());
        let raw_inode = self.inode.to_raw(b);
        let od_state = self.staging.flush_inode(raw_inode.as_u8_slice(), self.inode.get_ondisk_state(), flag).await?;
        self.inode.clear_attr_dirty();
        self.inode.set_ondisk_state(od_state);
        self.inode.set_last_ondisk_cno(self.inode.get_last_cno());
        Ok(())
    }

    // return: if last block data changed
    async fn truncate_last_data_block(&mut self, blk_idx: &BlockIndex, offset_to_discard: usize) -> Result<bool> {
        debug!("truncate_last_data_block - block index {}, offset_to_discard {}", blk_idx, offset_to_discard);
        if self.cache.truncate_data_block(blk_idx, offset_to_discard) {
            return Ok(true);
        }

        // blk index not in both dirty and cache list
        // or block ptr is zero block in bmap or not exist in bmap, no need to discard data
        let blk_ptr = match self.bmap.lookup(blk_idx).await {
            Ok(blk_ptr) => {
                if BlockPtrFormat::is_zero_block(&blk_ptr) {
                    // no need to discard data for a zero block
                    debug!("truncate_last_data_block - block ptr is zero block, nothing changed");
                    return Ok(false);
                } else if BlockPtrFormat::is_on_staging(&blk_ptr) {
                    blk_ptr
                } else {
                    panic!("invalid block ptr {} of block index {}", self.blk_ptr_decode_display(&blk_ptr), blk_idx);
                }
            },
            Err(e) => {
                if e.kind() != ErrorKind::NotFound {
                    return Err(e);
                }
                // no need to discard data for a non exists data block
                debug!("truncate_last_data_block - block index {} not found in bmap, nothing changed", blk_idx);
                return Ok(false);
            },
        };
        debug!("retrive block ptr {} for block index {}", self.blk_ptr_decode_display(&blk_ptr), blk_idx);
        let block = self.cache.new_block(*blk_idx);
        let buf = block.as_mut_slice();
        let _ = self.load_data_block_read_path(*blk_idx, blk_ptr, 0, buf).await?;
        // discard rest of data in the block
        let (_, to_clear) = buf.split_at_mut(offset_to_discard);
        to_clear.fill(0);
        // back to dirty list
        self.cache.insert(*blk_idx, block);
        let _ = self.bmap.insert(*blk_idx, BlockPtrFormat::dummy_value()).await.expect("failed to insert dummy value to bmap for dirty blocks");
        Ok(true)
    }

    // truncate
    pub async fn truncate(&mut self, new_size: usize) -> Result<()> {
        // POSIX ftruncate: "If fildes is not a valid file descriptor
        // open for writing, the ftruncate() function shall fail."
        // The spec allows EBADF or EINVAL here; we use EBADF to match
        // the write path.
        if !self.flags.is_writable() {
            return Err(Self::ebadf_not_writable());
        }
        let permit = self.sema.clone().acquire_owned().await.unwrap();
        let size = self.inode.size();
        debug!("truncate - file size from {} to {}", size, new_size);
        if new_size == size {
            // current size same as expected size
            return Ok(());
        }

        let data_block_size = self.config.meta.data_block_size;
        let tgt_blk_idx = (new_size / data_block_size) as BlockIndex;
        let cur_blk_idx = (size / data_block_size) as BlockIndex;
        let offset_to_discard = new_size % data_block_size;

        if tgt_blk_idx == cur_blk_idx {
            let data_changed = self.truncate_last_data_block(&tgt_blk_idx, offset_to_discard).await?;
            // no need to change metadata blocks, just update the new file size
            self.inode.set_size(new_size);
            self.cache.set_size(new_size);
            self.inode.update_mtime();
            if data_changed {
                debug!("truncate - data changed, trigger flush");
            } else {
                debug!("truncate - no bmap and data changed, update file attr only");
            }
            drop(permit);
            if let Err(e) = self.flush().await {
                let _ = self.rollback_from_persisted().await;
                return Err(e);
            }
            return Ok(());
        }

        // if need to extend file length
        if tgt_blk_idx > cur_blk_idx {
            // no need to modify bmap, just update new file size
            self.inode.set_size(new_size);
            self.cache.set_size(new_size);
            self.inode.update_mtime();
            debug!("truncate - extend file size with no bmap change, update file attr only");
            drop(permit);
            if let Err(e) = self.flush().await {
                let _ = self.rollback_from_persisted().await;
                return Err(e);
            }
            return Ok(());
        }

        debug!("truncate - shrink bmap to BlockIndex {}", tgt_blk_idx);
        // re-calc tgt_blk_idx for bmap truncate
        let tgt_blk_idx = ((new_size + data_block_size - 1) / data_block_size) as BlockIndex;

        // Count entries in [tgt_blk_idx, ∞) before we drop them, so
        // we can decrement i_blocks correctly. seek_key returns the
        // smallest key >= start, NotFound when none. Walking via
        // (found+1) gives a one-pass count of exactly the entries
        // about to be discarded.
        let mut removed_blocks: usize = 0;
        {
            let mut k = tgt_blk_idx;
            loop {
                match self.bmap.seek_key(&k).await {
                    Ok(found) => {
                        removed_blocks += 1;
                        let Some(next) = found.checked_add(1) else { break; };
                        k = next;
                    }
                    Err(_) => break,
                }
            }
        }

        // Drop every cached entry whose key is about to be
        // truncated from the bmap, dirty and clean alike.
        //
        // Dirty: without this the next flush would iterate the
        // dirty list, find a block with no matching bmap entry, and
        // fail `bmap.assign(blk_idx, ptr) -> NotFound`.
        //
        // Clean: a block that was already flushed is no longer part
        // of the file. Leaving it cached lets the read path serve
        // the pre-truncate bytes if the file is later grown back
        // past the old EOF, where POSIX requires a hole (zeros).
        let _ = self.cache.truncate_blocks_above(tgt_blk_idx);

        // if need to shrink bmap
        if let Err(e) = self.bmap.truncate(&tgt_blk_idx).await {
            if e.kind() != ErrorKind::NotFound {
                // bmap.truncate may have partially mutated the in-memory tree
                // even when returning error. Roll back to persisted state.
                let _ = self.rollback_from_persisted().await;
                return Err(e);
            }
            // NotFound is fine, let's continue
        }
        if new_size > 0 && offset_to_discard != 0 {
            // We only need to zero the tail of the LAST partially-
            // retained block. When new_size lands exactly on a block
            // boundary (offset_to_discard == 0), the block at
            // tgt_blk_idx-1 is fully retained and must NOT be
            // touched: passing offset_to_discard=0 to
            // `truncate_last_data_block` would interpret it as
            // "discard from offset 0", wiping the entire block.
            let tgt_blk_idx = tgt_blk_idx - 1;
            if let Err(e) = self.truncate_last_data_block(&tgt_blk_idx, offset_to_discard).await {
                let _ = self.rollback_from_persisted().await;
                return Err(e);
            }
        }
        self.inode.set_size(new_size);
        self.cache.set_size(new_size);
        if removed_blocks > 0 {
            self.inode.update_blocks(-((removed_blocks * data_block_size) as isize));
        }
        self.inode.update_mtime();
        drop(permit);
        if let Err(e) = self.flush().await {
            let _ = self.rollback_from_persisted().await;
            return Err(e);
        }
        Ok(())
    }

    pub async fn unlink(&self) -> Result<()> {
        let _ = self.staging.unlink().await?;
        Ok(())
    }

    // return last persistent cno on disk
    #[inline]
    pub fn last_cno(&self) -> u64 {
        #[cfg(not(feature = "wal"))]
        return self.inode.get_last_ondisk_cno();
        #[cfg(feature = "wal")]
        return self.inode.get_last_cno();
    }

    pub fn staging_config(&self) -> &StagingConfig {
        &self.config.staging
    }

    pub fn staging_interceptor(&mut self, i: impl StagingIntercept<T> + 'static) {
        self.staging.interceptor(i);
    }

    /// Test-only: number of dirty data blocks in the cache.
    #[doc(hidden)]
    pub fn dirty_block_count(&self) -> usize {
        self.cache.dirty_count()
    }

    /// Test-only: whether the inode has pending attr-only changes.
    #[doc(hidden)]
    pub fn is_attr_dirty(&self) -> bool {
        self.inode.is_attr_dirty()
    }

    /// Test-only: whether the bmap tree has dirty meta nodes.
    #[doc(hidden)]
    pub fn is_bmap_dirty(&self) -> bool {
        self.bmap.dirty()
    }

    /// Test-only: last committed checkpoint number (in-memory value).
    #[doc(hidden)]
    pub fn in_memory_last_cno(&self) -> u64 {
        self.inode.get_last_cno()
    }

    /// Test-only: last on-disk checkpoint number (in-memory tracking).
    #[doc(hidden)]
    pub fn in_memory_last_ondisk_cno(&self) -> u64 {
        self.inode.get_last_ondisk_cno()
    }

    /// Benchmark/regression-profiling only: cumulative per-phase
    /// timing counters accumulated across all completed flushes.
    /// Unstable interface, do not rely on it from production code.
    #[doc(hidden)]
    pub fn flush_timing(&self) -> &FlushTiming {
        &self.flush_timing
    }

    /// Benchmark/regression-profiling only: reset the flush timing
    /// counters to zero.
    #[doc(hidden)]
    pub fn flush_timing_reset(&self) {
        self.flush_timing.reset();
    }
}

impl<'a: 'static, T: Staging<L> + SegmentReadWrite + Send + Clone + 'static, L: BlockLoader<BlockPtr> + Clone + 'static, C: NodeCache<BlockPtr> + Clone> HyperFile<'a, T, L, C> {
    // we only care about incomplete blocks and not in dirty list
    // return:
    //   - vec of data block ptr we need to retrieve
    pub(crate) fn write_prepare(&mut self, off: usize, len: usize) -> Vec<BlockIndex> {
        self.cache.write_prepare(off, len)
    }

    // test if block of index need to be retrieve
    #[inline]
    pub(crate) fn write_prepare_block_index(&mut self, blk_idx: &BlockIndex) -> bool {
        !self.cache.contains(blk_idx)
    }

    async fn write_retrieve(&mut self, list: Vec<BlockIndex>) -> Result<Vec<DataBlock>> {
        let mut output = Vec::new();
        for blk_idx in list {
            match self.bmap.lookup(&blk_idx).await {
                Ok(blk_ptr) => {
                    debug!("retrive block ptr {} for block index {}", self.blk_ptr_decode_display(&blk_ptr), blk_idx);
                    let block = self.cache.new_block(blk_idx);
                    block.set_should_cache();
                    let buf = block.as_mut_slice();
                    if !BlockPtrFormat::is_zero_block(&blk_ptr) {
                        let _ = self.load_data_block_write_path(blk_idx, blk_ptr, 0, buf).await?;
                    }
                    output.push(block);
                },
                Err(e) => {
                    if e.kind() != ErrorKind::NotFound {
                        return Err(e);
                    }
                    debug!("block index {} not found in bmap, prepare a new block", blk_idx);
                    let block = self.cache.new_block(blk_idx);
                    block.set_should_cache();
                    output.push(block);
                },
            }
        }
        Ok(output)
    }

    pub(crate) fn update_cache(&mut self, blk_idx: BlockIndex, off: usize, buf: &[u8]) {
        self.cache.update_cache(&blk_idx, off, buf)
    }

    async fn load_data_block_read_path(&mut self, blk_idx: BlockIndex, blk_ptr: BlockPtr, offset: usize, buf: &mut [u8]) -> Result<()> {
        debug!("load_data_block_read_path - block index: {}, offset: {}, bytes: {}, block ptr: {}",
            blk_idx, offset, buf.len(), self.blk_ptr_decode_display(&blk_ptr));
        // in read path we would check both data and dirty cache before do real data load
        if let Some(block) = self.cache.get(&blk_idx) {
            let slice = block.as_slice();
            buf.copy_from_slice(&slice[offset..offset + buf.len()]);
            block.unlock();
            return Ok(());
        }
        #[cfg(feature = "wal")]
        if self.wal.is_some() && BlockPtrFormat::is_on_staging(&blk_ptr) && (self.inode().get_last_cno() > self.inode().get_last_ondisk_cno()) {
            let (segid, staging_off) = self.blk_ptr_decode(&blk_ptr);
            if segid > self.inode().get_last_ondisk_cno() {
                let data_buf = unsafe {
                    std::slice::from_raw_parts_mut(buf.as_mut_ptr() as *mut u8, buf.len())
                };
                let lock = self.flushing_segments.read().await;
                let Some(weak_data) = lock.get(&segid) else {
                    panic!("unable to find segid: {segid} from inflight flushing segments");
                };
                let Some(data) = weak_data.upgrade() else {
                    panic!("failed to get back shared data ref of inflight flushing segid: {segid}");
                };
                let start_off = staging_off + offset;
                let end = start_off + data_buf.len();
                data_buf.copy_from_slice(&data[start_off..end]);
                return Ok(());
            }
        }
        if BlockPtrFormat::is_on_staging(&blk_ptr) {
            let (segid, staging_off) = self.blk_ptr_decode(&blk_ptr);
            let _ = self.staging.load_data_block(segid, staging_off, offset, self.config.meta.data_block_size, buf).await?;
            return Ok(());
        } else if BlockPtrFormat::is_dummy_value(&blk_ptr) {
            panic!("failed to get block index: {} from data blocks dirty cache for dummy block ptr", blk_idx);
        } else if BlockPtrFormat::is_zero_block(&blk_ptr) {
            buf.fill(0);
            return Ok(());
        } else {
            panic!("load_data_block_read_path - block index: {}, offset: {}, bytes: {}, incorrect block ptr {} to load",
                blk_idx, offset, buf.len(), self.blk_ptr_decode_display(&blk_ptr));
        }
    }

    async fn load_data_block_write_path(&mut self, blk_idx: BlockIndex, blk_ptr: BlockPtr, offset: usize, buf: &mut [u8]) -> Result<()> {
        debug!("load_data_block_write_path - block index: {}, offset: {}, bytes: {}, block ptr: {}",
            blk_idx, offset, buf.len(), self.blk_ptr_decode_display(&blk_ptr));
        #[cfg(feature = "wal")]
        if self.wal.is_some() && BlockPtrFormat::is_on_staging(&blk_ptr) && (self.inode().get_last_cno() > self.inode().get_last_ondisk_cno()) {
            let (segid, staging_off) = self.blk_ptr_decode(&blk_ptr);
            if segid > self.inode().get_last_ondisk_cno() {
                let data_buf = unsafe {
                    std::slice::from_raw_parts_mut(buf.as_mut_ptr() as *mut u8, buf.len())
                };
                let lock = self.flushing_segments.read().await;
                let Some(weak_data) = lock.get(&segid) else {
                    panic!("unable to find segid: {segid} from inflight flushing segments");
                };
                let Some(data) = weak_data.upgrade() else {
                    panic!("failed to get back shared data ref of inflight flushing segid: {segid}");
                };
                let start_off = staging_off + offset;
                let end = start_off + data_buf.len();
                data_buf.copy_from_slice(&data[start_off..end]);
                return Ok(());
            }
        }
        if BlockPtrFormat::is_on_staging(&blk_ptr) {
            let (segid, staging_off) = self.blk_ptr_decode(&blk_ptr);
            let _ = self.staging.load_data_block(segid, staging_off, offset, self.config.meta.data_block_size, buf).await?;
            return Ok(());
        } else if BlockPtrFormat::is_dummy_value(&blk_ptr) {
            if let Some(block) = self.cache.get(&blk_idx) {
                let slice = block.as_slice();
                buf.copy_from_slice(&slice[offset..offset + buf.len()]);
                block.unlock();
                return Ok(());
            }
            panic!("failed to get block index: {} from data blocks dirty cache for dummy block ptr", blk_idx);
        } else if BlockPtrFormat::is_zero_block(&blk_ptr) {
            buf.fill(0);
            return Ok(());
        } else {
            panic!("load_data_block_write_path - block index: {}, offset: {}, bytes: {}, incorrect block ptr {} to load",
                blk_idx, offset, buf.len(), self.blk_ptr_decode_display(&blk_ptr));
        }
    }

    // return max dirty data blocks can hold
    fn calc_max_dirty_blocks(data_block_size: usize, max_dirty_bytes_threshold: usize, max_dirty_blocks_threshold: usize) -> usize {
        let max_blocks = max_dirty_bytes_threshold / data_block_size;
        std::cmp::max(max_blocks, max_dirty_blocks_threshold)
    }
}

impl<'a: 'static, T: Staging<L> + SegmentReadWrite + Send + Clone + 'static, L: BlockLoader<BlockPtr> + Clone + 'static, C: NodeCache<BlockPtr> + Clone> HyperFile<'a, T, L, C> {
    // write in batch style, input blocks could be incomplete
    pub async fn write_batch(&mut self, blocks: Vec<BatchDataBlockWrapper>) -> Result<usize> {
        if !self.flags.is_writable() {
            return Err(Self::ebadf_not_writable());
        }
        if blocks.len() == 0 {
            return Ok(0);
        }

        let data_block_size = self.config.meta.data_block_size;
        let mut bytes_write = 0;

        // group by block index
        let mut map: HashMap<BlockIndex, Vec<BatchDataBlockWrapper>> = HashMap::new();
        for block in blocks.into_iter() {
            let blk_idx = block.index() as BlockIndex;
            if let Some(v) = map.get_mut(&blk_idx) {
                v.push(block);
            } else {
                map.insert(blk_idx, vec![block]);
            }
        }

        // merge each block index group
        // (is one full block, vec of partial blocks or vec of one full block)
        let mut merged: BTreeMap<BlockIndex, (bool, Vec<BatchDataBlockWrapper>)> = BTreeMap::new();
        for (blk_idx, mut v) in map.into_iter() {
            // try find a full block from back to head
            let res = v.iter().rposition(|b| b.is_full_block());
            let m = if let Some(idx) = res {
                // if we found a full block
                let mut rest = v.split_off(idx);
                rest.reverse();
                let mut full_block = rest.pop().expect("invalid rest vec by split_off");
                while let Some(next_block) = rest.pop() {
                    full_block.merge_partial(&next_block);
                }
                (true, vec![full_block])
            } else {
                // if all partial blocks, we can't merge
                (false, v)
            };
            merged.insert(blk_idx, m);
        }

        let permit = self.sema.clone().acquire_owned().await.unwrap();

        // write prepare
        let mut v_need_retrieve = Vec::new();
        for blk_idx in merged.keys().into_iter() {
            if self.write_prepare_block_index(blk_idx) {
                v_need_retrieve.push(*blk_idx);
            }
        }

        #[cfg(not(feature = "reactor"))]
        let fetched = self.write_retrieve(v_need_retrieve).await?;

        #[cfg(feature = "reactor")]
        let mut fetched = Vec::new();
        #[cfg(feature = "reactor")]
        let mut joins = Vec::new();
        #[cfg(feature = "reactor")]
        for blk_idx in v_need_retrieve {
            match self.bmap.lookup(&blk_idx).await {
                Ok(blk_ptr) => {
                    let block = self.cache.new_block(blk_idx);
                    block.set_should_cache();
                    let buf = block.as_mut_slice();
                    let join = self.spawn_load_data_block_write_path(blk_idx, blk_ptr, 0, buf)?;
                    joins.push(join);
                    fetched.push(block);
                },
                Err(e) => {
                    if e.kind() != ErrorKind::NotFound {
                        return Err(e);
                    }
                    let block = self.cache.new_block(blk_idx);
                    block.set_should_cache();
                    fetched.push(block);
                },
            }
        }
        #[cfg(feature = "reactor")]
        while let Some(o) = joins.pop() {
            match o {
                super::reactor::ImmOrJoinSize::ImmSize(_) => {},
                super::reactor::ImmOrJoinSize::JoinSize(j) => { let _ = j.await; },
            }
        }

        // insert fetched data blocks into dirty list
        for block in fetched.into_iter() {
            let blk_idx = block.index();
            let None = self.cache.insert(blk_idx, block) else {
                panic!("BlockIndex {} already on data_blocks_dirty list", blk_idx);
            };
        }

        let mut new_blocks: usize = 0;
        for (blk_idx, (is_full_block, v_blocks)) in merged.iter() {
            if !is_full_block {
                // if not a full block, playback all partial data blocks
                let block = self.cache.get_mut(&blk_idx).expect("failed to get back data block from dirty list");
                for part in v_blocks.iter() {
                    if part.is_zero() {
                        let mut zero = Vec::new();
                        zero.resize(part.len(), 0);
                        block.copy(part.offset(), &zero);
                    } else {
                        block.copy(part.offset(), part.as_slice());
                    }
                    bytes_write += part.len();
                }
                block.unlock();
                let prev = self.bmap.insert(*blk_idx, BlockPtrFormat::dummy_value()).await.expect("failed to insert dummy value to bmap for dirty blocks");
                if prev.is_none() {
                    new_blocks += 1;
                }
                continue;
            }
            // is full block
            assert!(v_blocks.len() == 1);
            let block_wrapper = &v_blocks[0];
            let blk_sz = block_wrapper.size();
            if block_wrapper.is_zero() {
                let _ = self.cache.remove(&blk_idx);
                bytes_write += blk_sz;
                let prev = self.bmap.insert(*blk_idx, BlockPtrFormat::new_zero_block()).await.expect("failed to insert new zero to bmap");
                if prev.is_none() {
                    new_blocks += 1;
                }
                continue;
            }
            self.update_cache(*blk_idx, 0, block_wrapper.as_slice());
            bytes_write += blk_sz;
            // force bmap update for dirty blocks
            let prev = self.bmap.insert(*blk_idx, BlockPtrFormat::dummy_value()).await.expect("failed to insert dummy value to bmap for dirty blocks");
            if prev.is_none() {
                new_blocks += 1;
            }
        }
        // try update file size by offset and len from last block
        let (blk_idx, (is_full_block, v_blocks)) = merged.pop_last().expect("unable to get last block, input blocks is empty");
        let oldsize = self.inode.size();
        let off = (blk_idx as usize) * data_block_size;
        let len = if is_full_block {
            data_block_size
        } else {
            v_blocks.iter().max_by_key(|b| b.len()).expect("invalid vec of partial data block").len()
        };
        if off + len > oldsize {
            self.inode.set_size(off + len);
            self.cache.set_size(off + len);
        }
        if new_blocks > 0 {
            self.inode.update_blocks((new_blocks * data_block_size) as isize);
        }
        self.inode.update_mtime();
        drop(permit);
        let _flushed = self.try_flush().await?;
        Ok(bytes_write)
    }
}

impl<T, L, C> HyperTrait<T, L, C, BlockPtr> for HyperFile<'_, T, L, C>
    where
        T: Staging<L> + SegmentReadWrite + Send + Clone + 'static,
        L: BlockLoader<BlockPtr> + Clone,
        C: NodeCache<BlockPtr> + Clone,
{
    fn blk_ptr_encode(&self, segid: SegmentId, offset: SegmentOffset, seq: usize) -> BlockPtr {
        BlockPtrFormat::encode(segid, offset, seq, &self.bmap_ud.blk_ptr_format)
    }

    fn blk_ptr_decode(&self, blk_ptr: &BlockPtr) -> (SegmentId, SegmentOffset) {
        BlockPtrFormat::decode(blk_ptr, &self.bmap_ud.blk_ptr_format)
    }

    fn blk_ptr_decode_display(&self, blk_ptr: &BlockPtr) -> String {
        if BlockPtrFormat::is_dummy_value(blk_ptr) {
            return format!("[Dummy]");
        } else if BlockPtrFormat::is_invalid_value(blk_ptr) {
            return format!("[Invalid]");
        } else if BlockPtrFormat::is_zero_block(blk_ptr) {
            return format!("[Zero Block]");
        } else if BlockPtrFormat::is_on_staging(blk_ptr) {
            let (id, off) = self.blk_ptr_decode(blk_ptr);
            let group_id = BlockPtrFormat::decode_micro_group_id(blk_ptr);
            return format!("[Staging: id {} - offset {} - group {}]", id, off, group_id);
        } else {
            return format!("[Unkown: 0x{:x}]", blk_ptr);
        }
    }

    fn clear_data_blocks_cache(&mut self) {
        self.cache.clear_data_blocks_cache()
    }

    fn set_data_blocks_cache_unlimited(&mut self) {
        self.cache.set_unlimited()
    }

    fn restore_data_blocks_cache_limit(&mut self) {
        self.cache.restore_limit()
    }

    fn get_data_blocks_dirty(&self) -> DirtyDataBlocks<'_> {
        self.cache.get_dirty()
    }

    fn clear_data_blocks_dirty(&mut self) {
        self.cache.clear_dirty()
    }

    async fn lock(&self) -> OwnedSemaphorePermit {
        let permit = self.sema.clone().acquire_owned().await.unwrap();
        permit
    }

    fn unlock(&self, permit: OwnedSemaphorePermit) {
        drop(permit);
    }

    async fn flush_lock(&self) -> OwnedMutexGuard<()> {
        let lock = self.flush_lock.clone().lock_owned().await;
        self.state.set_flushing();
        lock
    }

    fn flush_unlock(&self, lock: OwnedMutexGuard<()>) {
        drop(lock);
        self.state.clear_flushing();
    }

    fn bmap_as_slice(&self) -> &[u8] {
        self.bmap.as_slice()
    }

    fn bmap_get_block_loader(&self) -> L {
        self.bmap.get_block_loader()
    }

    fn bmap_get_node_cache(&self) -> C {
        self.bmap.get_node_cache()
    }

    fn bmap_dirty(&self) -> bool {
        self.bmap.dirty()
    }

    fn bmap_lookup_dirty(&self) -> Vec<BtreeNodeDirty<'_, BlockIndex, BlockPtr, BlockPtr>> {
        self.bmap.lookup_dirty()
    }

    async fn bmap_assign_meta_node(&self, blk_ptr: BlockPtr, node: BtreeNodeDirty<'_, BlockIndex, BlockPtr, BlockPtr>) -> Result<()> {
        self.bmap.assign_meta_node(blk_ptr, node).await
    }

    async fn bmap_assign_data_node(&self, blk_idx: &BlockIndex, blk_ptr: BlockPtr) -> Result<()> {
        self.bmap.assign_data_node(blk_idx, blk_ptr).await
    }

    fn bmap_clear_dirty(&mut self) {
        self.bmap.clear_dirty()
    }

    fn bmap_update(&mut self, bmap: BMap<'_, BlockIndex, BlockPtr, BlockPtr, L, C>) {
        *&mut self.bmap = unsafe {
            std::mem::transmute::<BMap<'_, BlockIndex, BlockPtr, BlockPtr, L, C>, BMap<'_, BlockIndex, BlockPtr, BlockPtr, L, C>>(bmap)
        };
    }

    async fn bmap_insert_dummy_value(bmap: &mut BMap<'_, BlockIndex, BlockPtr, BlockPtr, L, C>, blk_idx: &BlockIndex) -> Result<Option<BlockPtr>> {
        bmap.insert(*blk_idx, BlockPtrFormat::dummy_value()).await
    }

    fn bmap_set_cache_unlimited(&self) -> usize {
        let limit = self.bmap.get_cache_limit();
        self.bmap.set_cache_limit(DEFAULT_CACHE_UNLIMITED);
        limit
    }

    fn bmap_set_cache_limit(&self, limit: usize) {
        self.bmap.set_cache_limit(limit);
    }

    fn staging(&self) -> &T {
        &self.staging
    }

    fn config(&self) -> &HyperFileConfig {
        &self.config
    }

    fn set_last_flush(&mut self) {
        self.state.set_last_flush();
    }

    fn flush_timing(&self) -> &FlushTiming {
        &self.flush_timing
    }

    fn inode(&self) -> &Inode {
        &self.inode
    }

    #[allow(mutable_transmutes)]
    fn inode_mut(&self) -> &mut Inode {
        unsafe {
            std::mem::transmute::<&Inode, &mut Inode>(&self.inode)
        }
    }

    async fn sleep(dur: Duration) {
        tokio::time::sleep(dur).await;
    }

    // wal
    #[cfg(feature = "wal")]
    async fn wal_set_mem_segment(&self, mem_segid: SegmentId, mem_segdata: Weak<Pin<Box<Vec<u8>>>>) {
        let mut lock = self.flushing_segments.write().await;
        if let Some(_) = lock.insert(mem_segid, mem_segdata) {
            panic!("wal set mem segment - segid {mem_segid} already exists in memory flushing segments");
        }
    }

    #[cfg(feature = "wal")]
    async fn wal_clear_mem_segment(&self, mem_segid: SegmentId) {
        let mut lock = self.flushing_segments.write().await;
        let Some(weak_mem_seg) = lock.remove(&mem_segid) else {
            panic!("wal clear mem segment - segid {mem_segid} did not exists in memory flushing segments");
        };
        let Some(mem_seg) = weak_mem_seg.upgrade() else {
            panic!("wal clear mem segment - segid {mem_segid} not be able to upgrade");
        };
        unsafe { Arc::decrement_strong_count(Arc::as_ptr(&mem_seg)) };
        // end of mem segment life
        drop(mem_seg);
    }

    #[cfg(feature = "wal")]
    fn wal_spawn_delete_segment(&self, segid: SegmentId) {
        if let Some(ref wal) = self.wal {
            let fut = wal.delete_segment(segid);
            tokio::task::spawn(async move {
                if let Err(e) = fut.await {
                    warn!("wal delete_segment {} failed: {:?}", segid, e);
                }
            });
        }
    }
}
