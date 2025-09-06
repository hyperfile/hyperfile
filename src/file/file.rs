use std::fmt;
use std::collections::{HashMap, BTreeMap};
use std::sync::Arc;
#[cfg(feature = "wal")]
use std::sync::Weak;
#[cfg(feature = "wal")]
use std::pin::Pin;
use std::time::{Instant, Duration};
use std::io::{Error, ErrorKind, Result};
use log::{debug, warn};
#[cfg(all(feature = "wal", feature = "reactor"))]
use reactor::TaskHandler;
use btree_ondisk::{bmap::BMap, BlockLoader};
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
use super::flags::HyperFileFlags;
use super::mode::HyperFileMode;
use super::{HyperTrait, DirtyDataBlocks};
#[cfg(feature = "range-lock")]
use super::lock::RangeLock;
use crate::cache::mem_cache::MemCache;

pub struct HyperFile<'a, T: Send + Clone, L: BlockLoader<BlockPtr>> {
    pub(crate) staging: T,
    pub(crate) bmap: BMap<'a, BlockIndex, BlockPtr, BlockPtr, L>,
    pub(crate) bmap_ud: BMapUserData,
    pub(crate) cache: MemCache,
    pub(crate) inode: Inode,
    pub(crate) config: HyperFileConfig,
    pub(crate) max_dirty_blocks: usize,
    pub(crate) flags: HyperFileFlags,
    pub(crate) last_flush: Instant,
    pub(crate) sema: Arc<Semaphore>,
    pub(crate) flush_lock: Arc<Mutex<()>>,
    #[cfg(feature = "range-lock")]
    pub(crate) range_lock: RangeLock,
    #[cfg(feature = "reactor")]
    pub(crate) rt: Option<tokio::runtime::Runtime>,
    #[cfg(feature = "wal")]
    pub(crate) wal: Option<Box<dyn WalReadWrite + Send>>,
    #[cfg(feature = "wal")]
    pub(crate) flushing_segments: Arc<RwLock<HashMap<SegmentId, Weak<Pin<Box<Vec<u8>>>>>>>,
}

impl<T: Send + Clone, L: BlockLoader<BlockPtr>> fmt::Display for HyperFile<'_, T, L> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(f, "==== dump HyperFile ====")?;
        writeln!(f, "  {:?}", self.config)?;
        writeln!(f, "  max dirty blocks: {}", self.max_dirty_blocks)?;
        writeln!(f, "  {}", self.cache)?;
        writeln!(f, "  {}", self.inode)
    }
}

impl<T: Send + Clone, L: BlockLoader<BlockPtr>> Drop for HyperFile<'_, T, L> {
    fn drop(&mut self) {
        #[cfg(feature = "reactor")]
        if let Some(rt) = self.rt.take() {
            rt.shutdown_background();
        }
    }
}

impl<'a: 'static, T: Staging<L> + SegmentReadWrite + Send + Clone + 'static, L: BlockLoader<BlockPtr> + Clone + 'static> HyperFile<'a, T, L> {
    pub async fn new(staging: T, meta_block_loader: L, config: HyperFileConfig, flags: HyperFileFlags, mode: HyperFileMode) -> Result<Self>
    {
        let meta_config = config.meta.clone();

        let bmap = BMap::<BlockIndex, BlockPtr, BlockPtr, L>::new(meta_config.root_size, meta_config.meta_block_size, meta_block_loader);
        let bmap_ud = BMapUserData::new(BlockPtrFormat::MicroGroup);
        bmap.set_userdata(bmap_ud.as_u32());

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
            cache: MemCache::new(data_cache_blocks, config.meta.data_block_size),
            inode: inode,
            config: config,
            max_dirty_blocks: max_dirty_blocks,
            flags: flags,
            last_flush: Instant::now(),
            sema: Arc::new(Semaphore::new(permits)),
            flush_lock: Arc::new(Mutex::new(())),
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
    pub async fn open(staging: T, meta_block_loader: L, config: HyperFileConfig, flags: HyperFileFlags) -> Result<Self>
    {
        Self::do_open(staging, meta_block_loader, config, flags, 0).await
    }

    /// open a hyper file with cno for read-only
    pub async fn open_cno(staging: T, meta_block_loader: L, config: HyperFileConfig, flags: HyperFileFlags, cno: u64) -> Result<Self>
    {
        if !flags.is_rdonly() {
            return Err(Error::new(ErrorKind::ReadOnlyFilesystem, "write access is not allowed for open specific cno"));
        }
        Self::do_open(staging, meta_block_loader, config, flags, cno).await
    }

    async fn do_open(staging: T, meta_block_loader: L, mut config: HyperFileConfig, flags: HyperFileFlags, cno: u64) -> Result<Self>
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
        let bmap = BMap::<BlockIndex, BlockPtr, BlockPtr, L>::read(&b, meta_config.meta_block_size, meta_block_loader);
        let bmap_ud = BMapUserData::from_u32(bmap.get_userdata());

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
            cache: MemCache::new(data_cache_blocks, config.meta.data_block_size),
            inode: inode,
            config: config,
            max_dirty_blocks: max_dirty_blocks,
            flags: flags,
            last_flush: Instant::now(),
            sema: Arc::new(Semaphore::new(permits)),
            flush_lock: Arc::new(Mutex::new(())),
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
        #[cfg(feature = "wal")]
        let Ok(_) = self.flush_lock.try_lock() else {
            return Err(Error::new(ErrorKind::ResourceBusy, "flush is in-progress"));
        };
        #[cfg(feature = "reactor")]
        if let Some(rt) = self.rt.take() {
            rt.shutdown_background();
        }
        self.flush().await
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

        let data_block_size = self.config.meta.data_block_size;
        let buf_len = buf.len();
        let blk_idx = (off / data_block_size) as BlockIndex;
        let blk_off = off % data_block_size;
        let blk_count = (buf_len + data_block_size - 1) / data_block_size;
        debug!("READ - block index {blk_idx}, block offset {blk_off}, block count {blk_count}");
        let mut bytes_read = 0;

        let blk_iter = BlockIndexIter::new(off, buf_len, data_block_size);
        let mut next_slice = buf;
        for (blk_idx, off, len) in blk_iter {
            let (this, next) = next_slice.split_at_mut(len);
            if let Some(block) = self.cache.get(&blk_idx) {
                // fast path check on dirty list
                debug!("      - BlockIndex {blk_idx} CACHE HIT");
                block.copy_out(off, this);
                block.unlock();
            } else {
                debug!("      - lookup BlockIndex {blk_idx} for BlockPtr");
                let blk_ptr = self.bmap.lookup(&blk_idx).await
                                        .or_else(|e| {
                                            // translate NotFound -> zero block from bmap
                                            if e.kind() == ErrorKind::NotFound {
                                                return Ok(BlockPtrFormat::new_zero_block());
                                            }
                                            warn!("READ - lookup bmap for block index {blk_idx} error: {}", e);
                                            Err(e)
                                        })?;
                debug!("      - load data block for block ptr {} at offset {} len {}", self.blk_ptr_decode_display(&blk_ptr), off, this.len());
                let _ = self.load_data_block_read_path(blk_idx, blk_ptr, off, this).await?;
            }
            bytes_read += this.len();
            next_slice = next;
        }

        let _ = fn_start;

        self.inode.update_atime();
        Ok(bytes_read)
    }

    pub async fn write(&mut self, off: usize, buf: &[u8]) -> Result<usize> {
        let permit = self.sema.clone().acquire_owned().await.unwrap();
        let fn_start = Instant::now();
        let len = buf.len();
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
        for (blk_idx, _, _) in blk_iter {
            // force bmap update for dirty blocks
            let _ = self.bmap.insert(blk_idx, BlockPtrFormat::dummy_value()).await.expect("failed to insert dummy value to bmap for dirty blocks");
        }

        let oldsize = self.inode.size();
        if off + len > oldsize {
            self.inode.set_size(off + len);
        }
        self.inode.update_mtime();
        // TODO: rollback to old size if flush failed
        drop(permit);

        let _flushed = self.try_flush().await?;
        let _ = fn_start;
        Ok(bytes_write)
    }

    pub async fn write_zero(&mut self, off: usize, len: usize) -> Result<usize> {
        let permit = self.sema.clone().acquire_owned().await.unwrap();
        let fn_start = Instant::now();
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

        let data_block_size = self.config.meta.data_block_size;
        let oldsize = self.inode.size();
        let blk_iter = BlockIndexIter::new(off, len, data_block_size);
        for (blk_idx, start_off, data_len) in blk_iter {
            // for a complete block,
            // no need to update data in cache, because is's already all zero
            // and insert zero block into block map
            if start_off == 0 && data_len == data_block_size {
                // insert or update
                let _ = self.bmap.insert(blk_idx, BlockPtrFormat::new_zero_block()).await.expect("failed to insert new zero to bmap");
                bytes_write += data_len;
                let _ = self.cache.remove(&blk_idx);
                continue;
            }
            // for a incomplete block
            // last block execption which start off from block start and len exceed current file
            // TODO: merge this with new cache impl
            if start_off == 0 && (blk_idx as usize * data_block_size) + start_off + data_len > oldsize {
                // insert or update
                let _ = self.bmap.insert(blk_idx, BlockPtrFormat::new_zero_block()).await.expect("failed to insert new zero to bmap");
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
            let _ = self.bmap.insert(blk_idx, BlockPtrFormat::dummy_value()).await.expect("failed to insert dummy value to bmap for dirty blocks");
            bytes_write += data_len;
        }

        let oldsize = self.inode.size();
        if off + len > oldsize {
            self.inode.set_size(off + len);
        }
        self.inode.update_mtime();
        // TODO: rollback to old size if flush failed
        drop(permit);

        let _flushed = self.try_flush().await?;
        let _ = fn_start;
        Ok(bytes_write)
    }

    // write in batch style, all blocks in input vec should be full block
    pub(crate) async fn write_aligned_batch(&mut self, mut blocks: Vec<AlignedDataBlockWrapper>) -> Result<usize> {
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
        for block_wrapper in blocks.iter() {
            let blk_idx = block_wrapper.index();
            let blk_sz = block_wrapper.size();
            assert!(blk_sz == data_block_size);
            if block_wrapper.is_zero() {
                let _ = self.cache.remove(&blk_idx);
                bytes_write += blk_sz;
                let _ = self.bmap.insert(blk_idx, BlockPtrFormat::new_zero_block()).await.expect("failed to insert new zero to bmap");
                continue;
            }
            self.update_cache(blk_idx, 0, block_wrapper.as_slice());
            bytes_write += blk_sz;
            // force bmap update for dirty blocks
            let _ = self.bmap.insert(blk_idx, BlockPtrFormat::dummy_value()).await.expect("failed to insert dummy value to bmap for dirty blocks");
        }
        // try update file size by offset and len from last block
        let last_block_wrapper = blocks.last().expect("unable to get last block, input blocks is empty");
        let oldsize = self.inode.size();
        let off = (last_block_wrapper.index() as usize) * data_block_size;
        let len = last_block_wrapper.size();
        if off + len > oldsize {
            self.inode.set_size(off + len);
        }
        self.inode.update_mtime();
        drop(permit);
        let _flushed = self.try_flush().await?;
        Ok(bytes_write)
    }

    pub(crate) fn need_flush(&self) -> bool {
        // check if dirty data bytes exceed segment buffer threshold
        let ndatadirty = self.cache.dirty_count();
        let data_block_size = self.config.meta.data_block_size;
        // trigger flush because we meet memory threshold
        let threshold_flush = ndatadirty > self.max_dirty_blocks
            || (ndatadirty * data_block_size) > self.config.runtime.segment_buffer_size;
        // trigger flush if file opened in O_SYNC
        let sync_flush = self.flags.is_sync();
        let sync_flush = if sync_flush {
            #[cfg(not(feature = "wal"))]
            { true }
            // in wal mode, we dont's need immediately flush
            #[cfg(feature = "wal")]
            { false }
        } else {
            false
        };

        // trigger flush if file opened in O_DIRECT
        let direct_flush = self.flags.is_direct();
        let direct_flush = if direct_flush {
            #[cfg(not(feature = "wal"))]
            { true }
            // in wal mode, we dont's need immediately flush
            #[cfg(feature = "wal")]
            { false }
        } else {
            false
        };

        let max_flush_interval = self.config.runtime.data_cache_dirty_max_flush_interval;
        let last_flush_expired = self.last_flush.elapsed() >= Duration::from_millis(max_flush_interval);
        if last_flush_expired || threshold_flush || sync_flush || direct_flush {
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
    pub(crate) async fn kick_wal_protected_flush_reactor(&mut self, fh: TaskHandler<FileContext<'a>>) -> Result<SegmentId> {
        let Ok(lock) = self.flush_lock.clone().try_lock_owned() else {
            // FIXME: skip this flush by return ResourceBusy for now,
            // should this flush be re-queue?
            return Err(Error::new(ErrorKind::ResourceBusy, "another flush is in-progress"));
        };
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
        // restore cache limit
        self.restore_data_blocks_cache_limit();
        self.bmap_set_cache_limit(bmap_cache_limit);
        self.set_last_flush();
        drop(lock);
    }

    // starting wal flush recovery process by reloading inode from backend storage
    // everything should be clean or give a panic if unrecoverable
    #[cfg(feature = "wal")]
    pub(crate) async fn wal_flush_recovery(&mut self, lock: OwnedMutexGuard<()>) -> Result<SegmentId> {
        debug!("wal_flush_recovery - started");
        match self.do_wal_flush_recovery(lock).await {
            Ok(cno) => {
                if cno != 0 { return Ok(cno); }
                warn!("wal_flush_recovery - return with cno 0");
            },
            Err(e) => {
                warn!("wal_flush_recovery - return with err {}", e);
            },
        }
        panic!("wal_flush_recovery - failed, please fix wal with offline tools");
    }

    #[cfg(feature = "wal")]
    async fn do_wal_flush_recovery(&mut self, lock: OwnedMutexGuard<()>) -> Result<SegmentId> {
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

        drop(lock);
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
        for (_, chunk) in map.iter() {
            if chunk.is_zero {
                let _ = self.write_zero(chunk.offset, chunk.len).await?;
            } else {
                let data = wal.as_ref().unwrap().read(chunk.seq, chunk.segid, chunk.offset, chunk.len).await?;
                let _ = self.write(chunk.offset, &data).await?;
            }
        }

        // install wal back
        self.wal = wal;

        // force flush
        self.flush_process().await
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
        let block = DataBlock::new(*blk_idx, self.config.meta.data_block_size);
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
            self.inode.update_mtime();
            if data_changed {
                debug!("truncate - data changed, trigger flush");
            } else {
                debug!("truncate - no bmap and data changed, update file attr only");
            }
            drop(permit);
            self.flush().await?;
            return Ok(());
        }

        // if need to extend file length
        if tgt_blk_idx > cur_blk_idx {
            // no need to modify bmap, just update new file size
            self.inode.set_size(new_size);
            self.inode.update_mtime();
            debug!("truncate - extend file size with no bmap change, update file attr only");
            drop(permit);
            self.flush().await?;
            return Ok(());
        }

        debug!("truncate - shrink bmap to BlockIndex {}", tgt_blk_idx);
        // re-calc tgt_blk_idx for bmap truncate
        let tgt_blk_idx = ((new_size + data_block_size - 1) / data_block_size) as BlockIndex;
        // if need to shrink bmap
        if let Err(e) = self.bmap.truncate(&tgt_blk_idx).await {
            if e.kind() != ErrorKind::NotFound {
                return Err(e);
            }
            // NotFound is fine, let's continue
        }
        if new_size > 0 {
            let tgt_blk_idx = tgt_blk_idx - 1;
            let _ = self.truncate_last_data_block(&tgt_blk_idx, offset_to_discard).await?;
        }
        self.inode.set_size(new_size);
        self.inode.update_mtime();
        drop(permit);
        self.flush().await?;
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
}

impl<'a: 'static, T: Staging<L> + SegmentReadWrite + Send + Clone + 'static, L: BlockLoader<BlockPtr> + Clone + 'static> HyperFile<'a, T, L> {
    // we only care about incomplete blocks and not in dirty list
    // return:
    //   - vec of data block ptr we need to retrieve
    pub(crate) fn write_prepare(&mut self, off: usize, len: usize) -> Vec<BlockIndex> {
        self.cache.write_prepare(off, len)
    }

    // test if block of index need to be retrieve
    #[inline]
    pub(crate) fn write_prepare_block_index(&mut self, blk_idx: &BlockIndex) -> bool {
        self.cache.contains(blk_idx)
    }

    async fn write_retrieve(&mut self, list: Vec<BlockIndex>) -> Result<Vec<DataBlock>> {
        let mut output = Vec::new();
        let data_block_size = self.config.meta.data_block_size;
        for blk_idx in list {
            match self.bmap.lookup(&blk_idx).await {
                Ok(blk_ptr) => {
                    debug!("retrive block ptr {} for block index {}", self.blk_ptr_decode_display(&blk_ptr), blk_idx);
                    let mut block = DataBlock::new(blk_idx, data_block_size);
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
                    let mut block = DataBlock::new(blk_idx, data_block_size);
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
        debug!("load_data_block - block ptr: {}", self.blk_ptr_decode_display(&blk_ptr));
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
            panic!("incorrect block ptr {} to load", self.blk_ptr_decode_display(&blk_ptr));
        }
    }

    async fn load_data_block_write_path(&mut self, blk_idx: BlockIndex, blk_ptr: BlockPtr, offset: usize, buf: &mut [u8]) -> Result<()> {
        debug!("load_data_block - block ptr: {}", self.blk_ptr_decode_display(&blk_ptr));
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
            panic!("incorrect block ptr {} to load", self.blk_ptr_decode_display(&blk_ptr));
        }
    }

    // return max dirty data blocks can hold
    fn calc_max_dirty_blocks(data_block_size: usize, max_dirty_bytes_threshold: usize, max_dirty_blocks_threshold: usize) -> usize {
        let max_blocks = max_dirty_bytes_threshold / data_block_size;
        std::cmp::max(max_blocks, max_dirty_blocks_threshold)
    }
}

impl<'a: 'static, T: Staging<L> + SegmentReadWrite + Send + Clone + 'static, L: BlockLoader<BlockPtr> + Clone + 'static> HyperFile<'a, T, L> {
    // write in batch style, input blocks could be incomplete
    pub async fn write_batch(&mut self, blocks: Vec<BatchDataBlockWrapper>) -> Result<usize> {
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
                    let mut block = DataBlock::new(blk_idx, data_block_size);
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
                    let mut block = DataBlock::new(blk_idx, data_block_size);
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
                let _ = self.bmap.insert(*blk_idx, BlockPtrFormat::dummy_value()).await.expect("failed to insert dummy value to bmap for dirty blocks");
                continue;
            }
            // is full block
            assert!(v_blocks.len() == 1);
            let block_wrapper = &v_blocks[0];
            let blk_sz = block_wrapper.size();
            if block_wrapper.is_zero() {
                let _ = self.cache.remove(&blk_idx);
                bytes_write += blk_sz;
                let _ = self.bmap.insert(*blk_idx, BlockPtrFormat::new_zero_block()).await.expect("failed to insert new zero to bmap");
                continue;
            }
            self.update_cache(*blk_idx, 0, block_wrapper.as_slice());
            bytes_write += blk_sz;
            // force bmap update for dirty blocks
            let _ = self.bmap.insert(*blk_idx, BlockPtrFormat::dummy_value()).await.expect("failed to insert dummy value to bmap for dirty blocks");
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
        }
        self.inode.update_mtime();
        drop(permit);
        let _flushed = self.try_flush().await?;
        Ok(bytes_write)
    }
}

impl<T, L> HyperTrait<T, L, BlockPtr> for HyperFile<'_, T, L>
    where
        T: Staging<L> + SegmentReadWrite + Send + Clone + 'static,
        L: BlockLoader<BlockPtr> + Clone,
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
        self.flush_lock.clone().lock_owned().await
    }

    fn flush_unlock(&self, lock: OwnedMutexGuard<()>) {
        drop(lock);
    }

    fn bmap_as_slice(&self) -> &[u8] {
        self.bmap.as_slice()
    }

    fn bmap_get_block_loader(&self) -> L {
        self.bmap.get_block_loader()
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

    fn bmap_update(&mut self, bmap: BMap<'_, BlockIndex, BlockPtr, BlockPtr, L>) {
        *&mut self.bmap = unsafe {
            std::mem::transmute::<BMap<'_, BlockIndex, BlockPtr, BlockPtr, L>, BMap<'_, BlockIndex, BlockPtr, BlockPtr, L>>(bmap)
        };
    }

    async fn bmap_insert_dummy_value(bmap: &mut BMap<'_, BlockIndex, BlockPtr, BlockPtr, L>, blk_idx: &BlockIndex) -> Result<Option<BlockPtr>> {
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
        self.last_flush = Instant::now();
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
}
