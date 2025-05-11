use std::fmt;
use std::collections::{HashMap, BTreeMap};
use std::sync::Arc;
use std::time::{Instant, Duration};
use std::io::{Error, ErrorKind, Result};
use log::{debug, warn};
use lru::LruCache;
use btree_ondisk::{bmap::BMap, BlockLoader};
use tokio::sync::{Semaphore, OwnedSemaphorePermit};
use crate::{BlockIndex, BlockPtr, BlockIndexIter, SegmentId, SegmentOffset, BMapUserData};
use crate::meta_format::BlockPtrFormat;
use crate::buffer::{DataBlock, AlignedDataBlockWrapper, BatchDataBlockWrapper};
use crate::staging::{StagingIntercept, Staging, config::StagingConfig};
use crate::segment::SegmentReadWrite;
use crate::ondisk::{InodeRaw, BMapRawType};
use crate::inode::{Inode, FlushInodeFlag};
use crate::config::{HyperFileConfig, HyperFileMetaConfig};
use super::flags::HyperFileFlags;
use super::{HyperTrait, DirtyDataBlocks};

pub struct HyperFile<'a, T, L: BlockLoader<BlockPtr>> {
    pub(crate) staging: T,
    pub(crate) bmap: BMap<'a, BlockIndex, BlockPtr, BlockPtr, L>,
    pub(crate) bmap_ud: BMapUserData,
    // NOTE:
    //   1) dirty list is higher priority than cache list
    //   2) data cache only intend to cache incomplete block access
    pub(crate) data_blocks_cache: LruCache<BlockIndex, DataBlock>,
    pub(crate) data_blocks_dirty: BTreeMap<BlockIndex, DataBlock>, // index by block uid
    pub(crate) inode: Inode,
    pub(crate) config: HyperFileConfig,
    pub(crate) max_dirty_blocks: usize,
    pub(crate) data_cache_blocks: usize,
    pub(crate) flags: HyperFileFlags,
    pub(crate) last_flush: Instant,
    pub(crate) sema: Arc<Semaphore>,
    #[cfg(feature = "reactor")]
    pub(crate) spawn_write_permit: Option<OwnedSemaphorePermit>, // hold owned permit for spawn_write
    #[cfg(feature = "reactor")]
    pub(crate) rt: Option<tokio::runtime::Runtime>,
}

impl<T, L: BlockLoader<BlockPtr>> fmt::Display for HyperFile<'_, T, L> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(f, "==== dump HyperFile ====")?;
        writeln!(f, "  {:?}", self.config)?;
        writeln!(f, "  max dirty blocks: {}, data dirty size: {}", self.max_dirty_blocks, self.data_blocks_dirty.len())?;
        writeln!(f, "  data cache blocks: {}, data cache size: {}", self.data_cache_blocks, self.data_blocks_cache.len())?;
        writeln!(f, "  {}", self.inode)
    }
}

impl<T, L: BlockLoader<BlockPtr>> Drop for HyperFile<'_, T, L> {
    fn drop(&mut self) {
        #[cfg(feature = "reactor")]
        if let Some(rt) = self.rt.take() {
            rt.shutdown_background();
        }
    }
}

impl<'a: 'static, T: Staging<T, L> + SegmentReadWrite + 'static, L: BlockLoader<BlockPtr> + Clone + 'static> HyperFile<'a, T, L> {
    pub async fn new(staging: T, meta_block_loader: L, config: HyperFileConfig, flags: HyperFileFlags) -> Result<Self>
    {
        let meta_config = config.meta.clone();

        let bmap = BMap::<BlockIndex, BlockPtr, BlockPtr, L>::new(meta_config.root_size, meta_config.meta_block_size, meta_block_loader);
        let bmap_ud = BMapUserData::new(BlockPtrFormat::MicroGroup);
        bmap.set_userdata(bmap_ud.as_u32());

        let inode = Inode::default_file().with_meta_config(&meta_config);
        let max_dirty_blocks = Self::calc_max_dirty_blocks(meta_config.data_block_size,
            config.runtime.data_cache_dirty_max_bytes_threshold,
            config.runtime.data_cache_dirty_max_blocks_threshold);

        let permits = if flags.is_rdonly() {
            Semaphore::MAX_PERMITS
        } else {
            1
        };

        let data_cache_blocks = if flags.is_direct() {
            0
        } else {
            config.runtime.data_cache_blocks
        };

        let mut file = Self {
            staging: staging,
            bmap: bmap,
            bmap_ud: bmap_ud,
            data_blocks_cache: LruCache::new(std::num::NonZeroUsize::new(data_cache_blocks).unwrap()),
            data_blocks_dirty: BTreeMap::new(),
            inode: inode,
            config: config,
            max_dirty_blocks: max_dirty_blocks,
            data_cache_blocks: data_cache_blocks,
            flags: flags,
            last_flush: Instant::now(),
            sema: Arc::new(Semaphore::new(permits)),
            #[cfg(feature = "reactor")]
            spawn_write_permit: None,
            #[cfg(feature = "reactor")]
            rt: Some(tokio::runtime::Runtime::new().unwrap()),
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
                if e.kind() == ErrorKind::NotFound {
                    return Self::new(staging, meta_block_loader, config, flags).await;
                }
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
            1
        };

        let data_cache_blocks = if flags.is_direct() {
            0
        } else {
            config.runtime.data_cache_blocks
        };

        // overwrite the default meta config with the one we get from inode
        config.meta = meta_config;

        let mut file = Self {
            staging: staging,
            bmap: bmap,
            bmap_ud: bmap_ud,
            data_blocks_cache: LruCache::new(std::num::NonZeroUsize::new(data_cache_blocks).unwrap()),
            data_blocks_dirty: BTreeMap::new(),
            inode: Inode::from_raw(&raw_inode, inode_state),
            config: config,
            max_dirty_blocks: max_dirty_blocks,
            data_cache_blocks: data_cache_blocks,
            flags: flags,
            last_flush: Instant::now(),
            sema: Arc::new(Semaphore::new(permits)),
            #[cfg(feature = "reactor")]
            spawn_write_permit: None,
            #[cfg(feature = "reactor")]
            rt: Some(tokio::runtime::Runtime::new().unwrap()),
        };
        // refresh bmap if need to do recovery
        let _ = file.refresh_bmap().await?;
        Ok(file)
    }

    pub async fn release(&mut self) -> Result<SegmentId> {
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
        let blksize = self.config.meta.data_block_size;
        self.inode.to_stat(dev, rdev, blksize)
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
            if let Some(block) = self.data_blocks_dirty.get(&blk_idx) {
                // fast path check on dirty list
                debug!("      - BlockIndex {blk_idx} HIT on data_blocks_dirty list");
                block.copy_out(off, this);
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
            let None = self.data_blocks_dirty.insert(blk_idx, block) else {
                panic!("BlockIndex {} already on data_blocks_dirty list", blk_idx);
            };
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
            let _ = self.bmap.insert(blk_idx, BlockPtrFormat::dummy_value()).await?;
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
            let None = self.data_blocks_dirty.insert(blk_idx, block) else {
                panic!("BlockIndex {} already on data_blocks_dirty list", blk_idx);
            };
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
                let _ = self.bmap.insert(blk_idx, BlockPtrFormat::new_zero_block()).await?;
                bytes_write += data_len;
                let _ = self.data_blocks_dirty.remove(&blk_idx);
                continue;
            }
            // for a incomplete block
            // last block execption which start off from block start and len exceed current file
            // TODO: merge this with new cache impl
            if start_off == 0 && (blk_idx as usize * data_block_size) + start_off + data_len > oldsize {
                // insert or update
                let _ = self.bmap.insert(blk_idx, BlockPtrFormat::new_zero_block()).await?;
                bytes_write += data_len;
                let _ = self.data_blocks_dirty.remove(&blk_idx);
                continue;
            }
            // update cache data with zero
            debug!("      - update cache block index {}, offset {}, len {}", blk_idx, start_off, data_len);
            let mut zero = Vec::with_capacity(data_len);
            zero.resize(data_len, 0);
            self.update_cache(blk_idx, start_off, &zero);
            // force bmap update for dirty blocks
            let _ = self.bmap.insert(blk_idx, BlockPtrFormat::dummy_value()).await?;
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
                let _ = self.data_blocks_dirty.remove(&blk_idx);
                bytes_write += blk_sz;
                let _ = self.bmap.insert(blk_idx, BlockPtrFormat::new_zero_block()).await?;
                continue;
            }
            if let Some(block) = self.data_blocks_dirty.get_mut(&blk_idx) {
                // found in dirty list, just update it's content
                block.copy(0, block_wrapper.as_slice());
                bytes_write += blk_sz;
            } else {
                // can't found in dirty list, create a new one
                let mut block = DataBlock::new(blk_idx, data_block_size);
                block.copy(0, block_wrapper.as_slice());
                self.data_blocks_dirty.insert(blk_idx, block);
                bytes_write += blk_sz;
            }
            // force bmap update for dirty blocks
            let _ = self.bmap.insert(blk_idx, BlockPtrFormat::dummy_value()).await?;
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

    // try flush out dirty data if all threshold condition meet
    pub(crate) async fn try_flush(&mut self) -> Result<bool> {
        // check if dirty data bytes exceed segment buffer threshold
        let ndatadirty = self.data_blocks_dirty.len();
        let data_block_size = self.config.meta.data_block_size;
        // trigger flush because we meet memory threshold
        let threshold_flush = ndatadirty > self.max_dirty_blocks
            || (ndatadirty * data_block_size) > self.config.runtime.segment_buffer_size;
        // trigger flush if file opened in O_SYNC
        let sync_flush = self.flags.is_sync();
        // trigger flush if file opened in O_DIRECT
        let direct_flush = self.flags.is_direct();
        let max_flush_interval = self.config.runtime.data_cache_dirty_max_flush_interval;
        let last_flush_expired = self.last_flush.elapsed() >= Duration::from_millis(max_flush_interval);
        if last_flush_expired || threshold_flush || sync_flush || direct_flush {
            let _ = self.flush().await?;
            return Ok(true);
        }
        Ok(false)
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
        if let Some(block) = self.data_blocks_dirty.get_mut(&blk_idx) {
            let buf = block.as_mut_slice();
            let (_, to_clear) = buf.split_at_mut(offset_to_discard);
            to_clear.fill(0);
            debug!("truncate_last_data_block - data block in dirty list, data cleared");
            return Ok(true);
        }
        if let Some(block) = (self.data_cache_blocks > 0).then(|| self.data_blocks_cache.pop(&blk_idx)).unwrap() {
            let buf = block.as_mut_slice();
            let (_, to_clear) = buf.split_at_mut(offset_to_discard);
            to_clear.fill(0);
            debug!("truncate_last_data_block - data block in cache list, data cleared");
            // move data block into dirty list
            self.data_blocks_dirty.insert(*blk_idx, block);
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
        self.data_blocks_dirty.insert(*blk_idx, block);
        let _ = self.bmap.insert(*blk_idx, BlockPtrFormat::dummy_value()).await?;
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
        self.inode.get_last_ondisk_cno()
    }

    pub fn staging_config(&self) -> &StagingConfig {
        &self.config.staging
    }

    pub fn staging_interceptor(&mut self, i: impl StagingIntercept<T> + 'static) {
        self.staging.interceptor(i);
    }
}

impl<'a: 'static, T: Staging<T, L> + SegmentReadWrite + 'static, L: BlockLoader<BlockPtr> + Clone + 'static> HyperFile<'a, T, L> {
    // we only care about incomplete blocks and not in dirty list
    // return:
    //   - vec of data block ptr we need to retrieve
    pub(crate) fn write_prepare(&mut self, off: usize, len: usize) -> Vec<BlockIndex> {
        let mut output = Vec::new();
        let data_block_size = self.config.meta.data_block_size;
        let blk_iter = BlockIndexIter::new(off, len, data_block_size);
        debug!("start to write prepare for write offset {}, len {}", off, len);
        for (blk_idx, start_off, data_len) in blk_iter {
            // for a complete block, we don't need to retrieve
            if start_off == 0 && data_len == data_block_size {
                continue;
            }
            // for incomplete block
            if self.data_blocks_dirty.contains_key(&blk_idx) {
                // incomplete block but already in dirty list
                continue;
            }
            if let Some(block) = (self.data_cache_blocks > 0).then(|| self.data_blocks_cache.pop(&blk_idx)).unwrap() {
                // incomplete block found in data blocks cache
                self.data_blocks_dirty.insert(blk_idx, block);
                continue;
            }
            // incomplete block and not in both dirty and cache list
            output.push(blk_idx);
        }
        debug!("end of write prepare {} of blocks need to be retrieve", output.len());
        output
    }

    // test if block of index need to be retrieve
    #[inline]
    pub(crate) fn write_prepare_block_index(&mut self, blk_idx: &BlockIndex) -> bool {
        if self.data_blocks_dirty.contains_key(blk_idx) {
            return false;
        }
        if let Some(block) = (self.data_cache_blocks > 0).then(|| self.data_blocks_cache.pop(blk_idx)).unwrap() {
            self.data_blocks_dirty.insert(*blk_idx, block);
            return false;
        }
        true
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
        let data_block_size = self.config.meta.data_block_size;
        if let Some(block) = self.data_blocks_dirty.get_mut(&blk_idx) {
            // found in dirty list, just update it's content
            block.copy(off, buf);
        } else {
            // can't found in dirty list, create a new one
            let mut block = DataBlock::new(blk_idx, data_block_size);
            block.copy(off, buf);
            self.data_blocks_dirty.insert(blk_idx, block);
        }
    }

    async fn load_data_block_read_path(&mut self, blk_idx: BlockIndex, blk_ptr: BlockPtr, offset: usize, buf: &mut [u8]) -> Result<()> {
        debug!("load_data_block - block ptr: {}", self.blk_ptr_decode_display(&blk_ptr));
        // in read path we would check both data and dirty cache before do real data load
        // check dirty cache
        if let Some(block) = self.data_blocks_dirty.get(&blk_idx) {
            // cache hit
            debug!("load_data_block - Cache Hit for block index: {}", blk_idx);
            let slice = block.as_slice();
            buf.copy_from_slice(&slice[offset..offset + buf.len()]);
            return Ok(());
        }
        // check data cache
        if let Some(block) = (self.data_cache_blocks > 0).then(|| self.data_blocks_cache.get(&blk_idx)).unwrap() {
            // cache hit
            debug!("load_data_block - Cache Hit for block index: {}", blk_idx);
            let slice = block.as_slice();
            buf.copy_from_slice(&slice[offset..offset + buf.len()]);
            return Ok(());
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

    async fn load_data_block_write_path(&self, blk_idx: BlockIndex, blk_ptr: BlockPtr, offset: usize, buf: &mut [u8]) -> Result<()> {
        debug!("load_data_block - block ptr: {}", self.blk_ptr_decode_display(&blk_ptr));
        if BlockPtrFormat::is_on_staging(&blk_ptr) {
            let (segid, staging_off) = self.blk_ptr_decode(&blk_ptr);
            let _ = self.staging.load_data_block(segid, staging_off, offset, self.config.meta.data_block_size, buf).await?;
            return Ok(());
        } else if BlockPtrFormat::is_dummy_value(&blk_ptr) {
            if let Some(block) = self.data_blocks_dirty.get(&blk_idx) {
                // cache hit
                debug!("load_data_block - Cache Hit for block index: {}", blk_idx);
                let slice = block.as_slice();
                buf.copy_from_slice(&slice[offset..offset + buf.len()]);
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

impl<'a: 'static, T: Staging<T, L> + SegmentReadWrite + Send + Clone + 'static, L: BlockLoader<BlockPtr> + Clone + 'static> HyperFile<'a, T, L> {
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
        while let Some(j) = joins.pop() {
            let _ = j.await.unwrap();
        }

        // insert fetched data blocks into dirty list
        for block in fetched.into_iter() {
            let blk_idx = block.index();
            let None = self.data_blocks_dirty.insert(blk_idx, block) else {
                panic!("BlockIndex {} already on data_blocks_dirty list", blk_idx);
            };
        }

        for (blk_idx, (is_full_block, v_blocks)) in merged.iter() {
            if !is_full_block {
                // if not a full block, playback all partial data blocks
                let block = self.data_blocks_dirty.get_mut(&blk_idx).expect("failed to get back data block from dirty list");
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
                let _ = self.bmap.insert(*blk_idx, BlockPtrFormat::dummy_value()).await?;
                continue;
            }
            // is full block
            assert!(v_blocks.len() == 1);
            let block_wrapper = &v_blocks[0];
            let blk_sz = block_wrapper.size();
            if block_wrapper.is_zero() {
                let _ = self.data_blocks_dirty.remove(&blk_idx);
                bytes_write += blk_sz;
                let _ = self.bmap.insert(*blk_idx, BlockPtrFormat::new_zero_block()).await?;
                continue;
            }
            if let Some(block) = self.data_blocks_dirty.get_mut(&blk_idx) {
                // found in dirty list, just update it's content
                block.copy(0, block_wrapper.as_slice());
                bytes_write += blk_sz;
            } else {
                // can't found in dirty list, create a new one
                let mut block = DataBlock::new(*blk_idx, data_block_size);
                block.copy(0, block_wrapper.as_slice());
                self.data_blocks_dirty.insert(*blk_idx, block);
                bytes_write += blk_sz;
            }
            // force bmap update for dirty blocks
            let _ = self.bmap.insert(*blk_idx, BlockPtrFormat::dummy_value()).await?;
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

impl<'a, T, L> HyperTrait<'a, T, L> for HyperFile<'a, T, L>
    where
        T: Staging<T, L> + SegmentReadWrite,
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
        if self.data_cache_blocks > 0 {
            self.data_blocks_cache.clear();
        }
    }

    fn get_data_blocks_dirty(&self) -> DirtyDataBlocks<'_> {
        let b: BTreeMap<BlockIndex, &DataBlock> = self.data_blocks_dirty.iter()
                        .map(|(idx, blk)| (*idx, blk))
                        .collect();
        DirtyDataBlocks { inner: Some(b), owned: None }
    }

    fn clear_data_blocks_dirty(&mut self) {
        while let Some((blk_idx, block)) = self.data_blocks_dirty.pop_first() {
            if !block.is_should_cache() {
                continue;
            }
            // keep block that should cache into cache list
            if let Some(_) = (self.data_cache_blocks > 0).then(|| self.data_blocks_cache.put(blk_idx, block)).unwrap() {
                panic!("block already exists, failed to put back block index {} into data blocks cache", blk_idx);
            }
        }
    }

    async fn lock(&self) -> OwnedSemaphorePermit {
        let permit = self.sema.clone().acquire_owned().await.unwrap();
        permit
    }

    fn unlock(&self, permit: OwnedSemaphorePermit) {
        drop(permit);
    }

    fn bmap(&self) -> &BMap<'a, BlockIndex, BlockPtr, BlockPtr, L> {
        &self.bmap
    }

    fn bmap_mut(&mut self) -> &mut BMap<'a, BlockIndex, BlockPtr, BlockPtr, L> {
        &mut self.bmap
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

    fn inode_mut(&mut self) -> &mut Inode {
        &mut self.inode
    }

    async fn sleep(dur: Duration) {
        tokio::time::sleep(dur).await;
    }
}
