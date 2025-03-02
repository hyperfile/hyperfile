use std::fmt;
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Instant, Duration};
use std::io::{ErrorKind, Result};
use log::{debug, warn};
use aws_sdk_s3::Client;
use btree_ondisk::{bmap::BMap, BlockLoader};
use tokio::sync::{Semaphore, OwnedSemaphorePermit};
use crate::{BlockIndex, BlockPtr, BlockIndexIter, SegmentId, SegmentOffset, BMapUserData};
use crate::meta_format::BlockPtrFormat;
use crate::buffer::Block;
use crate::staging::{StagingIntercept, Staging, config::StagingConfig};
use crate::segment::SegmentReadWrite;
use crate::ondisk::{InodeRaw, BMapRawType};
use crate::inode::{Inode, OnDiskState, FlushInodeFlag};
use crate::config::{HyperFileConfig, HyperFileMetaConfig};
use super::flags::HyperFileFlags;
use super::{HyperTrait, DirtyDataBlocks};

pub struct HyperFile<'a, T, L: BlockLoader<BlockPtr>> {
    pub(crate) client: Client,
    pub(crate) staging: T,
    pub(crate) bmap: BMap<'a, BlockIndex, BlockPtr, L>,
    pub(crate) bmap_ud: BMapUserData,
    pub(crate) data_blocks_dirty: BTreeMap<BlockIndex, Block>, // index by block uid
    pub(crate) inode: Inode,
    pub(crate) config: HyperFileConfig,
    pub(crate) max_dirty_blocks: usize,
    pub(crate) flags: HyperFileFlags,
    pub(crate) last_flush: Instant,
    pub(crate) sema: Arc<Semaphore>,
    pub(crate) spawn_write_permit: Option<OwnedSemaphorePermit>, // hold owned permit for spawn_write
}

impl<T, L: BlockLoader<BlockPtr>> fmt::Display for HyperFile<'_, T, L> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(f, "==== dump HyperFile ====")?;
        writeln!(f, "  {:?}", self.config)?;
        writeln!(f, "  max dirty blocks: {}", self.max_dirty_blocks)?;
        writeln!(f, "  data dirty size: {}", self.data_blocks_dirty.len())?;
        writeln!(f, "  {}", self.inode)
    }
}

impl<'a: 'static, T: Staging<T, L> + SegmentReadWrite + 'static, L: BlockLoader<BlockPtr> + Clone + 'static> HyperFile<'a, T, L> {
    pub async fn new(client: Client, staging: T, meta_block_loader: L, config: HyperFileConfig, flags: HyperFileFlags) -> Result<Self>
    {
        let meta_config = config.meta.clone();

        let mut bmap = BMap::<BlockIndex, BlockPtr, L>::new(meta_config.root_size, meta_config.meta_block_size, meta_block_loader);
        let bmap_ud = BMapUserData::new(BlockPtrFormat::MicroGroup);
        bmap.set_userdata(bmap_ud.as_u32());

        let inode = Inode::default_file();
        let max_dirty_blocks = Self::calc_max_dirty_blocks(meta_config.data_block_size,
            config.runtime.data_cache_dirty_max_bytes_threshold,
            config.runtime.data_cache_dirty_max_blocks_threshold);

        let mut file = Self {
            client: client,
            staging: staging,
            bmap: bmap,
            bmap_ud: bmap_ud,
            data_blocks_dirty: BTreeMap::new(),
            inode: inode,
            config: config,
            max_dirty_blocks: max_dirty_blocks,
            flags: flags,
            last_flush: Instant::now(),
            sema: Arc::new(Semaphore::new(1)),
            spawn_write_permit: None,
        };
        // flush inode for hyper file new created
        let _ = file.flush_inode(FlushInodeFlag::Create).await?;
        Ok(file)
    }

    /// open a hyper file
    /// open by loading inode from staging,
    /// if inode is not found in staging, create hyper file from scratch
    pub async fn open(client: Client, staging: T, meta_block_loader: L, config: HyperFileConfig, flags: HyperFileFlags) -> Result<Self>
    {
        let meta_config = config.meta.clone();

        let mut raw_inode: InodeRaw = unsafe { std::mem::MaybeUninit::zeroed().assume_init() };
        let inode_state;
        match staging.load_inode(&mut raw_inode.as_mut_u8_slice()).await {
            Ok(od_state) => {
                /* if we load inode without error, we use inode as truth of metadata */
                inode_state = od_state;
            },
            Err(e) => {
                if e.kind() == ErrorKind::NotFound {
                    return Self::new(client, staging, meta_block_loader, config, flags).await;
                }
                return Err(e);
            },
        }
        let b = raw_inode.i_bmap;
        let bmap = BMap::<BlockIndex, BlockPtr, L>::read(&b, meta_config.meta_block_size, meta_block_loader);
        let bmap_ud = BMapUserData::from_u32(bmap.get_userdata());

        // if inode exists, we trust it

        let max_dirty_blocks = Self::calc_max_dirty_blocks(meta_config.data_block_size,
            config.runtime.data_cache_dirty_max_bytes_threshold,
            config.runtime.data_cache_dirty_max_blocks_threshold);

        let mut file = Self {
            client: client,
            staging: staging,
            bmap: bmap,
            bmap_ud: bmap_ud,
            data_blocks_dirty: BTreeMap::new(),
            inode: Inode::from_raw(&raw_inode, inode_state),
            config: config,
            max_dirty_blocks: max_dirty_blocks,
            flags: flags,
            last_flush: Instant::now(),
            sema: Arc::new(Semaphore::new(1)),
            spawn_write_permit: None,
        };
        // refresh bmap if need to do recovery
        let _ = file.refresh_bmap().await?;
        Ok(file)
    }

    pub async fn release(&mut self) -> Result<SegmentId> {
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
                debug!("      - load data block for block ptr {} at offset {} len {}", blk_ptr, off, this.len());
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
            // don't care if insert failed for exist
            match self.bmap.try_insert(blk_idx, BlockPtrFormat::dummy_value()).await {
                Err(e) => {
                    if e.kind() != ErrorKind::AlreadyExists { return Err(e); }
                },
                Ok(_) => {},
            }
        }

        let oldsize = self.inode.size();
        if off + len > oldsize {
            self.inode.set_size(off + len);
        }
        self.inode.update_mtime();
        // TODO: rollback to old size if flush failed
        drop(permit);

        let flushed = self.try_flush().await?;
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
            // don't care if insert failed for exist
            match self.bmap.try_insert(blk_idx, BlockPtrFormat::dummy_value()).await {
                Err(e) => {
                    if e.kind() != ErrorKind::AlreadyExists { return Err(e); }
                },
                Ok(_) => {},
            }
            bytes_write += data_len;
        }

        let oldsize = self.inode.size();
        if off + len > oldsize {
            self.inode.set_size(off + len);
        }
        self.inode.update_mtime();
        // TODO: rollback to old size if flush failed
        drop(permit);

        let flushed = self.try_flush().await?;
        let _ = fn_start;
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
        let max_flush_interval = self.config.runtime.data_cache_dirty_max_flush_interval;
        let last_flush_expired = self.last_flush.elapsed() >= Duration::from_millis(max_flush_interval);
        if last_flush_expired || threshold_flush || sync_flush {
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
        let tgt_blk_idx = ((new_size + 4096 - 1)/ data_block_size) as BlockIndex;
        let cur_blk_idx = ((size + 4096 - 1)/ data_block_size) as BlockIndex;

        if tgt_blk_idx == cur_blk_idx {
            // if no need to change metadata blocks, just update the new file size
            self.inode.set_size(new_size);
            self.inode.update_mtime();
            debug!("truncate - no bmap change, update file attr only");
            self.flush_inode(FlushInodeFlag::Update).await?;
            return Ok(());
        }

        // if need to extend file length
        if tgt_blk_idx > cur_blk_idx {
            let last_key_res = self.bmap.last_key().await;
            let (start_blkidx, end_blkidx) = if cur_blk_idx == 0 {
                if size == 0 {
                    // assume bmap is empty
                    assert!(last_key_res.unwrap_err().kind() == ErrorKind::NotFound);
                    (0, tgt_blk_idx)
                } else {
                    (1, tgt_blk_idx)
                }
            } else {
                assert!(last_key_res.is_ok());
                (last_key_res.unwrap() + 1, tgt_blk_idx)
            };
            // extending bmap
            debug!("truncate - operation need to extend current bmap index from {} to {}", start_blkidx, end_blkidx);
            for i in start_blkidx..=end_blkidx as BlockIndex {
                let _ = self.bmap.insert(i, BlockPtrFormat::new_zero_block()).await?;
            }
            self.bmap.dirty();
            self.inode.set_size(new_size);
            self.inode.update_mtime();
            drop(permit);
            self.flush().await?;
            return Ok(());
        }

        debug!("truncate - shrink bmap to BlockIndex {}", tgt_blk_idx);
        // if need to shrink bmap
        let _ = self.bmap.truncate(&tgt_blk_idx).await?;
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
            if self.data_blocks_dirty.contains_key(&blk_idx) {
                continue;
            }
            // for a complete block, we don't need to retrieve
            if start_off == 0 && data_len == data_block_size {
                continue;
            }
            output.push(blk_idx);
        }
        debug!("end of write prepare {} of blocks need to be retrieve", output.len());
        output
    }

    async fn write_retrieve(&mut self, list: Vec<BlockIndex>) -> Result<Vec<Block>> {
        let mut output = Vec::new();
        let data_block_size = self.config.meta.data_block_size;
        for blk_idx in list {
            match self.bmap.lookup(&blk_idx).await {
                Ok(blk_ptr) => {
                    debug!("retrive block ptr {} for block index {}", blk_ptr, blk_idx);
                    let block = Block::new(blk_idx, data_block_size);
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
                    let block = Block::new(blk_idx, data_block_size);
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
            let mut block = Block::new(blk_idx, data_block_size);
            block.copy(off, buf);
            self.data_blocks_dirty.insert(blk_idx, block);
        }
    }

    async fn load_data_block_read_path(&self, blk_idx: BlockIndex, blk_ptr: BlockPtr, offset: usize, buf: &mut [u8]) -> Result<()> {
        debug!("load_data_block - block ptr: {}", blk_ptr);
        // check dirty cache
        if let Some(block) = self.data_blocks_dirty.get(&blk_idx) {
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
            panic!("incorrect block ptr {} to load", blk_ptr);
        }
    }

    async fn load_data_block_write_path(&self, blk_idx: BlockIndex, blk_ptr: BlockPtr, offset: usize, buf: &mut [u8]) -> Result<()> {
        debug!("load_data_block - block ptr: {}", blk_ptr);
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
            panic!("incorrect block ptr {} to load", blk_ptr);
        }
    }

    // return max dirty data blocks can hold
    fn calc_max_dirty_blocks(data_block_size: usize, max_dirty_bytes_threshold: usize, max_dirty_blocks_threshold: usize) -> usize {
        let max_bytes = std::cmp::max(max_dirty_bytes_threshold, max_dirty_blocks_threshold * data_block_size);
        max_bytes / data_block_size
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

    fn clear_data_blocks_cache(&mut self) {
        // do nothing
    }

    fn get_data_blocks_dirty(&self) -> DirtyDataBlocks<'_> {
        let b: BTreeMap<BlockIndex, &Block> = self.data_blocks_dirty.iter()
                        .map(|(idx, blk)| (*idx, blk))
                        .collect();
        DirtyDataBlocks { inner: Some(b), owned: None }
    }

    fn clear_data_blocks_dirty(&mut self) {
        self.data_blocks_dirty.clear();
    }

    async fn lock(&self) -> OwnedSemaphorePermit {
        let permit = self.sema.clone().acquire_owned().await.unwrap();
        permit
    }

    fn unlock(&self, permit: OwnedSemaphorePermit) {
        drop(permit);
    }

    fn bmap(&self) -> &BMap<'a, BlockIndex, BlockPtr, L> {
        &self.bmap
    }

    fn bmap_mut(&mut self) -> &mut BMap<'a, BlockIndex, BlockPtr, L> {
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
