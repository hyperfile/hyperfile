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
#[cfg(feature = "range-lock")]
pub mod lock;

use std::io::{Error, ErrorKind, Result};
use std::time::{Instant, Duration};
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
use ::reactor::TaskHandler;
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

    // wal
    #[cfg(feature = "wal")]
    fn wal_set_mem_segment(&self, mem_segid: SegmentId, mem_segdata: Weak<Pin<Box<Vec<u8>>>>) -> impl Future<Output = ()>;
    #[cfg(feature = "wal")]
    fn wal_clear_mem_segment(&self, mem_segid: SegmentId) -> impl Future<Output = ()>;

    // provided method
    fn bmap_get_raw(&self) -> BMapRawType {
        let mut b: BMapRawType = unsafe { std::mem::MaybeUninit::zeroed().assume_init() };
        b.copy_from_slice(self.bmap_as_slice());
        b
    }

    // recover inode from segment
    fn recover_partial_flush(&mut self, segid: u64, od_state: &Option<OnDiskState>) -> impl Future<Output = Result<()>> {async move {
        let mut raw_inode: InodeRaw = unsafe { std::mem::MaybeUninit::zeroed().assume_init() };
        let _ = self.staging().load_inode_from_segment(&mut raw_inode.as_mut_u8_slice(), segid).await?;
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
            let next_segid = raw_inode.i_last_seq + 1;
            match self.staging().load_segment_timestamp(next_segid).await {
                Ok((server_time, segment_lm)) => {
                    if server_time - segment_lm > DEFAULT_PARTIAL_FLUSH_TIMEOUT as i64 {
                        self.recover_partial_flush(next_segid, &inode_state).await?;
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

        if curr_segid == od_last_seq {
            debug!("REFRESH_BMAP - quit due: current segid {} == on disk segid {}", curr_segid, od_last_seq);
            return Ok(curr_segid);
        } else if curr_segid > od_last_seq {
            warn!("REFRESH_BMAP - current segid {} is ahead of on disk segid {}", curr_segid, od_last_seq);
            return Ok(curr_segid);
        }

        // get back block loader
        let meta_block_loader = self.bmap_get_block_loader();
        // get back node cache
        let node_cache = self.bmap_get_node_cache();

        let b = raw_inode.i_bmap;
        let mut bmap = BMap::<BlockIndex, V, BlockPtr, L, C>::read(&b, self.config().meta.meta_block_size, meta_block_loader, node_cache);

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

    fn flush_process_pre_build_segment(&self) -> impl Future<Output = Result<(SegmentId, DirtyDataBlocks<'_>)>> {async {
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
            return Ok((segid, DirtyDataBlocks { inner: None, owned: None }));
        }
        Ok((0, dirty_data_blocks))
    }}

    fn flush_process_build_segment(&self, dirty_data_blocks: DirtyDataBlocks<'_>)
            -> impl Future<Output = Result<(segment::Writer<T>, SegmentId, InodeRaw, Vec<BtreeNodeDirty<'_, BlockIndex, V, BlockPtr>>)>>
    {async move {
        // prepare for a segment
        let _start = Instant::now();

        // 1. collect all dirty meta data
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
        raw_inode.i_last_cno = segid;
        // TODO: calc segment checksum
        segwr.realize_ss(0, &raw_inode);

        // write out root node to staging file
        let _start = Instant::now();
        file_off = 0;
        for n in &dirty_meta_vec {
            let node_size = n.size();
            let _ = segwr.append(n.as_slice())?;
            file_off += node_size;
        }
        #[cfg(not(feature = "concurrent-segment-build"))]
        for (_, n) in dirty_data_blocks.data().iter() {
            let block_size = n.size();
            let _ = segwr.append(n.as_slice())?;
            file_off += block_size;
        }
        #[cfg(feature = "concurrent-segment-build")]
        {

        const TARGET_CHUNKS: usize = 50;
        let mut joins = Vec::new();
        let block_size = self.config().meta.data_block_size;
        let data_blocks = dirty_data_blocks.data()
                        .into_iter()
                        .map(|(_, block)| block)
                        .collect::<Vec<&DataBlock>>();
        // calc per chunk count based on target chunks
        let chunk_count = data_blocks.len() / (TARGET_CHUNKS - 1);
        let mut chunks = Vec::new();
        let mut remains = data_blocks;
        // turn flat data_blocks vec into vec of Vec<DataBlock>
        while remains.len() > 0 {
            let split_off = if remains.len() >= chunk_count {
                remains.split_off(chunk_count)
            } else {
                remains.split_off(remains.len())
            };
            chunks.push(remains);
            remains = split_off;
        }
        // spawn chunks
        for chunk in chunks {
            let chunk_count = chunk.len();
            let j = segwr.spawn_append(chunk);
            joins.push(j);
            file_off += block_size * chunk_count;
        }

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

    fn flush_process(&mut self) -> impl Future<Output = Result<SegmentId>> {async move {
        let fn_start = Instant::now();
        debug!("flush started");

        let (segid, dirty_data_blocks) = self.flush_process_pre_build_segment().await?;
        if segid > 0 {
            return Ok(segid);
        }
        let (segwr, segid, raw_inode, dirty_meta_vec) = self.flush_process_build_segment(dirty_data_blocks).await?;

        let _start = Instant::now();
        segwr.done().await?;
        // update last cno in memory after segment write out
        self.inode_mut().set_last_cno(segid);
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
        Ok(self.inode().get_last_ondisk_cno())
    }}

    #[cfg(all(feature = "wal", feature = "reactor"))]
    fn wal_flush_process_reactor<'a: 'static>(&mut self, fh: TaskHandler<FileContext<'a>>, lock: OwnedMutexGuard<()>)
            -> impl Future<Output = std::result::Result<SegmentId, (OwnedMutexGuard<()>, Error)>>
    {async move {
        let fn_start = Instant::now();
        debug!("flush started");

        let (segid, dirty_data_blocks) = match self.flush_process_pre_build_segment().await {
            Ok((segid, dirty_data_blocks)) => (segid, dirty_data_blocks),
            Err(e) => return Err((lock, e)),
        };
        if segid > 0 {
            return Ok(segid);
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
                    fh.send_cb(ctx);
                    return;
                },
            }
            match staging.flush_inode(raw_inode.as_u8_slice(), &od_state, FlushInodeFlag::Update).await {
                Ok(od_state) => {
                    let ctx = FileContext::new_wal_flush_done(lock, segid, od_state.unwrap().clone(), bmap_cache_limit);
                    fh.send_cb(ctx);
                },
                Err(e) => {
                    warn!("flush inode failed in wal flush process: {:?}", e);
                    let ctx = FileContext::new_wal_flush_recovery(lock);
                    fh.send_cb(ctx);
                },
            }
        });
        self.inode_mut().set_last_cno(segid);
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
        Ok(self.inode().get_last_cno())
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

        self.inode_mut().set_last_cno(segid);
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
        Ok(self.inode().get_last_ondisk_cno())
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
            let segid = self.inode().get_last_seq();
            return Ok(segid);
        }

        // prepare for a segment
        let _start = Instant::now();

        // 1. collect all dirty meta data
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
        raw_inode.i_last_cno = segid;
        // TODO: calc segment checksum
        segwr.realize_ss(0, &raw_inode);

        // write out root node to staging file
        let _start = Instant::now();
        file_off = 0;
        for n in &dirty_meta_vec {
            let node_size = n.size();
            let _ = segwr.append(n.as_slice())?;
            file_off += node_size;
        }
        for (_, n) in dirty_data_blocks.data().iter() {
            let block_size = n.size();
            let _ = segwr.append(n.as_slice())?;
            file_off += block_size;
        }
        let _ = _start.elapsed();

        let _start = Instant::now();
        segwr.done().await?;
        // update last cno in memory after segment write out
        self.inode_mut().set_last_cno(segid);
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
        Ok(self.inode().get_last_ondisk_cno())
    }}

    fn flush(&mut self) -> impl Future<Output = Result<SegmentId>> {async {
        let mut retries = 0;
        let mut backoff = DEFAULT_FLUSH_BACKOFF_SECS;
        while retries < DEFAULT_FLUSH_RETRIES {
            let lock = self.flush_lock().await;
            match self.flush_process().await {
                Ok(segid) => { return Ok(segid) },
                Err(err) => {
                    if err.kind() != ErrorKind::ResourceBusy {
                        return Err(err);
                    }
                    warn!("{err}");
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
