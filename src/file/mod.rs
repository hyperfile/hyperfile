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

use std::io::{Error, ErrorKind, Result};
use std::time::{Instant, Duration};
use std::collections::BTreeMap;
use log::{info, debug, warn};
use tokio::sync::OwnedSemaphorePermit;
use btree_ondisk::{bmap::BMap, BlockLoader, NodeValue};
use btree_ondisk::btree::BtreeNodeDirty;
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
        panic!("invalid DirtyDataBlocks");
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

pub trait HyperTrait<T: Staging<T, L> + segment::SegmentReadWrite, L: BlockLoader<BlockPtr> + Clone, V: Copy + Default + std::fmt::Display + NodeValue + 'static> {
    // block ptr
    fn blk_ptr_encode(&self, segid: SegmentId, offset: SegmentOffset, seq: usize) -> BlockPtr;
    fn blk_ptr_decode(&self, blk_ptr: &BlockPtr) -> (SegmentId, SegmentOffset);
    fn blk_ptr_decode_display(&self, blk_ptr: &BlockPtr) -> String;
    // data cache
    fn clear_data_blocks_cache(&mut self);
    // dirty data
    fn get_data_blocks_dirty(&self) -> DirtyDataBlocks<'_>;
    fn clear_data_blocks_dirty(&mut self);
    // lock
    fn lock(&self) -> impl Future<Output = OwnedSemaphorePermit>;
    fn unlock(&self, permit: OwnedSemaphorePermit);
    // bmap
    fn bmap_as_slice(&self) -> &[u8];
    fn bmap_get_block_loader(&self) -> L;
    fn bmap_dirty(&self) -> bool;
    fn bmap_lookup_dirty(&self) -> Vec<BtreeNodeDirty<'_, BlockIndex, V, BlockPtr>>;
    fn bmap_assign_meta_node(&self, blk_ptr: BlockPtr, node: BtreeNodeDirty<'_, BlockIndex, V, BlockPtr>) -> impl Future<Output = Result<()>>;
    fn bmap_assign_data_node(&self, blk_idx: &BlockIndex, blk_ptr: BlockPtr) -> impl Future<Output = Result<()>>;
    fn bmap_clear_dirty(&mut self);
    fn bmap_update<'a>(&mut self, bmap: BMap<'a, BlockIndex, V, BlockPtr, L>);
    fn bmap_insert_dummy_value(bmap: &mut BMap<'_, BlockIndex, V, BlockPtr, L>, blk_idx: &BlockIndex) -> impl Future<Output = Result<Option<V>>>;
    // inode
    fn inode(&self) -> &Inode;
    fn inode_mut(&self) -> &mut Inode;
    // others
    fn staging(&self) -> &T;
    fn config(&self) -> &HyperFileConfig;
    fn set_last_flush(&mut self);
    fn sleep(dur: Duration) -> impl Future<Output = ()>;

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

        let b = raw_inode.i_bmap;
        let mut bmap = BMap::<BlockIndex, V, BlockPtr, L>::read(&b, self.config().meta.meta_block_size, meta_block_loader);

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
            let _ = Self::bmap_insert_dummy_value(&mut bmap, blk_idx).await;
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

    // flush out dirty data
    fn flush_process(&mut self) -> impl Future<Output = Result<SegmentId>> {async {

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
        file_off += segment::Writer::<'_, T>::calc_ss_aligned_bytes(ndatadirty);

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
            let permit = self.lock().await;
            match self.flush_process().await {
                Ok(segid) => { return Ok(segid) },
                Err(err) => {
                    if err.kind() != ErrorKind::ResourceBusy {
                        return Err(err);
                    }
                    warn!("{err}");
                },
            }
            self.unlock(permit);
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
