pub mod file;
pub mod flags;
#[cfg(feature = "reactor")]
pub mod handler;
#[cfg(feature = "reactor")]
pub mod reactor;
pub mod hyper;
pub mod fs;

use std::io::{Error, ErrorKind, Result};
use std::time::{Instant, Duration};
use std::collections::BTreeMap;
use log::{debug, warn};
use tokio::sync::OwnedSemaphorePermit;
use btree_ondisk::{bmap::BMap, BlockLoader};
use crate::*;
use crate::buffer::Block;
use crate::{BlockIndex, BlockPtr};
use crate::{SegmentId, SegmentOffset};
use crate::config::HyperFileMetaConfig;
use crate::ondisk::{BMapRawType, InodeRaw};
use crate::inode::{Inode, OnDiskState, FlushInodeFlag};
use crate::staging::Staging;
use crate::meta_format::BlockPtrFormat;

pub(crate) struct DirtyDataBlocks<'a> {
    pub(crate) inner: Option<BTreeMap<BlockIndex, &'a Block>>,
    // if we need to owned (clone) the data
    pub(crate) owned: Option<BTreeMap<BlockIndex, Block>>,
}

impl<'a> DirtyDataBlocks<'a> {
    fn len(&self) -> usize {
        if let Some(inner) = &self.inner {
            return inner.len();
        } else if let Some(owned) = &self.owned {
            return owned.len();
        }
        panic!("invalid DirtyDataBlocks");
    }

    fn data(&'a self) -> BTreeMap<BlockIndex, &'a Block> {
        if let Some(inner) = &self.inner {
            return inner.clone();
        } else if let Some(owned) = &self.owned {
            let inner: BTreeMap<BlockIndex, &'a Block> = owned
                                .iter()
                                .map(|(idx, blk)| (*idx, blk))
                                .collect();
            return inner;
        }
        panic!("invalid DirtyDataBlocks");
    }
}

pub(crate) trait HyperTrait<'a, T: Staging<T, L> + segment::SegmentReadWrite, L: BlockLoader<BlockPtr> + Clone> {
    fn blk_ptr_encode(&self, segid: SegmentId, offset: SegmentOffset, seq: usize) -> BlockPtr;
    fn blk_ptr_decode(&self, blk_ptr: &BlockPtr) -> (SegmentId, SegmentOffset);
    fn dirty_data_blocks(&self) -> DirtyDataBlocks<'_>;
    fn data_blocks_cache_clear(&mut self);
    fn move_dirty_blocks_to_cache(&mut self);
    fn lock(&self) -> impl Future<Output = OwnedSemaphorePermit>;
    fn unlock(&self, permit: OwnedSemaphorePermit);
    fn bmap(&self) -> &BMap<'a, BlockIndex, BlockPtr, L>;
    fn bmap_mut(&mut self) -> &mut BMap<'a, BlockIndex, BlockPtr, L>;
    fn bmap_ud(&self) -> &BMapUserData;
    fn bmap_get_raw(&self) -> BMapRawType {
        let mut b: BMapRawType = unsafe { std::mem::MaybeUninit::zeroed().assume_init() };
        b.copy_from_slice(self.bmap().as_slice());
        b
    }
    fn staging(&self) -> &T;
    fn config(&self) -> &HyperFileMetaConfig;
    fn set_last_flush(&mut self);
    fn inode(&self) -> &Inode;
    fn inode_get_next_seq(&self) -> SegmentId;
    fn inode_set_last_cno(&self, segid: u64);
    fn inode_set_ondisk_state(&self, od_state: Option<OnDiskState>);
    fn inode_set_last_ondisk_cno(&self, cno: u64);
    fn inode_mut(&mut self) -> &mut Inode;
    fn sleep(dur: Duration) -> impl Future<Output = ()>;

    // recover inode from segment
    async fn recover_partial_flush(&mut self, segid: u64, od_state: &Option<OnDiskState>) -> Result<()> {
        let mut raw_inode: InodeRaw = unsafe { std::mem::MaybeUninit::zeroed().assume_init() };
        let _ = self.staging().load_inode_from_segment(&mut raw_inode.as_mut_u8_slice(), segid).await?;
        let od_state = self.staging().flush_inode(raw_inode.as_u8_slice(), od_state, FlushInodeFlag::Update).await?;
        self.inode_set_ondisk_state(od_state);
        self.inode_set_last_ondisk_cno(self.inode().get_last_cno());
        Ok(())
    }

    /// try recover partial flush (inode ahead segment)
    async fn try_recover_partial_flush(&mut self) -> Result<(InodeRaw, Option<OnDiskState>)> {
        let mut raw_inode: InodeRaw = unsafe { std::mem::MaybeUninit::zeroed().assume_init() };
        let mut inode_state;
        loop {
            match self.staging().load_inode(&mut raw_inode.as_mut_u8_slice()).await {
                Ok(od_state) => { inode_state = od_state; },
                Err(e) => { return Err(e); },
            }

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
    }

    /// refresh bmap by starting from reload inode
    /// and detect potential dead flush
    async fn refresh_bmap(&mut self) -> Result<SegmentId> {
        // save current in memory state
        let curr_size = self.inode().size();
        let curr_segid = self.inode().get_last_seq();
        let (raw_inode, inode_state) = self.try_recover_partial_flush().await?;
        let mut od_inode = Inode::from_raw(&raw_inode, inode_state);

        if curr_segid == od_inode.get_last_seq() {
            debug!("REFRESH_BMAP - quit due: current segid {} == on disk segid {}", curr_segid, od_inode.get_last_seq());
            return Ok(curr_segid);
        } else if curr_segid > od_inode.get_last_seq() {
            warn!("REFRESH_BMAP - current segid {} is ahead of on disk segid {}", curr_segid, od_inode.get_last_seq());
            return Ok(curr_segid);
        }

        // get back block loader
        let meta_block_loader = self.bmap().get_block_loader();

        let b = raw_inode.i_bmap;
        let mut bmap = BMap::<BlockIndex, BlockPtr, L>::read(&b, self.config().meta_block_size, meta_block_loader);

        let _permit = self.lock().await;

        // clear cached data
        let _ = self.data_blocks_cache_clear();


        // update size if needed
        if curr_size > od_inode.size() {
            od_inode.set_size(curr_size);
        }

        let dirty_data_blocks = self.dirty_data_blocks();

        // no dirty blocks, just return
        if dirty_data_blocks.len() == 0 {
            return Ok(self.inode().get_last_seq())
        }

        // rebuild dirty bmap
        for (blk_idx, _) in dirty_data_blocks.data().iter() {
            // if blk index is already exists, overwrite it
            let _ = bmap.insert(*blk_idx, BlockPtrFormat::dummy_value()).await?;
        }
        assert!(bmap.dirty() == true);

        let last_seq = self.inode().get_last_seq();

        // refresh inner bmap and inode
        *self.bmap_mut() = bmap;
        *self.inode_mut() = od_inode;

        Ok(last_seq)
    }

    // flush out dirty data
    async fn flush_process(&mut self) -> Result<SegmentId> {

        let fn_start = Instant::now();
        debug!("flush started");

        let dirty_data_blocks = self.dirty_data_blocks();

        if dirty_data_blocks.len() == 0 && !self.bmap().dirty() {
            debug!("flush quit, NO dirty data blocks amd bmap is NOT dirty");
            let segid = self.inode().get_last_seq();
            return Ok(segid);
        }

        // prepare for a segment
        let _start = Instant::now();

        // 1. collect all dirty meta data
        let dirty_meta_vec = self.bmap().lookup_dirty();
        debug!("start to create a new segemtnt: dirty meta nodes {}, dirty data blocks {}",
            dirty_meta_vec.len(), dirty_data_blocks.len());

        let segid = self.inode_get_next_seq();
        let mut file_off = 0;
        let mut segwr = self.staging().new_segwr(segid, self.config());

        let ndatadirty = dirty_data_blocks.len();
        file_off += segment::Writer::<'_, T>::calc_ss_aligned_bytes(ndatadirty);

        let mut block_seq = 0;
        // assign real blk ptr to meta data
        for n in &dirty_meta_vec {
            let blk_ptr = self.blk_ptr_encode(segid, file_off, block_seq);
            let node_size = n.size();
            // use 0 as key, but it's useless
            debug!("assign block ptr for meta node: block ptr {}", blk_ptr);
            self.bmap().assign_meta_node(blk_ptr, n.clone()).await?;
            segwr.inc_metablk();
            file_off += node_size;
            block_seq += 1;
        }

        // 2. collect all dirty data block

        // assign real blk ptr to data block
        for (blk_idx, n) in dirty_data_blocks.data().iter() {
            let blk_ptr = self.blk_ptr_encode(segid, file_off, block_seq);
            let block_size = n.size();
            debug!("assign block ptr for data node: block ptr {}, block index {}", blk_ptr, blk_idx);
            self.bmap().assign_data_node(blk_idx, blk_ptr).await?;
            segwr.inc_datablk(blk_idx, &blk_ptr);
            file_off += block_size;
            block_seq += 1;
        }

        let _ = _start.elapsed();

        // prepare inode
        self.inode_set_last_cno(segid);
        let b = self.bmap_get_raw();
        let raw_inode = self.inode().to_raw(b);
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
        let _ = _start.elapsed();

        // flush inode after writeout segment
        let _start = Instant::now();
        let od_state = self.staging().flush_inode(raw_inode.as_u8_slice(), self.inode().get_ondisk_state(), FlushInodeFlag::Update).await?;
        let _ = _start.elapsed();
        self.inode_set_ondisk_state(od_state);
        self.inode_set_last_ondisk_cno(self.inode().get_last_cno());

        // start to cleanup
        let _start = Instant::now();

        // clear dirty for all dirty meta node
        for n in dirty_meta_vec {
            n.clear_dirty();
        }

        self.move_dirty_blocks_to_cache();

        // clear dirty for bmap
        self.bmap_mut().clear_dirty();
        // reset last flush
        self.set_last_flush();

        let _ = _start.elapsed();
        let _ = fn_start.elapsed();
        Ok(self.inode().get_last_ondisk_cno())
    }

    async fn flush(&mut self) -> Result<SegmentId> {
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
    }
}
