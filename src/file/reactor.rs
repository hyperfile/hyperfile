//! IO function used by LocalSpawner reactor
use std::io::{Result, ErrorKind};
use log::{debug, warn};
use btree_ondisk::BlockLoader;
use tokio::task::JoinHandle;
use crate::{BlockIndex, BlockPtr, BlockIndexIter};
use crate::staging::Staging;
use crate::segment::SegmentReadWrite;
use crate::file::{HyperTrait, BlockPtrFormat};
use crate::buffer::DataBlock;
use super::file::HyperFile;
use super::handler::{FileReqRead, FileReqWrite, FileReqWriteZero, FileResp, FileContext};

enum SpawnReadSize {
    ImmSize(usize),
    JoinSize(JoinHandle<usize>),
}

impl SpawnReadSize {
    fn size(&self) -> usize {
        match self {
            Self::ImmSize(sz) => { *sz },
            Self::JoinSize(_) => { 0 },
        }
    }
}

impl<'a: 'static, T: Staging<T, L> + SegmentReadWrite + Send + Clone + 'static, L: BlockLoader<BlockPtr> + Clone + 'static> HyperFile<'a, T, L> {
    fn spawn_load_data_block_read_path(&mut self, blk_id: BlockIndex, blk_ptr: BlockPtr, offset: usize, buf: &mut [u8]) -> Result<SpawnReadSize> {
        debug!("spawn_load_data_block_read_path - offset: {}, bytes: {}, block ptr: {}", offset, buf.len(), self.blk_ptr_decode_display(&blk_ptr));
        // in read path we would check dirty cache before do real data load
        // check dirty cache
        if let Some(block) = self.data_blocks_dirty.get(&blk_id) {
            // cache hit
            debug!("load_data_block - Cache Hit on data blocks dirty for block index: {}", blk_id);
            let slice = unsafe {
                std::slice::from_raw_parts(block.as_slice().as_ptr() as *const u8, block.as_slice().len())
            };
            let data_buf = unsafe {
                std::slice::from_raw_parts_mut(buf.as_mut_ptr() as *mut u8, buf.len())
            };
            data_buf.copy_from_slice(&slice[offset..offset + data_buf.len()]);
            return Ok(SpawnReadSize::ImmSize(data_buf.len()));
        }
        if let Some(block) = (self.data_cache_blocks > 0).then(|| self.data_blocks_cache.get(&blk_id)).unwrap() {
            // cache hit
            debug!("load_data_block - Cache Hit on data blocks cache for block index: {}", blk_id);
            let slice = unsafe {
                std::slice::from_raw_parts(block.as_slice().as_ptr() as *const u8, block.as_slice().len())
            };
            let data_buf = unsafe {
                std::slice::from_raw_parts_mut(buf.as_mut_ptr() as *mut u8, buf.len())
            };
            data_buf.copy_from_slice(&slice[offset..offset + data_buf.len()]);
            return Ok(SpawnReadSize::ImmSize(data_buf.len()));
        }
        if BlockPtrFormat::is_on_staging(&blk_ptr) {
            let (segid, staging_off) = self.blk_ptr_decode(&blk_ptr);
            let staging = self.staging.clone();
            let data_block_size = self.config.meta.data_block_size;
            let data_buf = unsafe {
                std::slice::from_raw_parts_mut(buf.as_mut_ptr() as *mut u8, buf.len())
            };
            let join = self.rt.as_ref().unwrap().spawn(async move {
                let _ = staging.load_data_block(segid, staging_off, offset, data_block_size, data_buf).await;
                data_buf.len()
            });
            return Ok(SpawnReadSize::JoinSize(join));
        } else if BlockPtrFormat::is_dummy_value(&blk_ptr) {
            panic!("failed to get block index: {} from data blocks dirty cache for dummy block ptr", blk_id);
        } else if BlockPtrFormat::is_zero_block(&blk_ptr) {
            debug!("load_data_block - Fill Zero for block index: {}", blk_id);
            let data_buf = unsafe {
                std::slice::from_raw_parts_mut(buf.as_mut_ptr() as *mut u8, buf.len())
            };
            data_buf.fill(0);
            return Ok(SpawnReadSize::ImmSize(data_buf.len()));
        } else {
            panic!("incorrect block ptr {} to load", self.blk_ptr_decode_display(&blk_ptr));
        }
    }

    pub(crate) fn spawn_load_data_block_write_path(&self, blk_id: BlockIndex, blk_ptr: BlockPtr, offset: usize, buf: &mut [u8]) -> Result<JoinHandle<usize>> {
        debug!("spawn_load_data_block_write_path - offset: {}, bytes: {}, block ptr: {}", offset, buf.len(), self.blk_ptr_decode_display(&blk_ptr));
        if BlockPtrFormat::is_on_staging(&blk_ptr) {
            let (segid, staging_off) = self.blk_ptr_decode(&blk_ptr);
            let staging = self.staging.clone();
            let data_block_size = self.config.meta.data_block_size;
            let data_buf = unsafe {
                std::slice::from_raw_parts_mut(buf.as_mut_ptr() as *mut u8, buf.len())
            };
            let join = self.rt.as_ref().unwrap().spawn(async move {
                let _ = staging.load_data_block(segid, staging_off, offset, data_block_size, data_buf).await;
                data_buf.len()
            });
            return Ok(join);
        } else if BlockPtrFormat::is_dummy_value(&blk_ptr) {
            if let Some(block) = self.data_blocks_dirty.get(&blk_id) {
                // cache hit
                debug!("load_data_block - Cache Hit on data blocks dirty for block index: {}", blk_id);
                let slice = unsafe {
                    std::slice::from_raw_parts(block.as_slice().as_ptr() as *const u8, block.as_slice().len())
                };
                let data_buf = unsafe {
                    std::slice::from_raw_parts_mut(buf.as_mut_ptr() as *mut u8, buf.len())
                };
                let join = self.rt.as_ref().unwrap().spawn(async move {
                    data_buf.copy_from_slice(&slice[offset..offset + data_buf.len()]);
                    data_buf.len()
                });
                return Ok(join);
            }
            panic!("failed to get block index: {} from data blocks dirty cache for dummy block ptr", blk_id);
        } else if BlockPtrFormat::is_zero_block(&blk_ptr) {
            debug!("load_data_block - Fill Zero for block index: {}", blk_id);
            let data_buf = unsafe {
                std::slice::from_raw_parts_mut(buf.as_mut_ptr() as *mut u8, buf.len())
            };
            let join = self.rt.as_ref().unwrap().spawn(async move {
                data_buf.fill(0);
                data_buf.len()
            });
            return Ok(join);
        } else {
            panic!("incorrect block ptr {} to load", blk_ptr);
        }
    }

    // return:
    //   (total_bytes, Vec<(block index, block ptr, offset in block, out buffer)>)
    pub async fn collect_block_ptr(&self, off: usize, buf: &'a mut [u8]) -> Result<(usize, Vec<(BlockIndex, BlockPtr, usize, &'a mut [u8])>)> {
        let data_block_size = self.config.meta.data_block_size;
        let blk_idx = (off / data_block_size) as BlockIndex;
        let blk_off = off % data_block_size;
        let blk_count = (buf.len() + data_block_size - 1) / data_block_size;
        let mut bytes_read = 0;

        debug!("collect_block_ptr - block index {blk_idx}, block offset {blk_off}, block count {blk_count}");

        // blocks need to be read back
        let mut blocks = Vec::new();

        let head_off = off % data_block_size;

        let (mut next_blk_idx, next_slice) = if head_off != 0 {
            let blk_ptr = match self.bmap.lookup(&blk_idx).await {
                Ok(ptr) => { ptr },
                Err(e) => {
                    match e.kind() {
                        ErrorKind::NotFound => { BlockPtrFormat::new_zero_block() },
                        _ => {
                            warn!("collect_block_ptr - lookup bmap for block index {blk_idx} error: {}", e);
                            return Err(e);
                        },
                    }
                },
            };
            if head_off + buf.len() < data_block_size {
                let bytes = buf.len();
                blocks.push((blk_idx, blk_ptr, head_off, buf));
                bytes_read += bytes;
                return Ok((bytes_read, blocks));
            }

            // we need to split head bytes if offset is not aligned to block size
            let split_off = data_block_size - head_off;
            let (this, next) = buf.split_at_mut(split_off);
            debug!("       - read head data block for block ptr {} at offset {} len {}", self.blk_ptr_decode_display(&blk_ptr), head_off, this.len());
            let bytes = this.len();
            blocks.push((blk_idx, blk_ptr, head_off, this));
            bytes_read += bytes;
            if next.len() == 0 {
                return Ok((bytes_read, blocks));
            }
            (blk_idx + 1, next)
        } else {
            (blk_idx, buf)
        };

        for b in next_slice.chunks_mut(data_block_size) {
            debug!("     - lookup block index {next_blk_idx}");
            let blk_ptr = match self.bmap.lookup(&next_blk_idx).await {
                Ok(ptr) => { ptr },
                Err(e) => {
                    match e.kind() {
                        ErrorKind::NotFound => { BlockPtrFormat::new_zero_block() },
                        _ => {
                            warn!("collect_block_ptr - lookup bmap for block index {next_blk_idx} error: {}", e);
                            return Err(e);
                        },
                    }
                },
            };
            debug!("       - read data block for block ptr {} at offset {} len {}", self.blk_ptr_decode_display(&blk_ptr), 0, b.len());
            let bytes = b.len();
            blocks.push((next_blk_idx, blk_ptr, 0, b));
            bytes_read += bytes;
            next_blk_idx += 1;
        }
        Ok((bytes_read, blocks))
    }

    // for spawn_read/spawn_write resp is based on mpsc channel
    // so use try_send() instead send()
    pub async fn spawn_read(&mut self, req: FileReqRead<'a>, resp: FileResp) -> Result<usize> {
        let off = req.offset;
        let mut buf = req.buf;

        let _permit = self.sema.clone().acquire_owned().await.unwrap();

        debug!("READ - off: {}, buf len: {}", off, buf.len());
        if off >= self.inode.size() {
            let _ = resp.to_read().try_send(Ok(0));
            return Ok(0);
        }
        // if requested buffer exceed file size, cut off tailing buffer
        if off + buf.len() > self.inode.size() {
            let exceeded_len = off + buf.len() - self.inode.size();
            let mid = buf.len() - exceeded_len;
            debug!("READ - buf len shrink to: {}, due to file size {}", mid, self.inode.size());
            (buf, _) = buf.split_at_mut(mid);
        }

        let (total_bytes, blocks) = self.collect_block_ptr(off, buf).await?;
        let mut joins = Vec::new();
        for (blk_idx, blk_ptr, off, buf) in blocks {
            let join = self.spawn_load_data_block_read_path(blk_idx, blk_ptr, off, buf)?;
            joins.push(join)
        }

        // if nothing need to join, return directly
        if joins.iter().all(|x| match x { SpawnReadSize::ImmSize(_) => true, SpawnReadSize::JoinSize(_) => false, }) {
            let actual_bytes = joins.iter().map(|x| x.size()).sum();
            assert!(total_bytes == actual_bytes);
            if actual_bytes > 0 {
                self.inode.update_atime();
            }
            let _ = resp.to_read().try_send(Ok(actual_bytes));
            return Ok(total_bytes);
        }

        // no matter load backend data block success or not, update inode atime
        if total_bytes > 0 {
            self.inode.update_atime();
        }
        self.rt.as_ref().unwrap().spawn(async move {
            let mut actual_bytes = 0;
            while let Some(join) = joins.pop() {
                let bytes = match join {
                    SpawnReadSize::ImmSize(sz) => { sz },
                    SpawnReadSize::JoinSize(j) => { j.await.unwrap() },
                };
                actual_bytes += bytes;
            }
            assert!(total_bytes == actual_bytes);
            let _ = resp.to_read().try_send(Ok(actual_bytes));
        });

        Ok(total_bytes)
    }

    pub(crate) async fn absorb_write(&mut self, off: usize, buf: &[u8], fetched: Vec<DataBlock>) -> Result<usize> {
        let len = buf.len();
        let mut bytes_write = 0;

        // restore spawn_write permit
        let opt_permit = self.spawn_write_permit.take();
        assert!(opt_permit.is_some());

        // insert fetched block back to dirty list,
        // for the case: block idx exists on dirty
        // means some other writes success before this write, let's ignore feched data and go ahead
        // to update the block data
        for block in fetched.into_iter() {
            let blk_idx = block.index();
            if !self.data_blocks_dirty.contains_key(&blk_idx) {
                let None = self.data_blocks_dirty.insert(blk_idx, block) else {
                    panic!("BlockIndex {} already on data_blocks_dirty list", blk_idx);
                };
            } else {
                debug!("BlockIndex {} already on data_blocks_dirty list, ignore this block", blk_idx);
            }
        }

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
        drop(opt_permit);

        let flushed = self.try_flush().await?;
        // TODO: placehold for pref metrics
        let _ = flushed;

        Ok(bytes_write)
    }

    pub(crate) async fn absorb_write_zero(&mut self, off: usize, len: usize, fetched: Vec<DataBlock>) -> Result<usize> {
        let mut bytes_write = 0;

        // restore spawn_write permit
        let opt_permit = self.spawn_write_permit.take();
        assert!(opt_permit.is_some());

        // insert fetched block back to dirty list,
        // for the case: block idx exists on dirty
        // means some other writes success before this write, let's ignore feched data and go ahead
        // to update the block data
        for block in fetched.into_iter() {
            let blk_idx = block.index();
            if !self.data_blocks_dirty.contains_key(&blk_idx) {
                let None = self.data_blocks_dirty.insert(blk_idx, block) else {
                    panic!("BlockIndex {} already on data_blocks_dirty list", blk_idx);
                };
            } else {
                debug!("BlockIndex {} already on data_blocks_dirty list, ignore this block", blk_idx);
            }
        }

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
        drop(opt_permit);

        let flushed = self.try_flush().await?;
        // TODO: placehold for pref metrics
        let _ = flushed;

        Ok(bytes_write)
    }

    async fn spawn_write_retrieve(&mut self, mut req: FileReqWrite<'a>, resp: FileResp, list: Vec<BlockIndex>) -> Result<()> {
        let mut joins = Vec::new();
        let mut fetched = Vec::new();
        let data_block_size = self.config.meta.data_block_size;
        for blk_idx in list {
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
                    debug!("block index {} not found in bmap, prepare a new block", blk_idx);
                    let mut block = DataBlock::new(blk_idx, data_block_size);
                    block.set_should_cache();
                    fetched.push(block);
                },
            }
        }

        self.rt.as_ref().unwrap().spawn(async move {
            let mut actual_bytes = 0;
            while let Some(j) = joins.pop() {
                let bytes = j.await.unwrap();
                actual_bytes += bytes;
            }
            req.fetched.append(&mut fetched);
            let _ = actual_bytes;
            let fh = req.absorb_fh.clone();
            let ctx = FileContext::write_absorb(req, resp);
            fh.send_highprio(ctx);
        });

        Ok(())
    }

    // duplicate logic of spawn_write_retrieve()
    // TODO: can be merge with spawn_write_retrieve
    async fn spawn_write_zero_retrieve(&mut self, mut req: FileReqWriteZero<'a>, resp: FileResp, list: Vec<BlockIndex>) -> Result<()> {
        let mut joins = Vec::new();
        let mut fetched = Vec::new();
        let data_block_size = self.config.meta.data_block_size;
        for blk_idx in list {
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
                    debug!("block index {} not found in bmap, prepare a new block", blk_idx);
                    let mut block = DataBlock::new(blk_idx, data_block_size);
                    block.set_should_cache();
                    fetched.push(block);
                },
            }
        }

        self.rt.as_ref().unwrap().spawn(async move {
            let mut actual_bytes = 0;
            while let Some(j) = joins.pop() {
                let bytes = j.await.unwrap();
                actual_bytes += bytes;
            }
            req.fetched.append(&mut fetched);
            let _ = actual_bytes;
            let fh = req.absorb_fh.clone();
            let ctx = FileContext::write_zero_absorb(req, resp);
            fh.send_highprio(ctx);
        });

        Ok(())
    }

    // split write path into:
    //   1. prepare - check unaligned data write and update bmap index - in main reactor
    //   2. data retrieve - for any data need to be retrieve ahead - spawn
    //   3. data cache - update data cache and update bmap index - back to main reactor
    //
    // for spawn_read/spawn_write resp is based on mpsc channel
    // so use try_send() instead send()
    pub async fn spawn_write(&mut self, req: FileReqWrite<'a>, resp: FileResp) -> Result<usize> {
        let off = req.offset;
        let buf = req.buf;
        let len = buf.len();

        let permit = self.sema.clone().acquire_owned().await.unwrap();

        debug!("WRITE - off: {}, buf len: {}", off, len);
        let v: Vec<BlockIndex> = self.write_prepare(off, len);
        if v.len() > 0 {
            // retrieve data by spawn
            self.spawn_write_retrieve(req, resp, v).await?;
            self.spawn_write_permit = Some(permit);
            return Ok(len);
        }

        // pass permit through inner fields to next fn
        self.spawn_write_permit = Some(permit);

        // no need to pre-retrive anything,
        // this is HAPPY PATH, continue on this runtime
        let actual_bytes = self.absorb_write(off, buf, Vec::new()).await?;
        assert!(len == actual_bytes);

        // kick off response
        let _ = resp.to_write().try_send(Ok(actual_bytes));
        Ok(len)
    }

    pub async fn spawn_write_zero(&mut self, req: FileReqWriteZero<'a>, resp: FileResp) -> Result<usize> {
        let off = req.offset;
        let len = req.len;

        let permit = self.sema.clone().acquire_owned().await.unwrap();

        debug!("WRITE ZERO - off: {}, len: {}", off, len);
        let v: Vec<BlockIndex> = self.write_prepare(off, len);
        if v.len() > 0 {
            // retrieve data by spawn
            self.spawn_write_zero_retrieve(req, resp, v).await?;
            self.spawn_write_permit = Some(permit);
            return Ok(len);
        }

        // pass permit through inner fields to next fn
        self.spawn_write_permit = Some(permit);

        // no need to pre-retrive anything,
        // this is HAPPY PATH, continue on this runtime
        let actual_bytes = self.absorb_write_zero(off, len, Vec::new()).await?;
        assert!(len == actual_bytes);

        // kick off response
        let _ = resp.to_write().try_send(Ok(actual_bytes));
        Ok(len)
    }
}
