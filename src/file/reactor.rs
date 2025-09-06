//! IO function used by LocalSpawner reactor
use std::io::{Result, Error, ErrorKind};
use log::{debug, warn};
use btree_ondisk::BlockLoader;
use tokio::task::JoinHandle;
use crate::{BlockIndex, BlockPtr, BlockIndexIter};
use crate::staging::Staging;
use crate::segment::SegmentReadWrite;
use crate::file::HyperTrait;
use crate::meta_format::BlockPtrFormat;
use crate::buffer::DataBlock;
use super::file::HyperFile;
use super::handler::{FileReqRead, FileReqWrite, FileReqWriteZero, FileResp, FileContext};

pub(crate) enum ImmOrJoinSize {
    ImmSize(usize),
    JoinSize(JoinHandle<usize>),
}

impl ImmOrJoinSize {
    fn size(&self) -> usize {
        match self {
            Self::ImmSize(sz) => { *sz },
            Self::JoinSize(_) => { 0 },
        }
    }
}

impl<'a: 'static, T: Staging<L> + SegmentReadWrite + Send + Clone + 'static, L: BlockLoader<BlockPtr> + Clone + 'static> HyperFile<'a, T, L> {
    fn spawn_load_data_block_read_path(&mut self, blk_id: BlockIndex, blk_ptr: BlockPtr, offset: usize, buf: &mut [u8]) -> Result<ImmOrJoinSize> {
        debug!("spawn_load_data_block_read_path - offset: {}, bytes: {}, block ptr: {}", offset, buf.len(), self.blk_ptr_decode_display(&blk_ptr));
        // in read path we would check dirty cache before do real data load
        if let Some(block) = self.cache.get(&blk_id) {
            // cache hit
            let slice = unsafe {
                std::slice::from_raw_parts(block.as_slice().as_ptr() as *const u8, block.as_slice().len())
            };
            let data_buf = unsafe {
                std::slice::from_raw_parts_mut(buf.as_mut_ptr() as *mut u8, buf.len())
            };
            data_buf.copy_from_slice(&slice[offset..offset + data_buf.len()]);
            block.unlock();
            return Ok(ImmOrJoinSize::ImmSize(data_buf.len()));
        }
        #[cfg(feature = "wal")]
        if self.wal.is_some() && BlockPtrFormat::is_on_staging(&blk_ptr) && (self.inode().get_last_cno() > self.inode().get_last_ondisk_cno()) {
            let (segid, staging_off) = self.blk_ptr_decode(&blk_ptr);
            if segid > self.inode().get_last_ondisk_cno() {
                let data_buf = unsafe {
                    std::slice::from_raw_parts_mut(buf.as_mut_ptr() as *mut u8, buf.len())
                };
                let flushing_segments = self.flushing_segments.clone();
                let join = self.rt.as_ref().unwrap().spawn(async move {
                    let lock = flushing_segments.read().await;
                    let Some(weak_data) = lock.get(&segid) else {
                        panic!("unable to find segid: {segid} from inflight flushing segments");
                    };
                    let Some(data) = weak_data.upgrade() else {
                        panic!("failed to get back shared data ref of inflight flushing segid: {segid}");
                    };
                    let start_off = staging_off + offset;
                    let end = start_off + data_buf.len();
                    data_buf.copy_from_slice(&data[start_off..end]);
                    data_buf.len()
                });
                return Ok(ImmOrJoinSize::JoinSize(join));
            }
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
            return Ok(ImmOrJoinSize::JoinSize(join));
        } else if BlockPtrFormat::is_dummy_value(&blk_ptr) {
            panic!("failed to get block index: {} from data blocks dirty cache for dummy block ptr", blk_id);
        } else if BlockPtrFormat::is_zero_block(&blk_ptr) {
            debug!("load_data_block - Fill Zero for block index: {}", blk_id);
            let data_buf = unsafe {
                std::slice::from_raw_parts_mut(buf.as_mut_ptr() as *mut u8, buf.len())
            };
            data_buf.fill(0);
            return Ok(ImmOrJoinSize::ImmSize(data_buf.len()));
        } else {
            panic!("spawn_load_data_block_read_path - offset: {}, bytes: {}, incorrect block ptr {} to load", offset, buf.len(), self.blk_ptr_decode_display(&blk_ptr));
        }
    }

    pub(crate) fn spawn_load_data_block_write_path(&mut self, blk_id: BlockIndex, blk_ptr: BlockPtr, offset: usize, buf: &mut [u8]) -> Result<ImmOrJoinSize> {
        debug!("spawn_load_data_block_write_path - offset: {}, bytes: {}, block ptr: {}", offset, buf.len(), self.blk_ptr_decode_display(&blk_ptr));
        #[cfg(feature = "wal")]
        if self.wal.is_some() && BlockPtrFormat::is_on_staging(&blk_ptr) && (self.inode().get_last_cno() > self.inode().get_last_ondisk_cno()) {
            let (segid, staging_off) = self.blk_ptr_decode(&blk_ptr);
            if segid > self.inode().get_last_ondisk_cno() {
                let data_buf = unsafe {
                    std::slice::from_raw_parts_mut(buf.as_mut_ptr() as *mut u8, buf.len())
                };
                let flushing_segments = self.flushing_segments.clone();
                let join = self.rt.as_ref().unwrap().spawn(async move {
                    let lock = flushing_segments.read().await;
                    let Some(weak_data) = lock.get(&segid) else {
                        panic!("unable to find segid: {segid} from inflight flushing segments");
                    };
                    let Some(data) = weak_data.upgrade() else {
                        panic!("failed to get back shared data ref of inflight flushing segid: {segid}");
                    };
                    let start_off = staging_off + offset;
                    let end = start_off + data_buf.len();
                    data_buf.copy_from_slice(&data[start_off..end]);
                    data_buf.len()
                });
                return Ok(ImmOrJoinSize::JoinSize(join));
            }
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
            return Ok(ImmOrJoinSize::JoinSize(join));
        } else if BlockPtrFormat::is_dummy_value(&blk_ptr) {
            if let Some(block) = self.cache.get(&blk_id) {
                // cache hit
                debug!("load_data_block - Cache Hit on data blocks dirty for block index: {}", blk_id);
                let slice = unsafe {
                    std::slice::from_raw_parts(block.as_slice().as_ptr() as *const u8, block.as_slice().len())
                };
                let data_buf = unsafe {
                    std::slice::from_raw_parts_mut(buf.as_mut_ptr() as *mut u8, buf.len())
                };
                data_buf.copy_from_slice(&slice[offset..offset + data_buf.len()]);
                block.unlock();
                return Ok(ImmOrJoinSize::ImmSize(data_buf.len()));
            }
            panic!("failed to get block index: {} from data blocks dirty cache for dummy block ptr", blk_id);
        } else if BlockPtrFormat::is_zero_block(&blk_ptr) {
            debug!("load_data_block - Fill Zero for block index: {}", blk_id);
            let data_buf = unsafe {
                std::slice::from_raw_parts_mut(buf.as_mut_ptr() as *mut u8, buf.len())
            };
            data_buf.fill(0);
            return Ok(ImmOrJoinSize::ImmSize(data_buf.len()));
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
        let len = req.buf.len();

        #[cfg(feature = "range-lock")]
        let range = off as u64..(off + len) as u64;
        #[cfg(feature = "range-lock")]
        if self.range_lock.try_lock(range.clone()) == false {
            let fh = req.fh.clone();
            let ctx = FileContext::reform_read(req, resp);
            fh.send_highprio(ctx);
            return Err(Error::new(ErrorKind::ResourceBusy, "read range locked"));
        }
        let mut buf = req.buf;

        let _permit = self.sema.clone().acquire_owned().await.unwrap();

        debug!("READ - off: {}, buf len: {}", off, len);
        if off >= self.inode.size() {
            #[cfg(feature = "range-lock")]
            self.range_lock.try_unlock(range);
            let _ = resp.to_read().try_send(Ok(0));
            return Ok(0);
        }
        // if requested buffer exceed file size, cut off tailing buffer
        if off + len > self.inode.size() {
            let exceeded_len = off + len - self.inode.size();
            let mid = len - exceeded_len;
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
        if joins.iter().all(|x| match x { ImmOrJoinSize::ImmSize(_) => true, ImmOrJoinSize::JoinSize(_) => false, }) {
            let actual_bytes = joins.iter().map(|x| x.size()).sum();
            assert!(total_bytes == actual_bytes);
            if actual_bytes > 0 {
                self.inode.update_atime();
            }
            #[cfg(feature = "range-lock")]
            self.range_lock.try_unlock(range);
            let _ = resp.to_read().try_send(Ok(actual_bytes));
            return Ok(total_bytes);
        }

        // no matter load backend data block success or not, update inode atime
        if total_bytes > 0 {
            self.inode.update_atime();
        }

        #[cfg(feature = "range-lock")]
        let mut range_lock = self.range_lock.clone();
        self.rt.as_ref().unwrap().spawn(async move {
            let mut actual_bytes = 0;
            while let Some(join) = joins.pop() {
                let bytes = match join {
                    ImmOrJoinSize::ImmSize(sz) => { sz },
                    ImmOrJoinSize::JoinSize(j) => { j.await.unwrap() },
                };
                actual_bytes += bytes;
            }
            assert!(total_bytes == actual_bytes);
            #[cfg(feature = "range-lock")]
            range_lock.unlock(range).await;
            let _ = resp.to_read().try_send(Ok(actual_bytes));
        });

        Ok(total_bytes)
    }

    // NOTE: for write process, we use following return value convention
    //
    // Ok(bytes) => write process end successfully, call try_send(Ok(bytes)) in handler main loop
    // Err(ResourceBusy) => task requeued or pass to next stage task
    // Err(e) => faile happend, call unlock range and try_send() for error
    //
    // for *_bh task, it is always end of of process, so call try_send(res) in anyway
    // for wal* task, it is always in the middle of process, so only handle Err(e) is enough
    // WalFlush is another story, it's internal process

    pub(crate) async fn absorb_write(&mut self, req: FileReqWrite<'a>, resp: FileResp, fetched: Vec<DataBlock>) -> Result<usize> {
        // insert fetched block back to dirty list,
        // for the case: block idx exists on dirty
        // means some other writes success before this write, let's ignore feched data and go ahead
        // to update the block data
        for block in fetched.into_iter() {
            let blk_idx = block.index();
            let None = self.cache.insert(blk_idx, block) else {
                panic!("BlockIndex {} already on data_blocks_dirty list", blk_idx);
            };
        }

        #[cfg(feature = "wal")]
        if let Some(_) = &mut self.wal {
            let fh = req.fh.clone();
            let ctx = FileContext::write_wal(req, resp);
            fh.send_cb(ctx);
            // TODO: change to ErrorKind::InProgress when it's stable
            return Err(Error::new(ErrorKind::ResourceBusy, "op resubmit to exec write wal"));
        }

        self.absorb_write_bh(req, resp).await
    }

    pub(crate) async fn absorb_write_bh(&mut self, mut req: FileReqWrite<'a>, resp: FileResp) -> Result<usize> {
        let off = req.offset;
        let len = req.buf.len();
        let buf = req.buf;
        let mut bytes_write = 0;

        // restore spawn_write permit
        let opt_permit = req.spawn_write_permit.take();
        assert!(opt_permit.is_some());

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
            // NOTE:
            // since we have update the dirty blocks cache,
            // if we failed in bmap insert, we have not way to rollback, so let's panic here
            let _ = self.bmap.insert(blk_idx, BlockPtrFormat::dummy_value()).await.expect("failed to insert dummy value to bmap for dirty blocks");
        }

        let oldsize = self.inode.size();
        if off + len > oldsize {
            self.inode.set_size(off + len);
        }
        self.inode.update_mtime();
        drop(opt_permit);

        #[cfg(feature = "range-lock")]
        let range = off as u64..(off + len) as u64;
        #[cfg(feature = "range-lock")]
        self.range_lock.try_unlock(range);

        if self.need_flush() {
            #[cfg(feature = "wal")]
            if self.wal.is_none() {
                self.flush().await?;
            } else if let Err(_) = self.flush_lock.try_lock() {
                // if flushing is on going, let skip it
            } else {
                let fh = req.fh;
                let ctx = FileContext::new_wal_flush(fh.clone());
                fh.send_highprio(ctx);
            }
            #[cfg(not(feature = "wal"))]
            self.flush().await?;
        }
        // consume resp
        let _ = resp.to_write();

        Ok(bytes_write)
    }

    pub(crate) async fn absorb_write_zero(&mut self, req: FileReqWriteZero<'a>, resp: FileResp, fetched: Vec<DataBlock>) -> Result<usize> {
        // insert fetched block back to dirty list,
        // for the case: block idx exists on dirty
        // means some other writes success before this write, let's ignore feched data and go ahead
        // to update the block data
        for block in fetched.into_iter() {
            let blk_idx = block.index();
            let None = self.cache.insert(blk_idx, block) else {
                panic!("BlockIndex {} already on data_blocks_dirty list", blk_idx);
            };
        }

        #[cfg(feature = "wal")]
        if let Some(_) = &mut self.wal {
            let fh = req.fh.clone();
            let ctx = FileContext::write_zero_wal(req, resp);
            fh.send_cb(ctx);
            // TODO: change to ErrorKind::InProgress when it's stable
            return Err(Error::new(ErrorKind::ResourceBusy, "op resubmit to exec write zero wal"));
        }

        self.absorb_write_zero_bh(req, resp).await
    }

    pub(crate) async fn absorb_write_zero_bh(&mut self, mut req: FileReqWriteZero<'a>, resp: FileResp) -> Result<usize> {
        let off = req.offset;
        let len = req.len;
        let mut bytes_write = 0;

        // restore spawn_write permit
        let opt_permit = req.spawn_write_permit.take();
        assert!(opt_permit.is_some());

        // NOTE:
        // since we have update the dirty blocks cache,
        // if we failed in bmap operations, we have not way to rollback, so let's panic here

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
        drop(opt_permit);

        #[cfg(feature = "range-lock")]
        let range = off as u64..(off + len) as u64;
        #[cfg(feature = "range-lock")]
        self.range_lock.try_unlock(range);

        if self.need_flush() {
            #[cfg(feature = "wal")]
            if self.wal.is_none() {
                self.flush().await?;
            } else if let Err(_) = self.flush_lock.try_lock() {
                // if flushing is on going, let skip it
            } else {
                let fh = req.fh;
                let ctx = FileContext::new_wal_flush(fh.clone());
                fh.send_highprio(ctx);
            }
            #[cfg(not(feature = "wal"))]
            self.flush().await?;
        }
        // consume resp
        let _ = resp.to_write_zero();

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
            while let Some(o) = joins.pop() {
                let bytes = match o {
                    ImmOrJoinSize::ImmSize(size) => size,
                    ImmOrJoinSize::JoinSize(j) => j.await.unwrap(),
                };
                actual_bytes += bytes;
            }
            req.fetched.append(&mut fetched);
            let _ = actual_bytes;
            let fh = req.fh.clone();
            let ctx = FileContext::write_absorb(req, resp);
            fh.send_cb(ctx);
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
            while let Some(o) = joins.pop() {
                let bytes = match o {
                    ImmOrJoinSize::ImmSize(size) => size,
                    ImmOrJoinSize::JoinSize(j) => j.await.unwrap(),
                };
                actual_bytes += bytes;
            }
            req.fetched.append(&mut fetched);
            let _ = actual_bytes;
            let fh = req.fh.clone();
            let ctx = FileContext::write_zero_absorb(req, resp);
            fh.send_cb(ctx);
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
    pub async fn spawn_write(&mut self, mut req: FileReqWrite<'a>, resp: FileResp) -> Result<usize> {
        let off = req.offset;
        let buf = req.buf;
        let len = buf.len();

        #[cfg(feature = "range-lock")]
        let range = off as u64..(off + len) as u64;
        #[cfg(feature = "range-lock")]
        if self.range_lock.try_lock(range) == false {
            let fh = req.fh.clone();
            let ctx = FileContext::reform_write(req, resp);
            fh.send_highprio(ctx);
            return Err(Error::new(ErrorKind::ResourceBusy, "read range locked"));
        }

        let permit = self.sema.clone().acquire_owned().await.unwrap();
        req.spawn_write_permit = Some(permit);

        debug!("WRITE - off: {}, buf len: {}", off, len);
        let v: Vec<BlockIndex> = self.write_prepare(off, len);
        if v.len() > 0 {
            // retrieve data by spawn
            self.spawn_write_retrieve(req, resp, v).await?;
            return Err(Error::new(ErrorKind::ResourceBusy, "hand over to spawn write retrieve"));
        }

        // no need to pre-retrive anything,
        // this is HAPPY PATH, continue on this runtime
        let actual_bytes = self.absorb_write(req, resp, Vec::new()).await?;
        assert!(len == actual_bytes);
        Ok(len)
    }

    pub async fn spawn_write_zero(&mut self, mut req: FileReqWriteZero<'a>, resp: FileResp) -> Result<usize> {
        let off = req.offset;
        let len = req.len;

        #[cfg(feature = "range-lock")]
        let range = off as u64..(off + len) as u64;
        #[cfg(feature = "range-lock")]
        if self.range_lock.try_lock(range) == false {
            let fh = req.fh.clone();
            let ctx = FileContext::reform_write_zero(req, resp);
            fh.send_highprio(ctx);
            return Err(Error::new(ErrorKind::ResourceBusy, "read range locked"));
        }

        let permit = self.sema.clone().acquire_owned().await.unwrap();
        req.spawn_write_permit = Some(permit);

        debug!("WRITE ZERO - off: {}, len: {}", off, len);
        let v: Vec<BlockIndex> = self.write_prepare(off, len);
        if v.len() > 0 {
            // retrieve data by spawn
            self.spawn_write_zero_retrieve(req, resp, v).await?;
            return Err(Error::new(ErrorKind::ResourceBusy, "hand over to spawn write zero retrieve"));
        }

        // no need to pre-retrive anything,
        // this is HAPPY PATH, continue on this runtime
        let actual_bytes = self.absorb_write_zero(req, resp, Vec::new()).await?;
        assert!(len == actual_bytes);
        Ok(len)
    }

    #[cfg(feature = "wal")]
    pub async fn spawn_write_wal(&mut self, req: FileReqWrite<'a>, resp: FileResp) -> Result<usize> {
        let off = req.offset;
        let len = req.buf.len();
        let last_seq = self.inode.get_last_seq();
        let wal_fut_opt = if let Some(wal) = &mut self.wal {
            let buf = req.buf;
            let fut = wal.write(last_seq, off, buf);
            Some(fut)
        } else {
            None
        };
        self.rt.as_ref().unwrap().spawn(async move {
            if let Some(wal_fut) = wal_fut_opt {
                let _ = wal_fut.await;
            }
            let fh = req.fh.clone();
            let ctx = FileContext::write_absorb_bh(req, resp);
            fh.send_cb(ctx);
        });
        Ok(len)
    }

    #[cfg(feature = "wal")]
    pub async fn spawn_write_zero_wal(&mut self, req: FileReqWriteZero<'a>, resp: FileResp) -> Result<usize> {
        let len = req.len;
        let off = req.offset;
        let last_seq = self.inode.get_last_seq();
        let wal_fut_opt = if let Some(wal) = &mut self.wal {
            let fut = wal.write_zero(last_seq, off, len);
            Some(fut)
        } else {
            None
        };
        self.rt.as_ref().unwrap().spawn(async move {
            if let Some(wal_fut) = wal_fut_opt {
                let _ = wal_fut.await;
            }
            let fh = req.fh.clone();
            let ctx = FileContext::write_zero_absorb_bh(req, resp);
            fh.send_cb(ctx);
        });
        Ok(len)
    }
}
