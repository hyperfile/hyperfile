//! IO function used by LocalSpawner reactor
use log::{debug, warn};
use btree_ondisk::BlockLoader;
use tokio::io::{Result, ErrorKind};
use tokio::task::JoinHandle;
use super::HyperFile;
use crate::{BlockIndex, BlockIndexIter};
use crate::meta_format::BlockPtr;
use crate::staging::Staging;
use crate::segment::{SegmentReadWrite, SegmentId};
use crate::file::handler::{FileReqRead, FileReqWrite, FileReqWriteZero, FileResp, FileContext};

impl<'a: 'static, T: Staging<T, L> + SegmentReadWrite + Send + Sync + Clone + 'static, L: BlockLoader<BlockPtr> + Clone + Send + Sync + 'static> HyperFile<'a, T, L> {
    fn spawn_load_data_block_read_path(&self, blk_id: BlockIndex, blk_ptr: BlockPtr, offset: usize, buf: &mut [u8], _ra: bool) -> Result<JoinHandle<usize>> {
        debug!("load_data_block - block ptr: {}", blk_ptr);
        // in read path we would check both data and dirty cache before do real data load
        // check data cache first
        if let Some(block) = self.data_blocks_cache.borrow_mut().get(&blk_id) {
            // cache hit
            debug!("load_data_block - Cache Hit on data blocks cache for block index: {}", blk_id);
            let slice = unsafe {
                std::slice::from_raw_parts(block.as_slice().as_ptr() as *const u8, block.as_slice().len())
            };
            let data_buf = unsafe {
                std::slice::from_raw_parts_mut(buf.as_mut_ptr() as *mut u8, buf.len())
            };
            let join = tokio::task::spawn(async move {
                data_buf.copy_from_slice(&slice[offset..offset + data_buf.len()]);
                data_buf.len()
            });
            return Ok(join);
        }
        // then check dirty cache
        if let Some(block) = self.data_blocks_dirty.get(&blk_id) {
            // cache hit
            debug!("load_data_block - Cache Hit on data blocks dirty for block index: {}", blk_id);
            let slice = unsafe {
                std::slice::from_raw_parts(block.as_slice().as_ptr() as *const u8, block.as_slice().len())
            };
            let data_buf = unsafe {
                std::slice::from_raw_parts_mut(buf.as_mut_ptr() as *mut u8, buf.len())
            };
            let join = tokio::task::spawn(async move {
                data_buf.copy_from_slice(&slice[offset..offset + data_buf.len()]);
                data_buf.len()
            });
            return Ok(join);
        }
        if blk_ptr.is_on_origin() {
            let blk_id = blk_ptr.get_origin_blkid();
            debug!("load_data_block - Cache Miss for block index: {}", blk_id);
            // load from origin file
            let oofile = self.oofile.borrow_mut().clone();
            let client = self.client.clone();
            let data_buf = unsafe {
                std::slice::from_raw_parts_mut(buf.as_mut_ptr() as *mut u8, buf.len())
            };
            let join = tokio::task::spawn(async move {
                let _ = oofile.read(&client, blk_id, offset, data_buf).await;
                data_buf.len()
            });
            return Ok(join);
        } else if blk_ptr.is_on_staging() {
            let (segid, staging_off) = blk_ptr.get_staging_info();
            let staging = self.staging.clone();
            let data_block_size = self.config.data_block_size;
            let data_buf = unsafe {
                std::slice::from_raw_parts_mut(buf.as_mut_ptr() as *mut u8, buf.len())
            };
            let join = tokio::task::spawn(async move {
                let _ = staging.load_data_block(SegmentId::from_raw(segid), staging_off, offset, data_block_size, data_buf).await;
                data_buf.len()
            });
            return Ok(join);
        } else if blk_ptr.is_dummy() {
            panic!("failed to get block index: {} from data blocks dirty cache for dummy block ptr", blk_id);
        } else if blk_ptr.is_zero_block() {
            debug!("load_data_block - Fill Zero for block index: {}", blk_id);
            let data_buf = unsafe {
                std::slice::from_raw_parts_mut(buf.as_mut_ptr() as *mut u8, buf.len())
            };
            let join = tokio::task::spawn(async move {
                data_buf.fill(0);
                data_buf.len()
            });
            return Ok(join);
        } else {
            panic!("incorrect block ptr {} to load", blk_ptr);
        }
    }

    fn spawn_load_data_block_write_path(&self, blk_id: BlockIndex, blk_ptr: BlockPtr, offset: usize, buf: &mut [u8], _ra: bool) -> Result<JoinHandle<usize>> {
        if blk_ptr.is_on_origin() {
            let blk_id = blk_ptr.get_origin_blkid();
            debug!("load_data_block - block ptr: {}", blk_ptr);
            // check cache first
            if let Some(block) = self.data_blocks_cache.borrow_mut().get(&blk_id) {
                // cache hit
                debug!("load_data_block - Cache Hit on data blocks cache for block index: {}", blk_id);
                let slice = unsafe {
                    std::slice::from_raw_parts(block.as_slice().as_ptr() as *const u8, block.as_slice().len())
                };
                let data_buf = unsafe {
                    std::slice::from_raw_parts_mut(buf.as_mut_ptr() as *mut u8, buf.len())
                };
                let join = tokio::task::spawn(async move {
                    data_buf.copy_from_slice(&slice[offset..offset + data_buf.len()]);
                    data_buf.len()
                });
                return Ok(join);
            }
            debug!("load_data_block - Cache Miss for block index: {}", blk_id);
            // load from origin file
            let oofile = self.oofile.borrow_mut().clone();
            let client = self.client.clone();
            let data_buf = unsafe {
                std::slice::from_raw_parts_mut(buf.as_mut_ptr() as *mut u8, buf.len())
            };
            let join = tokio::task::spawn(async move {
                let _ = oofile.read(&client, blk_id, offset, data_buf).await;
                data_buf.len()
            });
            return Ok(join);
        } else if blk_ptr.is_on_staging() {
            let (segid, staging_off) = blk_ptr.get_staging_info();
            let staging = self.staging.clone();
            let data_block_size = self.config.data_block_size;
            let data_buf = unsafe {
                std::slice::from_raw_parts_mut(buf.as_mut_ptr() as *mut u8, buf.len())
            };
            let join = tokio::task::spawn(async move {
                let _ = staging.load_data_block(SegmentId::from_raw(segid), staging_off, offset, data_block_size, data_buf).await;
                data_buf.len()
            });
            return Ok(join);
        } else if blk_ptr.is_dummy() {
            if let Some(block) = self.data_blocks_dirty.get(&blk_id) {
                // cache hit
                debug!("load_data_block - Cache Hit on data blocks dirty for block index: {}", blk_id);
                let slice = unsafe {
                    std::slice::from_raw_parts(block.as_slice().as_ptr() as *const u8, block.as_slice().len())
                };
                let data_buf = unsafe {
                    std::slice::from_raw_parts_mut(buf.as_mut_ptr() as *mut u8, buf.len())
                };
                let join = tokio::task::spawn(async move {
                    data_buf.copy_from_slice(&slice[offset..offset + data_buf.len()]);
                    data_buf.len()
                });
                return Ok(join);
            }
            panic!("failed to get block index: {} from data blocks dirty cache for dummy block ptr", blk_id);
        } else if blk_ptr.is_zero_block() {
            debug!("load_data_block - Fill Zero for block index: {}", blk_id);
            let data_buf = unsafe {
                std::slice::from_raw_parts_mut(buf.as_mut_ptr() as *mut u8, buf.len())
            };
            let join = tokio::task::spawn(async move {
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
        let blk_idx = (off / self.config.data_block_size) as BlockIndex;
        let blk_off = off % self.config.data_block_size;
        let blk_count = (buf.len() + self.config.data_block_size - 1) / self.config.data_block_size;
        let mut bytes_read = 0;
        let ra = if self.hyper_config.feature.read_ahead_chunk_size > 0 {
            true
        } else {
            false
        };

        debug!("collect_block_ptr - block index {blk_idx}, block offset {blk_off}, block count {blk_count}, read ahead: {ra}");

        // blocks need to be read back
        let mut blocks = Vec::new();

        let head_off = off % self.config.data_block_size;

        let (mut next_blk_idx, next_slice) = if head_off != 0 {
            let blk_ptr = match self.bmap.lookup(&blk_idx).await {
                Ok(ptr) => { ptr },
                Err(e) => {
                    match e.kind() {
                        ErrorKind::NotFound => { BlockPtr::new_zero_block() },
                        _ => {
                            warn!("collect_block_ptr - lookup bmap for block index {blk_idx} error: {}", e);
                            return Err(e);
                        },
                    }
                },
            };
            if head_off + buf.len() < self.config.data_block_size {
                let bytes = buf.len();
                blocks.push((blk_idx, blk_ptr, head_off, buf));
                bytes_read += bytes;
                return Ok((bytes_read, blocks));
            }

            // we need to split head bytes if offset is not aligned to block size
            let split_off = self.config.data_block_size - head_off;
            let (this, next) = buf.split_at_mut(split_off);
            debug!("       - read head data block for block ptr {} at offset {} len {}", blk_ptr, head_off, this.len());
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

        for b in next_slice.chunks_mut(self.config.data_block_size) {
            debug!("     - lookup block index {next_blk_idx}");
            let blk_ptr = match self.bmap.lookup(&next_blk_idx).await {
                Ok(ptr) => { ptr },
                Err(e) => {
                    match e.kind() {
                        ErrorKind::NotFound => { BlockPtr::new_zero_block() },
                        _ => {
                            warn!("collect_block_ptr - lookup bmap for block index {next_blk_idx} error: {}", e);
                            return Err(e);
                        },
                    }
                },
            };
            debug!("       - read data block for block ptr {} at offset {} len {}", blk_ptr, 0, b.len());
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
        // if requested buffer excee file size, cut off tailing buffer
        if off + buf.len() > self.inode.size() {
            let exceeded_len = off + buf.len() - self.inode.size();
            let mid = buf.len() - exceeded_len;
            debug!("READ - buf len shrink to: {}, due to file size {}", mid, self.inode.size());
            (buf, _) = buf.split_at_mut(mid);
        }

        let (total_bytes, blocks) = self.collect_block_ptr(off, buf).await?;
        let mut joins = Vec::new();
        for (blk_idx, blk_ptr, off, buf) in blocks {
            let join = self.spawn_load_data_block_read_path(blk_idx, blk_ptr, off, buf, false)?;
            joins.push(join)
        }

        let mut actual_bytes = 0;
        while let Some(j) = joins.pop() {
            let bytes = j.await?;
            actual_bytes += bytes;
        }
        assert!(total_bytes == actual_bytes);
        if actual_bytes > 0 {
            self.inode.update_atime();
        }
        let _ = resp.to_read().try_send(Ok(actual_bytes));

        Ok(total_bytes)
    }

    pub(crate) async fn absorb_write(&mut self, off: usize, buf: &[u8]) -> Result<usize> {
        let len = buf.len();
        let mut bytes_write = 0;

        // restore spawn_write permit
        let opt_permit = self.spawn_write_permit.take();
        assert!(opt_permit.is_some());

        let blk_iter = BlockIndexIter::new(off, len, self.config.data_block_size);
		let mut next_slice = buf;
        for (blk_idx, off, len) in blk_iter {
            let (this, next) = next_slice.split_at(len);
            debug!("      - update cache block index {}, offset {}, len {}", blk_idx, off, len);
            self.update_cache(blk_idx, off, this);
            bytes_write += this.len();
            // don't care if insert failed for exist
            match self.bmap.try_insert(blk_idx, BlockPtr::dummy_value()).await {
                Err(e) => {
                    if e.kind() != ErrorKind::AlreadyExists { return Err(e); }
                },
                Ok(_) => {},
            }
            next_slice = next;
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

    pub(crate) async fn absorb_write_zero(&mut self, off: usize, len: usize) -> Result<usize> {
        let mut bytes_write = 0;

        // restore spawn_write permit
        let opt_permit = self.spawn_write_permit.take();
        assert!(opt_permit.is_some());

        let oldsize = self.inode.size();
        let blk_iter = BlockIndexIter::new(off, len, self.config.data_block_size);
        for (blk_idx, start_off, data_len) in blk_iter {
            // for a complete block,
            // no need to update data in cache, because is's already all zero
            // and insert zero block into block map
            if start_off == 0 && data_len == self.config.data_block_size {
                // insert or update
                let _ = self.bmap.insert(blk_idx, BlockPtr::new_zero_block()).await?;
                bytes_write += data_len;
                continue;
            }
            // for a incomplete block
            // last block execption which start off from block start and len exceed current file
            // TODO: merge this with new cache impl
            if start_off == 0 && (blk_idx as usize * self.config.data_block_size) + start_off + data_len > oldsize {
                // insert or update
                let _ = self.bmap.insert(blk_idx, BlockPtr::new_zero_block()).await?;
                bytes_write += data_len;
                continue;
            }
            debug!("      - update cache block index {}, offset {}, len {}", blk_idx, off, len);
            let mut zero = Vec::with_capacity(data_len);
            zero.resize(data_len, 0);
            self.update_cache(blk_idx, start_off, &zero);
            // don't care if insert failed for exist
            match self.bmap.try_insert(blk_idx, BlockPtr::dummy_value()).await {
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
        drop(opt_permit);

        let flushed = self.try_flush().await?;
        // TODO: placehold for pref metrics
        let _ = flushed;

        Ok(bytes_write)
    }

    fn spawn_write_retrieve(&self, req: FileReqWrite<'a>,  resp: FileResp, list: Vec<(BlockPtr, BlockIndex)>) -> Result<()> {
        let mut joins = Vec::new();
        for (blk_ptr, blk_idx) in list {
            debug!("retrive block ptr {} for block index {}", blk_ptr, blk_idx);
            if let Some(block) = self.data_blocks_cache.borrow().peek(&blk_idx) {
                let buf = block.as_mut_slice();
                let join = self.spawn_load_data_block_write_path(blk_idx, blk_ptr, 0, buf, false)?;
                joins.push(join);
            } else {
                panic!("block index {} is not found on cache list, *SHOULD NOT* happend", blk_idx);
            }
        }

        tokio::task::spawn(async move {
            let mut actual_bytes = 0;
            while let Some(j) = joins.pop() {
                let bytes = j.await.unwrap();
                actual_bytes += bytes;
            }
            let _ = actual_bytes;
            let fh = req.absorb_fh.clone();
            let ctx = FileContext::write_absorb(req, resp);
            fh.send(ctx);
        });

        Ok(())
    }

    // duplicate logic of spawn_write_retrieve()
    // TODO: can be merge with spawn_write_retrieve
    fn spawn_write_zero_retrieve(&self, req: FileReqWriteZero<'a>,  resp: FileResp, list: Vec<(BlockPtr, BlockIndex)>) -> Result<()> {
        let mut joins = Vec::new();
        for (blk_ptr, blk_idx) in list {
            debug!("retrive block ptr {} for block index {}", blk_ptr, blk_idx);
            if let Some(block) = self.data_blocks_cache.borrow().peek(&blk_idx) {
                let buf = block.as_mut_slice();
                let join = self.spawn_load_data_block_write_path(blk_idx, blk_ptr, 0, buf, false)?;
                joins.push(join);
            } else {
                panic!("block index {} is not found on cache list, *SHOULD NOT* happend", blk_idx);
            }
        }

        tokio::task::spawn(async move {
            let mut actual_bytes = 0;
            while let Some(j) = joins.pop() {
                let bytes = j.await.unwrap();
                actual_bytes += bytes;
            }
            let _ = actual_bytes;
            let fh = req.absorb_fh.clone();
            let ctx = FileContext::write_zero_absorb(req, resp);
            fh.send(ctx);
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
        let v: Vec<(BlockPtr, BlockIndex)> = self.write_prepare(off, len).await;
        if v.len() > 0 {
            // retrieve data by spawn
            self.spawn_write_retrieve(req, resp, v)?;
            self.spawn_write_permit = Some(permit);
            return Ok(len);
        }

        // pass permit through inner fields to next fn
        self.spawn_write_permit = Some(permit);

        // no need to pre-retrive anything,
        // this is HAPPY PATH, continue on this runtime
        let actual_bytes = self.absorb_write(off, buf).await?;
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
        let v: Vec<(BlockPtr, BlockIndex)> = self.write_prepare(off, len).await;
        if v.len() > 0 {
            // retrieve data by spawn
            self.spawn_write_zero_retrieve(req, resp, v)?;
            self.spawn_write_permit = Some(permit);
            return Ok(len);
        }

        // pass permit through inner fields to next fn
        self.spawn_write_permit = Some(permit);

        // no need to pre-retrive anything,
        // this is HAPPY PATH, continue on this runtime
        let actual_bytes = self.absorb_write_zero(off, len).await?;
        assert!(len == actual_bytes);

        // kick off response
        let _ = resp.to_write().try_send(Ok(actual_bytes));
        Ok(len)
    }
}
