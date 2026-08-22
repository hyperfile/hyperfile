//! IO function used by LocalSpawner reactor
use std::io::{Result, Error, ErrorKind};
use std::sync::Arc;
use log::{debug, warn};
use btree_ondisk::{BlockLoader, NodeCache};
use tokio::task::JoinHandle;
use tokio::sync::Semaphore;
use crate::{BlockIndex, BlockPtr, BlockIndexIter, SegmentId};
use crate::staging::Staging;
use crate::segment::SegmentReadWrite;
use crate::file::HyperTrait;
use crate::meta_format::BlockPtrFormat;
use crate::buffer::DataBlock;
use super::file::{HyperFile, ReadOp};
use super::handler::{FileReqRead, FileReqReadAhead, FileReqWrite, FileReqWriteZero, FileResp, FileContext, ChannelGroup, BlockAction};

/// Where a write retrieve sends the request once its loads finish.
pub(crate) enum AfterRetrieve {
    /// To the absorb, which under WAL still has the WAL write ahead of it.
    Absorb,
    /// Straight to applying the data. For a request whose WAL write has
    /// already happened and must not be repeated — a flush took a block
    /// out from under it and it came back for that block only.
    AbsorbBh,
}

pub(crate) enum ImmOrJoinSize {
    ImmSize(usize),
    /// A spawned load. `Err` means the block was not filled, which the
    /// caller has to treat as a failed write rather than absorb: the
    /// buffer is a fresh block, so absorbing it would replace the block's
    /// contents with zeroes and persist that.
    JoinSize(JoinHandle<Result<usize>>),
}

impl<'a, T, L, C> HyperFile<'a, T, L, C>
    where
        'a: 'static,
        T: Staging<L> + SegmentReadWrite + Send + Clone + 'static,
        L: BlockLoader<BlockPtr> + Clone + 'static,
        C: NodeCache<BlockPtr> + Clone,
{
    pub(crate) fn spawn_load_data_block_write_path(&mut self, blk_id: BlockIndex, blk_ptr: BlockPtr, offset: usize, buf: &mut [u8]) -> Result<ImmOrJoinSize> {
        debug!("spawn_load_data_block_write_path - block index: {}, offset: {}, bytes: {}, block ptr: {}",
            blk_id, offset, buf.len(), self.blk_ptr_decode_display(&blk_ptr));
        #[cfg(feature = "wal")]
        if self.wal.is_some() && BlockPtrFormat::is_on_staging(&blk_ptr) && (self.inode().get_last_cno() > self.inode().get_last_ondisk_cno()) {
            let (segid, staging_off) = self.blk_ptr_decode(&blk_ptr);
            if segid > self.inode().get_last_ondisk_cno() {
                let data_buf = unsafe {
                    std::slice::from_raw_parts_mut(buf.as_mut_ptr() as *mut u8, buf.len())
                };
                let flushing_segments = self.flushing_segments.clone();
                let staging = self.staging.clone();
                let data_block_size = self.config.meta.data_block_size;
                let join = self.rt.as_ref().unwrap().spawn(async move {
                    // As on the read path: the flush can land between the
                    // decision to read this from memory and getting here,
                    // taking the entry and the pinned buffer with it. The
                    // same bytes are on staging by then, so read them from
                    // there rather than treating a finished flush as a bug.
                    let copied = {
                        let lock = flushing_segments.read().await;
                        match lock.get(&segid).and_then(|weak_data| weak_data.upgrade()) {
                            Some(data) => {
                                let start_off = staging_off + offset;
                                let end = start_off + data_buf.len();
                                data_buf.copy_from_slice(&data[start_off..end]);
                                true
                            },
                            None => false,
                        }
                    };
                    let len = data_buf.len();
                    if !copied {
                        debug!("write retrieve - segid {} no longer held in memory, reading it from staging", segid);
                        staging.load_data_block(segid, staging_off, offset, data_block_size, data_buf).await?;
                    }
                    Ok(len)
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
                let len = data_buf.len();
                staging.load_data_block(segid, staging_off, offset, data_block_size, data_buf).await?;
                Ok(len)
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
            panic!("spawn_load_data_block_write_path - block index: {}, offset: {}, bytes: {}, incorrect block ptr {} to load",
                blk_id, offset, buf.len(), self.blk_ptr_decode_display(&blk_ptr));
        }
    }

    // for spawn_read/spawn_write resp is based on mpsc channel
    // so use try_send() instead send()
    /// Run a read-only block action, fetching the block off the handler
    /// task when it has to be fetched at all.
    ///
    /// The handler takes `&mut self` and runs one context at a time, so
    /// anything it awaits serializes every other request. A cache hit
    /// awaits nothing and stays here; a miss is an object-store round
    /// trip and must not.
    ///
    /// A miss can move off because the block it needs is owned rather
    /// than borrowed from the file — `Cache::new_block` hands out a
    /// fresh one, exactly as the write path's retrieve does — so the
    /// load, the closure and the gate all travel into a spawned task and
    /// nothing here stays borrowed past the return. `HyperFile::block`
    /// cannot do this: it yields a `BlockRef` borrowed from `&mut self`.
    ///
    /// The filled block is requeued for caching rather than dropped; see
    /// `absorb_block`. Caching is what makes repeated block access cost
    /// one request instead of one each.
    pub async fn spawn_block_ref(
        &mut self,
        blk_idx: BlockIndex,
        f: Box<dyn FnOnce(&[u8]) + Send>,
        gate: crate::file::handler::BlockActionGate,
        resp: FileResp,
        fh: ChannelGroup<FileContext<'a>>,
    ) -> Result<()>
    {
        if !self.flags.is_readable() {
            let e = Self::ebadf_bad_access_mode();
            resp.fail_with_block(Self::ebadf_bad_access_mode());
            return Err(e);
        }

        // Stop the world while a flush is in progress, as the byte read
        // path does. While this fetched inline the arm could not overlap
        // a flush; now that it spawns, it could.
        // Under WAL this need not wait at all.
        if self.must_wait_for_flush() {
            let ctx = FileContext::reform_with_block(blk_idx, false, BlockAction::Ref(f), gate, fh.clone(), resp);
            let _ = fh.send_highprio(ctx);
            return Err(Error::new(ErrorKind::ResourceBusy, "flush is ongoing"));
        }

        // Resident: no I/O, so run it here and answer.
        if self.cache.has(&blk_idx) {
            self.staging.read_timing().add_cache_hit();
            let block = self.cache.get(&blk_idx)
                .expect("cache lost a block between has() and get() under &mut self");
            {
                let cancelled = gate.lock().unwrap_or_else(|e| e.into_inner());
                if !*cancelled {
                    f(block.as_slice());
                }
            }
            block.unlock();
            resp.answer_with_block(Ok(true));
            return Ok(());
        }

        let blk_ptr = match self.bmap.lookup(&blk_idx).await {
            Ok(p) => p,
            Err(e) if e.kind() == ErrorKind::NotFound => {
                resp.answer_with_block(Ok(false));
                return Ok(());
            },
            Err(e) => {
                resp.fail_with_block(Error::new(e.kind(), format!("{e}")));
                return Err(e);
            },
        };
        if BlockPtrFormat::is_zero_block(&blk_ptr) {
            resp.answer_with_block(Ok(false));
            return Ok(());
        }

        let block = self.cache.new_block(blk_idx);
        block.set_should_cache();
        let buf = block.as_mut_slice();
        let join = self.spawn_load_data_block_write_path(blk_idx, blk_ptr, 0, buf)?;

        self.rt.as_ref().unwrap().spawn(async move {
            match join {
                ImmOrJoinSize::ImmSize(_) => {},
                ImmOrJoinSize::JoinSize(j) => { let _ = j.await; },
            }
            // The gate is taken only now, after the load, so a caller
            // that cancels waits for a closure body and never for I/O.
            {
                let cancelled = gate.lock().unwrap_or_else(|e| e.into_inner());
                if !*cancelled {
                    f(block.as_slice());
                }
            }
            resp.answer_with_block(Ok(true));
            // Hand the block over to be cached. The caller already has
            // its answer, so this is off the critical path.
            let _ = fh.send_cb(FileContext::block_absorb(blk_idx, block));
        });

        Ok(())
    }

    /// Whether an operation has to wait for a flush that is in progress.
    ///
    /// Without WAL it does. The flush is rewriting the bmap and draining
    /// the dirty cache underneath, and there is nowhere else to get the
    /// data from, so the world stops until it finishes.
    ///
    /// With WAL it does not, and this is the point of the WAL read path.
    /// The segment being flushed stays pinned in memory and registered in
    /// `flushing_segments`, and the WAL makes the flush's completion a
    /// given: a failure is recovered by replaying the WAL, not by
    /// unwinding what the flush already published. So the bmap can point
    /// at the new segment before it reaches staging, and blocks in it can
    /// be served from the pinned buffer as though it were already there —
    /// that is what the planner's `Inmem` op is for. Waiting would give up
    /// the point of writing the WAL first, which is that a flush stops
    /// blocking the front end.
    ///
    /// Writes may overlap a flush too. Their two halves can straddle one,
    /// because a WAL write hands the handler task back in between, so
    /// `absorb_write_bh` fetches again anything the flush took rather than
    /// applying the write over a block rebuilt from nothing.
    ///
    /// Note what a write holds while it overlaps: without `range-lock` the
    /// per-file permit, which is a single permit for the whole file, and
    /// with `range-lock` the range it is writing. Either is held across
    /// the WAL PUT, so an overlapping write costs concurrent readers of
    /// the *same* blocks a great deal, and readers of other blocks
    /// nothing at all — but only under `range-lock`, where the permit is
    /// unbounded and the range is what excludes.
    fn must_wait_for_flush(&self) -> bool {
        if !self.state.is_flushing() {
            return false;
        }
        #[cfg(feature = "wal")]
        if self.wal.is_some() {
            return false;
        }
        true
    }

    /// Warm a range into the data cache, keeping the object requests off
    /// the handler task.
    ///
    /// Planning happens here, costing the same bmap walk a read of the
    /// range would. The fetches and the copying into blocks happen in a
    /// spawned task, which hands each block back through `block_absorb` to
    /// be installed and answers the caller when it is done — so a wide
    /// read-ahead does not stall everything else for the length of its
    /// requests.
    pub async fn spawn_read_ahead(&mut self, req: FileReqReadAhead<'a>, resp: FileResp) -> Result<()> {
        let bs = self.config.meta.data_block_size;
        let i_size = self.inode().size() as usize;
        if req.len == 0 || req.offset >= i_size {
            let _ = resp.to_read_ahead().send(Ok(0));
            return Ok(());
        }
        let start = req.offset / bs * bs;
        let end = ((req.offset + req.len).min(i_size) + bs - 1) / bs * bs;
        let span = end - start;
        if span == 0 {
            let _ = resp.to_read_ahead().send(Ok(0));
            return Ok(());
        }

        // Same planning a read does: resident blocks are skipped, the rest
        // coalesced.
        let plan = self.plan_read(start, span).await?;

        // Only ops that need staging are worth carrying over; a hole or a
        // cache hit has nothing to install.
        let mut work: Vec<(SegmentId, usize, usize, usize)> = Vec::new();
        let mut consumed = 0usize;
        for op in plan {
            let dst_len = op.dst_len();
            match op {
                ReadOp::Cache { .. } | ReadOp::Zero { .. } => {},
                #[cfg(feature = "wal")]
                ReadOp::Inmem { segid, s3_off, dst_len: _ } => work.push((segid, s3_off, consumed, dst_len)),
                ReadOp::Range { segid, s3_off, dst_len: _ } => work.push((segid, s3_off, consumed, dst_len)),
            }
            consumed += dst_len;
        }
        if work.is_empty() {
            let _ = resp.to_read_ahead().send(Ok(0));
            return Ok(());
        }

        let staging = self.staging.clone();
        let data_block_size = bs;
        let fh = req.fh.clone();
        #[cfg(feature = "wal")]
        let flushing_segments = self.flushing_segments.clone();

        self.rt.as_ref().unwrap().spawn(async move {
            let mut buf = vec![0u8; span];
            let mut failed = None;
            for (segid, src_off, dst_off, len) in work.iter().copied() {
                #[cfg(feature = "wal")]
                {
                    // A segment still being written out is in memory.
                    let copied = {
                        let lock = flushing_segments.read().await;
                        match lock.get(&segid).and_then(|weak| weak.upgrade()) {
                            Some(data) => {
                                buf[dst_off..dst_off + len]
                                    .copy_from_slice(&data[src_off..src_off + len]);
                                true
                            },
                            None => false,
                        }
                    };
                    if copied {
                        continue;
                    }
                }
                if let Err(e) = staging.load_range(segid, src_off, &mut buf[dst_off..dst_off + len]).await {
                    warn!("read ahead load failed: {:?}", e);
                    failed = Some(e);
                    break;
                }
            }

            if let Some(e) = failed {
                // Nothing is installed. A read of the range still works, it
                // just pays for its own fetch.
                let _ = resp.to_read_ahead().send(Err(e));
                return;
            }

            let mut cached = 0usize;
            for (_, _, dst_off, len) in work.iter().copied() {
                let mut pos = dst_off;
                while pos + data_block_size <= dst_off + len {
                    let blk_idx = ((start + pos) / data_block_size) as BlockIndex;
                    let block = DataBlock::new(blk_idx, data_block_size);
                    block.set_should_cache();
                    block.as_mut_slice().copy_from_slice(&buf[pos..pos + data_block_size]);
                    let ctx = FileContext::block_absorb(blk_idx, block);
                    if fh.send_cb(ctx).is_err() {
                        break;
                    }
                    cached += 1;
                    pos += data_block_size;
                }
            }
            let _ = resp.to_read_ahead().send(Ok(cached));
        });

        Ok(())
    }

    pub async fn spawn_read(&mut self, mut req: FileReqRead<'a>, resp: FileResp) -> Result<usize> {
        // POSIX: a read on a handle not opened for reading fails with
        // EBADF. Checked before the flush-state test and the range
        // lock, for the same reason as spawn_write(): the request must
        // not acquire state that an error path would have to unwind.
        // Errors are answered here rather than by the caller. The
        // response may be a oneshot — an owned read cannot clone its
        // sender the way the borrowed form did — so there is exactly
        // one owner of the reply, and it is this function. The two
        // requeue paths below deliberately do not answer: they hand the
        // response on to the retried request.
        if !self.flags.is_readable() {
            let e = Self::ebadf_bad_access_mode();
            resp.fail_read(Self::ebadf_bad_access_mode());
            return Err(e);
        }
        let off = req.offset;
        let len = req.buf.len();

        // Stop the world while a flush runs, unless WAL makes that
        // unnecessary. See `read_must_wait_for_flush`.
        if self.must_wait_for_flush() {
            let fh = req.fh.clone();
            let ctx = FileContext::reform_read(req, resp);
            let _ = fh.send_highprio(ctx);
            return Err(Error::new(ErrorKind::ResourceBusy, "flush is ongoing"));
        }

        #[cfg(feature = "range-lock")]
        let range = off as u64..(off + len) as u64;
        // A flush waiting to start has to see every range released. Taking
        // a new one here would keep pushing that moment out of reach, and
        // a steady stream of reads would starve the flush indefinitely.
        #[cfg(feature = "range-lock")]
        if self.state.is_flush_pending() {
            let fh = req.fh.clone();
            let ctx = FileContext::reform_read(req, resp);
            let _ = fh.send_highprio(ctx);
            return Err(Error::new(ErrorKind::ResourceBusy, "flush is pending"));
        }
        #[cfg(feature = "range-lock")]
        if self.range_lock.try_lock(range.clone()) == false {
            let fh = req.fh.clone();
            let ctx = FileContext::reform_read(req, resp);
            let _ = fh.send_highprio(ctx);
            return Err(Error::new(ErrorKind::ResourceBusy, "read range locked"));
        }
        // Take the per-file permit without waiting. Waiting here would
        // block the handler task, and the handler is the only thing that
        // can run the callback hop which releases the permit — a write's
        // retrieve carries it from `spawn_write` until `absorb_write`.
        // Waiting would therefore deadlock against exactly the work that
        // would let the wait finish. Put the request back instead; `cb`
        // outranks `highprio`, so the hop we are waiting on runs first.
        let permit = match self.sema.clone().try_acquire_owned() {
            Ok(p) => p,
            Err(_) => {
                let fh = req.fh.clone();
                let ctx = FileContext::reform_read(req, resp);
                let _ = fh.send_highprio(ctx);
                return Err(Error::new(ErrorKind::ResourceBusy, "per-file permit busy"));
            },
        };

        let mut buf = req.buf;
        // Taken now so the send sites below can hand it back. `buf`
        // already points into it; nothing touches the `Vec` itself until
        // the reads are done and it is moved into the response.
        let owned = req.owned.take();
        let _permit = permit;

        debug!("READ - off: {}, buf len: {}", off, len);
        if off >= self.inode.size() {
            #[cfg(feature = "range-lock")]
            self.range_lock.try_unlock(range);
            resp.answer_read(0, owned);
            return Ok(0);
        }
        // if requested buffer exceed file size, cut off tailing buffer
        if off + len > self.inode.size() {
            let exceeded_len = off + len - self.inode.size();
            let mid = len - exceeded_len;
            debug!("READ - buf len shrink to: {}, due to file size {}", mid, self.inode.size());
            (buf, _) = buf.split_at_mut(mid);
        }

        let buf_len = buf.len();
        if buf_len == 0 {
            if !self.flags.is_noatime() {
                self.inode.update_atime();
            }
            #[cfg(feature = "range-lock")]
            self.range_lock.try_unlock(range);
            resp.answer_read(0, owned);
            return Ok(0);
        }

        // Stage 1: build coalesced plan (shared with direct API).
        let plan = match self.plan_read(off, buf_len).await {
            Ok(p) => p,
            Err(e) => {
                resp.fail_read(Error::new(e.kind(), format!("{e}")));
                return Err(e);
            },
        };
        debug!("spawn_read - planned {} ops for {} bytes", plan.len(), buf_len);

        // Stage 2: walk plan + buf in lockstep. Synchronous ops
        // (cache copy, zero fill) happen on the main reactor
        // thread; range / wal-inmem ops get spawned onto
        // self.rt with backpressure from a per-call semaphore.
        let read_max_concurrency = self.config.runtime.read_max_concurrency;
        let semaphore = Arc::new(Semaphore::new(read_max_concurrency));

        let mut joins: Vec<JoinHandle<Result<usize>>> = Vec::new();
        let mut imm_bytes: usize = 0;
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
                    imm_bytes += dst_len;
                }
                ReadOp::Zero { dst_len: _ } => {
                    this.fill(0);
                    imm_bytes += dst_len;
                }
                #[cfg(feature = "wal")]
                ReadOp::Inmem { segid, s3_off, dst_len: _ } => {
                    // Spawn a task that copies from the
                    // in-memory in-flight segment.
                    let data_buf = unsafe {
                        std::slice::from_raw_parts_mut(this.as_mut_ptr() as *mut u8, this.len())
                    };
                    let flushing_segments = self.flushing_segments.clone();
                    let staging = self.staging.clone();
                    let sema = semaphore.clone();
                    let join = self.rt.as_ref().unwrap().spawn(async move {
                        let _slot = sema.acquire_owned().await.unwrap();
                        let len = data_buf.len();
                        // The planner saw this segment as in flight, but its
                        // flush can land between that decision and this task
                        // running: once the segment is on staging the entry is
                        // removed and the pinned buffer dropped. That is not a
                        // failure — the same bytes are now readable the
                        // ordinary way, at the same offset — so fall back to
                        // it rather than treating a completed flush as a bug.
                        let copied = {
                            let lock = flushing_segments.read().await;
                            match lock.get(&segid).and_then(|weak| weak.upgrade()) {
                                Some(data) => {
                                    let end = s3_off + len;
                                    data_buf.copy_from_slice(&data[s3_off..end]);
                                    staging.read_timing().add_inflight_read();
                                    true
                                },
                                None => false,
                            }
                        };
                        if !copied {
                            debug!("read - segid {} no longer held in memory, reading it from staging", segid);
                            staging.load_range(segid, s3_off, data_buf).await?;
                        }
                        Ok(len)
                    });
                    joins.push(join);
                }
                ReadOp::Range { segid, s3_off, dst_len: _ } => {
                    // Spawn a task that issues one ranged S3 GET
                    // for the whole coalesced range.
                    let data_buf = unsafe {
                        std::slice::from_raw_parts_mut(this.as_mut_ptr() as *mut u8, this.len())
                    };
                    let staging = self.staging.clone();
                    let sema = semaphore.clone();
                    let join = self.rt.as_ref().unwrap().spawn(async move {
                        let _slot = sema.acquire_owned().await.unwrap();
                        let len = data_buf.len();
                        // Report a failed GET. Discarding it here would hand
                        // the caller a successful read over a buffer that was
                        // never filled.
                        staging.load_range(segid, s3_off, data_buf).await?;
                        Ok(len)
                    });
                    joins.push(join);
                }
            }
            remaining = next;
        }
        let total_bytes = buf_len;

        // Fast path: nothing was spawned, all ops were
        // synchronous. Send the response directly.
        if joins.is_empty() {
            if total_bytes > 0 && !self.flags.is_noatime() {
                self.inode.update_atime();
            }
            #[cfg(feature = "range-lock")]
            self.range_lock.try_unlock(range);
            resp.answer_read(total_bytes, owned);
            return Ok(total_bytes);
        }

        if total_bytes > 0 && !self.flags.is_noatime() {
            self.inode.update_atime();
        }

        #[cfg(feature = "range-lock")]
        let mut range_lock = self.range_lock.clone();
        self.rt.as_ref().unwrap().spawn(async move {
            let mut actual = imm_bytes;
            let mut failed = None;
            for join in joins {
                match join.await {
                    Ok(Ok(bytes)) => actual += bytes,
                    // A load that failed. Reporting it is the point: the
                    // buffer was not filled, so answering with a byte
                    // count would hand the caller stale memory as data.
                    Ok(Err(e)) => {
                        warn!("read load failed: {:?}", e);
                        failed = Some(e);
                    },
                    // A load task that panicked or was cancelled must not
                    // take the range lock down with it. Releasing it is
                    // the only thing that lets a waiting flush start, so
                    // do not leave by way of a panic here: the read would
                    // report an error to its caller and the file would
                    // never flush or accept another range again.
                    Err(e) => {
                        warn!("read load task failed: {:?}", e);
                        failed = Some(Error::other(format!("read load task failed: {}", e)));
                    },
                }
            }
            if failed.is_none() && total_bytes != actual {
                warn!("short read: expected {} bytes, got {}", total_bytes, actual);
                failed = Some(Error::other(format!("short read: expected {} bytes, got {}", total_bytes, actual)));
            }
            #[cfg(feature = "range-lock")]
            range_lock.unlock(range).await;
            match failed {
                Some(e) => resp.fail_read(e),
                None => resp.answer_read(actual, owned),
            }
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

    pub(crate) async fn absorb_write(&mut self, mut req: FileReqWrite<'a>, resp: FileResp, fetched: Vec<DataBlock>) -> Result<usize> {
        // A block this write needs could not be read. Applying the write
        // now would put its bytes into a block that was never filled and
        // mark it dirty, so the next flush would persist zeroes over
        // whatever the block held. Fail the write instead: the arm
        // releases the range lock and answers the caller, and dropping the
        // request returns the permit.
        if let Some(e) = req.fetch_err.take() {
            return Err(e);
        }

        // Carry the fetched blocks on in the request rather than putting
        // them in the cache here.
        //
        // Under WAL this arm ends by requeueing for the WAL write, which
        // hands the handler task back, and a flush running in that gap
        // would take a block that is in the dirty list but has not been
        // modified yet — `clear_data_blocks_dirty` sweeps it into the
        // segment, or drops it outright when the data cache is off. The
        // write would then reach `update_cache` with nothing resident and
        // rebuild the block from zeroes, keeping only the bytes it covers
        // and losing the rest.
        //
        // Installing them in `absorb_write_bh` instead, immediately before
        // the data is applied and in the same arm, leaves no such gap. A
        // block that is not in the cache is not a block a flush can take,
        // and a read that wants it in the meantime still finds the
        // pre-write contents through the bmap, which is what it should
        // see while this write is unfinished.
        let mut fetched = fetched;
        req.fetched.append(&mut fetched);

        #[cfg(feature = "wal")]
        if let Some(_) = &mut self.wal {
            let fh = req.fh.clone();
            let ctx = FileContext::write_wal(req, resp);
            let _ = fh.send_cb(ctx);
            // TODO: change to ErrorKind::InProgress when it's stable
            return Err(Error::new(ErrorKind::ResourceBusy, "op resubmit to exec write wal"));
        }

        self.absorb_write_bh(req, resp).await
    }

    pub(crate) async fn absorb_write_bh(&mut self, mut req: FileReqWrite<'a>, resp: FileResp) -> Result<usize> {
        // A block this write needs could not be read. Applying the write
        // now would put its bytes into a block that was never filled and
        // mark it dirty, so the next flush would persist zeroes over
        // whatever the block held. Fail the write instead: the arm
        // releases the range lock and answers the caller, and dropping the
        // request returns the permit.
        if let Some(e) = req.fetch_err.take() {
            return Err(e);
        }

        let off = req.offset;
        let len = req.buf.len();
        let buf = req.buf;
        let mut bytes_write = 0;

        // A partial write needs its block's current contents to modify.
        // Anything this write fetched is still in hand, but a block that
        // was in the dirty list when the write started had nothing
        // fetched for it, and a flush arriving between then and now takes
        // it into a segment. Fetch those again rather than applying the
        // write over a block rebuilt from nothing. Checked before the
        // permit is taken so the requeued request keeps holding it.
        let data_block_size = self.config.meta.data_block_size;
        let mut refetch = Vec::new();
        for (blk_idx, blk_off, blk_len) in BlockIndexIter::new(off, len, data_block_size) {
            if blk_off == 0 && blk_len == data_block_size {
                // Fully overwritten, so its previous contents do not matter.
                continue;
            }
            if self.cache.has(&blk_idx) || req.fetched.iter().any(|b| b.index() == blk_idx) {
                continue;
            }
            // Usually recoverable right here, with no object request: the
            // flush that took it is still uploading, so the block is a
            // memcpy out of the pinned segment.
            #[cfg(feature = "wal")]
            if self.try_refill_block_in_place(blk_idx).await? {
                continue;
            }
            refetch.push(blk_idx);
        }
        if !refetch.is_empty() {
            debug!("absorb_write_bh - {} block(s) need an object request after a flush took them", refetch.len());
            self.spawn_write_retrieve(req, resp, refetch, AfterRetrieve::AbsorbBh).await?;
            return Err(Error::new(ErrorKind::ResourceBusy, "refetch blocks taken by a flush"));
        }

        // restore spawn_write permit
        let opt_permit = req.spawn_write_permit.take();
        assert!(opt_permit.is_some());

        // Install the blocks fetched for this write, now that the data is
        // about to be applied in this same arm. Doing it here rather than
        // when they were fetched means a flush cannot sweep an unmodified
        // block into a segment, or drop it, in between.
        let mut fetched = Vec::new();
        fetched.append(&mut req.fetched);
        for block in fetched.into_iter() {
            let blk_idx = block.index();
            // Another write, or a read populating the cache, landed on
            // this block while we were fetching it. The resident copy is
            // newer than what we just read out of staging, so drop the
            // fetch: installing it would lose that write. `update_cache`
            // below then edits the resident block, promoting it out of the
            // clean tier if that is where it sits.
            //
            // Skipping is only safe because the block is resident. With
            // nothing resident, `update_cache` would fabricate a
            // zero-filled block, and the bytes this write does not cover
            // would read back as zeroes instead of the staged data.
            if self.cache.has(&blk_idx) {
                debug!("absorb - block index {} already resident, dropping the fetched copy", blk_idx);
                continue;
            }
            let None = self.cache.insert(blk_idx, block) else {
                panic!("BlockIndex {} already on data_blocks_dirty list", blk_idx);
            };
        }

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
            // NOTE:
            // since we have update the dirty blocks cache,
            // if we failed in bmap insert, we have not way to rollback, so let's panic here
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
            self.inode.update_blocks((new_blocks * data_block_size) as isize);
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
            } else if self.state.is_flushing() {
                // if flushing is on going, let skip it
            } else {
                let fh = req.fh;
                let ctx = FileContext::new_wal_flush(fh.clone());
                let _ = fh.send_highprio(ctx);
            }
            #[cfg(not(feature = "wal"))]
            self.flush().await?;
        }
        // consume resp
        let _ = resp.to_write();

        Ok(bytes_write)
    }

    pub(crate) async fn absorb_write_zero(&mut self, mut req: FileReqWriteZero<'a>, resp: FileResp, fetched: Vec<DataBlock>) -> Result<usize> {
        // A block this write needs could not be read. Applying the write
        // now would put its bytes into a block that was never filled and
        // mark it dirty, so the next flush would persist zeroes over
        // whatever the block held. Fail the write instead: the arm
        // releases the range lock and answers the caller, and dropping the
        // request returns the permit.
        if let Some(e) = req.fetch_err.take() {
            return Err(e);
        }

        // Carry the fetched blocks on in the request rather than putting
        // them in the cache here.
        //
        // Under WAL this arm ends by requeueing for the WAL write, which
        // hands the handler task back, and a flush running in that gap
        // would take a block that is in the dirty list but has not been
        // modified yet — `clear_data_blocks_dirty` sweeps it into the
        // segment, or drops it outright when the data cache is off. The
        // write would then reach `update_cache` with nothing resident and
        // rebuild the block from zeroes, keeping only the bytes it covers
        // and losing the rest.
        //
        // Installing them in `absorb_write_bh` instead, immediately before
        // the data is applied and in the same arm, leaves no such gap. A
        // block that is not in the cache is not a block a flush can take,
        // and a read that wants it in the meantime still finds the
        // pre-write contents through the bmap, which is what it should
        // see while this write is unfinished.
        let mut fetched = fetched;
        req.fetched.append(&mut fetched);

        #[cfg(feature = "wal")]
        if let Some(_) = &mut self.wal {
            let fh = req.fh.clone();
            let ctx = FileContext::write_zero_wal(req, resp);
            let _ = fh.send_cb(ctx);
            // TODO: change to ErrorKind::InProgress when it's stable
            return Err(Error::new(ErrorKind::ResourceBusy, "op resubmit to exec write zero wal"));
        }

        self.absorb_write_zero_bh(req, resp).await
    }

    pub(crate) async fn absorb_write_zero_bh(&mut self, mut req: FileReqWriteZero<'a>, resp: FileResp) -> Result<usize> {
        // A block this write needs could not be read. Applying the write
        // now would put its bytes into a block that was never filled and
        // mark it dirty, so the next flush would persist zeroes over
        // whatever the block held. Fail the write instead: the arm
        // releases the range lock and answers the caller, and dropping the
        // request returns the permit.
        if let Some(e) = req.fetch_err.take() {
            return Err(e);
        }

        let off = req.offset;
        let len = req.len;
        let mut bytes_write = 0;

        // See `absorb_write_bh`: a partial zero keeps the bytes it does not
        // cover, so it needs the block, and a flush may have taken it
        // since this request last ran. Checked before the permit is taken
        // so the requeued request keeps holding it.
        let data_block_size = self.config.meta.data_block_size;
        let oldsize = self.inode.size();
        let mut refetch = Vec::new();
        for (blk_idx, start_off, data_len) in BlockIndexIter::new(off, len, data_block_size) {
            if start_off == 0 && data_len == data_block_size {
                continue;
            }
            if start_off == 0 && (blk_idx as usize * data_block_size) + start_off + data_len > oldsize {
                // Becomes a hole outright, so its previous contents do not matter.
                continue;
            }
            if self.cache.has(&blk_idx) || req.fetched.iter().any(|b| b.index() == blk_idx) {
                continue;
            }
            // See `absorb_write_bh`: normally no object request is needed.
            #[cfg(feature = "wal")]
            if self.try_refill_block_in_place(blk_idx).await? {
                continue;
            }
            refetch.push(blk_idx);
        }
        if !refetch.is_empty() {
            debug!("absorb_write_zero_bh - {} block(s) need an object request after a flush took them", refetch.len());
            self.spawn_write_zero_retrieve(req, resp, refetch, AfterRetrieve::AbsorbBh).await?;
            return Err(Error::new(ErrorKind::ResourceBusy, "refetch blocks taken by a flush"));
        }

        // restore spawn_write permit
        let opt_permit = req.spawn_write_permit.take();
        assert!(opt_permit.is_some());

        // Install the blocks fetched for this write, now that the data is
        // about to be applied in this same arm. See `absorb_write_bh` for
        // why this cannot happen at fetch time.
        let mut fetched = Vec::new();
        fetched.append(&mut req.fetched);
        for block in fetched.into_iter() {
            let blk_idx = block.index();
            if self.cache.has(&blk_idx) {
                debug!("absorb - block index {} already resident, dropping the fetched copy", blk_idx);
                continue;
            }
            let None = self.cache.insert(blk_idx, block) else {
                panic!("BlockIndex {} already on data_blocks_dirty list", blk_idx);
            };
        }

        // NOTE:
        // since we have update the dirty blocks cache,
        // if we failed in bmap operations, we have not way to rollback, so let's panic here

        let blk_iter = BlockIndexIter::new(off, len, data_block_size);
        let mut new_blocks: usize = 0;
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
        drop(opt_permit);

        #[cfg(feature = "range-lock")]
        let range = off as u64..(off + len) as u64;
        #[cfg(feature = "range-lock")]
        self.range_lock.try_unlock(range);

        if self.need_flush() {
            #[cfg(feature = "wal")]
            if self.wal.is_none() {
                self.flush().await?;
            } else if self.state.is_flushing() {
                // if flushing is on going, let skip it
            } else {
                let fh = req.fh;
                let ctx = FileContext::new_wal_flush(fh.clone());
                let _ = fh.send_highprio(ctx);
            }
            #[cfg(not(feature = "wal"))]
            self.flush().await?;
        }
        // consume resp
        let _ = resp.to_write_zero();

        Ok(bytes_write)
    }

    /// Fetch the blocks a write needs to modify, then hand the request to
    /// `after`.
    ///
    /// Serves both directions a write can arrive from: its first pass,
    /// which still has the WAL write ahead of it, and a second pass for
    /// blocks a flush took away in between, which must not repeat that
    /// write. See `AfterRetrieve`.
    async fn spawn_write_retrieve(&mut self, mut req: FileReqWrite<'a>, resp: FileResp, list: Vec<BlockIndex>, after: AfterRetrieve) -> Result<()> {
        let mut joins = Vec::new();
        let mut fetched = Vec::new();
        for blk_idx in list {
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
                    debug!("block index {} not found in bmap, prepare a new block", blk_idx);
                    let block = self.cache.new_block(blk_idx);
                    block.set_should_cache();
                    fetched.push(block);
                },
            }
        }

        self.rt.as_ref().unwrap().spawn(async move {
            // A load that failed leaves its block a fresh, zeroed one.
            // Absorbing that would replace the block's real contents with
            // zeroes and persist them, so record the failure and let the
            // absorb fail the write instead. Keep going through the rest
            // of the joins either way, so nothing is left dangling.
            for o in joins.drain(..) {
                let res = match o {
                    ImmOrJoinSize::ImmSize(size) => Ok(size),
                    ImmOrJoinSize::JoinSize(j) => match j.await {
                        Ok(res) => res,
                        Err(e) => Err(Error::other(format!("write retrieve task failed: {}", e))),
                    },
                };
                if let Err(e) = res {
                    warn!("write retrieve failed: {:?}", e);
                    if req.fetch_err.is_none() {
                        req.fetch_err = Some(e);
                    }
                }
            }
            req.fetched.append(&mut fetched);
            let fh = req.fh.clone();
            let ctx = match after {
                AfterRetrieve::Absorb => FileContext::write_absorb(req, resp),
                AfterRetrieve::AbsorbBh => FileContext::write_absorb_bh(req, resp),
            };
            let _ = fh.send_cb(ctx);
        });

        Ok(())
    }

    /// Put a block a flush took back in the cache, without leaving the
    /// handler task and without an object request.
    ///
    /// Returns whether the block is resident afterwards. Anything that
    /// would need to read staging returns `false` and is left to
    /// `spawn_write_refetch`, because waiting for an object request here
    /// would stall the handler for every other request too.
    ///
    /// The case worth having is the middle one: the flush that took the
    /// block is usually still uploading its segment, and that segment is
    /// pinned in memory, so the block's contents are a memcpy away. A
    /// block with nothing behind it, or a hole, is free as well — a fresh
    /// block already reads as zeroes.
    #[cfg(feature = "wal")]
    async fn try_refill_block_in_place(&mut self, blk_idx: BlockIndex) -> Result<bool> {
        let blk_ptr = match self.bmap.lookup(&blk_idx).await {
            Ok(blk_ptr) => blk_ptr,
            Err(e) if e.kind() == ErrorKind::NotFound => {
                self.insert_fresh_block(blk_idx);
                return Ok(true);
            },
            Err(e) => return Err(e),
        };
        if BlockPtrFormat::is_zero_block(&blk_ptr) {
            self.insert_fresh_block(blk_idx);
            return Ok(true);
        }
        if !BlockPtrFormat::is_on_staging(&blk_ptr) {
            return Ok(false);
        }
        let (segid, staging_off) = self.blk_ptr_decode(&blk_ptr);
        if segid <= self.inode().get_last_ondisk_cno() {
            // Already written out, so reading it means an object request.
            return Ok(false);
        }
        let flushing_segments = self.flushing_segments.clone();
        let lock = flushing_segments.read().await;
        let Some(data) = lock.get(&segid).and_then(|weak| weak.upgrade()) else {
            // The upload finished and the buffer went with it.
            return Ok(false);
        };
        let block = self.cache.new_block(blk_idx);
        block.set_should_cache();
        let buf = block.as_mut_slice();
        let end = staging_off + buf.len();
        buf.copy_from_slice(&data[staging_off..end]);
        drop(lock);
        let None = self.cache.insert(blk_idx, block) else {
            panic!("BlockIndex {} already on data_blocks_dirty list", blk_idx);
        };
        Ok(true)
    }

    /// A block with no contents to recover: a fresh one reads as zeroes,
    /// which is what a hole or an unbacked block should read as.
    #[cfg(feature = "wal")]
    fn insert_fresh_block(&mut self, blk_idx: BlockIndex) {
        let block = self.cache.new_block(blk_idx);
        block.set_should_cache();
        let None = self.cache.insert(blk_idx, block) else {
            panic!("BlockIndex {} already on data_blocks_dirty list", blk_idx);
        };
    }

    /// Fetch blocks again for a write that has already been through the
    /// WAL, and hand it straight back to `absorb_write_bh`.
    ///
    /// Needed because a WAL write's two halves can straddle a flush. Its
    /// first half decides which blocks to fetch; a block already in the
    /// dirty list needs none, so nothing is fetched for it. The WAL write
    /// then hands the handler task back, a flush takes that block into a
    /// segment, and the second half arrives to find it gone. Applying the
    /// write at that point would rebuild the block from nothing and keep
    /// only the bytes this write covers, losing the rest.
    ///
    /// The block is on staging, or in the segment still being uploaded, by
    /// the time we get here, so fetching it again reads exactly what the
    /// flush persisted. Unlike `spawn_write_retrieve` this returns to
    /// `write_absorb_bh`: the WAL write has already happened and must not
    /// be repeated.
     /// `spawn_write_refetch` for a zeroing write. Same reason: a partial
    /// zero has to keep the bytes it does not cover, so it needs the
    /// block's current contents, and a flush may have taken them since
    /// this request last ran.
     // duplicate logic of spawn_write_retrieve()
    // TODO: can be merge with spawn_write_retrieve
    async fn spawn_write_zero_retrieve(&mut self, mut req: FileReqWriteZero<'a>, resp: FileResp, list: Vec<BlockIndex>, after: AfterRetrieve) -> Result<()> {
        let mut joins = Vec::new();
        let mut fetched = Vec::new();
        for blk_idx in list {
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
                    debug!("block index {} not found in bmap, prepare a new block", blk_idx);
                    let block = self.cache.new_block(blk_idx);
                    block.set_should_cache();
                    fetched.push(block);
                },
            }
        }

        self.rt.as_ref().unwrap().spawn(async move {
            // A load that failed leaves its block a fresh, zeroed one.
            // Absorbing that would replace the block's real contents with
            // zeroes and persist them, so record the failure and let the
            // absorb fail the write instead. Keep going through the rest
            // of the joins either way, so nothing is left dangling.
            for o in joins.drain(..) {
                let res = match o {
                    ImmOrJoinSize::ImmSize(size) => Ok(size),
                    ImmOrJoinSize::JoinSize(j) => match j.await {
                        Ok(res) => res,
                        Err(e) => Err(Error::other(format!("write retrieve task failed: {}", e))),
                    },
                };
                if let Err(e) = res {
                    warn!("write retrieve failed: {:?}", e);
                    if req.fetch_err.is_none() {
                        req.fetch_err = Some(e);
                    }
                }
            }
            req.fetched.append(&mut fetched);
            let fh = req.fh.clone();
            let ctx = match after {
                AfterRetrieve::Absorb => FileContext::write_zero_absorb(req, resp),
                AfterRetrieve::AbsorbBh => FileContext::write_zero_absorb_bh(req, resp),
            };
            let _ = fh.send_cb(ctx);
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
        // POSIX: a write on a handle not opened for writing fails
        // with EBADF. Checked before the flush-state test and the
        // range lock, so the request never acquires state that the
        // caller's error path would have to unwind. The handler
        // forwards this error to the caller because its kind is not
        // ResourceBusy.
        //
        // The handler's error path does call `range_lock.try_unlock`
        // for a range this request never locked. That is a no-op on
        // the underlying RangeSet unless a concurrent write holds an
        // overlapping range — impossible here, because every write on
        // a read-only handle is rejected at this same guard, so no
        // sibling writer can exist.
        if !self.flags.is_writable() {
            return Err(Self::ebadf_bad_access_mode());
        }
        let buf = req.buf;
        let len = buf.len();
        // O_APPEND: override caller-supplied offset to current
        // i_size, every time the request is dispatched. If this
        // request gets re-queued via send_highprio (range-lock /
        // sema / flush-in-progress conflict), it will re-enter
        // here on the next dispatch and read the latest i_size,
        // which by then reflects whatever sibling write completed
        // first. The borrow on &mut self plus the per-request
        // sema permit guarantees i_size is stable from this read
        // through to the inode size update on the absorb path.
        let off = if self.flags.is_append() {
            self.inode.size()
        } else {
            req.offset
        };
        req.offset = off;

        // Stop the world while a flush runs, unless WAL makes that
        // unnecessary. See `must_wait_for_flush`.
        if self.must_wait_for_flush() {
            let fh = req.fh.clone();
            let ctx = FileContext::reform_write(req, resp);
            let _ = fh.send_highprio(ctx);
            return Err(Error::new(ErrorKind::ResourceBusy, "flush is ongoing"));
        }

        #[cfg(feature = "range-lock")]
        let range = off as u64..(off + len) as u64;
        // See the note in `spawn_read`: a pending flush is waiting for
        // every range to be released, so do not take a new one.
        #[cfg(feature = "range-lock")]
        if self.state.is_flush_pending() {
            let fh = req.fh.clone();
            let ctx = FileContext::reform_write(req, resp);
            let _ = fh.send_highprio(ctx);
            return Err(Error::new(ErrorKind::ResourceBusy, "flush is pending"));
        }
        #[cfg(feature = "range-lock")]
        if self.range_lock.try_lock(range) == false {
            let fh = req.fh.clone();
            let ctx = FileContext::reform_write(req, resp);
            let _ = fh.send_highprio(ctx);
            return Err(Error::new(ErrorKind::ResourceBusy, "read range locked"));
        }

        let Ok(permit) = self.sema.clone().try_acquire_owned() else {
            let fh = req.fh.clone();
            let ctx = FileContext::reform_write(req, resp);
            let _ = fh.send_highprio(ctx);
            return Err(Error::new(ErrorKind::ResourceBusy, "sema locked"));
        };
        req.spawn_write_permit = Some(permit);

        debug!("WRITE - off: {}, buf len: {}", off, len);
        let v: Vec<BlockIndex> = self.write_prepare(off, len);
        if v.len() > 0 {
            // retrieve data by spawn
            self.spawn_write_retrieve(req, resp, v, AfterRetrieve::Absorb).await?;
            return Err(Error::new(ErrorKind::ResourceBusy, "hand over to spawn write retrieve"));
        }

        // no need to pre-retrive anything,
        // this is HAPPY PATH, continue on this runtime
        let actual_bytes = self.absorb_write(req, resp, Vec::new()).await?;
        assert!(len == actual_bytes);
        Ok(len)
    }

    pub async fn spawn_write_zero(&mut self, mut req: FileReqWriteZero<'a>, resp: FileResp) -> Result<usize> {
        // See spawn_write() for why this is checked here.
        if !self.flags.is_writable() {
            return Err(Self::ebadf_bad_access_mode());
        }
        let len = req.len;
        // O_APPEND: same rule as spawn_write(). See the
        // corresponding comment there.
        let off = if self.flags.is_append() {
            self.inode.size()
        } else {
            req.offset
        };
        req.offset = off;

        // Stop the world while a flush runs, unless WAL makes that
        // unnecessary. See `must_wait_for_flush`.
        if self.must_wait_for_flush() {
            let fh = req.fh.clone();
            let ctx = FileContext::reform_write_zero(req, resp);
            let _ = fh.send_highprio(ctx);
            return Err(Error::new(ErrorKind::ResourceBusy, "flush is ongoing"));
        }

        #[cfg(feature = "range-lock")]
        let range = off as u64..(off + len) as u64;
        // See the note in `spawn_read`: a pending flush is waiting for
        // every range to be released, so do not take a new one.
        #[cfg(feature = "range-lock")]
        if self.state.is_flush_pending() {
            let fh = req.fh.clone();
            let ctx = FileContext::reform_write_zero(req, resp);
            let _ = fh.send_highprio(ctx);
            return Err(Error::new(ErrorKind::ResourceBusy, "flush is pending"));
        }
        #[cfg(feature = "range-lock")]
        if self.range_lock.try_lock(range) == false {
            let fh = req.fh.clone();
            let ctx = FileContext::reform_write_zero(req, resp);
            let _ = fh.send_highprio(ctx);
            return Err(Error::new(ErrorKind::ResourceBusy, "read range locked"));
        }

        let Ok(permit) = self.sema.clone().try_acquire_owned() else {
            let fh = req.fh.clone();
            let ctx = FileContext::reform_write_zero(req, resp);
            let _ = fh.send_highprio(ctx);
            return Err(Error::new(ErrorKind::ResourceBusy, "sema locked"));
        };
        req.spawn_write_permit = Some(permit);

        debug!("WRITE ZERO - off: {}, len: {}", off, len);
        let v: Vec<BlockIndex> = self.write_prepare(off, len);
        if v.len() > 0 {
            // retrieve data by spawn
            self.spawn_write_zero_retrieve(req, resp, v, AfterRetrieve::Absorb).await?;
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
            let _ = fh.send_cb(ctx);
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
            let _ = fh.send_cb(ctx);
        });
        Ok(len)
    }
}
