use std::io::Result;
use aws_sdk_s3::Client;
use hyperfile_reactor::Reactor;
use tokio::sync::oneshot;
use crate::BlockIndex;
use crate::file::handler::{ChannelGroup, build_channel_group, BlockAction, TimingOp, TimingValue};

/// Stops a borrowed closure from running after the caller has gone.
///
/// `fh_with_block*` erases the lifetime of a closure that may borrow
/// the caller's locals, so the closure must not run once the caller
/// has returned, unwound, or been cancelled. This guard marks the
/// shared gate on drop; the dispatcher checks it under the same lock
/// and skips the closure if it is set.
///
/// Dropping it blocks only if the closure is running at that instant,
/// and the dispatcher takes the gate after the block is in hand, so
/// the wait is one closure body with no I/O in it. Waiting across I/O
/// would deadlock: the reactor's object-store work can be driven by
/// the caller's runtime.
struct BorrowGuard {
    gate: crate::file::handler::BlockActionGate,
}

impl Drop for BorrowGuard {
    fn drop(&mut self) {
        let mut cancelled = self.gate.lock().unwrap_or_else(|e| e.into_inner());
        *cancelled = true;
    }
}
use crate::config::{HyperFileMetaConfig, HyperFileRuntimeConfig};
use crate::buffer::{AlignedDataBlockWrapper, BatchDataBlockWrapper};
use crate::staging::{s3::S3Staging, StagingIntercept};
use super::hyper::Hyper;
use super::flags::FileFlags;
use super::mode::FileMode;
use super::handler::{FileContext, SeekWhence};

/// Reactor-mode handle to a hyperfile-backed file.
///
/// Wraps a [`hyperfile_reactor::Reactor`]-spawned task that
/// owns the underlying [`Hyper`] and serializes operations
/// through a request/response channel. Cloning is cheap (just
/// duplicates channel senders); use [`Self::clone`] to share a
/// handle across tasks. The reactor task winds down when every
/// `HyperFileHandler` clone (and any sibling `HyperFileTokio`)
/// has been dropped.
///
/// # Security: hyperfile does not enforce POSIX permissions
///
/// `fh_chmod` / `fh_chown` / `fh_setattr` record the
/// `mode` / `uid` / `gid` you give them, and `fh_getattr` reads
/// them back, but the read/write/truncate paths **do not check
/// the bits**. There is no `PermissionDenied` / `EACCES` path.
/// Permission enforcement is the caller's responsibility (FUSE
/// kernel checks, app-level IAM, etc.); see `docs/posix.md`'s
/// "Permissions and ownership" section.
#[derive(Clone)]
pub struct HyperFileHandler<'a> {
    inner: ChannelGroup<FileContext<'a>>,
}

impl<'a: 'static> HyperFileHandler<'a> {
    /// Wrap a pre-constructed `Hyper` with a handler task. Useful when
    /// the caller needs full `HyperFileConfig` control (e.g. WAL
    /// configuration) that isn't surfaced by the other `fh_*`
    /// constructors.
    pub async fn fh_from_hyper(reactor: &Reactor<FileContext<'a>, Hyper<'a>>, hyper: Hyper<'a>) -> Result<Self>
    {
        let (builder, finish) = build_channel_group();
        let handler = reactor.spawn_async(hyper, builder).await
            .map_err(|_| std::io::Error::new(std::io::ErrorKind::BrokenPipe, "reactor thread is gone"))?;
        Ok(Self { inner: finish(handler) })
    }

    pub async fn fh_create(reactor: &Reactor<FileContext<'a>, Hyper<'a>>, client: &Client, uri: &str, flags: FileFlags, mode: FileMode) -> Result<Self>
    {
        let hyper = Hyper::fs_create(client, uri, flags, mode).await?;
        let (builder, finish) = build_channel_group();
        let handler = reactor.spawn_async(hyper, builder).await
            .map_err(|_| std::io::Error::new(std::io::ErrorKind::BrokenPipe, "reactor thread is gone"))?;
        Ok(Self { inner: finish(handler) })
    }

    pub async fn fh_create_with(reactor: &Reactor<FileContext<'a>, Hyper<'a>>, client: &Client, uri: &str, flags: FileFlags, mode: FileMode, interceptor: impl StagingIntercept<S3Staging> + 'static) -> Result<Self>
    {
        let hyper = Hyper::fs_create_with_interceptor(client, uri, flags, mode, interceptor).await?;
        let (builder, finish) = build_channel_group();
        let handler = reactor.spawn_async(hyper, builder).await
            .map_err(|_| std::io::Error::new(std::io::ErrorKind::BrokenPipe, "reactor thread is gone"))?;
        Ok(Self { inner: finish(handler) })
    }

    pub async fn fh_create_opt(reactor: &Reactor<FileContext<'a>, Hyper<'a>>, client: &Client, uri: &str, flags: FileFlags, mode: FileMode,
            meta_config: &HyperFileMetaConfig, runtime_config: &HyperFileRuntimeConfig) -> Result<Self>
    {
        let hyper = Hyper::fs_create_opt(client, uri, flags, mode, meta_config, runtime_config).await?;
        let (builder, finish) = build_channel_group();
        let handler = reactor.spawn_async(hyper, builder).await
            .map_err(|_| std::io::Error::new(std::io::ErrorKind::BrokenPipe, "reactor thread is gone"))?;
        Ok(Self { inner: finish(handler) })
    }

    pub async fn fh_create_opt_with_interceptor(reactor: &Reactor<FileContext<'a>, Hyper<'a>>,
            client: &Client, uri: &str, flags: FileFlags, mode: FileMode,
            meta_config: &HyperFileMetaConfig, runtime_config: &HyperFileRuntimeConfig,
            interceptor: impl StagingIntercept<S3Staging> + 'static) -> Result<Self>
    {
        let hyper = Hyper::fs_create_opt_with_interceptor(client, uri, flags, mode, meta_config, runtime_config, interceptor).await?;
        let (builder, finish) = build_channel_group();
        let handler = reactor.spawn_async(hyper, builder).await
            .map_err(|_| std::io::Error::new(std::io::ErrorKind::BrokenPipe, "reactor thread is gone"))?;
        Ok(Self { inner: finish(handler) })
    }

    pub async fn fh_open(reactor: &Reactor<FileContext<'a>, Hyper<'a>>, client: &Client, uri: &str, flags: FileFlags) -> Result<Self>
    {
        let hyper = Hyper::fs_open(client, uri, flags).await?;
        let (builder, finish) = build_channel_group();
        let handler = reactor.spawn_async(hyper, builder).await
            .map_err(|_| std::io::Error::new(std::io::ErrorKind::BrokenPipe, "reactor thread is gone"))?;
        Ok(Self { inner: finish(handler) })
    }

    pub async fn fh_open_opt(reactor: &Reactor<FileContext<'a>, Hyper<'a>>, client: &Client, uri: &str, flags: FileFlags,
            runtime_config: &HyperFileRuntimeConfig) -> Result<Self>
    {
        let hyper = Hyper::fs_open_opt(client, uri, flags, runtime_config).await?;
        let (builder, finish) = build_channel_group();
        let handler = reactor.spawn_async(hyper, builder).await
            .map_err(|_| std::io::Error::new(std::io::ErrorKind::BrokenPipe, "reactor thread is gone"))?;
        Ok(Self { inner: finish(handler) })
    }

    pub async fn fh_open_or_create_with_default_opt(reactor: &Reactor<FileContext<'a>, Hyper<'a>>, client: &Client, uri: &str, flags: FileFlags, mode: FileMode) -> Result<Self>
    {
        let hyper = Hyper::fs_open_or_create_with_default_opt(client, uri, flags, mode).await?;
        let (builder, finish) = build_channel_group();
        let handler = reactor.spawn_async(hyper, builder).await
            .map_err(|_| std::io::Error::new(std::io::ErrorKind::BrokenPipe, "reactor thread is gone"))?;
        Ok(Self { inner: finish(handler) })
    }

    pub async fn fh_unlink(client: &Client, uri: &str) -> Result<()>
    {
        Hyper::fs_unlink(client, uri).await
    }

    /// Rename the file at `src_uri` to `dst_uri`.
    ///
    /// **Not yet implemented.** Delegates to
    /// [`Hyper::fs_rename`], which currently returns
    /// [`std::io::ErrorKind::Unsupported`]. See that method's
    /// docs (and `docs/posix.md`) for the design issues blocking
    /// implementation.
    pub async fn fh_rename(client: &Client, src_uri: &str, dst_uri: &str) -> Result<()>
    {
        Hyper::fs_rename(client, src_uri, dst_uri).await
    }

    pub async fn fh_release(&mut self) -> Result<u64>
    {
        let (ctx, rx) = FileContext::new_release(self.inner.clone());
        self.inner.send(ctx)?;
        rx.await.map_err(|_| std::io::Error::new(std::io::ErrorKind::BrokenPipe, "reactor handler task died"))?
    }

    /// Read `buf.len()` bytes from `off`, returning the number of
    /// bytes read. Reads stop at `i_size`.
    ///
    /// **Does not populate the data cache**, matching
    /// `Hyper::fs_read`. Reading the same bytes twice fetches them
    /// twice. A read does consult the cache, so it is served from
    /// there when the blocks happen to be resident.
    ///
    /// `docs/block-api.md` has the full table of which entry points
    /// populate the cache.
    ///
    /// # Do not cancel this future
    ///
    /// `buf` is handed to the reactor as a bare pointer, because the
    /// reactor's request type is `'static` and cannot name the
    /// caller's lifetime. That is sound only while the caller stays
    /// parked on the response, which is what keeps `buf` alive and
    /// unaliased.
    ///
    /// Dropping this future before it completes — a `select!` branch
    /// losing, a `timeout` firing — breaks that: the reactor may still
    /// write into `buf` after the caller has freed or reused it.
    ///
    /// Unlike [`Self::fh_with_block`], this cannot be made safe by
    /// skipping the work, because the reactor writes into `buf` from
    /// inside the object-store read rather than in one step
    /// afterwards; waiting for that to finish would mean waiting
    /// across I/O, which can deadlock, since the reactor's I/O may be
    /// driven by the caller's runtime.
    ///
    /// If you need a cancellable read, read into a buffer you own and
    /// copy afterwards, or drive this from a task you can let run to
    /// completion.
    pub async fn fh_read(&mut self, off: usize, buf: &mut [u8]) -> Result<usize>
    {
        let b = unsafe {
            std::slice::from_raw_parts_mut(buf.as_ptr() as *mut u8, buf.len())
        };
        let (ctx, tx, mut rx) = FileContext::new_read(b, off, self.inner.clone());
        self.inner.send(ctx)?;
        let res = rx.recv().await.ok_or_else(|| std::io::Error::new(std::io::ErrorKind::BrokenPipe, "reactor handler task died"))?;
        drop(tx);
        let _ = buf;
        res
    }

    /// Write `buf` at `off`, returning the number of bytes written.
    ///
    /// # Do not cancel this future
    ///
    /// As with [`Self::fh_read`], `buf` reaches the reactor as a bare
    /// pointer and stays borrowed for as long as the caller is parked
    /// on the response. Dropping this future before it completes may
    /// leave the reactor reading from freed memory.
    ///
    /// With the `wal` feature this window includes an object-store
    /// PUT issued straight from `buf`, so it can be as long as a
    /// round trip.
    pub async fn fh_write(&mut self, off: usize, buf: &[u8]) -> Result<usize>
    {
        let b = unsafe {
            std::slice::from_raw_parts(buf.as_ptr() as *const u8, buf.len())
        };
        let (ctx, tx, mut rx) = FileContext::new_write(b, off, self.inner.clone());
        self.inner.send(ctx)?;
        let res = rx.recv().await.ok_or_else(|| std::io::Error::new(std::io::ErrorKind::BrokenPipe, "reactor handler task died"))?;
        drop(tx);
        let _ = buf;
        res
    }

    pub async fn fh_write_zero(&mut self, off: usize, len: usize) -> Result<usize>
    {
        let (ctx, tx, mut rx) = FileContext::new_write_zero(off, len, self.inner.clone());
        self.inner.send(ctx)?;
        let res = rx.recv().await.ok_or_else(|| std::io::Error::new(std::io::ErrorKind::BrokenPipe, "reactor handler task died"))?;
        drop(tx);
        res
    }

    pub async fn fh_write_aligned_batch(&mut self, blocks: Vec<AlignedDataBlockWrapper>) -> Result<usize>
    {
        let (ctx, mut rx) = FileContext::new_write_aligned_batch(blocks, self.inner.clone());
        self.inner.send(ctx)?;
        rx.recv().await.ok_or_else(|| std::io::Error::new(std::io::ErrorKind::BrokenPipe, "reactor handler task died"))?
    }

    pub async fn fh_write_batch(&mut self, blocks: Vec<BatchDataBlockWrapper>) -> Result<usize>
    {
        let (ctx, mut rx) = FileContext::new_write_batch(blocks, self.inner.clone());
        self.inner.send(ctx)?;
        rx.recv().await.ok_or_else(|| std::io::Error::new(std::io::ErrorKind::BrokenPipe, "reactor handler task died"))?
    }

    pub async fn fh_flush(&mut self) -> Result<u64>
    {
        let (ctx, rx) = FileContext::new_flush(self.inner.clone());
        self.inner.send(ctx)?;
        rx.await.map_err(|_| std::io::Error::new(std::io::ErrorKind::BrokenPipe, "reactor handler task died"))?
    }

    /// POSIX-`fdatasync` flavoured flush. See
    /// `Hyper::fs_fdatasync` for semantics. Skips the segment
    /// write entirely when only attrs are dirty.
    pub async fn fh_fdatasync(&mut self) -> Result<u64>
    {
        let (ctx, rx) = FileContext::new_flush_data(self.inner.clone());
        self.inner.send(ctx)?;
        rx.await.map_err(|_| std::io::Error::new(std::io::ErrorKind::BrokenPipe, "reactor handler task died"))?
    }

    pub async fn fh_truncate(&mut self, offset: usize) -> Result<()>
    {
        let (ctx, rx) = FileContext::new_trunc(offset, self.inner.clone());
        self.inner.send(ctx)?;
        rx.await.map_err(|_| std::io::Error::new(std::io::ErrorKind::BrokenPipe, "reactor handler task died"))?
    }

    /// Write `buf` at `off`, returning the number of bytes written.
    ///
    /// The cancel-safe counterpart of [`Self::fh_write`]. The request
    /// takes ownership of `buf`, so nothing of the caller's stays
    /// borrowed and dropping this future is harmless — the bytes live
    /// as long as the write needs them, including across the
    /// object-store PUT the `wal` feature issues from them.
    ///
    /// `Bytes` is reference-counted, so a caller that wants to keep its
    /// copy can clone first at no cost. Otherwise this is the same
    /// write as [`Self::fh_write`]: same pipeline, same absorb and
    /// range-lock behavior, same flush semantics.
    pub async fn fh_write_owned(&mut self, off: usize, buf: bytes::Bytes) -> Result<usize>
    {
        let (ctx, tx, mut rx) = FileContext::new_write_owned(buf, off, self.inner.clone());
        self.inner.send(ctx)?;
        let res = rx.recv().await.ok_or_else(|| std::io::Error::new(std::io::ErrorKind::BrokenPipe, "reactor handler task died"))?;
        drop(tx);
        res
    }

    /// Read up to `len` bytes from `off` into a buffer the reactor
    /// allocates, and return it.
    ///
    /// The cancel-safe counterpart of [`Self::fh_read`]. Nothing of the
    /// caller's is borrowed, so dropping this future is harmless: the
    /// buffer belongs to the reactor and goes away with the request.
    /// Use this wherever the read might be cancelled — a `select!`
    /// branch, a `timeout`, a task that may be aborted.
    ///
    /// The returned `Bytes` is truncated to what was actually read, so
    /// its length is the count; it is empty at or past `i_size`. Being
    /// reference-counted, handing it on costs nothing.
    ///
    /// Two differences from [`Self::fh_read`] beyond the buffer:
    ///
    /// * the reactor allocates, so a caller that already has a buffer
    ///   to fill pays one copy out of the returned `Bytes` — whereas
    ///   `fh_read` fills that buffer directly;
    /// * the read runs serially inside the reactor rather than
    ///   splitting a coalesced plan across `read_max_concurrency`
    ///   tasks. For a request that covers one contiguous range — which
    ///   is what a single object request means — that is the same
    ///   work.
    ///
    /// **Does not populate the data cache**, like every byte read.
    pub async fn fh_read_owned(&self, off: usize, len: usize) -> Result<bytes::Bytes>
    {
        let (ctx, rx) = FileContext::new_read_owned(off, len, self.inner.clone());
        self.inner.send(ctx)?;
        rx.await.map_err(|_| std::io::Error::new(std::io::ErrorKind::BrokenPipe, "reactor handler task died"))?
    }

    /// How many data blocks are dirty, i.e. would be written by the
    /// next flush.
    ///
    /// The counterpart of `Hyper::dirty_block_count`. Zero means a
    /// flush would have no data to write, so a caller can skip one.
    /// Fetch a range into the data block cache without returning it.
    ///
    /// For a caller that knows what will be asked for next and would
    /// rather it were already here — a read-ahead. Nothing comes back but
    /// a count: a later `fh_read` of those bytes asks the ordinary way and
    /// finds them.
    ///
    /// This exists because a byte read queries the data cache and does not
    /// fill it, so bytes fetched speculatively through `fh_read` have
    /// nowhere to live and the next read fetches them again. Only blocks
    /// brought in through here are cached, so a plain read still cannot
    /// evict what the write path is holding.
    ///
    /// Costs one crossing for the whole range, and the requests are
    /// coalesced the way a read of the same range would be — warming a
    /// megabyte is a handful of requests, not one per block. The fetches
    /// run off the handler task, so other operations are not held up for
    /// their duration.
    ///
    /// The range is widened to whole blocks and clamped to the end of the
    /// file. Blocks already cached are left alone, and holes are skipped:
    /// they read as zeroes without a request, so caching them buys
    /// nothing. Returns how many blocks were installed.
    ///
    /// A failure means nothing was installed; the reads that follow simply
    /// pay for their own fetches.
    pub async fn fh_read_ahead(&self, off: usize, len: usize) -> Result<usize>
    {
        let (ctx, rx) = FileContext::new_read_ahead(off, len, self.inner.clone());
        self.inner.send(ctx)?;
        rx.await.map_err(|_| std::io::Error::new(std::io::ErrorKind::BrokenPipe, "reactor handler task died"))?
    }

    /// `lseek(SEEK_HOLE)`: smallest offset >= `off` in a hole, or `None`
    /// (caller maps to `ENXIO`) if `off` is at or past EOF.
    ///
    /// EOF counts as a hole, so a file that is data all the way through
    /// answers with its size rather than `None`.
    ///
    /// The direct-API equivalent is `Hyper::fs_seek_hole`. Both walk the map
    /// and consider unflushed writes to be data, so the answer accounts for
    /// what has been written but not yet persisted.
    pub async fn fh_seek_hole(&self, off: usize) -> Result<Option<usize>>
    {
        let (ctx, rx) = FileContext::new_seek(off, SeekWhence::Hole);
        self.inner.send(ctx)?;
        rx.await.map_err(|_| std::io::Error::new(std::io::ErrorKind::BrokenPipe, "reactor handler task died"))?
    }

    /// `lseek(SEEK_DATA)`: smallest offset >= `off` holding data, or `None`
    /// (`ENXIO`) if there is none before EOF.
    ///
    /// The counterpart of [`Self::fh_seek_hole`], and the direct-API
    /// equivalent is `Hyper::fs_seek_data`.
    pub async fn fh_seek_data(&self, off: usize) -> Result<Option<usize>>
    {
        let (ctx, rx) = FileContext::new_seek(off, SeekWhence::Data);
        self.inner.send(ctx)?;
        rx.await.map_err(|_| std::io::Error::new(std::io::ErrorKind::BrokenPipe, "reactor handler task died"))?
    }

    /// What reading `[off, off + len)` would cost, without reading it.
    ///
    /// See [`HyperFile::read_plan`](crate::file::file::HyperFile::read_plan)
    /// for what the entries mean and for the one thing worth watching: the
    /// answer depends on the data cache, so a warm file looks cheap. Use
    /// [`Self::fh_block_placement`] to judge a layout regardless of cache.
    pub async fn fh_read_plan(&self, off: u64, len: u64) -> Result<Vec<crate::file::PlannedRead>>
    {
        let (ctx, rx) = FileContext::new_read_plan(vec![(off, len)]);
        self.inner.send(ctx)?;
        let mut many = rx.await
            .map_err(|_| std::io::Error::new(std::io::ErrorKind::BrokenPipe, "reactor handler task died"))??;
        Ok(many.pop().expect("one range asked about, one answer"))
    }

    /// [`Self::fh_read_plan`] for several ranges in one crossing, answered
    /// in the order the ranges were given.
    ///
    /// Worth batching because the query is metadata-only: with no object
    /// request to wait on, the crossing is most of the cost, so a tool
    /// asking about thousands of files pays for the asking rather than for
    /// the answers. A long batch does occupy the file for its whole walk.
    pub async fn fh_read_plan_many(&self, ranges: &[(u64, u64)]) -> Result<Vec<Vec<crate::file::PlannedRead>>>
    {
        let (ctx, rx) = FileContext::new_read_plan(ranges.to_vec());
        self.inner.send(ctx)?;
        rx.await.map_err(|_| std::io::Error::new(std::io::ErrorKind::BrokenPipe, "reactor handler task died"))?
    }

    /// Where each of `n` blocks starting at `start` currently lives.
    ///
    /// See
    /// [`HyperFile::block_placement`](crate::file::file::HyperFile::block_placement).
    /// Does not consult the data cache, so it answers about placement alone.
    pub async fn fh_block_placement(&self, start: BlockIndex, n: usize)
        -> Result<Vec<Option<(crate::SegmentId, u64)>>>
    {
        let (ctx, rx) = FileContext::new_block_placement(vec![(start, n)]);
        self.inner.send(ctx)?;
        let mut many = rx.await
            .map_err(|_| std::io::Error::new(std::io::ErrorKind::BrokenPipe, "reactor handler task died"))??;
        Ok(many.pop().expect("one range asked about, one answer"))
    }

    /// [`Self::fh_block_placement`] for several block ranges in one
    /// crossing, batched for the reason given on
    /// [`Self::fh_read_plan_many`].
    pub async fn fh_block_placement_many(&self, ranges: &[(BlockIndex, usize)])
        -> Result<Vec<Vec<Option<(crate::SegmentId, u64)>>>>
    {
        let (ctx, rx) = FileContext::new_block_placement(ranges.to_vec());
        self.inner.send(ctx)?;
        rx.await.map_err(|_| std::io::Error::new(std::io::ErrorKind::BrokenPipe, "reactor handler task died"))?
    }

    pub async fn fh_dirty_block_count(&self) -> Result<usize>
    {
        let (ctx, rx) = FileContext::new_dirty_block_count();
        self.inner.send(ctx)?;
        rx.await.map_err(|_| std::io::Error::new(std::io::ErrorKind::BrokenPipe, "reactor handler task died"))?
    }

    /// Read-side counters for this file. See
    /// [`ReadTiming`](crate::file::ReadTiming).
    ///
    /// Returns an owned snapshot rather than a reference to the live
    /// counters, which cannot leave the reactor task. Otherwise this
    /// matches `Hyper::read_timing`.
    pub async fn fh_read_timing(&self) -> Result<crate::file::ReadTimingSnapshot>
    {
        match self.timing(TimingOp::Read).await? {
            TimingValue::Read(s) => Ok(s),
            other => panic!("read timing request answered with {other:?}"),
        }
    }

    /// Zero the read counters, to bracket a measurement.
    pub async fn fh_read_timing_reset(&self) -> Result<()>
    {
        let _ = self.timing(TimingOp::ReadReset).await?;
        Ok(())
    }

    /// Flush-side timings for this file. See
    /// [`FlushTiming`](crate::file::FlushTiming).
    ///
    /// Returns an owned snapshot, for the same reason as
    /// [`Self::fh_read_timing`]. Hidden to match
    /// `Hyper::flush_timing`, which is benchmark-only.
    #[doc(hidden)]
    pub async fn fh_flush_timing(&self) -> Result<crate::file::FlushTimingSnapshot>
    {
        match self.timing(TimingOp::Flush).await? {
            TimingValue::Flush(s) => Ok(s),
            other => panic!("flush timing request answered with {other:?}"),
        }
    }

    /// Benchmark-only: zero the flush timings.
    #[doc(hidden)]
    pub async fn fh_flush_timing_reset(&self) -> Result<()>
    {
        let _ = self.timing(TimingOp::FlushReset).await?;
        Ok(())
    }

    async fn timing(&self, op: TimingOp) -> Result<TimingValue>
    {
        let (ctx, rx) = FileContext::new_timing(op);
        self.inner.send(ctx)?;
        rx.await.map_err(|_| std::io::Error::new(std::io::ErrorKind::BrokenPipe, "reactor handler task died"))?
    }

    /// Run `f` against block `idx` for reading, and return what it
    /// produced. `Ok(None)` when the block is not backed by data, in
    /// which case `f` is not called.
    ///
    /// **Populates the data cache**, matching `Hyper::fs_block`. A
    /// block fetched here is retained, so a later block access or byte
    /// read of it is served from memory. [`Self::fh_read`] does not do
    /// this.
    ///
    /// # Why a closure and not a guard
    ///
    /// The direct API hands out a borrow — `Hyper::fs_block` returns a
    /// `BlockRef` — but the reactor surface cannot. The `Hyper` lives
    /// inside the reactor task, so a borrow would reach the caller
    /// only once the response arrives, and from that moment the
    /// reactor is free to serve the next request from this handle or
    /// any clone of it. A write, flush, truncate or cache eviction
    /// would then invalidate a borrow the caller still holds; blocks
    /// in the local-disk cache additionally have their backing space
    /// punched when evicted. There is no lifetime that expresses
    /// "valid until the next message", which is why `fh_read` is
    /// sound only for as long as its caller is parked awaiting the
    /// response.
    ///
    /// So the action travels to the block. `f` runs inside the reactor
    /// task while it holds the real borrow, and only owned values
    /// cross the channel.
    ///
    /// `f` may borrow the caller's locals — it is not required to be
    /// `'static`. That is what lets a caller copy straight out of the
    /// block into a buffer it already owns, rather than returning an
    /// owned buffer and copying again. `Send` is still required,
    /// because `f` runs on the reactor's thread.
    ///
    /// The slice is always `data_block_size` bytes.
    ///
    /// # Cancellation
    ///
    /// If this future is dropped while the reactor is still running
    /// `f`, `Drop` blocks until the reactor is finished with it. `f`
    /// may hold references into the caller's frame, and a cancelled
    /// caller must not free what `f` still points at. The block is
    /// short — one closure body, no I/O, since the block is already in
    /// hand by then — and the ordinary path never blocks at all.
    /// Read a list of blocks, fetching the ones that are not cached.
    ///
    /// `f` runs once per index, inside the reactor, with the block's bytes
    /// — or `None` for an index at or past EOF, or one whose fetch found
    /// nothing behind it. Returns how many the closure was given bytes for.
    ///
    /// Two crossings for the whole list, whatever it holds: one to ask, one
    /// to deliver. The fetches run off the handler task, runs of
    /// consecutive indices are coalesced into one object request each, and
    /// separate runs go concurrently. A list is the right shape rather than
    /// a range because reads over a sparse file have gaps, and naming the
    /// blocks lets those be skipped instead of fetched.
    ///
    /// Use [`Self::fh_with_blocks`] instead when the point is to look at
    /// what is already cached without paying for anything that is not.
    /// Doing this in two steps — [`Self::fh_read_ahead`] then
    /// `fh_with_blocks` — is two round trips where the second waits on the
    /// first, which measures slower on cold blocks than reading one block
    /// at a time.
    ///
    /// `f` may borrow from its environment. It has run to completion by the
    /// time this returns, and cannot run afterwards.
    pub async fn fh_read_many<F>(&mut self, indices: &[BlockIndex], f: F) -> Result<usize>
    where
        F: FnMut(BlockIndex, Option<&[u8]>) + Send,
    {
        if indices.is_empty() {
            return Ok(0);
        }
        // As in `fh_with_block`: dropping the guard closes the gate, so the
        // borrowed closure cannot run once the caller is gone.
        let gate: crate::file::handler::BlockActionGate =
            std::sync::Arc::new(std::sync::Mutex::new(false));
        let guard = BorrowGuard { gate: gate.clone() };

        let action: Box<dyn FnMut(BlockIndex, Option<&[u8]>) + Send + '_> = Box::new(f);
        // SAFETY: as in `fh_with_block`.
        let action: crate::file::handler::BatchBlockAction = unsafe { std::mem::transmute(action) };

        let (ctx, rx) = FileContext::new_read_many(indices.to_vec(), action, gate, self.inner.clone());
        self.inner.send(ctx)?;
        let served = rx.await
            .map_err(|_| std::io::Error::new(std::io::ErrorKind::BrokenPipe, "reactor handler task died"))?;
        drop(guard);
        served
    }

    /// Visit many blocks in one crossing.
    ///
    /// `f` runs once per index, inside the reactor: `Some(bytes)` for a
    /// block resident in the data cache, `None` for one that is not.
    /// Returns how many were resident.
    ///
    /// This exists because the cost of block access on this surface is the
    /// channel crossing, not the copy. A crossing is tens of microseconds
    /// against a fraction of one to copy a block, so a caller touching
    /// hundreds of blocks — a directory listing reading inode records, say
    /// — spends nearly all its time in the channel. One message for the
    /// whole batch removes that, and needs no sharing of memory with the
    /// reactor to do it.
    ///
    /// Nothing here reaches staging, which is what keeps a batch cheap: an
    /// index that is not cached is reported absent rather than fetched, so
    /// the whole batch is answered without the handler task awaiting
    /// anything. The pairing is [`Self::fh_read_ahead`] first, which warms
    /// a range with its requests coalesced, then this — two crossings for
    /// a region however many blocks it holds.
    ///
    /// `f` may borrow from its environment. It has run to completion by
    /// the time this returns, and cannot run afterwards.
    pub async fn fh_with_blocks<F>(&mut self, indices: &[BlockIndex], f: F) -> Result<usize>
    where
        F: FnMut(BlockIndex, Option<&[u8]>) + Send,
    {
        if indices.is_empty() {
            return Ok(0);
        }
        // Shared with the request: dropping the guard closes the gate, so
        // the borrowed closure cannot run once the caller is gone. Same
        // reasoning as `fh_with_block`.
        let gate: crate::file::handler::BlockActionGate =
            std::sync::Arc::new(std::sync::Mutex::new(false));
        let guard = BorrowGuard { gate: gate.clone() };

        let action: Box<dyn FnMut(BlockIndex, Option<&[u8]>) + Send + '_> = Box::new(f);
        // SAFETY: as in `fh_with_block` — erases the closure's lifetime so
        // it can travel to the reactor, whose request type is `'static`.
        // The reactor runs it before answering, and the guard above closes
        // the gate on every path out of this function.
        let action: crate::file::handler::BatchBlockAction = unsafe { std::mem::transmute(action) };

        let (ctx, rx) = FileContext::new_with_blocks(indices.to_vec(), action, gate);
        self.inner.send(ctx)?;
        let resident = rx.await
            .map_err(|_| std::io::Error::new(std::io::ErrorKind::BrokenPipe, "reactor handler task died"))?;
        drop(guard);
        resident
    }

    pub async fn fh_with_block<R, F>(&mut self, idx: BlockIndex, f: F) -> Result<Option<R>>
    where
        F: FnOnce(&[u8]) -> R + Send,
        R: Send,
    {
        let (rtx, rrx) = oneshot::channel::<R>();
        let action: Box<dyn FnOnce(&[u8]) + Send + '_> = Box::new(move |buf: &[u8]| {
            let _ = rtx.send(f(buf));
        });
        // SAFETY: erases the closure's lifetime so it can travel to the
        // reactor, whose request type is `'static`. The closure is only
        // reachable until the reactor drops the request, and
        // `BlockActionDone` makes the caller wait for exactly that
        // before returning or unwinding — see `dispatch_with_block`.
        let action: Box<dyn FnOnce(&[u8]) + Send + 'static> = unsafe { std::mem::transmute(action) };
        self.dispatch_with_block(idx, false, BlockAction::Ref(action), rrx).await
    }

    /// Run `f` against block `idx` for modification, and return what
    /// it produced. `Ok(None)` when the block is not backed by data
    /// and `create` is false, in which case `f` is not called.
    ///
    /// Writes through the slice land in the buffer the next flush will
    /// write out; no write-back call is needed. `create` materializes
    /// a zero-filled block for an index with no data. `i_size` is not
    /// changed.
    ///
    /// **Populates the data cache**, and the block stays cached after
    /// the flush that persists it. See [`Hyper::fs_block_mut`] for the
    /// full semantics and [`Self::fh_with_block`] for why this takes a
    /// closure rather than returning a guard, for the `Send`-but-not-
    /// `'static` bound, and for what cancellation does.
    ///
    /// [`Hyper::fs_block_mut`]: crate::file::hyper::Hyper::fs_block_mut
    pub async fn fh_with_block_mut<R, F>(&mut self, idx: BlockIndex, create: bool, f: F) -> Result<Option<R>>
    where
        F: FnOnce(&mut [u8]) -> R + Send,
        R: Send,
    {
        let (rtx, rrx) = oneshot::channel::<R>();
        let action: Box<dyn FnOnce(&mut [u8]) + Send + '_> = Box::new(move |buf: &mut [u8]| {
            let _ = rtx.send(f(buf));
        });
        // SAFETY: as in `fh_with_block`.
        let action: Box<dyn FnOnce(&mut [u8]) + Send + 'static> = unsafe { std::mem::transmute(action) };
        self.dispatch_with_block(idx, create, BlockAction::Mut(action), rrx).await
    }

    /// Shared tail of the two `fh_with_block*` entry points: send the
    /// action, learn whether it ran, and if it did collect its result.
    async fn dispatch_with_block<R>(
        &mut self,
        idx: BlockIndex,
        create: bool,
        action: BlockAction,
        rrx: oneshot::Receiver<R>,
    ) -> Result<Option<R>>
    {
        // Shared with the request. Dropping the guard — on the normal
        // path, on an early return, while unwinding, or on
        // cancellation — closes the gate, so the borrowed closure
        // cannot run afterwards.
        let gate: crate::file::handler::BlockActionGate =
            std::sync::Arc::new(std::sync::Mutex::new(false));
        let guard = BorrowGuard { gate: gate.clone() };

        let (ctx, rx) = FileContext::new_with_block(idx, create, action, gate, self.inner.clone());
        self.inner.send(ctx)?;
        let ran = rx.await
            .map_err(|_| std::io::Error::new(std::io::ErrorKind::BrokenPipe, "reactor handler task died"))??;
        // The action has already run by the time the reactor answers.
        drop(guard);
        if !ran {
            return Ok(None);
        }
        // The action ran to completion before the reactor answered, so
        // its value is already queued. A closure that panicked would
        // have taken the reactor task with it, surfacing as the send
        // error above rather than here.
        let value = rrx.await
            .map_err(|_| std::io::Error::new(std::io::ErrorKind::BrokenPipe,
                "block action did not report a result"))?;
        Ok(Some(value))
    }

    pub async fn fh_getattr(&self) -> Result<libc::stat>
    {
        let (ctx, rx) = FileContext::new_getattr();
        self.inner.send(ctx)?;
        rx.await.map_err(|_| std::io::Error::new(std::io::ErrorKind::BrokenPipe, "reactor handler task died"))?
    }

    pub async fn fh_setattr(&self, stat: libc::stat) -> Result<libc::stat>
    {
        let (ctx, rx) = FileContext::new_setattr(self.inner.clone(), stat);
        self.inner.send(ctx)?;
        rx.await.map_err(|_| std::io::Error::new(std::io::ErrorKind::BrokenPipe, "reactor handler task died"))?
    }

    pub async fn fh_last_cno(&self) -> Result<u64>
    {
        let (ctx, rx) = FileContext::new_last_cno();
        self.inner.send(ctx)?;
        rx.await.map_err(|_| std::io::Error::new(std::io::ErrorKind::BrokenPipe, "reactor handler task died"))
    }
}
