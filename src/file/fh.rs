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
use super::handler::FileContext;

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
        let (ctx, mut rx) = FileContext::new_write_aligned_batch(blocks);
        self.inner.send(ctx)?;
        rx.recv().await.ok_or_else(|| std::io::Error::new(std::io::ErrorKind::BrokenPipe, "reactor handler task died"))?
    }

    pub async fn fh_write_batch(&mut self, blocks: Vec<BatchDataBlockWrapper>) -> Result<usize>
    {
        let (ctx, mut rx) = FileContext::new_write_batch(blocks);
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
        let (ctx, rx) = FileContext::new_trunc(offset);
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

        let (ctx, rx) = FileContext::new_with_block(idx, create, action, gate);
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
        let (ctx, rx) = FileContext::new_setattr(stat);
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
