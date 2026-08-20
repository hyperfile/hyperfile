//! impl request handler style IO process
use std::mem::ManuallyDrop;
use std::sync::Arc;
use bytes::Bytes;
use std::io::{Error, Result, ErrorKind};
#[cfg(feature = "wal")]
use log::{warn, info, debug};
use tokio::sync::{mpsc, oneshot, OwnedSemaphorePermit};
#[cfg(feature = "wal")]
use tokio::sync::OwnedMutexGuard;
use hyperfile_reactor::{Capacity, Channel, Task, TaskBuilder, TaskHandler};
use crate::{SegmentId, BlockIndex};
#[cfg(feature = "wal")]
use crate::inode::OnDiskState;
use crate::buffer::{DataBlock, AlignedDataBlockWrapper, BatchDataBlockWrapper};
use super::hyper::Hyper;
use super::HyperTrait;

/// Wrapper around `TaskHandler` that bundles three priority
/// channels (highprio / cb / user) and exposes them as named send
/// methods. All three channels are unbounded.
///
/// Built once per Hyper task by [`build_channel_group`]; cheaply
/// cloneable thereafter (the inner handler is `Arc`-backed and the
/// `Channel` tokens are `Copy`).
///
/// ## Channel priorities
///
/// - `highprio` (priority 0): retried requests after a contention
///   miss (range-lock taken, semaphore busy). They were already
///   accepted from the user but couldn't proceed; we put them
///   ahead of new user requests so the request being retried
///   doesn't get starved.
/// - `cb` (priority 1): internal callback re-routes (multi-hop
///   pipelines like WAL write -> WAL PUT done -> cache update).
///   Higher than `user` so the in-progress hop finishes before a
///   new request starts a new hop and grabs the per-file
///   semaphore.
/// - `user` (priority 2): incoming user requests from
///   `HyperFileHandler::fh_*` / `HyperFileTokio::*`.
pub struct ChannelGroup<Ctx> {
    inner: TaskHandler<Ctx>,
    highprio: Channel<Ctx>,
    cb: Channel<Ctx>,
    user: Channel<Ctx>,
}

impl<Ctx> Clone for ChannelGroup<Ctx> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            highprio: self.highprio,
            cb: self.cb,
            user: self.user,
        }
    }
}

impl<Ctx> ChannelGroup<Ctx> {
    /// Send a context to the user channel. Returns
    /// `Err(BrokenPipe)` if the reactor task has terminated.
    pub fn send(&self, ctx: Ctx) -> Result<()> {
        self.inner.send(self.user, ctx).map_err(|_| {
            Error::new(ErrorKind::BrokenPipe, "reactor handler task died")
        })
    }

    /// Send a context to the callback channel.
    pub fn send_cb(&self, ctx: Ctx) -> Result<()> {
        self.inner.send(self.cb, ctx).map_err(|_| {
            Error::new(ErrorKind::BrokenPipe, "reactor handler task died")
        })
    }

    /// Send a context to the high-priority channel.
    pub fn send_highprio(&self, ctx: Ctx) -> Result<()> {
        self.inner.send(self.highprio, ctx).map_err(|_| {
            Error::new(ErrorKind::BrokenPipe, "reactor handler task died")
        })
    }
}

/// Build the standard 3-channel `TaskBuilder` used by every
/// hyperfile reactor task, returning the matching `ChannelGroup`
/// constructor.
///
/// Returns `(builder, |handler| -> ChannelGroup)`: the caller
/// passes the builder to `Reactor::spawn_async`, then feeds the
/// resulting `TaskHandler` through the closure to get a fully
/// wired `ChannelGroup`.
pub fn build_channel_group<Ctx>() -> (TaskBuilder<Ctx>, impl FnOnce(TaskHandler<Ctx>) -> ChannelGroup<Ctx>)
where
    Ctx: Send + 'static,
{
    let mut builder = TaskBuilder::<Ctx>::new();
    let highprio = builder.add_channel(0, Capacity::Unbounded);
    let cb       = builder.add_channel(1, Capacity::Unbounded);
    let user     = builder.add_channel(2, Capacity::Unbounded);
    (builder, move |inner| ChannelGroup { inner, highprio, cb, user })
}

pub type FileRespGetAttr = Result<libc::stat>;
pub type FileRespSetAttr = Result<libc::stat>;
pub type FileRespRead = Result<usize>;
pub type FileRespWrite = Result<usize>;
pub type FileRespWriteZero = Result<usize>;
pub type FileRespTrunc = Result<()>;
/// Whether the block existed, so whether the action ran. The action's
/// own return value travels on its own channel.
pub type FileRespWithBlock = Result<bool>;
pub type FileRespTiming = Result<TimingValue>;
/// The bytes read, truncated to what was actually available.
pub type FileRespReadOwned = Result<Bytes>;
pub type FileRespFlush = Result<SegmentId>;
pub type FileRespRelease = Result<SegmentId>;
pub type FileRespLastCno = u64;

/// Response carrier for a request handed off to the reactor
/// handler.
///
/// Each variant holds the sender side of a channel the caller is
/// awaiting on. When the handler finishes normally it consumes the
/// variant (via `to_*()` accessors) and drives the sender directly.
///
/// The enum (rather than `union { ManuallyDrop<T> }`) matters for
/// crash safety: if the handler task panics before it reaches the
/// consumer call, this `FileResp` is dropped as part of the task
/// frame unwind, and the enum's automatic Drop drops the contained
/// sender. That closes the channel and lets the caller's
/// `rx.await` resolve with `Err(RecvError)` instead of hanging.
pub enum FileResp {
    GetAttr(oneshot::Sender<FileRespGetAttr>),
    SetAttr(oneshot::Sender<FileRespSetAttr>),
    Read(mpsc::Sender<FileRespRead>),
    Write(mpsc::Sender<FileRespWrite>),
    WriteZero(mpsc::Sender<FileRespWriteZero>),
    WriteAlignedBatch(mpsc::Sender<FileRespWrite>),
    WriteBatch(mpsc::Sender<FileRespWrite>),
    Trunc(oneshot::Sender<FileRespTrunc>),
    WithBlock(oneshot::Sender<FileRespWithBlock>),
    Timing(oneshot::Sender<FileRespTiming>),
    ReadOwned(oneshot::Sender<FileRespReadOwned>),
    Flush(oneshot::Sender<FileRespFlush>),
    #[cfg(feature = "wal")]
    WalFlush,
    #[cfg(feature = "wal")]
    WalFlushDone,
    #[cfg(feature = "wal")]
    WalFlushRecovery,
    Release(oneshot::Sender<FileRespRelease>),
    LastCno(oneshot::Sender<FileRespLastCno>),
}

impl FileResp {
    pub fn to_getattr(self) -> oneshot::Sender<FileRespGetAttr> {
        match self {
            Self::GetAttr(tx) => tx,
            _ => panic!("FileResp::to_getattr called on wrong variant"),
        }
    }

    pub fn to_setattr(self) -> oneshot::Sender<FileRespSetAttr> {
        match self {
            Self::SetAttr(tx) => tx,
            _ => panic!("FileResp::to_setattr called on wrong variant"),
        }
    }

    pub fn to_read(self) -> mpsc::Sender<FileRespRead> {
        match self {
            Self::Read(tx) => tx,
            _ => panic!("FileResp::to_read called on wrong variant"),
        }
    }

    pub fn to_write(self) -> mpsc::Sender<FileRespWrite> {
        match self {
            Self::Write(tx) | Self::WriteAlignedBatch(tx) | Self::WriteBatch(tx) => tx,
            _ => panic!("FileResp::to_write called on wrong variant"),
        }
    }

    pub fn to_write_zero(self) -> mpsc::Sender<FileRespWriteZero> {
        match self {
            Self::WriteZero(tx) => tx,
            _ => panic!("FileResp::to_write_zero called on wrong variant"),
        }
    }

    /// Answer a read, whichever form it took.
    ///
    /// A borrowed read reports how many bytes it put in the caller's
    /// buffer. An owned read hands the buffer over instead, truncated to
    /// that same count, so `owned` must be the buffer the read filled.
    pub fn answer_read(self, n: usize, owned: Option<Vec<u8>>) {
        match self {
            Self::Read(tx) => { let _ = tx.try_send(Ok(n)); },
            Self::ReadOwned(tx) => {
                let mut buf = owned.unwrap_or_default();
                buf.truncate(n);
                let _ = tx.send(Ok(Bytes::from(buf)));
            },
            _ => panic!("FileResp::answer_read called on wrong variant"),
        }
    }

    /// Fail a read, whichever form it took.
    pub fn fail_read(self, e: Error) {
        match self {
            Self::Read(tx) => { let _ = tx.try_send(Err(e)); },
            Self::ReadOwned(tx) => { let _ = tx.send(Err(e)); },
            _ => panic!("FileResp::fail_read called on wrong variant"),
        }
    }

    pub fn to_read_owned(self) -> oneshot::Sender<FileRespReadOwned> {
        match self {
            Self::ReadOwned(tx) => tx,
            _ => panic!("FileResp::to_read_owned called on wrong variant"),
        }
    }

    pub fn to_timing(self) -> oneshot::Sender<FileRespTiming> {
        match self {
            Self::Timing(tx) => tx,
            _ => panic!("FileResp::to_timing called on wrong variant"),
        }
    }

    pub fn to_with_block(self) -> oneshot::Sender<FileRespWithBlock> {
        match self {
            Self::WithBlock(tx) => tx,
            _ => panic!("FileResp::to_with_block called on wrong variant"),
        }
    }

    pub fn to_trunc(self) -> oneshot::Sender<FileRespTrunc> {
        match self {
            Self::Trunc(tx) => tx,
            _ => panic!("FileResp::to_trunc called on wrong variant"),
        }
    }

    pub fn to_flush(self) -> oneshot::Sender<FileRespFlush> {
        match self {
            Self::Flush(tx) => tx,
            _ => panic!("FileResp::to_flush called on wrong variant"),
        }
    }

    #[cfg(feature = "wal")]
    pub fn to_wal_flush(self) {
        match self {
            Self::WalFlush => {}
            _ => panic!("FileResp::to_wal_flush called on wrong variant"),
        }
    }

    #[cfg(feature = "wal")]
    pub fn to_wal_flush_done(self) {
        match self {
            Self::WalFlushDone => {}
            _ => panic!("FileResp::to_wal_flush_done called on wrong variant"),
        }
    }

    #[cfg(feature = "wal")]
    pub fn to_wal_flush_recovery(self) {
        match self {
            Self::WalFlushRecovery => {}
            _ => panic!("FileResp::to_wal_flush_recovery called on wrong variant"),
        }
    }

    pub fn to_release(self) -> oneshot::Sender<FileRespRelease> {
        match self {
            Self::Release(tx) => tx,
            _ => panic!("FileResp::to_release called on wrong variant"),
        }
    }

    pub fn to_last_cno(self) -> oneshot::Sender<FileRespLastCno> {
        match self {
            Self::LastCno(tx) => tx,
            _ => panic!("FileResp::to_last_cno called on wrong variant"),
        }
    }

    pub fn clone_write_resp(&self) -> mpsc::Sender<FileRespWrite> {
        match self {
            Self::Write(tx) | Self::WriteAlignedBatch(tx) | Self::WriteBatch(tx) => tx.clone(),
            _ => panic!("FileResp::clone_write_resp called on wrong variant"),
        }
    }

    pub fn clone_write_zero_resp(&self) -> mpsc::Sender<FileRespWriteZero> {
        match self {
            Self::WriteZero(tx) => tx.clone(),
            _ => panic!("FileResp::clone_write_zero_resp called on wrong variant"),
        }
    }
}

// define request params
pub struct FileReqRead<'a> {
    /// Set when the reactor, not the caller, owns the destination.
    ///
    /// `fh_read` points `buf` at the caller's memory, valid only while
    /// the caller stays parked on the response. `fh_read_owned` instead
    /// allocates here and points `buf` into it, so the destination
    /// belongs to the request and travels with it — through the
    /// flush-wait and range-lock requeues, into the task that fills it,
    /// and safely past a caller that has gone away. A `Vec` does not
    /// move its allocation when the handle moves, so `buf` stays valid.
    ///
    /// Which of the two it is decides what the response carries: a byte
    /// count for a borrowed read, the buffer itself for an owned one.
    pub owned: Option<Vec<u8>>,
    pub buf: &'a mut [u8],
    pub offset: usize,
    pub fh: ChannelGroup<FileContext<'a>>,
}

pub struct FileReqWrite<'a> {
    pub buf: &'a [u8],
    pub offset: usize,
    pub fetched: Vec<DataBlock>,
    pub spawn_write_permit: Option<OwnedSemaphorePermit>, // hold owned permit for spawn_write
    pub fh: ChannelGroup<FileContext<'a>>,
    /// Keeps `buf`'s referent alive when the caller does not.
    ///
    /// `fh_write` points `buf` at the caller's memory, which is only
    /// valid while the caller stays parked on the response.
    /// `fh_write_owned` instead puts the bytes here and points `buf`
    /// into them, so the referent belongs to the request and travels
    /// with it — through the absorb and requeue stages, and safely past
    /// a caller that has gone away. `Bytes` does not move its
    /// allocation when the handle moves, so `buf` stays valid.
    pub owned: Option<Bytes>,
}

pub struct FileReqWriteZero<'a> {
    pub offset: usize,
    pub len: usize,
    pub fetched: Vec<DataBlock>,
    pub spawn_write_permit: Option<OwnedSemaphorePermit>, // hold owned permit for spawn_write
    pub fh: ChannelGroup<FileContext<'a>>,
}

pub struct FileReqWriteAlignedBatch {
    pub data_blocks: Vec<AlignedDataBlockWrapper>,
}

pub struct FileReqWriteBatch {
    pub data_blocks: Vec<BatchDataBlockWrapper>,
}

pub struct FileReqTrunc {
    pub offset: usize,
}

/// A caller-supplied action to run against a borrowed data block.
///
/// The reactor owns the `Hyper`, so a block borrow cannot be handed
/// back across the channel: the caller would receive it only after
/// the response arrives, by which point the reactor is free to
/// process the next request — from this handle or any clone of it —
/// and a write, flush, truncate or eviction could invalidate the
/// borrow. Views into the local-disk cache also have their backing
/// space punched on eviction.
///
/// So the action travels to the block instead of the block travelling
/// to the caller, and the borrow never leaves the reactor task. The
/// closure returns nothing: `fh_with_block*` wraps the caller's
/// closure in one that sends the result down a dedicated channel,
/// which keeps this type free of the caller's return type and so
/// storable in `FileReqBody`.
///
/// The boxes are `'static` in the type but not in fact: the closure
/// may borrow the caller's locals, and `fh_with_block*` erases that
/// lifetime to get it here. What keeps that sound is
/// [`BlockActionGate`], not the type.
pub enum BlockAction {
    Ref(Box<dyn FnOnce(&[u8]) + Send>),
    Mut(Box<dyn FnOnce(&mut [u8]) + Send>),
}

/// Decides whether a borrowed closure may still run.
///
/// The closure may hold references into the caller's frame, so it must
/// not run after the caller has gone. Awaiting the response covers the
/// ordinary path, but an awaiting future can be dropped, and a
/// cancelled caller would free what the closure still points at.
///
/// `true` means the caller is gone. The caller sets it when its guard
/// drops; the dispatcher checks it while holding the lock and skips the
/// closure if set. Holding the lock across the check and the call is
/// what closes the race.
///
/// The lock is deliberately taken *after* the block has been fetched,
/// so the critical section is one closure body with no I/O in it. A
/// cancelling caller therefore blocks for microseconds at worst. This
/// matters: the reactor's object-store I/O can be driven by the
/// caller's runtime, so blocking the caller across an await would
/// deadlock.
pub type BlockActionGate = Arc<std::sync::Mutex<bool>>;

/// Which counters a timing request is about.
#[derive(Clone, Copy, Debug)]
pub enum TimingOp {
    Read,
    ReadReset,
    Flush,
    FlushReset,
}

pub struct FileReqTiming {
    pub op: TimingOp,
}

/// A counter snapshot, or an acknowledgement for a reset.
///
/// Snapshots are owned values rather than references to the live
/// counters, which cannot leave the reactor task.
#[derive(Clone, Copy, Debug)]
pub enum TimingValue {
    Read(super::ReadTimingSnapshot),
    Flush(super::FlushTimingSnapshot),
    Reset,
}

pub struct FileReqWithBlock {
    pub blk_idx: BlockIndex,
    /// Materialize a zero-filled block if the index has no data.
    /// Ignored by [`BlockAction::Ref`], which cannot create.
    pub create: bool,
    pub action: BlockAction,
    /// Guards `action` against running after the caller is gone. See
    /// [`BlockActionGate`].
    pub gate: BlockActionGate,
}

pub struct FileReqGetAttr {}

pub struct FileReqSetAttr {
    pub stat: libc::stat,
}

pub struct FileReqFlush<'a> {
    pub fh: ChannelGroup<FileContext<'a>>,
}

#[cfg(feature = "wal")]
pub struct FileReqWalFlush<'a> {
    pub fh: ChannelGroup<FileContext<'a>>,
}

#[cfg(feature = "wal")]
pub struct FileReqWalFlushDone {
    pub lock: OwnedMutexGuard<()>,
    pub segid: SegmentId,
    pub od_state: OnDiskState,
    pub bmap_cache_limit: usize,
}

#[cfg(feature = "wal")]
pub struct FileReqWalFlushRecovery {
    pub lock: OwnedMutexGuard<()>,
}

pub struct FileReqRelease<'a> {
    pub fh: ChannelGroup<FileContext<'a>>,
}

pub struct FileReqLastCno {}

pub enum FileReqOp {
    GetAttr,
    SetAttr,
    Read,
    Write,
    WriteAbsorb,
    WriteAbsorbBh,
    WriteZero,
    WriteZeroAbsorb,
    WriteZeroAbsorbBh,
    WriteAlignedBatch,
    WriteBatch,
    Trunc,
    WithBlock,
    Timing,
    Flush,
    FlushData,
    #[cfg(feature = "wal")]
    WalFlush,
    #[cfg(feature = "wal")]
    WalFlushDone,
    #[cfg(feature = "wal")]
    WalFlushRecovery,
    Release,
    LastCno,
    #[cfg(feature = "wal")]
    WriteWal,
    #[cfg(feature = "wal")]
    WriteZeroWal,
}

#[repr(C)]
pub union FileReqBody<'a> {
    getattr: ManuallyDrop<FileReqGetAttr>,
    setattr: ManuallyDrop<FileReqSetAttr>,
    read: ManuallyDrop<FileReqRead<'a>>,
    write: ManuallyDrop<FileReqWrite<'a>>,
    write_zero: ManuallyDrop<FileReqWriteZero<'a>>,
    write_aligned_batch: ManuallyDrop<FileReqWriteAlignedBatch>,
    write_batch: ManuallyDrop<FileReqWriteBatch>,
    trunc: ManuallyDrop<FileReqTrunc>,
    with_block: ManuallyDrop<FileReqWithBlock>,
    timing: ManuallyDrop<FileReqTiming>,
    #[cfg(feature = "wal")]
    wal_flush: ManuallyDrop<FileReqWalFlush<'a>>,
    #[cfg(feature = "wal")]
    wal_flush_done: ManuallyDrop<FileReqWalFlushDone>,
    #[cfg(feature = "wal")]
    wal_flush_recovery: ManuallyDrop<FileReqWalFlushRecovery>,
    flush: ManuallyDrop<FileReqFlush<'a>>,
    release: ManuallyDrop<FileReqRelease<'a>>,
    last_cno: ManuallyDrop<FileReqLastCno>,
}

pub struct FileReq<'a> {
    op: FileReqOp,
    body: FileReqBody<'a>,
}

pub struct FileContext<'a> {
    req: Option<FileReq<'a>>,
    resp: Option<FileResp>,
}

impl<'a> FileContext<'a> {
    pub fn take(mut self) -> (FileReq<'a>, FileResp) {
        (
            self.req.take().unwrap(),
            self.resp.take().unwrap()
        )
    }

    pub fn new_getattr() -> (Self, oneshot::Receiver<FileRespGetAttr>) {
        let (tx, rx) = oneshot::channel::<FileRespGetAttr>();
        let req = FileReq {
            op: FileReqOp::GetAttr,
            body: FileReqBody { getattr: ManuallyDrop::new(FileReqGetAttr {}), },
        };
        let resp = FileResp::GetAttr(tx);
        (Self { req: Some(req), resp: Some(resp), }, rx)
    }

    pub fn new_setattr(stat: libc::stat) -> (Self, oneshot::Receiver<FileRespSetAttr>) {
        let (tx, rx) = oneshot::channel::<FileRespSetAttr>();
        let req = FileReq {
            op: FileReqOp::SetAttr,
            body: FileReqBody { setattr: ManuallyDrop::new(FileReqSetAttr { stat: stat }), },
        };
        let resp = FileResp::SetAttr(tx);
        (Self { req: Some(req), resp: Some(resp), }, rx)
    }

    // return both tx and rx, for mpsc channel we need to keep tx until we receved response
    pub fn new_read(buf: &'a mut [u8], offset: usize, fh: ChannelGroup<FileContext<'a>>) -> (Self, mpsc::Sender<FileRespRead>, mpsc::Receiver<FileRespRead>) {
        let (tx, rx) = mpsc::channel::<FileRespRead>(1);
        let req = FileReq {
            op: FileReqOp::Read,
            body: FileReqBody { read: ManuallyDrop::new(FileReqRead { buf: buf, offset: offset, fh: fh, owned: None }), },
        };
        let resp = FileResp::Read(tx.clone());
        (Self { req: Some(req), resp: Some(resp), }, tx, rx)
    }

    // return both tx and rx, for mpsc channel we need to keep tx until we receved response
    pub fn new_write(buf: &'a [u8], offset: usize, fh: ChannelGroup<FileContext<'a>>) -> (Self, mpsc::Sender<FileRespWrite>, mpsc::Receiver<FileRespWrite>) {
        let (tx, rx) = mpsc::channel::<FileRespWrite>(1);
        let req = FileReq {
            op: FileReqOp::Write,
            body: FileReqBody { write: ManuallyDrop::new(FileReqWrite { buf: buf, offset: offset, spawn_write_permit: None, fh: fh, fetched: Vec::new(), owned: None, }), },
        };
        let resp = FileResp::Write(tx.clone());
        (Self { req: Some(req), resp: Some(resp), }, tx, rx)
    }

    #[cfg(feature = "wal")]
    pub fn write_wal(req: FileReqWrite<'a>, resp: FileResp) -> Self {
        let new_req = FileReq {
            op: FileReqOp::WriteWal,
            body: FileReqBody { write: ManuallyDrop::new(req), },
        };
        Self { req: Some(new_req), resp: Some(resp), }
    }

    // convert context from op Write to op WriteAbsorb
    pub fn write_absorb(req: FileReqWrite<'a>, resp: FileResp) -> Self {
        let new_req = FileReq {
            op: FileReqOp::WriteAbsorb,
            body: FileReqBody { write: ManuallyDrop::new(req), },
        };
        Self { req: Some(new_req), resp: Some(resp), }
    }

    pub fn write_absorb_bh(req: FileReqWrite<'a>, resp: FileResp) -> Self {
        let new_req = FileReq {
            op: FileReqOp::WriteAbsorbBh,
            body: FileReqBody { write: ManuallyDrop::new(req), },
        };
        Self { req: Some(new_req), resp: Some(resp), }
    }

    // return both tx and rx, for mpsc channel we need to keep tx until we receved response
    pub fn new_write_zero(offset: usize, len: usize, fh: ChannelGroup<FileContext<'a>>) -> (Self, mpsc::Sender<FileRespWrite>, mpsc::Receiver<FileRespWrite>) {
        let (tx, rx) = mpsc::channel::<FileRespWriteZero>(1);
        let req = FileReq {
            op: FileReqOp::WriteZero,
            body: FileReqBody { write_zero: ManuallyDrop::new(FileReqWriteZero { offset: offset, len: len, spawn_write_permit: None, fh: fh, fetched: Vec::new(), }), },
        };
        let resp = FileResp::WriteZero(tx.clone());
        (Self { req: Some(req), resp: Some(resp), }, tx, rx)
    }

    #[cfg(feature = "wal")]
    pub fn write_zero_wal(req: FileReqWriteZero<'a>, resp: FileResp) -> Self {
        let new_req = FileReq {
            op: FileReqOp::WriteZeroWal,
            body: FileReqBody { write_zero: ManuallyDrop::new(req), },
        };
        Self { req: Some(new_req), resp: Some(resp), }
    }

    // convert context from op Write to op WriteAbsorb
    pub fn write_zero_absorb(req: FileReqWriteZero<'a>, resp: FileResp) -> Self {
        let new_req = FileReq {
            op: FileReqOp::WriteZeroAbsorb,
            body: FileReqBody { write_zero: ManuallyDrop::new(req), },
        };
        Self { req: Some(new_req), resp: Some(resp), }
    }

    pub fn write_zero_absorb_bh(req: FileReqWriteZero<'a>, resp: FileResp) -> Self {
        let new_req = FileReq {
            op: FileReqOp::WriteZeroAbsorbBh,
            body: FileReqBody { write_zero: ManuallyDrop::new(req), },
        };
        Self { req: Some(new_req), resp: Some(resp), }
    }

    pub fn new_write_aligned_batch(v: Vec<AlignedDataBlockWrapper>) -> (Self, mpsc::Receiver<FileRespWrite>) {
        let (tx, rx) = mpsc::channel::<FileRespWrite>(1);
        let new_req = FileReq {
            op: FileReqOp::WriteAlignedBatch,
            body: FileReqBody { write_aligned_batch: ManuallyDrop::new(FileReqWriteAlignedBatch { data_blocks: v }), },
        };
        let resp = FileResp::WriteAlignedBatch(tx);
        (Self { req: Some(new_req), resp: Some(resp), }, rx)
    }

    pub fn new_write_batch(v: Vec<BatchDataBlockWrapper>) -> (Self, mpsc::Receiver<FileRespWrite>) {
        let (tx, rx) = mpsc::channel::<FileRespWrite>(1);
        let new_req = FileReq {
            op: FileReqOp::WriteBatch,
            body: FileReqBody { write_batch: ManuallyDrop::new(FileReqWriteBatch { data_blocks: v }), },
        };
        let resp = FileResp::WriteBatch(tx);
        (Self { req: Some(new_req), resp: Some(resp), }, rx)
    }

    pub fn new_trunc(offset: usize) -> (Self, oneshot::Receiver<FileRespTrunc>) {
        let (tx, rx) = oneshot::channel::<FileRespTrunc>();
        let req = FileReq {
            op: FileReqOp::Trunc,
            body: FileReqBody { trunc: ManuallyDrop::new(FileReqTrunc { offset: offset }), },
        };
        let resp = FileResp::Trunc(tx);
        (Self { req: Some(req), resp: Some(resp), }, rx)
    }

    /// A write whose bytes the request owns. See
    /// [`FileReqWrite::owned`].
    pub fn new_write_owned(buf: Bytes, offset: usize, fh: ChannelGroup<FileContext<'a>>)
        -> (Self, mpsc::Sender<FileRespWrite>, mpsc::Receiver<FileRespWrite>)
    {
        let (tx, rx) = mpsc::channel::<FileRespWrite>(1);
        // SAFETY: points into the `Bytes` stored alongside it. `Bytes`
        // keeps its allocation put when the handle moves, and the
        // handle travels with the request, so the referent outlives
        // every stage that reads it — including after the caller is
        // gone.
        let slice: &'a [u8] = unsafe {
            std::slice::from_raw_parts(buf.as_ptr(), buf.len())
        };
        let req = FileReq {
            op: FileReqOp::Write,
            body: FileReqBody { write: ManuallyDrop::new(FileReqWrite {
                buf: slice, offset, spawn_write_permit: None, fh,
                fetched: Vec::new(), owned: Some(buf),
            }), },
        };
        let resp = FileResp::Write(tx.clone());
        (Self { req: Some(req), resp: Some(resp), }, tx, rx)
    }

    /// A read whose destination the reactor owns. Shares
    /// [`FileReqOp::Read`] with the borrowed form, so it takes the same
    /// spawning path; see [`FileReqRead::owned`].
    pub fn new_read_owned(offset: usize, len: usize, fh: ChannelGroup<FileContext<'a>>)
        -> (Self, oneshot::Receiver<FileRespReadOwned>)
    {
        let (tx, rx) = oneshot::channel::<FileRespReadOwned>();
        let mut owned = vec![0u8; len];
        // SAFETY: points into the `Vec` stored alongside it. A `Vec`
        // keeps its allocation put when the handle moves, and the handle
        // travels with the request, so the referent outlives every
        // stage that writes to it. The `Vec` is not touched again until
        // the reads have finished and it is moved out to be returned.
        let slice: &'a mut [u8] = unsafe {
            std::slice::from_raw_parts_mut(owned.as_mut_ptr(), owned.len())
        };
        let req = FileReq {
            op: FileReqOp::Read,
            body: FileReqBody { read: ManuallyDrop::new(FileReqRead {
                buf: slice, offset, fh, owned: Some(owned),
            }), },
        };
        let resp = FileResp::ReadOwned(tx);
        (Self { req: Some(req), resp: Some(resp), }, rx)
    }

    pub fn new_timing(op: TimingOp) -> (Self, oneshot::Receiver<FileRespTiming>) {
        let (tx, rx) = oneshot::channel::<FileRespTiming>();
        let req = FileReq {
            op: FileReqOp::Timing,
            body: FileReqBody { timing: ManuallyDrop::new(FileReqTiming { op }), },
        };
        let resp = FileResp::Timing(tx);
        (Self { req: Some(req), resp: Some(resp), }, rx)
    }

    pub fn new_with_block(blk_idx: BlockIndex, create: bool, action: BlockAction, gate: BlockActionGate)
        -> (Self, oneshot::Receiver<FileRespWithBlock>)
    {
        let (tx, rx) = oneshot::channel::<FileRespWithBlock>();
        let req = FileReq {
            op: FileReqOp::WithBlock,
            body: FileReqBody {
                with_block: ManuallyDrop::new(FileReqWithBlock { blk_idx, create, action, gate }),
            },
        };
        let resp = FileResp::WithBlock(tx);
        (Self { req: Some(req), resp: Some(resp), }, rx)
    }

    pub fn new_flush(fh: ChannelGroup<FileContext<'a>>) -> (Self, oneshot::Receiver<FileRespFlush>) {
        let (tx, rx) = oneshot::channel::<FileRespFlush>();
        let req = FileReq {
            op: FileReqOp::Flush,
            body: FileReqBody { flush: ManuallyDrop::new(FileReqFlush { fh, }), },
        };
        let resp = FileResp::Flush(tx);
        (Self { req: Some(req), resp: Some(resp), }, rx)
    }

    /// Like `new_flush` but with `fdatasync` semantics — see
    /// `Hyper::fs_fdatasync`. Reuses the `FileReqFlush` body and
    /// `FileResp::Flush` response shape; the only difference is
    /// the `op` discriminator routes to the
    /// `FileReqOp::FlushData` arm of `Task::handle`.
    pub fn new_flush_data(fh: ChannelGroup<FileContext<'a>>) -> (Self, oneshot::Receiver<FileRespFlush>) {
        let (tx, rx) = oneshot::channel::<FileRespFlush>();
        let req = FileReq {
            op: FileReqOp::FlushData,
            body: FileReqBody { flush: ManuallyDrop::new(FileReqFlush { fh, }), },
        };
        let resp = FileResp::Flush(tx);
        (Self { req: Some(req), resp: Some(resp), }, rx)
    }

    // only used by internal driven process when wal enabled
    #[cfg(feature = "wal")]
    pub fn new_wal_flush(fh: ChannelGroup<FileContext<'a>>) -> Self {
        let req = FileReq {
            op: FileReqOp::WalFlush,
            body: FileReqBody { wal_flush: ManuallyDrop::new(FileReqWalFlush { fh, }), },
        };
        let resp = FileResp::WalFlush;
        Self { req: Some(req), resp: Some(resp) }
    }

    #[cfg(feature = "wal")]
    pub fn new_wal_flush_done(lock: OwnedMutexGuard<()>, segid: SegmentId, od_state: OnDiskState, bmap_cache_limit: usize) -> Self {
        let req = FileReq {
            op: FileReqOp::WalFlushDone,
            body: FileReqBody { wal_flush_done: ManuallyDrop::new(FileReqWalFlushDone { lock, segid, od_state, bmap_cache_limit, }), },
        };
        let resp = FileResp::WalFlushDone;
        Self { req: Some(req), resp: Some(resp) }
    }

    #[cfg(feature = "wal")]
    pub fn new_wal_flush_recovery(lock: OwnedMutexGuard<()>) -> Self {
        let req = FileReq {
            op: FileReqOp::WalFlushRecovery,
            body: FileReqBody { wal_flush_recovery: ManuallyDrop::new(FileReqWalFlushRecovery { lock, }), },
        };
        let resp = FileResp::WalFlushRecovery;
        Self { req: Some(req), resp: Some(resp) }
    }

    pub fn new_release(fh: ChannelGroup<FileContext<'a>>) -> (Self, oneshot::Receiver<FileRespRelease>) {
        let (tx, rx) = oneshot::channel::<FileRespRelease>();
        let req = FileReq {
            op: FileReqOp::Release,
            body: FileReqBody { release: ManuallyDrop::new(FileReqRelease { fh, }), },
        };
        let resp = FileResp::Release(tx);
        (Self { req: Some(req), resp: Some(resp), }, rx)
    }

    pub fn new_last_cno() -> (Self, oneshot::Receiver<FileRespLastCno>) {
        let (tx, rx) = oneshot::channel::<FileRespLastCno>();
        let req = FileReq {
            op: FileReqOp::LastCno,
            body: FileReqBody { last_cno: ManuallyDrop::new(FileReqLastCno {}), },
        };
        let resp = FileResp::LastCno(tx);
        (Self { req: Some(req), resp: Some(resp), }, rx)
    }

    pub fn reform_read(req: FileReqRead<'a>, resp: FileResp) -> Self {
        let req = FileReq {
            op: FileReqOp::Read,
            body: FileReqBody { read: ManuallyDrop::new(req) },
        };
        Self { req: Some(req), resp: Some(resp) }
    }

    pub fn reform_write(req: FileReqWrite<'a>, resp: FileResp) -> Self {
        let req = FileReq {
            op: FileReqOp::Write,
            body: FileReqBody { write: ManuallyDrop::new(req) },
        };
        Self { req: Some(req), resp: Some(resp) }
    }

    pub fn reform_write_zero(req: FileReqWriteZero<'a>, resp: FileResp) -> Self {
        let req = FileReq {
            op: FileReqOp::WriteZero,
            body: FileReqBody { write_zero: ManuallyDrop::new(req) },
        };
        Self { req: Some(req), resp: Some(resp) }
    }

    pub fn reform_release(req: FileReqRelease<'a>, resp: FileResp) -> Self {
        let req = FileReq {
            op: FileReqOp::Release,
            body: FileReqBody { release: ManuallyDrop::new(req) },
        };
        Self { req: Some(req), resp: Some(resp) }
    }

    pub fn reform_flush(req: FileReqFlush<'a>, resp: FileResp) -> Self {
        let req = FileReq {
            op: FileReqOp::Flush,
            body: FileReqBody { flush: ManuallyDrop::new(req) },
        };
        Self { req: Some(req), resp: Some(resp) }
    }

    pub fn reform_flush_data(req: FileReqFlush<'a>, resp: FileResp) -> Self {
        let req = FileReq {
            op: FileReqOp::FlushData,
            body: FileReqBody { flush: ManuallyDrop::new(req) },
        };
        Self { req: Some(req), resp: Some(resp) }
    }

    #[cfg(feature = "wal")]
    pub fn reform_wal_flush(req: FileReqWalFlush<'a>, resp: FileResp) -> Self {
        let req = FileReq {
            op: FileReqOp::WalFlush,
            body: FileReqBody { wal_flush: ManuallyDrop::new(req) },
        };
        Self { req: Some(req), resp: Some(resp) }
    }
}

impl<'a: 'static> Task<FileContext<'a>> for Hyper<'a>
{
    // main loop
    async fn handle(&mut self, ctx: FileContext<'a>) {
        let (req, resp) = ctx.take();
        match req.op {
            FileReqOp::GetAttr => {
                let md = unsafe { req.body.getattr };
                let _ = ManuallyDrop::into_inner(md);
                let stat = self.inner.stat();
                let resp = resp.to_getattr();
                let _ = resp.send(Ok(stat));
            },
            FileReqOp::SetAttr => {
                let md = unsafe { req.body.setattr };
                let req = ManuallyDrop::into_inner(md);
                let stat = req.stat;
                let res = self.inner.update_stat(&stat).await;
                let _ = resp.to_setattr().send(res);
            },
            FileReqOp::Read => {
                let md = unsafe { req.body.read };
                let req = ManuallyDrop::into_inner(md);
                #[cfg(feature = "range-lock")]
                let range = req.offset as u64..(req.offset + req.buf.len()) as u64;
                // `spawn_read` answers its own errors, so the response
                // is handed straight over. An owned read replies on a
                // oneshot, which cannot be cloned for a second owner the
                // way the borrowed form's mpsc sender was.
                let res = self.inner.spawn_read(req, resp).await;
                match res {
                    Ok(_) => {}
                    Err(ref e) => {
                        if e.kind() != ErrorKind::ResourceBusy {
                            #[cfg(feature = "range-lock")]
                            self.inner.range_lock.try_unlock(range);
                        }
                    },
                }
            },
            FileReqOp::Write => {
                let md = unsafe { req.body.write };
                let req = ManuallyDrop::into_inner(md);
                #[cfg(feature = "range-lock")]
                let range = req.offset as u64..(req.offset + req.buf.len()) as u64;
                // prepare error response handler
                let _resp_write = resp.to_write();
                let resp_write = _resp_write.clone();
                let resp = FileResp::Write(_resp_write);
                let res = self.inner.spawn_write(req, resp).await;
                match res {
                    Ok(bytes) => {
                        let _ = resp_write.try_send(Ok(bytes));
                    },
                    Err(ref e) => {
                        if e.kind() != ErrorKind::ResourceBusy {
                            #[cfg(feature = "range-lock")]
                            self.inner.range_lock.try_unlock(range);
                            let _ = resp_write.try_send(res);
                        }
                    },
                }
            },
            #[cfg(feature = "wal")]
            FileReqOp::WriteWal => {
                let md = unsafe { req.body.write };
                let req = ManuallyDrop::into_inner(md);
                #[cfg(feature = "range-lock")]
                let range = req.offset as u64..(req.offset + req.buf.len()) as u64;
                // prepare error response handler
                let _resp_write = resp.to_write();
                let resp_write = _resp_write.clone();
                let resp = FileResp::Write(_resp_write);
                let res = self.inner.spawn_write_wal(req, resp).await;
                match res {
                    Ok(_) => {},
                    Err(ref e) => {
                        if e.kind() != ErrorKind::ResourceBusy {
                            #[cfg(feature = "range-lock")]
                            self.inner.range_lock.try_unlock(range);
                            let _ = resp_write.try_send(res);
                        }
                    },
                }
            },
            FileReqOp::WriteAbsorb => {
                let md = unsafe { req.body.write };
                let mut req = ManuallyDrop::into_inner(md);
                #[cfg(feature = "range-lock")]
                let range = req.offset as u64..(req.offset + req.buf.len()) as u64;
                let _resp_write = resp.to_write();
                let resp_write = _resp_write.clone();
                let resp = FileResp::Write(_resp_write);
                let mut fetched = Vec::new();
                fetched.append(&mut req.fetched);
                let res = self.inner.absorb_write(req, resp, fetched).await;
                match res {
                    Ok(bytes) => {
                        let _ = resp_write.try_send(Ok(bytes));
                    },
                    Err(ref e) => {
                        if e.kind() != ErrorKind::ResourceBusy {
                            #[cfg(feature = "range-lock")]
                            self.inner.range_lock.try_unlock(range);
                            let _ = resp_write.try_send(res);
                        }
                    },
                }
            },
            FileReqOp::WriteAbsorbBh => {
                let md = unsafe { req.body.write };
                let req = ManuallyDrop::into_inner(md);
                let _resp_write = resp.to_write();
                let resp_write = _resp_write.clone();
                let resp = FileResp::Write(_resp_write);
                let res = self.inner.absorb_write_bh(req, resp).await;
                let _ = resp_write.try_send(res);
            },
            FileReqOp::WriteZero => {
                let md = unsafe { req.body.write_zero };
                let req = ManuallyDrop::into_inner(md);
                #[cfg(feature = "range-lock")]
                let range = req.offset as u64..(req.offset + req.len) as u64;
                // prepare error response handler
                let _resp_write = resp.to_write_zero();
                let resp_write = _resp_write.clone();
                let resp = FileResp::WriteZero(_resp_write);
                let res = self.inner.spawn_write_zero(req, resp).await;
                match res {
                    Ok(bytes) => {
                        let _ = resp_write.try_send(Ok(bytes));
                    },
                    Err(ref e) => {
                        if e.kind() != ErrorKind::ResourceBusy {
                            #[cfg(feature = "range-lock")]
                            self.inner.range_lock.try_unlock(range);
                            let _ = resp_write.try_send(res);
                        }
                    },
                }
            },
            #[cfg(feature = "wal")]
            FileReqOp::WriteZeroWal => {
                let md = unsafe { req.body.write_zero };
                let req = ManuallyDrop::into_inner(md);
                #[cfg(feature = "range-lock")]
                let range = req.offset as u64..(req.offset + req.len) as u64;
                // prepare error response handler
                let _resp_write = resp.to_write_zero();
                let resp_write = _resp_write.clone();
                let resp = FileResp::WriteZero(_resp_write);
                let res = self.inner.spawn_write_zero_wal(req, resp).await;
                match res {
                    Ok(_) => {},
                    Err(ref e) => {
                        if e.kind() != ErrorKind::ResourceBusy {
                            #[cfg(feature = "range-lock")]
                            self.inner.range_lock.try_unlock(range);
                            let _ = resp_write.try_send(res);
                        }
                    },
                }
            },
            FileReqOp::WriteZeroAbsorb => {
                let md = unsafe { req.body.write_zero };
                let mut req = ManuallyDrop::into_inner(md);
                #[cfg(feature = "range-lock")]
                let range = req.offset as u64..(req.offset + req.len) as u64;
                let _resp_write = resp.to_write_zero();
                let resp_write = _resp_write.clone();
                let resp = FileResp::WriteZero(_resp_write);
                let mut fetched = Vec::new();
                fetched.append(&mut req.fetched);
                let res = self.inner.absorb_write_zero(req, resp, fetched).await;
                match res {
                    Ok(bytes) => {
                        let _ = resp_write.try_send(Ok(bytes));
                    },
                    Err(ref e) => {
                        if e.kind() != ErrorKind::ResourceBusy {
                            #[cfg(feature = "range-lock")]
                            self.inner.range_lock.try_unlock(range);
                            let _ = resp_write.try_send(res);
                        }
                    },
                }
            },
            FileReqOp::WriteZeroAbsorbBh => {
                let md = unsafe { req.body.write_zero };
                let req = ManuallyDrop::into_inner(md);
                let _resp_write = resp.to_write_zero();
                let resp_write = _resp_write.clone();
                let resp = FileResp::WriteZero(_resp_write);
                let res = self.inner.absorb_write_zero_bh(req, resp).await;
                let _ = resp_write.try_send(res);
            },
            FileReqOp::WriteAlignedBatch => {
                let md = unsafe { req.body.write_aligned_batch };
                let req = ManuallyDrop::into_inner(md);
                let blocks = req.data_blocks;
                let res = self.inner.write_aligned_batch(blocks).await;
                let _ = resp.to_write().try_send(res);
            },
            FileReqOp::WriteBatch => {
                let md = unsafe { req.body.write_batch };
                let req = ManuallyDrop::into_inner(md);
                let blocks = req.data_blocks;
                let res = self.inner.write_batch(blocks).await;
                let _ = resp.to_write().try_send(res);
            },
            FileReqOp::Trunc => {
                let md = unsafe { req.body.trunc };
                let req = ManuallyDrop::into_inner(md);
                let offset = req.offset;
                let res = self.inner.truncate(offset).await;
                let _ = resp.to_trunc().send(res);
            },
            FileReqOp::Timing => {
                let md = unsafe { req.body.timing };
                let req = ManuallyDrop::into_inner(md);
                let v = match req.op {
                    TimingOp::Read => TimingValue::Read(self.inner.read_timing().snapshot()),
                    TimingOp::ReadReset => {
                        self.inner.read_timing_reset();
                        TimingValue::Reset
                    },
                    TimingOp::Flush => TimingValue::Flush(self.inner.flush_timing().snapshot()),
                    TimingOp::FlushReset => {
                        self.inner.flush_timing_reset();
                        TimingValue::Reset
                    },
                };
                let _ = resp.to_timing().send(Ok(v));
            },
            FileReqOp::WithBlock => {
                let md = unsafe { req.body.with_block };
                let req = ManuallyDrop::into_inner(md);
                let FileReqWithBlock { blk_idx, create, action, gate } = req;
                // The borrow stays inside this task for the whole
                // call; only the action's own result leaves, on the
                // channel its closure captured.
                // Fetch the block first, then take the gate to run the
                // action. Nothing awaits inside the gate, so a
                // cancelling caller waits only for the closure body.
                let res = match action {
                    BlockAction::Ref(f) => {
                        match self.inner.block(blk_idx).await {
                            Ok(Some(block)) => {
                                let cancelled = gate.lock().unwrap_or_else(|e| e.into_inner());
                                if !*cancelled {
                                    f(block.as_slice());
                                }
                                Ok(true)
                            },
                            Ok(None) => Ok(false),
                            Err(e) => Err(e),
                        }
                    },
                    BlockAction::Mut(f) => {
                        match self.inner.block_mut(blk_idx, create).await {
                            Ok(Some(mut block)) => {
                                let cancelled = gate.lock().unwrap_or_else(|e| e.into_inner());
                                if !*cancelled {
                                    f(block.as_mut_slice());
                                }
                                Ok(true)
                            },
                            Ok(None) => Ok(false),
                            Err(e) => Err(e),
                        }
                    },
                };
                let _ = resp.to_with_block().send(res);
            },
            // single flush interface for extenral
            #[cfg(not(feature = "wal"))]
            FileReqOp::Flush => {
                let md = unsafe { req.body.flush };
                let _req = ManuallyDrop::into_inner(md);
                #[cfg(feature = "range-lock")]
                if self.inner.range_lock.is_locked() {
                    let fh = _req.fh.clone();
                    let ctx = FileContext::reform_flush(_req, resp);
                    let _ = fh.send_cb(ctx);
                    return;
                }
                let res = self.inner.flush().await;
                let _ = resp.to_flush().send(res);
            },
            // POSIX-`fdatasync` flavoured flush. Under non-WAL,
            // `inner.flush_data` short-circuits when only attrs
            // are dirty.
            #[cfg(not(feature = "wal"))]
            FileReqOp::FlushData => {
                let md = unsafe { req.body.flush };
                let _req = ManuallyDrop::into_inner(md);
                #[cfg(feature = "range-lock")]
                if self.inner.range_lock.is_locked() {
                    let fh = _req.fh.clone();
                    let ctx = FileContext::reform_flush_data(_req, resp);
                    let _ = fh.send_cb(ctx);
                    return;
                }
                let res = self.inner.flush_data().await;
                let _ = resp.to_flush().send(res);
            },
            #[cfg(feature = "wal")]
            FileReqOp::Flush => {
                let md = unsafe { req.body.flush };
                let req = ManuallyDrop::into_inner(md);
                #[cfg(feature = "range-lock")]
                if self.inner.range_lock.is_locked() {
                    let fh = req.fh.clone();
                    let ctx = FileContext::reform_flush(req, resp);
                    // move flush op to cb queue
                    // so that flush op can run immediately after all inflight write op finished
                    let _ = fh.send_cb(ctx);
                    return;
                }
                let res = if self.inner.wal.is_none() {
                    let res = self.inner.flush().await;
                    if res.is_err() {
                        warn!("kick flush failed {:?}", res);
                    }
                    res
                } else {
                    let res = self.inner.kick_wal_protected_flush_reactor(req.fh.clone()).await;
                    if res.is_err() {
                        debug!("kick wal flush failed {:?}, requeue this request", res);
                        let fh = req.fh.clone();
                        let ctx = FileContext::reform_flush(req, resp);
                        // move flush op to cb queue
                        // so that flush op can run immediately after all inflight write op finished
                        let _ = fh.send_cb(ctx);
                        return;
                    }
                    res
                };
                let _ = resp.to_flush().send(res);
            },
            // POSIX-`fdatasync` flavoured flush, WAL on. Same
            // routing as `Flush` except we short-circuit when
            // only attrs are dirty: the WAL kick path doesn't
            // itself check, so we'd otherwise pay an unnecessary
            // segment build for a metadata-only change.
            #[cfg(feature = "wal")]
            FileReqOp::FlushData => {
                let md = unsafe { req.body.flush };
                let req = ManuallyDrop::into_inner(md);
                #[cfg(feature = "range-lock")]
                if self.inner.range_lock.is_locked() {
                    let fh = req.fh.clone();
                    let ctx = FileContext::reform_flush_data(req, resp);
                    let _ = fh.send_cb(ctx);
                    return;
                }
                // Short-circuit: only attr is dirty (or nothing
                // is dirty) — fdatasync is allowed to skip.
                if self.inner.dirty_block_count() == 0 && !self.inner.is_bmap_dirty() {
                    let cno = self.inner.in_memory_last_ondisk_cno();
                    let _ = resp.to_flush().send(Ok(cno));
                    return;
                }
                // Otherwise fdatasync collapses to fsync — we
                // need to write a segment for the dirty data /
                // bmap, which carries the inode along anyway.
                let res = if self.inner.wal.is_none() {
                    self.inner.flush().await
                } else {
                    let res = self.inner.kick_wal_protected_flush_reactor(req.fh.clone()).await;
                    if res.is_err() {
                        debug!("kick wal flush_data failed {:?}, requeue this request", res);
                        let fh = req.fh.clone();
                        let ctx = FileContext::reform_flush_data(req, resp);
                        let _ = fh.send_cb(ctx);
                        return;
                    }
                    res
                };
                let _ = resp.to_flush().send(res);
            },
            #[cfg(feature = "wal")]
            FileReqOp::WalFlush => {
                let md = unsafe { req.body.wal_flush };
                let req = ManuallyDrop::into_inner(md);
                #[cfg(feature = "range-lock")]
                if self.inner.range_lock.is_locked() {
                    let fh = req.fh.clone();
                    let ctx = FileContext::reform_wal_flush(req, resp);
                    // move flush op to cb queue
                    // so that flush op can run immediately after all inflight write op finished
                    let _ = fh.send_cb(ctx);
                    return;
                }
                let res = self.inner.kick_wal_protected_flush_reactor(req.fh).await;
                if res.is_err() {
                    debug!("kick wal flush failed {:?}, ignore this flush request", res);
                }
                let _ = resp.to_wal_flush();
            },
            #[cfg(feature = "wal")]
            FileReqOp::WalFlushDone => {
                let md = unsafe { req.body.wal_flush_done };
                let req = ManuallyDrop::into_inner(md);
                let lock = req.lock;
                let segid = req.segid;
                let od_state = req.od_state;
                let bmap_cache_limit = req.bmap_cache_limit;
                self.inner.wal_flush_done(lock, segid, od_state, bmap_cache_limit).await;
                info!("wal flush done, segid: {}", segid);
                let _ = resp.to_wal_flush_done();
            },
            #[cfg(feature = "wal")]
            FileReqOp::WalFlushRecovery => {
                let md = unsafe { req.body.wal_flush_recovery };
                let req = ManuallyDrop::into_inner(md);
                let lock = req.lock;
                let res = self.inner.wal_flush_recovery(lock).await;
                if res.is_err() {
                    panic!("wal flush recovery failed {:?}", res);
                }
                let _ = resp.to_wal_flush_recovery();
            },
            FileReqOp::Release => {
                let md = unsafe { req.body.release };
                let req = ManuallyDrop::into_inner(md);
                let res = self.inner.release().await;
                match res {
                    Ok(_) => {
                        let _ = resp.to_release().send(res);
                    },
                    Err(ref e) => {
                        if e.kind() != ErrorKind::ResourceBusy {
                            let _ = resp.to_release().send(res);
                        } else {
                            let fh = req.fh.clone();
                            let ctx = FileContext::reform_release(req, resp);
                            let _ = fh.send_highprio(ctx);
                        }
                    },
                }
            },
            FileReqOp::LastCno => {
                let md = unsafe { req.body.last_cno };
                let _ = ManuallyDrop::into_inner(md);
                let res = self.inner.last_cno();
                let _ = resp.to_last_cno().send(res);
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::mem::ManuallyDrop;
use std::sync::Arc;
use bytes::Bytes;
    use tokio::task::LocalSet;
    use hyperfile_reactor::Reactor;

    // Minimal Task impl to obtain a real ChannelGroup for tests.
    struct DummyTask;
    impl Task<FileContext<'static>> for DummyTask {
        async fn handle(&mut self, _ctx: FileContext<'static>) {}
    }

    async fn make_handler() -> ChannelGroup<FileContext<'static>> {
        // Tests construct FileReq structs that carry an `fh:
        // ChannelGroup` field, but never actually send through it.
        // Spawn a throwaway reactor + dummy task to obtain a live
        // ChannelGroup, then leak the reactor so the senders inside
        // the ChannelGroup stay valid for the lifetime of the test.
        let reactor = Reactor::<FileContext<'static>, DummyTask>::new_current()
            .expect("reactor");
        let (builder, finish) = build_channel_group();
        let handler = reactor.spawn_async(DummyTask, builder).await
            .expect("spawn");
        std::mem::forget(reactor);
        finish(handler)
    }

    // ==================== oneshot patterns ====================

    #[test]
    fn getattr_construct_take_respond() {
        let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
        let local = LocalSet::new();
        local.block_on(&rt, async {
            let (ctx, rx) = FileContext::new_getattr();
            let (req, resp) = ctx.take();
            assert!(matches!(req.op, FileReqOp::GetAttr));
            let _body = ManuallyDrop::into_inner(unsafe { req.body.getattr });
            let stat: libc::stat = unsafe { std::mem::zeroed() };
            resp.to_getattr().send(Ok(stat)).unwrap();
            assert!(rx.await.unwrap().is_ok());
        });
    }

    #[test]
    fn setattr_construct_take_respond() {
        let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
        let local = LocalSet::new();
        local.block_on(&rt, async {
            let stat_in: libc::stat = unsafe { std::mem::zeroed() };
            let (ctx, rx) = FileContext::new_setattr(stat_in);
            let (req, resp) = ctx.take();
            assert!(matches!(req.op, FileReqOp::SetAttr));
            let body = ManuallyDrop::into_inner(unsafe { req.body.setattr });
            assert_eq!(body.stat.st_size, 0);
            let stat_out: libc::stat = unsafe { std::mem::zeroed() };
            resp.to_setattr().send(Ok(stat_out)).unwrap();
            assert!(rx.await.unwrap().is_ok());
        });
    }

    #[test]
    fn trunc_construct_take_respond() {
        let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
        let local = LocalSet::new();
        local.block_on(&rt, async {
            let (ctx, rx) = FileContext::new_trunc(4096);
            let (req, resp) = ctx.take();
            assert!(matches!(req.op, FileReqOp::Trunc));
            let body = ManuallyDrop::into_inner(unsafe { req.body.trunc });
            assert_eq!(body.offset, 4096);
            resp.to_trunc().send(Ok(())).unwrap();
            assert!(rx.await.unwrap().is_ok());
        });
    }

    #[test]
    fn last_cno_construct_take_respond() {
        let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
        let local = LocalSet::new();
        local.block_on(&rt, async {
            let (ctx, rx) = FileContext::new_last_cno();
            let (req, resp) = ctx.take();
            assert!(matches!(req.op, FileReqOp::LastCno));
            let _body = ManuallyDrop::into_inner(unsafe { req.body.last_cno });
            resp.to_last_cno().send(42).unwrap();
            assert_eq!(rx.await.unwrap(), 42);
        });
    }

    // ==================== mpsc patterns ====================

    #[test]
    fn read_construct_take_respond() {
        let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
        let local = LocalSet::new();
        local.block_on(&rt, async {
            let fh = make_handler().await;
            let mut buf = vec![0u8; 100];
            let buf_ref = unsafe { std::slice::from_raw_parts_mut(buf.as_mut_ptr(), buf.len()) };
            let (ctx, _tx, mut rx) = FileContext::new_read(buf_ref, 0, fh);
            let (req, resp) = ctx.take();
            assert!(matches!(req.op, FileReqOp::Read));
            let body = ManuallyDrop::into_inner(unsafe { req.body.read });
            assert_eq!(body.buf.len(), 100);
            assert_eq!(body.offset, 0);
            resp.to_read().send(Ok(100)).await.unwrap();
            assert_eq!(rx.recv().await.unwrap().unwrap(), 100);
        });
    }

    #[test]
    fn write_construct_take_respond() {
        let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
        let local = LocalSet::new();
        local.block_on(&rt, async {
            let fh = make_handler().await;
            let data = [0xABu8; 200];
            let data_ref = unsafe { std::slice::from_raw_parts(data.as_ptr(), data.len()) };
            let (ctx, _tx, mut rx) = FileContext::new_write(data_ref, 512, fh);
            let (req, resp) = ctx.take();
            assert!(matches!(req.op, FileReqOp::Write));
            let body = ManuallyDrop::into_inner(unsafe { req.body.write });
            assert_eq!(body.buf.len(), 200);
            assert_eq!(body.buf[0], 0xAB);
            assert_eq!(body.offset, 512);
            resp.to_write().send(Ok(200)).await.unwrap();
            assert_eq!(rx.recv().await.unwrap().unwrap(), 200);
        });
    }

    #[test]
    fn write_zero_construct_take_respond() {
        let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
        let local = LocalSet::new();
        local.block_on(&rt, async {
            let fh = make_handler().await;
            let (ctx, _tx, mut rx) = FileContext::new_write_zero(1024, 4096, fh);
            let (req, resp) = ctx.take();
            assert!(matches!(req.op, FileReqOp::WriteZero));
            let body = ManuallyDrop::into_inner(unsafe { req.body.write_zero });
            assert_eq!(body.offset, 1024);
            assert_eq!(body.len, 4096);
            resp.to_write_zero().send(Ok(4096)).await.unwrap();
            assert_eq!(rx.recv().await.unwrap().unwrap(), 4096);
        });
    }

    // ==================== flush / release (oneshot + TaskHandler) ====================

    #[test]
    fn flush_construct_take_respond() {
        let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
        let local = LocalSet::new();
        local.block_on(&rt, async {
            let fh = make_handler().await;
            let (ctx, rx) = FileContext::new_flush(fh);
            let (req, resp) = ctx.take();
            assert!(matches!(req.op, FileReqOp::Flush));
            let _body = ManuallyDrop::into_inner(unsafe { req.body.flush });
            resp.to_flush().send(Ok(7)).unwrap();
            assert_eq!(rx.await.unwrap().unwrap(), 7);
        });
    }

    #[test]
    fn release_construct_take_respond() {
        let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
        let local = LocalSet::new();
        local.block_on(&rt, async {
            let fh = make_handler().await;
            let (ctx, rx) = FileContext::new_release(fh);
            let (req, resp) = ctx.take();
            assert!(matches!(req.op, FileReqOp::Release));
            let _body = ManuallyDrop::into_inner(unsafe { req.body.release });
            resp.to_release().send(Ok(99)).unwrap();
            assert_eq!(rx.await.unwrap().unwrap(), 99);
        });
    }

    // ==================== batch patterns ====================

    #[test]
    fn write_aligned_batch_construct_take_respond() {
        let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
        let local = LocalSet::new();
        local.block_on(&rt, async {
            let blocks = vec![
                AlignedDataBlockWrapper::new(0, 4096, false),
                AlignedDataBlockWrapper::new(1, 4096, true),
            ];
            let (ctx, mut rx) = FileContext::new_write_aligned_batch(blocks);
            let (req, resp) = ctx.take();
            assert!(matches!(req.op, FileReqOp::WriteAlignedBatch));
            let body = ManuallyDrop::into_inner(unsafe { req.body.write_aligned_batch });
            assert_eq!(body.data_blocks.len(), 2);
            assert!(!body.data_blocks[0].is_zero());
            assert!(body.data_blocks[1].is_zero());
            resp.to_write().send(Ok(8192)).await.unwrap();
            assert_eq!(rx.recv().await.unwrap().unwrap(), 8192);
        });
    }

    #[test]
    fn write_batch_construct_take_respond() {
        let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
        let local = LocalSet::new();
        local.block_on(&rt, async {
            let blocks = vec![BatchDataBlockWrapper::new(0, 4096, false)];
            let (ctx, mut rx) = FileContext::new_write_batch(blocks);
            let (req, resp) = ctx.take();
            assert!(matches!(req.op, FileReqOp::WriteBatch));
            let body = ManuallyDrop::into_inner(unsafe { req.body.write_batch });
            assert_eq!(body.data_blocks.len(), 1);
            resp.to_write().send(Ok(4096)).await.unwrap();
            assert_eq!(rx.recv().await.unwrap().unwrap(), 4096);
        });
    }

    // ==================== clone_write_resp / clone_write_zero_resp ====================

    #[test]
    fn clone_write_resp_sends_on_cloned_channel() {
        let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
        let local = LocalSet::new();
        local.block_on(&rt, async {
            let fh = make_handler().await;
            static BUF: [u8; 10] = [0u8; 10];
            let (ctx, _tx, mut rx) = FileContext::new_write(&BUF, 0, fh);
            let (_req, resp) = ctx.take();
            let cloned = resp.clone_write_resp();
            cloned.send(Ok(10)).await.unwrap();
            assert_eq!(rx.recv().await.unwrap().unwrap(), 10);
        });
    }

    #[test]
    fn clone_write_zero_resp_sends_on_cloned_channel() {
        let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
        let local = LocalSet::new();
        local.block_on(&rt, async {
            let fh = make_handler().await;
            let (ctx, _tx, mut rx) = FileContext::new_write_zero(0, 100, fh);
            let (_req, resp) = ctx.take();
            let cloned = resp.clone_write_zero_resp();
            cloned.send(Ok(100)).await.unwrap();
            assert_eq!(rx.recv().await.unwrap().unwrap(), 100);
        });
    }

    // ==================== reform_* constructors ====================

    #[test]
    fn reform_read_preserves_data() {
        let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
        let local = LocalSet::new();
        local.block_on(&rt, async {
            let fh = make_handler().await;
            let mut buf = vec![0u8; 50];
            let buf_ref = unsafe { std::slice::from_raw_parts_mut(buf.as_mut_ptr(), buf.len()) };
            let (ctx, _tx, mut rx) = FileContext::new_read(buf_ref, 10, fh);
            let (req, resp) = ctx.take();
            let body = ManuallyDrop::into_inner(unsafe { req.body.read });
            let ctx2 = FileContext::reform_read(body, resp);
            let (req2, resp2) = ctx2.take();
            assert!(matches!(req2.op, FileReqOp::Read));
            let body2 = ManuallyDrop::into_inner(unsafe { req2.body.read });
            assert_eq!(body2.offset, 10);
            assert_eq!(body2.buf.len(), 50);
            resp2.to_read().send(Ok(50)).await.unwrap();
            assert_eq!(rx.recv().await.unwrap().unwrap(), 50);
        });
    }

    #[test]
    fn reform_write_preserves_data() {
        let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
        let local = LocalSet::new();
        local.block_on(&rt, async {
            let fh = make_handler().await;
            let data = [1u8; 30];
            let data_ref = unsafe { std::slice::from_raw_parts(data.as_ptr(), data.len()) };
            let (ctx, _tx, mut rx) = FileContext::new_write(data_ref, 20, fh);
            let (req, resp) = ctx.take();
            let body = ManuallyDrop::into_inner(unsafe { req.body.write });
            let ctx2 = FileContext::reform_write(body, resp);
            let (req2, resp2) = ctx2.take();
            assert!(matches!(req2.op, FileReqOp::Write));
            let body2 = ManuallyDrop::into_inner(unsafe { req2.body.write });
            assert_eq!(body2.offset, 20);
            assert_eq!(body2.buf[0], 1);
            resp2.to_write().send(Ok(30)).await.unwrap();
            assert_eq!(rx.recv().await.unwrap().unwrap(), 30);
        });
    }

    #[test]
    fn reform_write_zero_preserves_data() {
        let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
        let local = LocalSet::new();
        local.block_on(&rt, async {
            let fh = make_handler().await;
            let (ctx, _tx, mut rx) = FileContext::new_write_zero(100, 200, fh);
            let (req, resp) = ctx.take();
            let body = ManuallyDrop::into_inner(unsafe { req.body.write_zero });
            let ctx2 = FileContext::reform_write_zero(body, resp);
            let (req2, resp2) = ctx2.take();
            assert!(matches!(req2.op, FileReqOp::WriteZero));
            let body2 = ManuallyDrop::into_inner(unsafe { req2.body.write_zero });
            assert_eq!(body2.offset, 100);
            assert_eq!(body2.len, 200);
            resp2.to_write_zero().send(Ok(200)).await.unwrap();
            assert_eq!(rx.recv().await.unwrap().unwrap(), 200);
        });
    }

    #[test]
    fn reform_flush_preserves_channel() {
        let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
        let local = LocalSet::new();
        local.block_on(&rt, async {
            let fh = make_handler().await;
            let (ctx, rx) = FileContext::new_flush(fh);
            let (req, resp) = ctx.take();
            let body = ManuallyDrop::into_inner(unsafe { req.body.flush });
            let ctx2 = FileContext::reform_flush(body, resp);
            let (req2, resp2) = ctx2.take();
            assert!(matches!(req2.op, FileReqOp::Flush));
            let _body2 = ManuallyDrop::into_inner(unsafe { req2.body.flush });
            resp2.to_flush().send(Ok(5)).unwrap();
            assert_eq!(rx.await.unwrap().unwrap(), 5);
        });
    }

    #[test]
    fn reform_release_preserves_channel() {
        let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
        let local = LocalSet::new();
        local.block_on(&rt, async {
            let fh = make_handler().await;
            let (ctx, rx) = FileContext::new_release(fh);
            let (req, resp) = ctx.take();
            let body = ManuallyDrop::into_inner(unsafe { req.body.release });
            let ctx2 = FileContext::reform_release(body, resp);
            let (req2, resp2) = ctx2.take();
            assert!(matches!(req2.op, FileReqOp::Release));
            let _body2 = ManuallyDrop::into_inner(unsafe { req2.body.release });
            resp2.to_release().send(Ok(11)).unwrap();
            assert_eq!(rx.await.unwrap().unwrap(), 11);
        });
    }

    // ==================== absorb_* constructors ====================

    #[test]
    fn write_absorb_sets_correct_op() {
        let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
        let local = LocalSet::new();
        local.block_on(&rt, async {
            let fh = make_handler().await;
            static BUF: [u8; 10] = [0u8; 10];
            let (ctx, _tx, mut rx) = FileContext::new_write(&BUF, 0, fh);
            let (req, resp) = ctx.take();
            let body = ManuallyDrop::into_inner(unsafe { req.body.write });
            let ctx2 = FileContext::write_absorb(body, resp);
            let (req2, resp2) = ctx2.take();
            assert!(matches!(req2.op, FileReqOp::WriteAbsorb));
            let _body2 = ManuallyDrop::into_inner(unsafe { req2.body.write });
            resp2.to_write().send(Ok(10)).await.unwrap();
            assert_eq!(rx.recv().await.unwrap().unwrap(), 10);
        });
    }

    #[test]
    fn write_absorb_bh_sets_correct_op() {
        let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
        let local = LocalSet::new();
        local.block_on(&rt, async {
            let fh = make_handler().await;
            static BUF: [u8; 10] = [0u8; 10];
            let (ctx, _tx, _rx) = FileContext::new_write(&BUF, 0, fh);
            let (req, resp) = ctx.take();
            let body = ManuallyDrop::into_inner(unsafe { req.body.write });
            let ctx2 = FileContext::write_absorb_bh(body, resp);
            let (req2, _resp2) = ctx2.take();
            assert!(matches!(req2.op, FileReqOp::WriteAbsorbBh));
        });
    }

    #[test]
    fn write_zero_absorb_sets_correct_op() {
        let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
        let local = LocalSet::new();
        local.block_on(&rt, async {
            let fh = make_handler().await;
            let (ctx, _tx, mut rx) = FileContext::new_write_zero(0, 100, fh);
            let (req, resp) = ctx.take();
            let body = ManuallyDrop::into_inner(unsafe { req.body.write_zero });
            let ctx2 = FileContext::write_zero_absorb(body, resp);
            let (req2, resp2) = ctx2.take();
            assert!(matches!(req2.op, FileReqOp::WriteZeroAbsorb));
            let _body2 = ManuallyDrop::into_inner(unsafe { req2.body.write_zero });
            resp2.to_write_zero().send(Ok(100)).await.unwrap();
            assert_eq!(rx.recv().await.unwrap().unwrap(), 100);
        });
    }

    #[test]
    fn write_zero_absorb_bh_sets_correct_op() {
        let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
        let local = LocalSet::new();
        local.block_on(&rt, async {
            let fh = make_handler().await;
            let (ctx, _tx, _rx) = FileContext::new_write_zero(0, 100, fh);
            let (req, resp) = ctx.take();
            let body = ManuallyDrop::into_inner(unsafe { req.body.write_zero });
            let ctx2 = FileContext::write_zero_absorb_bh(body, resp);
            let (req2, _resp2) = ctx2.take();
            assert!(matches!(req2.op, FileReqOp::WriteZeroAbsorbBh));
        });
    }

    // ==================== error path ====================

    #[test]
    fn getattr_error_response() {
        let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
        let local = LocalSet::new();
        local.block_on(&rt, async {
            let (ctx, rx) = FileContext::new_getattr();
            let (_req, resp) = ctx.take();
            resp.to_getattr().send(Err(std::io::Error::new(ErrorKind::NotFound, "not found"))).unwrap();
            let result = rx.await.unwrap();
            assert!(result.is_err());
            assert_eq!(result.unwrap_err().kind(), ErrorKind::NotFound);
        });
    }

    #[test]
    fn write_error_response() {
        let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
        let local = LocalSet::new();
        local.block_on(&rt, async {
            let fh = make_handler().await;
            static BUF: [u8; 1] = [0u8; 1];
            let (ctx, _tx, mut rx) = FileContext::new_write(&BUF, 0, fh);
            let (_req, resp) = ctx.take();
            resp.to_write().send(Err(std::io::Error::new(ErrorKind::Other, "fail"))).await.unwrap();
            let result = rx.recv().await.unwrap();
            assert!(result.is_err());
        });
    }
}
