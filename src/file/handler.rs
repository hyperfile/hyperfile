//! impl request handler style IO process
use std::mem::ManuallyDrop;
use std::io::Result;
#[cfg(feature = "range-lock")]
use std::io::{Error, ErrorKind};
use tokio::sync::{mpsc, oneshot};
use reactor::{Task, TaskHandler};
use crate::SegmentId;
use crate::buffer::{DataBlock, AlignedDataBlockWrapper, BatchDataBlockWrapper};
use super::hyper::Hyper;
use super::HyperTrait;

pub type FileRespGetAttr = Result<libc::stat>;
pub type FileRespSetAttr = Result<libc::stat>;
pub type FileRespRead = Result<usize>;
pub type FileRespWrite = Result<usize>;
pub type FileRespWriteZero = Result<usize>;
pub type FileRespTrunc = Result<()>;
pub type FileRespFlush = Result<SegmentId>;
pub type FileRespRelease = Result<SegmentId>;
pub type FileRespLastCno = u64;

#[repr(C)]
pub union FileResp {
    getattr: ManuallyDrop<oneshot::Sender<FileRespGetAttr>>,
    setattr: ManuallyDrop<oneshot::Sender<FileRespSetAttr>>,
    read: ManuallyDrop<mpsc::Sender<FileRespRead>>,
    write: ManuallyDrop<mpsc::Sender<FileRespWrite>>,
    write_zero: ManuallyDrop<mpsc::Sender<FileRespWriteZero>>,
    write_aligned_batch: ManuallyDrop<mpsc::Sender<FileRespWrite>>,
    write_batch: ManuallyDrop<mpsc::Sender<FileRespWrite>>,
    trunc: ManuallyDrop<oneshot::Sender<FileRespTrunc>>,
    flush: ManuallyDrop<oneshot::Sender<FileRespFlush>>,
    release: ManuallyDrop<oneshot::Sender<FileRespRelease>>,
    last_cno: ManuallyDrop<oneshot::Sender<FileRespLastCno>>,
}

impl FileResp {
    pub fn to_getattr(self) -> oneshot::Sender<FileRespGetAttr> {
        ManuallyDrop::into_inner(unsafe { self.getattr })
    }

    pub fn to_setattr(self) -> oneshot::Sender<FileRespSetAttr> {
        ManuallyDrop::into_inner(unsafe { self.setattr })
    }

    pub fn to_read(self) -> mpsc::Sender<FileRespRead> {
        ManuallyDrop::into_inner(unsafe { self.read })
    }

    pub fn to_write(self) -> mpsc::Sender<FileRespWrite> {
        ManuallyDrop::into_inner(unsafe { self.write })
    }

    pub fn to_write_zero(self) -> mpsc::Sender<FileRespWriteZero> {
        ManuallyDrop::into_inner(unsafe { self.write_zero })
    }

    pub fn to_trunc(self) -> oneshot::Sender<FileRespTrunc> {
        ManuallyDrop::into_inner(unsafe { self.trunc })
    }

    pub fn to_flush(self) -> oneshot::Sender<FileRespFlush> {
        ManuallyDrop::into_inner(unsafe { self.flush })
    }

    pub fn to_release(self) -> oneshot::Sender<FileRespRelease> {
        ManuallyDrop::into_inner(unsafe { self.release })
    }

    pub fn to_last_cno(self) -> oneshot::Sender<FileRespLastCno> {
        ManuallyDrop::into_inner(unsafe { self.last_cno })
    }
}

// define request params
pub struct FileReqRead<'a> {
    pub buf: &'a mut [u8],
    pub offset: usize,
    pub fh: TaskHandler<FileContext<'a>>,
}

pub struct FileReqWrite<'a> {
    pub buf: &'a [u8],
    pub offset: usize,
    pub fetched: Vec<DataBlock>,
    pub fh: TaskHandler<FileContext<'a>>,
}

pub struct FileReqWriteZero<'a> {
    pub offset: usize,
    pub len: usize,
    pub fetched: Vec<DataBlock>,
    pub fh: TaskHandler<FileContext<'a>>,
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

pub struct FileReqGetAttr {}

pub struct FileReqSetAttr {
    pub stat: libc::stat,
}

pub struct FileReqFlush {}

pub struct FileReqRelease {}

pub struct FileReqLastCno {}

pub enum FileReqOp {
    GetAttr,
    SetAttr,
    Read,
    Write,
    WriteAbsorb,
    WriteZero,
    WriteZeroAbsorb,
    WriteAlignedBatch,
    WriteBatch,
    Trunc,
    Flush,
    Release,
    LastCno,
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
    flush: ManuallyDrop<FileReqFlush>,
    release: ManuallyDrop<FileReqRelease>,
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
        let resp = FileResp {
            getattr: ManuallyDrop::new(tx),
        };
        (Self { req: Some(req), resp: Some(resp), }, rx)
    }

    pub fn new_setattr(stat: libc::stat) -> (Self, oneshot::Receiver<FileRespSetAttr>) {
        let (tx, rx) = oneshot::channel::<FileRespSetAttr>();
        let req = FileReq {
            op: FileReqOp::SetAttr,
            body: FileReqBody { setattr: ManuallyDrop::new(FileReqSetAttr { stat: stat }), },
        };
        let resp = FileResp {
            setattr: ManuallyDrop::new(tx),
        };
        (Self { req: Some(req), resp: Some(resp), }, rx)
    }

    // return both tx and rx, for mpsc channel we need to keep tx until we receved response
    pub fn new_read(buf: &'a mut [u8], offset: usize, fh: TaskHandler<FileContext<'a>>) -> (Self, mpsc::Sender<FileRespRead>, mpsc::Receiver<FileRespRead>) {
        let (tx, rx) = mpsc::channel::<FileRespRead>(1);
        let req = FileReq { 
            op: FileReqOp::Read,
            body: FileReqBody { read: ManuallyDrop::new(FileReqRead { buf: buf, offset: offset, fh: fh }), },
        };
        let resp = FileResp {
            read: ManuallyDrop::new(tx.clone()),
        };
        (Self { req: Some(req), resp: Some(resp), }, tx, rx)
    }

    // return both tx and rx, for mpsc channel we need to keep tx until we receved response
    pub fn new_write(buf: &'a [u8], offset: usize, fh: TaskHandler<FileContext<'a>>) -> (Self, mpsc::Sender<FileRespWrite>, mpsc::Receiver<FileRespWrite>) {
        let (tx, rx) = mpsc::channel::<FileRespWrite>(1);
        let req = FileReq { 
            op: FileReqOp::Write,
            body: FileReqBody { write: ManuallyDrop::new(FileReqWrite { buf: buf, offset: offset, fh: fh, fetched: Vec::new(), }), },
        };
        let resp = FileResp {
            write: ManuallyDrop::new(tx.clone()),
        };
        (Self { req: Some(req), resp: Some(resp), }, tx, rx)
    }

    // convert context from op Write to op WriteAbsorb
    pub fn write_absorb(req: FileReqWrite<'a>, resp: FileResp) -> Self {
        let new_req = FileReq {
            op: FileReqOp::WriteAbsorb,
            body: FileReqBody { write: ManuallyDrop::new(req), },
        };
        Self { req: Some(new_req), resp: Some(resp), }
    }

    // return both tx and rx, for mpsc channel we need to keep tx until we receved response
    pub fn new_write_zero(offset: usize, len: usize, fh: TaskHandler<FileContext<'a>>) -> (Self, mpsc::Sender<FileRespWrite>, mpsc::Receiver<FileRespWrite>) {
        let (tx, rx) = mpsc::channel::<FileRespWriteZero>(1);
        let req = FileReq {
            op: FileReqOp::WriteZero,
            body: FileReqBody { write_zero: ManuallyDrop::new(FileReqWriteZero { offset: offset, len: len, fh: fh, fetched: Vec::new(), }), },
        };
        let resp = FileResp {
            write_zero: ManuallyDrop::new(tx.clone()),
        };
        (Self { req: Some(req), resp: Some(resp), }, tx, rx)
    }

    // convert context from op Write to op WriteAbsorb
    pub fn write_zero_absorb(req: FileReqWriteZero<'a>, resp: FileResp) -> Self {
        let new_req = FileReq {
            op: FileReqOp::WriteZeroAbsorb,
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
        let resp = FileResp {
            write_aligned_batch: ManuallyDrop::new(tx),
        };
        (Self { req: Some(new_req), resp: Some(resp), }, rx)
    }

    pub fn new_write_batch(v: Vec<BatchDataBlockWrapper>) -> (Self, mpsc::Receiver<FileRespWrite>) {
        let (tx, rx) = mpsc::channel::<FileRespWrite>(1);
        let new_req = FileReq {
            op: FileReqOp::WriteBatch,
            body: FileReqBody { write_batch: ManuallyDrop::new(FileReqWriteBatch { data_blocks: v }), },
        };
        let resp = FileResp {
            write_batch: ManuallyDrop::new(tx),
        };
        (Self { req: Some(new_req), resp: Some(resp), }, rx)
    }

    pub fn new_trunc(offset: usize) -> (Self, oneshot::Receiver<FileRespTrunc>) {
        let (tx, rx) = oneshot::channel::<FileRespTrunc>();
        let req = FileReq { 
            op: FileReqOp::Trunc,
            body: FileReqBody { trunc: ManuallyDrop::new(FileReqTrunc { offset: offset }), },
        };
        let resp = FileResp {
            trunc: ManuallyDrop::new(tx),
        };
        (Self { req: Some(req), resp: Some(resp), }, rx)
    }

    pub fn new_flush() -> (Self, oneshot::Receiver<FileRespFlush>) {
        let (tx, rx) = oneshot::channel::<FileRespFlush>();
        let req = FileReq { 
            op: FileReqOp::Flush,
            body: FileReqBody { flush: ManuallyDrop::new(FileReqFlush {}), },
        };
        let resp = FileResp {
            flush: ManuallyDrop::new(tx),
        };
        (Self { req: Some(req), resp: Some(resp), }, rx)
    }

    pub fn new_release() -> (Self, oneshot::Receiver<FileRespRelease>) {
        let (tx, rx) = oneshot::channel::<FileRespRelease>();
        let req = FileReq { 
            op: FileReqOp::Release,
            body: FileReqBody { release: ManuallyDrop::new(FileReqRelease {}), },
        };
        let resp = FileResp {
            release: ManuallyDrop::new(tx),
        };
        (Self { req: Some(req), resp: Some(resp), }, rx)
    }

    pub fn new_last_cno() -> (Self, oneshot::Receiver<FileRespLastCno>) {
        let (tx, rx) = oneshot::channel::<FileRespLastCno>();
        let req = FileReq { 
            op: FileReqOp::LastCno,
            body: FileReqBody { last_cno: ManuallyDrop::new(FileReqLastCno {}), },
        };
        let resp = FileResp {
            last_cno: ManuallyDrop::new(tx),
        };
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
}

impl<'a: 'static> Task<FileContext<'a>> for Hyper<'a>
{
    // main loop
    async fn handler(&mut self, ctx: FileContext<'a>) {
        let (req, resp) = ctx.take();
        match req.op {
            FileReqOp::GetAttr => {
                let stat = self.inner.stat();
                let resp = resp.to_getattr();
                let _ = resp.send(Ok(stat));
            },
            FileReqOp::SetAttr => {
                let body = unsafe { req.body.setattr };
                let stat = body.stat;
                let res = self.inner.update_stat(&stat).await;
                let _ = resp.to_setattr().send(res);
            },
            FileReqOp::Read => {
                let md = unsafe { req.body.read };
                let req = ManuallyDrop::into_inner(md);
                #[cfg(feature = "range-lock")]
                let range = req.offset as u64..(req.offset + req.buf.len()) as u64;
                // prepare error response handler
                let _resp_read = resp.to_read();
                let resp_read = _resp_read.clone();
                let resp = FileResp {
                    read: ManuallyDrop::new(_resp_read),
                };
                let res = self.inner.spawn_read(req, resp).await;
                match res {
                    Ok(_) => {}
                    Err(ref e) => {
                        if e.kind() != ErrorKind::ResourceBusy {
                            #[cfg(feature = "range-lock")]
                            self.inner.range_lock.try_unlock(range);
                            let _ = resp_read.try_send(res);
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
                let resp = FileResp {
                    write: ManuallyDrop::new(_resp_write),
                };
                let res = self.inner.spawn_write(req, resp).await;
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
                let off = req.offset;
                let buf = req.buf;
                let mut fetched = Vec::new();
                fetched.append(&mut req.fetched);
                let res = self.inner.absorb_write(off, buf, fetched).await;
                #[cfg(feature = "range-lock")]
                let range = off as u64..(off + buf.len()) as u64;
                #[cfg(feature = "range-lock")]
                self.inner.range_lock.try_unlock(range);
                let _ = resp.to_write().try_send(res);
            },
            FileReqOp::WriteZero => {
                let md = unsafe { req.body.write_zero };
                let req = ManuallyDrop::into_inner(md);
                #[cfg(feature = "range-lock")]
                let range = req.offset as u64..(req.offset + req.len) as u64;
                // prepare error response handler
                let _resp_write = resp.to_write();
                let resp_write = _resp_write.clone();
                let resp = FileResp {
                    write: ManuallyDrop::new(_resp_write),
                };
                let res = self.inner.spawn_write_zero(req, resp).await;
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
                let off = req.offset;
                let len = req.len;
                let mut fetched = Vec::new();
                fetched.append(&mut req.fetched);
                let res = self.inner.absorb_write_zero(off, len, fetched).await;
                #[cfg(feature = "range-lock")]
                let range = off as u64..(off + len) as u64;
                #[cfg(feature = "range-lock")]
                self.inner.range_lock.try_unlock(range);
                let _ = resp.to_write().try_send(res);
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
                let body = unsafe { req.body.trunc };
                let offset = body.offset;
                let res = self.inner.truncate(offset).await;
                let _ = resp.to_trunc().send(res);
            },
            FileReqOp::Flush => {
                let res = self.inner.flush().await;
                let _ = resp.to_flush().send(res);
            },
            FileReqOp::Release => {
                let res = self.inner.release().await;
                let _ = resp.to_release().send(res);
            },
            FileReqOp::LastCno => {
                let res = self.inner.last_cno();
                let _ = resp.to_last_cno().send(res);
            },
        }
    }
}
