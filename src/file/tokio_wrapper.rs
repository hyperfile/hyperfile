use std::io::{Result, Error, ErrorKind};
use std::io::SeekFrom;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::sync::{oneshot, mpsc};
use tokio::io::ReadBuf;
use tokio::io::{AsyncRead, AsyncSeek, AsyncWrite};
use tokio::sync::mpsc::error::TryRecvError as MpscTryRecvError;
use tokio::sync::oneshot::error::TryRecvError as OneshotTryRecvError;
use aws_sdk_s3::Client;
use reactor::{LocalSpawner, TaskHandler};
use crate::config::{HyperFileMetaConfig, HyperFileRuntimeConfig};
use super::hyper::Hyper;
use super::flags::FileFlags;
use super::mode::FileMode;
use super::handler::{
    FileContext,
    FileRespGetAttr,
    FileRespRead,
    FileRespWrite,
    FileRespFlush,
    FileRespRelease
};

enum State {
    Idle(()),
    Busy(Operation),
}

enum Operation {
    Read((mpsc::Sender<FileRespRead>, mpsc::Receiver<FileRespRead>)),
    Write((mpsc::Sender<FileRespWrite>, mpsc::Receiver<FileRespWrite>)),
    WriteZero(()),
    Flush(oneshot::Receiver<FileRespFlush>),
    Release(oneshot::Receiver<FileRespRelease>),
    Seek(oneshot::Receiver<FileRespGetAttr>),
}

pub struct HyperFileTokio<'a> {
    inner: TaskHandler<FileContext<'a>>,
    #[allow(dead_code)]
    spawner: LocalSpawner<FileContext<'a>, Hyper<'a>>,
    state: State,
    pos: u64,
    seek_target: SeekFrom,
    read_buf: Pin<Box<Vec<u8>>>,
}

impl<'a: 'static> HyperFileTokio<'a> {
    pub async fn create(client: &Client, uri: &str, flags: FileFlags, mode: FileMode) -> Result<Self>
    {
        let hyper = Hyper::fs_create(client, uri, flags, mode).await?;
        let (tx, rx) = oneshot::channel();
        let spawner: LocalSpawner<FileContext<'_>, Hyper<'_>> = LocalSpawner::new_current();
        spawner.spawn(hyper, tx);
        let fh = rx.await.expect("failed to get back file handler");
        Ok(Self { inner: fh, spawner: spawner, state: State::Idle(()), pos: 0, seek_target: SeekFrom::Start(0), read_buf: Pin::new(Box::new(Vec::new())), })
    }
    pub async fn create_opt(client: &Client, uri: &str, flags: FileFlags, mode: FileMode,
            meta_config: &HyperFileMetaConfig, runtime_config: &HyperFileRuntimeConfig) -> Result<Self>
    {
        let hyper = Hyper::fs_create_opt(client, uri, flags, mode, meta_config, runtime_config).await?;
        let (tx, rx) = oneshot::channel();
        let spawner: LocalSpawner<FileContext<'_>, Hyper<'_>> = LocalSpawner::new_current();
        spawner.spawn(hyper, tx);
        let fh = rx.await.expect("failed to get back file handler");
        Ok(Self { inner: fh, spawner: spawner, state: State::Idle(()), pos: 0, seek_target: SeekFrom::Start(0), read_buf: Pin::new(Box::new(Vec::new())), })
    }

    pub async fn open(client: &Client, uri: &str, flags: FileFlags) -> Result<Self>
    {
        let hyper = Hyper::fs_open(client, uri, flags).await?;
        let (tx, rx) = oneshot::channel();
        let spawner: LocalSpawner<FileContext<'_>, Hyper<'_>> = LocalSpawner::new_current();
        spawner.spawn(hyper, tx);
        let fh = rx.await.expect("failed to get back file handler");
        Ok(Self { inner: fh, spawner: spawner, state: State::Idle(()), pos: 0, seek_target: SeekFrom::Start(0), read_buf: Pin::new(Box::new(Vec::new())), })
    }

    pub async fn open_opt(client: &Client, uri: &str, flags: FileFlags,
            runtime_config: &HyperFileRuntimeConfig) -> Result<Self>
    {
        let hyper = Hyper::fs_open_opt(client, uri, flags, runtime_config).await?;
        let (tx, rx) = oneshot::channel();
        let spawner: LocalSpawner<FileContext<'_>, Hyper<'_>> = LocalSpawner::new_current();
        spawner.spawn(hyper, tx);
        let fh = rx.await.expect("failed to get back file handler");
        Ok(Self { inner: fh, spawner: spawner, state: State::Idle(()), pos: 0, seek_target: SeekFrom::Start(0), read_buf: Pin::new(Box::new(Vec::new())), })
    }

    pub async fn open_or_create_with_default_opt(client: &Client, uri: &str, flags: FileFlags, mode: FileMode) -> Result<Self>
    {
        let hyper = Hyper::fs_open_or_create_with_default_opt(client, uri, flags, mode).await?;
        let (tx, rx) = oneshot::channel();
        let spawner: LocalSpawner<FileContext<'_>, Hyper<'_>> = LocalSpawner::new_current();
        spawner.spawn(hyper, tx);
        let fh = rx.await.expect("failed to get back file handler");
        Ok(Self { inner: fh, spawner: spawner, state: State::Idle(()), pos: 0, seek_target: SeekFrom::Start(0), read_buf: Pin::new(Box::new(Vec::new())), })
    }

    pub async fn set_len(&self, size: u64) -> Result<()> {
        let (ctx, rx) = FileContext::new_trunc(size as usize);
        self.inner.send(ctx);
        rx.await.expect("task channel closed")
    }

    pub async fn metadata(&self) -> Result<libc::stat> {
        let (ctx, rx) = FileContext::new_getattr();
        self.inner.send(ctx);
        rx.await.expect("task channel closed")
    }

    pub async fn last_cno(&self) -> u64 {
        let (ctx, rx) = FileContext::new_last_cno();
        self.inner.send(ctx);
        rx.await.expect("task channel closed")
    }

    pub async fn flush_ext(&self) -> Result<u64> {
        let (ctx, rx) = FileContext::new_flush();
        self.inner.send(ctx);
        rx.await.expect("task channel closed")
    }

    pub async fn write_zero(&mut self, len: usize) -> Result<usize> {
        loop {
            match self.state {
                State::Busy(_) => {
                    tokio::task::yield_now().await;
                },
                State::Idle(_) => {
                    let (ctx, tx, mut rx) = FileContext::new_write_zero(self.pos as usize, len, self.inner.clone());
                    self.inner.send(ctx);
                    self.state = State::Busy(Operation::WriteZero(()));
                    let res = rx.recv().await.expect("task channel closed");
                    drop(tx);
                    self.state = State::Idle(());
                    return res;
                },
            }
        }
    }
}

impl AsyncRead for HyperFileTokio<'_> {
    fn poll_read(self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &mut ReadBuf<'_>) -> Poll<Result<()>> {
        let me = self.get_mut();
        match me.state {
            State::Idle(_) => {
                // prepare data buf for read
                let mut data_buf = Vec::with_capacity(buf.remaining());
                data_buf.resize(buf.remaining(), 0);
                me.read_buf = Pin::new(Box::new(data_buf));
                let buf_mut_ref = unsafe {
                    std::slice::from_raw_parts_mut(me.read_buf.as_ptr() as *mut u8, me.read_buf.len())
                };
                let (ctx, tx, rx) = FileContext::new_read(buf_mut_ref, me.pos as usize);
                me.inner.send(ctx);
                me.state = State::Busy(Operation::Read((tx, rx)));
                cx.waker().wake_by_ref();
                Poll::Pending
            },
            State::Busy(ref mut op) => {
                match op {
                    Operation::Read((_, rx)) => {
                        match rx.try_recv() {
                            Ok(res) => {
                                if res.is_err() {
                                    me.state = State::Idle(());
                                    return Poll::Ready(Err(res.err().unwrap()));
                                }
                                let read_size = res.unwrap();
                                assert!(buf.capacity() == read_size);
                                buf.put_slice(&me.read_buf);
                                assert!(buf.remaining() == 0);
                                me.read_buf.truncate(0);
                                me.pos += read_size as u64;
                                me.state = State::Idle(());
                                return Poll::Ready(Ok(()));
                            },
                            Err(MpscTryRecvError::Disconnected) => {
                                me.state = State::Idle(());
                                return Poll::Ready(Err(Error::new(ErrorKind::Other, "handler channel disconnected")));
                            },
                            Err(MpscTryRecvError::Empty) => {},
                        }
                    },
                    _ => {},
                }
                cx.waker().wake_by_ref();
                Poll::Pending
            },
        }
    }
}

impl AsyncSeek for HyperFileTokio<'_> {
    fn start_seek(self: Pin<&mut Self>, pos: SeekFrom) -> Result<()> {
        let me = self.get_mut();
        match me.state {
            State::Busy(_) => Err(Error::new(ErrorKind::Other, "Other file operation is pending")),
            State::Idle(_) => {
                me.seek_target = pos;
                let (ctx, rx) = FileContext::new_getattr();
                me.inner.send(ctx);
                me.state = State::Busy(Operation::Seek(rx));
                Ok(())
            },
        } 
    }

    fn poll_complete(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<u64>> {
        let me = self.get_mut();
        match me.state {
            State::Idle(_) => Poll::Ready(Ok(me.pos)),
            State::Busy(ref mut op) => {
                match op {
                    Operation::Seek(rx) => {
                        match rx.try_recv() {
                            Ok(res) => {
                                if res.is_err() {
                                    me.state = State::Idle(());
                                    return Poll::Ready(Err(res.err().unwrap()));
                                }
                                let stat = res.unwrap();
                                // TODO: anything need to check for current file size
                                match me.seek_target {
                                    SeekFrom::Start(start) => { me.pos = start; },
                                    SeekFrom::Current(curr) => {
                                        let pos = me.pos as i64;
                                        me.pos = (pos + curr) as u64;
                                    },
                                    SeekFrom::End(end) => {
                                        let size = stat.st_size as i64;
                                        me.pos = (size + end) as u64;
                                    },
                                }
                                me.state = State::Idle(());
                                return Poll::Ready(Ok(me.pos));
                            },
                            Err(OneshotTryRecvError::Closed) => {
                                me.state = State::Idle(());
                                return Poll::Ready(Err(Error::new(ErrorKind::Other, "handler channel closed")));
                            },
                            Err(OneshotTryRecvError::Empty) => {},
                        }
                    },
                    _ => {},
                }
                cx.waker().wake_by_ref();
                Poll::Pending
            },
        }
    }
}

impl AsyncWrite for HyperFileTokio<'_> {
    fn poll_write(self: Pin<&mut Self>, cx: &mut Context<'_>, src: &[u8],) -> Poll<Result<usize>> {
        let me = self.get_mut();
        match me.state {
            State::Idle(_) => {
                let b = unsafe {
                    std::slice::from_raw_parts(src.as_ptr() as *const u8, src.len())
                };
                let (ctx, tx, rx) = FileContext::new_write(b, me.pos as usize, me.inner.clone());
                me.inner.send(ctx);
                me.state = State::Busy(Operation::Write((tx, rx)));
                cx.waker().wake_by_ref();
                Poll::Pending
            },
            State::Busy(ref mut op) => {
                match op {
                    Operation::Write((_, rx)) => {
                        match rx.try_recv() {
                            Ok(res) => {
                                if res.is_err() {
                                    me.state = State::Idle(());
                                    return Poll::Ready(Err(res.err().unwrap()));
                                }
                                let write_size = res.unwrap();
                                me.pos += write_size as u64;
                                me.state = State::Idle(());
                                return Poll::Ready(Ok(write_size));
                            },
                            Err(MpscTryRecvError::Disconnected) => {
                                me.state = State::Idle(());
                                return Poll::Ready(Err(Error::new(ErrorKind::Other, "handler channel disconnected")));
                            },
                            Err(MpscTryRecvError::Empty) => {},
                        }
                    },
                    _ => {},
                }
                cx.waker().wake_by_ref();
                Poll::Pending
            },
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<()>> {
        let me = self.get_mut();
        match me.state {
            State::Idle(_) => {
                let (ctx, rx) = FileContext::new_flush();
                me.inner.send(ctx);
                me.state = State::Busy(Operation::Flush(rx));
                cx.waker().wake_by_ref();
                Poll::Pending
            },
            State::Busy(ref mut op) => {
                match op {
                    Operation::Flush(rx) => {
                        match rx.try_recv() {
                            Ok(res) => {
                                if res.is_err() {
                                    me.state = State::Idle(());
                                    return Poll::Ready(Err(res.err().unwrap()));
                                }
                                me.state = State::Idle(());
                                return Poll::Ready(Ok(()));
                            },
                            Err(OneshotTryRecvError::Closed) => {
                                me.state = State::Idle(());
                                return Poll::Ready(Err(Error::new(ErrorKind::Other, "handler channel disconnected")));
                            },
                            Err(OneshotTryRecvError::Empty) => {},
                        }
                    },
                    _ => {},
                }
                cx.waker().wake_by_ref();
                Poll::Pending
            },
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<()>> {
        let me = self.get_mut();
        match me.state {
            State::Idle(_) => {
                let (ctx, rx) = FileContext::new_release();
                me.inner.send(ctx);
                me.state = State::Busy(Operation::Release(rx));
                cx.waker().wake_by_ref();
                Poll::Pending
            },
            State::Busy(ref mut op) => {
                match op {
                    Operation::Release(rx) => {
                        match rx.try_recv() {
                            Ok(res) => {
                                if res.is_err() {
                                    me.state = State::Idle(());
                                    return Poll::Ready(Err(res.err().unwrap()));
                                }
                                me.state = State::Idle(());
                                return Poll::Ready(Ok(()));
                            },
                            Err(OneshotTryRecvError::Closed) => {
                                me.state = State::Idle(());
                                return Poll::Ready(Err(Error::new(ErrorKind::Other, "handler channel disconnected")));
                            },
                            Err(OneshotTryRecvError::Empty) => {},
                        }
                    },
                    _ => {},
                }
                cx.waker().wake_by_ref();
                Poll::Pending
            },
        }
    }
}
