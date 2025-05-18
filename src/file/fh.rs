use std::io::Result;
use tokio::sync::oneshot;
use aws_sdk_s3::Client;
use reactor::{LocalSpawner, TaskHandler};
use crate::config::{HyperFileMetaConfig, HyperFileRuntimeConfig};
use crate::buffer::{AlignedDataBlockWrapper, BatchDataBlockWrapper};
use super::hyper::Hyper;
use super::flags::FileFlags;
use super::mode::FileMode;
use super::handler::FileContext;

#[derive(Clone)]
pub struct HyperFileHandler<'a> {
    inner: TaskHandler<FileContext<'a>>,
}

impl<'a: 'static> HyperFileHandler<'a> {
    pub async fn fh_create(spawner: &LocalSpawner<FileContext<'a>, Hyper<'a>>, client: &Client, uri: &str, flags: FileFlags, mode: FileMode) -> Result<Self>
    {
        let hyper = Hyper::fs_create(client, uri, flags, mode).await?;
        let (tx, rx) = oneshot::channel();
        spawner.spawn(hyper, tx);
        let fh = rx.await.expect("failed to get back file handler");
        Ok(Self { inner: fh })
    }

    pub async fn fh_create_opt(spawner: &LocalSpawner<FileContext<'a>, Hyper<'a>>, client: &Client, uri: &str, flags: FileFlags, mode: FileMode,
            meta_config: &HyperFileMetaConfig, runtime_config: &HyperFileRuntimeConfig) -> Result<Self>
    {
        let hyper = Hyper::fs_create_opt(client, uri, flags, mode, meta_config, runtime_config).await?;
        let (tx, rx) = oneshot::channel();
        spawner.spawn(hyper, tx);
        let fh = rx.await.expect("failed to get back file handler");
        Ok(Self { inner: fh })
    }

    pub async fn fh_open(spawner: &LocalSpawner<FileContext<'a>, Hyper<'a>>, client: &Client, uri: &str, flags: FileFlags) -> Result<Self>
    {
        let hyper = Hyper::fs_open(client, uri, flags).await?;
        let (tx, rx) = oneshot::channel();
        spawner.spawn(hyper, tx);
        let fh = rx.await.expect("failed to get back file handler");
        Ok(Self { inner: fh })
    }

    pub async fn fh_open_opt(spawner: &LocalSpawner<FileContext<'a>, Hyper<'a>>, client: &Client, uri: &str, flags: FileFlags,
            runtime_config: &HyperFileRuntimeConfig) -> Result<Self>
    {
        let hyper = Hyper::fs_open_opt(client, uri, flags, runtime_config).await?;
        let (tx, rx) = oneshot::channel();
        spawner.spawn(hyper, tx);
        let fh = rx.await.expect("failed to get back file handler");
        Ok(Self { inner: fh })
    }

    pub async fn fh_open_or_create_with_default_opt(spawner: &LocalSpawner<FileContext<'a>, Hyper<'a>>, client: &Client, uri: &str, flags: FileFlags, mode: FileMode) -> Result<Self>
    {
        let hyper = Hyper::fs_open_or_create_with_default_opt(client, uri, flags, mode).await?;
        let (tx, rx) = oneshot::channel();
        spawner.spawn(hyper, tx);
        let fh = rx.await.expect("failed to get back file handler");
        Ok(Self { inner: fh })
    }

    pub async fn fh_unlink(client: &Client, uri: &str) -> Result<()>
    {
        Hyper::fs_unlink(client, uri).await
    }

    pub async fn fh_release(&mut self) -> Result<u64>
    {
        let (ctx, rx) = FileContext::new_release();
        self.inner.send(ctx);
        rx.await.expect("task channel closed")
    }

    pub async fn fh_read(&mut self, off: usize, buf: &mut [u8]) -> Result<usize>
    {
        let b = unsafe {
            std::slice::from_raw_parts_mut(buf.as_ptr() as *mut u8, buf.len())
        };
        let (ctx, tx, mut rx) = FileContext::new_read(b, off);
        self.inner.send(ctx);
        let res = rx.recv().await.expect("task channel closed");
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
        self.inner.send(ctx);
        let res = rx.recv().await.expect("task channel closed");
        drop(tx);
        let _ = buf;
        res
    }

    pub async fn fh_write_zero(&mut self, off: usize, len: usize) -> Result<usize>
    {
        let (ctx, tx, mut rx) = FileContext::new_write_zero(off, len, self.inner.clone());
        self.inner.send(ctx);
        let res = rx.recv().await.expect("task channel closed");
        drop(tx);
        res
    }

    pub async fn fh_write_aligned_batch(&mut self, blocks: Vec<AlignedDataBlockWrapper>) -> Result<usize>
    {
        let (ctx, mut rx) = FileContext::new_write_aligned_batch(blocks);
        self.inner.send(ctx);
        rx.recv().await.expect("task channel closed")
    }

    pub async fn fh_write_batch(&mut self, blocks: Vec<BatchDataBlockWrapper>) -> Result<usize>
    {
        let (ctx, mut rx) = FileContext::new_write_batch(blocks);
        self.inner.send(ctx);
        rx.recv().await.expect("task channel closed")
    }

    pub async fn fh_flush(&mut self) -> Result<u64>
    {
        let (ctx, rx) = FileContext::new_flush();
        self.inner.send(ctx);
        rx.await.expect("task channel closed")
    }

    pub async fn fh_truncate(&mut self, offset: usize) -> Result<()>
    {
        let (ctx, rx) = FileContext::new_trunc(offset);
        self.inner.send(ctx);
        rx.await.expect("task channel closed")
    }

    pub async fn fh_getattr(&self) -> Result<libc::stat>
    {
        let (ctx, rx) = FileContext::new_getattr();
        self.inner.send(ctx);
        rx.await.expect("task channel closed")
    }

    pub async fn fh_last_cno(&self) -> u64
    {
        let (ctx, rx) = FileContext::new_last_cno();
        self.inner.send(ctx);
        rx.await.expect("task channel closed")
    }
}
