use std::io::Result;
use log::debug;
use aws_sdk_s3::Client;
use crate::staging::{Staging, config::StagingConfig, s3::S3Staging};
use crate::config::{HyperFileConfigBuilder, HyperFileMetaConfig, HyperFileRuntimeConfig};
use crate::buffer::{AlignedDataBlockWrapper, BatchDataBlockWrapper};
use super::HyperTrait;
use super::hyper::Hyper;
use super::flags::{HyperFileFlags, FileFlags};
use super::mode::{HyperFileMode, FileMode};

impl<'a: 'static> Hyper<'a> {
    pub async fn fs_create(client: &Client, uri: &str, flags: FileFlags, mode: FileMode) -> Result<Self>
    {
        debug!("fs_create - uri: {}, flags: {}", uri, flags);
        let staging_config = StagingConfig::new_s3_uri(uri, None);
        let file_config = HyperFileConfigBuilder::new()
                            .with_staging_config(&staging_config)
                            .build();
        let f = HyperFileFlags::from_flags(flags);
        let m = HyperFileMode::from_mode(mode);
        return Self::create(client.clone(), file_config, f, m).await;
    }

    pub async fn fs_create_opt(client: &Client, uri: &str, flags: FileFlags, mode: FileMode, meta_config: &HyperFileMetaConfig, runtime_config: &HyperFileRuntimeConfig) -> Result<Self>
    {
        debug!("fs_create_opt - uri: {}, flags: {}", uri, flags);
        let staging_config = StagingConfig::new_s3_uri(uri, None);
        let file_config = HyperFileConfigBuilder::new()
                            .with_meta_config(meta_config)
                            .with_staging_config(&staging_config)
                            .with_runtime_config(runtime_config)
                            .build();
        let f = HyperFileFlags::from_flags(flags);
        let m = HyperFileMode::from_mode(mode);
        return Self::create(client.clone(), file_config, f, m).await;
    }

    pub async fn fs_open(client: &Client, uri: &str, flags: FileFlags) -> Result<Self>
    {
        debug!("fs_open - uri: {}, flags: {}", uri, flags);
        let staging_config = StagingConfig::new_s3_uri(uri, None);
        let file_config = HyperFileConfigBuilder::new()
                            .with_staging_config(&staging_config)
                            .build();
        let f = HyperFileFlags::from_flags(flags);
        return Self::open(client.clone(), file_config, f).await;
    }

    pub async fn fs_open_opt(client: &Client, uri: &str, flags: FileFlags, runtime_config: &HyperFileRuntimeConfig) -> Result<Self>
    {
        debug!("fs_open_opt - uri: {}, flags: {}", uri, flags);
        let staging_config = StagingConfig::new_s3_uri(uri, None);
        let file_config = HyperFileConfigBuilder::new()
                            .with_staging_config(&staging_config)
                            .with_runtime_config(runtime_config)
                            .build();
        let f = HyperFileFlags::from_flags(flags);
        return Self::open(client.clone(), file_config, f).await;
    }

    pub async fn fs_open_or_create_with_default_opt(client: &Client, uri: &str, flags: FileFlags, mode: FileMode) -> Result<Self>
    {
        debug!("fs_open_or_create - uri: {}, flags: {}", uri, flags);
        let staging_config = StagingConfig::new_s3_uri(uri, None);
        let file_config = HyperFileConfigBuilder::new()
                            .with_staging_config(&staging_config)
                            .build();
        let f = HyperFileFlags::from_flags(flags);
        let m = HyperFileMode::from_mode(mode);
        return Self::do_open_or_create(client.clone(), file_config, f, m, true).await;
    }

    pub async fn fs_unlink(client: &Client, uri: &str) -> Result<()>
    {
        debug!("fs_unlink - uri: {}", uri);
        let staging_config = StagingConfig::new_s3_uri(uri, None);
        let staging = S3Staging::from(client, staging_config, HyperFileRuntimeConfig::default()).await?;
        staging.unlink().await
    }

    pub async fn fs_release(&mut self) -> Result<u64>
    {
        debug!("fs_release - ");
        self.inner.release().await
    }

    pub async fn fs_read(&mut self, off: usize, buf: &mut [u8]) -> Result<usize>
    {
        debug!("fs_read - offset: {}, size: {}", off, buf.len());
        self.inner.read(off, buf).await
    }

    pub async fn fs_write(&mut self, off: usize, buf: &[u8]) -> Result<usize>
    {
        debug!("fs_write - offset: {}, size: {}", off, buf.len());
        self.inner.write(off, buf).await
    }

    pub async fn fs_write_zero(&mut self, off: usize, len: usize) -> Result<usize>
    {
        debug!("fs_write_zero - offset: {}, len: {}", off, len);
        self.inner.write_zero(off, len).await
    }

    pub async fn fs_write_aligned_batch(&mut self, blocks: Vec<AlignedDataBlockWrapper>) -> Result<usize>
    {
        debug!("fs_write_aligned_batch - batch count: {}", blocks.len());
        self.inner.write_aligned_batch(blocks).await
    }

    pub async fn fs_write_batch(&mut self, blocks: Vec<BatchDataBlockWrapper>) -> Result<usize>
    {
        debug!("fs_write_batch - batch count: {}", blocks.len());
        self.inner.write_batch(blocks).await
    }

    pub async fn fs_flush(&mut self) -> Result<u64>
    {
        debug!("fs_flush - ");
        self.inner.flush().await
    }

    pub async fn fs_truncate(&mut self, offset: usize) -> Result<()>
    {
        debug!("fs_truncate - offset: {}", offset);
        self.inner.truncate(offset).await
    }

    pub fn fs_getattr(&self) -> Result<libc::stat>
    {
        debug!("fs_getattr -");
        Ok(self.inner.stat())
    }

    pub async fn fs_getattr_fast(client: &Client, uri: &str) -> Result<libc::stat>
    {
        debug!("fs_getattr_fast - uri: {}", uri);
        let staging_config = StagingConfig::new_s3_uri(uri, None);
        let file_config = HyperFileConfigBuilder::new()
                            .with_staging_config(&staging_config)
                            .build();
        return Self::stat_fast(client.clone(), file_config).await;
    }

    pub fn fs_last_cno(&self) -> u64
    {
        debug!("fs_last_cno -");
        self.inner.last_cno()
    }
}
