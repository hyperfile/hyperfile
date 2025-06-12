use std::io::{ErrorKind, Result};
use aws_sdk_s3::Client;
use crate::config::HyperFileConfig;
use crate::meta_loader::s3::S3BlockLoader;
use crate::staging::{Staging, StagingIntercept, config::StagingConfig, s3::S3Staging};
use super::file::HyperFile;
use super::flags::HyperFileFlags;
use super::mode::HyperFileMode;

pub struct Hyper<'a> {
    pub(crate) inner: HyperFile<'a, S3Staging, S3BlockLoader>
}

impl<'a: 'static> Hyper<'a> {
    pub(crate) async fn do_open_or_create(client: Client, file_config: HyperFileConfig, flags: HyperFileFlags, mode: HyperFileMode, create: bool) -> Result<Self>
    {
        match Self::open(client.clone(), file_config.clone(), flags.clone()).await {
            Ok(hyper) => {
                return Ok(hyper);
            },
            Err(e) => {
                if create && e.kind() == ErrorKind::NotFound {
                    return Self::create(client, file_config, flags, mode).await;
                }
                return Err(e);
            }
        }
    }

    pub async fn open(client: Client, file_config: HyperFileConfig, flags: HyperFileFlags) -> Result<Self>
    {
        let staging = S3Staging::from(&client, file_config.staging.clone(), file_config.runtime.clone()).await?;
        let loader = S3BlockLoader::new(&client, &staging.bucket, staging.root_path());
        let file = HyperFile::<S3Staging, S3BlockLoader>::open(staging, loader, file_config, flags).await?;
        Ok(Self {
            inner: file,
        })
    }

    pub async fn create(client: Client, file_config: HyperFileConfig, flags: HyperFileFlags, mode: HyperFileMode) -> Result<Self>
    {
        let staging = S3Staging::create(&client, file_config.staging.clone(), file_config.runtime.clone()).await?;
        let loader = S3BlockLoader::new(&client, &staging.bucket, staging.root_path());
        let file = HyperFile::<S3Staging, S3BlockLoader>::new(staging, loader, file_config, flags, mode).await?;
        Ok(Self {
            inner: file,
        })
    }

    pub async fn create_with_interceptor(client: Client, file_config: HyperFileConfig, flags: HyperFileFlags, mode: HyperFileMode, interceptor: impl StagingIntercept<S3Staging> + 'static) -> Result<Self>
    {
        let mut staging = S3Staging::create(&client, file_config.staging.clone(), file_config.runtime.clone()).await?;
        staging.interceptor(interceptor);
        let loader = S3BlockLoader::new(&client, &staging.bucket, staging.root_path());
        let file = HyperFile::<S3Staging, S3BlockLoader>::new(staging, loader, file_config, flags, mode).await?;
        Ok(Self {
            inner: file,
        })
    }

    pub async fn stat_fast(client: Client, file_config: HyperFileConfig) -> Result<libc::stat>
    {
        let staging = S3Staging::from(&client, file_config.staging.clone(), file_config.runtime.clone()).await?;
        HyperFile::stat_fast(staging).await
    }

    pub async fn update_stat_fast(client: Client, file_config: HyperFileConfig, stat: &libc::stat) -> Result<libc::stat>
    {
        let staging = S3Staging::from(&client, file_config.staging.clone(), file_config.runtime.clone()).await?;
        HyperFile::update_stat_fast(staging, stat).await
    }
}

/// expose helper fn
impl<'a: 'static> Hyper<'a> {
    pub fn staging_config(&self) -> &StagingConfig {
        self.inner.staging_config()
    }

    pub fn with_staging_interceptor(&mut self, i: impl StagingIntercept<S3Staging> + 'static) {
        self.inner.staging_interceptor(i)
    }
}
