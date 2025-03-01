use std::fmt;
use log::info;
use std::io::{Error, ErrorKind, Result};
use aws_sdk_s3::Client;
use crate::SegmentId;
use crate::staging::config::{StagingConfig, StagingType};
use crate::config::HyperFileConfig;
use crate::meta_loader::s3::S3BlockLoader;
use crate::staging::{Staging, StagingIntercept, s3::S3Staging};
use crate::segment::SegmentSum;
use super::{HyperTrait, file::HyperFile};
use super::flags::HyperFileFlags;

pub struct Hyper<'a> {
    pub(crate) inner: HyperFile<'a, S3Staging, S3BlockLoader>
}

impl<'a: 'static> Hyper<'a> {
    pub(crate) async fn do_open_or_create(client: Client, file_config: HyperFileConfig, flags: HyperFileFlags, create: bool) -> Result<Self>
    {
        match Self::open(client.clone(), file_config.clone(), flags.clone()).await {
            Ok(hyper) => {
                return Ok(hyper);
            },
            Err(e) => {
                if create && e.kind() == ErrorKind::NotFound {
                    return Self::create(client, file_config, flags).await;
                }
                return Err(e);
            }
        }
    }

    pub async fn open(client: Client, file_config: HyperFileConfig, flags: HyperFileFlags) -> Result<Self>
    {
        let staging = S3Staging::from(&client, file_config.staging.clone(), file_config.runtime.clone()).await?;
        let loader = S3BlockLoader::new(&client, &staging.bucket, staging.root_path());
        let file = HyperFile::<S3Staging, S3BlockLoader>::open(client, staging, loader, file_config, flags).await?;
        Ok(Self {
            inner: file,
        })
    }

    pub async fn create(client: Client, file_config: HyperFileConfig, flags: HyperFileFlags) -> Result<Self>
    {
        let staging = S3Staging::create(&client, file_config.staging.clone(), file_config.runtime.clone()).await?;
        let loader = S3BlockLoader::new(&client, &staging.bucket, staging.root_path());
        let file = HyperFile::<S3Staging, S3BlockLoader>::new(client, staging, loader, file_config, flags).await?;
        Ok(Self {
            inner: file,
        })
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
