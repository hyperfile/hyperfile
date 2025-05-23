use std::io::Result;
use crate::SegmentId;
use crate::segment;
use crate::config::HyperFileMetaConfig;
use crate::staging::config::StagingConfig;
use crate::inode::{OnDiskState, FlushInodeFlag};

pub trait StagingIntercept<T>: Send + Sync {
    fn after_flush_inode(&self, staging: &T, payload: &[u8], flag: FlushInodeFlag) -> std::pin::Pin<Box<dyn Future<Output = Result<()>> + '_ + Send>>;
    fn after_remove_inode(&self, staging: &T) -> std::pin::Pin<Box<dyn Future<Output = Result<()>> + '_ + Send>>;
}

pub trait Staging<T, L> {
    fn load_inode(&self, buf: &mut [u8]) -> impl Future<Output = Result<Option<OnDiskState>>>;
    fn load_inode_from_segment(&self, buf: &mut [u8], segid: u64) -> impl Future<Output = Result<Option<OnDiskState>>>;
    fn load_segment_timestamp(&self, segid: u64) -> impl Future<Output = Result<(i64, i64)>>;
    fn flush_inode(&self, buf: &[u8], inode_state: &Option<OnDiskState>, flag: FlushInodeFlag) -> impl Future<Output = Result<Option<OnDiskState>>> + Send;
    fn remove_inode(&self, inode_state: &Option<OnDiskState>) -> impl Future<Output = Result<()>>;
    fn load_data_block(&self, segid: SegmentId, staging_off: usize, offset: usize, block_size: usize, buf: &mut [u8]) -> impl Future<Output = Result<()>> + Send;
    fn new_segwr(&self, segid: SegmentId, hyper_file_config: &HyperFileMetaConfig) -> segment::Writer<T>;
    fn dir_filename(&self) -> (&str, &str);
    fn root_path(&self) -> &str;
    fn config(&self) -> &StagingConfig;
    fn unlink(&self) -> impl Future<Output = Result<()>>;
    fn to_block_loader(&self) -> L;
    fn interceptor(&mut self, i: impl StagingIntercept<T> + 'static);
}

pub mod config;
pub mod s3;
#[cfg(feature="bench")]
pub mod bench;
