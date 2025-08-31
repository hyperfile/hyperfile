use std::pin::Pin;
use std::io::Result;
use std::collections::BTreeMap;
use crate::SegmentId;

pub mod config;
pub(crate) mod s3;

// ondisk chunk desc
#[derive(Debug)]
pub struct WalChunkDesc {
    pub seq: usize,
    pub key: String,
    pub offset: usize,
    pub len: usize,
    pub is_zero: bool,
}

pub trait WalReadWrite {
    // write
    fn write(&mut self, segid: SegmentId, offset: usize, buf: &[u8]) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'static>>;
    fn write_zero(&mut self, segid: SegmentId, offset: usize, len: usize) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'static>>;
    // reader
    fn list_segments(&self) -> Pin<Box<dyn Future<Output = Result<Vec<SegmentId>>> + Send + '_>>;
    fn list_chunks(&self, segid: SegmentId) -> Pin<Box<dyn Future<Output = Result<BTreeMap<usize, WalChunkDesc>>> + Send + '_>>;
}
