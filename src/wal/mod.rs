use std::pin::Pin;
use std::io::Result;
use crate::SegmentId;
use crate::buffer::BatchDataBlockWrapper;

pub mod config;
pub(crate) mod s3;

pub(crate) trait WalReadWrite {
    // write
    fn write(&mut self, segid: SegmentId, offset: usize, buf: &[u8]) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'static>>;
    fn write_zero(&mut self, segid: SegmentId, offset: usize, len: usize) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'static>>;
    // reader
    fn collect_segments(&self) -> Pin<Box<dyn Future<Output = Result<Vec<SegmentId>>> + Send + '_>>;
    #[allow(dead_code)]
    fn collect(&self, segid: SegmentId) -> Pin<Box<dyn Future<Output = Result<Vec<BatchDataBlockWrapper>>> + Send + '_>>;
}
