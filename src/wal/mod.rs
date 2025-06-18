use std::pin::Pin;
use std::io::Result;
use crate::SegmentId;
use crate::buffer::BatchDataBlockWrapper;

pub mod config;
pub(crate) mod s3;

pub(crate) trait WalReadWrite {
    // write
    fn write<'a>(&'a mut self, segid: SegmentId, offset: usize, buf: &'a [u8]) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'a>>;
    fn write_zero(&mut self, segid: SegmentId, offset: usize, len: usize) -> Pin<Box<dyn Future<Output = Result<()>> + Send + '_>>;
    // reader
    fn collect(&self, segid: SegmentId) -> Pin<Box<dyn Future<Output = Result<Vec<BatchDataBlockWrapper>>> + Send + '_>>;
}
