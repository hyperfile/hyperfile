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
    pub segid: SegmentId,
    pub key: String,
    pub offset: usize,
    pub len: usize,
    pub is_zero: bool,
}

pub trait WalReadWrite {
    // write
    fn write(&mut self, segid: SegmentId, offset: usize, buf: &[u8]) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'static>>;
    fn write_zero(&mut self, segid: SegmentId, offset: usize, len: usize) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'static>>;
    // read
    fn read(&self, seq: usize, segid: SegmentId, offset: usize, len: usize) -> Pin<Box<dyn Future<Output = Result<Vec<u8>>> + Send + '_>>;
    // list
    fn list_segments(&self) -> Pin<Box<dyn Future<Output = Result<Vec<SegmentId>>> + Send + '_>>;
    fn list_chunks(&self, segid: SegmentId) -> Pin<Box<dyn Future<Output = Result<BTreeMap<usize, WalChunkDesc>>> + Send + '_>>;
    /// Delete every WAL object for the given segid from backing
    /// storage.
    ///
    /// Intended to be called after a successful flush has made the
    /// WAL chunks for `segid` redundant. Callers typically run this
    /// via `tokio::spawn` as a low-priority fire-and-forget cleanup:
    /// the WAL is safe to keep around (recovery filters by
    /// `last_ondisk_cno`), so a lost delete leaves storage slightly
    /// bloated but does not affect correctness. The returned future
    /// is `'static + Send` so callers can detach it freely.
    fn delete_segment(&self, segid: SegmentId) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'static>>;
}
