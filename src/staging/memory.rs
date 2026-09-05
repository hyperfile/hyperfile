//! In-memory staging, for tests that want the real format without an
//! object store.
//!
//! Segments and the inode are held in a map shared by every clone of the
//! handle, so a `MemoryStaging` behaves like one file's storage: write it,
//! flush it, drop the file, open it again from the same handle and the
//! data is there. Nothing is written outside the process and no
//! credentials are needed.
//!
//! What this exercises is worth being precise about. The segment format,
//! the bmap, the block pointers, the cache tiers, flush and reopen all
//! run exactly as they do against S3, because the only thing replaced is
//! where the bytes land. What it cannot exercise is anything S3 decides:
//! conditional writes and the OCC that rides on them, multipart upload,
//! ranged-GET behaviour, and the error kinds a real service returns.
//!
//! ## Meta nodes
//!
//! Meta nodes are served by `btree_ondisk`'s `MemoryBlockLoader`,
//! which is a flat map from key to bytes. That
//! fits without any translation: a `BlockPtr` is already a single encoded
//! value, so it is used as the key directly and nothing has to decode it
//! on the way back out.
//!
//! Filling the map is this module's job, and it happens in
//! `SegmentReadWrite::done`, which is the first point where a whole
//! segment exists. The key each node needs is the one the bmap recorded,
//! `BlockPtrFormat::encode(segid, offset, seq)`, and all three parts are
//! recoverable from the segment header: nodes of one size follow the
//! aligned summary, so node `i` is at `aligned_ss + i * meta_block_size`
//! and carries sequence `i` — the flush numbers meta nodes before data
//! blocks. `encode` itself is called rather than reimplemented, so the
//! keys cannot drift from what the bmap holds.

use std::collections::BTreeMap;
use std::io::{Error, ErrorKind, Result};
use std::sync::{Arc, RwLock};

use btree_ondisk::MemoryBlockLoader;
use btree_ondisk::node::{BtreeNode, BTREE_NODE_LEVEL_DATA};
use log::debug;

use crate::{BlockPtr, BlockIndex, SegmentId};
use crate::config::{HyperFileMetaConfig, HyperFileRuntimeConfig};
use crate::file::ReadTiming;
use crate::inode::{FlushInodeFlag, OnDiskState};
use crate::ondisk::{SegmentHeader, InodeRaw};
use crate::meta_format::BlockPtrFormat;
use crate::segment::{self, SegmentSum, SegmentReadWrite};
use crate::segment_body::SegmentBody;
use crate::staging::config::StagingConfig;
use crate::staging::{Staging, StagingIntercept};

/// The bytes, shared by every clone of a handle.
#[derive(Default, Debug)]
struct MemoryStore {
    /// Segment objects, keyed the way staging names them: a checkpoint written
    /// as one object has `None`, and one streamed as several has a part each.
    segments: BTreeMap<SegmentId, Vec<u8>>,
    /// The inode object, absent until the first flush.
    inode: Option<Vec<u8>>,
    /// Stands in for an object's last-modified time.
    timestamps: BTreeMap<SegmentId, i64>,
    /// Meta node keys already handed to the loader. `MemoryBlockLoader`
    /// panics on a duplicate key, and a segment id can legitimately come
    /// round twice: a failed flush rolls back by reloading the persisted
    /// inode, which rewinds the sequence, so the retry reuses the id.
    meta_keys: std::collections::HashSet<BlockPtr>,
}

impl MemoryStore {
    /// Read `buf.len()` bytes at `off` of a segment, the way a ranged GET
    /// would. Out of range is `InvalidData` rather than a short read,
    /// since a caller asking past the end of a segment has a bad pointer.
    fn read_at(&self, segid: SegmentId, off: usize, buf: &mut [u8]) -> Result<()> {
        let Some(seg) = self.segments.get(&segid) else {
            return Err(Error::new(ErrorKind::NotFound,
                format!("segment {} does not exist in memory staging", segid)));
        };
        let end = off + buf.len();
        if end > seg.len() {
            return Err(Error::new(ErrorKind::InvalidData,
                format!("read of {} bytes at offset {} is past the end of segment {} ({} bytes)",
                    buf.len(), off, segid, seg.len())));
        }
        buf.copy_from_slice(&seg[off..end]);
        Ok(())
    }
}

/// Staging that keeps everything in memory. See the module docs.
pub struct MemoryStaging {
    store: Arc<RwLock<MemoryStore>>,
    /// Shared with every clone, and handed to the bmap by
    /// `to_block_loader`. Filled in `done`; see the module docs.
    loader: MemoryBlockLoader<BlockPtr>,
    /// Needed to rebuild the keys the bmap recorded.
    ///
    /// This cannot be discovered: the segment header does not record it,
    /// and the meta config is not it either — `HyperFile::new` hardcodes
    /// `BlockPtrFormat::MicroGroup` regardless of
    /// `HyperFileMetaConfig::block_ptr_format`, and `open` takes it from
    /// the bmap's user data, which the staging layer cannot see. So it is
    /// stated here, defaulting to what create actually uses.
    block_ptr_format: BlockPtrFormat,
    meta_block_size: usize,
    root_path: String,
    config: StagingConfig,
    runtime_config: HyperFileRuntimeConfig,
    interceptor: Option<Arc<dyn StagingIntercept<Self>>>,
    read_timing: Arc<ReadTiming>,
}

impl Clone for MemoryStaging {
    fn clone(&self) -> Self {
        Self {
            store: self.store.clone(),
            loader: self.loader.clone(),
            block_ptr_format: self.block_ptr_format,
            meta_block_size: self.meta_block_size,
            root_path: self.root_path.clone(),
            config: self.config.clone(),
            runtime_config: self.runtime_config.clone(),
            interceptor: self.interceptor.clone(),
            read_timing: self.read_timing.clone(),
        }
    }
}

impl MemoryStaging {
    /// A handle over fresh, empty storage. Clones share it; a new call
    /// does not.
    pub fn new(config: StagingConfig, runtime_config: HyperFileRuntimeConfig) -> Self {
        Self::with_meta_block_size(config, runtime_config, HyperFileMetaConfig::default().meta_block_size)
    }

    /// The loader is sized at construction because
    /// `MemoryBlockLoader::read` requires the buffer it is handed to be
    /// exactly one node long, so it has to agree with the file's meta
    /// block size.
    pub fn with_meta_block_size(config: StagingConfig, runtime_config: HyperFileRuntimeConfig, meta_block_size: usize) -> Self {
        let root_path = config.root_uri.clone();
        Self {
            store: Arc::new(RwLock::new(MemoryStore::default())),
            loader: MemoryBlockLoader::new(meta_block_size),
            block_ptr_format: BlockPtrFormat::MicroGroup,
            meta_block_size,
            root_path,
            config,
            runtime_config,
            interceptor: None,
            read_timing: Arc::new(ReadTiming::default()),
        }
    }

    /// Storage with default configuration, named `name`. Enough for a
    /// test that does not care about paths.
    pub fn with_name(name: &str) -> Self {
        let config = StagingConfig::new_memory(name);
        Self::new(config, HyperFileRuntimeConfig::default())
    }

    /// State which block pointer format the file on this storage uses, if
    /// it is not the `MicroGroup` that `HyperFile::new` applies.
    pub fn with_block_ptr_format(mut self, fmt: BlockPtrFormat) -> Self {
        self.block_ptr_format = fmt;
        self
    }

    /// Number of segments currently held. For tests that want to assert a
    /// flush produced one, or that a rollback left none.
    pub fn segment_count(&self) -> usize {
        self.store.read().unwrap().segments.len()
    }

    /// Total bytes across all segments plus the inode, so a test can
    /// assert on write amplification without inspecting the format.
    pub fn bytes_stored(&self) -> usize {
        let store = self.store.read().unwrap();
        store.segments.values().map(|s| s.len()).sum::<usize>()
            + store.inode.as_ref().map_or(0, |i| i.len())
    }

    /// Whether an inode has been written, i.e. whether a file exists here.
    pub fn has_inode(&self) -> bool {
        self.store.read().unwrap().inode.is_some()
    }

    fn now() -> i64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs() as i64)
            .unwrap_or(0)
    }

    /// Hand the segment's meta nodes to the loader under the keys the bmap
    /// recorded for them. See the module docs for why the keys can be
    /// rebuilt here.
    fn publish_meta_nodes(&self, segid: SegmentId, seg: &[u8]) -> Result<()> {
        if seg.len() < SegmentHeader::size() {
            return Err(Error::new(ErrorKind::InvalidData, "incorrect segment header size"));
        }
        let hdr = SegmentHeader::from_slice(&seg[..SegmentHeader::size()]);
        let nmetablk = hdr.s_nmetablk as usize;
        if nmetablk == 0 {
            return Ok(());
        }
        let node_size = 1usize << hdr.s_meta_blk_shift;
        let base = hdr.aligned_ss_bytes();
        if node_size != self.meta_block_size {
            // The loader was sized for a different file. Its `read` asserts
            // on the buffer length, so this would fail later and further
            // away; say so here instead.
            return Err(Error::new(ErrorKind::InvalidInput, format!(
                "segment {} has {} byte meta nodes but this staging was built for {}",
                segid, node_size, self.meta_block_size)));
        }
        if base + nmetablk * node_size > seg.len() {
            return Err(Error::new(ErrorKind::InvalidData, format!(
                "segment {} claims {} meta nodes at offset {} but is only {} bytes",
                segid, nmetablk, base, seg.len())));
        }

        let fmt = self.block_ptr_format;
        for i in 0..nmetablk {
            let off = base + i * node_size;
            // Same three parts the flush used: this segment, the node's
            // offset in it, and its sequence — meta nodes are numbered
            // first, so node `i` has sequence `i`.
            let key = BlockPtrFormat::encode(segid, off, i, &fmt);
            let mut store = self.store.write().unwrap();
            if !store.meta_keys.insert(key) {
                // Already published. Reachable because a rolled back flush
                // rewinds the sequence and the retry reuses the segment id;
                // writing again would panic inside the loader.
                debug!("memory staging: meta key {} already published, skipping", key);
                continue;
            }
            drop(store);
            self.loader.write(key, &seg[off..off + node_size]);
        }
        Ok(())
    }

    /// Parse a segment's summary out of the stored bytes. Shares
    /// `SegmentSum::from_slice` with the S3 path so the two cannot
    /// disagree about the format.
    fn segment_sum(&self, segid: SegmentId) -> Result<SegmentSum> {
        let store = self.store.read().unwrap();
        let Some(seg) = store.segments.get(&segid) else {
            return Err(Error::new(ErrorKind::NotFound,
                format!("segment {} does not exist in memory staging", segid)));
        };
        let hdr_size = SegmentHeader::size();
        if seg.len() < hdr_size {
            return Err(Error::new(ErrorKind::InvalidData, "incorrect segment header size"));
        }
        let hdr = SegmentHeader::from_slice(&seg[..hdr_size]);
        let ss_bytes = hdr.s_bytes as usize;
        if seg.len() < ss_bytes {
            return Err(Error::new(ErrorKind::InvalidData,
                format!("segment summary truncated in segment {}: header claims {} bytes, segment has {}",
                    segid, ss_bytes, seg.len())));
        }
        Ok(SegmentSum::from_slice(&seg[..ss_bytes]))
    }
}

impl Staging<MemoryBlockLoader<BlockPtr>> for MemoryStaging {
    fn to_block_loader(&self) -> MemoryBlockLoader<BlockPtr> {
        self.loader.clone()
    }

    fn read_timing(&self) -> &ReadTiming {
        &self.read_timing
    }

    async fn load_inode(&self, buf: &mut [u8]) -> Result<Option<OnDiskState>> {
        let start = std::time::Instant::now();
        let store = self.store.read().unwrap();
        let Some(inode) = store.inode.as_ref() else {
            self.read_timing.add_inode_get(start.elapsed().as_nanos() as u64);
            return Err(Error::new(ErrorKind::NotFound, "inode does not exist in memory staging"));
        };
        if buf.len() > inode.len() {
            return Err(Error::new(ErrorKind::InvalidData,
                format!("inode read of {} bytes but only {} stored", buf.len(), inode.len())));
        }
        buf.copy_from_slice(&inode[..buf.len()]);
        drop(store);
        self.read_timing.add_inode_get(start.elapsed().as_nanos() as u64);
        Ok(None)
    }

    async fn load_inode_from_segment(&self, buf: &mut [u8], segid: SegmentId) -> Result<Option<OnDiskState>> {
        if segid.as_cno() == 0 {
            return self.load_inode(buf).await;
        }
        let inode_off = std::mem::offset_of!(SegmentHeader, s_inode);
        let inode_bytes = std::mem::size_of::<InodeRaw>();
        if buf.len() > inode_bytes {
            return Err(Error::new(ErrorKind::InvalidData,
                format!("inode read of {} bytes exceeds the {} an inode occupies", buf.len(), inode_bytes)));
        }
        let start = std::time::Instant::now();
        // See the S3 staging: which name holds the inode is recorded in the
        // inode, so both are tried.
        let res = {
            let store = self.store.read().unwrap();
            let mut r = store.read_at(segid.at_part(crate::segment::Segment::SUMMARY_PART), inode_off, buf);
            if r.is_err() {
                r = store.read_at(segid.whole(), inode_off, buf);
            }
            r
        };
        self.read_timing.add_inode_get(start.elapsed().as_nanos() as u64);
        res?;
        Ok(None)
    }

    async fn load_segment_timestamp(&self, segid: SegmentId) -> Result<(i64, i64)> {
        let store = self.store.read().unwrap();
        let Some(ts) = store.timestamps.get(&segid.at_part(crate::segment::Segment::SUMMARY_PART))
            .or_else(|| store.timestamps.get(&segid.whole())) else {
            return Err(Error::new(ErrorKind::NotFound,
                format!("checkpoint {} does not exist in memory staging", segid)));
        };
        // Server time and last-modified are the same clock here, which is
        // the degenerate case of what S3 reports.
        Ok((Self::now(), *ts))
    }

    async fn flush_inode(&self, buf: &[u8], inode_state: &Option<OnDiskState>, flag: FlushInodeFlag) -> Result<Option<OnDiskState>> {
        if let Some(i) = &self.interceptor {
            let _ = i.before_flush_inode(&self, buf, flag.clone()).await?;
        }
        self.store.write().unwrap().inode = Some(buf.to_vec());
        if let Some(i) = &self.interceptor {
            let _ = i.after_flush_inode(&self, buf, flag).await;
        }
        // No conditional write to carry state for, so whatever the caller
        // had stays as it was.
        Ok(inode_state.clone())
    }

    async fn remove_inode(&self, _inode_state: &Option<OnDiskState>) -> Result<()> {
        self.store.write().unwrap().inode = None;
        if let Some(i) = &self.interceptor {
            let _ = i.after_remove_inode(&self).await;
        }
        Ok(())
    }

    async fn load_data_block(&self, segid: SegmentId, staging_off: usize, offset: usize, block_size: usize, buf: &mut [u8]) -> Result<()> {
        let start_off = staging_off + offset;
        debug!("mem staging: load data block from segment {} at offset {} size {}", segid, start_off, block_size);
        let start = std::time::Instant::now();
        let res = self.store.read().unwrap().read_at(segid, start_off, buf);
        self.read_timing.add_data_get(block_size, start.elapsed().as_nanos() as u64);
        res
    }

    async fn load_range(&self, segid: SegmentId, s3_off: usize, buf: &mut [u8]) -> Result<()> {
        let len = buf.len();
        debug_assert!(len > 0, "load_range with zero length");
        debug!("mem staging: load range from segment {} at offset {} len {}", segid, s3_off, len);
        let start = std::time::Instant::now();
        let res = self.store.read().unwrap().read_at(segid, s3_off, buf);
        self.read_timing.add_data_get(len, start.elapsed().as_nanos() as u64);
        res
    }

    fn new_segwr(&self, segid: SegmentId, hyper_file_config: &HyperFileMetaConfig) -> segment::Writer<MemoryStaging> {
        segment::Writer::<MemoryStaging>::new(self.clone(), self.runtime_config.segment_buffer_size, segid, hyper_file_config)
    }

    fn dir_filename(&self) -> (&str, &str) {
        if let Some((dir, filename)) = self.root_path.rsplit_once('/') {
            return (dir, filename);
        }
        ("", &self.root_path)
    }

    fn root_path(&self) -> &str {
        &self.root_path
    }

    fn config(&self) -> &StagingConfig {
        &self.config
    }

    async fn unlink(&self) -> Result<()> {
        self.remove_inode(&None).await?;
        let mut store = self.store.write().unwrap();
        store.segments.clear();
        store.timestamps.clear();
        Ok(())
    }

    fn interceptor(&mut self, i: impl StagingIntercept<Self> + 'static) {
        self.interceptor = Some(Arc::new(i));
    }
}

impl SegmentReadWrite for MemoryStaging {
    fn append(&self, segid: SegmentId, buf: &[u8]) -> Result<()> {
        // The writer buffers the segment itself and hands it over whole in
        // `done`, exactly as the S3 path does.
        let _ = (segid, buf);
        Ok(())
    }

    async fn done(&self, segid: SegmentId, buf: &[u8], len: usize) -> Result<()> {
        if let Some(i) = &self.interceptor {
            let _ = i.before_segment_done(&self, segid, buf, len).await?;
        }
        let (data, _) = buf.split_at(len);
        debug!("mem staging: store segment {} of {} bytes", segid, len);
        {
            let mut store = self.store.write().unwrap();
            store.segments.insert(segid, data.to_vec());
            store.timestamps.insert(segid, Self::now());
        }
        self.publish_meta_nodes(segid, data)
    }

    async fn done_pieces(&self, segid: SegmentId, body: SegmentBody) -> Result<()> {
        // Nothing here can scatter-write, so the pieces are joined. The
        // interceptor sees the same bytes either way.
        let mut data = Vec::with_capacity(body.len());
        for piece in body.pieces() {
            data.extend_from_slice(piece);
        }
        if let Some(i) = &self.interceptor {
            let _ = i.before_segment_done(&self, segid, &data, data.len()).await?;
        }
        debug!("mem staging: store segment {} of {} bytes (pieces)", segid, data.len());
        {
            let mut store = self.store.write().unwrap();
            store.segments.insert(segid, data.clone());
            store.timestamps.insert(segid, Self::now());
        }
        self.publish_meta_nodes(segid, &data)
    }

    async fn remove(&self, segid: SegmentId) -> Result<()> {
        let mut store = self.store.write().unwrap();
        store.segments.remove(&segid);
        store.timestamps.remove(&segid);
        Ok(())
    }

    async fn open(&self, segid: SegmentId) -> Result<SegmentSum> {
        self.segment_sum(segid)
    }

    async fn list(&self, segid: SegmentId) -> Result<Vec<SegmentId>> {
        // Same contract as the S3 listing: segment ids at or above
        // `segid`, ascending. `BTreeMap` keeps them ordered already.
        let store = self.store.read().unwrap();
        {
            // Checkpoint ids, so a streamed one counts once however many objects
            // it was written as.
            let mut v: Vec<SegmentId> = store.segments.keys()
                .map(|o| o.whole()).filter(|s| *s >= segid.whole()).collect();
            v.dedup();
            Ok(v)
        }
    }

    async fn build_block_map(&self, segid: SegmentId) -> Result<Vec<(BlockIndex, BlockPtr)>> {
        // Same geometry the S3 path derives in `do_fetch_meta_blocks_chunk`:
        // the meta blocks sit after the aligned summary, all of one size.
        let (meta_block_off, meta_block_size, nmetablk) = {
            let store = self.store.read().unwrap();
            let Some(seg) = store.segments.get(&segid) else {
                return Err(Error::new(ErrorKind::NotFound,
                    format!("segment {} does not exist in memory staging", segid)));
            };
            if seg.len() < SegmentHeader::size() {
                return Err(Error::new(ErrorKind::InvalidData, "incorrect segment header size"));
            }
            let hdr = SegmentHeader::from_slice(&seg[..SegmentHeader::size()]);
            (hdr.aligned_ss_bytes(), (1usize << hdr.s_meta_blk_shift), hdr.s_nmetablk as usize)
        };
        if nmetablk == 0 {
            return Ok(Vec::new());
        }

        let mut buf = vec![0u8; meta_block_size * nmetablk];
        self.store.read().unwrap().read_at(segid, meta_block_off, &mut buf)?;

        let mut v = Vec::new();
        for meta_block_slice in buf.chunks(meta_block_size) {
            let node = BtreeNode::<BlockIndex, BlockPtr, BlockPtr>::from_slice_ref(meta_block_slice)?;
            if node.get_level() == BTREE_NODE_LEVEL_DATA + 1 {
                for idx in 0..node.get_nchild() {
                    v.push((*node.get_key(idx), *node.get_val(idx)));
                }
            }
        }
        Ok(v)
    }
}

impl std::fmt::Debug for MemoryStaging {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let store = self.store.read().unwrap();
        f.debug_struct("MemoryStaging")
            .field("root_path", &self.root_path)
            .field("segments", &store.segments.len())
            .field("has_inode", &store.inode.is_some())
            .finish()
    }
}
