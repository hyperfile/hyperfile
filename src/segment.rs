use std::fmt;
use std::io::Result;
#[cfg(feature = "wal")]
use std::sync::Arc;
#[cfg(feature = "wal")]
use std::sync::Weak;
#[cfg(feature = "wal")]
use std::pin::Pin;
#[cfg(not(feature = "wal"))]
use bytes::Bytes;
use crate::SegmentId;
use crate::ondisk::{SegmentHeader, SegmentBlockEntryRaw};
use crate::{BlockIndex, BlockPtr};
use crate::config::HyperFileMetaConfig;
use crate::inode::Inode;
use crate::ondisk::InodeRaw;
use crate::buffer::DataBlock;
use crate::segment_body::SegmentBody;

pub struct Segment;

impl Segment {
    /// The object name for one piece of a checkpoint.
    ///
    /// `None` is a checkpoint written as a single object, which is every format
    /// before parting, and it keeps the bare name it has always had — a container
    /// written by an older build has to stay readable. `Some(p)` names one object
    /// of a streamed checkpoint.
    ///
    /// `None` and `Some(0)` are deliberately different keys rather than the same
    /// one. Sharing a name would mean a parted container and an unparted one
    /// disagreeing about what a bare name holds, and the disagreement would only
    /// show up as a read of the wrong bytes.
    pub fn segid_to_staging_file_id(segid: SegmentId) -> String {
        match segid.part_id() {
            None => format!("{:0>10}", segid.seq_id()),
            Some(p) => format!("{:0>10}.{}", segid.seq_id(), p),
        }
    }

    /// The part every checkpoint's summary, metadata blocks and inode live in.
    ///
    /// Written last, because until the data parts exist there is nothing to
    /// describe. That ordering is what makes its presence mean the stream
    /// finished.
    pub const SUMMARY_PART: u16 = 0;

    /// Where data blocks start. Part 0 is the summary's.
    pub const FIRST_DATA_PART: u16 = 1;
}

pub trait SegmentReadWrite {
    // writer
    fn append(&self, segid: SegmentId, buf: &[u8]) -> Result<()>;

    /// Upload a fully-built segment whose contents live in a
    /// single contiguous `&[u8]`. Used by the WAL feature path,
    /// which needs the segment buffer pinned in memory across the
    /// upload+replay window.
    fn done(&self, segid: SegmentId, buf: &[u8], len: usize) -> impl Future<Output = Result<()>> + Send;

    /// Upload a fully-built segment whose contents live as a
    /// list of `Bytes` pieces. Implementations should stream the
    /// pieces straight to the remote (e.g. via
    /// `http_body::Body`) rather than concatenating them, which
    /// is the whole point of having this entry point separate
    /// from `done`.
    fn done_pieces(&self, segid: SegmentId, body: SegmentBody) -> impl Future<Output = Result<()>> + Send;
    fn remove(&self, segid: SegmentId) -> impl Future<Output = Result<()>>;
    // reader
    fn open(&self, segid: SegmentId) -> impl Future<Output = Result<SegmentSum>>;
    fn list(&self, segid: SegmentId) -> impl Future<Output = Result<Vec<SegmentId>>>;
    fn build_block_map(&self, segid: SegmentId) -> impl Future<Output = Result<Vec<(BlockIndex, BlockPtr)>>>;
}

const SEGMENT_SUMMARY_HEADER_SIZE: usize = std::mem::size_of::<SegmentHeader>();
const SEGMENT_SUMMARY_BLOCK_ENTRY_SIZE: usize = std::mem::size_of::<SegmentBlockEntryRaw>();

// entry of block
#[derive(Debug)]
pub struct SegmentBlockDesc {
    pub(crate) blkidx: BlockIndex,
    pub(crate) blkptr: BlockPtr,
}

impl fmt::Display for SegmentBlockDesc {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "SegmentBlockDesc {{ blkidx: {}, blkptr: {} }}", self.blkidx, self.blkptr)
    }
}

// in memory struct for segment summary
pub struct SegmentSum {
    pub hdr: SegmentHeader,
    pub blocks: Vec<SegmentBlockDesc>,
}

impl fmt::Display for SegmentSum {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(f, "==== dump Segment Summary ====")?;
        writeln!(f, "  ino: {}, cno: {}, next segment: {}", self.hdr.s_ino, self.hdr.s_cno, self.hdr.s_next)?;
        writeln!(f, "  magic {:#x}, checksum: {:#x}", self.hdr.s_magic, self.hdr.s_chksum)?;
        writeln!(f, "  bytes: {}, flags: {:#x}", self.hdr.s_bytes, self.hdr.s_flags)?;
        writeln!(f, "  meta block shift: {}, data block shift: {}", self.hdr.s_meta_blk_shift, self.hdr.s_data_blk_shift)?;
        writeln!(f, "  meta block count: {}, data block count: {}", self.hdr.s_nmetablk, self.hdr.s_ndatablk)?;
        for blkdesc in self.blocks.iter() {
            writeln!(f, "    {}", blkdesc)?;
        }
        writeln!(f, "  {}", Inode::from_raw(&self.hdr.s_inode, None))
    }
}

impl SegmentSum {
    pub fn from_slice(buf: &[u8]) -> Self {
        let hdrsz = SEGMENT_SUMMARY_HEADER_SIZE;
        let bufsz = buf.len();
        if bufsz < hdrsz {
            panic!("failed to create segment sum from buf, buf len {} < hdr size {}, it's too small", bufsz, hdrsz);
        }
        // 1. decode header.
        let hdr = SegmentHeader::read_from(&buf[..hdrsz]);

        let ndatablk = hdr.s_ndatablk as usize;
        let need = hdrsz + ndatablk * SEGMENT_SUMMARY_BLOCK_ENTRY_SIZE;
        if bufsz < need {
            panic!("failed to create segment sum from buf, buf len {} < hdr size {} + num of blk idx {}, it's too small", bufsz, hdrsz, ndatablk);
        }

        // 2. decode each entry field-by-field.
        let mut blocks: Vec<SegmentBlockDesc> = Vec::with_capacity(ndatablk);
        let mut off = hdrsz;
        for _ in 0..ndatablk {
            let raw = SegmentBlockEntryRaw::read_from(&buf[off..off + SEGMENT_SUMMARY_BLOCK_ENTRY_SIZE]);
            blocks.push(SegmentBlockDesc {
                blkidx: raw.e_blkidx,
                blkptr: raw.e_blkptr,
            });
            off += SEGMENT_SUMMARY_BLOCK_ENTRY_SIZE;
        }

        Self { hdr, blocks }
    }

    // write ss in segment header raw format into segment output buffer
    pub fn write_to(&self, buf: &mut [u8]) {
        let hdrsz = SEGMENT_SUMMARY_HEADER_SIZE;
        let bufsz = buf.len();
        if bufsz < hdrsz {
            panic!("failed to write segment sum to buf, buf len {} < hdr size {}, it's too small", bufsz, hdrsz);
        }

        let ndatablk = self.hdr.s_ndatablk as usize;
        let need = hdrsz + ndatablk * SEGMENT_SUMMARY_BLOCK_ENTRY_SIZE;
        if bufsz < need {
            panic!("failed to write segment sum to buf, buf len {} < hdr size {} + num of blk idx {}, it's too small", bufsz, hdrsz, ndatablk);
        }

        // 1. write header (first hdrsz bytes).
        self.hdr.write_to(&mut buf[..hdrsz]);

        // 2. write entries one by one, field-by-field. Note: we iterate
        //    over `self.blocks` (`Vec<SegmentBlockDesc>`), which is the
        //    in-memory representation; on-disk `SegmentBlockEntryRaw` is
        //    written directly from `blkidx` / `blkptr`.
        let mut off = hdrsz;
        for entry in self.blocks.iter() {
            let raw = SegmentBlockEntryRaw {
                e_blkidx: entry.blkidx,
                e_blkptr: entry.blkptr,
            };
            raw.write_to(&mut buf[off..off + SEGMENT_SUMMARY_BLOCK_ENTRY_SIZE]);
            off += SEGMENT_SUMMARY_BLOCK_ENTRY_SIZE;
        }
    }

    // return real size of segment summary
    pub fn update(&mut self, chksum: u32, inode: &InodeRaw) -> usize {
        let data_block_count = self.blocks.len();
        let ss_bytes = SEGMENT_SUMMARY_HEADER_SIZE + data_block_count * SEGMENT_SUMMARY_BLOCK_ENTRY_SIZE;

        assert!(self.hdr.s_ndatablk == data_block_count as u32);
        self.hdr.s_inode = inode.to_owned();
        self.hdr.s_bytes = ss_bytes as u32;
        self.hdr.s_chksum = chksum;

        ss_bytes
    }

    // calc bytes off of data block by it's index at blocks section
    pub fn calc_staging_off(&self, data_block_index: usize) -> usize {
        let mut offset = 0;
        offset += self.hdr.aligned_ss_bytes();
        offset += (self.hdr.s_nmetablk << self.hdr.s_meta_blk_shift) as usize;
        offset += data_block_index << self.hdr.s_data_blk_shift;
        offset
    }
}

pub struct Writer<T> {
    ctx: T,
    /// Header / segment summary buffer. Small (a few KiB), built
    /// in place by `realize_ss`. Promoted to a `Bytes` and pushed
    /// into `body` as the first piece during `done`.
    #[cfg(not(feature = "wal"))]
    header: Vec<u8>,
    /// Scatter list of pieces (meta blocks + data blocks) appended
    /// in order, each as its own refcounted `Bytes`. The S3 staging
    /// uploads them via `http_body::Body` streaming, so they never
    /// get concatenated into one allocation.
    #[cfg(not(feature = "wal"))]
    body: SegmentBody,
    #[cfg(feature = "wal")]
    data: Arc<Pin<Box<Vec<u8>>>>,
    offset: usize,
    /// Which object this writer produces.
    ///
    /// Unparted for a checkpoint written as one object; part 0 for a streamed
    /// one — the writer only ever builds that one, because the data parts are
    /// uploaded straight from the flush as they fill and never pass through here.
    segid: SegmentId,
    ss: SegmentSum,
}

// router stub to real impl of writer function in Staging
impl<T: SegmentReadWrite> Writer<T> {
    pub fn new(ctx: T, buf_size: usize, segid: SegmentId, hyper_file_config: &HyperFileMetaConfig) -> Self {
        // _buf_size is intentionally unused on the non-WAL path:
        // we no longer pre-allocate one big segment buffer up
        // front. Pieces are appended as they arrive and total
        // memory is the sum of pieces.
        let _ = buf_size;

        let mut hdr = SegmentHeader::new();
        hdr.s_meta_blk_shift = hyper_file_config.meta_block_size.checked_ilog2().unwrap() as u8;
        hdr.s_data_blk_shift = hyper_file_config.data_block_size.checked_ilog2().unwrap() as u8;
        hdr.s_next = 0;
        hdr.s_ino = 0;
        hdr.s_cno = segid.as_cno();

        Self {
            ctx: ctx,
            #[cfg(not(feature = "wal"))]
            header: Vec::new(),
            #[cfg(not(feature = "wal"))]
            body: SegmentBody::new(),
            #[cfg(feature = "wal")]
            data: Arc::new(Box::pin(Vec::with_capacity(buf_size))),
            offset: 0,
            segid: if hyper_file_config.block_ptr_format.is_parted() {
                segid.at_part(Segment::SUMMARY_PART)
            } else {
                segid.whole()
            },
            ss: SegmentSum {
                hdr: hdr,
                blocks: Vec::new(),
            },
        }
    }

    // calc aligned segment summary bytes based on num data blocks input
    // segment summary is 4KiB block aligned
    #[inline]
    pub fn calc_ss_aligned_bytes(ndatablk: usize) -> usize {
        let ss_bytes = SEGMENT_SUMMARY_HEADER_SIZE + ndatablk * SEGMENT_SUMMARY_BLOCK_ENTRY_SIZE;
        // align ss_bytes into 4KiB boundary
        (ss_bytes + 4096 - 1) >> 12 << 12
    }

    pub fn realize_ss(&mut self, checksum: u32, inode: &InodeRaw) {
        // don't actually need checksum now
        let ss_bytes = self.ss.update(checksum, inode);
        #[cfg(not(feature = "wal"))]
        let ss_aligned_bytes = {

        // Build the header / summary into a small contiguous Vec
        // that becomes the first piece of the body.
        self.header.resize(ss_bytes, 0);
        self.ss.write_to(&mut self.header);
        // calc 4KiB aligned bytes from real size of ss
        let ss_aligned_bytes = (ss_bytes + 4096 - 1) >> 12 << 12;
        // extend current ss to aligned size
        self.header.resize(ss_aligned_bytes, 0);

        ss_aligned_bytes

        };

        #[cfg(feature = "wal")]
        let ss_aligned_bytes = {

        let Some(data) = Arc::get_mut(&mut self.data) else {
            panic!("failed to get back inner data buffer during segment build");
        };
        data.resize(ss_bytes, 0);
        self.ss.write_to(data);
        // calc 4KiB aligned bytes from real size of ss
        let ss_aligned_bytes = (ss_bytes + 4096 - 1) >> 12 << 12;
        // extend current ss to aligned size
        data.resize(ss_aligned_bytes, 0);

        ss_aligned_bytes

        };

        self.offset += ss_aligned_bytes;
    }

    pub fn append(&mut self, buf: &[u8]) -> Result<()> {
        let len = buf.len();
        #[cfg(not(feature = "wal"))]
        {
            // Push a refcounted copy of `buf` as a single piece;
            // no contiguous-buffer concat happens.
            self.body.push(Bytes::copy_from_slice(buf));
        }
        #[cfg(feature = "wal")]
        let slice_start = {

        let Some(data) = Arc::get_mut(&mut self.data) else {
            panic!("failed to get back inner data buffer during segment build");
        };
        data.resize(self.offset + len, 0);
        let slice_start = &mut data[self.offset..];
        slice_start

        };

        #[cfg(feature = "wal")]
        {
            let (data, _) = slice_start.split_at_mut(len);
            data.copy_from_slice(buf);
        }
        self.offset += len;
        self.ctx.append(self.segid, buf)
    }

    /// Append one cached data block to the segment.
    ///
    /// Non-WAL: pushes a zero-copy `Bytes` view over the block's
    /// internal `Arc<AllocDataBlock>` (no memcpy of the 4 KiB
    /// payload). The cache continues to hold the block; the
    /// segment body holds an extra `Arc` clone via the `Bytes`,
    /// so the buffer lives as long as the upload needs it.
    ///
    /// WAL: falls back to the buffer-based `append` path because
    /// the WAL feature requires the segment body to live as one
    /// contiguous `Vec<u8>` for in-memory replay.
    pub fn append_data_block(&mut self, block: &DataBlock) -> Result<()> {
        #[cfg(not(feature = "wal"))]
        {
            let bytes = block.bytes_view();
            let len = bytes.len();
            self.body.push(bytes);
            self.offset += len;
            // Mirror the side-effect of the buffer-based append:
            // notify staging context. No-op for S3, kept for
            // trait conformance.
            self.ctx.append(self.segid, block.as_slice())
        }
        #[cfg(feature = "wal")]
        {
            self.append(block.as_slice())
        }
    }

    /// Concurrent segment-build path for the WAL feature only.
    /// Without WAL the segment body lives as a `Vec<Bytes>`
    /// already, so there is no contiguous buffer to concat into
    /// and parallelizing memcpy via `spawn_blocking` would be
    /// pure overhead — the non-WAL `flush_process_build_segment`
    /// call site uses sequential `append` instead.
    #[cfg(all(feature = "concurrent-segment-build", feature = "wal"))]
    pub fn spawn_append(&mut self, chunk: Vec<&DataBlock>) -> Result<tokio::task::JoinHandle<()>> {
        let len = chunk.iter().map(|block| block.size()).sum();
        let slice_start = {

        let Some(data) = Arc::get_mut(&mut self.data) else {
            panic!("failed to get back inner data buffer during segment build");
        };
        data.resize(self.offset + len, 0);
        let slice_start = &mut data[self.offset..];
        slice_start

        };

        let (data, _) = slice_start.split_at_mut(len);
        // extend source and target buf lifetime for spawn_blocking
        let new_data = unsafe {
            std::slice::from_raw_parts_mut(data.as_mut_ptr() as *mut u8, data.len())
        };
        let new_chunk = chunk.into_iter()
                .map(|block| unsafe {
                    let buf = block.as_slice();
                    std::slice::from_raw_parts(buf.as_ptr() as *const u8, buf.len())
                })
                .collect::<Vec<&[u8]>>();
        let join = tokio::task::spawn_blocking(move || {
            for (i, buf) in new_chunk.into_iter().enumerate() {
                let start = i * buf.len();
                let end = start + buf.len();
                let data = &mut new_data[start..end];
                data.copy_from_slice(buf);
            }
        });
        self.offset += len;
        // NOTE:
        // call of self.ctx.append(self.segid.seq_id(), buf) is removed
        Ok(join)
    }

    pub async fn done(self) -> Result<()> {
        #[cfg(not(feature = "wal"))]
        {
            // Stitch the header (one piece) and the appended
            // meta+data pieces (already in `body`) into the
            // final scatter body, then hand it to the staging
            // for streaming upload. Total memory footprint of
            // the body equals the sum of its pieces — no
            // contiguous segment buffer is allocated.
            let mut body = SegmentBody::with_capacity(self.body.pieces().len() + 1);
            // Header / summary is the first frame.
            body.push(Bytes::from(self.header));
            for piece in self.body.pieces() {
                body.push(piece.clone());
            }
            assert_eq!(
                body.len(),
                self.offset,
                "scatter body length must match the cumulative writer offset",
            );
            self.ctx.done_pieces(self.segid, body).await
        }
        #[cfg(feature = "wal")]
        {
            let _ = self.ctx.done(self.segid, &self.data, self.offset).await?;
            // force a strong count to keep data's life until wal_clear_mem_segment()
            unsafe { Arc::increment_strong_count(Arc::as_ptr(&self.data)) };
            Ok(())
        }
    }

    #[cfg(feature = "wal")]
    pub fn get_weak_data(&self) -> (SegmentId, Weak<Pin<Box<Vec<u8>>>>) {
        (self.segid.whole(), Arc::downgrade(&self.data))
    }
}

// functions for SegmentSummary
impl<T> Writer<T> {
    // count one meta block
    pub fn inc_metablk(&mut self) {
        self.ss.hdr.s_nmetablk += 1;
    }

    // count one data block
    pub fn inc_datablk(&mut self, blkidx: &BlockIndex, blkptr: &BlockPtr) {
        self.ss.hdr.s_ndatablk += 1;
        self.ss.blocks.push(SegmentBlockDesc { blkidx: *blkidx, blkptr: *blkptr });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_segment_sum(ndatablk: usize) -> SegmentSum {
        let mut hdr = SegmentHeader::new();
        hdr.s_cno = 7;
        hdr.s_ino = 1;
        hdr.s_meta_blk_shift = 12;
        hdr.s_data_blk_shift = 12;
        hdr.s_nmetablk = 2;
        hdr.s_ndatablk = ndatablk as u32;
        let blocks: Vec<SegmentBlockDesc> = (0..ndatablk)
            .map(|i| SegmentBlockDesc { blkidx: i as u64, blkptr: 0x4000_0001_0000_0000 + i as u64 })
            .collect();
        SegmentSum { hdr, blocks }
    }

    #[test]
    fn segment_sum_write_read_round_trip() {
        let ndatablk = 3;
        let mut ss = make_segment_sum(ndatablk);
        let inode = InodeRaw::default();
        let ss_bytes = ss.update(0xDEAD, &inode);

        let mut buf = vec![0u8; ss_bytes];
        ss.write_to(&mut buf);

        let restored = SegmentSum::from_slice(&buf);
        assert_eq!(restored.hdr.s_magic, 0x48465353);
        assert_eq!(restored.hdr.s_cno, 7);
        assert_eq!(restored.hdr.s_nmetablk, 2);
        assert_eq!(restored.hdr.s_ndatablk, ndatablk as u32);
        assert_eq!(restored.hdr.s_chksum, 0xDEAD);
        assert_eq!(restored.hdr.s_bytes, ss_bytes as u32);
        assert_eq!(restored.blocks.len(), ndatablk);
        for i in 0..ndatablk {
            assert_eq!(restored.blocks[i].blkidx, i as u64);
            assert_eq!(restored.blocks[i].blkptr, 0x4000_0001_0000_0000 + i as u64);
        }
    }

    #[test]
    fn segment_sum_zero_data_blocks() {
        let mut ss = make_segment_sum(0);
        let inode = InodeRaw::default();
        let ss_bytes = ss.update(0, &inode);

        let mut buf = vec![0u8; ss_bytes];
        ss.write_to(&mut buf);

        let restored = SegmentSum::from_slice(&buf);
        assert_eq!(restored.hdr.s_ndatablk, 0);
        assert!(restored.blocks.is_empty());
    }

    #[test]
    fn segment_sum_calc_staging_off() {
        let ss = make_segment_sum(2);
        // aligned_ss_bytes for header + 2 block entries should be 4096 (fits in one page)
        let aligned = ss.hdr.aligned_ss_bytes();
        // meta blocks: 2 * (1 << 12) = 8192
        // data block 0 offset: aligned + 8192 + 0 * 4096
        assert_eq!(ss.calc_staging_off(0), aligned + 8192);
        assert_eq!(ss.calc_staging_off(1), aligned + 8192 + 4096);
    }

    #[test]
    #[should_panic(expected = "too small")]
    fn segment_sum_from_slice_too_small() {
        SegmentSum::from_slice(&[0u8; 10]);
    }

    #[test]
    fn segment_sum_write_to_oversized_buf() {
        // Over-sized buffer is fine — only the needed prefix bytes are written.
        let ndatablk = 2;
        let mut ss = make_segment_sum(ndatablk);
        let inode = InodeRaw::default();
        let ss_bytes = ss.update(0, &inode);

        let mut buf = vec![0xFFu8; ss_bytes * 2]; // bigger than needed
        ss.write_to(&mut buf);

        // Tail beyond ss_bytes must remain untouched (still 0xFF).
        assert!(buf[ss_bytes..].iter().all(|&b| b == 0xFF));

        // Prefix must decode correctly.
        let restored = SegmentSum::from_slice(&buf[..ss_bytes]);
        assert_eq!(restored.blocks.len(), ndatablk);
    }

    #[test]
    #[should_panic(expected = "too small")]
    fn segment_sum_write_to_too_small_for_entries() {
        // Buffer big enough for header but not entries — must panic BEFORE
        // any bytes are written. Prevents the partial-write bug the old
        // implementation had.
        let ndatablk = 3;
        let mut ss = make_segment_sum(ndatablk);
        let inode = InodeRaw::default();
        let _ = ss.update(0, &inode);

        let hdrsz = std::mem::size_of::<SegmentHeader>();
        let mut buf = vec![0u8; hdrsz]; // exactly header size, no room for entries
        ss.write_to(&mut buf);
    }

    #[test]
    fn segment_sum_many_blocks_crossing_4kib() {
        // SegmentHeader is 208 bytes. With 16-byte entries, (4096 - 208) / 16 = 242
        // entries fit within the first 4 KiB. Use 500 entries so the serialized
        // SegmentSum spans multiple 4 KiB pages and exercises the offset math.
        let ndatablk = 500;
        let mut ss = make_segment_sum(ndatablk);
        let inode = InodeRaw::default();
        let ss_bytes = ss.update(0, &inode);
        assert!(ss_bytes > 4096, "test should span multiple 4KiB pages");

        let mut buf = vec![0u8; ss_bytes];
        ss.write_to(&mut buf);

        let restored = SegmentSum::from_slice(&buf);
        assert_eq!(restored.blocks.len(), ndatablk);
        for i in 0..ndatablk {
            assert_eq!(restored.blocks[i].blkidx, i as u64);
            assert_eq!(restored.blocks[i].blkptr, 0x4000_0001_0000_0000 + i as u64);
        }
    }

    #[test]
    fn segment_sum_write_is_idempotent() {
        // Two consecutive write_to calls must produce byte-identical output.
        // This is essential for any future checksum computation.
        let ndatablk = 10;
        let mut ss = make_segment_sum(ndatablk);
        let inode = InodeRaw::default();
        let ss_bytes = ss.update(0xCAFE, &inode);

        let mut buf1 = vec![0u8; ss_bytes];
        let mut buf2 = vec![0u8; ss_bytes];
        ss.write_to(&mut buf1);
        ss.write_to(&mut buf2);
        assert_eq!(buf1, buf2, "SegmentSum::write_to is not byte-identical on repeat");
    }

    #[test]
    fn segment_sum_round_trip_reserialize_matches() {
        // write -> read -> write should produce byte-identical output to
        // the original write. This verifies the serialization is truly
        // reversible without any lossy conversions.
        let ndatablk = 5;
        let mut ss = make_segment_sum(ndatablk);
        let inode = InodeRaw::default();
        let ss_bytes = ss.update(0xBEEF, &inode);

        let mut buf1 = vec![0u8; ss_bytes];
        ss.write_to(&mut buf1);

        let ss2 = SegmentSum::from_slice(&buf1);
        let mut buf2 = vec![0u8; ss_bytes];
        ss2.write_to(&mut buf2);

        assert_eq!(buf1, buf2, "round-trip write-read-write mismatch");
    }

    #[test]
    fn segid_to_staging_file_id_format() {
        assert_eq!(Segment::segid_to_staging_file_id(SegmentId::new(1)), "0000000001");
        // Ten digits is the padding width, and a checkpoint number is a u32, so
        // the widest one is u32::MAX. A pointer can only name 30 bits of it, so
        // the name has room the addressing does not.
        assert_eq!(Segment::segid_to_staging_file_id(SegmentId::new(u32::MAX)), "4294967295");
        // A streamed checkpoint's objects, and the summary part is not the same
        // key as an unparted checkpoint's.
        assert_eq!(Segment::segid_to_staging_file_id(SegmentId::with_part(1, 0)), "0000000001.0");
        assert_eq!(Segment::segid_to_staging_file_id(SegmentId::with_part(1, 7)), "0000000001.7");
        assert_ne!(Segment::segid_to_staging_file_id(SegmentId::with_part(1, 0)),
                   Segment::segid_to_staging_file_id(SegmentId::new(1)));
    }

    #[test]
    fn writer_calc_ss_aligned_bytes() {
        // calc_ss_aligned_bytes logic: (hdr + n*entry + 4095) >> 12 << 12
        let hdr_sz = std::mem::size_of::<SegmentHeader>();
        let entry_sz = std::mem::size_of::<SegmentBlockEntryRaw>();

        // 0 data blocks: just header, should align to 4096
        let raw = hdr_sz;
        let expected = (raw + 4095) >> 12 << 12;
        assert_eq!(expected, 4096);

        // many blocks that push past 4096
        let n = (4096 - hdr_sz) / entry_sz + 1;
        let raw = hdr_sz + n * entry_sz;
        let expected = (raw + 4095) >> 12 << 12;
        assert!(expected > 4096);
        assert_eq!(expected % 4096, 0);
    }
}
