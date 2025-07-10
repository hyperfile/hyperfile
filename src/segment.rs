use std::fmt;
use std::io::Result;
#[cfg(feature = "wal")]
use std::sync::Arc;
#[cfg(feature = "wal")]
use std::sync::Weak;
#[cfg(feature = "wal")]
use std::pin::Pin;
use crate::SegmentId;
use crate::ondisk::{SegmentHeader, SegmentBlockEntryRaw};
use crate::{BlockIndex, BlockPtr};
use crate::config::HyperFileMetaConfig;
use crate::inode::Inode;
use crate::ondisk::InodeRaw;
#[cfg(feature = "concurrent-segment-build")]
use crate::buffer::DataBlock;

pub struct Segment;

impl Segment {
    pub fn segid_to_staging_file_id(segid: SegmentId) -> String {
        format!("{:0>10}", segid)
    }
}

pub trait SegmentReadWrite {
    // writer
    fn append(&self, segid: SegmentId, buf: &[u8]) -> Result<()>;
    fn done(&self, segid: SegmentId, buf: &[u8], len: usize) -> impl Future<Output = Result<()>> + Send;
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
        // intermediate object for header
        let hdr: SegmentHeader = unsafe { std::mem::MaybeUninit::zeroed().assume_init() };
        let hdr_u8_slice = unsafe {
            std::slice::from_raw_parts_mut(
                std::ptr::addr_of!(hdr) as *mut SegmentHeader as *mut u8,
                hdrsz
            )
        };
        // split input buffer at header size
        let (buf_hdr_slice, buf_remain) = buf.split_at(hdrsz);
        hdr_u8_slice.copy_from_slice(buf_hdr_slice);

        let ndatablk = hdr.s_ndatablk as usize;
        if bufsz < hdrsz + ndatablk * SEGMENT_SUMMARY_BLOCK_ENTRY_SIZE {
            panic!("failed to create segment sum from buf, buf len {} < hdr size {} + num of blk idx {}, it's too small", bufsz, hdrsz, ndatablk);
        }
        // split remain input buffer at block index array boundary
        let (buf_blocks, _) = buf_remain.split_at(ndatablk * SEGMENT_SUMMARY_BLOCK_ENTRY_SIZE);
        // build block index list
        let blocks_raw = unsafe {
            std::slice::from_raw_parts(buf_blocks.as_ptr() as *const SegmentBlockEntryRaw, ndatablk)
        };
        // raw to in memory struct conversion
        let blocks: Vec<SegmentBlockDesc> = blocks_raw.into_iter()
                        .map(|e| SegmentBlockDesc { blkidx: e.e_blkidx, blkptr: e.e_blkptr })
                        .collect();
        Self {
            hdr: hdr,
            blocks: Vec::from(blocks),
        }
    }

    // write ss in segment header raw format into segment output buffer
    pub fn write_to(&self, buf: &mut [u8]) {
        let hdrsz = SEGMENT_SUMMARY_HEADER_SIZE;
        let bufsz = buf.len();
        if bufsz < hdrsz {
            panic!("failed to write segment sum to buf, buf len {} < hdr size {}, it's too small", buf.len(), hdrsz);
        }
        let hdr_u8_slice = unsafe {
            std::slice::from_raw_parts(
                std::ptr::addr_of!(self.hdr) as *const SegmentHeader as *const u8,
                hdrsz
            )
        };

        let (buf_hdr_slice, buf_remain) = buf.split_at_mut(hdrsz);
        buf_hdr_slice.copy_from_slice(hdr_u8_slice);

        let ndatablk = self.hdr.s_ndatablk as usize;
        if bufsz < hdrsz + ndatablk * SEGMENT_SUMMARY_BLOCK_ENTRY_SIZE {
            panic!("failed to write segment sum to buf, buf len {} < hdr size {} + num of blk idx {}, it's too small", bufsz, hdrsz, ndatablk);
        }

        let blocks_slice = unsafe {
            std::mem::transmute::<&mut [u8], &mut [SegmentBlockEntryRaw]>(buf_remain)
        };
        for (i, entry) in self.blocks.iter().enumerate() {
            blocks_slice[i].e_blkidx = entry.blkidx;
            blocks_slice[i].e_blkptr = entry.blkptr.into();
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
    #[cfg(not(feature = "wal"))]
    data: Vec<u8>,
    #[cfg(feature = "wal")]
    data: Arc<Pin<Box<Vec<u8>>>>,
    offset: usize,
    segid: SegmentId,
    ss: SegmentSum,
}

// router stub to real impl of writer function in Staging
impl<T: SegmentReadWrite> Writer<T> {
    pub fn new(ctx: T, buf_size: usize, segid: SegmentId, hyper_file_config: &HyperFileMetaConfig) -> Self {
        let data = Vec::with_capacity(buf_size);

        let mut hdr = SegmentHeader::new();
        hdr.s_meta_blk_shift = hyper_file_config.meta_block_size.checked_ilog2().unwrap() as u8;
        hdr.s_data_blk_shift = hyper_file_config.data_block_size.checked_ilog2().unwrap() as u8;
        hdr.s_next = 0;
        hdr.s_ino = 0;
        hdr.s_cno = segid;

        Self {
            ctx: ctx,
            #[cfg(not(feature = "wal"))]
            data: data,
            #[cfg(feature = "wal")]
            data: Arc::new(Box::pin(data)),
            offset: 0,
            segid: segid,
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

        self.data.resize(ss_bytes, 0);
        self.ss.write_to(&mut self.data);

        // calc 4KiB aligned bytes from real size of ss
        let ss_aligned_bytes = (ss_bytes + 4096 - 1) >> 12 << 12;
        // extend current ss to aligned size
        self.data.resize(ss_aligned_bytes, 0);

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
        let slice_start = {

        self.data.resize(self.offset + len, 0);
        let slice_start = &mut self.data[self.offset..];
        slice_start

        };
        #[cfg(feature = "wal")]
        let slice_start = {

        let Some(data) = Arc::get_mut(&mut self.data) else {
            panic!("failed to get back inner data buffer during segment build");
        };
        data.resize(self.offset + len, 0);
        let slice_start = &mut data[self.offset..];
        slice_start

        };

        let (data, _) = slice_start.split_at_mut(len);
        data.copy_from_slice(buf);
        self.offset += len;
        self.ctx.append(self.segid, buf)
    }

    #[cfg(feature = "concurrent-segment-build")]
    pub fn spawn_append(&mut self, chunk: Vec<&DataBlock>) -> Result<tokio::task::JoinHandle<()>> {
        let len = chunk.iter().map(|block| block.size()).sum();
        #[cfg(not(feature = "wal"))]
        let slice_start = {

        self.data.resize(self.offset + len, 0);
        let slice_start = &mut self.data[self.offset..];
        slice_start

        };
        #[cfg(feature = "wal")]
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
        // call of self.ctx.append(self.segid, buf) is removed
        Ok(join)
    }

    pub async fn done(self) -> Result<()> {
        let _ = self.ctx.done(self.segid, &self.data, self.offset).await?;
        #[cfg(feature = "wal")]
        // force a strong count to keep data's life until wal_clear_mem_segment()
        unsafe { Arc::increment_strong_count(Arc::as_ptr(&self.data)) };
        Ok(())
    }

    #[cfg(feature = "wal")]
    pub fn get_weak_data(&self) -> (SegmentId, Weak<Pin<Box<Vec<u8>>>>) {
        (self.segid, Arc::downgrade(&self.data))
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
