use crate::config::DEFAULT_ROOT_SIZE;
// "HFSS"
const DEFAULT_SS_MAGIC: u32 = 0x48465353;
const DEFAULT_INODE_BMAP_SIZE: usize = DEFAULT_ROOT_SIZE;

pub type BMapRawType = [u8; DEFAULT_INODE_BMAP_SIZE];

#[derive(Debug, Clone, Copy)]
#[repr(C, align(8))]
pub struct InodeRaw {
    pub i_ino: u64,
    pub i_blocks: u64,
    pub i_size: u64,
    pub i_atime: u64,
    pub i_ctime: u64,
    pub i_mtime: u64,
    pub i_atime_nsec: u32,
    pub i_ctime_nsec: u32,
    pub i_mtime_nsec: u32,
    pub i_meta_config: u32,
    pub i_uid: u32,
    pub i_gid: u32,
    pub i_mode: u32,
    pub i_flags: u32,
    pub i_nlink: u64,
    pub i_last_seq: u64,
    pub i_last_cno: u64,
    pub i_bmap: BMapRawType,
}

impl Default for InodeRaw {
    fn default() -> Self {
        Self {
            i_ino: 0,
            i_blocks: 0,
            i_size: 0,
            i_atime: 0,
            i_ctime: 0,
            i_mtime: 0,
            i_atime_nsec: 0,
            i_ctime_nsec: 0,
            i_mtime_nsec: 0,
            i_meta_config: 0,
            i_uid: 0,
            i_gid: 0,
            i_mode: 0,
            i_flags: 0,
            i_nlink: 0,
            i_last_seq: 0,
            i_last_cno: 0,
            i_bmap: [0u8; DEFAULT_INODE_BMAP_SIZE],
        }
    }
}

impl InodeRaw {
    pub fn as_mut_u8_slice(&mut self) -> &mut [u8] {
        unsafe {
            std::slice::from_raw_parts_mut(
                (self as *mut Self) as *mut u8,
                std::mem::size_of::<Self>()
            )
        }
    }

    pub fn as_u8_slice(&self) -> &[u8] {
        unsafe {
            std::slice::from_raw_parts(
                (self as *const Self) as *const u8,
                std::mem::size_of::<Self>()
            )
        }
    }

    pub fn from_u8_slice(buf: &[u8]) -> Self {
        let mut raw: InodeRaw = unsafe { std::mem::MaybeUninit::zeroed().assume_init() };
        raw.as_mut_u8_slice().copy_from_slice(buf);
        raw
    }
}

#[derive(Debug, Clone, Copy)]
#[repr(C, align(8))]
pub(crate) struct SegmentBlockEntryRaw {
    pub(crate) e_blkidx: u64,
    pub(crate) e_blkptr: u64,
}

impl SegmentBlockEntryRaw {
    pub(crate) fn new() -> Self {
        Self { e_blkidx: 0, e_blkptr: 0 }
    }
}

#[derive(Debug, Clone, Copy)]
#[repr(C, align(8))]
pub struct SegmentHeader {
    pub s_magic: u32,       // magic number
    pub s_chksum: u32,      // checksum of this segment

    pub s_bytes: u32,       // size of seg sum header + block index array
    pub s_flags: u16,       // all flags
    pub s_meta_blk_shift: u8,   // block size of meta node
    pub s_data_blk_shift: u8,   // block size of data node

    pub s_next: u64,        // next segment id

    // node
    pub s_inode: InodeRaw,

    //  entry desccription
    pub s_ino: u64,         // ino of this file
    pub s_cno: u64,         // cno of this file
    pub s_nmetablk: u32,    // total meta blocks
    pub s_ndatablk: u32,    // total data blocks
    s_blocks: [SegmentBlockEntryRaw; 0], // array of block index for data blocks
}

impl SegmentHeader {
    pub fn new() -> Self {
        Self {
            s_magic: DEFAULT_SS_MAGIC,
            s_chksum: 0,
            s_bytes: 0,
            s_flags: 0,
            s_meta_blk_shift: 0,
            s_data_blk_shift: 0,
            s_next: 0,
            s_inode: unsafe { std::mem::MaybeUninit::zeroed().assume_init() },
            s_ino: 0,
            s_cno: 0,
            s_nmetablk: 0,
            s_ndatablk: 0,
            s_blocks: [SegmentBlockEntryRaw::new(); 0],
        }
    }

    pub fn from_slice(buf: &[u8]) -> Self {
        let hdrsz = std::mem::size_of::<Self>();
        let bufsz = buf.len();
        if bufsz != hdrsz {
            panic!("failed to segment header from buf, buf len {} != hdr size {}", bufsz, hdrsz);
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
        let (buf_hdr_slice, _buf_remain) = buf.split_at(hdrsz);
        hdr_u8_slice.copy_from_slice(buf_hdr_slice);
        hdr
    }

    #[inline]
    pub fn size() -> usize {
        std::mem::size_of::<Self>()
    }

    // return aligned segment summary bytes boundary
    #[inline]
    pub fn aligned_ss_bytes(&self) -> usize {
        (self.s_bytes as usize + 4096 - 1) >> 12 << 12
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // --- InodeRaw ---

    #[test]
    fn inode_raw_size() {
        // InodeRaw is #[repr(C, align(8))] — verify it stays stable
        assert_eq!(std::mem::size_of::<InodeRaw>(), 160);
    }

    #[test]
    fn inode_raw_round_trip_via_slice() {
        let mut raw = InodeRaw::default();
        raw.i_ino = 42;
        raw.i_size = 1024 * 1024;
        raw.i_blocks = 2048;
        raw.i_uid = 1000;
        raw.i_gid = 1000;
        raw.i_mode = 0o100644;
        raw.i_last_seq = 99;
        raw.i_last_cno = 99;
        raw.i_bmap[0] = 0xAB;
        raw.i_bmap[55] = 0xCD;

        let bytes = raw.as_u8_slice().to_vec();
        let restored = InodeRaw::from_u8_slice(&bytes);

        assert_eq!(restored.i_ino, 42);
        assert_eq!(restored.i_size, 1024 * 1024);
        assert_eq!(restored.i_blocks, 2048);
        assert_eq!(restored.i_uid, 1000);
        assert_eq!(restored.i_gid, 1000);
        assert_eq!(restored.i_mode, 0o100644);
        assert_eq!(restored.i_last_seq, 99);
        assert_eq!(restored.i_last_cno, 99);
        assert_eq!(restored.i_bmap[0], 0xAB);
        assert_eq!(restored.i_bmap[55], 0xCD);
    }

    #[test]
    fn inode_raw_as_mut_u8_slice() {
        let mut raw = InodeRaw::default();
        let slice = raw.as_mut_u8_slice();
        assert_eq!(slice.len(), std::mem::size_of::<InodeRaw>());
        // writing to the slice should modify the struct
        slice[0] = 0xFF;
        assert_eq!(raw.as_u8_slice()[0], 0xFF);
    }

    #[test]
    fn inode_raw_default_is_zeroed() {
        let raw = InodeRaw::default();
        assert!(raw.as_u8_slice().iter().all(|&b| b == 0));
    }

    // --- SegmentHeader ---

    #[test]
    fn segment_header_new_has_magic() {
        let hdr = SegmentHeader::new();
        assert_eq!(hdr.s_magic, 0x48465353); // "HFSS"
    }

    #[test]
    fn segment_header_aligned_ss_bytes() {
        let mut hdr = SegmentHeader::new();
        hdr.s_bytes = 100; // less than 4096
        assert_eq!(hdr.aligned_ss_bytes(), 4096);
        hdr.s_bytes = 4096;
        assert_eq!(hdr.aligned_ss_bytes(), 4096);
        hdr.s_bytes = 4097;
        assert_eq!(hdr.aligned_ss_bytes(), 8192);
    }

    // --- SegmentBlockEntryRaw ---

    #[test]
    fn segment_block_entry_size() {
        assert_eq!(std::mem::size_of::<SegmentBlockEntryRaw>(), 16);
    }
}
