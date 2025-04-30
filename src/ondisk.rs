// "HFSS"
const DEFAULT_SS_MAGIC: u32 = 0x48465353;
const DEFAULT_INODE_BMAP_SIZE: usize = 56;

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
