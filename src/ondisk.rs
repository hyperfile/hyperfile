use crate::config::DEFAULT_ROOT_SIZE;
// "HFSS"
const DEFAULT_SS_MAGIC: u32 = 0x48465353;
const DEFAULT_INODE_BMAP_SIZE: usize = DEFAULT_ROOT_SIZE;

pub type BMapRawType = [u8; DEFAULT_INODE_BMAP_SIZE];

#[derive(Debug, Clone, Copy)]
#[repr(C, align(8))]
pub struct InodeRaw {
    pub i_ino: u64,
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
    // The tail below (i_blocks, i_last_seq, i_last_cno, i_bmap) is laid out
    // contiguously so that, for an inlined small file (no data blocks / no
    // segments), the 80 bytes from `i_blocks` through `i_bmap` can be reused as
    // an inline-data payload (with `i_size` above as its length). See the INLINE
    // inode flag.
    pub i_size: u64,
    pub i_blocks: u64,
    pub i_last_seq: u64,
    pub i_last_cno: u64,
    pub i_bmap: BMapRawType,
}

impl Default for InodeRaw {
    fn default() -> Self {
        Self {
            i_ino: 0,
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
            i_size: 0,
            i_blocks: 0,
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

    /// `i_flags` bit marking a fully-inlined small file: its bytes live in this
    /// inode's tail region (`i_blocks`..`i_bmap`) instead of in data blocks, and
    /// the file has NO separate `FILE/<uuid>` object. `i_size` is the length.
    pub const FLAG_INLINE: u32 = 0x1;

    /// `i_flags` bit marking a file that opts in to **cross-mount
    /// open-but-unlinked** semantics: while any mount holds it open (tracked by
    /// an S3 open-lease), an unlink from another mount silly-renames it instead
    /// of tombstoning, so its storage survives until the last close. Set via the
    /// `user.hyperfs.keep_open` xattr; off by default (no extra coordination
    /// cost). Independent of `FLAG_INLINE`.
    pub const FLAG_KEEP_OPEN: u32 = 0x2;

    /// True if this inode opts in to cross-mount open-but-unlinked (see
    /// [`FLAG_KEEP_OPEN`]).
    pub fn is_keep_open(&self) -> bool { self.i_flags & Self::FLAG_KEEP_OPEN != 0 }

    /// Set/clear [`FLAG_KEEP_OPEN`].
    pub fn set_keep_open(&mut self, on: bool) {
        if on { self.i_flags |= Self::FLAG_KEEP_OPEN; } else { self.i_flags &= !Self::FLAG_KEEP_OPEN; }
    }

    /// Byte offset of the inline-data region within the raw inode (the start of
    /// the contiguous tail `i_blocks, i_last_seq, i_last_cno, i_bmap`).
    pub const fn inline_offset() -> usize { std::mem::offset_of!(InodeRaw, i_blocks) }

    /// Maximum inline payload (the size of that tail region).
    pub const fn inline_cap() -> usize { std::mem::size_of::<InodeRaw>() - Self::inline_offset() }

    /// True if this inode is a fully-inlined small file.
    pub fn is_inline(&self) -> bool { self.i_flags & Self::FLAG_INLINE != 0 }

    /// The inlined bytes (`i_size` of them); empty if not inlined.
    pub fn inline_data(&self) -> &[u8] {
        if !self.is_inline() { return &[]; }
        let off = Self::inline_offset();
        let n = (self.i_size as usize).min(Self::inline_cap());
        &self.as_u8_slice()[off..off + n]
    }

    /// Mark this inode inline and store `data` (must fit `inline_cap()`):
    /// sets the flag, `i_size = data.len()`, zeroes the tail region, copies in.
    /// Caller is responsible for there being no `FILE/<uuid>` object/segments.
    pub fn set_inline(&mut self, data: &[u8]) {
        debug_assert!(data.len() <= Self::inline_cap());
        let off = Self::inline_offset();
        self.i_flags |= Self::FLAG_INLINE;
        self.i_blocks = 0;
        self.i_last_seq = 0;
        self.i_last_cno = 0;
        self.i_size = data.len() as u64;
        let buf = self.as_mut_u8_slice();
        buf[off..].fill(0);
        buf[off..off + data.len()].copy_from_slice(data);
    }

    /// Clear the inline flag (e.g. when spilling to a real segmented file). Does
    /// not touch the tail region; the caller rebuilds the bmap.
    pub fn clear_inline(&mut self) { self.i_flags &= !Self::FLAG_INLINE; }

    /// Persist a char/block device node's `rdev`. A device node has no data
    /// segments, so the otherwise-unused `i_last_cno` slot holds it. Call after
    /// `set_inline(&[])` (which zeroes the tail). `Inode::to_stat` reads it back
    /// for device-mode inodes.
    pub fn set_rdev(&mut self, rdev: u64) { self.i_last_cno = rdev; }

    /// The persisted device `rdev` (see [`set_rdev`]); meaningful only for a
    /// char/block device-mode inode.
    pub fn rdev(&self) -> u64 { self.i_last_cno }
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

    /// Serialize this entry into a 16-byte buffer using native endian.
    /// Panics if `buf.len() < 16`.
    ///
    /// The on-disk format matches the memory layout of the
    /// `#[repr(C, align(8))]` struct (two `u64` fields in sequence).
    pub(crate) fn write_to(&self, buf: &mut [u8]) {
        const SIZE: usize = std::mem::size_of::<SegmentBlockEntryRaw>();
        assert!(buf.len() >= SIZE,
            "SegmentBlockEntryRaw::write_to buf len {} < {}", buf.len(), SIZE);
        buf[0..8].copy_from_slice(&self.e_blkidx.to_ne_bytes());
        buf[8..16].copy_from_slice(&self.e_blkptr.to_ne_bytes());
    }

    /// Deserialize a 16-byte buffer into an entry.
    /// Panics if `buf.len() < 16`.
    pub(crate) fn read_from(buf: &[u8]) -> Self {
        const SIZE: usize = std::mem::size_of::<SegmentBlockEntryRaw>();
        assert!(buf.len() >= SIZE,
            "SegmentBlockEntryRaw::read_from buf len {} < {}", buf.len(), SIZE);
        let mut idx_bytes = [0u8; 8];
        let mut ptr_bytes = [0u8; 8];
        idx_bytes.copy_from_slice(&buf[0..8]);
        ptr_bytes.copy_from_slice(&buf[8..16]);
        Self {
            e_blkidx: u64::from_ne_bytes(idx_bytes),
            e_blkptr: u64::from_ne_bytes(ptr_bytes),
        }
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
        Self::read_from(buf)
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

    /// Serialize this header to a byte buffer. The on-disk layout matches
    /// the memory representation of this `#[repr(C, align(8))]` struct
    /// (including the `s_inode: InodeRaw` nested struct).
    ///
    /// Panics if `buf.len()` is smaller than `SegmentHeader::size()`.
    pub fn write_to(&self, buf: &mut [u8]) {
        let sz = Self::size();
        assert!(buf.len() >= sz,
            "SegmentHeader::write_to buf len {} < {}", buf.len(), sz);
        // SAFETY: `self` is a fully initialized `#[repr(C)]` value with
        // `Copy` semantics. Reading its bytes through a `*const u8` read
        // does not observe any non-`Copy` state. Padding bytes in the
        // struct may have indeterminate values but are still legal to
        // read as `u8` (reading uninit bytes as `u8` is defined behavior
        // since Rust 1.75).
        let src = unsafe {
            std::slice::from_raw_parts(
                std::ptr::addr_of!(*self) as *const u8,
                sz,
            )
        };
        buf[..sz].copy_from_slice(src);
    }

    /// Deserialize a header from a byte buffer. The buffer may be larger
    /// than `SegmentHeader::size()`; only the first `size()` bytes are read.
    /// Panics if `buf.len()` is smaller than `SegmentHeader::size()`.
    pub fn read_from(buf: &[u8]) -> Self {
        let sz = Self::size();
        assert!(buf.len() >= sz,
            "SegmentHeader::read_from buf len {} < {}", buf.len(), sz);
        // SAFETY: `SegmentHeader` is `#[repr(C, align(8))]` and contains
        // only `Pod`-like fields (ints and another `#[repr(C)]` struct of
        // the same shape). Zero-initialized memory is a valid bit pattern
        // for every field. The subsequent `copy_from_slice` overwrites
        // every byte before any field is read.
        let mut hdr: SegmentHeader = unsafe {
            std::mem::MaybeUninit::zeroed().assume_init()
        };
        // SAFETY: `hdr` is a local value we own; writing to its backing
        // bytes does not alias anything. `sz` bytes equals the struct size,
        // so the write stays within the struct.
        let dst = unsafe {
            std::slice::from_raw_parts_mut(
                std::ptr::addr_of_mut!(hdr) as *mut u8,
                sz,
            )
        };
        dst.copy_from_slice(&buf[..sz]);
        hdr
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
    fn inode_inline_region() {
        // The inline tail (i_blocks..i_bmap) starts at 80 and spans 80 bytes.
        assert_eq!(InodeRaw::inline_offset(), 80);
        assert_eq!(InodeRaw::inline_cap(), 80);

        let mut raw = InodeRaw::default();
        assert!(!raw.is_inline());
        assert_eq!(raw.inline_data(), b"");

        let data = b"hello inline world";
        raw.set_inline(data);
        assert!(raw.is_inline());
        assert_eq!(raw.i_size as usize, data.len());
        assert_eq!(raw.inline_data(), data);

        // round-trips through the raw byte serialization
        let raw2 = InodeRaw::from_u8_slice(raw.as_u8_slice());
        assert!(raw2.is_inline());
        assert_eq!(raw2.inline_data(), data);

        // full-capacity payload
        let full = vec![0xABu8; InodeRaw::inline_cap()];
        raw.set_inline(&full);
        assert_eq!(raw.inline_data(), &full[..]);

        raw.clear_inline();
        assert!(!raw.is_inline());
    }

    #[test]
    fn device_node_rdev_round_trips() {
        // A char device node: dataless (inline, i_size 0) with rdev persisted in
        // the otherwise-unused i_last_cno slot.
        let rdev: u64 = 0x1234_5678;
        let mut raw = InodeRaw::default();
        raw.i_mode = libc::S_IFCHR | 0o644;
        raw.set_inline(&[]);   // zeroes the tail, sets FLAG_INLINE, i_size = 0
        raw.set_rdev(rdev);
        assert_eq!(raw.rdev(), rdev);
        assert_eq!(raw.i_size, 0, "device node has no size");
        assert_eq!(raw.inline_data(), b"", "no inline data");

        // survives raw byte serialization (scatter/bmap)
        let raw2 = InodeRaw::from_u8_slice(raw.as_u8_slice());
        assert_eq!(raw2.rdev(), rdev);

        // to_stat surfaces rdev for a device-mode inode (ignoring the param),
        // and 0 for a non-device inode.
        let st = crate::inode::Inode::from_raw(&raw2, None).to_stat(0, 0);
        assert_eq!(st.st_rdev, rdev, "char device rdev in stat");
        assert_eq!(st.st_mode & libc::S_IFMT, libc::S_IFCHR);
        assert_eq!(st.st_size, 0);

        let mut reg = InodeRaw::default();
        reg.i_mode = libc::S_IFREG | 0o644;
        reg.set_rdev(rdev); // i_last_cno set, but mode isn't a device
        let streg = crate::inode::Inode::from_raw(&reg, None).to_stat(0, 7);
        assert_eq!(streg.st_rdev, 7, "non-device inode uses the passed rdev, not i_last_cno");
    }

    #[test]
    fn keep_open_flag_round_trips() {
        let mut raw = InodeRaw::default();
        assert!(!raw.is_keep_open());
        raw.set_keep_open(true);
        assert!(raw.is_keep_open());
        assert_eq!(raw.i_flags & InodeRaw::FLAG_KEEP_OPEN, InodeRaw::FLAG_KEEP_OPEN);
        // independent of the inline flag
        raw.set_inline(b"x");
        assert!(raw.is_keep_open() && raw.is_inline());
        // survives serialization + Inode round-trip
        let raw2 = InodeRaw::from_u8_slice(raw.as_u8_slice());
        assert!(raw2.is_keep_open());
        assert!(crate::inode::Inode::from_raw(&raw2, None).is_keep_open());
        // clears cleanly without touching the inline flag
        let mut r3 = raw2;
        r3.set_keep_open(false);
        assert!(!r3.is_keep_open() && r3.is_inline());
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

    #[test]
    fn segment_block_entry_round_trip() {
        let entry = SegmentBlockEntryRaw { e_blkidx: 0xDEAD_BEEF_CAFE_BABE, e_blkptr: 0x1234_5678_9ABC_DEF0 };
        let mut buf = [0u8; 16];
        entry.write_to(&mut buf);
        let decoded = SegmentBlockEntryRaw::read_from(&buf);
        assert_eq!(decoded.e_blkidx, entry.e_blkidx);
        assert_eq!(decoded.e_blkptr, entry.e_blkptr);
    }

    #[test]
    fn segment_block_entry_round_trip_zero() {
        let entry = SegmentBlockEntryRaw::new();
        let mut buf = [0u8; 16];
        entry.write_to(&mut buf);
        let decoded = SegmentBlockEntryRaw::read_from(&buf);
        assert_eq!(decoded.e_blkidx, 0);
        assert_eq!(decoded.e_blkptr, 0);
    }

    #[test]
    fn segment_block_entry_write_accepts_oversized_buf() {
        let entry = SegmentBlockEntryRaw { e_blkidx: 7, e_blkptr: 42 };
        let mut buf = [0u8; 32]; // bigger than 16 is fine
        entry.write_to(&mut buf);
        assert_eq!(&buf[16..], &[0u8; 16]); // tail untouched
    }

    #[test]
    #[should_panic(expected = "SegmentBlockEntryRaw::write_to buf len")]
    fn segment_block_entry_write_panics_on_short_buf() {
        let entry = SegmentBlockEntryRaw::new();
        let mut buf = [0u8; 10];
        entry.write_to(&mut buf);
    }

    #[test]
    #[should_panic(expected = "SegmentBlockEntryRaw::read_from buf len")]
    fn segment_block_entry_read_panics_on_short_buf() {
        SegmentBlockEntryRaw::read_from(&[0u8; 10]);
    }

    // --- SegmentHeader write_to / read_from ---

    #[test]
    fn segment_header_write_read_round_trip() {
        let mut hdr = SegmentHeader::new();
        hdr.s_cno = 42;
        hdr.s_ino = 7;
        hdr.s_nmetablk = 3;
        hdr.s_ndatablk = 5;
        hdr.s_meta_blk_shift = 12;
        hdr.s_data_blk_shift = 12;
        hdr.s_next = 100;
        hdr.s_bytes = 4096;
        hdr.s_chksum = 0xABCDu32;
        hdr.s_flags = 0x42;
        hdr.s_inode.i_ino = 77;
        hdr.s_inode.i_size = 8192;

        let mut buf = vec![0u8; SegmentHeader::size()];
        hdr.write_to(&mut buf);
        let decoded = SegmentHeader::read_from(&buf);

        assert_eq!(decoded.s_cno, 42);
        assert_eq!(decoded.s_ino, 7);
        assert_eq!(decoded.s_nmetablk, 3);
        assert_eq!(decoded.s_ndatablk, 5);
        assert_eq!(decoded.s_next, 100);
        assert_eq!(decoded.s_bytes, 4096);
        assert_eq!(decoded.s_chksum, 0xABCDu32);
        assert_eq!(decoded.s_flags, 0x42);
        assert_eq!(decoded.s_magic, hdr.s_magic);
        assert_eq!(decoded.s_inode.i_ino, 77);
        assert_eq!(decoded.s_inode.i_size, 8192);
    }

    #[test]
    fn segment_header_read_accepts_oversized_buf() {
        let hdr = SegmentHeader::new();
        let mut buf = vec![0u8; SegmentHeader::size() * 2];
        hdr.write_to(&mut buf);
        // read_from should still work on the oversized buffer by reading
        // only the first `size()` bytes.
        let decoded = SegmentHeader::read_from(&buf);
        assert_eq!(decoded.s_magic, hdr.s_magic);
    }

    #[test]
    #[should_panic(expected = "SegmentHeader::write_to buf len")]
    fn segment_header_write_panics_on_short_buf() {
        let hdr = SegmentHeader::new();
        let mut buf = vec![0u8; SegmentHeader::size() - 1];
        hdr.write_to(&mut buf);
    }

    #[test]
    #[should_panic(expected = "SegmentHeader::read_from buf len")]
    fn segment_header_read_panics_on_short_buf() {
        let buf = vec![0u8; 10];
        SegmentHeader::read_from(&buf);
    }

    // --- Layout stability tests ---
    //
    // These tests pin down the on-disk byte layout so that accidental
    // field reordering or type changes are caught at test time rather
    // than producing unreadable segments in production.

    #[test]
    fn segment_header_size_is_208_no_padding() {
        // Sum of explicit field sizes:
        //   4 (s_magic) + 4 (s_chksum) + 4 (s_bytes) + 2 (s_flags)
        //   + 1 (s_meta_blk_shift) + 1 (s_data_blk_shift) + 8 (s_next)
        //   + 160 (s_inode: InodeRaw) + 8 (s_ino) + 8 (s_cno)
        //   + 4 (s_nmetablk) + 4 (s_ndatablk)
        //   + 0 (s_blocks: [_; 0])
        // = 208
        let fields_sum: usize = 4 + 4 + 4 + 2 + 1 + 1 + 8 + 160 + 8 + 8 + 4 + 4;
        assert_eq!(
            std::mem::size_of::<SegmentHeader>(),
            fields_sum,
            "SegmentHeader has padding — on-disk format may be unstable"
        );
        assert_eq!(std::mem::size_of::<SegmentHeader>(), 208);
        assert_eq!(std::mem::align_of::<SegmentHeader>(), 8);
    }

    #[test]
    fn segment_block_entry_byte_layout() {
        // Field offsets on disk must be stable:
        //   bytes 0..8:  e_blkidx (u64, native endian)
        //   bytes 8..16: e_blkptr (u64, native endian)
        let entry = SegmentBlockEntryRaw {
            e_blkidx: 0x0102_0304_0506_0708,
            e_blkptr: 0x1122_3344_5566_7788,
        };
        let mut buf = [0u8; 16];
        entry.write_to(&mut buf);
        assert_eq!(&buf[0..8], &0x0102_0304_0506_0708u64.to_ne_bytes());
        assert_eq!(&buf[8..16], &0x1122_3344_5566_7788u64.to_ne_bytes());
    }

    #[test]
    fn segment_block_entry_preserves_high_bits() {
        // BlockPtr encodes staging/location bits in the high part of the
        // u64 (bit 62 = staging flag, bits 32-61 = segid). We must not
        // silently truncate or re-order those bits.
        let entry = SegmentBlockEntryRaw {
            e_blkidx: u64::MAX,
            e_blkptr: 0xC000_0000_1234_5678, // top 2 bits set + offset
        };
        let mut buf = [0u8; 16];
        entry.write_to(&mut buf);
        let decoded = SegmentBlockEntryRaw::read_from(&buf);
        assert_eq!(decoded.e_blkidx, u64::MAX);
        assert_eq!(decoded.e_blkptr, 0xC000_0000_1234_5678);
    }

    #[test]
    fn segment_header_byte_layout_key_offsets() {
        // Verify key field offsets in the serialized layout. This doubles
        // as a change-detector: if anyone reorders fields, this test will
        // fail and point at the exact offset that shifted.
        let mut hdr = SegmentHeader::new();
        hdr.s_magic = 0x4847_4647; // "GFGH" backwards
        hdr.s_chksum = 0xDEAD_BEEF;
        hdr.s_bytes = 0x1234_5678;
        hdr.s_flags = 0xABCD;
        hdr.s_meta_blk_shift = 0x13;
        hdr.s_data_blk_shift = 0x14;
        hdr.s_next = 0x7777_7777_7777_7777;

        let mut buf = vec![0u8; SegmentHeader::size()];
        hdr.write_to(&mut buf);

        // Offsets follow #[repr(C)] layout with 8-byte alignment.
        assert_eq!(&buf[0..4], &0x4847_4647u32.to_ne_bytes(), "s_magic");
        assert_eq!(&buf[4..8], &0xDEAD_BEEFu32.to_ne_bytes(), "s_chksum");
        assert_eq!(&buf[8..12], &0x1234_5678u32.to_ne_bytes(), "s_bytes");
        assert_eq!(&buf[12..14], &0xABCDu16.to_ne_bytes(), "s_flags");
        assert_eq!(buf[14], 0x13, "s_meta_blk_shift");
        assert_eq!(buf[15], 0x14, "s_data_blk_shift");
        assert_eq!(&buf[16..24], &0x7777_7777_7777_7777u64.to_ne_bytes(), "s_next");
        // s_inode starts at offset 24 (160 bytes)
        // s_ino at offset 184, s_cno at 192, s_nmetablk at 200, s_ndatablk at 204
    }

    #[test]
    fn segment_header_inode_fields_round_trip() {
        // The nested InodeRaw takes 160 bytes inside SegmentHeader. Verify
        // that all its important fields survive the serialize/deserialize cycle.
        let mut hdr = SegmentHeader::new();
        hdr.s_inode.i_ino = 99;
        hdr.s_inode.i_size = 1_048_576;
        hdr.s_inode.i_blocks = 2048;
        hdr.s_inode.i_uid = 1001;
        hdr.s_inode.i_gid = 1002;
        hdr.s_inode.i_mode = libc::S_IFREG | 0o644;
        hdr.s_inode.i_nlink = 3;
        hdr.s_inode.i_last_seq = 42;
        hdr.s_inode.i_last_cno = 42;
        hdr.s_inode.i_bmap[0] = 0xAA;
        hdr.s_inode.i_bmap[55] = 0xBB; // last byte of bmap

        let mut buf = vec![0u8; SegmentHeader::size()];
        hdr.write_to(&mut buf);
        let decoded = SegmentHeader::read_from(&buf);

        assert_eq!(decoded.s_inode.i_ino, 99);
        assert_eq!(decoded.s_inode.i_size, 1_048_576);
        assert_eq!(decoded.s_inode.i_blocks, 2048);
        assert_eq!(decoded.s_inode.i_uid, 1001);
        assert_eq!(decoded.s_inode.i_gid, 1002);
        assert_eq!(decoded.s_inode.i_mode, libc::S_IFREG | 0o644);
        assert_eq!(decoded.s_inode.i_nlink, 3);
        assert_eq!(decoded.s_inode.i_last_seq, 42);
        assert_eq!(decoded.s_inode.i_last_cno, 42);
        assert_eq!(decoded.s_inode.i_bmap[0], 0xAA);
        assert_eq!(decoded.s_inode.i_bmap[55], 0xBB);
    }

    #[test]
    fn segment_header_write_is_idempotent() {
        // Two consecutive write_to calls on the same header must produce
        // byte-identical output. This validates that padding (if any) is
        // deterministic, since a checksum computed over `buf` must be stable.
        let mut hdr = SegmentHeader::new();
        hdr.s_cno = 7;
        hdr.s_ino = 3;
        hdr.s_bytes = 4096;
        hdr.s_inode.i_size = 1234;

        let mut buf1 = vec![0u8; SegmentHeader::size()];
        let mut buf2 = vec![0u8; SegmentHeader::size()];
        hdr.write_to(&mut buf1);
        hdr.write_to(&mut buf2);
        assert_eq!(buf1, buf2, "write_to is not byte-identical on repeat");
    }

    /// `BMap::read` wants an 8-byte-aligned buffer, and the bmap root is
    /// handed to it as `&raw_inode.i_bmap`. That reference is only aligned
    /// because `InodeRaw` is `align(8)` and `i_bmap` sits at an 8-aligned
    /// offset within it — `BMapRawType` is `[u8; N]`, whose own alignment is
    /// 1, so nothing about the field's type carries the property.
    ///
    /// Inserting a `u32` anywhere above `i_bmap` would break this silently:
    /// the code would keep compiling and keep working on x86-64 until it
    /// did not.
    #[test]
    fn inode_bmap_is_eight_byte_aligned() {
        assert_eq!(std::mem::align_of::<InodeRaw>() % 8, 0,
            "InodeRaw must stay at least 8-aligned");
        assert_eq!(std::mem::offset_of!(InodeRaw, i_bmap) % 8, 0,
            "i_bmap must sit at an 8-aligned offset, since BMap::read is handed \
             a reference to it and BMapRawType has alignment 1");

        // And the reference really is aligned, not merely computed to be.
        let raw: InodeRaw = unsafe { std::mem::MaybeUninit::zeroed().assume_init() };
        assert_eq!((&raw.i_bmap as *const _ as usize) % 8, 0);
    }
}
