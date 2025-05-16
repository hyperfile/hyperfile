use std::fmt;
use std::time::SystemTime;
use chrono::{Utc, TimeZone};
use crate::SegmentId;
use crate::ondisk::{InodeRaw, BMapRawType};
use crate::config::HyperFileMetaConfig;

pub struct Stat(pub libc::stat);

impl fmt::Display for Stat {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(f, "==== dump Stat ino: {} ====", self.0.st_ino)?;
        writeln!(f, "  file size: {}, blocks: {}", self.0.st_size, self.0.st_blocks)?;
        writeln!(f, "  uid: {}, gid: {}, mode: {}, nlink: {}, dev: {}, rdev: {}",
            self.0.st_uid, self.0.st_gid, self.0.st_mode, self.0.st_nlink, self.0.st_dev, self.0.st_rdev)?;
        let dt_atime = Utc.timestamp_opt(self.0.st_atime as i64, self.0.st_atime_nsec as u32);
        let dt_ctime = Utc.timestamp_opt(self.0.st_ctime as i64, self.0.st_ctime_nsec as u32);
        let dt_mtime = Utc.timestamp_opt(self.0.st_mtime as i64, self.0.st_mtime_nsec as u32);
        writeln!(f, "  access time: {:?}", dt_atime.unwrap())?;
        writeln!(f, "  change time: {:?}", dt_ctime.unwrap())?;
        writeln!(f, "  modify time: {:?}", dt_mtime.unwrap())
    }
}

#[derive(Default, Debug)]
pub struct OnDiskState {
    pub checksum: String,
    pub timestamp: i64,
}

#[derive(Default, Debug)]
pub struct Inode {
    i_ino: u64,
    i_blocks: u64,
    i_size: u64,
    i_atime: u64,
    i_ctime: u64,
    i_mtime: u64,
    i_atime_nsec: u32,
    i_ctime_nsec: u32,
    i_mtime_nsec: u32,
    i_meta_config: u32,
    i_uid: u32,
    i_gid: u32,
    i_mode: u32,
    i_flags: u32,
    i_nlink: u64,
    pub(crate) i_last_seq: SegmentId,
    pub(crate) i_last_cno: u64,
    // in memory only fields
    pub(crate) i_last_ondisk_cno: u64, // tracking last cno ondisk
    pub(crate) i_ondisk_state: Option<OnDiskState>,
    pub(crate) i_attr_dirty: bool, // tracking any of attr modified
}

impl fmt::Display for Inode {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(f, "==== dump Inode ino: {} attr dirty: {}====", self.i_ino, self.i_attr_dirty)?;
        if let Some(od_state) = &self.i_ondisk_state {
            writeln!(f, "  ondisk checksum: {}, ondisk timestamp: {}", od_state.checksum, od_state.timestamp)?;
        } else {
            writeln!(f, "  ondisk checksum: -, ondisk timestamp: -")?;
        }
        writeln!(f, "  file size: {}, blocks: {}", self.i_size, self.i_blocks)?;
        writeln!(f, "  uid: {}, gid: {}, mode: {}, flags: {}, nlink: {}",
            self.i_uid, self.i_gid, self.i_mode, self.i_flags, self.i_nlink)?;
        let dt_atime = Utc.timestamp_opt(self.i_atime as i64, self.i_atime_nsec);
        let dt_ctime = Utc.timestamp_opt(self.i_ctime as i64, self.i_ctime_nsec);
        let dt_mtime = Utc.timestamp_opt(self.i_mtime as i64, self.i_mtime_nsec);
        writeln!(f, "  access time: {:?}", dt_atime.unwrap())?;
        writeln!(f, "  change time: {:?}", dt_ctime.unwrap())?;
        writeln!(f, "  modify time: {:?}", dt_mtime.unwrap())?;
        let meta_config = HyperFileMetaConfig::from_u32(self.i_meta_config);
        writeln!(f, "  {:?}, root size: {}, meta block size: {}, data block size: {}",
            meta_config.block_ptr_format, meta_config.root_size,
            meta_config.meta_block_size, meta_config.data_block_size)?;
        writeln!(f, "  last seq: {}, last cno: {}, last ondisk cno: {}",
            self.i_last_seq, self.i_last_cno, self.i_last_ondisk_cno)
    }
}

impl Inode {
    // get timestamp for now
    // return (sec, nsec)
    #[inline]
    fn get_now() -> (u64, u32) {
        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap();
        (now.as_secs(), now.subsec_nanos())
    }

    #[inline]
    fn update_ctime(&mut self) {
        let (sec, nsec) = Self::get_now();
        self.i_ctime = sec;
        self.i_ctime_nsec = nsec;
        self.i_attr_dirty = true;
    }

    #[inline]
    pub fn update_atime(&mut self) {
        let (sec, nsec) = Self::get_now();
        self.i_atime = sec;
        self.i_atime_nsec = nsec;
        self.i_attr_dirty = true;
    }

    #[inline]
    pub fn update_mtime(&mut self) {
        let (sec, nsec) = Self::get_now();
        self.i_mtime = sec;
        self.i_mtime_nsec = nsec;
        self.i_attr_dirty = true;
    }

    pub fn default_dir() -> Self {
        let mut inode = Self::default();
        inode.i_meta_config = HyperFileMetaConfig::default().as_u32();
        inode.i_mode = libc::S_IFDIR;
        inode.i_uid = 1000;
        inode.i_gid = 1000;
        inode.i_nlink = 1;
        inode.update_ctime();
        inode
    }

    pub fn default_file() -> Self {
        let mut inode = Self::default();
        inode.i_meta_config = HyperFileMetaConfig::default().as_u32();
        inode.i_mode = libc::S_IFREG;
        inode.i_uid = 1000;
        inode.i_gid = 1000;
        inode.i_nlink = 1;
        inode.update_ctime();
        inode
    }

    pub fn with_meta_config(mut self, meta_config: &HyperFileMetaConfig) -> Self {
        self.i_meta_config = meta_config.as_u32();
        self
    }

    pub fn is_attr_dirty(&self) -> bool {
        self.i_attr_dirty
    }

    pub fn clear_attr_dirty(&mut self) {
        self.i_attr_dirty = false
    }

    pub fn clear_attr_dirty_unsafe(&self) {
        let inode = std::ptr::addr_of!(*self) as *mut Inode;
        unsafe {
            (*inode).clear_attr_dirty();
        }
    }

    pub fn from_origin(origin_size: usize, meta_config: &HyperFileMetaConfig) -> Self {
        let mut inode = Self::default_file();
        inode.i_meta_config = meta_config.as_u32();
        inode.i_blocks = (origin_size / 512) as u64;
        inode.i_size = origin_size as u64;
        inode
    }

    pub fn from_raw(raw: &InodeRaw, od_state: Option<OnDiskState>) -> Self {
        Self {
            i_ino: raw.i_ino,
            i_blocks: raw.i_blocks,
            i_size: raw.i_size,
            i_atime: raw.i_atime,
            i_ctime: raw.i_ctime,
            i_mtime: raw.i_mtime,
            i_atime_nsec: raw.i_atime_nsec,
            i_ctime_nsec: raw.i_ctime_nsec,
            i_mtime_nsec: raw.i_mtime_nsec,
            i_meta_config: raw.i_meta_config,
            i_uid: raw.i_uid,
            i_gid: raw.i_gid,
            i_mode: raw.i_mode,
            i_flags: raw.i_flags,
            i_nlink: raw.i_nlink,
            i_last_seq: raw.i_last_seq,
            i_last_cno: raw.i_last_cno,
            i_last_ondisk_cno: raw.i_last_cno,
            i_ondisk_state: od_state,
            i_attr_dirty: false,
        }
    }

    pub fn to_raw(&self, bmap: BMapRawType) -> InodeRaw {
        InodeRaw {
            i_ino: self.i_ino,
            i_blocks: self.i_blocks,
            i_size: self.i_size,
            i_atime: self.i_atime,
            i_ctime: self.i_ctime,
            i_mtime: self.i_mtime,
            i_atime_nsec: self.i_atime_nsec,
            i_ctime_nsec: self.i_ctime_nsec,
            i_mtime_nsec: self.i_mtime_nsec,
            i_meta_config: self.i_meta_config,
            i_uid: self.i_uid,
            i_gid: self.i_gid,
            i_mode: self.i_mode,
            i_flags: self.i_flags,
            i_nlink: self.i_nlink,
            i_last_seq: self.i_last_seq,
            i_last_cno: self.i_last_cno,
            i_bmap: bmap,
        }
    }

    pub fn to_stat(&self, dev: u64, rdev: u64, blksize: usize) -> libc::stat {
        let mut stat: libc::stat = unsafe { std::mem::MaybeUninit::zeroed().assume_init() };
        stat.st_dev = dev;
        stat.st_ino = self.i_ino;
        stat.st_nlink = self.i_nlink;
        stat.st_mode = self.i_mode;
        stat.st_uid = self.i_uid;
        stat.st_gid = self.i_gid;
        stat.st_rdev = rdev;
        stat.st_size = self.i_size as i64;
        stat.st_blksize = blksize as i64;
        stat.st_blocks = self.i_blocks as i64;
        stat.st_atime = self.i_atime as i64;
        stat.st_atime_nsec = self.i_atime_nsec as i64;
        stat.st_mtime = self.i_mtime as i64;
        stat.st_mtime_nsec = self.i_mtime_nsec as i64;
        stat.st_ctime = self.i_ctime as i64;
        stat.st_ctime_nsec = self.i_ctime_nsec as i64;
        stat
    }

    pub fn size(&self) -> usize {
        self.i_size as usize
    }

    // for non-sparse file
    pub fn set_size(&mut self, size: usize) {
        self.i_size = size as u64;
        self.i_blocks = ((size + 511) / 512) as u64;
        self.i_attr_dirty = true;
    }

    // for sparse file:
    //   - diff: size diff in bytes to update
    pub fn update_blocks(&mut self, diff: isize) {
        let blocks_diff = ((diff.abs() + 511) / 512) as u64;
        if diff > 0 {
            self.i_blocks += blocks_diff;
            self.i_attr_dirty = true;
        } else if diff < 0 {
            self.i_blocks -= blocks_diff;
            self.i_attr_dirty = true;
        }
    }

    // for sparse file:
    //   - size: max size of file
    pub fn extend_size(&mut self, size: usize) {
        self.i_size = size as u64;
        self.i_attr_dirty = true;
    }

    pub fn get_next_seq(&mut self) -> SegmentId {
        self.i_last_seq += 1;
        self.i_last_seq
    }

    pub fn get_last_seq(&self) -> SegmentId {
        self.i_last_seq
    }

    #[inline]
    pub fn get_last_cno(&self) -> u64 {
        self.i_last_cno
    }

    #[inline]
    pub fn set_last_cno(&mut self, cno: u64) {
        self.i_last_cno = cno;
    }

    #[inline]
    pub fn get_last_ondisk_cno(&self) -> u64 {
        self.i_last_ondisk_cno
    }

    #[inline]
    pub fn set_last_ondisk_cno(&mut self, cno: u64) {
        self.i_last_ondisk_cno = cno;
    }

    #[inline]
    pub fn is_flushing(&self) -> bool {
        if self.i_last_cno > self.i_last_ondisk_cno {
            return true;
        }
        false
    }

    pub fn get_ondisk_state(&self) -> &Option<OnDiskState> {
        &self.i_ondisk_state
    }

    pub fn set_ondisk_state(&mut self, od_state: Option<OnDiskState>) {
        self.i_ondisk_state = od_state;
    }
}

#[derive(PartialEq, Debug, Clone)]
#[repr(u8)]
pub enum FlushInodeFlag {
    Ignore = 0,
    Create = 1,
    Update = 2,
    Delete = 3,
    Unkown = 255,
}
