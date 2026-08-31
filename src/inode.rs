use std::fmt;
use std::time::SystemTime;
use chrono::{Utc, TimeZone};
use crate::SegmentId;
use crate::ondisk::{InodeRaw, BMapRawType};
use crate::config::HyperFileMetaConfig;
use crate::file::mode::{HyperFileMode, FileMode};

pub struct Stat(pub libc::stat);

impl fmt::Display for Stat {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(f, "==== dump Stat ino: {} ====", self.0.st_ino)?;
        writeln!(f, "  file size: {}, blocks: {}", self.0.st_size, self.0.st_blocks)?;
        writeln!(f, "  uid: {}, gid: {}, mode: {:#o}, nlink: {}, dev: {}, rdev: {}",
            self.0.st_uid, self.0.st_gid, self.0.st_mode, self.0.st_nlink, self.0.st_dev, self.0.st_rdev)?;
        let dt_atime = Utc.timestamp_opt(self.0.st_atime as i64, self.0.st_atime_nsec as u32);
        let dt_ctime = Utc.timestamp_opt(self.0.st_ctime as i64, self.0.st_ctime_nsec as u32);
        let dt_mtime = Utc.timestamp_opt(self.0.st_mtime as i64, self.0.st_mtime_nsec as u32);
        writeln!(f, "  access time: {:?}", dt_atime.unwrap())?;
        writeln!(f, "  change time: {:?}", dt_ctime.unwrap())?;
        writeln!(f, "  modify time: {:?}", dt_mtime.unwrap())
    }
}

#[derive(Default, Debug, Clone)]
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
        writeln!(f, "==== dump Inode ino: {} attr dirty: {} ====", self.i_ino, self.i_attr_dirty)?;
        if let Some(od_state) = &self.i_ondisk_state {
            writeln!(f, "  ondisk checksum: {}, ondisk timestamp: {}", od_state.checksum, od_state.timestamp)?;
        } else {
            writeln!(f, "  ondisk checksum: -, ondisk timestamp: -")?;
        }
        writeln!(f, "  file size: {}, blocks: {}", self.i_size, self.i_blocks)?;
        writeln!(f, "  uid: {}, gid: {}, mode: {:#o}, flags: {}, nlink: {}",
            self.i_uid, self.i_gid, self.i_mode, self.i_flags, self.i_nlink)?;
        let dt_atime = Utc.timestamp_opt(self.i_atime as i64, self.i_atime_nsec);
        let dt_ctime = Utc.timestamp_opt(self.i_ctime as i64, self.i_ctime_nsec);
        let dt_mtime = Utc.timestamp_opt(self.i_mtime as i64, self.i_mtime_nsec);
        writeln!(f, "  access time: {:?}", dt_atime.unwrap())?;
        writeln!(f, "  change time: {:?}", dt_ctime.unwrap())?;
        writeln!(f, "  modify time: {:?}", dt_mtime.unwrap())?;
        let meta_config = HyperFileMetaConfig::try_from_u32(self.i_meta_config).unwrap_or_default();
        writeln!(f, "  format: {:?}, root size: {}, meta block size: {}, data block size: {}",
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

    /// Stamp `atime`, `ctime` and `mtime` to a single "now" reading.
    /// Used at create time so a freshly created, never-written
    /// object reports all three timestamps as its creation time
    /// (POSIX), rather than leaving atime/mtime at epoch 0.
    #[inline]
    fn init_times(&mut self) {
        let (sec, nsec) = Self::get_now();
        self.i_atime = sec;
        self.i_atime_nsec = nsec;
        self.i_ctime = sec;
        self.i_ctime_nsec = nsec;
        self.i_mtime = sec;
        self.i_mtime_nsec = nsec;
        self.i_attr_dirty = true;
    }

    #[inline]
    pub fn update_atime(&mut self) {
        let (sec, nsec) = Self::get_now();
        self.i_atime = sec;
        self.i_atime_nsec = nsec;
        self.i_attr_dirty = true;
    }

    /// Bump `mtime` and `ctime` to the current wall clock.
    ///
    /// POSIX requires `ctime` to advance on every operation that
    /// changes the file's content (write, truncate, write_zero):
    /// changing content also changes a piece of inode-resident
    /// metadata (`i_size`, `i_blocks`), so the "status change"
    /// timestamp must move. Calling `update_mtime` is the single
    /// place hyperfile signals "data was just modified", so the
    /// ctime bump lives here too.
    #[inline]
    pub fn update_mtime(&mut self) {
        let (sec, nsec) = Self::get_now();
        self.i_mtime = sec;
        self.i_mtime_nsec = nsec;
        self.i_ctime = sec;
        self.i_ctime_nsec = nsec;
        self.i_attr_dirty = true;
    }

    pub fn default_dir() -> Self {
        let mut inode = Self::default();
        inode.i_meta_config = HyperFileMetaConfig::default().as_u32();
        // o755
        inode.i_mode = libc::S_IFDIR | libc::S_IRWXU | libc::S_IWUSR | libc::S_IRGRP | libc::S_IXGRP | libc::S_IROTH | libc::S_IXOTH;
        inode.i_uid = 1000;
        inode.i_gid = 1000;
        inode.i_nlink = 1;
        inode.init_times();
        inode
    }

    pub fn default_file() -> Self {
        let mut inode = Self::default();
        inode.i_meta_config = HyperFileMetaConfig::default().as_u32();
        // o644
        inode.i_mode = libc::S_IFREG | libc::S_IRUSR | libc::S_IWUSR | libc::S_IRGRP | libc::S_IROTH;
        inode.i_uid = 1000;
        inode.i_gid = 1000;
        inode.i_nlink = 1;
        inode.init_times();
        inode
    }

    pub fn with_meta_config(mut self, meta_config: &HyperFileMetaConfig) -> Self {
        self.i_meta_config = meta_config.as_u32();
        self
    }

    pub fn with_mode(mut self, mode: &HyperFileMode) -> Self {
        let mode_value = mode.to_u32();
        let file_type =  mode_value & libc::S_IFMT;
        if file_type > 0 {
            // use mode's filetype if it is set
            self.i_mode = file_type | (mode_value & !libc::S_IFMT);
        } else {
            // failback to default file type
            self.i_mode = (self.i_mode & libc::S_IFMT) | (mode_value & !libc::S_IFMT);
        }
        self
    }

    pub fn meta_config(&self) -> HyperFileMetaConfig {
        HyperFileMetaConfig::try_from_u32(self.i_meta_config).unwrap_or_default()
    }

    pub fn mode(&self) -> HyperFileMode {
        HyperFileMode::from_mode(FileMode::from(self.i_mode))
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

    pub fn to_stat(&self, dev: u64, rdev: u64) -> libc::stat {
        // Falls back rather than refusing, because refusing a container this
        // build cannot represent is `HyperFile::do_open`'s job and it happens
        // before any inode is trusted. What reaches here is either an inode
        // that passed that gate, or one built in memory whose config field is
        // not populated yet — and neither `to_stat` nor `Display` may panic
        // over a field they only report.
        let meta_config = HyperFileMetaConfig::try_from_u32(self.i_meta_config).unwrap_or_default();
        let mut stat: libc::stat = unsafe { std::mem::MaybeUninit::zeroed().assume_init() };
        stat.st_dev = dev;
        stat.st_ino = self.i_ino;
        #[cfg(target_arch = "x86_64")]
        {
            stat.st_nlink = self.i_nlink;
        }
        #[cfg(target_arch = "aarch64")]
        {
            stat.st_nlink = self.i_nlink as u32;
        }
        stat.st_mode = self.i_mode;
        stat.st_uid = self.i_uid;
        stat.st_gid = self.i_gid;
        // A char/block device node persists its rdev in i_last_cno (it has no
        // segments, so that slot is free); other inodes use the passed value.
        let fmt = self.i_mode & libc::S_IFMT;
        stat.st_rdev = if fmt == libc::S_IFCHR || fmt == libc::S_IFBLK { self.i_last_cno } else { rdev };
        stat.st_size = self.i_size as i64;
        #[cfg(target_arch = "x86_64")]
        {
            stat.st_blksize = meta_config.data_block_size as i64;
        }
        #[cfg(target_arch = "aarch64")]
        {
            stat.st_blksize = meta_config.data_block_size as i32;
        }
        stat.st_blocks = self.i_blocks as i64;
        stat.st_atime = self.i_atime as i64;
        stat.st_atime_nsec = self.i_atime_nsec as i64;
        stat.st_mtime = self.i_mtime as i64;
        stat.st_mtime_nsec = self.i_mtime_nsec as i64;
        stat.st_ctime = self.i_ctime as i64;
        stat.st_ctime_nsec = self.i_ctime_nsec as i64;
        stat
    }

    pub fn update_stat(&mut self, stat: &libc::stat) -> libc::stat {
        self.i_ino = stat.st_ino;
        #[cfg(target_arch = "x86_64")]
        {
            self.i_nlink = stat.st_nlink;
        }
        #[cfg(target_arch = "aarch64")]
        {
            self.i_nlink = stat.st_nlink as u64;
        }
        self.i_mode = stat.st_mode;
        self.i_uid = stat.st_uid;
        self.i_gid = stat.st_gid;
        self.i_size = stat.st_size as u64;
        self.i_blocks = stat.st_blocks as u64;
        self.i_atime = stat.st_atime as u64;
        self.i_atime_nsec = stat.st_atime_nsec as u32;
        self.i_mtime = stat.st_mtime as u64;
        self.i_mtime_nsec = stat.st_mtime_nsec as u32;
        // POSIX: any setattr-style call (chmod / chown / setattr)
        // is itself a metadata change, so `ctime` must advance,
        // overriding whatever the caller put in `stat.st_ctime`.
        // The mtime is taken from the caller because some
        // futimens-style call sites legitimately set it
        // explicitly; ctime is never user-settable.
        let (now_sec, now_nsec) = Self::get_now();
        self.i_ctime = now_sec;
        self.i_ctime_nsec = now_nsec;
        self.i_attr_dirty = true;
        let mut out = *stat;
        out.st_ctime = now_sec as i64;
        out.st_ctime_nsec = now_nsec as i64;
        out
    }

    /// True if this inode opts in to cross-mount open-but-unlinked
    /// ([`InodeRaw::FLAG_KEEP_OPEN`]).
    pub fn is_keep_open(&self) -> bool { self.i_flags & crate::ondisk::InodeRaw::FLAG_KEEP_OPEN != 0 }

    /// Set/clear the keep-open flag (marks the inode attr-dirty so the change is
    /// persisted on the next flush).
    pub fn set_keep_open(&mut self, on: bool) {
        if on { self.i_flags |= crate::ondisk::InodeRaw::FLAG_KEEP_OPEN; }
        else { self.i_flags &= !crate::ondisk::InodeRaw::FLAG_KEEP_OPEN; }
        self.i_attr_dirty = true;
    }

    pub fn size(&self) -> usize {
        self.i_size as usize
    }

    // for non-sparse file
    /// Set `i_size` only.
    ///
    /// Callers are responsible for keeping `i_blocks` consistent
    /// with the actual amount of storage backed by the bmap. For
    /// extending writes that allocate new blocks, call
    /// `update_blocks` with the byte delta of newly-allocated
    /// blocks. For truncate-shrink, count the entries removed from
    /// the bmap and pass a negative byte delta. The function name
    /// no longer reflects "non-sparse" since hyperfile is sparse-
    /// aware everywhere; the doc comment is preserved as a hint
    /// that the original intent was for fully-dense files.
    pub fn set_size(&mut self, size: usize) {
        self.i_size = size as u64;
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

    /// Move the sequence forward, so records written from now on belong to a
    /// checkpoint no earlier group occupies.
    ///
    /// Only moves forward: going back would put this session's records into a
    /// namespace another session already used.
    pub fn set_last_seq(&mut self, seq: SegmentId) {
        if seq > self.i_last_seq {
            self.i_last_seq = seq;
        }
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

    /// Snapshot mutable attr-related fields so a failed operation can roll
    /// back the in-memory inode to its pre-operation state. Combine with
    /// `restore_state` to form a scope guard for mutations done by
    /// write/write_zero/truncate before flush.
    pub fn save_state(&self) -> InodeSnapshot {
        InodeSnapshot {
            i_size: self.i_size,
            i_blocks: self.i_blocks,
            i_mtime: self.i_mtime,
            i_mtime_nsec: self.i_mtime_nsec,
            i_attr_dirty: self.i_attr_dirty,
        }
    }

    /// Restore fields saved by `save_state`. Intended to be called only on
    /// the failure path when the caller also refreshes bmap from persisted
    /// inode to undo any bmap mutations.
    pub fn restore_state(&mut self, snap: &InodeSnapshot) {
        self.i_size = snap.i_size;
        self.i_blocks = snap.i_blocks;
        self.i_mtime = snap.i_mtime;
        self.i_mtime_nsec = snap.i_mtime_nsec;
        self.i_attr_dirty = snap.i_attr_dirty;
    }

    /// Restore mutable attr-related fields from a raw on-disk inode.
    /// Used by the failure-path rollback to bring in-memory state back
    /// in sync with persisted state. Does NOT touch read-only identity
    /// fields (ino, uid, gid, mode, nlink) because those are not mutated
    /// by the code paths that require rollback.
    pub fn restore_attr_from_raw(&mut self, raw: &InodeRaw) {
        self.i_size = raw.i_size;
        self.i_blocks = raw.i_blocks;
        self.i_mtime = raw.i_mtime;
        self.i_mtime_nsec = raw.i_mtime_nsec;
        self.i_atime = raw.i_atime;
        self.i_atime_nsec = raw.i_atime_nsec;
        self.i_ctime = raw.i_ctime;
        self.i_ctime_nsec = raw.i_ctime_nsec;
        self.i_attr_dirty = false;
    }
}

/// Snapshot of the mutable inode fields affected by write / truncate
/// before flush. Produced by `Inode::save_state`, applied by
/// `Inode::restore_state` on the failure path.
#[derive(Debug, Clone)]
pub struct InodeSnapshot {
    i_size: u64,
    i_blocks: u64,
    i_mtime: u64,
    i_mtime_nsec: u32,
    i_attr_dirty: bool,
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

#[cfg(test)]
mod tests {
    use super::*;

    // --- from_raw / to_raw round-trip ---

    #[test]
    fn from_raw_to_raw_round_trip() {
        let mut raw = InodeRaw::default();
        raw.i_ino = 42;
        raw.i_size = 65536;
        raw.i_blocks = 128;
        raw.i_uid = 1000;
        raw.i_gid = 1000;
        raw.i_mode = libc::S_IFREG | 0o644;
        raw.i_nlink = 2;
        raw.i_last_seq = 10;
        raw.i_last_cno = 10;
        raw.i_atime = 1000;
        raw.i_atime_nsec = 500;
        raw.i_mtime = 2000;
        raw.i_mtime_nsec = 600;
        raw.i_ctime = 3000;
        raw.i_ctime_nsec = 700;
        raw.i_meta_config = HyperFileMetaConfig::default().as_u32();
        raw.i_bmap[0] = 0xAB;

        let inode = Inode::from_raw(&raw, None);
        let bmap: BMapRawType = [0xAB; 56]; // different bmap to verify it's passed through
        let raw2 = inode.to_raw(bmap);

        assert_eq!(raw2.i_ino, 42);
        assert_eq!(raw2.i_size, 65536);
        assert_eq!(raw2.i_blocks, 128);
        assert_eq!(raw2.i_uid, 1000);
        assert_eq!(raw2.i_gid, 1000);
        assert_eq!(raw2.i_mode, libc::S_IFREG | 0o644);
        assert_eq!(raw2.i_nlink, 2);
        assert_eq!(raw2.i_last_seq, 10);
        assert_eq!(raw2.i_last_cno, 10);
        assert_eq!(raw2.i_atime, 1000);
        assert_eq!(raw2.i_atime_nsec, 500);
        assert_eq!(raw2.i_bmap[0], 0xAB); // from the bmap arg, not original
    }

    #[test]
    fn from_raw_sets_ondisk_cno() {
        let mut raw = InodeRaw::default();
        raw.i_last_cno = 77;
        let inode = Inode::from_raw(&raw, None);
        assert_eq!(inode.get_last_ondisk_cno(), 77);
        assert!(!inode.is_attr_dirty());
    }

    #[test]
    fn from_raw_with_ondisk_state() {
        let od = OnDiskState { checksum: "abc".into(), timestamp: 123 };
        let raw = InodeRaw::default();
        let inode = Inode::from_raw(&raw, Some(od));
        let state = inode.get_ondisk_state().as_ref().unwrap();
        assert_eq!(state.checksum, "abc");
        assert_eq!(state.timestamp, 123);
    }

    // --- default_file / default_dir ---

    #[test]
    fn default_file_properties() {
        let inode = Inode::default_file();
        assert_eq!(inode.i_mode & libc::S_IFMT, libc::S_IFREG);
        assert_eq!(inode.i_uid, 1000);
        assert_eq!(inode.i_gid, 1000);
        assert_eq!(inode.i_nlink, 1);
        assert!(inode.is_attr_dirty()); // times were set
        // POSIX: a freshly created object has atime, ctime, mtime
        // all stamped to creation time (not left at epoch 0), and
        // from a single "now" reading they are equal.
        assert!(inode.i_atime > 0, "atime must be set on create");
        assert!(inode.i_ctime > 0, "ctime must be set on create");
        assert!(inode.i_mtime > 0, "mtime must be set on create");
        assert_eq!(inode.i_atime, inode.i_mtime);
        assert_eq!(inode.i_atime, inode.i_ctime);
        assert_eq!(inode.i_atime_nsec, inode.i_mtime_nsec);
        assert_eq!(inode.i_atime_nsec, inode.i_ctime_nsec);
    }

    #[test]
    fn default_dir_properties() {
        let inode = Inode::default_dir();
        assert_eq!(inode.i_mode & libc::S_IFMT, libc::S_IFDIR);
        assert_eq!(inode.i_uid, 1000);
        assert_eq!(inode.i_nlink, 1);
        assert!(inode.i_atime > 0, "atime must be set on create");
        assert!(inode.i_ctime > 0, "ctime must be set on create");
        assert!(inode.i_mtime > 0, "mtime must be set on create");
        assert_eq!(inode.i_atime, inode.i_mtime);
        assert_eq!(inode.i_atime, inode.i_ctime);
    }

    // --- set_size (size-only contract) ---

    #[test]
    fn set_size_basic_only_changes_size() {
        let mut inode = Inode::default_file();
        inode.i_blocks = 100;
        inode.i_attr_dirty = false;
        inode.set_size(4096);
        assert_eq!(inode.size(), 4096);
        assert_eq!(inode.i_blocks, 100, "set_size must not touch i_blocks");
        assert!(inode.is_attr_dirty());
    }

    #[test]
    fn set_size_zero_keeps_blocks() {
        let mut inode = Inode::default_file();
        inode.i_blocks = 7;
        inode.set_size(0);
        assert_eq!(inode.size(), 0);
        assert_eq!(inode.i_blocks, 7, "set_size must not touch i_blocks");
    }

    #[test]
    fn set_size_does_not_compute_blocks_from_size() {
        // The previous behaviour was i_blocks = ceil(size / 512).
        // Verify that contract is NO LONGER honoured: callers must
        // maintain i_blocks separately to reflect actually-allocated
        // bmap entries (the file may be sparse).
        let mut inode = Inode::default_file();
        inode.i_blocks = 0;
        inode.set_size(1024 * 1024 * 1024); // 1 GiB virtual size
        assert_eq!(
            inode.i_blocks, 0,
            "sparse file: virtual size != allocated blocks; \
             set_size must not infer blocks from size"
        );
    }

    // --- update_blocks (the explicit-update path) ---

    #[test]
    fn update_blocks_positive_diff() {
        let mut inode = Inode::default_file();
        inode.i_blocks = 8;
        inode.i_attr_dirty = false;
        inode.update_blocks(8192); // 16 of 512-byte blocks
        assert_eq!(inode.i_blocks, 8 + 16);
        assert!(inode.is_attr_dirty());
    }

    #[test]
    fn update_blocks_negative_diff() {
        let mut inode = Inode::default_file();
        inode.i_blocks = 32;
        inode.update_blocks(-8192);
        assert_eq!(inode.i_blocks, 32 - 16);
    }

    #[test]
    fn update_blocks_zero_diff_no_dirty() {
        let mut inode = Inode::default_file();
        inode.i_blocks = 5;
        inode.i_attr_dirty = false;
        inode.update_blocks(0);
        assert_eq!(inode.i_blocks, 5);
        assert!(!inode.is_attr_dirty(), "zero diff must not dirty attrs");
    }

    // --- extend_size (sparse) ---

    #[test]
    fn extend_size_does_not_change_blocks() {
        let mut inode = Inode::default_file();
        inode.i_blocks = 8;
        inode.i_attr_dirty = false;
        inode.extend_size(1048576);
        assert_eq!(inode.size(), 1048576);
        assert_eq!(inode.i_blocks, 8); // unchanged
        assert!(inode.is_attr_dirty());
    }

    // --- update_blocks (sparse) ---

    #[test]
    fn update_blocks_positive() {
        let mut inode = Inode::default_file();
        inode.i_blocks = 0;
        inode.i_attr_dirty = false;
        inode.update_blocks(4096); // +4096 bytes → +8 blocks
        assert_eq!(inode.i_blocks, 8);
        assert!(inode.is_attr_dirty());
    }

    #[test]
    fn update_blocks_negative() {
        let mut inode = Inode::default_file();
        inode.i_blocks = 16;
        inode.i_attr_dirty = false;
        inode.update_blocks(-4096); // -4096 bytes → -8 blocks
        assert_eq!(inode.i_blocks, 8);
        assert!(inode.is_attr_dirty());
    }

    #[test]
    fn update_blocks_zero_no_change() {
        let mut inode = Inode::default_file();
        inode.i_blocks = 10;
        inode.i_attr_dirty = false;
        inode.update_blocks(0);
        assert_eq!(inode.i_blocks, 10);
        assert!(!inode.is_attr_dirty());
    }

    // --- to_stat ---

    #[test]
    fn to_stat_fields() {
        let mut inode = Inode::default_file();
        inode.i_ino = 99;
        inode.i_size = 8192;
        inode.i_blocks = 16;
        inode.i_uid = 500;
        inode.i_gid = 600;
        inode.i_atime = 1000;
        inode.i_atime_nsec = 111;
        inode.i_mtime = 2000;
        inode.i_mtime_nsec = 222;
        inode.i_ctime = 3000;
        inode.i_ctime_nsec = 333;

        let stat = inode.to_stat(1, 2);
        assert_eq!(stat.st_dev, 1);
        assert_eq!(stat.st_rdev, 2);
        assert_eq!(stat.st_ino, 99);
        assert_eq!(stat.st_size, 8192);
        assert_eq!(stat.st_blocks, 16);
        assert_eq!(stat.st_uid, 500);
        assert_eq!(stat.st_gid, 600);
        assert_eq!(stat.st_mode, inode.i_mode);
        assert_eq!(stat.st_atime, 1000);
        assert_eq!(stat.st_atime_nsec, 111);
        assert_eq!(stat.st_mtime, 2000);
        assert_eq!(stat.st_mtime_nsec, 222);
        assert_eq!(stat.st_ctime, 3000);
        assert_eq!(stat.st_ctime_nsec, 333);
        // blksize should be data_block_size from default meta config
        assert_eq!(stat.st_blksize as usize, HyperFileMetaConfig::default().data_block_size);
    }

    // --- update_stat ---

    #[test]
    fn update_stat_round_trip() {
        let mut inode = Inode::default_file();
        inode.i_attr_dirty = false;
        let mut stat = inode.to_stat(0, 0);
        stat.st_uid = 999;
        stat.st_gid = 888;
        stat.st_size = 12345;
        let returned = inode.update_stat(&stat);
        assert_eq!(inode.i_uid, 999);
        assert_eq!(inode.i_gid, 888);
        assert_eq!(inode.size(), 12345);
        assert!(inode.is_attr_dirty());
        // The returned stat must reflect the bumped ctime (it
        // doesn't echo back the caller's st_ctime).
        assert_eq!(returned.st_ctime as u64, inode.i_ctime);
        assert_eq!(returned.st_ctime_nsec as u32, inode.i_ctime_nsec);
    }

    #[test]
    fn update_stat_overrides_caller_ctime() {
        let mut inode = Inode::default_file();
        // Pretend a previous setattr left ctime at a specific value.
        inode.i_ctime = 1_000_000_000;
        inode.i_ctime_nsec = 0;

        let mut stat = inode.to_stat(0, 0);
        // Caller passes a stale (or even fabricated) ctime — the
        // POSIX rule is "ctime is not user-settable; any setattr
        // bumps it to NOW". Verify that's what update_stat does.
        stat.st_ctime = 42;
        stat.st_ctime_nsec = 7;

        inode.update_stat(&stat);
        assert!(inode.i_ctime > 1_000_000_000,
            "update_stat must bump ctime to NOW (got {})", inode.i_ctime);
        assert_ne!(inode.i_ctime as i64, 42);
    }

    // --- time updates ---

    #[test]
    fn update_atime_sets_timestamp() {
        let mut inode = Inode::default_file();
        inode.i_atime = 0;
        inode.i_attr_dirty = false;
        inode.update_atime();
        assert!(inode.i_atime > 0);
        assert!(inode.is_attr_dirty());
    }

    #[test]
    fn update_atime_does_not_bump_ctime() {
        // POSIX: atime updates are the only metadata change that
        // does NOT bump ctime — otherwise every read would also
        // bump ctime (feedback loop) and ctime would lose its
        // "metadata change" meaning.
        let mut inode = Inode::default_file();
        inode.i_ctime = 1_000_000_000;
        inode.i_ctime_nsec = 12345;
        inode.update_atime();
        assert_eq!(inode.i_ctime, 1_000_000_000);
        assert_eq!(inode.i_ctime_nsec, 12345);
    }

    #[test]
    fn update_mtime_sets_timestamp() {
        let mut inode = Inode::default_file();
        inode.i_mtime = 0;
        inode.i_attr_dirty = false;
        inode.update_mtime();
        assert!(inode.i_mtime > 0);
        assert!(inode.is_attr_dirty());
    }

    #[test]
    fn update_mtime_also_bumps_ctime() {
        // POSIX: a content change (write / truncate) is a metadata
        // change too (it changes i_size at minimum), so mtime and
        // ctime advance together.
        let mut inode = Inode::default_file();
        inode.i_ctime = 0;
        inode.i_ctime_nsec = 0;
        inode.update_mtime();
        // mtime and ctime should hold the same wall-clock reading.
        assert_eq!(inode.i_mtime, inode.i_ctime);
        assert_eq!(inode.i_mtime_nsec, inode.i_ctime_nsec);
        assert!(inode.i_ctime > 0);
    }

    // --- seq / cno ---

    #[test]
    fn get_next_seq_increments() {
        let mut inode = Inode::default_file();
        assert_eq!(inode.get_last_seq(), 0);
        assert_eq!(inode.get_next_seq(), 1);
        assert_eq!(inode.get_next_seq(), 2);
        assert_eq!(inode.get_last_seq(), 2);
    }

    #[test]
    fn is_flushing_logic() {
        let mut inode = Inode::default_file();
        inode.i_last_cno = 5;
        inode.i_last_ondisk_cno = 5;
        assert!(!inode.is_flushing());
        inode.i_last_cno = 6;
        assert!(inode.is_flushing());
    }

    // --- with_mode ---

    #[test]
    fn with_mode_preserves_file_type_when_mode_has_none() {
        let inode = Inode::default_file()
            .with_mode(&HyperFileMode::from_mode(FileMode::from(0o755)));
        // file type should remain S_IFREG since mode had no file type bits
        assert_eq!(inode.i_mode & libc::S_IFMT, libc::S_IFREG);
        assert_eq!(inode.i_mode & !libc::S_IFMT, 0o755);
    }

    #[test]
    fn with_mode_overrides_file_type_when_set() {
        let inode = Inode::default_file()
            .with_mode(&HyperFileMode::from_mode(FileMode::from(libc::S_IFDIR | 0o755)));
        assert_eq!(inode.i_mode & libc::S_IFMT, libc::S_IFDIR);
    }

    // --- meta_config round-trip ---

    #[test]
    fn meta_config_round_trip() {
        let cfg = HyperFileMetaConfig::new(56, 8192, 65536, crate::meta_format::BlockPtrFormat::MicroGroup);
        let inode = Inode::default_file().with_meta_config(&cfg);
        let recovered = inode.meta_config();
        assert_eq!(recovered, cfg);
    }

    // --- from_origin ---

    #[test]
    fn from_origin_sets_size_and_blocks() {
        let cfg = HyperFileMetaConfig::default();
        let inode = Inode::from_origin(10240, &cfg);
        assert_eq!(inode.size(), 10240);
        assert_eq!(inode.i_blocks, 10240 / 512);
    }

    // --- clear_attr_dirty ---

    #[test]
    fn clear_attr_dirty() {
        let mut inode = Inode::default_file();
        assert!(inode.is_attr_dirty());
        inode.clear_attr_dirty();
        assert!(!inode.is_attr_dirty());
    }

    // --- ondisk_state ---

    #[test]
    fn ondisk_state_set_get() {
        let mut inode = Inode::default_file();
        assert!(inode.get_ondisk_state().is_none());
        let od = OnDiskState { checksum: "x".into(), timestamp: 1 };
        inode.set_ondisk_state(Some(od));
        assert!(inode.get_ondisk_state().is_some());
        inode.set_ondisk_state(None);
        assert!(inode.get_ondisk_state().is_none());
    }

    // --- save_state / restore_state ---

    #[test]
    fn save_restore_state_round_trip() {
        let mut inode = Inode::default_file();
        inode.set_size(4096);
        inode.update_blocks(4096); // 8 of 512-byte blocks (matches a fully-dense 4 KiB file)
        inode.i_mtime = 1000;
        inode.i_mtime_nsec = 500;
        inode.i_attr_dirty = false;

        let snap = inode.save_state();

        // Mutate all the fields tracked by the snapshot.
        inode.set_size(8192);
        inode.update_blocks(4096); // pretend we allocated more
        inode.update_mtime();
        assert_ne!(inode.size(), 4096);
        assert!(inode.is_attr_dirty());

        // Restore and verify.
        inode.restore_state(&snap);
        assert_eq!(inode.size(), 4096);
        assert_eq!(inode.i_blocks, 8);
        assert_eq!(inode.i_mtime, 1000);
        assert_eq!(inode.i_mtime_nsec, 500);
        assert!(!inode.is_attr_dirty());
    }

    #[test]
    fn save_restore_preserves_original_attr_dirty() {
        let mut inode = Inode::default_file();
        inode.i_attr_dirty = true;
        let snap = inode.save_state();

        inode.clear_attr_dirty();
        inode.set_size(123);
        assert!(inode.is_attr_dirty());

        inode.restore_state(&snap);
        // attr_dirty should be restored to true (original value).
        assert!(inode.is_attr_dirty());
    }
}
