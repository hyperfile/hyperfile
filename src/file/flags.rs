use std::fmt;
use log::warn;

#[derive(Default, Clone)]
pub struct HyperFileFlags {
    pub read: bool,
    pub write: bool,
    pub creat: bool,
    pub excl: bool,
    pub append: bool,
    pub trunc: bool,
    pub sync: bool,
    pub dsync: bool,
    pub direct: bool,
    pub noatime: bool,
    sync_flush_mode: bool,
}

impl HyperFileFlags {
    pub fn from_flags(f: FileFlags) -> Self {
        let (read, write) = if f.is_rdonly() {
            (true, false)
        } else if f.is_wronly() {
            (false, true)
        } else if f.is_rdwr() {
            (true, true)
        } else {
            warn!("invalid file flags, both read and write bit not been set");
            (false, false)
        };

        Self {
            read: read,
            write: write,
            creat: f.is_creat(),
            excl: f.is_excl(),
            append: f.is_append(),
            trunc: f.is_trunc(),
            sync: f.is_sync(),
            dsync: f.is_dsync(),
            direct: f.is_direct(),
            noatime: f.is_noatime(),
            sync_flush_mode: f.is_direct() | f.is_sync() | f.is_dsync(),
        }
    }

    pub fn rdonly() -> Self {
        let mut f = Self::default();
        f.read = true;
        f
    }

    pub fn wronly() -> Self {
        let mut f = Self::default();
        f.write = true;
        f
    }

    pub fn all() -> Self {
        Self {
            read: true,
            write: true,
            creat: true,
            excl: true,
            append: true,
            trunc: true,
            sync: true,
            dsync: true,
            direct: true,
            noatime: true,
            sync_flush_mode: true,
        }
    }

    pub fn is_sync(&self) -> bool {
        self.sync
    }

    pub fn is_dsync(&self) -> bool {
        self.dsync
    }

    pub fn is_rdonly(&self) -> bool {
        self.read && !self.write && !self.append
    }

    /// True if the handle was opened for writing, i.e. `O_WRONLY`
    /// or `O_RDWR`.
    ///
    /// This is the predicate POSIX uses to decide whether `write`,
    /// `ftruncate` and friends may proceed ("a file descriptor open
    /// for writing"); when it is false those operations must fail
    /// with `EBADF`. Note it keys purely off the access mode:
    /// `O_APPEND` does not grant write access on its own, matching
    /// Linux, where `open(O_RDONLY | O_APPEND)` followed by a
    /// `write` fails with `EBADF`.
    pub fn is_writable(&self) -> bool {
        self.write
    }

    /// True if the handle was opened for reading, i.e. `O_RDONLY` or
    /// `O_RDWR`.
    ///
    /// The mirror of [`Self::is_writable`]: POSIX `read()` lists
    /// `[EBADF] The fildes argument is not a valid file descriptor
    /// open for reading` as a mandatory error, so a read on an
    /// `O_WRONLY` handle must fail with `EBADF`.
    ///
    /// Note this governs `read` only. `lseek` — including the
    /// `SEEK_DATA` / `SEEK_HOLE` extensions behind `seek_data` /
    /// `seek_hole` — requires no particular access mode and is not
    /// gated on this.
    pub fn is_readable(&self) -> bool {
        self.read
    }

    pub fn is_direct(&self) -> bool {
        self.direct
    }

    pub fn is_append(&self) -> bool {
        self.append
    }

    pub fn is_trunc(&self) -> bool {
        self.trunc
    }

    pub fn is_excl(&self) -> bool {
        self.excl
    }

    pub fn is_noatime(&self) -> bool {
        self.noatime
    }

    // according to hyperfile's nature behavior,
    // direct || sync || dsync will be treated as sync_flush_mode ON
    // which should trigger flush every write
    pub fn is_sync_flush_mode(&self) -> bool {
        self.sync_flush_mode
    }
}

/// The four `open(2)` flags Linux has and darwin does not.
///
/// Given Linux's own values, so that a flag word originating on Linux -- which is the
/// only way these bits reach a host without them -- still reads as intended. A host
/// that has no such flag cannot set the bit itself, so the predicate answers no, which
/// is the right answer there.
///
/// Two of the four behave oddly off Linux and are worth knowing before anything starts
/// deciding on them:
///
/// - `O_PATH`'s Linux value is darwin's `O_SYMLINK`, so a darwin caller asking for
///   `O_SYMLINK` reads as [`FileFlags::is_path`].
/// - `O_LARGEFILE` is zero on 64-bit Linux, where every file is a large file, and
///   `(flags & 0) == 0` holds for any input -- so [`FileFlags::is_largefile`] is true
///   whatever it is given. That is already the answer on x86_64 Linux and not something
///   this introduces.
///
/// Both of those are read only by the `Display` impl. The two that reach behaviour,
/// `O_DIRECT` (whether reads and writes use the data block cache) and `O_NOATIME`
/// (whether a read moves the access time), land on bits darwin does not define, so they
/// answer no there and cannot be turned on by accident.
#[cfg(target_os = "linux")]
mod linux_only {
    pub(super) use libc::{O_DIRECT, O_LARGEFILE, O_NOATIME, O_PATH};
}

#[cfg(not(target_os = "linux"))]
mod linux_only {
    use libc::c_int;

    pub(super) const O_DIRECT: c_int = 0o40000;
    pub(super) const O_LARGEFILE: c_int = 0;
    pub(super) const O_NOATIME: c_int = 0o1000000;
    pub(super) const O_PATH: c_int = 0o10000000;
}

pub struct FileFlags(libc::c_int);

impl fmt::Display for FileFlags {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.is_rdonly() {
            write!(f, "O_RDONLY")?;
        }
        if self.is_wronly() {
            write!(f, "O_WRONLY")?;
        }
        if self.is_rdwr() {
            write!(f, "O_RDWR")?;
        }
        if self.is_append() {
            write!(f, " | O_APPEND")?;
        }
        if self.is_async() {
            write!(f, " | O_ASYNC")?;
        }
        if self.is_cloexec() {
            write!(f, " | O_CLOEXEC")?;
        }
        if self.is_creat() {
            write!(f, " | O_CREAT")?;
        }
        if self.is_direct() {
            write!(f, " | O_DIRECT")?;
        }
        if self.is_directory() {
            write!(f, " | O_DIRECTORY")?;
        }
        if self.is_dsync() {
            write!(f, " | O_DSYNC")?;
        }
        if self.is_excl() {
            write!(f, " | O_EXCL")?;
        }
        if self.is_largefile() {
            write!(f, " | O_LARGEFILE")?;
        }
        if self.is_noatime() {
            write!(f, " | O_NOATIME")?;
        }
        if self.is_noctty() {
            write!(f, " | O_NOCTTY")?;
        }
        if self.is_nofollow() {
            write!(f, " | O_NOFOLLOW")?;
        }
        if self.is_nonblock() {
            write!(f, " | O_NONBLOCK")?;
        }
        if self.is_ndelay() {
            write!(f, " | O_NDELAY")?;
        }
        if self.is_path() {
            write!(f, " | O_PATH")?;
        }
        if self.is_sync() {
            write!(f, " | O_SYNC")?;
        }
        if self.is_trunc() {
            write!(f, " | O_TRUNC")?;
        }
        write!(f, "")
    }
}

impl FileFlags {
    pub fn from(flags: libc::c_int) -> Self {
        Self(flags)
    }

    pub fn rdonly() -> Self {
        Self(libc::O_RDONLY)
    }

    pub fn wronly() -> Self {
        Self(libc::O_WRONLY)
    }

    pub fn rdwr() -> Self {
        Self(libc::O_RDWR)
    }

    /// Ask for what Linux's `O_DIRECT` names: reads and writes that do not go through
    /// the data block cache.
    ///
    /// A method rather than a bit to hand [`Self::from`], because `from` takes the host's
    /// own flag word and darwin has no `O_DIRECT` -- there is no bit to set. Without this
    /// a caller there could not ask for it at all.
    ///
    /// Only this and [`Self::noatime`] are spelled out as methods. Every other flag
    /// hyperfile acts on exists on both platforms, so `from` can carry it.
    ///
    /// ```no_run
    /// # use hyperfile::file::flags::FileFlags;
    /// let flags = FileFlags::rdwr().direct();
    /// assert!(flags.is_direct());
    /// ```
    pub fn direct(mut self) -> Self {
        self.0 |= linux_only::O_DIRECT;
        self
    }

    /// Ask for what Linux's `O_NOATIME` names: reads that leave the access time alone.
    ///
    /// A method for the same reason as [`Self::direct`]: darwin has no such flag, so
    /// there is no bit a caller could pass.
    ///
    /// ```no_run
    /// # use hyperfile::file::flags::FileFlags;
    /// let flags = FileFlags::rdonly().noatime();
    /// assert!(flags.is_noatime());
    /// ```
    pub fn noatime(mut self) -> Self {
        self.0 |= linux_only::O_NOATIME;
        self
    }

    pub fn is_rdonly(&self) -> bool {
        (self.0 & libc::O_ACCMODE) == libc::O_RDONLY
    }

    pub fn is_wronly(&self) -> bool {
        (self.0 & libc::O_ACCMODE) == libc::O_WRONLY
    }

    pub fn is_rdwr(&self) -> bool {
        (self.0 & libc::O_ACCMODE) == libc::O_RDWR
    }

    pub fn is_append(&self) -> bool {
        (self.0 & libc::O_APPEND) == libc::O_APPEND
    }

    pub fn is_async(&self) -> bool {
        (self.0 & libc::O_ASYNC) == libc::O_ASYNC
    }

    pub fn is_cloexec(&self) -> bool {
        (self.0 & libc::O_CLOEXEC) == libc::O_CLOEXEC
    }

    pub fn is_creat(&self) -> bool {
        (self.0 & libc::O_CREAT) == libc::O_CREAT
    }

    pub fn is_direct(&self) -> bool {
        (self.0 & linux_only::O_DIRECT) == linux_only::O_DIRECT
    }

    pub fn is_directory(&self) -> bool {
        (self.0 & libc::O_DIRECTORY) == libc::O_DIRECTORY
    }

    pub fn is_dsync(&self) -> bool {
        (self.0 & libc::O_DSYNC) == libc::O_DSYNC
    }

    pub fn is_excl(&self) -> bool {
        (self.0 & libc::O_EXCL) == libc::O_EXCL
    }

    pub fn is_largefile(&self) -> bool {
        (self.0 & linux_only::O_LARGEFILE) == linux_only::O_LARGEFILE
    }

    pub fn is_noatime(&self) -> bool {
        (self.0 & linux_only::O_NOATIME) == linux_only::O_NOATIME
    }

    pub fn is_noctty(&self) -> bool {
        (self.0 & libc::O_NOCTTY) == libc::O_NOCTTY
    }

    pub fn is_nofollow(&self) -> bool {
        (self.0 & libc::O_NOFOLLOW) == libc::O_NOFOLLOW
    }

    pub fn is_nonblock(&self) -> bool {
        (self.0 & libc::O_NONBLOCK) == libc::O_NONBLOCK
    }

    pub fn is_ndelay(&self) -> bool {
        (self.0 & libc::O_NDELAY) == libc::O_NDELAY
    }

    pub fn is_path(&self) -> bool {
        (self.0 & linux_only::O_PATH) == linux_only::O_PATH
    }

    pub fn is_sync(&self) -> bool {
        (self.0 & libc::O_SYNC) == libc::O_SYNC
    }

    pub fn is_trunc(&self) -> bool {
        (self.0 & libc::O_TRUNC) == libc::O_TRUNC
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn direct_and_noatime_can_be_asked_for_without_naming_a_host_bit() {
        let d = FileFlags::rdwr().direct();
        assert!(d.is_direct());
        assert!(d.is_rdwr());
        assert!(!d.is_noatime(), "one must not turn the other on");

        let n = FileFlags::rdonly().noatime();
        assert!(n.is_noatime());
        assert!(n.is_rdonly());
        assert!(!n.is_direct());

        let both = FileFlags::rdwr().direct().noatime();
        assert!(both.is_direct() && both.is_noatime() && both.is_rdwr());
    }

    /// The other way in, which is the one a caller with a real `open(2)` flag word uses.
    ///
    /// Kept as a separate Linux-only test on purpose: the methods above compile
    /// everywhere and would happily stand in for this, and then nothing would check that
    /// a flag word carrying the bit still arrives as `is_direct()` -- which is the path
    /// every caller on Linux actually takes.
    #[cfg(target_os = "linux")]
    #[test]
    fn a_host_flag_word_carrying_the_bit_arrives_the_same_way() {
        let from_word = FileFlags::from(libc::O_RDWR | libc::O_DIRECT | libc::O_NOATIME);
        assert!(from_word.is_direct());
        assert!(from_word.is_noatime());

        let from_methods = FileFlags::rdwr().direct().noatime();
        assert_eq!(
            HyperFileFlags::from_flags(from_word).direct,
            HyperFileFlags::from_flags(from_methods).direct,
            "the two ways in must reach the same decision");
        assert_eq!(
            HyperFileFlags::from_flags(FileFlags::from(libc::O_RDWR | libc::O_NOATIME)).noatime,
            HyperFileFlags::from_flags(FileFlags::rdwr().noatime()).noatime);
    }

    // --- FileFlags constructors ---

    #[test]
    fn file_flags_rdonly() {
        let f = FileFlags::rdonly();
        assert!(f.is_rdonly());
        assert!(!f.is_wronly());
        assert!(!f.is_rdwr());
    }

    #[test]
    fn file_flags_wronly() {
        let f = FileFlags::wronly();
        assert!(f.is_wronly());
        assert!(!f.is_rdonly());
        assert!(!f.is_rdwr());
    }

    #[test]
    fn file_flags_rdwr() {
        let f = FileFlags::rdwr();
        assert!(f.is_rdwr());
        assert!(!f.is_rdonly());
        assert!(!f.is_wronly());
    }

    // --- FileFlags combined ---

    #[test]
    fn file_flags_combined() {
        let f = FileFlags::from(libc::O_RDWR | libc::O_CREAT | libc::O_TRUNC | libc::O_APPEND);
        assert!(f.is_rdwr());
        assert!(f.is_creat());
        assert!(f.is_trunc());
        assert!(f.is_append());
        assert!(!f.is_direct());
        assert!(!f.is_sync());
    }

    #[test]
    fn file_flags_direct_sync_dsync() {
        let f = FileFlags::from(libc::O_WRONLY | linux_only::O_DIRECT | libc::O_SYNC | libc::O_DSYNC);
        assert!(f.is_wronly());
        assert!(f.is_direct());
        assert!(f.is_sync());
        assert!(f.is_dsync());
    }

    // --- HyperFileFlags from FileFlags ---

    #[test]
    fn hyper_flags_from_rdonly() {
        let hf = HyperFileFlags::from_flags(FileFlags::rdonly());
        assert!(hf.read);
        assert!(!hf.write);
        assert!(hf.is_rdonly());
        assert!(!hf.is_sync_flush_mode());
    }

    #[test]
    fn hyper_flags_from_rdwr() {
        let hf = HyperFileFlags::from_flags(FileFlags::rdwr());
        assert!(hf.read);
        assert!(hf.write);
        assert!(!hf.is_rdonly());
    }

    #[test]
    fn hyper_flags_from_wronly() {
        let hf = HyperFileFlags::from_flags(FileFlags::wronly());
        assert!(!hf.read);
        assert!(hf.write);
    }

    #[test]
    fn hyper_flags_creat_trunc_append() {
        let f = FileFlags::from(libc::O_RDWR | libc::O_CREAT | libc::O_TRUNC | libc::O_APPEND);
        let hf = HyperFileFlags::from_flags(f);
        assert!(hf.creat);
        assert!(hf.is_trunc());
        assert!(hf.is_append());
    }

    #[test]
    fn hyper_flags_sync_flush_mode_from_direct() {
        let f = FileFlags::from(libc::O_WRONLY | linux_only::O_DIRECT);
        let hf = HyperFileFlags::from_flags(f);
        assert!(hf.is_direct());
        assert!(hf.is_sync_flush_mode());
    }

    #[test]
    fn hyper_flags_sync_flush_mode_from_sync() {
        let f = FileFlags::from(libc::O_WRONLY | libc::O_SYNC);
        let hf = HyperFileFlags::from_flags(f);
        assert!(hf.is_sync());
        assert!(hf.is_sync_flush_mode());
    }

    #[test]
    fn hyper_flags_sync_flush_mode_from_dsync() {
        let f = FileFlags::from(libc::O_WRONLY | libc::O_DSYNC);
        let hf = HyperFileFlags::from_flags(f);
        assert!(hf.is_dsync());
        assert!(hf.is_sync_flush_mode());
    }

    // --- HyperFileFlags convenience constructors ---

    #[test]
    fn hyper_flags_rdonly_constructor() {
        let hf = HyperFileFlags::rdonly();
        assert!(hf.is_rdonly());
        assert!(!hf.write);
    }

    #[test]
    fn hyper_flags_wronly_constructor() {
        let hf = HyperFileFlags::wronly();
        assert!(hf.write);
        assert!(!hf.read);
    }

    #[test]
    fn hyper_flags_all() {
        let hf = HyperFileFlags::all();
        assert!(hf.read);
        assert!(hf.write);
        assert!(hf.creat);
        assert!(hf.is_append());
        assert!(hf.is_trunc());
        assert!(hf.is_sync());
        assert!(hf.is_dsync());
        assert!(hf.is_direct());
        assert!(hf.is_noatime());
        assert!(hf.is_sync_flush_mode());
    }

    // --- O_NOATIME plumbing ---

    #[test]
    fn hyper_flags_noatime_off_by_default() {
        let hf = HyperFileFlags::from_flags(FileFlags::rdonly());
        assert!(!hf.is_noatime());
        let hf = HyperFileFlags::from_flags(FileFlags::wronly());
        assert!(!hf.is_noatime());
        let hf = HyperFileFlags::from_flags(FileFlags::rdwr());
        assert!(!hf.is_noatime());
    }

    #[test]
    fn hyper_flags_noatime_set_when_o_noatime() {
        let f = FileFlags::from(libc::O_RDONLY | linux_only::O_NOATIME);
        let hf = HyperFileFlags::from_flags(f);
        assert!(hf.is_noatime());
        // O_NOATIME alone shouldn't enable sync_flush_mode
        assert!(!hf.is_sync_flush_mode());
    }

    #[test]
    fn hyper_flags_noatime_orthogonal_to_other_flags() {
        let f = FileFlags::from(libc::O_RDWR | linux_only::O_NOATIME | linux_only::O_DIRECT);
        let hf = HyperFileFlags::from_flags(f);
        assert!(hf.is_noatime());
        assert!(hf.is_direct());
        assert!(hf.is_sync_flush_mode());
    }

    #[test]
    fn hyper_flags_default_noatime_false() {
        let hf = HyperFileFlags::default();
        assert!(!hf.is_noatime());
        let hf = HyperFileFlags::rdonly();
        assert!(!hf.is_noatime());
        let hf = HyperFileFlags::wronly();
        assert!(!hf.is_noatime());
    }

    // --- O_EXCL plumbing ---

    #[test]
    fn hyper_flags_excl_off_by_default() {
        let hf = HyperFileFlags::from_flags(FileFlags::rdonly());
        assert!(!hf.is_excl());
        let hf = HyperFileFlags::from_flags(FileFlags::wronly());
        assert!(!hf.is_excl());
        let hf = HyperFileFlags::from_flags(FileFlags::rdwr());
        assert!(!hf.is_excl());
    }

    #[test]
    fn hyper_flags_excl_set_when_o_excl() {
        let f = FileFlags::from(libc::O_RDWR | libc::O_CREAT | libc::O_EXCL);
        let hf = HyperFileFlags::from_flags(f);
        assert!(hf.is_excl());
        assert!(hf.creat);
    }

    #[test]
    fn hyper_flags_excl_without_creat_still_carries_through() {
        // POSIX leaves O_EXCL without O_CREAT undefined; we still
        // record the bit so the caller can inspect, even though
        // do_open_or_create will ignore it on the bare-open path.
        let f = FileFlags::from(libc::O_RDWR | libc::O_EXCL);
        let hf = HyperFileFlags::from_flags(f);
        assert!(hf.is_excl());
        assert!(!hf.creat);
    }

    #[test]
    fn hyper_flags_default_excl_false() {
        assert!(!HyperFileFlags::default().is_excl());
        assert!(!HyperFileFlags::rdonly().is_excl());
        assert!(!HyperFileFlags::wronly().is_excl());
    }

    #[test]
    fn hyper_flags_all_includes_excl() {
        assert!(HyperFileFlags::all().is_excl());
    }

    // --- is_writable (POSIX "open for writing") ---

    #[test]
    fn hyper_flags_rdonly_is_not_writable() {
        assert!(!HyperFileFlags::from_flags(FileFlags::rdonly()).is_writable());
    }

    #[test]
    fn hyper_flags_wronly_and_rdwr_are_writable() {
        assert!(HyperFileFlags::from_flags(FileFlags::wronly()).is_writable());
        assert!(HyperFileFlags::from_flags(FileFlags::rdwr()).is_writable());
    }

    /// O_APPEND does not grant write access on its own: on Linux,
    /// `open(O_RDONLY | O_APPEND)` followed by a write fails with
    /// EBADF. Note this differs from `is_rdonly()`, which treats
    /// O_APPEND as clearing read-only-ness.
    #[test]
    fn hyper_flags_rdonly_append_is_not_writable() {
        let f = HyperFileFlags::from_flags(FileFlags(libc::O_RDONLY | libc::O_APPEND));
        assert!(!f.is_writable(), "O_RDONLY|O_APPEND must not be writable");
    }

    // --- is_readable (POSIX "open for reading") ---

    #[test]
    fn hyper_flags_wronly_is_not_readable() {
        assert!(!HyperFileFlags::from_flags(FileFlags::wronly()).is_readable());
    }

    #[test]
    fn hyper_flags_rdonly_and_rdwr_are_readable() {
        assert!(HyperFileFlags::from_flags(FileFlags::rdonly()).is_readable());
        assert!(HyperFileFlags::from_flags(FileFlags::rdwr()).is_readable());
    }

    /// The two predicates are independent and each access mode grants
    /// exactly the expected pair.
    #[test]
    fn hyper_flags_access_mode_matrix() {
        for (flags, name, readable, writable) in [
            (FileFlags::rdonly(), "O_RDONLY", true, false),
            (FileFlags::wronly(), "O_WRONLY", false, true),
            (FileFlags::rdwr(), "O_RDWR", true, true),
        ] {
            let f = HyperFileFlags::from_flags(flags);
            assert_eq!(f.is_readable(), readable, "{name} is_readable");
            assert_eq!(f.is_writable(), writable, "{name} is_writable");
        }
    }
}
