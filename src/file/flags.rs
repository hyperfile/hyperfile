use std::fmt;
use log::warn;

#[derive(Default, Clone)]
pub struct HyperFileFlags {
    pub read: bool,
    pub write: bool,
    pub creat: bool,
    pub append: bool,
    pub trunc: bool,
    pub sync: bool,
    pub dsync: bool,
    pub direct: bool,
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
            append: f.is_append(),
            trunc: f.is_trunc(),
            sync: f.is_sync(),
            dsync: f.is_dsync(),
            direct: f.is_direct(),
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
            append: true,
            trunc: true,
            sync: true,
            dsync: true,
            direct: true,
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

    pub fn is_direct(&self) -> bool {
        self.direct
    }

    pub fn is_append(&self) -> bool {
        self.append
    }

    pub fn is_trunc(&self) -> bool {
        self.trunc
    }
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
        (self.0 & libc::O_DIRECT) == libc::O_DIRECT
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
        (self.0 & libc::O_LARGEFILE) == libc::O_LARGEFILE
    }

    pub fn is_noatime(&self) -> bool {
        (self.0 & libc::O_NOATIME) == libc::O_NOATIME
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
        (self.0 & libc::O_PATH) == libc::O_PATH
    }

    pub fn is_sync(&self) -> bool {
        (self.0 & libc::O_SYNC) == libc::O_SYNC
    }

    pub fn is_trunc(&self) -> bool {
        (self.0 & libc::O_TRUNC) == libc::O_TRUNC
    }
}
