use std::fmt;

#[derive(Clone)]
pub struct HyperFileMode {
    pub mode: FileMode,
}

impl HyperFileMode {
    pub fn from_mode(mode: FileMode) -> Self {
        Self {
            mode,
        }
    }

    pub fn to_u32(&self) -> u32 {
        self.mode.0 as u32
    }
}

#[derive(Clone)]
pub struct FileMode(libc::mode_t);

impl fmt::Display for FileMode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.is_reg() {
            write!(f, "S_IFREG")?;
        }
        if self.is_dir() {
            write!(f, "S_IFDIR")?;
        }
        write!(f, "")
    }
}

impl FileMode {
    pub fn from(mode: libc::mode_t) -> Self {
        Self(mode)
    }

    pub fn default_dir() -> Self {
        // o755
        Self(
            libc::S_IFDIR | libc::S_IRWXU | libc::S_IWUSR | libc::S_IRGRP | libc::S_IXGRP | libc::S_IROTH | libc::S_IXOTH
        )
    }

    pub fn default_file() -> Self {
        // o644
        Self(
            libc::S_IFREG | libc::S_IRUSR | libc::S_IWUSR | libc::S_IRGRP | libc::S_IROTH
        )
    }

    pub fn is_reg(&self) -> bool {
        (self.0 & libc::S_IFREG) == libc::S_IFREG
    }

    pub fn is_dir(&self) -> bool {
        (self.0 & libc::S_IFDIR) == libc::S_IFDIR
    }
}
