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
        let perm = self.0 & !libc::S_IFMT;
        write!(f, " | {:#o}", perm)
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn file_mode_default_file() {
        let m = FileMode::default_file();
        assert!(m.is_reg());
        assert!(!m.is_dir());
    }

    #[test]
    fn file_mode_default_dir() {
        let m = FileMode::default_dir();
        assert!(m.is_dir());
        assert!(!m.is_reg());
    }

    #[test]
    fn file_mode_from_raw() {
        let m = FileMode::from(libc::S_IFREG | 0o755);
        assert!(m.is_reg());
        assert!(!m.is_dir());
    }

    #[test]
    fn hyper_file_mode_to_u32_round_trip() {
        let fm = FileMode::from(libc::S_IFREG | 0o644);
        let hm = HyperFileMode::from_mode(fm);
        let val = hm.to_u32();
        assert_eq!(val & libc::S_IFMT, libc::S_IFREG);
        assert_eq!(val & !libc::S_IFMT, 0o644);
    }

    #[test]
    fn hyper_file_mode_permissions_only() {
        let fm = FileMode::from(0o755); // no file type bits
        let hm = HyperFileMode::from_mode(fm);
        assert_eq!(hm.to_u32() & libc::S_IFMT, 0);
        assert_eq!(hm.to_u32() & !libc::S_IFMT, 0o755);
    }

    #[test]
    fn file_mode_display() {
        let m = FileMode::default_file();
        let s = format!("{}", m);
        assert!(s.contains("S_IFREG"));
    }
}