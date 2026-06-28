use std::io::{Result, Error, ErrorKind};
use log::debug;
use aws_sdk_s3::Client;
use crate::staging::{Staging, config::StagingConfig, s3::S3Staging, StagingIntercept};
use crate::config::{HyperFileConfig, HyperFileConfigBuilder, HyperFileMetaConfig, HyperFileRuntimeConfig};
use crate::buffer::{AlignedDataBlockWrapper, BatchDataBlockWrapper};
use super::hyper::Hyper;
use super::flags::{HyperFileFlags, FileFlags};
use super::mode::{HyperFileMode, FileMode};

impl<'a: 'static> Hyper<'a> {
    pub async fn fs_create(client: &Client, uri: &str, flags: FileFlags, mode: FileMode) -> Result<Self>
    {
        debug!("fs_create - uri: {}, flags: {}", uri, flags);
        let staging_config = StagingConfig::new_s3_uri(uri, None);
        let file_config = HyperFileConfigBuilder::new()
                            .with_staging_config(&staging_config)
                            .build();
        let f = HyperFileFlags::from_flags(flags);
        let m = HyperFileMode::from_mode(mode);
        return Self::create(client.clone(), file_config, f, m).await;
    }

    pub async fn fs_create_with_interceptor(client: &Client, uri: &str, flags: FileFlags, mode: FileMode, interceptor: impl StagingIntercept<S3Staging> + 'static) -> Result<Self>
    {
        debug!("fs_create_with_interceptor - uri: {}, flags: {}", uri, flags);
        let staging_config = StagingConfig::new_s3_uri(uri, None);
        let file_config = HyperFileConfigBuilder::new()
                            .with_staging_config(&staging_config)
                            .build();
        let f = HyperFileFlags::from_flags(flags);
        let m = HyperFileMode::from_mode(mode);
        return Self::create_with_interceptor(client.clone(), file_config, f, m, interceptor).await;
    }

    pub async fn fs_create_opt(client: &Client, uri: &str, flags: FileFlags, mode: FileMode, meta_config: &HyperFileMetaConfig, runtime_config: &HyperFileRuntimeConfig) -> Result<Self>
    {
        debug!("fs_create_opt - uri: {}, flags: {}", uri, flags);
        let staging_config = StagingConfig::new_s3_uri(uri, None);
        let file_config = HyperFileConfigBuilder::new()
                            .with_meta_config(meta_config)
                            .with_staging_config(&staging_config)
                            .with_runtime_config(runtime_config)
                            .build();
        let f = HyperFileFlags::from_flags(flags);
        let m = HyperFileMode::from_mode(mode);
        return Self::create(client.clone(), file_config, f, m).await;
    }

    pub async fn fs_create_opt_with_interceptor(client: &Client, uri: &str, flags: FileFlags, mode: FileMode,
            meta_config: &HyperFileMetaConfig, runtime_config: &HyperFileRuntimeConfig,
            interceptor: impl StagingIntercept<S3Staging> + 'static) -> Result<Self>
    {
        debug!("fs_create_opt_with_interceptor - uri: {}, flags: {}", uri, flags);
        let staging_config = StagingConfig::new_s3_uri(uri, None);
        let file_config = HyperFileConfigBuilder::new()
                            .with_meta_config(meta_config)
                            .with_staging_config(&staging_config)
                            .with_runtime_config(runtime_config)
                            .build();
        let f = HyperFileFlags::from_flags(flags);
        let m = HyperFileMode::from_mode(mode);
        return Self::create_with_interceptor(client.clone(), file_config, f, m, interceptor).await;
    }

    pub async fn fs_open(client: &Client, uri: &str, flags: FileFlags) -> Result<Self>
    {
        debug!("fs_open - uri: {}, flags: {}", uri, flags);
        let staging_config = StagingConfig::new_s3_uri(uri, None);
        let file_config = HyperFileConfigBuilder::new()
                            .with_staging_config(&staging_config)
                            .build();
        let f = HyperFileFlags::from_flags(flags);
        return Self::open(client.clone(), file_config, f).await;
    }

    pub async fn fs_open_opt(client: &Client, uri: &str, flags: FileFlags, runtime_config: &HyperFileRuntimeConfig) -> Result<Self>
    {
        debug!("fs_open_opt - uri: {}, flags: {}", uri, flags);
        let staging_config = StagingConfig::new_s3_uri(uri, None);
        let file_config = HyperFileConfigBuilder::new()
                            .with_staging_config(&staging_config)
                            .with_runtime_config(runtime_config)
                            .build();
        let f = HyperFileFlags::from_flags(flags);
        return Self::open(client.clone(), file_config, f).await;
    }

    pub async fn fs_open_or_create_with_default_opt(client: &Client, uri: &str, flags: FileFlags, mode: FileMode) -> Result<Self>
    {
        debug!("fs_open_or_create - uri: {}, flags: {}", uri, flags);
        let staging_config = StagingConfig::new_s3_uri(uri, None);
        let file_config = HyperFileConfigBuilder::new()
                            .with_staging_config(&staging_config)
                            .build();
        let f = HyperFileFlags::from_flags(flags);
        let m = HyperFileMode::from_mode(mode);
        return Self::do_open_or_create(client.clone(), file_config, f, m, true).await;
    }

    pub async fn fs_open_or_create_with_config(client: &Client, config: HyperFileConfig, flags: FileFlags, mode: FileMode) -> Result<Self>
    {
        debug!("fs_open_or_create_with_config - flags: {}, mode: {}", flags, mode);
        let f = HyperFileFlags::from_flags(flags);
        let m = HyperFileMode::from_mode(mode);
        return Self::do_open_or_create(client.clone(), config, f, m, true).await;
    }

    pub async fn fs_unlink(client: &Client, uri: &str) -> Result<()>
    {
        debug!("fs_unlink - uri: {}", uri);
        let staging_config = StagingConfig::new_s3_uri(uri, None);
        let staging = S3Staging::from(client, staging_config, HyperFileRuntimeConfig::default()).await?;
        staging.unlink().await
    }

    pub async fn fs_unlink_with_interceptor(client: &Client, uri: &str, interceptor: impl StagingIntercept<S3Staging> + 'static) -> Result<()>
    {
        debug!("fs_unlink_with_interceptor - uri: {}", uri);
        let staging_config = StagingConfig::new_s3_uri(uri, None);
        let mut staging = S3Staging::from(client, staging_config, HyperFileRuntimeConfig::default()).await?;
        staging.interceptor(interceptor);
        staging.unlink().await
    }

    /// Rename the file at `src_uri` to `dst_uri`.
    ///
    /// **Not yet implemented.** This entry point is a placeholder
    /// for a future POSIX-`rename(2)`-equivalent operation. It
    /// will return [`std::io::ErrorKind::Unsupported`] until the
    /// implementation lands.
    ///
    /// # Why it isn't here yet
    ///
    /// Hyperfile stores a file as multiple S3 objects under a URI
    /// prefix (the inode object, segment objects, optional WAL
    /// chunks, optional local cache files). A POSIX-faithful
    /// rename has to:
    ///
    /// 1. Atomically move every object from the source prefix to
    ///    the destination prefix. S3 has no native cross-key
    ///    atomic-rename primitive; the natural implementation is
    ///    a copy-then-delete loop, which is not atomic across
    ///    multiple objects.
    /// 2. Decide what happens if `dst_uri` already exists. POSIX
    ///    requires atomic replacement; under copy-then-delete the
    ///    intermediate state is observable to a concurrent reader
    ///    on either prefix.
    /// 3. Coordinate with any open `Hyper` / `HyperFileHandler` /
    ///    `HyperFileTokio` instances pointing at either URI; an
    ///    in-flight write or flush must not race the rename.
    /// 4. Handle the WAL-enabled case (the WAL prefix lives at a
    ///    sibling URI and would need to move in lock-step) and
    ///    the local-cache case (cached blocks under one URI must
    ///    be invalidated on rename).
    ///
    /// None of those problems are unsolvable, but each requires
    /// design decisions (acceptable atomicity, retention of the
    /// destination URI's prior content if the rename half-fails,
    /// behavior under concurrent open handles) that haven't been
    /// made yet. See `docs/posix.md`'s "Rename" section for the
    /// status and the recommended workaround until then.
    pub async fn fs_rename(_client: &Client, src_uri: &str, dst_uri: &str) -> Result<()>
    {
        debug!("fs_rename - src: {}, dst: {} (not implemented)", src_uri, dst_uri);
        Err(Error::new(
            ErrorKind::Unsupported,
            "fs_rename is not yet implemented; see docs/posix.md for the rationale",
        ))
    }

    pub async fn fs_release(&mut self) -> Result<u64>
    {
        debug!("fs_release - ");
        self.inner.release().await
    }

    pub async fn fs_read(&mut self, off: usize, buf: &mut [u8]) -> Result<usize>
    {
        debug!("fs_read - offset: {}, size: {}", off, buf.len());
        self.inner.read(off, buf).await
    }

    /// `lseek(SEEK_DATA)`: smallest offset >= `off` holding data, or `None`
    /// (caller maps to `ENXIO`) if none before EOF.
    pub async fn fs_seek_data(&self, off: usize) -> Result<Option<usize>>
    {
        debug!("fs_seek_data - offset: {}", off);
        self.inner.seek_data(off).await
    }

    /// `lseek(SEEK_HOLE)`: smallest offset >= `off` in a hole (EOF counts), or
    /// `None` (`ENXIO`) if `off` is at/past EOF.
    pub async fn fs_seek_hole(&self, off: usize) -> Result<Option<usize>>
    {
        debug!("fs_seek_hole - offset: {}", off);
        self.inner.seek_hole(off).await
    }

    pub async fn fs_write(&mut self, off: usize, buf: &[u8]) -> Result<usize>
    {
        debug!("fs_write - offset: {}, size: {}", off, buf.len());
        self.inner.write(off, buf).await
    }

    pub async fn fs_write_zero(&mut self, off: usize, len: usize) -> Result<usize>
    {
        debug!("fs_write_zero - offset: {}, len: {}", off, len);
        self.inner.write_zero(off, len).await
    }

    pub async fn fs_write_aligned_batch(&mut self, blocks: Vec<AlignedDataBlockWrapper>) -> Result<usize>
    {
        debug!("fs_write_aligned_batch - batch count: {}", blocks.len());
        self.inner.write_aligned_batch(blocks).await
    }

    pub async fn fs_write_batch(&mut self, blocks: Vec<BatchDataBlockWrapper>) -> Result<usize>
    {
        debug!("fs_write_batch - batch count: {}", blocks.len());
        self.inner.write_batch(blocks).await
    }

    pub async fn fs_flush(&mut self) -> Result<u64>
    {
        debug!("fs_flush - ");
        self.inner.flush_with_rollback().await
    }

    /// POSIX-`fdatasync` flavoured flush. Persists pending data
    /// and any metadata required for reads to be correct (i.e.
    /// the bmap and `i_size`), but skips writing the inode when
    /// only attribute fields (`atime` / `mtime` / `ctime` /
    /// `mode` / `uid` / `gid`) are dirty. See `docs/posix.md` for
    /// the full semantic.
    ///
    /// Equivalent to `fs_flush` when there's data or bmap dirt;
    /// strictly cheaper when only attrs are dirty (no S3 PUT
    /// happens).
    pub async fn fs_fdatasync(&mut self) -> Result<u64>
    {
        debug!("fs_fdatasync - ");
        self.inner.flush_data().await
    }

    pub async fn fs_truncate(&mut self, offset: usize) -> Result<()>
    {
        debug!("fs_truncate - offset: {}", offset);
        self.inner.truncate(offset).await
    }

    pub fn fs_getattr(&self) -> Result<libc::stat>
    {
        debug!("fs_getattr -");
        Ok(self.inner.stat())
    }

    pub async fn fs_getattr_fast(client: &Client, uri: &str) -> Result<libc::stat>
    {
        debug!("fs_getattr_fast - uri: {}", uri);
        let staging_config = StagingConfig::new_s3_uri(uri, None);
        let file_config = HyperFileConfigBuilder::new()
                            .with_staging_config(&staging_config)
                            .build();
        return Self::stat_fast(client.clone(), file_config).await;
    }

    pub async fn fs_chmod(&mut self, mode: libc::mode_t) -> Result<libc::stat>
    {
        debug!("fs_chmod - mode: {:#o}", mode);
        let mut stat = self.inner.stat();
        // update permission part only, don't change file type part
        stat.st_mode = (stat.st_mode & libc::S_IFMT) | (mode & !libc::S_IFMT);
        self.inner.update_stat(&stat).await
    }

    pub async fn fs_chown(&mut self, uid: libc::uid_t, gid: libc::gid_t) -> Result<libc::stat> {
        debug!("fs_chown - uid: {}, gid: {}", uid, gid);
        let mut stat = self.inner.stat();
        stat.st_uid = uid;
        stat.st_gid = gid;
        self.inner.update_stat(&stat).await
    }

    pub async fn fs_setattr(&mut self, stat: &libc::stat) -> Result<libc::stat> {
        debug!("fs_setattr - mode: {}, uid: {}, gid: {}", stat.st_mode, stat.st_uid, stat.st_gid);
        self.inner.update_stat(stat).await
    }

    /// Opt this open file in/out of cross-mount open-but-unlinked
    /// ([`InodeRaw::FLAG_KEEP_OPEN`]); persists on flush.
    pub async fn fs_set_keep_open(&mut self, on: bool) -> Result<()> {
        debug!("fs_set_keep_open - {}", on);
        self.inner.set_keep_open(on).await
    }

    /// True if this open file opts in to cross-mount open-but-unlinked.
    pub fn fs_is_keep_open(&self) -> bool { self.inner.is_keep_open() }

    pub async fn fs_setattr_fast(client: &Client, uri: &str, stat: &libc::stat) -> Result<libc::stat>
    {
        debug!("fs_setattr_fast - uri: {}", uri);
        let staging_config = StagingConfig::new_s3_uri(uri, None);
        let file_config = HyperFileConfigBuilder::new()
                            .with_staging_config(&staging_config)
                            .build();
        return Self::update_stat_fast(client.clone(), file_config, &stat).await;
    }

    pub fn fs_last_cno(&self) -> u64
    {
        debug!("fs_last_cno -");
        self.inner.last_cno()
    }
}
