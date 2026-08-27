use std::io::{ErrorKind, Result};
use aws_sdk_s3::Client;
use crate::config::HyperFileConfig;
use crate::meta_loader::s3::S3BlockLoader;
use crate::staging::{Staging, StagingIntercept, config::StagingConfig, s3::S3Staging};
use crate::node_cache::localdisk::LocalDiskNodeCache;
use super::file::HyperFile;
use super::flags::HyperFileFlags;
use super::mode::HyperFileMode;

/// Direct, single-task entry point to a hyperfile-backed file
/// living on S3.
///
/// Use this type when you call `fs_*` methods directly from one
/// task at a time. For multi-task / multi-handle access through
/// a reactor, see [`crate::file::fh::HyperFileHandler`].
///
/// # Security: hyperfile does not enforce POSIX permissions
///
/// `fs_chmod` / `fs_chown` / `fs_setattr` record the
/// `mode` / `uid` / `gid` you give them, and `fs_getattr` reads
/// them back, but the read/write/truncate paths **do not check
/// the bits**. A handle opened against a file with `mode = 0`
/// is fully read/writable. There is no `PermissionDenied` /
/// `EACCES` path. The fields are opaque metadata for upper
/// layers (FUSE adapter / IAM / your app) to enforce; see
/// `docs/posix.md`'s "Permissions and ownership" section for
/// the full rationale and the recommended enforcement
/// integration patterns.
pub struct Hyper<'a> {
    pub(crate) inner: HyperFile<'a, S3Staging, S3BlockLoader, LocalDiskNodeCache>,
}

impl<'a: 'static> Hyper<'a> {
    pub(crate) async fn do_open_or_create(client: Client, file_config: HyperFileConfig, flags: HyperFileFlags, mode: HyperFileMode, create: bool) -> Result<Self>
    {
        // POSIX O_EXCL: when paired with O_CREAT, the open MUST
        // create a new file and fail with EEXIST if one already
        // exists. We honor this by routing straight to create()
        // (which already errors AlreadyExists on collision)
        // without first attempting open(). This mirrors what the
        // Linux kernel does for open(O_CREAT|O_EXCL): a single
        // atomic create-or-fail attempt, no try-open dance.
        //
        // O_EXCL without an effective O_CREAT (i.e. on the bare
        // fs_open path where create=false) is undefined per POSIX
        // and silently ignored on Linux; we match that.
        if create && flags.is_excl() {
            return Self::create(client, file_config, flags, mode).await;
        }
        match Self::open(client.clone(), file_config.clone(), flags.clone()).await {
            Ok(hyper) => {
                return Ok(hyper);
            },
            Err(e) => {
                if create && e.kind() == ErrorKind::NotFound {
                    return Self::create(client, file_config, flags, mode).await;
                }
                return Err(e);
            }
        }
    }

    pub async fn open(client: Client, file_config: HyperFileConfig, flags: HyperFileFlags) -> Result<Self>
    {
        let staging = S3Staging::from(&client, file_config.staging.clone(), file_config.runtime.clone()).await?;
        let loader = staging.to_block_loader();
        let node_cache = LocalDiskNodeCache::from(&file_config.node_cache).await;
        let mut file = HyperFile::<S3Staging, S3BlockLoader, LocalDiskNodeCache>::open(staging, loader, node_cache, file_config, flags.clone()).await?;
        // POSIX O_TRUNC: when opening an existing regular file with
        // write access (O_WRONLY or O_RDWR), the file length is
        // truncated to 0. atime / mtime / ctime get updated by the
        // truncate path itself. If the caller asked for O_TRUNC but
        // didn't request write access we silently ignore it, matching
        // Linux's behaviour (the open(2) man page documents this as
        // unspecified, but glibc / the kernel both no-op rather than
        // erroring).
        if flags.is_trunc() && flags.write {
            file.truncate(0).await?;
        }
        Ok(Self {
            inner: file,
        })
    }

    /// Open a published checkpoint, read-only.
    ///
    /// The inode is read from segment `cno` rather than from the container's
    /// current inode, so the file is seen as it stood at that checkpoint while
    /// the container carries on. `flags` must be read-only; anything else is
    /// refused by [`HyperFile::open_cno`].
    ///
    /// This is not what `hypercli file rollback` does. That reads an inode out
    /// of a segment and publishes it as the container's current inode, so it
    /// moves the whole container back and every reader with it. This changes
    /// nothing: it is a second, read-only view alongside the live one.
    ///
    /// `cno` must name a checkpoint that has been published. A cno returned by
    /// a flush is published by the time the flush returns, on the default
    /// build.
    ///
    /// [`HyperFile::open_cno`]: crate::file::file::HyperFile::open_cno
    pub async fn open_cno(client: Client, file_config: HyperFileConfig, flags: HyperFileFlags, cno: u64) -> Result<Self>
    {
        let staging = S3Staging::from(&client, file_config.staging.clone(), file_config.runtime.clone()).await?;
        let loader = staging.to_block_loader();
        let node_cache = LocalDiskNodeCache::from(&file_config.node_cache).await;
        let file = HyperFile::<S3Staging, S3BlockLoader, LocalDiskNodeCache>::open_cno(
            staging, loader, node_cache, file_config, flags, cno).await?;
        Ok(Self {
            inner: file,
        })
    }

    pub async fn create(client: Client, file_config: HyperFileConfig, flags: HyperFileFlags, mode: HyperFileMode) -> Result<Self>
    {
        let staging = S3Staging::create(&client, file_config.staging.clone(), file_config.runtime.clone()).await?;
        let loader = staging.to_block_loader();
        let node_cache = LocalDiskNodeCache::from(&file_config.node_cache).await;
        let file = HyperFile::<S3Staging, S3BlockLoader, LocalDiskNodeCache>::new(staging, loader, node_cache, file_config, flags, mode).await?;
        Ok(Self {
            inner: file,
        })
    }

    pub async fn create_with_interceptor(client: Client, file_config: HyperFileConfig, flags: HyperFileFlags, mode: HyperFileMode, interceptor: impl StagingIntercept<S3Staging> + 'static) -> Result<Self>
    {
        let mut staging = S3Staging::create(&client, file_config.staging.clone(), file_config.runtime.clone()).await?;
        staging.interceptor(interceptor);
        let loader = staging.to_block_loader();
        let node_cache = LocalDiskNodeCache::from(&file_config.node_cache).await;
        let file = HyperFile::<S3Staging, S3BlockLoader, LocalDiskNodeCache>::new(staging, loader, node_cache, file_config, flags, mode).await?;
        Ok(Self {
            inner: file,
        })
    }

    pub async fn stat_fast(client: Client, file_config: HyperFileConfig) -> Result<libc::stat>
    {
        let staging = S3Staging::from(&client, file_config.staging.clone(), file_config.runtime.clone()).await?;
        HyperFile::<S3Staging, S3BlockLoader, LocalDiskNodeCache>::stat_fast(staging).await
    }

    pub async fn update_stat_fast(client: Client, file_config: HyperFileConfig, stat: &libc::stat) -> Result<libc::stat>
    {
        let staging = S3Staging::from(&client, file_config.staging.clone(), file_config.runtime.clone()).await?;
        HyperFile::<S3Staging, S3BlockLoader, LocalDiskNodeCache>::update_stat_fast(staging, stat).await
    }
}

/// expose helper fn
impl<'a: 'static> Hyper<'a> {
    pub fn staging_config(&self) -> &StagingConfig {
        self.inner.staging_config()
    }

    pub fn with_staging_interceptor(&mut self, i: impl StagingIntercept<S3Staging> + 'static) {
        self.inner.staging_interceptor(i)
    }

    /// Test-only: number of dirty data blocks currently in cache.
    #[doc(hidden)]
    pub fn dirty_block_count(&self) -> usize {
        self.inner.dirty_block_count()
    }

    /// Test-only: whether inode has unflushed attr changes.
    #[doc(hidden)]
    pub fn is_attr_dirty(&self) -> bool {
        self.inner.is_attr_dirty()
    }

    /// Test-only: whether bmap has dirty meta nodes.
    #[doc(hidden)]
    pub fn is_bmap_dirty(&self) -> bool {
        self.inner.is_bmap_dirty()
    }

    /// Test-only: in-memory view of the last cno written to a segment.
    #[doc(hidden)]
    pub fn in_memory_last_cno(&self) -> u64 {
        self.inner.in_memory_last_cno()
    }

    /// Test-only: in-memory view of the last cno persisted in the inode.
    #[doc(hidden)]
    pub fn in_memory_last_ondisk_cno(&self) -> u64 {
        self.inner.in_memory_last_ondisk_cno()
    }

    /// Benchmark-only: cumulative per-phase flush timings.
    /// Read-side counters for this file. See
    /// [`ReadTiming`](crate::file::ReadTiming).
    ///
    /// The read path's cost is dominated by object-store round trips,
    /// and timing a read cannot tell one coalesced request from many,
    /// or a cache hit from a fetch.
    pub fn read_timing(&self) -> &crate::file::ReadTiming {
        self.inner.read_timing()
    }

    /// Zero the read counters, to bracket a measurement.
    pub fn read_timing_reset(&self) {
        self.inner.read_timing_reset()
    }

    #[doc(hidden)]
    pub fn flush_timing(&self) -> &crate::file::FlushTiming {
        self.inner.flush_timing()
    }

    /// Benchmark-only: reset the flush timing counters.
    #[doc(hidden)]
    pub fn flush_timing_reset(&self) {
        self.inner.flush_timing_reset()
    }
}
