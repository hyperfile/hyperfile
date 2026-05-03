use std::io::Result;
use log::{warn, error};
use serde::{Deserialize, Serialize};
use crate::SegmentId;
use super::WalReadWrite;
use super::s3::S3Wal;

/// Configuration for the write-ahead log.
///
/// The WAL persists each write to a dedicated S3 prefix before the
/// write's ack, so that an unflushed write survives a crash. On
/// reopen, [`HyperFile`][crate::file::file::HyperFile] inspects
/// this prefix and replays any chunks whose owning segment has not
/// yet been persisted in the inode.
///
/// # Required shape
///
/// - `root_uri` must be an `s3://bucket/path` URI (case-insensitive
///   scheme prefix). Any other value disables WAL with a warning.
/// - The prefix must be dedicated to **one** Hyperfile at a time.
///   Two concurrent writers sharing a prefix will collide on object
///   keys (see `docs/wal.md` "Current limitations").
/// - The prefix must be empty when `Hyper::create` runs. `create`
///   returns [`ErrorKind::ResourceBusy`][std::io::ErrorKind::ResourceBusy]
///   if it is not.
///
/// # Typical usage
///
/// ```ignore
/// use hyperfile::config::HyperFileConfigBuilder;
/// use hyperfile::staging::config::StagingConfig;
/// use hyperfile::wal::config::HyperFileWalConfig;
///
/// let uri = "s3://my-bucket/data/file-1";
/// let staging = StagingConfig::new_s3_uri(uri, None);
/// let wal = HyperFileWalConfig::new(&format!("{}/wal", uri));
/// let file_config = HyperFileConfigBuilder::new()
///     .with_staging_config(&staging)
///     .with_wal_config(&wal)
///     .build();
/// ```
///
/// See [`docs/wal.md`](../../docs/wal.md) for the full semantics,
/// durability guarantees, and performance trade-offs.
#[derive(Clone, Debug, Default, PartialEq, Deserialize, Serialize)]
pub struct HyperFileWalConfig {
    /// The S3 URI (`s3://bucket/key-prefix`) under which the WAL
    /// places per-segid subdirectories of `<seq>_<offset>_<len>`
    /// objects. Leave empty to disable WAL.
    pub root_uri: String,
}

impl HyperFileWalConfig {
    /// Build a WAL config targeting the given S3 URI.
    ///
    /// The URI should be an `s3://bucket/prefix` string. Empty or
    /// non-S3 URIs will be logged and treated as "WAL disabled"
    /// during [`to_wal`](Self::to_wal).
    pub fn new(uri: &str) -> Self {
        Self {
            root_uri: uri.to_string(),
        }
    }

    /// Construct the runtime WAL instance from this config.
    ///
    /// Returns `Ok(None)` when WAL is effectively disabled (empty
    /// URI or unrecognized scheme). Currently only `s3://` URIs are
    /// supported.
    ///
    /// `data_block_size` and `last_segid` are plumbed through to
    /// the WAL implementation but are not used for correctness
    /// decisions — they are informational for the underlying
    /// storage driver.
    pub fn to_wal(&self, data_block_size: usize, last_segid: SegmentId) -> Result<Option<Box<dyn WalReadWrite + Send>>> {
        if self.root_uri.starts_with("S3://") || self.root_uri.starts_with("s3://") {
            return S3Wal::from_uri(&self.root_uri, data_block_size, last_segid);
        } else if self.root_uri.is_empty() {
            warn!("root uri of in wal config is not configured, disable wal");
        } else {
            error!("unknown root uri: {} in wal config, disable wal", self.root_uri);
        }
        Ok(None)
    }
}
