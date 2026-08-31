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
/// Where recovery should stop when it replays the log.
///
/// Both landing points are defensible and they trade different things, so this
/// is a choice rather than a fixed behaviour.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Deserialize, Serialize)]
pub enum WalRecoveryMode {
    /// Apply every group that can be applied, sealed or not.
    ///
    /// Keeps every write that was acknowledged, which is what
    /// [`writes_durable_on_ack`][crate::file::file::HyperFile::writes_durable_on_ack]
    /// promises, so this is the default. The cost is that the landing point
    /// need not be a state the layer above ever had: if the crash fell in the
    /// middle of that layer's own transaction, the replay reproduces the middle
    /// of it, and putting that right is the caller's problem.
    #[default]
    Latest,
    /// Stop at the last group that was sealed and is complete.
    ///
    /// The landing point is one the layer above declared consistent, so it
    /// needs no repair. The cost is everything acknowledged after that seal —
    /// which is why `writes_durable_on_ack` reports `false` in this mode: a
    /// write with no flush behind it is in an unsealed group, and this mode
    /// discards those by design.
    Barrier,
}

#[derive(Clone, Debug, Default, PartialEq, Deserialize, Serialize)]
pub struct HyperFileWalConfig {
    /// The S3 URI (`s3://bucket/key-prefix`) under which the WAL
    /// places per-segid subdirectories of `<seq>_<offset>_<len>`
    /// objects. Leave empty to disable WAL.
    pub root_uri: String,
    /// Where recovery stops. See [`WalRecoveryMode`].
    #[serde(default)]
    pub recovery_mode: WalRecoveryMode,
    /// How many flushes may be satisfied by the log alone before one of them
    /// publishes a segment. `1`, the default, publishes on every flush.
    ///
    /// What this buys is a smaller number of objects, not a shorter flush. A
    /// consumer measured 11, 7, 5 and 4 container objects for 1, 2, 4 and 8 over
    /// the same work — a factor of 2.75 — with flush latency flat across all
    /// four (0.213, 0.210, 0.228, 0.197 seconds). So choose it for storage
    /// economics: object count drives storage cost, the cost of listing, and how
    /// much there is to prune later.
    ///
    /// Flush latency does not move because publishing was never on its critical
    /// path. A flush answers after one log append — the barrier sealing its
    /// group — and hands the segment upload and the inode write to a spawned
    /// task. Deferring an upload that nobody was waiting for saves nothing, and
    /// an earlier version of this note claimed otherwise; see the correction in
    /// the changelog for 0.6.16.
    ///
    /// The cost is at recovery, and only when a crash lands between publishes:
    /// the groups not yet published are replayed on the next open, and each
    /// replay publishes. A crash that lands on a publish boundary has nothing
    /// outstanding and nothing to replay — which is what the same consumer
    /// measured, flat at 1.05 to 1.21 seconds, because their run length was
    /// divisible by every value they tried. So the mount cost is real by
    /// construction and has not been measured; treat the worst case as `n - 1`
    /// groups to replay.
    ///
    /// It does not weaken durability. Every write is in the log before its call
    /// returns either way, which is what `writes_durable_on_ack` reports; what
    /// is deferred is the checkpoint, not the data. It does mean a flush can
    /// return without a checkpoint existing for what it flushed, so a caller
    /// that needs one — to open it by cno, say — has to publish.
    ///
    /// The dirty-data thresholds still apply, and are what bounds how much can
    /// accumulate. Under `WalRecoveryMode::Barrier` crossing one is an error
    /// rather than a publish, so there the bound is a hard one.
    #[serde(default = "default_publish_every")]
    pub publish_every: usize,
}

fn default_publish_every() -> usize { 1 }

impl HyperFileWalConfig {
    /// Build a WAL config targeting the given S3 URI.
    ///
    /// The URI should be an `s3://bucket/prefix` string. Empty or
    /// non-S3 URIs will be logged and treated as "WAL disabled"
    /// during [`to_wal`](Self::to_wal).
    pub fn new(uri: &str) -> Self {
        Self {
            root_uri: uri.to_string(),
            recovery_mode: WalRecoveryMode::default(),
            publish_every: default_publish_every(),
        }
    }

    /// Choose where recovery stops. See [`WalRecoveryMode`].
    pub fn with_recovery_mode(mut self, mode: WalRecoveryMode) -> Self {
        self.recovery_mode = mode;
        self
    }

    /// How many flushes may be satisfied by the log before one publishes. See
    /// [`Self::publish_every`]. Zero is treated as one.
    pub fn with_publish_every(mut self, n: usize) -> Self {
        self.publish_every = n.max(1);
        self
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
