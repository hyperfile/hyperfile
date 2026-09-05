use std::fmt;
use std::io::{Error, ErrorKind, Result};
use log::warn;
use serde::{Deserialize, Serialize};
use crate::meta_format::BlockPtrFormat;
use crate::staging::config::StagingConfig;
use crate::data_cache::config::HyperFileDataCacheConfig;
use crate::node_cache::config::HyperFileNodeCacheConfig;
#[cfg(feature = "wal")]
use crate::wal::config::HyperFileWalConfig;
use crate::*;

const MIN_ROOT_SIZE: usize = 56;
const MIN_META_BLOCK_SIZE: usize = 4096;
const MIN_DATA_BLOCK_SIZE: usize = 4096;
/// Largest block-size shift a container may name. Not a policy — 1 GiB
/// blocks are already absurd — but a bound, so that a byte from a container
/// this build does not understand cannot become a shift overflow. The
/// encoded field is a whole byte, so without this a stray value shifts by up
/// to 255.
const MAX_BLOCK_SHIFT: u32 = 30;

pub(crate) const DEFAULT_ROOT_SIZE: usize = MIN_ROOT_SIZE;
const DEFAULT_META_BLOCK_SIZE: usize = MIN_META_BLOCK_SIZE;
const DEFAULT_DATA_BLOCK_SIZE: usize = MIN_DATA_BLOCK_SIZE;
// `MicroGroup`, because that is what every file created so far actually
// got: `HyperFile::new` hardcoded it and ignored this config. Now that it
// honours the config, the default has to name the same format or existing
// callers would silently start writing a different one.
const DEFAULT_BLOCK_PTR_FORMAT: BlockPtrFormat = BlockPtrFormat::MicroGroup;

#[derive(Clone, Debug, PartialEq, Deserialize, Serialize)]
pub struct HyperFileMetaConfig {
    pub root_size: usize,
    pub meta_block_size: usize,
    pub data_block_size: usize,
    pub block_ptr_format: BlockPtrFormat,
}

impl fmt::Display for HyperFileMetaConfig {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "root size: {}, meta block size: {}, data block size: {}, block ptr format: {:?}",
            self.root_size, self.meta_block_size, self.data_block_size, self.block_ptr_format)
    }
}

impl HyperFileMetaConfig {
    pub fn new(root_size: usize, meta_block_size: usize, data_block_size: usize, block_ptr_format: BlockPtrFormat) -> Self {
        let root_size = if root_size != DEFAULT_ROOT_SIZE {
            warn!("only support fixed root size == {} at this moment", DEFAULT_ROOT_SIZE);
            DEFAULT_ROOT_SIZE
        } else {
            root_size
        };
        // enforce input min size and log2 aligned
        let root_size = std::cmp::max(root_size, DEFAULT_ROOT_SIZE);
        let meta_block_size = std::cmp::max(meta_block_size, DEFAULT_META_BLOCK_SIZE);
        let data_block_size = std::cmp::max(data_block_size, DEFAULT_DATA_BLOCK_SIZE);
        let meta_block_size = 1 << meta_block_size.checked_ilog2().unwrap();
        let data_block_size = 1 << data_block_size.checked_ilog2().unwrap();
        Self {
            root_size: root_size,
            meta_block_size: meta_block_size,
            data_block_size: data_block_size,
            block_ptr_format: block_ptr_format,
        }
    }

    // encode as u32
    // format:
    //   root_mul (multiple of 8 bytes) | meta block shift | data block shift | block ptr format
    pub fn as_u32(&self) -> u32 {
        let root_multiple: u32 = (self.root_size / 8) as u32;
        assert!(self.root_size % 8 == 0);
        let meta_block_shift: u32 = self.meta_block_size.checked_ilog2().expect("invalid meta block size");
        let data_block_shift: u32 = self.data_block_size.checked_ilog2().expect("invalid meta block size");
        let block_ptr_format: u32 = self.block_ptr_format as u8 as u32;
        root_multiple << 24 | meta_block_shift << 16 | data_block_shift << 8 | block_ptr_format
    }

    /// Decode from the `u32` an inode carries, rejecting anything this build
    /// cannot represent.
    ///
    /// Every field is checked, because this value comes from storage and may
    /// have been written by a version whose layout differs. A container from
    /// before this field was populated carries zero, which decodes to a root
    /// of 0 bytes and blocks of 1 byte — all three below the minimums that
    /// were already named above. Read without checking, those propagate: the
    /// caller's own config is overwritten by what the container says (see
    /// `HyperFile::do_open`), so a one-byte block size then decides how every
    /// read and write is cut up.
    ///
    /// Refusing is the point. A container this build does not understand
    /// cannot be read correctly, and reporting the raw value at the first
    /// step is much better than presenting a file whose size and block size
    /// are quietly wrong.
    pub fn try_from_u32(data: u32) -> Result<Self> {
        let block_ptr_format = BlockPtrFormat::try_from_u8((data & 0xFF) as u8)?;
        if block_ptr_format == BlockPtrFormat::Nop {
            return Err(Error::new(ErrorKind::InvalidData,
                "block ptr format is Nop, which no container is written with"));
        }

        let data_shift = (data >> 8) & 0xFF;
        let meta_shift = (data >> 16) & 0xFF;
        // Checked before shifting, not after: the shift itself would
        // overflow.
        for (what, shift, min) in [
            ("data block size", data_shift, MIN_DATA_BLOCK_SIZE),
            ("meta block size", meta_shift, MIN_META_BLOCK_SIZE),
        ] {
            let min_shift = min.ilog2();
            if shift < min_shift || shift > MAX_BLOCK_SHIFT {
                return Err(Error::new(ErrorKind::InvalidData, format!(
                    "{} shift {} is outside {}..={}", what, shift, min_shift, MAX_BLOCK_SHIFT)));
            }
        }
        let data_block_size = 1usize << data_shift;
        let meta_block_size = 1usize << meta_shift;

        let root_size: usize = (((data >> 24) & 0xFF) * 8) as usize;
        if root_size < MIN_ROOT_SIZE {
            return Err(Error::new(ErrorKind::InvalidData, format!(
                "root size {} is below the minimum {}", root_size, MIN_ROOT_SIZE)));
        }

        Ok(Self {
            root_size,
            meta_block_size,
            data_block_size,
            block_ptr_format,
        })
    }

    /// Decode from a `u32` this crate produced itself, panicking on one it
    /// cannot represent. Anything read from a container should go through
    /// [`Self::try_from_u32`].
    pub fn from_u32(data: u32) -> Self {
        Self::try_from_u32(data).expect("meta config")
    }
}

impl Default for HyperFileMetaConfig {
    fn default() -> Self {
        Self {
            root_size: DEFAULT_ROOT_SIZE,
            meta_block_size: DEFAULT_META_BLOCK_SIZE,
            data_block_size: DEFAULT_DATA_BLOCK_SIZE,
            block_ptr_format: DEFAULT_BLOCK_PTR_FORMAT,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Deserialize, Serialize)]
pub struct HyperFileRuntimeConfig {
    // forward origin
    pub forward_origin_concurrency: usize,
    pub forward_origin_threshold: usize,
    pub forward_origin_chunk_size: usize,
    // segment
    pub segment_buffer_size: usize,
    pub segment_mpu_chunk_size: usize,
    // data cache & dirty cache
    pub data_cache_blocks: usize,
    pub data_cache_dirty_max_bytes_threshold: usize,
    pub data_cache_dirty_max_blocks_threshold: usize,
    pub data_cache_dirty_max_flush_interval: u64,
    // bmap node cache
    pub node_cache_blocks: usize,
    /// Cap on the number of bytes a single S3 GET issued by the
    /// read path's coalescing logic may cover. Larger contiguous
    /// regions are split into this-sized sub-ranges.
    #[serde(default = "default_read_get_max_bytes")]
    pub read_get_max_bytes: usize,

    /// Whether this container writes partial segments when memory pressure asks
    /// for it, instead of publishing a checkpoint nobody asked for.
    ///
    /// On by default, which changes nothing for a container whose format is not
    /// [`crate::meta_format::BlockPtrFormat::PartedSegment`] — a partial segment is
    /// addressable only under that format, and the format is fixed when the
    /// container is created. So asking for the format is asking for the behaviour,
    /// and this exists to take it back: a container can be created with the format
    /// and made to behave exactly as before by clearing this.
    ///
    /// What it changes: a dirty-data threshold, or the flush interval, writes the
    /// dirty set out as a partial segment and keeps accumulating, rather than
    /// publishing. Under a transaction or `WalRecoveryMode::Barrier`, where
    /// crossing a threshold is refused with `OutOfMemory` because nothing may
    /// publish, it writes a partial instead — which is the case this exists for.
    ///
    /// What it does not change: an explicit `flush`, `fdatasync`, `release` or
    /// `commit_txn` still writes a consistency point, and with no partial
    /// outstanding it writes exactly the single segment it always did.
    #[serde(default = "default_parted_segment_enabled")]
    pub parted_segment_enabled: bool,

    /// Least dirty data worth writing as a partial segment. Below it, a threshold
    /// crossing behaves as it always did.
    ///
    /// A partial segment costs an object and a break in what a sequential read can
    /// coalesce, so a small one buys little memory for a lasting cost in
    /// fragmentation. Defaults to `data_cache_dirty_max_bytes_threshold`'s own
    /// default, which is the amount that triggers a write in the first place.
    #[serde(default = "default_data_cache_dirty_min_bytes_to_part")]
    pub data_cache_dirty_min_bytes_to_part: usize,
    /// Backpressure cap on in-flight S3 GETs spawned by a single
    /// `fs_read` / `fh_read` after Level-A coalescing.
    #[serde(default = "default_read_max_concurrency")]
    pub read_max_concurrency: usize,
    /// How to resolve flush conflicts when another writer has modified
    /// the same file between our read and our write. See
    /// `FlushConflictPolicy` for details.
    #[serde(default)]
    pub flush_conflict_policy: FlushConflictPolicy,
}

fn default_read_get_max_bytes() -> usize { DEFAULT_READ_GET_MAX_BYTES }
fn default_parted_segment_enabled() -> bool { true }
fn default_data_cache_dirty_min_bytes_to_part() -> usize { DEFAULT_MAX_DIRTY_DATA_BYTES_THRESHOLD }
fn default_read_max_concurrency() -> usize { DEFAULT_READ_MAX_CONCURRENCY }

/// Policy that controls how `flush` handles a concurrent modification
/// detected at the storage layer (S3 PutObject returns 412 Precondition
/// Failed because another writer committed since we read the ETag).
///
/// Default is `RetryLastWriterWins` to preserve the pre-existing
/// single-writer / relaxed behavior.
#[derive(Clone, Copy, Debug, PartialEq, Deserialize, Serialize)]
pub enum FlushConflictPolicy {
    /// Retry on conflict by refreshing local state from the persisted
    /// inode and re-flushing. In a multi-writer scenario this means the
    /// later writer silently overwrites the earlier writer's change.
    /// Convenient for single-writer workloads; NOT safe if your workload
    /// cannot tolerate silent overwrites.
    RetryLastWriterWins,

    /// Return `std::io::ErrorKind::AlreadyExists` on the first conflict
    /// without retrying. The caller decides whether to re-read, merge,
    /// or abort. Use this for workflows that need explicit conflict
    /// detection (e.g. ensuring no lost updates).
    FailFast,
}

impl Default for FlushConflictPolicy {
    fn default() -> Self {
        Self::RetryLastWriterWins
    }
}

impl Default for HyperFileRuntimeConfig {
    fn default() -> Self {
        Self {
            forward_origin_concurrency: DEFAULT_FORWARD_ORIGIN_CONCURRENCY,
            forward_origin_threshold: DEFAULT_FORWARD_ORIGIN_THRESHOLD,
            forward_origin_chunk_size: DEFAULT_FORWARD_ORIGIN_CHUNK_SIZE,
            segment_buffer_size: DEFAULT_SEGMENT_BUFFER_SIZE,
            segment_mpu_chunk_size: DEFAULT_SEGMENT_MPU_CHUNK_SIZE,
            data_cache_blocks: DEFAULT_DATA_CACHE_BLOCKS,
            data_cache_dirty_max_bytes_threshold: DEFAULT_MAX_DIRTY_DATA_BYTES_THRESHOLD,
            data_cache_dirty_max_blocks_threshold: DEFAULT_MAX_DIRTY_DATA_BLOCKS_THRESHOLD,
            data_cache_dirty_max_flush_interval: DEFAULT_MAX_DIRTY_DATA_FLUSH_INTERVAL,
            node_cache_blocks: DEFAULT_NODE_CACHE_BLOCKS,
            flush_conflict_policy: FlushConflictPolicy::default(),
            read_get_max_bytes: DEFAULT_READ_GET_MAX_BYTES,
            parted_segment_enabled: true,
            data_cache_dirty_min_bytes_to_part: DEFAULT_MAX_DIRTY_DATA_BYTES_THRESHOLD,
            read_max_concurrency: DEFAULT_READ_MAX_CONCURRENCY,
        }
    }
}

impl HyperFileRuntimeConfig {
    pub fn default_large() -> Self {
        Self {
            forward_origin_concurrency: DEFAULT_FORWARD_ORIGIN_CONCURRENCY,
            forward_origin_threshold: DEFAULT_FORWARD_ORIGIN_THRESHOLD,
            forward_origin_chunk_size: DEFAULT_FORWARD_ORIGIN_CHUNK_SIZE,
            segment_buffer_size: DEFAULT_LARGE_SEGMENT_BUFFER_SIZE,
            segment_mpu_chunk_size: DEFAULT_SEGMENT_MPU_CHUNK_SIZE,
            data_cache_blocks: DEFAULT_LARGE_DATA_CACHE_BLOCKS,
            data_cache_dirty_max_bytes_threshold: DEFAULT_LARGE_MAX_DIRTY_DATA_BYTES_THRESHOLD,
            data_cache_dirty_max_blocks_threshold: DEFAULT_LARGE_MAX_DIRTY_DATA_BLOCKS_THRESHOLD,
            data_cache_dirty_max_flush_interval: DEFAULT_MAX_DIRTY_DATA_FLUSH_INTERVAL,
            node_cache_blocks: DEFAULT_MAX_NODE_CACHE_BLOCKS,
            flush_conflict_policy: FlushConflictPolicy::default(),
            read_get_max_bytes: DEFAULT_READ_GET_MAX_BYTES,
            parted_segment_enabled: true,
            data_cache_dirty_min_bytes_to_part: DEFAULT_MAX_DIRTY_DATA_BYTES_THRESHOLD,
            read_max_concurrency: DEFAULT_READ_MAX_CONCURRENCY,
        }
    }

    pub fn default_middle() -> Self {
        Self {
            forward_origin_concurrency: DEFAULT_FORWARD_ORIGIN_CONCURRENCY,
            forward_origin_threshold: DEFAULT_FORWARD_ORIGIN_THRESHOLD,
            forward_origin_chunk_size: DEFAULT_FORWARD_ORIGIN_CHUNK_SIZE,
            segment_buffer_size: DEFAULT_MIDDLE_SEGMENT_BUFFER_SIZE,
            segment_mpu_chunk_size: DEFAULT_SEGMENT_MPU_CHUNK_SIZE,
            data_cache_blocks: DEFAULT_MIDDLE_DATA_CACHE_BLOCKS,
            data_cache_dirty_max_bytes_threshold: DEFAULT_MIDDLE_MAX_DIRTY_DATA_BYTES_THRESHOLD,
            data_cache_dirty_max_blocks_threshold: DEFAULT_MIDDLE_MAX_DIRTY_DATA_BLOCKS_THRESHOLD,
            data_cache_dirty_max_flush_interval: DEFAULT_MAX_DIRTY_DATA_FLUSH_INTERVAL,
            node_cache_blocks: DEFAULT_MAX_NODE_CACHE_BLOCKS,
            flush_conflict_policy: FlushConflictPolicy::default(),
            read_get_max_bytes: DEFAULT_READ_GET_MAX_BYTES,
            parted_segment_enabled: true,
            data_cache_dirty_min_bytes_to_part: DEFAULT_MAX_DIRTY_DATA_BYTES_THRESHOLD,
            read_max_concurrency: DEFAULT_READ_MAX_CONCURRENCY,
        }
    }
}

/// Central config per hyper file
#[derive(Clone, Default, Debug, PartialEq, Deserialize, Serialize)]
pub struct HyperFileConfig {
	/// metadata config for hyper file
	pub meta: HyperFileMetaConfig,
	/// staging config for hyper file
	pub staging: StagingConfig,
	/// hyper file runtime tunables
	#[serde(default)]
	pub runtime: HyperFileRuntimeConfig,
    #[cfg(feature = "wal")]
	/// wal config for hyper file
	pub wal: HyperFileWalConfig,
    /// data cache config for hyper file
    pub data_cache: HyperFileDataCacheConfig,
    /// node cache config for hyper file
    pub node_cache: HyperFileNodeCacheConfig,
}

impl HyperFileConfig {
    pub fn from_json_string(s: &str) -> Result<Self> {
        serde_json::from_str(s)
            .map_err(|e| {
                let err_msg = format!("{}", e);
                Error::new(ErrorKind::InvalidInput, err_msg)
            })
    }

    pub fn to_json_string(&self, pretty: bool) -> String {
        if pretty {
            return serde_json::to_string_pretty(self).unwrap();
        }
        serde_json::to_string(self).unwrap()
    }
}

pub struct HyperFileConfigBuilder {
	pub(crate) config: HyperFileConfig,
}

impl HyperFileConfigBuilder {
	/// Get a new hyper file config builder with all default values,
	/// by default, it is a read-only hyper file with 4KiB block size.
	pub fn new() -> Self {
		let config = HyperFileConfig::default();
		Self {
			config
		}
	}

	pub fn from(config: &HyperFileConfig) -> Self {
		Self {
			config: config.to_owned(),
		}
	}

	pub fn with_meta_config(mut self, meta: &HyperFileMetaConfig) -> Self {
		self.config.meta = meta.to_owned();
		self
	}

	pub fn with_staging_config(mut self, staging: &StagingConfig) -> Self {
		self.config.staging = staging.to_owned();
		self
	}

	pub fn with_runtime_config(mut self, runtime: &HyperFileRuntimeConfig) -> Self {
		self.config.runtime = runtime.to_owned();
		self
	}

    #[cfg(feature = "wal")]
	pub fn with_wal_config(mut self, wal: &HyperFileWalConfig) -> Self {
		self.config.wal = wal.to_owned();
		self
	}

	pub fn with_data_cache_config(mut self, cache: &HyperFileDataCacheConfig) -> Self {
		self.config.data_cache = cache.to_owned();
		self
	}

	pub fn with_node_cache_config(mut self, cache: &HyperFileNodeCacheConfig) -> Self {
		self.config.node_cache = cache.to_owned();
		self
	}

	pub fn build(&self) -> HyperFileConfig {
		self.config.clone()
	}
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn meta_config_default_round_trip() {
        let cfg = HyperFileMetaConfig::default();
        let encoded = cfg.as_u32();
        let decoded = HyperFileMetaConfig::from_u32(encoded);
        assert_eq!(cfg, decoded);
    }

    #[test]
    fn meta_config_custom_round_trip() {
        let cfg = HyperFileMetaConfig::new(56, 8192, 65536, BlockPtrFormat::MicroGroup);
        let encoded = cfg.as_u32();
        let decoded = HyperFileMetaConfig::from_u32(encoded);
        assert_eq!(decoded.root_size, 56);
        assert_eq!(decoded.meta_block_size, 8192);
        assert_eq!(decoded.data_block_size, 65536);
        assert_eq!(decoded.block_ptr_format, BlockPtrFormat::MicroGroup);
    }

    #[test]
    fn meta_config_enforces_min_sizes() {
        // Passing values below minimum should be clamped up
        let cfg = HyperFileMetaConfig::new(56, 1, 1, BlockPtrFormat::Flat);
        assert!(cfg.meta_block_size >= MIN_META_BLOCK_SIZE);
        assert!(cfg.data_block_size >= MIN_DATA_BLOCK_SIZE);
    }

    #[test]
    fn meta_config_power_of_two_alignment() {
        // 5000 is not power-of-two; should be rounded down to 4096
        let cfg = HyperFileMetaConfig::new(56, 5000, 5000, BlockPtrFormat::Flat);
        assert_eq!(cfg.meta_block_size, 4096);
        assert_eq!(cfg.data_block_size, 4096);
    }

    #[test]
    fn meta_config_json_round_trip() {
        let cfg = HyperFileConfig::default();
        let json = cfg.to_json_string(false);
        let decoded = HyperFileConfig::from_json_string(&json).unwrap();
        assert_eq!(cfg, decoded);
    }

    #[test]
    fn meta_config_json_pretty() {
        let cfg = HyperFileConfig::default();
        let json = cfg.to_json_string(true);
        assert!(json.contains('\n'));
        let decoded = HyperFileConfig::from_json_string(&json).unwrap();
        assert_eq!(cfg, decoded);
    }

    #[test]
    fn meta_config_from_invalid_json() {
        let result = HyperFileConfig::from_json_string("not json");
        assert!(result.is_err());
    }

    /// A container written before `i_meta_config` was populated carries zero.
    /// That used to decode without complaint into a root of 0 bytes and
    /// blocks of 1 byte, and since the container's config overwrites the
    /// caller's, the 1-byte block size then decided how reads were cut up.
    #[test]
    fn meta_config_rejects_an_unpopulated_field() {
        let e = HyperFileMetaConfig::try_from_u32(0).unwrap_err();
        assert_eq!(e.kind(), ErrorKind::InvalidData);
        // Nop is reached first, being the low byte.
        assert!(format!("{}", e).contains("Nop"), "{}", e);
    }

    /// Every field is checked, not just the one that happens to be wrong in
    /// the containers that prompted this.
    #[test]
    fn meta_config_rejects_each_field_out_of_range() {
        let good = HyperFileMetaConfig::default();
        let ok = good.as_u32();
        assert_eq!(HyperFileMetaConfig::try_from_u32(ok).unwrap(), good,
            "a config this crate encoded must decode back");

        // Block ptr format this build does not know.
        assert!(HyperFileMetaConfig::try_from_u32((ok & !0xFF) | 0x07).is_err());

        // Data block shift below the minimum, and absurdly high. The high
        // case is the one that would shift by more than a usize has bits.
        let below = (ok & !0x0000FF00) | (11 << 8);
        assert!(HyperFileMetaConfig::try_from_u32(below).is_err(), "shift 11 accepted");
        let absurd = (ok & !0x0000FF00) | (200 << 8);
        assert!(HyperFileMetaConfig::try_from_u32(absurd).is_err(), "shift 200 accepted");

        // Meta block shift, same two ends.
        assert!(HyperFileMetaConfig::try_from_u32((ok & !0x00FF0000) | (11 << 16)).is_err());
        assert!(HyperFileMetaConfig::try_from_u32((ok & !0x00FF0000) | (200 << 16)).is_err());

        // Root size below the minimum.
        assert!(HyperFileMetaConfig::try_from_u32(ok & !0xFF000000).is_err());
    }

    /// The sizes hypercli round-trips must stay acceptable, so the guard
    /// rejects the unrepresentable rather than the merely unusual.
    #[test]
    fn meta_config_accepts_the_sizes_in_use() {
        for data_block_size in [4096usize, 65536, 524288] {
            let c = HyperFileMetaConfig::new(
                DEFAULT_ROOT_SIZE, DEFAULT_META_BLOCK_SIZE, data_block_size,
                DEFAULT_BLOCK_PTR_FORMAT);
            let back = HyperFileMetaConfig::try_from_u32(c.as_u32())
                .expect("a config in use must decode");
            assert_eq!(back.data_block_size, data_block_size);
            assert_eq!(back, c);
        }
    }
}
