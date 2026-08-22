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

    // decode from u32
    pub fn from_u32(data: u32) -> Self {
        let block_ptr_format = BlockPtrFormat::from_u8((data & 0xFF) as u8);
        let data_block_size = 1 << ((data >> 8) & 0xFF);
        let meta_block_size = 1 << ((data >> 16) & 0xFF);
        let root_size = (((data >> 24) & 0xFF) * 8).try_into().unwrap();
        Self {
            root_size,
            meta_block_size,
            data_block_size,
            block_ptr_format,
        }
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
}
