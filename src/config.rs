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
const DEFAULT_BLOCK_PTR_FORMAT: BlockPtrFormat = BlockPtrFormat::Flat;

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
