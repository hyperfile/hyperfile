use std::io::{Error, ErrorKind, Result};
use std::path::Path;
use log::error;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Default, PartialEq, Deserialize, Serialize)]
pub struct MemCacheConfig {}

#[derive(Clone, Debug, Default, PartialEq, Deserialize, Serialize)]
pub struct LocalDiskCacheConfig {
    pub cache_dir: Option<String>,
    pub cache_file_path: Option<String>,
}

const DEFAULT_CACHE_DIR: &str = "/tmp";

impl LocalDiskCacheConfig {
    pub fn full_file_path(&self) -> Result<String> {
        let cache_dir_str = self.cache_dir.clone().unwrap_or(DEFAULT_CACHE_DIR.to_string());
        let cache_dir = Path::new(&cache_dir_str);
        if !cache_dir.try_exists()? {
            let err_msg = format!("cache dir {} for LocalDiskCache not found", cache_dir.display());
            error!("{}", err_msg);
            return Err(Error::new(ErrorKind::InvalidInput, err_msg));
        }

        let cache_file_path = self.cache_file_path.clone().unwrap_or(ulid::Ulid::new().to_string());
        if let Some(s) = cache_dir.join(cache_file_path).as_path().to_str() {
            return Ok(s.to_string());
        }
        let err_msg = "unable to get cache file";
        error!("{err_msg}");
        return Err(Error::new(ErrorKind::InvalidInput, err_msg));
    }
}

#[derive(Clone, Debug, PartialEq, Deserialize, Serialize)]
pub enum HyperFileDataCacheConfig {
    Memory(MemCacheConfig),
    LocalDisk(LocalDiskCacheConfig),
}

impl Default for HyperFileDataCacheConfig {
    fn default() -> Self {
        Self::Memory(MemCacheConfig::default())
    }
}

impl HyperFileDataCacheConfig {
    pub fn new_mem() -> HyperFileDataCacheConfig {
        Self::Memory(MemCacheConfig::default())
    }

    pub fn new_local_disk(cache_dir: Option<&str>, cache_file_path: Option<&str>) -> HyperFileDataCacheConfig {
        Self::LocalDisk(
            LocalDiskCacheConfig {
                cache_dir: cache_dir.and_then(|s| Some(s.to_string())),
                cache_file_path: cache_file_path.and_then(|s| Some(s.to_string())),
            }
        )
    }

    pub fn new_local_disk_default() -> HyperFileDataCacheConfig {
        Self::new_local_disk(None, None)
    }
}
