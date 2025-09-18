use std::str::FromStr;
use serde::{Deserialize, Serialize};
use super::localdisk::LocalDiskNodeCacheOpenMode;

#[derive(Clone, Debug, Default, PartialEq, Deserialize, Serialize)]
pub struct LocalDiskNodeCacheConfig {
    pub dir: String,
    pub capacity: usize,
    pub mode: LocalDiskNodeCacheOpenMode,
}

#[derive(Clone, Debug, PartialEq, Deserialize, Serialize)]
pub enum HyperFileNodeCacheConfig {
    LocalDisk(LocalDiskNodeCacheConfig),
}

impl Default for HyperFileNodeCacheConfig {
    fn default() -> Self {
        Self::new_local_disk("", 0, LocalDiskNodeCacheOpenMode::Disabled)
    }
}

impl HyperFileNodeCacheConfig {
    pub fn new_local_disk(dir: &str, capacity: usize, mode: LocalDiskNodeCacheOpenMode) -> HyperFileNodeCacheConfig {
        Self::LocalDisk(
            LocalDiskNodeCacheConfig {
                dir: dir.to_string(),
                capacity,
                mode,
            }
        )
    }

    // format type,dir,capacity,mode
    pub fn from_str(s: &str) -> Self {
        let params: Vec<&str> = s.split(',').collect();

        loop {
            if params.len() != 4 || params[0] != "LocalDisk" ||
                 params[1] == "" || params[2] == "" ||
                 params[3] == "" || params[3] == "Disabled"
            { break; }

            let dir = params[1];

            let Ok(capacity) = usize::from_str(params[2]) else {
                break;
            };

            let mode = match params[3] {
                "Disabled" => LocalDiskNodeCacheOpenMode::Disabled,
                "ReuseCache" => LocalDiskNodeCacheOpenMode::ReuseCache,
                "ReuseSpace" => LocalDiskNodeCacheOpenMode::ReuseSpace,
                "Recreate" => LocalDiskNodeCacheOpenMode::Recreate,
                _ => break,
            };

            return Self::new_local_disk(dir, capacity, mode);
        }

        Self::default()
    }
}
