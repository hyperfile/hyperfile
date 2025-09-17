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
}
