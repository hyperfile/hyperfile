use std::io::{Error, ErrorKind, Result};
use std::sync::atomic::Ordering;
use std::path::Path;
use serde::{Deserialize, Serialize};
use foyer::{
    HybridCacheBuilder, HybridCache,
    BlockEngineBuilder, FsDeviceBuilder, DeviceBuilder,
    HybridCachePolicy, HybridCacheProperties, Location,
};
use btree_ondisk::{NodeCache, NodeTieredCacheStats};
use super::config::HyperFileNodeCacheConfig;

#[derive(Clone)]
pub struct LocalDiskNodeCache {
    hybrid: Option<HybridCache<u64, Vec<u8>>>,
    stats: NodeTieredCacheStats,
}

#[derive(Clone, Debug, Default, PartialEq, Deserialize, Serialize)]
pub enum LocalDiskNodeCacheOpenMode {
    #[default]
    Disabled,
    ReuseCache,
    ReuseSpace,
    Recreate,
}

impl Drop for LocalDiskNodeCache {
    fn drop(&mut self) {
        tokio::task::block_in_place(move || {
            tokio::runtime::Handle::current().block_on(async move {
                self.close().await
            })
        });
    }
}

impl<P: Send + Copy + Into<u64>> NodeCache<P> for LocalDiskNodeCache {
    fn push(&self, p: &P, data: &[u8]) {
        let key = (*p).into();
        self.stats.total_push.fetch_add(1, Ordering::SeqCst);
        let Some(ref hybrid) = self.hybrid else {
            return;
        };
        hybrid.insert_with_properties(key, data.to_vec(),
            HybridCacheProperties::default()
                .with_ephemeral(false)
                .with_location(Location::OnDisk)
        );
    }

    async fn load(&self, p: P, data: &mut [u8]) -> Result<bool> {
        let key = p.into();
        self.stats.total_load.fetch_add(1, Ordering::SeqCst);
        let Some(ref hybrid) = self.hybrid else {
            return Ok(false);
        };
        match hybrid.get(&key).await
                .map_err(|e| {
                    let err_msg = format!("{}", e);
                    Error::new(ErrorKind::Other, err_msg)
                })?
        {
            Some(entry) => {
                self.stats.total_hit.fetch_add(1, Ordering::SeqCst);
                data.copy_from_slice(&entry.value());
                Ok(true)
            },
            None => {
                Ok(false)
            },
        }
    }
    
    fn invalid(&self, p: &P) {
        let key = (*p).into();
        self.stats.total_remove.fetch_add(1, Ordering::SeqCst);
        let Some(ref hybrid) = self.hybrid else {
            return;
        };
        hybrid.remove(&key);
    }

    fn evict(&self) {
        let Some(ref hybrid) = self.hybrid else {
            return;
        };
        hybrid.memory().evict_all();
    }

    fn get_stats(&self) -> NodeTieredCacheStats {
        self.stats.clone()
    }
}

impl LocalDiskNodeCache {
    pub async fn from(config: &HyperFileNodeCacheConfig) -> Self {
        let HyperFileNodeCacheConfig::LocalDisk(c) = config;

        if c.mode == LocalDiskNodeCacheOpenMode::Disabled {
            return Self::new_none();
        }
        Self::new(&c.dir, c.capacity, c.mode.clone()).await
    }

    pub fn new_none() -> Self {
        Self {
            hybrid: None,
            stats: NodeTieredCacheStats::default(),
        }
    }

    pub async fn new(dir: impl AsRef<Path>, capacity: usize, mode: LocalDiskNodeCacheOpenMode) -> Self {
        if mode == LocalDiskNodeCacheOpenMode::Recreate {
            let _ = tokio::fs::remove_dir_all(&dir).await;
        }

        let cache_device = FsDeviceBuilder::new(dir)
            .with_capacity(capacity)
            .build()
            .expect("failed to build cache device");

        let hybrid: HybridCache<u64, Vec<u8>> = HybridCacheBuilder::new()
            .with_name("btncache")
            .with_policy(HybridCachePolicy::WriteOnInsertion)
            .memory(0)
            .storage()
            .with_engine_config(BlockEngineBuilder::new(cache_device))
            .build()
            .await
            .expect("failed to build cache");

        if mode == LocalDiskNodeCacheOpenMode::ReuseSpace {
            let _ = hybrid.storage().destroy().await.expect("failed to clear cache");
            let _ = hybrid.storage().wait().await;
        }

        Self {
            hybrid: Some(hybrid),
            stats: NodeTieredCacheStats::default(),
        }
    }

    pub async fn close(&self) {
        let Some(ref hybrid) = self.hybrid else {
            return;
        };
        hybrid.memory().flush().await;
        hybrid.close().await.expect("failed to close hybrid");
    }
}
