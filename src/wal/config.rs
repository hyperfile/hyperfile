use std::io::Result;
use log::{warn, error};
use serde::{Deserialize, Serialize};
use crate::SegmentId;
use super::WalReadWrite;
use super::s3::S3Wal;

#[derive(Clone, Debug, Default, PartialEq, Deserialize, Serialize)]
pub struct HyperFileWalConfig {
    pub root_uri: String,
}

impl HyperFileWalConfig {
    pub fn new(uri: &str) -> Self {
        Self {
            root_uri: uri.to_string(),
        }
    }

    pub(crate) fn to_wal(&self, data_block_size: usize, last_segid: SegmentId) -> Result<Option<Box<dyn WalReadWrite + Send>>> {
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
