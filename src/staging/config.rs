use serde::{Deserialize, Serialize};

const DEFAULT_INODE_FILE_NAME: &str = "inode";

#[derive(Clone, Debug, Default, PartialEq, Deserialize, Serialize)]
pub enum StagingType {
    S3,
    #[default]
    Blank,
}

#[derive(Clone, Debug, Default, PartialEq, Deserialize, Serialize)]
pub enum StagingInodeLocationType {
    WithinRootPath,
    #[default]
    Invalid,
}

#[derive(Clone, Debug, Default, PartialEq, Deserialize, Serialize)]
pub struct StagingConfig {
    pub typ: StagingType,
    pub inode_loc_type: StagingInodeLocationType,
    pub root_path: String,
    pub inode_file_path: String,
}

impl StagingConfig {
    // parse staging root path and inode file path based on origin key
    fn parse_input(origin_key: &str, staging_root_path: &str, inode_filename: Option<&str>) -> (String, String) {
        // remove leading slash of origin key if we have
        let origin_key = if let Some(remove_leading_slash) = origin_key.strip_prefix('/') {
            remove_leading_slash
        } else {
            origin_key
        };

        // remove tail slash of staging root if we have
        let staging_root_path = if let Some(remove_tail_slash) = staging_root_path.strip_suffix('/') {
            remove_tail_slash
        } else {
            staging_root_path
        };

        // concat to staging root for hyper file
        let root_path = if staging_root_path != "" {
            // for dump and test purpose origin key could be ""
            if origin_key == "" {
                staging_root_path.to_string()
            } else {
                format!("{}/{}", staging_root_path, origin_key)
            }
        } else {
            origin_key.to_string()
        };

        // concat inode
        let inode_file_path = if let Some(inode) = inode_filename {
            format!("{}/{}", root_path, inode)
        } else {
            format!("{}/{}", root_path, DEFAULT_INODE_FILE_NAME)
        };

        (root_path, inode_file_path.to_string())
    }

    pub fn new_s3_staging(bucket: &str, origin_key: &str, staging_root_path: &str, inode_filename: Option<&str>) -> Self {
        let (root_path, inode_file_path) = Self::parse_input(origin_key, staging_root_path, inode_filename);
        let root_path_uri = format!("s3://{}/{}", bucket, root_path);
        let inode_file_uri = format!("s3://{}/{}", bucket, inode_file_path);
        Self {
            typ: StagingType::S3,
            inode_loc_type: StagingInodeLocationType::WithinRootPath,
            root_path: root_path_uri,
            inode_file_path: inode_file_uri,
        }
    }

    // FIXME: simple config derive impl by replace, should be better
    pub fn derive(&self, old_key: &str, new_key: &str) -> Self {
        Self {
            typ: self.typ.clone(),
            inode_loc_type: self.inode_loc_type.clone(),
            root_path: self.root_path.clone().replace(old_key, new_key),
            inode_file_path: self.inode_file_path.clone().replace(old_key, new_key),
        }
    }

    pub fn from_staging_root(staging_root: &str, origin_key: &str) -> Self {
        let typ = if staging_root.starts_with("s3://") {
            StagingType::S3
        } else {
            panic!("invalid staging root from input {}", staging_root);
        };

        let (root_path, inode_file_path) = if staging_root.ends_with('/') {
            (
                format!("{}{}", staging_root, origin_key),
                format!("{}{}/inode", staging_root, origin_key)
            )
        } else {
            (
                format!("{}/{}", staging_root, origin_key),
                format!("{}/{}/inode", staging_root, origin_key)
            )
        };

        Self {
            typ: typ,
            inode_loc_type: StagingInodeLocationType::WithinRootPath,
            root_path: root_path,
            inode_file_path: inode_file_path,
        }
    }

    pub fn from_env(origin_key: &str) -> Self {
        let staging_root = std::env::var("HYPERFILE_STAGING_ROOT").unwrap();
        Self::from_staging_root(&staging_root, origin_key)
    }

    pub fn from_staging_uri(staging_uri: &str, origin_key: &str) -> Self {
        Self::from_staging_root(staging_uri, origin_key)
    }
}
