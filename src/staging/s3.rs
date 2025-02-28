use std::sync::Arc;
use log::{debug, error, warn};
use tokio::io::{Error, ErrorKind, Result};
use bytes::Buf;
use aws_sdk_s3::Client;
use aws_sdk_s3::primitives::SdkBody;
use aws_sdk_s3::types::Object;
use crate::staging::{Staging, FlushInodeFlag};
use crate::staging::config::StagingConfig;
use crate::{BlockIndex, BlockPtr, SegmentId};
use crate::segment;
use crate::segment::{Segment, SegmentSum};
use crate::config::{HyperFileMetaConfig, HyperFileRuntimeConfig};
use crate::ondisk::{SegmentHeader, InodeRaw};
use crate::s3uri::S3Uri;
use crate::meta_loader::s3::S3BlockLoader;
use crate::inode::OnDiskState;
use crate::s3commons::S3Ops;
use btree_ondisk::node::{BtreeNode, BTREE_NODE_LEVEL_DATA};
use super::StagingIntercept;

const SEGMENT_HEADER_FETCH_SIZE: usize = 512 * 1024;

#[derive(Clone)]
pub struct S3Staging {
    pub client: Client,
    pub bucket: String,
    pub root_path: String,
    pub root_path_slash: String,  // root path with tail slash
    pub inode_file: String, // full path of inode file
    pub config: StagingConfig,
    pub runtime_config: HyperFileRuntimeConfig,
    pub interceptor: Option<Arc<dyn StagingIntercept<Self>>>,
}

impl Staging<S3Staging, S3BlockLoader> for S3Staging {
    fn to_block_loader(&self) -> S3BlockLoader {
        S3BlockLoader::new(&self.client, &self.bucket, &self.root_path)
    }

    async fn load_inode(&self, buf: &mut [u8]) -> Result<Option<OnDiskState>> {
        S3Ops::do_get_object(&self.client, &self.bucket, &self.inode_file, buf, None, true).await
    }

    async fn load_inode_from_segment(&self, buf: &mut [u8], segid: SegmentId) -> Result<Option<OnDiskState>> {
        if segid == 0 {
            // read from latest inode
            return self.load_inode(buf).await;
        }

        let key = format!("{}/{}", self.root_path, Segment::segid_to_staging_file_id(segid));
        let inode_off = std::mem::offset_of!(SegmentHeader, s_inode);
        let inode_bytes = std::mem::size_of::<InodeRaw>();
        let range = format!("bytes={}-{}", inode_off, inode_off + inode_bytes - 1);
        S3Ops::do_get_object(&self.client, &self.bucket, &key, buf, Some(&range), false).await
    }

    async fn load_segment_timestamp(&self, segid: SegmentId) -> Result<(i64, i64)> {
        // get date from server response with interceptor
        use std::fmt;
        use std::sync::Arc;
        use aws_sdk_s3::config::ConfigBag;
        use aws_sdk_s3::config::interceptors::FinalizerInterceptorContextRef;
        use aws_sdk_s3::config::Intercept;
        use aws_sdk_s3::config::RuntimeComponents;
        use tokio::sync::Mutex;
        use chrono::DateTime;

        struct ResponseDateInterceptor {
            inner: Arc<Mutex<i64>>,
        }

        impl fmt::Debug for ResponseDateInterceptor {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(f, "")
            }
        }

        impl Intercept for ResponseDateInterceptor {
            fn name(&self) -> &'static str { "date" }

            fn read_after_execution(&self, ctx: &FinalizerInterceptorContextRef<'_>, _: &RuntimeComponents, _: &mut ConfigBag,)
                -> std::result::Result<(), Box<dyn std::error::Error + Send + Sync>>
            {
                if let Some(resp) = ctx.response() {
                    if let Some(date) = resp.headers().get("date") {
                        let dt = DateTime::parse_from_rfc2822(&date).unwrap();
                        if let Ok(mut lock) = self.inner.try_lock() {
                            *lock = dt.timestamp();
                        }
                    }
                }
                Ok(())
            }
        }

        let server_datetime = Arc::new(Mutex::new(0));
        let key = format!("{}/{}", self.root_path, Segment::segid_to_staging_file_id(segid));

        match self.client
            .head_object()
            .bucket(&self.bucket)
            .key(&key)
            .customize()
            .interceptor(ResponseDateInterceptor { inner: server_datetime.clone() })
            .send()
            .await
        {
            Ok(output) => {
                let server_dt = *server_datetime.lock().await;
                let lm = output.last_modified.unwrap().secs();
                return Ok((server_dt, lm));
            },
            Err(sdk_err) => {
                if sdk_err.as_service_error().map(|e| e.is_not_found()) == Some(true) {
                    let err_str = format!("HeadObject s3://{}/{} not exist", self.bucket, key);
                    error!("{}", err_str);
                    return Err(Error::new(ErrorKind::NotFound, err_str));
                }
                let err_str = format!("HeadObject s3://{}/{} error: {}", self.bucket, key, sdk_err);
                error!("{}", err_str);
                return Err(Error::new(ErrorKind::Other, err_str));
            },
        }
    }

    async fn flush_inode(&self, buf: &[u8], inode_state: &Option<OnDiskState>, flag: FlushInodeFlag) -> Result<Option<OnDiskState>> {
        let key = &self.inode_file;
        let inode_state = S3Ops::do_put_object(&self.client, &self.bucket, &self.inode_file, buf, inode_state).await?;
        if let Some(i) = &self.interceptor {
            let _ = i.after_flush_inode(&self, buf, flag).await;
        }
        Ok(inode_state)
    }

    async fn remove_inode(&self, inode_state: &Option<OnDiskState>) -> Result<()> {
        let key = &self.inode_file;
        if self.is_directory_bucket() {
            let _  = S3Ops::do_delete_object(&self.client, &self.bucket, &self.inode_file, inode_state).await?;
        } else {
            let _  = S3Ops::do_delete_object(&self.client, &self.bucket, &self.inode_file, &None).await?;
        }
        if let Some(i) = &self.interceptor {
            let _ = i.after_remove_inode(&self).await;
        }
        Ok(())
    }

    // load a single data block from staging
    async fn load_data_block(&self, segid: SegmentId, staging_off: usize, offset: usize, block_size: usize, buf: &mut [u8]) -> Result<()> {
        // open staging file for read
        let start_off = staging_off + offset;
        let end = start_off + block_size - 1;
        let key = format!("{}/{}", self.root_path, Segment::segid_to_staging_file_id(segid));
        let range = format!("bytes={}-{}", start_off, end);
        debug!("load_data_block from s3 staging s3://{}/{} at offset: {} range: {} block size: {}", &self.bucket, &key, start_off, &range, block_size);
        let _ = S3Ops::do_get_object(&self.client, &self.bucket, &key, buf, Some(&range), false).await?;
        Ok(())
    }

    fn new_segwr(&self, segid: SegmentId, hyper_file_config: &HyperFileMetaConfig) -> segment::Writer<S3Staging> {
        segment::Writer::<S3Staging>::new(self, self.runtime_config.segment_buffer_size, segid, hyper_file_config)
    }

    fn dir_filename(&self) -> (&str, &str) {
        if let Some((dir, filename)) = self.root_path.split_once('/') {
            return (dir, filename);
        }
        return ("", &self.root_path);
    }

    fn root_path(&self) -> &str {
        &self.root_path
    }

    fn config(&self) -> &StagingConfig {
        &self.config
    }

    async fn unlink(&self) -> Result<()> {
        self.remove_inode(&None).await?;

        let mut delete_keys = Vec::new();
        let filter = |obj: &Object| delete_keys.push(obj.key().unwrap().to_string());
        let _ = S3Ops::do_list_objects(&self.client, &self.bucket, &self.root_path_slash, filter).await?;
        debug!("unlink - found delete keys {:?}", delete_keys);

        let _ = S3Ops::do_delete_objects(&self.client, &self.bucket, delete_keys).await?;
        Ok(())
    }

    fn interceptor(&mut self, i: impl StagingIntercept<Self> + 'static) {
        self.interceptor = Some(Arc::new(i));
    }
}

impl S3Staging {
    async fn try_exists(client: &Client, bucket: &str, key: &str) -> Result<bool> {
        match S3Ops::do_head_object(client, bucket, key).await {
            Ok(_) => {
                return Ok(true);
            },
            Err(e) => {
                if e.kind() == ErrorKind::NotFound {
                    return Ok(false);
                }
                return Err(e);
            },
        }
    }

    pub fn pesudo(client: &Client, config: StagingConfig, runtime_config: HyperFileRuntimeConfig) -> Self {
        Self {
            client: client.to_owned(),
            bucket: String::new(),
            root_path: String::new(),
            root_path_slash: String::new(),
            inode_file: String::new(),
            config: config,
            runtime_config: runtime_config,
            interceptor: None,
        }
    }

    pub async fn from(client: &Client, config: StagingConfig, runtime_config: HyperFileRuntimeConfig) -> Result<Self> {
        // convert s3uri in config to bucket / key
        let s3uri = S3Uri::parse(&config.inode_file_uri).unwrap();
        let inode_file_path = s3uri.key.to_string();
        let inode_exists = Self::try_exists(client, s3uri.bucket, &inode_file_path).await?;
        if !inode_exists {
            return Err(Error::new(ErrorKind::NotFound, format!("inode not exists: {}", config.inode_file_uri)));
        }

        let s3uri = S3Uri::parse(&config.root_uri).unwrap();
        let root_path = s3uri.key.to_string();
        let mut root_path_slash = root_path.clone();
        root_path_slash.push('/');
        Ok(Self {
            client: client.to_owned(),
            bucket: s3uri.bucket.to_string(),
            root_path: root_path,
            root_path_slash: root_path_slash,
            inode_file: inode_file_path,
            config: config,
            runtime_config: runtime_config,
            interceptor: None,
        })
    }

    pub async fn create(client: &Client, config: StagingConfig, runtime_config: HyperFileRuntimeConfig) -> Result<Self> {
        // convert s3uri in config to bucket / key
        let s3uri = S3Uri::parse(&config.inode_file_uri).unwrap();
        let inode_file_path = s3uri.key.to_string();
        let inode_exists = Self::try_exists(client, s3uri.bucket, &inode_file_path).await?;
        if inode_exists {
            return Err(Error::new(ErrorKind::AlreadyExists, "inode exists"));
        }

        let s3uri = S3Uri::parse(&config.root_uri).unwrap();
        let root_path = s3uri.key.to_string();
        let mut root_path_slash = root_path.clone();
        root_path_slash.push('/');
        Ok(Self {
            client: client.to_owned(),
            bucket: s3uri.bucket.to_string(),
            root_path: root_path,
            root_path_slash: root_path_slash,
            inode_file: inode_file_path,
            config: config,
            runtime_config: runtime_config,
            interceptor: None,
        })
    }

    #[inline]
    fn is_directory_bucket(&self) -> bool {
        self.bucket.ends_with("--x-s3")
    }
}

impl segment::SegmentReadWrite for S3Staging {
    fn append(&self, segid: SegmentId, buf: &[u8]) -> Result<()> {
        debug!("appending to inner buffer {}/{} len {}", self.root_path, Segment::segid_to_staging_file_id(segid), buf.len());
        Ok(())
    }

    async fn done(&self, segid: SegmentId, buf: &[u8], len: usize) -> Result<()> {
        let key = format!("{}/{}", self.root_path, Segment::segid_to_staging_file_id(segid));
        debug!("bufwr done s3://{}/{} {}", &self.bucket, &key, len);
        let (data, _) = buf.split_at(len);

        // check if should use MPU
        if self.segment_should_mp_upload(len) {
            let _ = S3Ops::do_mp_upload(&self.client, &self.bucket, &key, data, &None, self.runtime_config.segment_mpu_chunk_size).await?;
            return Ok(());
        }

        let _ = S3Ops::do_put_object(&self.client, &self.bucket, &key, buf, &None).await?;
        Ok(())
    }

    async fn remove(&self, segid: SegmentId) -> Result<()> {
        let key = format!("{}/{}", self.root_path, Segment::segid_to_staging_file_id(segid));
        Self::do_remove(&self.client, &self.bucket, &key).await
    }

    async fn open(&self, segid: SegmentId) -> Result<SegmentSum> {
        let filename = format!("{}/{}", self.root_path, Segment::segid_to_staging_file_id(segid));
        Self::do_open(&self.client, &self.bucket, &filename).await
    }

    async fn list(&self, segid: SegmentId) -> Result<Vec<SegmentId>> {
        Self::do_list(&self.client, &self.bucket, &self.root_path_slash, segid).await
    }

    async fn build_block_map(&self, segid: SegmentId) -> Result<Vec<(BlockIndex, BlockPtr)>> {
        let filename = format!("{}/{}", self.root_path, Segment::segid_to_staging_file_id(segid));
        Self::do_build_block_map(&self.client, &self.bucket, &filename).await
    }
}

impl S3Staging {
    pub(crate) async fn do_remove(client: &Client, bucket: &str, key: &str) -> Result<()> {
        S3Ops::do_delete_object(client, bucket, key, &None).await
    }

    pub(crate) async fn do_open(client: &Client, bucket: &str, key: &str) -> Result<SegmentSum> {
        let mut buf = Vec::with_capacity(SEGMENT_HEADER_FETCH_SIZE);
        buf.resize(SEGMENT_HEADER_FETCH_SIZE, 0);
        // read at least min size of header data
        let range = format!("bytes={}-{}", 0, SEGMENT_HEADER_FETCH_SIZE);
        let _ = S3Ops::do_get_object(client, bucket, key, &mut buf, Some(&range), false).await?;

        let hdr_size = std::mem::size_of::<SegmentHeader>();
        if buf.len() < hdr_size {
            // if segment size is too small
            return Err(Error::new(ErrorKind::InvalidData, "incorrect segment header size"));
        }

        // check actual segment sum bytes
        let hdr_buf = &buf[0..hdr_size];
        let hdr = SegmentHeader::from_slice(&hdr_buf);
        let actual_ss_bytes = hdr.s_bytes as usize;
        let remain_bytes = actual_ss_bytes - SEGMENT_HEADER_FETCH_SIZE;
        if buf.len() < actual_ss_bytes {
            // readin remain bytes
            let mut remain_buf = Vec::with_capacity(remain_bytes);
            remain_buf.resize(remain_bytes, 0);
            let range = format!("bytes={}-{}", buf.len(), actual_ss_bytes - 1);
            let _ = S3Ops::do_get_object(client, bucket, key, &mut remain_buf, Some(&range), false).await?;
            buf.append(&mut remain_buf);
        }

        Ok(SegmentSum::from_slice(&buf))
    }

    // list staging dir for all segments id <= input segid, skip anythig else
    // if input segment id is 0, return all
    pub(crate) async fn do_list(client: &Client, bucket: &str, prefix: &str, segid: SegmentId) -> Result<Vec<SegmentId>> {
        let mut output = Vec::new();
        let filter = |obj: &Object| {
            let key = obj.key().unwrap();
            let filename = if let Some((_, f)) = key.rsplit_once('/') {
                f
            } else {
                key
            };
            if let Ok(id) = filename.parse::<u64>() {
                if segid == 0 || id <= segid {
                    output.push(id)
                }
            }
        };
        let _ = S3Ops::do_list_objects(client, bucket, prefix, filter).await?;
        output.sort();
        Ok(output)
    }

    pub async fn cli_list(client: &Client, bucket: &str, prefix: &str, segid: SegmentId) -> Result<Vec<SegmentId>> {
        Self::do_list(client, bucket, prefix, segid).await
    }

    pub(crate) async fn do_build_block_map(client: &Client, bucket: &str, key: &str) -> Result<Vec<(BlockIndex, BlockPtr)>> {
        let mut buf = Vec::with_capacity(SegmentHeader::size());
        buf.resize(SegmentHeader::size(), 0);
        // read in header
        let range = format!("bytes={}-{}", 0, SegmentHeader::size() - 1);
        let _ = S3Ops::do_get_object(client, bucket, key, &mut buf, Some(&range), false).await?;

        let hdr = SegmentHeader::from_slice(&buf);
        let meta_block_size = (1 << hdr.s_meta_blk_shift) as usize;
        let meta_blocks = hdr.s_nmetablk as usize;
        let meta_block_off = hdr.aligned_ss_bytes();
        if meta_blocks == 0 {
            return Ok(Vec::new());
        }

        let meta_blocks_len = (meta_block_size * meta_blocks) as usize;

        let mut buf = Vec::with_capacity(meta_blocks_len);
        buf.resize(meta_blocks_len, 0);
        // readin meta blocks
        let range = format!("bytes={}-{}", meta_block_off, meta_block_off + meta_blocks_len - 1);
        let _ = S3Ops::do_get_object(client, bucket, key, &mut buf, Some(&range), false).await?;

        // collect all valid entry from all level 0 node
        let mut v = Vec::new();
        for meta_block_slice in buf.chunks(meta_block_size) {
            let node = BtreeNode::<BlockIndex, BlockPtr>::from_slice(meta_block_slice);
            if node.get_level() == BTREE_NODE_LEVEL_DATA + 1 {
                for idx in 0..node.get_nchild() {
                    v.push((*node.get_key(idx), *node.get_val(idx)));
                }
            }
        }
        return Ok(v);
    }

    pub async fn cli_build_block_map(client: &Client, bucket: &str, key: &str) -> Result<Vec<(BlockIndex, BlockPtr)>> {
        Self::do_build_block_map(client, bucket, key).await
    }

    fn segment_should_mp_upload(&self, len: usize) -> bool {
        if len > self.runtime_config.segment_mpu_chunk_size {
            return true;
        }
        false
    }
}
