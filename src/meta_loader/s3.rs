use std::io::Result;
use log::debug;
use aws_sdk_s3::Client;
use btree_ondisk::BlockLoader;
use crate::{BlockPtr, BMapUserData};
use crate::meta_format::BlockPtrFormat;
use crate::segment::Segment;
use crate::s3uri::S3Uri;
use crate::s3commons::S3Ops;

pub struct S3BlockLoader {
    pub client: Client,
    pub bucket: String,
    pub root_path: String,
}

impl Clone for S3BlockLoader {
    fn clone(&self) -> Self {
        Self {
            client: self.client.to_owned(),
            bucket: self.bucket.clone(),
            root_path: self.root_path.clone(),
        }
    }
}

impl S3BlockLoader {
    pub fn new(client: &Client, bucket: &str, root_path: &str) -> Self {
        Self {
            client: client.to_owned(),
            bucket: bucket.to_string(),
            root_path: root_path.to_string(),
        }
    }
}

impl BlockLoader<BlockPtr> for S3BlockLoader {
    async fn read(&self, v: BlockPtr, buf: &mut [u8], user_data: u32) -> Result<Vec<(BlockPtr, Vec<u8>)>> {
        let meta_block_size = buf.len();
        let ud = BMapUserData::from_u32(user_data);
        let (segid, offset) = BlockPtrFormat::decode(&v, &ud.blk_ptr_format);
        let key = format!("{}/{}", self.root_path, Segment::segid_to_staging_file_id(segid));
        let end = offset + meta_block_size - 1;
        let range = format!("bytes={}-{}", offset, end);
        debug!("read s3://{}/{} from s3 at offset {} for range {}", self.bucket, &key, offset, range);
        let _ = S3Ops::do_get_object(&self.client, &self.bucket, &key, buf, Some(&range), false).await?;
        // TODO return more
        Ok(Vec::new())
    }

    fn from_new_path(self, new_path: &str) -> Self {
        let s3uri = S3Uri::parse(new_path).expect("input new path is not a valid s3 uri");
        let mut clone = self.clone();
        clone.bucket = s3uri.bucket.to_string();
        clone.root_path = s3uri.key.to_string();
        clone
    }
}
