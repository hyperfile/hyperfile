use std::io::Result;
use std::sync::Arc;
use std::collections::{HashMap, HashSet};
use log::debug;
use aws_sdk_s3::Client;
use tokio::sync::Mutex;
use btree_ondisk::BlockLoader;
use btree_ondisk::node::{BtreeNode, BTREE_NODE_LEVEL_MIN};
use crate::{BlockPtr, BMapUserData};
use crate::meta_format::BlockPtrFormat;
use crate::segment::Segment;
use crate::s3uri::S3Uri;
use crate::BlockIndex;
use crate::staging::s3::S3Staging;

pub struct S3BlockLoader {
    pub client: Client,
    pub bucket: String,
    pub root_path: String,
    backlog: Arc<Mutex<HashMap<u64, HashSet<BlockPtr>>>>, // key: segid, val: blkptr
}

impl Clone for S3BlockLoader {
    fn clone(&self) -> Self {
        Self {
            client: self.client.to_owned(),
            bucket: self.bucket.clone(),
            root_path: self.root_path.clone(),
            backlog: self.backlog.clone(),
        }
    }
}

impl S3BlockLoader {
    pub fn new(client: &Client, bucket: &str, root_path: &str) -> Self {
        Self {
            client: client.to_owned(),
            bucket: bucket.to_string(),
            root_path: root_path.to_string(),
            backlog: Arc::new(Mutex::new(HashMap::new())),
        }
    }
}

impl BlockLoader<BlockPtr> for S3BlockLoader {
    async fn read(&self, v: BlockPtr, buf: &mut [u8], user_data: u32) -> Result<Vec<(BlockPtr, Vec<u8>)>> {
        let meta_block_size = buf.len();
        let ud = BMapUserData::from_u32(user_data);
        let (this_segid, offset) = BlockPtrFormat::decode(&v, &ud.blk_ptr_format);
        let key = format!("{}/{}", self.root_path, Segment::segid_to_staging_file_id(this_segid));

        debug!("read meta blocks chunk from s3://{}/{} from s3", self.bucket, &key);
        let (meta_block_offset, seg_meta_block_size, mut meta_blocks, meta_block_buf) = S3Staging::do_fetch_meta_blocks_chunk(&self.client, &self.bucket, &key).await?;
        assert!(meta_block_size == seg_meta_block_size);
        assert!(offset >= meta_block_offset);

        buf.copy_from_slice(&meta_block_buf[offset - meta_block_offset..offset - meta_block_offset + meta_block_size]);

        let mut v_nonleaf_meta_block_ptr: Vec<(u64, BlockPtr)> = Vec::new();
        // for each meta block:
        // 1. get all next level block ptr
        // 2. rebuild block ptr for meta block itself
        let mut block_seq = 0;
        let mut file_off = meta_block_offset;
        let mut output = Vec::new();
        // TODO: more efficient way to split meta_block_buf into meta_block
        for meta_block_slice in meta_block_buf.chunks(meta_block_size) {
            let meta_block = meta_block_slice.to_vec();

            // decode nonleaf node, get back all next level block ptr
            let node = BtreeNode::<BlockIndex, BlockPtr>::from_slice(&meta_block);
            // we only care about meta data nodes
            if node.get_level() > BTREE_NODE_LEVEL_MIN {
                for idx in 0..node.get_nchild() {
                    let blk_ptr = *node.get_val(idx);
                    let (segid, _) = BlockPtrFormat::decode(&blk_ptr, &ud.blk_ptr_format);
                    // segid could be eq or lower than this_segid
                    v_nonleaf_meta_block_ptr.push((segid, *node.get_val(idx)));
                }
            }

            // rebuild bloock ptr for myself
            let block_ptr = BlockPtrFormat::encode(this_segid, file_off, block_seq, &ud.blk_ptr_format);
            output.push((block_ptr, meta_block));
            file_off += meta_block_size;
            block_seq += 1;
            meta_blocks -= 1;
        }
        assert!(meta_blocks == 0);

        // handle backlog
        let mut lock = self.backlog.lock().await;

        // insert all next level block ptr in lower segid into backlog
        for (segid, blk_ptr) in v_nonleaf_meta_block_ptr.into_iter() {
            if let Some(seg_group) = lock.get_mut(&segid) {
                seg_group.insert(blk_ptr);
            } else {
                // if segment id not found in current backlog, create new one as key
                let mut set = HashSet::new();
                set.insert(blk_ptr);
                lock.insert(segid, set);
            }
        }

        // get back segid list of this segid in backlog
        let this_segid_backlog: Option<HashSet<BlockPtr>> = lock.remove(&this_segid);

        drop(lock);

        // discard block ptr not in our backlog waiting list
        if let Some(segid_backlog) = this_segid_backlog {
            output.retain(|(blk_ptr, _)| segid_backlog.contains(blk_ptr));
            return Ok(output);
        }

        // if we don't have backlog wait list for this segment, just return blank vec
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
