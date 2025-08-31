use std::sync::Arc;
use std::pin::Pin;
use std::io::{Result, Error, ErrorKind};
use std::str::FromStr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::collections::BTreeMap;
use aws_sdk_s3::Client;
use crate::{segment::Segment, SegmentId};
use crate::s3commons::S3Ops;
use crate::s3uri::S3Uri;
use super::{WalReadWrite, WalChunkDesc};

pub(crate) struct S3Wal {
    pub(crate) client: Client,
    pub(crate) bucket: String,
    pub(crate) root_path: String,
    pub(crate) root_path_slash: String,  // root path with tail slash
    #[allow(dead_code)]
    pub(crate) data_block_size: usize,
    pub(crate) last_segid: SegmentId,
    pub(crate) seq: Arc<AtomicU64>,
}

impl S3Wal {
    pub(crate) fn from_uri(uri: &str, data_block_size: usize, last_segid: SegmentId) -> Result<Option<Box<dyn WalReadWrite + Send>>> {
        let Ok(s3uri) = S3Uri::parse(uri) else {
            return Err(Error::new(ErrorKind::InvalidInput, "failed to parse wal config from uri"));
        };
        let bucket = s3uri.bucket.to_string();
        let root_path = s3uri.key.trim_end_matches("/").to_string();
        let root_path_slash = format!("{}/", root_path);
        let config = tokio::task::block_in_place(move || {
            tokio::runtime::Handle::current().block_on(async move {
                aws_config::load_from_env().await
            })
        });
        let client = Client::new(&config);
        let s = Self {
            client,
            bucket,
            root_path,
            root_path_slash,
            data_block_size,
            last_segid,
            seq: Arc::new(AtomicU64::new(0)),
        };
        Ok(Some(Box::new(s)))
    }

    #[inline]
    fn next_seq(&self) -> u64 {
        self.seq.fetch_add(1, Ordering::SeqCst)
    }

    #[inline]
    fn reset_seq(&self) {
        self.seq.store(0, Ordering::SeqCst);
    }

    #[inline]
    fn encode(&mut self, segid: SegmentId, offset: usize, len: usize) -> String {
        let seg_s = Segment::segid_to_staging_file_id(segid);
        if self.last_segid != segid {
            self.last_segid = segid;
            self.reset_seq();
        }
        let seq = self.next_seq();
        format!("{}/{}/{}_{}_{}", self.root_path, seg_s, seq, offset, len)
    }

    #[inline]
    fn encode_static(&self, seq: usize, segid: SegmentId, offset: usize, len: usize) -> String {
        let seg_s = Segment::segid_to_staging_file_id(segid);
        format!("{}/{}/{}_{}_{}", self.root_path, seg_s, seq, offset, len)
    }

    // return: (seq, offset, len)
    #[inline]
    fn decode(&self, objname: &str) -> Option<(usize, usize, usize)> {
        let parts: Vec<&str> = objname.split('_').collect();
        if parts.len() != 3 {
            return None;
        }
        let Ok(seq) = usize::from_str(parts[0]) else {
            return None;
        };
        let Ok(off) = usize::from_str(parts[1]) else {
            return None;
        };
        let Ok(len) = usize::from_str(parts[2]) else {
            return None;
        };
        Some((seq, off, len))
    }
}

impl WalReadWrite for S3Wal {
    fn write(&mut self, segid: SegmentId, offset: usize, buf: &[u8]) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'static>> {
        let key = self.encode(segid, offset, buf.len());
        let buf_dup = unsafe {
            std::slice::from_raw_parts(buf.as_ptr() as *const u8, buf.len())
        };
        let client = self.client.clone();
        let bucket = self.bucket.clone();
        Box::pin(async move {
            S3Ops::do_put_object(&client, &bucket, &key, buf_dup, &None).await.and(Ok(()))
        })
    }

    fn write_zero(&mut self, segid: SegmentId, offset: usize, len: usize) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'static>> {
        let key = self.encode(segid, offset, len);
        let client = self.client.clone();
        let bucket = self.bucket.clone();
        Box::pin(async move {
            let zero: Vec<u8> = Vec::new();
            S3Ops::do_put_object(&client, &bucket, &key, &zero, &None).await.and(Ok(()))
        })
    }

    fn read(&self, seq: usize, segid: SegmentId, offset: usize, len: usize) -> Pin<Box<dyn Future<Output = Result<Vec<u8>>> + Send + '_>> {
        let key = self.encode_static(seq, segid, offset, len);
        let client = self.client.clone();
        let bucket = self.bucket.clone();
        Box::pin(async move {
            let mut buf = Vec::with_capacity(len);
            let res = S3Ops::do_get_object(&client, &bucket, &key, &mut buf, None, false).await;
            match res {
                Ok(_) => Ok(buf),
                Err(e) => Err(e),
            }
        })
    }

    // get segment ids by list first level of directory with delimit
    fn list_segments(&self) -> Pin<Box<dyn Future<Output = Result<Vec<SegmentId>>> + Send + '_>> {
        let client = self.client.clone();
        let bucket = self.bucket.clone();
        let root_path_slash = self.root_path_slash.clone();
        Box::pin(async move {
            let mut v = Vec::new();
            let filter = |c: &aws_sdk_s3::types::CommonPrefix| {
                if let Some(prefix) = c.prefix() {
                    let prefix = prefix.trim_end_matches('/');
                    let segid_str = prefix.trim_start_matches(&root_path_slash);
                    if let Ok(segid) = segid_str.parse::<u64>() {
                        v.push(segid);
                    }
                }
            };
            let res = S3Ops::do_list_directory(&client, &bucket, &root_path_slash, filter).await;
            match res {
                Ok(_) => {
                    v.sort();
                    Ok(v)
                },
                Err(e) => Err(e),
            }
        })
    }

    // list ondisk wal chunks by segment id
    fn list_chunks(&self, segid: SegmentId) -> Pin<Box<dyn Future<Output = Result<BTreeMap<usize, WalChunkDesc>>> + Send + '_>> {
        let client = self.client.clone();
        let bucket = self.bucket.clone();
        let wal_segment_root_path = format!("{}{}/", self.root_path_slash, Segment::segid_to_staging_file_id(segid));
        Box::pin(async move {
            let mut map = BTreeMap::new();
            let filter = |o: &aws_sdk_s3::types::Object| {
                if let Some(key) = o.key() {
                    let objname = key.trim_start_matches(&wal_segment_root_path);
                    if let Some((seq, offset, len)) = self.decode(&objname) {
                        let ondisk_size = o.size().expect("unable to get object size");
                        let is_zero = if ondisk_size == 0 {
                            true
                        } else {
                            false
                        };
                        map.insert(seq, WalChunkDesc { seq, segid, key: key.to_string(), offset, len, is_zero });
                    }
                }
            };
            let res = S3Ops::do_list_objects(&client, &bucket, &wal_segment_root_path, filter).await;
            match res {
                Ok(_) => Ok(map),
                Err(e) => Err(e),
            }
        })
    }
}
