use std::sync::Arc;
use std::pin::Pin;
use std::io::{Result, Error, ErrorKind};
use std::str::FromStr;
use std::sync::atomic::{AtomicU64, Ordering};
use bytes::BytesMut;
use aws_sdk_s3::Client;
use aws_sdk_s3::types::Object;
use crate::buffer::BatchDataBlockWrapper;
use crate::{segment::Segment, SegmentId};
use crate::s3commons::S3Ops;
use crate::s3uri::S3Uri;
use super::WalReadWrite;

pub(crate) struct S3Wal {
    pub(crate) client: Client,
    pub(crate) bucket: String,
    pub(crate) root_path: String,
    pub(crate) root_path_slash: String,  // root path with tail slash
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
    fn write<'a>(&'a mut self, segid: SegmentId, offset: usize, buf: &'a [u8]) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'a>> {
        let key = self.encode(segid, offset, buf.len());
        Box::pin(async move {
            S3Ops::do_put_object(&self.client, &self.bucket, &key, buf, &None).await.and(Ok(()))
        })
    }

    fn write_zero(&mut self, segid: SegmentId, offset: usize, len: usize) -> Pin<Box<dyn Future<Output = Result<()>> + Send + '_>> {
        let key = self.encode(segid, offset, len);
        let zero: Vec<u8> = Vec::new();
        Box::pin(async move {
            S3Ops::do_put_object(&self.client, &self.bucket, &key, &zero, &None).await.and(Ok(()))
        })
    }

    fn collect(&self, segid: SegmentId) -> Pin<Box<dyn Future<Output = Result<Vec<BatchDataBlockWrapper>>> + Send + '_>> {
        todo!();
    }
}
