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
use super::{WalReadWrite, WalChunkDesc, WalBarrier};

pub(crate) struct S3Wal {
    pub(crate) client: Client,
    pub(crate) bucket: String,
    pub(crate) root_path: String,
    pub(crate) root_path_slash: String,  // root path with tail slash
    #[allow(dead_code)]
    pub(crate) data_block_size: usize,
    pub(crate) last_segid: SegmentId,
    pub(crate) seq: Arc<AtomicU64>,
    /// `(seq, offset, len)` of the records written since the last barrier.
    ///
    /// Kept here rather than asked of the caller because this is the side that
    /// assigns `seq`, so it is the only side that can state the group without
    /// being told. Cleared when the barrier is written.
    pub(crate) pending: Vec<(usize, usize, usize)>,
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
            pending: Vec::new(),
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
        self.pending.push((seq as usize, offset, len));
        format!("{}/{}/{}_{}_{}", self.root_path, seg_s, seq, offset, len)
    }

    #[inline]
    fn txn_key(&self, segid: SegmentId) -> String {
        // Same reasoning as `barrier_key`: not `seq_offset_len`, so the record
        // decoder rejects it.
        format!("{}/{}/txn", self.root_path, Segment::segid_to_staging_file_id(segid))
    }

    #[inline]
    fn barrier_key(&self, segid: SegmentId) -> String {
        // Deliberately not `seq_offset_len`, so `decode` rejects it and
        // `list_chunks` cannot mistake it for a record.
        format!("{}/{}/barrier", self.root_path, Segment::segid_to_staging_file_id(segid))
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
            buf.resize(len, 0);
            let res = S3Ops::do_get_object(&client, &bucket, &key, &mut buf, None, false).await;
            match res {
                Ok(_) => Ok(buf),
                Err(e) => Err(e),
            }
        })
    }

    fn write_barrier(&mut self, segid: SegmentId) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'static>> {
        // Nothing written since the last barrier means nothing to seal, and
        // writing an empty one would be worse than skipping it: records written
        // after it under the same checkpoint would then sit behind a barrier
        // claiming the group is empty, and recovery would drop them as
        // out-of-manifest.
        if self.pending.is_empty() {
            return Box::pin(async { Ok(()) });
        }
        let key = self.barrier_key(segid);
        let body = WalBarrier::new(std::mem::take(&mut self.pending)).encode();
        let client = self.client.clone();
        let bucket = self.bucket.clone();
        Box::pin(async move {
            S3Ops::do_put_object(&client, &bucket, &key, &body, &None).await.and(Ok(()))
        })
    }

    fn read_barrier(&self, segid: SegmentId) -> Pin<Box<dyn Future<Output = Result<Option<WalBarrier>>> + Send + '_>> {
        let key = self.barrier_key(segid);
        let client = self.client.clone();
        let bucket = self.bucket.clone();
        Box::pin(async move {
            match S3Ops::do_get_object_speculative(&client, &bucket, &key, None, false).await {
                // A body this build cannot parse is reported as absent, so
                // recovery stops rather than applying a group it cannot vouch
                // for.
                Ok((bytes, _)) => Ok(WalBarrier::decode(&bytes)),
                Err(e) if e.kind() == ErrorKind::NotFound => Ok(None),
                Err(e) => Err(e),
            }
        })
    }

    fn next_seq_peek(&self, segid: SegmentId) -> usize {
        if self.last_segid != segid {
            // `encode` resets the counter when the checkpoint changes, so the
            // first record under a new one starts from zero.
            return 0;
        }
        self.seq.load(Ordering::SeqCst) as usize
    }

    fn write_txn_marker(&mut self, segid: SegmentId, from_seq: usize) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'static>> {
        let key = self.txn_key(segid);
        let body = (from_seq as u64).to_ne_bytes().to_vec();
        let client = self.client.clone();
        let bucket = self.bucket.clone();
        Box::pin(async move {
            S3Ops::do_put_object(&client, &bucket, &key, &body, &None).await.and(Ok(()))
        })
    }

    fn read_txn_marker(&self, segid: SegmentId) -> Pin<Box<dyn Future<Output = Result<Option<usize>>> + Send + '_>> {
        let key = self.txn_key(segid);
        let client = self.client.clone();
        let bucket = self.bucket.clone();
        Box::pin(async move {
            match S3Ops::do_get_object_speculative(&client, &bucket, &key, None, false).await {
                Ok((bytes, _)) => {
                    if bytes.len() != 8 {
                        // Unreadable means "a transaction was open and this build
                        // cannot say where it started", which has to be treated
                        // as open from the beginning rather than ignored.
                        return Ok(Some(0));
                    }
                    let mut b = [0u8; 8];
                    b.copy_from_slice(&bytes[..8]);
                    Ok(Some(u64::from_ne_bytes(b) as usize))
                },
                Err(e) if e.kind() == ErrorKind::NotFound => Ok(None),
                Err(e) => Err(e),
            }
        })
    }

    fn delete_txn_marker(&mut self, segid: SegmentId) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'static>> {
        let key = self.txn_key(segid);
        let client = self.client.clone();
        let bucket = self.bucket.clone();
        Box::pin(async move {
            S3Ops::do_delete_object(&client, &bucket, &key, &None).await.and(Ok(()))
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

    fn delete_segment(&self, segid: SegmentId) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'static>> {
        let client = self.client.clone();
        let bucket = self.bucket.clone();
        let wal_segment_root_path = format!("{}{}/", self.root_path_slash, Segment::segid_to_staging_file_id(segid));
        Box::pin(async move {
            // Collect keys under the segid prefix, then batch-delete.
            let mut keys = Vec::new();
            {
                let keys_ref = &mut keys;
                let filter = |o: &aws_sdk_s3::types::Object| {
                    if let Some(key) = o.key() {
                        keys_ref.push(key.to_string());
                    }
                };
                S3Ops::do_list_objects(&client, &bucket, &wal_segment_root_path, filter).await?;
            }
            if keys.is_empty() {
                return Ok(());
            }
            S3Ops::do_delete_objects(&client, &bucket, keys).await
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Build an `S3Wal` with a dummy AWS client suitable for
    /// testing pure-logic methods (encode / decode / next_seq /
    /// reset_seq). The client is never actually invoked.
    fn wal_for_tests(root_path: &str, last_segid: SegmentId) -> S3Wal {
        let sdk_config = aws_config::SdkConfig::builder()
            .behavior_version(aws_config::BehaviorVersion::latest())
            .build();
        let client = Client::new(&sdk_config);
        let root_path = root_path.to_string();
        let root_path_slash = format!("{}/", root_path);
        S3Wal {
            client,
            bucket: "test-bucket".to_string(),
            root_path,
            root_path_slash,
            data_block_size: 4096,
            last_segid,
            seq: Arc::new(AtomicU64::new(0)),
            pending: Vec::new(),
        }
    }

    #[test]
    fn next_seq_increments_monotonically() {
        let wal = wal_for_tests("root", 0);
        assert_eq!(wal.next_seq(), 0);
        assert_eq!(wal.next_seq(), 1);
        assert_eq!(wal.next_seq(), 2);
        assert_eq!(wal.next_seq(), 3);
    }

    #[test]
    fn reset_seq_restarts_from_zero() {
        let wal = wal_for_tests("root", 0);
        let _ = wal.next_seq();
        let _ = wal.next_seq();
        let _ = wal.next_seq();
        wal.reset_seq();
        assert_eq!(wal.next_seq(), 0);
        assert_eq!(wal.next_seq(), 1);
    }

    #[test]
    fn encode_format_matches_expected_scheme() {
        let mut wal = wal_for_tests("root", 0);
        let key = wal.encode(0, 0, 4096);
        // Path shape: <root>/<padded-segid>/<seq>_<off>_<len>
        assert_eq!(key, "root/0000000000/0_0_4096");
    }

    #[test]
    fn encode_seq_advances_for_same_segid() {
        let mut wal = wal_for_tests("root", 0);
        assert_eq!(wal.encode(0, 0, 100), "root/0000000000/0_0_100");
        assert_eq!(wal.encode(0, 100, 50), "root/0000000000/1_100_50");
        assert_eq!(wal.encode(0, 200, 25), "root/0000000000/2_200_25");
    }

    #[test]
    fn encode_resets_seq_when_segid_changes() {
        let mut wal = wal_for_tests("root", 0);
        let _ = wal.encode(0, 0, 100);
        let _ = wal.encode(0, 100, 100);
        // New segid — seq should reset to 0.
        assert_eq!(wal.encode(1, 0, 200), "root/0000000001/0_0_200");
        assert_eq!(wal.encode(1, 200, 50), "root/0000000001/1_200_50");
    }

    #[test]
    fn encode_static_ignores_internal_seq_state() {
        let wal = wal_for_tests("root", 99);
        // encode_static takes seq as a parameter, doesn't touch
        // self.seq; repeated calls produce the same key.
        let k1 = wal.encode_static(7, 42, 1024, 2048);
        let k2 = wal.encode_static(7, 42, 1024, 2048);
        assert_eq!(k1, k2);
        assert_eq!(k1, "root/0000000042/7_1024_2048");
    }

    #[test]
    fn decode_roundtrip_after_encode() {
        let mut wal = wal_for_tests("root", 0);
        let key = wal.encode(5, 16384, 8192);
        // decode receives just the basename (the "<seq>_<off>_<len>"
        // tail), as produced by the trim_start_matches in
        // list_chunks. Extract it manually here.
        let basename = key.rsplit('/').next().unwrap();
        assert_eq!(wal.decode(basename), Some((0, 16384, 8192)));
    }

    #[test]
    fn decode_rejects_malformed_names() {
        let wal = wal_for_tests("root", 0);
        // Missing a component.
        assert_eq!(wal.decode("0_100"), None);
        // Too many components.
        assert_eq!(wal.decode("0_100_200_extra"), None);
        // Non-numeric parts.
        assert_eq!(wal.decode("a_100_200"), None);
        assert_eq!(wal.decode("0_x_200"), None);
        assert_eq!(wal.decode("0_100_z"), None);
        // Empty.
        assert_eq!(wal.decode(""), None);
    }

    #[test]
    fn decode_accepts_zero_values() {
        let wal = wal_for_tests("root", 0);
        assert_eq!(wal.decode("0_0_0"), Some((0, 0, 0)));
    }

    #[test]
    fn encode_does_not_reset_seq_when_same_segid_seen_twice_in_a_row() {
        let mut wal = wal_for_tests("root", 0);
        let _ = wal.encode(7, 0, 100); // initial: last_segid changes 0->7, reset, seq=0 returned
        let _ = wal.encode(7, 100, 100); // same segid: seq=1
        let k3 = wal.encode(7, 200, 100); // still same: seq=2
        assert!(k3.ends_with("/2_200_100"), "got: {}", k3);
    }
}
