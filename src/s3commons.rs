use std::io::{Error, ErrorKind, Result};
use log::{error, warn};
use bytes::Buf;
use aws_sdk_s3::Client;
use aws_sdk_s3::error::SdkError;
use aws_sdk_s3::primitives::SdkBody;
use aws_sdk_s3::operation::head_object::HeadObjectOutput;
use aws_sdk_s3::types::{Object, ObjectIdentifier};
#[cfg(feature = "wal")]
use aws_sdk_s3::types::CommonPrefix;
use crate::inode::OnDiskState;

/// Map an S3 SDK error to the semantically closest [`std::io::ErrorKind`] so
/// callers (and crates layered on top of hyperfile) can react precisely
/// instead of treating every S3 failure as an opaque `Other`:
///
/// * missing object (404)                         -> `NotFound`
/// * auth/authorization (403)                     -> `PermissionDenied`
/// * conditional-write conflict (409/412)         -> `AlreadyExists`
/// * throttling / server-side (429, 5xx)          -> `ResourceBusy` (retryable)
/// * malformed / unsupported request (400/405/..) -> `InvalidInput`
/// * no HTTP response (timeout/dispatch/build)    -> `TimedOut`/`ConnectionReset`/`InvalidInput`
///
/// Conditional-write conflicts (409/412) are normally caught at the call site
/// for OCC; they are mapped here too for completeness.
pub(crate) fn s3_error_kind<E>(err: &SdkError<E>) -> ErrorKind {
    match err {
        SdkError::TimeoutError(_) => ErrorKind::TimedOut,
        SdkError::DispatchFailure(_) => ErrorKind::ConnectionReset,
        SdkError::ConstructionFailure(_) => ErrorKind::InvalidInput,
        _ => match err.raw_response().map(|r| r.status().as_u16()) {
            Some(404) => ErrorKind::NotFound,
            Some(403) => ErrorKind::PermissionDenied,
            Some(408) => ErrorKind::TimedOut,
            Some(409 | 412) => ErrorKind::AlreadyExists,
            Some(429 | 500 | 502 | 503 | 504) => ErrorKind::ResourceBusy,
            Some(400 | 405 | 411 | 416 | 501) => ErrorKind::InvalidInput,
            _ => ErrorKind::Other,
        },
    }
}

pub(crate) struct S3Ops;

impl S3Ops {
    #[cfg(feature = "wal")]
    pub(crate) async fn do_list_directory(client: &Client, bucket: &str, prefix: &str, mut f: impl FnMut(&CommonPrefix)) -> Result<()> {
        let mut stream = client
            .list_objects_v2()
            .bucket(bucket)
            .prefix(prefix)
            .delimiter("/")
            .into_paginator()
            .send();

        while let Some(page) = stream.next().await {
            match page {
                Ok(list_res) => {
                    if let Some(prefixes) = list_res.common_prefixes {
                        prefixes.iter().for_each(|p| f(p))
                    }
                },
                Err(sdk_err) => {
                    let mut err_str = format!("ListObjectV2 s3://{}/{} error: ", bucket, prefix);
                    if let Some(serv_err) = sdk_err.as_service_error() {
                        err_str.push_str(&format!("{}", serv_err));
                    } else {
                        err_str.push_str(&format!("{}", sdk_err));
                    };
                    error!("{}", err_str);
                    return Err(Error::new(s3_error_kind(&sdk_err), err_str));
                },
            }
        }
        Ok(())
    }

    pub(crate) async fn do_list_objects(client: &Client, bucket: &str, prefix: &str, mut f: impl FnMut(&Object)) -> Result<()> {
        let mut stream = client
            .list_objects_v2()
            .bucket(bucket)
            .prefix(prefix)
            .into_paginator()
            .send();

        while let Some(page) = stream.next().await {
            match page {
                Ok(list_res) => {
                    if let Some(objects) = list_res.contents {
                        objects.iter().for_each(|obj| f(obj))
                    }
                },
                Err(sdk_err) => {
                    let mut err_str = format!("ListObjectV2 s3://{}/{} error: ", bucket, prefix);
                    if let Some(serv_err) = sdk_err.as_service_error() {
                        err_str.push_str(&format!("{}", serv_err));
                    } else {
                        err_str.push_str(&format!("{}", sdk_err));
                    };
                    error!("{}", err_str);
                    return Err(Error::new(s3_error_kind(&sdk_err), err_str));
                },
            }
        }
        Ok(())
    }

    pub(crate) async fn do_head_object(client: &Client, bucket: &str, key: &str) -> Result<HeadObjectOutput> {
        let res = client
            .head_object()
            .bucket(bucket)
            .key(key)
            .send()
            .await;
        match res {
            Ok(output) => {
                return Ok(output);
            },
            Err(sdk_err) => {
                let mut err_str = format!("HeadObject s3://{}/{} error: ", bucket, key);
                if let Some(serv_err) = sdk_err.as_service_error() {
                    err_str.push_str(&format!("{}", serv_err));
                } else {
                    err_str.push_str(&format!("{}", sdk_err));
                };
                if sdk_err.as_service_error().map(|e| e.is_not_found()) == Some(true) {
                    warn!("{}", err_str);
                    return Err(Error::new(ErrorKind::NotFound, err_str));
                }
                error!("{}", err_str);
                return Err(Error::new(s3_error_kind(&sdk_err), err_str));
            }
        }
    }

    pub(crate) async fn do_delete_object(client: &Client, bucket: &str, key: &str,
            inode_state: &Option<OnDiskState>) -> Result<()>
    {
        let builder = client
            .delete_object()
            .bucket(bucket)
            .key(key);
        let op = if let Some(state) = inode_state {
                builder
                    .if_match(state.checksum.as_str())
                    .if_match_last_modified_time(aws_sdk_s3::primitives::DateTime::from_secs(state.timestamp))
            } else {
                builder
        };
        match op.send().await {
            Ok(_) => {
                // do nothing
            },
            Err(sdk_err) => {
                if let Some(resp) = sdk_err.raw_response() {
                    if resp.status().as_u16() == 412 {
                        // OCC conflict: another writer modified the object
                        // between our read and our delete. Surfaced as
                        // AlreadyExists so the flush retry loop can
                        // distinguish it from generic ResourceBusy.
                        let err_str = format!("Conditional DeleteObject failed on s3://{}/{}, status: 412 (concurrent modification)", bucket, key);
                        warn!("{}", err_str);
                        return Err(Error::new(ErrorKind::AlreadyExists, err_str));
                    }
                }
                let mut err_str = format!("DeleteObject s3://{}/{} error: ", bucket, key);
                if let Some(serv_err) = sdk_err.as_service_error() {
                    err_str.push_str(&format!("{}", serv_err));
                } else {
                    err_str.push_str(&format!("{}", sdk_err));
                };
                error!("{}", err_str);
                return Err(Error::new(s3_error_kind(&sdk_err), err_str));
            }
        }
        Ok(())
    }

    pub(crate) async fn do_get_object(client: &Client, bucket: &str, key: &str,
            buf: &mut [u8], range: Option<&str>, with_ods: bool) -> Result<Option<OnDiskState>>
    {
        let builder = client
            .get_object()
            .bucket(bucket)
            .key(key);
        let op = if let Some(r) = range {
            builder.range(r)
        } else {
            builder
        };
        match op.send().await {
            Ok(output) => {
                let mut bytes = output.body.collect().await?;
                if bytes.remaining() < buf.len() {
                    let err_str = format!("GetObject s3://{}/{} feched size {} less than input buffer size {}",
                        bucket, key, bytes.remaining(), buf.len());
                    error!("{}", err_str);
                    return Err(Error::new(ErrorKind::InvalidData, err_str));
                }
                bytes.copy_to_slice(buf);
                if with_ods {
                    let od_state = OnDiskState {
                        checksum: output.e_tag.unwrap().replace("\"", ""),
                        timestamp: output.last_modified.unwrap().secs(),
                    };
                    return Ok(Some(od_state));
                }
                return Ok(None);
            },
            Err(sdk_err) => {
                let mut err_str = if let Some(r) = range {
                    format!("GetObject s3://{}/{} by range {} error: ", bucket, key, r)
                } else {
                    format!("GetObject s3://{}/{} error: ", bucket, key)
                };
                if let Some(serv_err) = sdk_err.as_service_error() {
                    err_str.push_str(&format!("{}", serv_err));
                } else {
                    err_str.push_str(&format!("{}", sdk_err));
                };
                if sdk_err.as_service_error().map(|e| e.is_no_such_key()) == Some(true) {
                    warn!("{}", err_str);
                    return Err(Error::new(ErrorKind::NotFound, err_str));
                }
                error!("{}", err_str);
                return Err(Error::new(s3_error_kind(&sdk_err), err_str));
            },
        }
    }

    pub(crate) async fn do_put_object(client: &Client, bucket: &str, key: &str,
            buf: &[u8], inode_state: &Option<OnDiskState>) -> Result<Option<OnDiskState>>
    {
        let body = SdkBody::from(buf);
        let builder = client
            .put_object()
            .bucket(bucket)
            .key(key)
            .body(body.into());
        let op = if let Some(state) = inode_state {
            builder
                .if_match(state.checksum.as_str())
        } else {
            builder
                .if_none_match('*')
        };
        match op.send().await {
            Ok(output) => {
                let od_state = OnDiskState {
                    checksum: output.e_tag.unwrap().replace("\"", ""),
                    timestamp: 0,
                };
                return Ok(Some(od_state));
            },
            Err(sdk_err) => {
                if let Some(resp) = sdk_err.raw_response() {
                    match resp.status().as_u16() {
                        412 | 409 => {
                            // OCC conflict: another writer committed to the
                            // same key (412 = If-Match failed, 409 = bucket
                            // state conflict). Surfaced as AlreadyExists so
                            // the flush retry loop can apply FlushConflictPolicy
                            // (RetryLastWriterWins vs FailFast).
                            let err_str = format!("Conditional PutObject failed on s3://{}/{}, status: {} (concurrent modification)", bucket, key, resp.status().as_u16());
                            warn!("{}", err_str);
                            return Err(Error::new(ErrorKind::AlreadyExists, err_str));
                        },
                        _ => {},
                    }
                }
                let mut err_str = format!("PutObject s3://{}/{} error: ", bucket, key);
                if let Some(serv_err) = sdk_err.as_service_error() {
                    err_str.push_str(&format!("{}", serv_err));
                } else {
                    err_str.push_str(&format!("{}", sdk_err));
                };
                error!("{}", err_str);
                return Err(Error::new(s3_error_kind(&sdk_err), err_str));
            }
        }
    }

    // do multipart upload in concurrent
    pub(crate) async fn do_mp_upload(client: &Client, bucket: &str, key: &str,
            buf: &[u8], inode_state: &Option<OnDiskState>, mpu_chunk_size: usize) -> Result<Option<OnDiskState>>
    {
        let upload_id;
        let res = client
            .create_multipart_upload()
            .bucket(bucket)
            .key(key)
            .send()
            .await;
        match res {
            Ok(output) => {
                if let Some(id) = output.upload_id {
                    upload_id = id;
                } else {
                    let err_str = format!("CreateMultipartUpload s3://{}/{} error: unable to get a valid upload id", bucket, key);
                    error!("{}", err_str);
                    return Err(Error::new(ErrorKind::InvalidData, err_str));
                }
            },
            Err(sdk_err) => {
                let mut err_str = format!("CreateMultipartUpload s3://{}/{} error: ", bucket, key);
                if let Some(serv_err) = sdk_err.as_service_error() {
                    err_str.push_str(&format!("{}", serv_err));
                } else {
                    err_str.push_str(&format!("{}", sdk_err));
                };
                error!("{}", err_str);
                return Err(Error::new(s3_error_kind(&sdk_err), err_str));
            }
        }

        // do concurrent upload
        let mut complete_parts = Vec::new();
        let mut set: tokio::task::JoinSet<Result<(usize, String)>> = tokio::task::JoinSet::new();
        let parts = (buf.len() / mpu_chunk_size) + 1;
        for part_id in 1..=parts {
            let part_data = if part_id == parts {
                // last one
                &buf[(part_id - 1) * mpu_chunk_size..]
            } else {
                &buf[(part_id - 1) * mpu_chunk_size..part_id * mpu_chunk_size]
            };
            // FIXME:
            //  spawn need 'statc lifetime, but data from input not having static,
            //  use unsafe code to create a owned reference from mem ptr to avoid lifetime check error by spawn
            //  it is actually SAFE because after all spawned tasks joined below, slice ref will not be used anymore
            let part_data = unsafe {
                std::slice::from_raw_parts(part_data.as_ptr() as *const u8, part_data.len())
            };
            let uid = upload_id.clone();
            let pid = part_id.to_owned();
            let k = key.to_owned();
            let c = client.clone();
            let b = bucket.to_owned();
            set.spawn(async move {
                let etag = Self::do_upload_parts(&c, &b, &k, part_data, pid, &uid).await?;
                Ok((part_id, etag))
            });
        }
        while let Some(res) = set.join_next().await {
            let (part_id, etag) = res??;
            complete_parts.push((part_id as i32, etag));
        }
        complete_parts.sort_by_key(|t| t.0);

        let parts = complete_parts.into_iter().map(|p|
                aws_sdk_s3::types::builders::CompletedPartBuilder::default()
                .part_number(p.0)
                .e_tag(p.1)
                .build()
        ).collect();
        let completed = aws_sdk_s3::types::builders::CompletedMultipartUploadBuilder::default()
            .set_parts(Some(parts))
            .build();

        // complete
        let builder = client
            .complete_multipart_upload()
            .bucket(bucket)
            .key(key)
            .multipart_upload(completed)
            .upload_id(&upload_id);
        let op = if let Some(state) = inode_state {
            builder
                .if_match(state.checksum.as_str())
        } else {
            builder
                .if_none_match('*')
        };
        match op.send().await {
            Ok(output) => {
                let od_state = OnDiskState {
                    checksum: output.e_tag.unwrap().replace("\"", ""),
                    timestamp: 0,
                };
                return Ok(Some(od_state));
            },
            Err(sdk_err) => {
                if let Some(resp) = sdk_err.raw_response() {
                    match resp.status().as_u16() {
                        412 | 409 => {
                            // OCC conflict on the final multipart commit.
                            // Same semantics as do_put_object's conflict branch.
                            let err_str = format!("Conditional CompleteMultipartUpload failed on s3://{}/{}, status: {} (concurrent modification)",
                                bucket, key, resp.status().as_u16());
                            warn!("{}", err_str);
                            return Err(Error::new(ErrorKind::AlreadyExists, err_str));
                        },
                        _ => {},
                    }
                }
                let mut err_str = format!("CompleteMultipartUpload s3://{}/{} error: ", bucket, key);
                if let Some(serv_err) = sdk_err.as_service_error() {
                    err_str.push_str(&format!("{}", serv_err));
                } else {
                    err_str.push_str(&format!("{}", sdk_err));
                };
                error!("{}", err_str);
                return Err(Error::new(s3_error_kind(&sdk_err), err_str));
            }
        }
    }

    // return etag if request success
    async fn do_upload_parts(client: &Client, bucket: &str, key: &str,
            buf: &[u8], part_id: usize, upload_id: &str) -> Result<String>
    {
        let body = SdkBody::from(buf);
        let res = client
            .upload_part()
            .bucket(bucket)
            .key(key)
            .part_number(part_id as i32)
            .upload_id(upload_id)
            .body(body.into())
            .send()
            .await;
        match res {
            Ok(output) => {
                return Ok(output.e_tag.unwrap());
            },
            Err(sdk_err) => {
                let mut err_str = format!("UploadPart s3://{}/{} upload_id: {}, part_id: {}, error: ",
                    bucket, key, upload_id, part_id);
                if let Some(serv_err) = sdk_err.as_service_error() {
                    err_str.push_str(&format!("{}", serv_err));
                } else {
                    err_str.push_str(&format!("{}", sdk_err));
                };
                error!("{}", err_str);
                return Err(Error::new(s3_error_kind(&sdk_err), err_str));
            },
        }
    }

    pub(crate) async fn do_delete_objects(client: &Client, bucket: &str, delete_keys: Vec<String>) -> Result<()> {
        let mut err = false;
        for keys in delete_keys.chunks(1000).into_iter() {
            let obj_ids = keys.into_iter().map(|k| {
                    // FIXME: move the value, avoid ref
                    ObjectIdentifier::builder()
                        .key(k.to_string())
                        .build()
                        .unwrap()
                }).collect::<Vec<ObjectIdentifier>>();
            let delete = aws_sdk_s3::types::Delete::builder()
                    .set_objects(Some(obj_ids))
                    .quiet(true)
                    .build()
                    .unwrap();
            match client.delete_objects()
                .bucket(bucket)
                .delete(delete)
                .send()
                .await
            {
                Ok(_) => {},
                Err(sdk_err) => {
                    err = true;
                    // TODO: check delete objects result with signle delete error
                    error!("delete objects error: {}", sdk_err);
                }
            }
        }
        if err {
            return Err(Error::new(ErrorKind::Interrupted, "at least one of delete objects op failed"));
        }
        Ok(())
    }

    /// Scatter-gather variant of [`Self::do_put_object`]: the
    /// body is delivered as a list of `Bytes` pieces, streamed
    /// to the SDK via `http_body::Body` rather than copied into
    /// a single contiguous buffer. Used by the segment flush
    /// path under `done_pieces`. See `src/segment_body.rs` for
    /// the plumbing.
    pub(crate) async fn do_put_object_pieces(
        client: &Client,
        bucket: &str,
        key: &str,
        body: crate::segment_body::SegmentBody,
        inode_state: &Option<OnDiskState>,
    ) -> Result<Option<OnDiskState>> {
        let sdk_body = body.into_sdk_body();
        let builder = client
            .put_object()
            .bucket(bucket)
            .key(key)
            .body(sdk_body.into());
        let op = if let Some(state) = inode_state {
            builder.if_match(state.checksum.as_str())
        } else {
            builder.if_none_match('*')
        };
        match op.send().await {
            Ok(output) => {
                let od_state = OnDiskState {
                    checksum: output.e_tag.unwrap().replace("\"", ""),
                    timestamp: 0,
                };
                Ok(Some(od_state))
            }
            Err(sdk_err) => {
                if let Some(resp) = sdk_err.raw_response() {
                    match resp.status().as_u16() {
                        412 | 409 => {
                            // Same semantics as do_put_object's conflict branch.
                            let err_str = format!(
                                "Conditional PutObject (pieces) failed on s3://{}/{}, status: {} (concurrent modification)",
                                bucket, key, resp.status().as_u16(),
                            );
                            warn!("{}", err_str);
                            return Err(Error::new(ErrorKind::AlreadyExists, err_str));
                        }
                        _ => {}
                    }
                }
                let mut err_str = format!("PutObject (pieces) s3://{}/{} error: ", bucket, key);
                if let Some(serv_err) = sdk_err.as_service_error() {
                    err_str.push_str(&format!("{}", serv_err));
                } else {
                    err_str.push_str(&format!("{}", sdk_err));
                }
                error!("{}", err_str);
                Err(Error::new(s3_error_kind(&sdk_err), err_str))
            }
        }
    }

    /// Scatter-gather variant of [`Self::do_mp_upload`]. Each
    /// part is a `SegmentBody::slice(...)` covering a contiguous
    /// sub-range; slicing a piece list is O(N_pieces_in_range)
    /// and copies no bytes (`Bytes::slice` is refcount).
    pub(crate) async fn do_mp_upload_pieces(
        client: &Client,
        bucket: &str,
        key: &str,
        body: crate::segment_body::SegmentBody,
        _inode_state: &Option<OnDiskState>,
        mpu_chunk_size: usize,
    ) -> Result<Option<OnDiskState>> {
        let total_len = body.len();

        // Open the multipart upload.
        let upload_id;
        let res = client
            .create_multipart_upload()
            .bucket(bucket)
            .key(key)
            .send()
            .await;
        match res {
            Ok(output) => {
                if let Some(id) = output.upload_id {
                    upload_id = id;
                } else {
                    let err_str = format!(
                        "CreateMultipartUpload s3://{}/{} error: unable to get a valid upload id",
                        bucket, key,
                    );
                    error!("{}", err_str);
                    return Err(Error::new(ErrorKind::InvalidData, err_str));
                }
            }
            Err(sdk_err) => {
                let mut err_str = format!("CreateMultipartUpload s3://{}/{} error: ", bucket, key);
                if let Some(serv_err) = sdk_err.as_service_error() {
                    err_str.push_str(&format!("{}", serv_err));
                } else {
                    err_str.push_str(&format!("{}", sdk_err));
                }
                error!("{}", err_str);
                return Err(Error::new(s3_error_kind(&sdk_err), err_str));
            }
        }

        // Slice the body into per-part sub-bodies and upload in
        // parallel. Slicing only touches piece ref-counts; no
        // bytes are copied.
        let parts = total_len.div_ceil(mpu_chunk_size).max(1);
        let mut set: tokio::task::JoinSet<Result<(usize, String)>> =
            tokio::task::JoinSet::new();
        for part_id in 1..=parts {
            let start = (part_id - 1) * mpu_chunk_size;
            let part_len = if part_id == parts {
                total_len - start
            } else {
                mpu_chunk_size
            };
            let sub = body.slice(start, part_len);
            let uid = upload_id.clone();
            let pid = part_id;
            let k = key.to_owned();
            let c = client.clone();
            let b = bucket.to_owned();
            set.spawn(async move {
                let etag =
                    Self::do_upload_parts_pieces(&c, &b, &k, sub, pid, &uid).await?;
                Ok((part_id, etag))
            });
        }

        let mut complete_parts: Vec<(i32, String)> = Vec::new();
        while let Some(res) = set.join_next().await {
            let (pid, etag) = res??;
            complete_parts.push((pid as i32, etag));
        }
        complete_parts.sort_by_key(|t| t.0);
        let parts_built = complete_parts
            .into_iter()
            .map(|p| {
                aws_sdk_s3::types::builders::CompletedPartBuilder::default()
                    .part_number(p.0)
                    .e_tag(p.1)
                    .build()
            })
            .collect::<Vec<_>>();
        let completed = aws_sdk_s3::types::builders::CompletedMultipartUploadBuilder::default()
            .set_parts(Some(parts_built))
            .build();

        let res = client
            .complete_multipart_upload()
            .bucket(bucket)
            .key(key)
            .upload_id(&upload_id)
            .multipart_upload(completed)
            .send()
            .await;
        match res {
            Ok(output) => {
                let od_state = output.e_tag.map(|t| OnDiskState {
                    checksum: t.replace("\"", ""),
                    timestamp: 0,
                });
                Ok(od_state)
            }
            Err(sdk_err) => {
                let mut err_str = format!(
                    "CompleteMultipartUpload (pieces) s3://{}/{} upload_id: {}, error: ",
                    bucket, key, upload_id,
                );
                if let Some(serv_err) = sdk_err.as_service_error() {
                    err_str.push_str(&format!("{}", serv_err));
                } else {
                    err_str.push_str(&format!("{}", sdk_err));
                }
                error!("{}", err_str);
                Err(Error::new(s3_error_kind(&sdk_err), err_str))
            }
        }
    }

    async fn do_upload_parts_pieces(
        client: &Client,
        bucket: &str,
        key: &str,
        body: crate::segment_body::SegmentBody,
        part_id: usize,
        upload_id: &str,
    ) -> Result<String> {
        let sdk_body = body.into_sdk_body();
        let res = client
            .upload_part()
            .bucket(bucket)
            .key(key)
            .part_number(part_id as i32)
            .upload_id(upload_id)
            .body(sdk_body.into())
            .send()
            .await;
        match res {
            Ok(output) => Ok(output.e_tag.unwrap()),
            Err(sdk_err) => {
                let mut err_str = format!(
                    "UploadPart (pieces) s3://{}/{} upload_id: {}, part_id: {}, error: ",
                    bucket, key, upload_id, part_id,
                );
                if let Some(serv_err) = sdk_err.as_service_error() {
                    err_str.push_str(&format!("{}", serv_err));
                } else {
                    err_str.push_str(&format!("{}", sdk_err));
                }
                error!("{}", err_str);
                Err(Error::new(s3_error_kind(&sdk_err), err_str))
            }
        }
    }
}
