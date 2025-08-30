use std::io::{Error, ErrorKind, Result};
use log::{error, warn};
use bytes::Buf;
use aws_sdk_s3::Client;
use aws_sdk_s3::primitives::SdkBody;
use aws_sdk_s3::operation::head_object::HeadObjectOutput;
use aws_sdk_s3::types::{Object, ObjectIdentifier, CommonPrefix};
use crate::inode::OnDiskState;

pub(crate) struct S3Ops;

impl S3Ops {
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
                    return Err(Error::new(ErrorKind::Other, err_str));
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
                    return Err(Error::new(ErrorKind::Other, err_str));
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
                return Err(Error::new(ErrorKind::Other, err_str));
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
                        let err_str = format!("Conditional DeleteObject failed on s3://{}/{}, status: {}", bucket, key, resp.status().as_u16());
                        warn!("{}", err_str);
                        return Err(Error::new(ErrorKind::ResourceBusy, err_str));
                    }
                }
                let mut err_str = format!("DeleteObject s3://{}/{} error: ", bucket, key);
                if let Some(serv_err) = sdk_err.as_service_error() {
                    err_str.push_str(&format!("{}", serv_err));
                } else {
                    err_str.push_str(&format!("{}", sdk_err));
                };
                error!("{}", err_str);
                return Err(Error::new(ErrorKind::Other, err_str));
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
                return Err(Error::new(ErrorKind::Other, err_str));
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
                            let err_str = format!("Conditional PutObject failed on s3://{}/{}, status: {}", bucket, key, resp.status().as_u16());
                            warn!("{}", err_str);
                            return Err(Error::new(ErrorKind::ResourceBusy, err_str));
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
                return Err(Error::new(ErrorKind::Other, err_str));
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
                    return Err(Error::new(ErrorKind::Other, err_str));
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
                return Err(Error::new(ErrorKind::Other, err_str));
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
                            let err_str = format!("Conditional CompleteMultipartUpload failed on s3://{}/{}, status: {}",
                                bucket, key, resp.status().as_u16());
                            warn!("{}", err_str);
                            return Err(Error::new(ErrorKind::ResourceBusy, err_str));
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
                return Err(Error::new(ErrorKind::Other, err_str));
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
                return Err(Error::new(ErrorKind::Other, err_str));
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
}
