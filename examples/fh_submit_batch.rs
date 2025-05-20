use std::ops::Range;
use std::io::{Result, ErrorKind};
use std::io::{Seek, Write, Read};
use std::time::Instant;
use rand::Rng;
use aws_sdk_s3::Client;
use hyperfile::{BlockIndex, BlockIndexIter};
use hyperfile::config::{HyperFileMetaConfig, HyperFileRuntimeConfig};
use hyperfile::file::hyper::Hyper;
use hyperfile::file::fh::HyperFileHandler;
use hyperfile::file::flags::FileFlags;
use hyperfile::file::mode::FileMode;
use hyperfile::buffer::BatchDataBlockWrapper;
use hyperfile::file::handler::FileContext;
use hyperfile::utils;
use reactor::LocalSpawner;

const NUM_ITER: usize = 1000;
const VERIFY_CHUNK_SIZE: usize = 8 * 1024 * 1024;
const DEFAULT_BLOCK_SIZE: usize = 4096;
const DEFAULT_MAX_FILE_SIZE: usize = 100 * 1024 * 1024;
const DEFAULT_RANDOM_WRITE_OFFSET: Range<usize> = 0..DEFAULT_MAX_FILE_SIZE;
const DEFAULT_RANDOM_WRITE_BYTES: Range<usize> = 1..DEFAULT_BLOCK_SIZE*1024; // 1Byte to 4MiB

async fn prepare_hyper(spawner: &LocalSpawner<FileContext<'static>, Hyper<'static>>, client: &Client, uri: &str) -> Result<HyperFileHandler<'static>> {
    // clear any existing on staging
    let _ = Hyper::fs_unlink(client, uri).await
                        .or_else(|e| {
                            if e.kind() == ErrorKind::NotFound {
                                return Ok(());
                            }
                            Err(e)
                        })?;
    let flags = FileFlags::rdwr();
    let mode = FileMode::default_file();
    let meta_config = HyperFileMetaConfig::default();
    let mut runtime_config = HyperFileRuntimeConfig::default_large();
    runtime_config.data_cache_dirty_max_flush_interval = u64::MAX;
    runtime_config.data_cache_dirty_max_bytes_threshold = usize::MAX;
    runtime_config.data_cache_dirty_max_blocks_threshold = usize::MAX;
    let hyper = HyperFileHandler::fh_create_opt(spawner, client, uri, flags, mode, &meta_config, &runtime_config).await?;
    Ok(hyper)
}

async fn verify(hyper: &mut HyperFileHandler<'static>) -> Result<()> {
    let stat = hyper.fh_getattr().await?;
    println!("  file size {}", stat.st_size);
    let mut crc = crc64fast::Digest::new();
    let mut off = 0;
    let mut remains = stat.st_size as usize;
    let chunk_size = VERIFY_CHUNK_SIZE;
    while remains > 0 {
        let read_size = if remains > chunk_size {
            chunk_size
        } else {
            remains
        };
        let mut buf = Vec::new();
        buf.resize(read_size as usize, 0);
        let _ = hyper.fh_read(off, &mut buf).await?;
        crc.write(&buf);
        off += read_size as usize;
        remains -= read_size as usize;
    }
    let checksum = crc.sum64();
    println!("  file crc64 {}", checksum);
    Ok(())
}

fn verify_cur(mut cur: std::io::Cursor<Vec<u8>>) {
    let mut crc = crc64fast::Digest::new();
    let mut off = 0;
    let mut remains = cur.get_ref().len();
    let chunk_size = VERIFY_CHUNK_SIZE;
    while remains > 0 {
        let read_size = if remains > chunk_size {
            chunk_size
        } else {
            remains
        };
        let mut buf = Vec::new();
        buf.resize(read_size as usize, 0);
        let _ = cur.seek(std::io::SeekFrom::Start(off as u64));
        let _ = cur.read(&mut buf);
        crc.write(&buf);
        off += read_size as usize;
        remains -= read_size as usize;
    }
    let checksum = crc.sum64();
    println!("  buff crc64 {}", checksum);
}

fn random_write(write_zero: bool, part: bool) -> (usize, Vec<u8>) {
    let rand_data_offset = if part {
        rand::random_range(DEFAULT_RANDOM_WRITE_OFFSET) >> 9 << 9
    } else {
        rand::random_range(DEFAULT_RANDOM_WRITE_OFFSET)
    };
    let mut rand_data_bytes = if part {
        512
    } else {
        rand::random_range(DEFAULT_RANDOM_WRITE_BYTES)
    };
    if rand_data_offset + rand_data_bytes >= DEFAULT_MAX_FILE_SIZE {
        rand_data_bytes = DEFAULT_MAX_FILE_SIZE - rand_data_offset;
    }
    let mut data = Vec::with_capacity(rand_data_bytes);
    data.resize(rand_data_bytes, 0);
    if !write_zero {
        let buf = &mut data[..];
        rand::fill(buf);
    }
    (rand_data_offset, data)
}

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();
	let uri = std::env::var("HYPERFILE_STAGING_ROOT_URI").unwrap();

    // build s3 client
	let config = aws_config::load_from_env().await;
    let client = aws_sdk_s3::Client::new(&config);

    let mut write_chunks = Vec::new();
    for _ in 0..NUM_ITER {
        let (offset, data) = if rand::rng().random_ratio(4, 10) {
            random_write(false, false)
        } else if rand::rng().random_ratio(2, 10) {
            random_write(true, false)
        } else if rand::rng().random_ratio(2, 10) {
            random_write(false, true)
        } else {
            random_write(true, true)
        };
        write_chunks.push((offset, data));
    }

    let spawner = LocalSpawner::new_current();
    let mut hyper = prepare_hyper(&spawner, &client, &uri).await?;
    let start = Instant::now();

    // write in memory buffer
    let mut cur = std::io::Cursor::new(Vec::new());
    for (offset, data) in write_chunks.iter() {
        let _ = cur.seek(std::io::SeekFrom::Start(*offset as u64));
        let _ = cur.write(data);
    }
    verify_cur(cur);

    let mut batch: Vec<BatchDataBlockWrapper> = Vec::new();
    for (offset, data) in write_chunks.iter() {
        let blk_iter = BlockIndexIter::new(*offset, data.len(), DEFAULT_BLOCK_SIZE);
        let mut next_slice: &[u8] = data.as_slice();
        for (blk_idx, off, len) in blk_iter {
            let (this, next) = next_slice.split_at(len);
            if len == DEFAULT_BLOCK_SIZE {
                if utils::is_all_zeros(this) {
                    let block = BatchDataBlockWrapper::new(blk_idx as BlockIndex, len, true);
                    batch.push(block);
                } else {
                    let block = BatchDataBlockWrapper::new(blk_idx as BlockIndex, len, false);
                    let buf = block.as_mut_slice();
                    buf.copy_from_slice(this);
                    batch.push(block);
                }
            } else {
                assert!(len < DEFAULT_BLOCK_SIZE);
                if utils::is_all_zeros(this) {
                    let block = BatchDataBlockWrapper::new_partial_block(blk_idx as BlockIndex, DEFAULT_BLOCK_SIZE, off, len, true);
                    batch.push(block);
                } else {
                    let block = BatchDataBlockWrapper::new_partial_block(blk_idx as BlockIndex, DEFAULT_BLOCK_SIZE, off, len, false);
                    let buf = block.as_mut_slice();
                    buf.copy_from_slice(this);
                    batch.push(block);
                }
            };
            next_slice = next;
        }
    }

    let _ = hyper.fh_write_batch(batch).await?;
    println!("  write data cost {:?}", start.elapsed());
    verify(&mut hyper).await?;

    let start = Instant::now();
    let cno = hyper.fh_flush().await?;
    println!("  flush data cost {:?}, cno {}", start.elapsed(), cno);


    let flags = FileFlags::rdonly();
    let mut runtime_config = HyperFileRuntimeConfig::default_large();
    runtime_config.data_cache_dirty_max_flush_interval = u64::MAX;
    runtime_config.data_cache_dirty_max_bytes_threshold = usize::MAX;
    runtime_config.data_cache_dirty_max_blocks_threshold = usize::MAX;
    let mut hyper = HyperFileHandler::fh_open_opt(&spawner, &client, &uri, flags, &runtime_config).await?;

    verify(&mut hyper).await?;

    Ok(())
}
