use std::io::{Result, ErrorKind};
use std::collections::BTreeMap;
use std::time::Instant;
use rand::Rng;
use rand::prelude::SliceRandom;
use aws_sdk_s3::Client;
use reactor::LocalSpawner;
use hyperfile::BlockIndex;
use hyperfile::config::{HyperFileMetaConfig, HyperFileRuntimeConfig};
use hyperfile::file::fh::HyperFileHandler;
use hyperfile::file::handler::FileContext;
use hyperfile::file::hyper::Hyper;
use hyperfile::file::flags::FileFlags;
use hyperfile::buffer::DataBlockWrapper;

// 10 GiB
const MAX_FILE_SIZE: u64 = 10 * 1024 * 1024 * 1024;
const BLOCK_SIZE: u64 = 4096;
const VERIFY_CHUNK_SIZE: usize = 1 * 1024 * 1024 * 1024;
const SCATTER_BLOCKS_COUNT: usize = 1000_000; // 4GiB random data

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
    let meta_config = HyperFileMetaConfig::default();
    let mut runtime_config = HyperFileRuntimeConfig::default_large();
    runtime_config.segment_buffer_size = usize::MAX;
    runtime_config.data_cache_dirty_max_flush_interval = u64::MAX;
    runtime_config.data_cache_dirty_max_bytes_threshold = usize::MAX;
    runtime_config.data_cache_dirty_max_blocks_threshold = usize::MAX;
    let hyper = HyperFileHandler::fh_create_opt(spawner, client, uri, flags, &meta_config, &runtime_config).await?;
    Ok(hyper)
}

async fn verify(mut hyper: HyperFileHandler<'static>) -> Result<()> {
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

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();
	let uri = std::env::var("HYPERFILE_STAGING_ROOT_URI").unwrap();

    // build s3 client
	let config = aws_config::load_from_env().await;
    let client = aws_sdk_s3::Client::new(&config);

    // data prepare
    let mut blkidxs: Vec<BlockIndex> = (0..(MAX_FILE_SIZE/BLOCK_SIZE) as BlockIndex).collect();
    let mut rng = rand::rng();
    blkidxs.shuffle(&mut rng);

    let mut blocks = BTreeMap::new();
    for _ in 0..SCATTER_BLOCKS_COUNT {
        let blk_idx = blkidxs.pop().unwrap();
        let block = if rand::rng().random_ratio(8, 10) {
            let b = DataBlockWrapper::new(blk_idx, BLOCK_SIZE as usize, false);
            let buf = b.as_mut_slice();
            rand::fill(buf);
            b
        } else {
            DataBlockWrapper::new(blk_idx, BLOCK_SIZE as usize, true)
        };
        blocks.insert(blk_idx, block);
    }
    println!("{} of random blocks prepared...", SCATTER_BLOCKS_COUNT);

    let spawner = LocalSpawner::new_current();
    let mut hyper = prepare_hyper(&spawner, &client, &uri).await?;
    println!("write random data with fh_write* fn...");
    let start = Instant::now();
    for (_, block) in blocks.iter() {
        let off = (block.index() * BLOCK_SIZE) as usize;
        if block.is_zero() {
            let _ = hyper.fh_write_zero(off, block.size()).await?;
        } else {
            let buf = block.as_slice();
            let _ = hyper.fh_write(off, buf).await?;
        }
    }
    println!("  write data cost {:?}", start.elapsed());
    verify(hyper).await?;

    // get sorted vec for batch
    let v: Vec<DataBlockWrapper> = blocks.into_values().collect();

    let spawner = LocalSpawner::new_current();
    let mut hyper = prepare_hyper(&spawner, &client, &uri).await?;
    println!("write random data with batch fn...");
    let start = Instant::now();
    let _ = hyper.fh_write_aligned_batch(v).await?;
    println!("  write data cost {:?}", start.elapsed());
    verify(hyper).await?;

    Ok(())
}
