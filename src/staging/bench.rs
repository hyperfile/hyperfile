use std::ops::Range;
use std::time::{Duration, Instant};
use tokio::io::Result;
use crate::{BlockPtr, SegmentId};
use crate::staging::Staging;
use crate::segment::SegmentReadWrite;
use crate::config::HyperFileMetaConfig;
use btree_ondisk::BlockLoader;

pub async fn do_staging_benchmark_write<T: Staging<T, L> + SegmentReadWrite, L: BlockLoader<BlockPtr>>(staging: &T, segid: &mut SegmentId, buf: &[u8], iter: u64) -> Result<()> {
    let mut total = Duration::new(0, 0);
    for _ in 0..iter {
        let hyper_file_config = HyperFileMetaConfig::default();
        let mut segwr = staging.new_segwr(*segid, &hyper_file_config);
        let _ = segwr.append(buf)?;
        let start = Instant::now();
        segwr.done().await?;
        total += start.elapsed();
        *segid += 1;
    }
    let avg = total / (iter as u32);
    println!("{} iters of WRITE {:>8} bytes, total time {:>12?}, avg latency {:>12?}", iter, buf.len(), total, avg);
    Ok(())
}

pub async fn do_staging_benchmark_read<T: Staging<T, L> + SegmentReadWrite, L: BlockLoader<BlockPtr>>(staging: &T, segid: &mut SegmentId, buf: &mut [u8], iter: u64) -> Result<()> {
    let mut total = Duration::new(0, 0);
    for _ in 0..iter {
        let start = Instant::now();
        staging.load_data_block(*segid, 0, 0, buf.len(), buf).await?;
        total += start.elapsed();
        *segid += 1;
    }
    let avg = total / (iter as u32);
    println!("{} iters of READ  {:>8} bytes, total time {:>12?}, avg latency {:>12?}", iter, buf.len(), total, avg);
    Ok(())
}

pub async fn do_staging_benchmark<T: Staging<T, L> + SegmentReadWrite, L: BlockLoader<BlockPtr>>(staging: T, iter: u64, bit_shift: Range<usize>, force_clean: bool) -> Result<()> {

    if force_clean {
        print!("Staging benchmark force cleanup ...");
        staging.unlink().await?;
        println!(" Done");
    }

    println!("Staging benchmark WRITE started ...");
    // write test
    let mut segid: SegmentId = 1;
    for shift in bit_shift.clone() {
        let block_size = 1 << shift;
        // prepare local data buffer
        let mut buf = Vec::with_capacity(block_size);
        buf.resize(block_size, 0);
        rand::fill(&mut buf[..]);

        do_staging_benchmark_write(&staging, &mut segid, &buf, iter).await?;
    }

    println!("Staging benchmark READ started ...");
    // read test
    let mut segid: SegmentId = 1;
    for shift in bit_shift {
        let block_size = 1 << shift;
        // prepare local data buffer
        let mut buf = Vec::with_capacity(block_size);
        buf.resize(block_size, 0);

        do_staging_benchmark_read(&staging, &mut segid, &mut buf, iter).await?;
    }

    // clean up
    print!("Staging benchmark cleanup ...");
    staging.unlink().await?;
    println!(" Done");
    println!("Staging benchmark completed");

    Ok(())
}
