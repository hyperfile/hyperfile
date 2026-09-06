use std::ops::Range;
use std::time::{Duration, Instant};
use tokio::io::Result;
use crate::{BlockPtr, SegmentId};
use crate::staging::Staging;
use crate::segment::SegmentReadWrite;
use crate::config::HyperFileMetaConfig;
use btree_ondisk::BlockLoader;

pub async fn do_staging_benchmark_write<T: Staging<L> + SegmentReadWrite, L: BlockLoader<BlockPtr>>(staging: &T, segid: &mut SegmentId, buf: &[u8], iter: u64, config: &HyperFileMetaConfig) -> Result<()> {
    let mut total = Duration::new(0, 0);
    for _ in 0..iter {
        let mut segwr = staging.new_segwr(*segid, config);
        let _ = segwr.append(buf)?;
        let start = Instant::now();
        segwr.done().await?;
        total += start.elapsed();
        // A fresh checkpoint each time, because a segment object is written
        // create-only and reusing an id would be refused. Advancing by part instead
        // would collide: `new_segwr` puts a checkpoint's contents in part 0 whatever
        // part the id carries, so every iteration would name the same object.
        *segid = segid.next();
    }
    let avg = total / (iter as u32);
    println!("{} iters of WRITE {:>8} bytes, total time {:>12?}, avg latency {:>12?}", iter, buf.len(), total, avg);
    Ok(())
}

pub async fn do_staging_benchmark_read<T: Staging<L> + SegmentReadWrite, L: BlockLoader<BlockPtr>>(staging: &T, segid: &mut SegmentId, buf: &mut [u8], iter: u64, config: &HyperFileMetaConfig) -> Result<()> {
    let mut total = Duration::new(0, 0);
    for _ in 0..iter {
        let start = Instant::now();
        // The object the write went into, asked for the same way it was named.
        staging.load_data_block(
            crate::segment::Segment::object_of(*segid, config), 0, 0, buf.len(), buf).await?;
        total += start.elapsed();
        *segid = segid.next();
    }
    let avg = total / (iter as u32);
    println!("{} iters of READ  {:>8} bytes, total time {:>12?}, avg latency {:>12?}", iter, buf.len(), total, avg);
    Ok(())
}

pub async fn do_staging_benchmark<T: Staging<L> + SegmentReadWrite, L: BlockLoader<BlockPtr>>(staging: T, iter: u64, bit_shift: Range<usize>, force_clean: bool) -> Result<()> {

    if force_clean {
        print!("Staging benchmark force cleanup ...");
        staging.unlink().await?;
        println!(" Done");
    }

    // One config for both halves, so the reader asks for the object the writer made.
    let config = HyperFileMetaConfig::default();

    println!("Staging benchmark WRITE started ...");
    // write test
    let mut segid = SegmentId::new(1);
    for shift in bit_shift.clone() {
        let block_size = 1 << shift;
        // prepare local data buffer
        let mut buf = Vec::with_capacity(block_size);
        buf.resize(block_size, 0);
        rand::fill(&mut buf[..]);

        do_staging_benchmark_write(&staging, &mut segid, &buf, iter, &config).await?;
    }

    println!("Staging benchmark READ started ...");
    // read test
    let mut segid = SegmentId::new(1);
    for shift in bit_shift {
        let block_size = 1 << shift;
        // prepare local data buffer
        let mut buf = Vec::with_capacity(block_size);
        buf.resize(block_size, 0);

        do_staging_benchmark_read(&staging, &mut segid, &mut buf, iter, &config).await?;
    }

    // clean up
    print!("Staging benchmark cleanup ...");
    staging.unlink().await?;
    println!(" Done");
    println!("Staging benchmark completed");

    Ok(())
}
