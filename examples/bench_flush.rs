//! Flush benchmark: sequential write + flush latency under the
//! reactor API.
//!
//! Runs N iterations per payload size. Each iteration:
//!   - unlinks any prior state for the URI;
//!   - creates a fresh file;
//!   - writes the payload to offset 0;
//!   - flushes, timing only the flush call;
//!   - releases.
//!
//! Prints a table of `{payload, iter, total, avg, p50, p95}` to
//! stdout. Run twice to compare `concurrent-segment-build` on vs
//! off:
//!
//! ```bash
//! # Default (concurrent-segment-build OFF)
//! HYPERFILE_STAGING_ROOT_URI=s3://my-bucket/bench-flush \
//!     cargo run --release --example bench_flush
//!
//! # With concurrent-segment-build ON
//! HYPERFILE_STAGING_ROOT_URI=s3://my-bucket/bench-flush \
//!     cargo run --release --features concurrent-segment-build \
//!     --example bench_flush
//! ```
//!
//! Bucket + credentials come from the AWS SDK default provider
//! chain (env vars, `~/.aws/credentials`, IMDS, ...).

use std::io::Result;
use std::time::{Duration, Instant};

use aws_sdk_s3::Client;
use hyperfile::config::{
    HyperFileConfig, HyperFileConfigBuilder, HyperFileMetaConfig, HyperFileRuntimeConfig,
};
use hyperfile::file::flags::FileFlags;
use hyperfile::file::hyper::Hyper;
use hyperfile::file::mode::FileMode;
use hyperfile::file::tokio_wrapper::HyperFileTokio;
use hyperfile::staging::config::StagingConfig;
use tokio::io::AsyncWriteExt;

/// Default payload sizes and iterations. Override via env vars:
///   HYPERFILE_BENCH_SIZES="1048576,8388608,33554432"
///   HYPERFILE_BENCH_ITERS=20
const DEFAULT_SIZES: &[usize] = &[
    1 * 1024 * 1024,  //  1 MiB
    8 * 1024 * 1024,  //  8 MiB
    32 * 1024 * 1024, // 32 MiB
];
const DEFAULT_ITERS: usize = 20;

fn feature_label() -> &'static str {
    if cfg!(feature = "concurrent-segment-build") {
        "concurrent-segment-build=ON"
    } else {
        "concurrent-segment-build=OFF"
    }
}

fn parse_sizes_env() -> Vec<usize> {
    match std::env::var("HYPERFILE_BENCH_SIZES") {
        Ok(s) => s
            .split(',')
            .map(|t| t.trim().parse::<usize>().expect("HYPERFILE_BENCH_SIZES: invalid size"))
            .collect(),
        Err(_) => DEFAULT_SIZES.to_vec(),
    }
}

fn parse_iters_env() -> usize {
    match std::env::var("HYPERFILE_BENCH_ITERS") {
        Ok(s) => s.parse::<usize>().expect("HYPERFILE_BENCH_ITERS: not a usize"),
        Err(_) => DEFAULT_ITERS,
    }
}

/// Percentile over a slice of Duration, 0.0 <= p <= 1.0. Uses
/// the nearest-rank method; sample must be non-empty.
fn percentile(sorted: &[Duration], p: f64) -> Duration {
    assert!(!sorted.is_empty());
    let rank = ((p * sorted.len() as f64).ceil() as usize).saturating_sub(1);
    sorted[rank.min(sorted.len() - 1)]
}

fn fmt_ms(d: Duration) -> String {
    format!("{:>8.2} ms", d.as_secs_f64() * 1000.0)
}

fn fmt_bytes(n: usize) -> String {
    let mib = n as f64 / (1024.0 * 1024.0);
    format!("{:>6.1} MiB", mib)
}

async fn bench_one_size(
    client: &Client,
    uri: &str,
    meta_config: &HyperFileMetaConfig,
    runtime_config: &HyperFileRuntimeConfig,
    payload_size: usize,
    iters: usize,
) -> Result<()> {
    let staging_config = StagingConfig::new_s3_uri(uri, None);
    let file_config: HyperFileConfig = HyperFileConfigBuilder::new()
        .with_meta_config(meta_config)
        .with_staging_config(&staging_config)
        .with_runtime_config(runtime_config)
        .build();

    // Prepare the payload once. Zeroed content is fine for this
    // measurement: we're timing the flush pipeline, not compression.
    let payload = vec![0xABu8; payload_size];

    let mut samples: Vec<Duration> = Vec::with_capacity(iters);

    for i in 0..iters {
        // Clean slate for each iteration so we're timing the first
        // flush of a fresh file, not incremental updates.
        let _ = Hyper::fs_unlink(client, uri).await;

        let flags = FileFlags::rdwr();
        let mode = FileMode::default_file();
        let mut file =
            HyperFileTokio::open_or_create_with_config(client, file_config.clone(), flags, mode)
                .await?;
        file.write_all(&payload).await?;

        let start = Instant::now();
        file.flush().await?;
        let elapsed = start.elapsed();

        file.shutdown().await?;
        samples.push(elapsed);

        // Light progress indicator on stderr so long runs are visible.
        eprint!(
            "\r  {} iter {:>3}/{:<3} flushed in {}",
            fmt_bytes(payload_size),
            i + 1,
            iters,
            fmt_ms(elapsed),
        );
    }
    eprintln!();

    samples.sort();
    let total: Duration = samples.iter().sum();
    let avg = total / (iters as u32);
    let p50 = percentile(&samples, 0.50);
    let p95 = percentile(&samples, 0.95);

    println!(
        "{}  iter={:<3}  total={}  avg={}  p50={}  p95={}",
        fmt_bytes(payload_size),
        iters,
        fmt_ms(total),
        fmt_ms(avg),
        fmt_ms(p50),
        fmt_ms(p95),
    );

    // Final cleanup for this size.
    let _ = Hyper::fs_unlink(client, uri).await;
    Ok(())
}

#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
async fn main() -> Result<()> {
    env_logger::init();
    let uri = std::env::var("HYPERFILE_STAGING_ROOT_URI")
        .expect("HYPERFILE_STAGING_ROOT_URI must be set to an s3:// URI");

    let sizes = parse_sizes_env();
    let iters = parse_iters_env();

    let aws_config = aws_config::load_from_env().await;
    let client = Client::new(&aws_config);

    let meta_config = HyperFileMetaConfig::default();
    let runtime_config = HyperFileRuntimeConfig::default_large();

    println!("Hyperfile flush benchmark ({})", feature_label());
    println!("  uri:    {}", uri);
    println!("  iters:  {}", iters);
    println!(
        "  sizes:  {}",
        sizes
            .iter()
            .map(|s| fmt_bytes(*s).trim().to_string())
            .collect::<Vec<_>>()
            .join(", ")
    );
    println!();

    for size in sizes {
        bench_one_size(&client, &uri, &meta_config, &runtime_config, size, iters).await?;
    }

    Ok(())
}
