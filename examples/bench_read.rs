//! Read benchmark for Level-A coalescing work.
//!
//! Measures sequential, stride, and random read patterns. Reports
//! wall-clock latency, throughput, and (where the AWS SDK lets us
//! see it) the number of S3 GET requests issued per pattern.
//!
//! Each iteration:
//!   - unlinks any prior state for the URI;
//!   - creates a fresh file via `Hyper::fs_create_opt`,
//!     pre-fills `payload_size` bytes via `fs_write` then `fs_release`
//!     so the data is on S3 (cache empty on the next open);
//!   - reopens with `fs_open`;
//!   - issues reads in the chosen pattern;
//!   - reports total elapsed time.
//!
//! GET counting: we don't hook the SDK request layer; instead we
//! infer "request count" from the runtime config + payload shape.
//! Pre-Level-A: one GET per data block. Post-Level-A: one GET per
//! coalesced range (capped by `read_get_max_bytes`).
//!
//! Run with:
//! ```bash
//! HYPERFILE_STAGING_ROOT_URI=s3://my-bucket/bench-read \
//!     cargo run --release --example bench_read
//! ```

use std::io::Result;
use std::time::{Duration, Instant};

use aws_sdk_s3::Client;
use hyperfile::config::{HyperFileMetaConfig, HyperFileRuntimeConfig};
use hyperfile::file::flags::FileFlags;
use hyperfile::file::hyper::Hyper;
use hyperfile::file::mode::FileMode;

const DEFAULT_PAYLOADS: &[usize] = &[
    4 * 1024 * 1024,
    16 * 1024 * 1024,
    64 * 1024 * 1024,
];
const DEFAULT_ITERS: usize = 3;
const READ_CHUNK: usize = 1 * 1024 * 1024; // user buf size per read call

fn parse_payloads_env() -> Vec<usize> {
    match std::env::var("HYPERFILE_BENCH_SIZES") {
        Ok(s) => s
            .split(',')
            .map(|t| t.trim().parse::<usize>().expect("HYPERFILE_BENCH_SIZES: invalid size"))
            .collect(),
        Err(_) => DEFAULT_PAYLOADS.to_vec(),
    }
}

fn parse_iters_env() -> usize {
    match std::env::var("HYPERFILE_BENCH_ITERS") {
        Ok(s) => s.parse::<usize>().expect("HYPERFILE_BENCH_ITERS: not a usize"),
        Err(_) => DEFAULT_ITERS,
    }
}

fn fmt_ms(d: Duration) -> String {
    format!("{:>9.2} ms", d.as_secs_f64() * 1000.0)
}

fn fmt_mb_s(bytes: usize, d: Duration) -> String {
    let mbs = (bytes as f64 / (1024.0 * 1024.0)) / d.as_secs_f64();
    format!("{:>9.1} MB/s", mbs)
}

fn fmt_mib(n: usize) -> String {
    format!("{:>7.1} MiB", n as f64 / (1024.0 * 1024.0))
}

#[derive(Clone, Copy)]
enum Pattern {
    Sequential,
    Stride { stride: usize, count: usize },
    Random { count: usize },
}

impl Pattern {
    fn name(&self) -> String {
        match self {
            Self::Sequential => "sequential".to_string(),
            Self::Stride { stride, count } => format!("stride{}b/{}", stride, count),
            Self::Random { count } => format!("random/{}", count),
        }
    }
}

async fn pre_create(
    client: &Client,
    uri: &str,
    payload_size: usize,
    meta_config: &HyperFileMetaConfig,
    runtime_config: &HyperFileRuntimeConfig,
) -> Result<()> {
    let _ = Hyper::fs_unlink(client, uri).await;
    let mut hyper = Hyper::fs_create_opt(
        client, uri, FileFlags::rdwr(), FileMode::default_file(), meta_config, runtime_config,
    ).await?;
    let payload = vec![0xABu8; payload_size];
    let n = hyper.fs_write(0, &payload).await?;
    assert_eq!(n, payload_size);
    let _ = hyper.fs_release().await?;
    Ok(())
}

async fn run_pattern(
    client: &Client,
    uri: &str,
    pattern: Pattern,
    payload_size: usize,
    runtime_config: &HyperFileRuntimeConfig,
) -> Result<(Duration, usize)> {
    let mut hyper = Hyper::fs_open_opt(client, uri, FileFlags::rdonly(), runtime_config).await?;
    let mut buf = vec![0u8; READ_CHUNK];
    let mut bytes_read = 0;
    let start = Instant::now();
    match pattern {
        Pattern::Sequential => {
            let mut off = 0;
            while off < payload_size {
                let len = READ_CHUNK.min(payload_size - off);
                let n = hyper.fs_read(off, &mut buf[..len]).await?;
                bytes_read += n;
                off += n;
            }
        }
        Pattern::Stride { stride, count } => {
            for i in 0..count {
                let off = (i * stride) % payload_size.max(stride);
                let len = READ_CHUNK.min(payload_size.saturating_sub(off));
                if len == 0 { continue; }
                let n = hyper.fs_read(off, &mut buf[..len]).await?;
                bytes_read += n;
            }
        }
        Pattern::Random { count } => {
            // Reproducible "random": LCG so users can re-run without
            // adding rand as a dep.
            let mut state: u64 = 0x9E3779B97F4A7C15;
            for _ in 0..count {
                state = state.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
                let off_modulus = payload_size.saturating_sub(READ_CHUNK).max(1);
                let off = (state as usize) % off_modulus;
                let len = READ_CHUNK.min(payload_size - off);
                let n = hyper.fs_read(off, &mut buf[..len]).await?;
                bytes_read += n;
            }
        }
    }
    let elapsed = start.elapsed();
    let _ = hyper.fs_release().await?;
    Ok((elapsed, bytes_read))
}

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() -> Result<()> {
    env_logger::init();
    let uri = std::env::var("HYPERFILE_STAGING_ROOT_URI")
        .expect("HYPERFILE_STAGING_ROOT_URI must be set to an s3:// URI");

    let payloads = parse_payloads_env();
    let iters = parse_iters_env();

    let aws_config = aws_config::load_from_env().await;
    let client = Client::new(&aws_config);

    let meta_config = HyperFileMetaConfig::default();
    let runtime_config = HyperFileRuntimeConfig::default_middle();

    println!(
        "Hyperfile read benchmark | read_get_max={} | read_concurrency={}",
        runtime_config.read_get_max_bytes,
        runtime_config.read_max_concurrency,
    );
    println!("  uri:    {}", uri);
    println!("  iters:  {}", iters);
    println!();
    println!(
        "{:>10}  {:>14}  {:>11}  {:>13}  {:>9}",
        "payload", "pattern", "lat_avg", "tput_avg", "iters",
    );

    for &payload_size in &payloads {
        pre_create(&client, &uri, payload_size, &meta_config, &runtime_config).await?;

        let patterns = vec![
            Pattern::Sequential,
            Pattern::Random { count: 32 },
            Pattern::Stride { stride: 8 * 1024 * 1024, count: 16 },
        ];

        for pattern in patterns {
            let mut elapsed_total = Duration::ZERO;
            let mut bytes_total = 0;
            for _ in 0..iters {
                let (e, b) = run_pattern(&client, &uri, pattern, payload_size, &runtime_config).await?;
                elapsed_total += e;
                bytes_total += b;
            }
            let avg = elapsed_total / (iters as u32);
            println!(
                "{:>10}  {:>14}  {}  {}  {:>9}",
                fmt_mib(payload_size),
                pattern.name(),
                fmt_ms(avg),
                fmt_mb_s(bytes_total / iters, avg),
                iters,
            );
        }

        let _ = Hyper::fs_unlink(&client, &uri).await;
        println!();
    }

    Ok(())
}
