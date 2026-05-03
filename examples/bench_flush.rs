//! Flush benchmark: sequential write + flush latency.
//!
//! Runs N iterations per payload size. Each iteration:
//!   - unlinks any prior state for the URI;
//!   - creates a fresh file via direct API (Hyper::fs_create_opt);
//!   - writes the payload to offset 0;
//!   - resets the per-phase flush timing counters;
//!   - flushes, timing both the wall-clock round trip and the
//!     per-phase ns counters;
//!   - releases.
//!
//! Two output modes:
//!   - default: one row per payload size with
//!     `{total, avg, p50, p95}` wall-clock flush latency;
//!   - verbose (HYPERFILE_BENCH_VERBOSE=1): also prints a
//!     per-phase breakdown table showing how much of each flush is
//!     spent in pre_build / build_segment / segment_done /
//!     flush_inode / cleanup.
//!
//! Run with:
//!
//! ```bash
//! HYPERFILE_STAGING_ROOT_URI=s3://my-bucket/bench-flush \
//!     cargo run --release --example bench_flush
//!
//! # verbose with per-phase breakdown:
//! HYPERFILE_BENCH_VERBOSE=1 \
//! HYPERFILE_STAGING_ROOT_URI=s3://my-bucket/bench-flush \
//!     cargo run --release --example bench_flush
//!
//! # with concurrent-segment-build on:
//! HYPERFILE_STAGING_ROOT_URI=s3://my-bucket/bench-flush \
//!     cargo run --release --features concurrent-segment-build \
//!     --example bench_flush
//! ```

use std::io::Result;
use std::time::{Duration, Instant};

use aws_sdk_s3::Client;
use hyperfile::config::{HyperFileMetaConfig, HyperFileRuntimeConfig};
use hyperfile::file::flags::FileFlags;
use hyperfile::file::hyper::Hyper;
use hyperfile::file::mode::FileMode;

const DEFAULT_SIZES: &[usize] = &[
    1 * 1024 * 1024,
    8 * 1024 * 1024,
    32 * 1024 * 1024,
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

fn verbose() -> bool {
    std::env::var("HYPERFILE_BENCH_VERBOSE")
        .map(|v| v != "0" && v != "")
        .unwrap_or(false)
}

fn percentile(sorted: &[Duration], p: f64) -> Duration {
    assert!(!sorted.is_empty());
    let rank = ((p * sorted.len() as f64).ceil() as usize).saturating_sub(1);
    sorted[rank.min(sorted.len() - 1)]
}

fn fmt_ms(d: Duration) -> String {
    format!("{:>8.2} ms", d.as_secs_f64() * 1000.0)
}

fn fmt_ns_ms(ns: u64) -> String {
    format!("{:>8.2} ms", ns as f64 / 1_000_000.0)
}

fn fmt_bytes(n: usize) -> String {
    let mib = n as f64 / (1024.0 * 1024.0);
    format!("{:>6.1} MiB", mib)
}

/// Per-phase accumulators across one size's iterations.
#[derive(Default)]
struct PhaseSums {
    pre_build_ns:     u64,
    build_segment_ns: u64,
    segment_done_ns:  u64,
    flush_inode_ns:   u64,
    cleanup_ns:       u64,
}

async fn bench_one_size(
    client: &Client,
    uri: &str,
    meta_config: &HyperFileMetaConfig,
    runtime_config: &HyperFileRuntimeConfig,
    payload_size: usize,
    iters: usize,
) -> Result<(Vec<Duration>, PhaseSums)> {
    let payload = vec![0xABu8; payload_size];
    let mut samples: Vec<Duration> = Vec::with_capacity(iters);
    let mut phases = PhaseSums::default();

    for i in 0..iters {
        let _ = Hyper::fs_unlink(client, uri).await;

        let flags = FileFlags::rdwr();
        let mode = FileMode::default_file();
        let mut hyper = Hyper::fs_create_opt(
            client, uri, flags, mode, meta_config, runtime_config,
        )
        .await?;
        let n = hyper.fs_write(0, &payload).await?;
        assert_eq!(n, payload.len());

        // Reset phase counters right before the timed flush so the
        // snapshot we take after only reflects this one flush.
        hyper.flush_timing_reset();

        let start = Instant::now();
        let _cno = hyper.fs_flush().await?;
        let elapsed = start.elapsed();

        let s = hyper.flush_timing().snapshot();
        phases.pre_build_ns     += s.pre_build_ns;
        phases.build_segment_ns += s.build_segment_ns;
        phases.segment_done_ns  += s.segment_done_ns;
        phases.flush_inode_ns   += s.flush_inode_ns;
        phases.cleanup_ns       += s.cleanup_ns;

        let _ = hyper.fs_release().await;
        samples.push(elapsed);

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
    let _ = Hyper::fs_unlink(client, uri).await;
    Ok((samples, phases))
}

#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
async fn main() -> Result<()> {
    env_logger::init();
    let uri = std::env::var("HYPERFILE_STAGING_ROOT_URI")
        .expect("HYPERFILE_STAGING_ROOT_URI must be set to an s3:// URI");

    let sizes = parse_sizes_env();
    let iters = parse_iters_env();
    let verbose_mode = verbose();

    let aws_config = aws_config::load_from_env().await;
    let client = Client::new(&aws_config);

    let meta_config = HyperFileMetaConfig::default();
    let runtime_config = HyperFileRuntimeConfig::default_large();

    println!("Hyperfile flush benchmark ({})", feature_label());
    println!("  uri:      {}", uri);
    println!("  iters:    {}", iters);
    println!(
        "  sizes:    {}",
        sizes
            .iter()
            .map(|s| fmt_bytes(*s).trim().to_string())
            .collect::<Vec<_>>()
            .join(", "),
    );
    println!("  verbose:  {}", verbose_mode);
    println!();

    let mut results: Vec<(usize, Vec<Duration>, PhaseSums)> = Vec::new();
    for size in &sizes {
        let (samples, phases) = bench_one_size(
            &client, &uri, &meta_config, &runtime_config, *size, iters,
        )
        .await?;

        let total: Duration = samples.iter().sum();
        let avg = total / (iters as u32);
        let p50 = percentile(&samples, 0.50);
        let p95 = percentile(&samples, 0.95);

        println!(
            "{}  iter={:<3}  total={}  avg={}  p50={}  p95={}",
            fmt_bytes(*size),
            iters,
            fmt_ms(total),
            fmt_ms(avg),
            fmt_ms(p50),
            fmt_ms(p95),
        );

        results.push((*size, samples, phases));
    }

    if verbose_mode {
        println!();
        println!("Per-phase flush breakdown (average per flush):");
        println!(
            "{:>10}  {:>11}  {:>11}  {:>11}  {:>11}  {:>11}",
            "size", "pre_build", "build_seg", "seg_done", "flush_inode", "cleanup",
        );
        for (size, _samples, phases) in &results {
            let n = iters as u64;
            println!(
                "{:>10}  {}  {}  {}  {}  {}",
                fmt_bytes(*size),
                fmt_ns_ms(phases.pre_build_ns     / n),
                fmt_ns_ms(phases.build_segment_ns / n),
                fmt_ns_ms(phases.segment_done_ns  / n),
                fmt_ns_ms(phases.flush_inode_ns   / n),
                fmt_ns_ms(phases.cleanup_ns       / n),
            );
        }
    }

    Ok(())
}
