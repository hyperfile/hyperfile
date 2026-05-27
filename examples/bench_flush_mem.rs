//! Memory + latency benchmark for the flush path.
//!
//! Identical iteration shape as `bench_flush` (create file → write
//! payload → flush → release → unlink) but installs a process-wide
//! tracking allocator and reports per-iteration peak heap usage in
//! addition to wall-clock latency.
//!
//! The numbers it captures are the ones the zero-copy / scatter-
//! gather work cares about:
//!   - **peak heap during flush** — drops if the segment buffer
//!     stops being a 1×-segment-sized contiguous allocation;
//!   - **flush wall time** — should not regress.
//!
//! Because the peak allocator is global, every allocation in the
//! process counts (cache, AWS SDK, tokio, hyperfile internals,
//! everything). The "delta from baseline" pattern (peak after a
//! flush minus current bytes alive before a flush) isolates the
//! per-flush working set.
//!
//! Run with the same env vars as `bench_flush`:
//!
//! ```bash
//! HYPERFILE_STAGING_ROOT_URI=s3://my-bucket/bench-flush \
//!     cargo run --release --example bench_flush_mem
//! ```

use std::alloc::{GlobalAlloc, Layout, System};
use std::io::Result;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use aws_sdk_s3::Client;
use hyperfile::config::{HyperFileMetaConfig, HyperFileRuntimeConfig};
use hyperfile::file::flags::FileFlags;
use hyperfile::file::hyper::Hyper;
use hyperfile::file::mode::FileMode;

// ---------------------------------------------------------------
// Tracking allocator
// ---------------------------------------------------------------

struct TrackingAlloc {
    current: AtomicUsize,
    peak: AtomicUsize,
}

impl TrackingAlloc {
    const fn new() -> Self {
        Self {
            current: AtomicUsize::new(0),
            peak: AtomicUsize::new(0),
        }
    }

    fn current(&self) -> usize {
        self.current.load(Ordering::Relaxed)
    }
    fn peak(&self) -> usize {
        self.peak.load(Ordering::Relaxed)
    }
    fn reset_peak_to_current(&self) {
        // Snap the peak back down to whatever's currently alive so
        // a subsequent measurement is delta-from-here.
        let cur = self.current.load(Ordering::Relaxed);
        self.peak.store(cur, Ordering::Relaxed);
    }
}

unsafe impl GlobalAlloc for TrackingAlloc {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let ptr = unsafe { System.alloc(layout) };
        if !ptr.is_null() {
            let new_cur = self.current.fetch_add(layout.size(), Ordering::Relaxed)
                + layout.size();
            self.peak.fetch_max(new_cur, Ordering::Relaxed);
        }
        ptr
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) };
        self.current.fetch_sub(layout.size(), Ordering::Relaxed);
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        let ptr = unsafe { System.alloc_zeroed(layout) };
        if !ptr.is_null() {
            let new_cur = self.current.fetch_add(layout.size(), Ordering::Relaxed)
                + layout.size();
            self.peak.fetch_max(new_cur, Ordering::Relaxed);
        }
        ptr
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        let p = unsafe { System.realloc(ptr, layout, new_size) };
        if !p.is_null() {
            if new_size > layout.size() {
                let inc = new_size - layout.size();
                let new_cur = self.current.fetch_add(inc, Ordering::Relaxed) + inc;
                self.peak.fetch_max(new_cur, Ordering::Relaxed);
            } else if new_size < layout.size() {
                self.current
                    .fetch_sub(layout.size() - new_size, Ordering::Relaxed);
            }
        }
        p
    }
}

#[global_allocator]
static A: TrackingAlloc = TrackingAlloc::new();

// ---------------------------------------------------------------
// Bench config (mirrors bench_flush)
// ---------------------------------------------------------------

const DEFAULT_SIZES: &[usize] = &[
    1 * 1024 * 1024,
    8 * 1024 * 1024,
    32 * 1024 * 1024,
];
const DEFAULT_ITERS: usize = 5;

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

fn fmt_ms(d: Duration) -> String {
    format!("{:>8.2} ms", d.as_secs_f64() * 1000.0)
}

fn fmt_mib(n: usize) -> String {
    format!("{:>8.2} MiB", n as f64 / (1024.0 * 1024.0))
}

fn percentile(sorted: &[Duration], p: f64) -> Duration {
    let rank = ((p * sorted.len() as f64).ceil() as usize).saturating_sub(1);
    sorted[rank.min(sorted.len() - 1)]
}

#[derive(Default, Clone, Copy)]
struct PerIter {
    elapsed: Duration,
    flush_peak_delta: usize,
}

async fn bench_one_size(
    client: &Client,
    uri: &str,
    meta_config: &HyperFileMetaConfig,
    runtime_config: &HyperFileRuntimeConfig,
    payload_size: usize,
    iters: usize,
) -> Result<Vec<PerIter>> {
    let payload = vec![0xABu8; payload_size];
    let mut samples: Vec<PerIter> = Vec::with_capacity(iters);

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

        // Reset peak just before the timed flush so the recorded
        // peak excludes any allocations from create / write.
        A.reset_peak_to_current();
        let baseline = A.current();
        let start = Instant::now();
        let _cno = hyper.fs_flush().await?;
        let elapsed = start.elapsed();
        let peak = A.peak();
        let peak_delta = peak.saturating_sub(baseline);

        let _ = hyper.fs_release().await;
        samples.push(PerIter {
            elapsed,
            flush_peak_delta: peak_delta,
        });

        eprint!(
            "\r  payload={} iter={:>3}/{:<3} elapsed={} peak_delta={}",
            fmt_mib(payload_size),
            i + 1,
            iters,
            fmt_ms(elapsed),
            fmt_mib(peak_delta),
        );
    }
    eprintln!();
    let _ = Hyper::fs_unlink(client, uri).await;
    Ok(samples)
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
    let runtime_config = HyperFileRuntimeConfig::default_middle();

    let feat = if cfg!(feature = "concurrent-segment-build") {
        "concurrent-segment-build=ON"
    } else {
        "concurrent-segment-build=OFF"
    };

    println!("Hyperfile flush memory + latency benchmark ({})", feat);
    println!("  uri:      {}", uri);
    println!("  iters:    {}", iters);
    println!();
    println!(
        "{:>10}  {:>10}  {:>11}  {:>11}  {:>11}  {:>11}  {:>11}",
        "size", "iters", "lat_avg", "lat_p50", "lat_p95", "peak_avg", "peak_max",
    );

    for size in &sizes {
        let samples = bench_one_size(
            &client, &uri, &meta_config, &runtime_config, *size, iters,
        )
        .await?;

        let mut elapsed_sorted: Vec<Duration> =
            samples.iter().map(|s| s.elapsed).collect();
        elapsed_sorted.sort();
        let total: Duration = elapsed_sorted.iter().sum();
        let avg = total / (iters as u32);
        let p50 = percentile(&elapsed_sorted, 0.50);
        let p95 = percentile(&elapsed_sorted, 0.95);

        let peak_avg: usize =
            samples.iter().map(|s| s.flush_peak_delta).sum::<usize>() / iters;
        let peak_max: usize = samples
            .iter()
            .map(|s| s.flush_peak_delta)
            .max()
            .unwrap_or(0);

        println!(
            "{:>10}  {:>10}  {}  {}  {}  {}  {}",
            fmt_mib(*size),
            iters,
            fmt_ms(avg),
            fmt_ms(p50),
            fmt_ms(p95),
            fmt_mib(peak_avg),
            fmt_mib(peak_max),
        );
    }

    Ok(())
}
