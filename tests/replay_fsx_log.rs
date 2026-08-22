//! Replay an `fsx` operation log against hyperfile, checking every read
//! against an in-memory model.
//!
//! Optional: it needs a log to replay, and skips with a message if there
//! is none. Point it at one with `HYPERFILE_FSX_LOG`, or drop the file at
//! the default path below. Produce one with:
//!
//! ```bash
//! fsx -N 1000 -S 1 -P /tmp -d <mountpoint>/fsx.1000 > ops.txt
//! ```
//!
//! ## Why replay rather than run fsx
//!
//! fsx runs through a filesystem, so the kernel page cache sits between it
//! and hyperfile. A read the kernel answers itself never arrives, which
//! means a read that returns stale bytes can go unnoticed until some later
//! read is served from the page cache — and by then the operation that
//! caused it is several steps back. That is exactly what happened with the
//! report this was written for: fsx flagged a read three operations after
//! the one that actually went wrong, and every test written against the
//! flagged read passed.
//!
//! Replaying the same operations directly has no page cache in the way, so
//! a bad read is caught at the read that is bad. It also makes the failure
//! deterministic and cheap to iterate on.
//!
//! ## What it checks
//!
//! A `Vec<u8>` model is kept alongside the file and every operation is
//! applied to both. Reads are compared byte for byte, and `i_size` is
//! compared after every operation. Reads are preceded by a read-ahead, the
//! way a filesystem layer doing its own read-ahead would, fired without
//! waiting — which is what leaves a fetch in flight while later operations
//! run, and is how the stale-install window in `spawn_read_ahead` was
//! found.
//!
//! fsx writes the operation number as its byte pattern, so the model can
//! be reconstructed from the log alone.

#![cfg(feature = "reactor")]
#[allow(dead_code)]
mod common;
#[allow(dead_code)]
mod common_reactor;
use common::*;
use common_reactor::*;
use hyperfile::file::fh::HyperFileHandler;
use hyperfile::file::flags::FileFlags;
use hyperfile::file::mode::FileMode;

/// Overridden by `HYPERFILE_FSX_LOG`.
const DEFAULT_LOG: &str = "/tmp/hyperfile-fsx-ops-N1000-S1.txt";
/// The window the consumer's read-ahead layer uses.
const RA_WINDOW: usize = 8 * 1024 * 1024;

#[derive(Debug, Clone)]
enum Op {
    Write { off: usize, len: usize, val: u8 },
    Read { off: usize, len: usize },
    Copy { src: usize, len: usize, dst: usize },
    /// Punch keeps the size; zero and falloc may extend it.
    Zero { off: usize, len: usize, extend: bool },
    Trunc { to: usize },
}

fn hex(s: &str) -> Option<usize> {
    usize::from_str_radix(s.trim().trim_start_matches("0x").trim_end_matches(','), 16).ok()
}

fn parse(line: &str) -> Option<(usize, Op)> {
    let t: Vec<&str> = line.split_whitespace().collect();
    if t.len() < 2 { return None; }
    let n: usize = t[0].parse().ok()?;
    let val = (n % 256) as u8;
    match t[1] {
        // N write 0xSTART thru 0xEND (0xLEN bytes) ...
        "write" | "mapwrite" => {
            let off = hex(t[2])?;
            let len = hex(t[5].trim_start_matches('('))?;
            Some((n, Op::Write { off, len, val }))
        },
        "read" | "mapread" => {
            let off = hex(t[2])?;
            let len = hex(t[5].trim_start_matches('('))?;
            Some((n, Op::Read { off, len }))
        },
        // N copy from 0xSRC to 0xSRCEND, (0xLEN bytes) at 0xDST
        "copy" => {
            let src = hex(t[3])?;
            let len = hex(t[6].trim_start_matches('('))?;
            let dst = hex(t.last()?)?;
            Some((n, Op::Copy { src, len, dst }))
        },
        // N punch|zero from 0xA to 0xB, (0xLEN bytes)
        "punch" | "zero" => {
            let off = hex(t[3])?;
            let len = hex(t[6].trim_start_matches('('))?;
            Some((n, Op::Zero { off, len, extend: t[1] == "zero" }))
        },
        // N falloc from 0xA to 0xB (0xLEN bytes)
        "falloc" => {
            let off = hex(t[3])?;
            let len = hex(t[6].trim_start_matches('('))?;
            Some((n, Op::Zero { off, len, extend: true }))
        },
        // N trunc from 0xOLD to 0xNEW
        "trunc" => Some((n, Op::Trunc { to: hex(t.last()?)? })),
        _ => None,
    }
}

/// The model: what the file should contain.
struct Model(Vec<u8>);

impl Model {
    fn grow_to(&mut self, n: usize) {
        if self.0.len() < n {
            self.0.resize(n, 0);
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn replay_fsx_log() {
    let _ = env_logger::try_init();

    let path = std::env::var("HYPERFILE_FSX_LOG").unwrap_or_else(|_| DEFAULT_LOG.to_string());
    let Ok(text) = std::fs::read_to_string(&path) else {
        eprintln!("no fsx log at {path}, skipping. Set HYPERFILE_FSX_LOG to replay one.");
        return;
    };
    let ops: Vec<(usize, Op)> = text.lines().filter_map(parse).collect();
    assert!(ops.len() > 10,
        "parsed only {} ops from {}, so the log format is not what this expects",
        ops.len(), path);
    eprintln!("replaying {} ops from {}", ops.len(), path);

    let client = make_client().await;
    let tf = TestFile::new(&client).await;
    let reactor = make_reactor();
    let mut fh = HyperFileHandler::fh_open_or_create_with_default_opt(
        &reactor, &client, tf.uri(), FileFlags::rdwr(), FileMode::default_file()).await.unwrap();

    let mut model = Model(Vec::new());
    let mut reads = 0usize;

    for (n, op) in ops {
        match op {
            Op::Write { off, len, val } => {
                model.grow_to(off + len);
                model.0[off..off + len].fill(val);
                fh.fh_write(off, &vec![val; len]).await.expect("write");
            },
            Op::Read { off, len } => {
                // What the consumer's layer does before a read: fire it and
                // do not wait — waiting would defeat the point of a
                // read-ahead, and it is the not-waiting that leaves the
                // fetch in flight while later operations run.
                {
                    let ra = fh.clone();
                    tokio::spawn(async move { let _ = ra.fh_read_ahead(off, RA_WINDOW).await; });
                }

                let want_len = len.min(model.0.len().saturating_sub(off));
                let got = fh.fh_read_owned(off, len).await.expect("read");
                assert_eq!(got.len(), want_len,
                    "op {}: read {:#x}+{:#x} returned {:#x} bytes, model says {:#x}",
                    n, off, len, got.len(), want_len);
                if let Some(i) = (0..want_len).find(|&i| got[i] != model.0[off + i]) {
                    panic!("op {}: READ BAD DATA at {:#x} (read {:#x}+{:#x}): \
                            got {:#x}, model says {:#x}",
                        n, off + i, off, len, got[i], model.0[off + i]);
                }
                reads += 1;
            },
            Op::Copy { src, len, dst } => {
                // What a FUSE copy_file_range does: read then write.
                model.grow_to(dst + len);
                let mut tmp = vec![0u8; len];
                let avail = len.min(model.0.len().saturating_sub(src));
                tmp[..avail].copy_from_slice(&model.0[src..src + avail]);
                model.0[dst..dst + len].copy_from_slice(&tmp);

                let read = fh.fh_read_owned(src, len).await.expect("copy read");
                let mut buf = vec![0u8; len];
                buf[..read.len()].copy_from_slice(&read);
                fh.fh_write(dst, &buf).await.expect("copy write");
            },
            Op::Zero { off, len, extend } => {
                if extend {
                    model.grow_to(off + len);
                } else if off >= model.0.len() {
                    continue;
                }
                let end = (off + len).min(model.0.len());
                if end > off {
                    model.0[off..end].fill(0);
                }
                if extend && off + len > 0 {
                    // A zero/falloc past EOF grows the file first.
                    let cur = fh.fh_getattr().await.expect("getattr").st_size as usize;
                    if off + len > cur {
                        fh.fh_truncate(off + len).await.expect("extend for zero");
                    }
                }
                if end > off {
                    let _ = fh.fh_write_zero(off, end - off).await.expect("write_zero");
                }
            },
            Op::Trunc { to } => {
                model.0.resize(to, 0);
                fh.fh_truncate(to).await.expect("truncate");
            },
        }

        // Size must track, every step.
        let st = fh.fh_getattr().await.expect("getattr");
        assert_eq!(st.st_size as usize, model.0.len(),
            "op {}: i_size {:#x} but model says {:#x}", n, st.st_size, model.0.len());
    }

    eprintln!("RESULT replay clean: {} reads verified", reads);
    let _ = fh.fh_release().await;
    tf.cleanup(&client).await;
}
