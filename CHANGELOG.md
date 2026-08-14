# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

> Pre-1.0 caveat: while we are still on `0.x`, every minor version (`0.x.0`)
> may contain breaking API or on-disk changes. Read the **Breaking changes**
> section before upgrading.

## [0.4.4] - 2026-08-14

### Added

- **POSIX access mode enforcement.** An operation the handle's access
  mode does not permit now fails with `EBADF`:

  | Handle | Rejected |
  | --- | --- |
  | `O_RDONLY` | `write`, `write_zero`, `write_batch`, `write_aligned_batch`, `truncate` |
  | `O_WRONLY` | `read` |

  Previously a write on a read-only handle landed in the data cache
  and a subsequent flush could persist it, and a read on a
  write-only handle succeeded. POSIX lists `[EBADF] ... not a valid
  file descriptor open for writing` (`write()`) and `... open for
  reading` (`read()`) as mandatory errors. `ftruncate()` permits
  `[EBADF]` or `[EINVAL]`; `EBADF` is used so every access-mode
  violation reports one errno. Enforced on the direct `fs_*` API, the
  reactor `fh_*` API, and the tokio wrapper.

  `lseek` is not gated — POSIX requires no particular access mode for
  it, and neither do the `SEEK_DATA` / `SEEK_HOLE` extensions behind
  `fs_seek_data` / `fs_seek_hole`.

  Two categories of internal I/O deliberately bypass the checks and
  keep working: write-side operations that read (a sub-block write's
  read-modify-write, and a shrink to a non-block-aligned size reading
  the tail block), and WAL crash recovery, which replays previously
  acknowledged writes during `open` — including a read-only open, so
  a file that crashed mid-flush still presents correct contents.

- `HyperFileFlags::is_readable()` and `is_writable()`, keyed off the
  access mode alone. `O_APPEND` does not grant write access on its
  own, matching Linux, where `open(O_RDONLY | O_APPEND)` followed by
  a `write` fails with `EBADF`.

### Changed

- **Compatibility note.** Callers that relied on the previous
  permissive behaviour — writing through an `O_RDONLY` handle, or
  reading through an `O_WRONLY` handle — now receive an error where
  the call used to succeed. Open with `O_RDWR` if a handle needs both
  directions.
- Because `std::io::ErrorKind` has no `EBADF` variant, the error is
  built with `std::io::Error::from_raw_os_error`. Test it with
  `err.raw_os_error() == Some(libc::EBADF)`; `err.kind()` is the
  unmatchable `Uncategorized`. See `docs/posix.md` for why
  `PermissionDenied` (EACCES) and `InvalidInput` (EINVAL) were
  rejected as inaccurate.

### Documentation

- `docs/posix.md`: the open-mode table previously claimed `fs_write` /
  `fs_truncate` "return `EBADF`-shaped errors from the underlying
  staging layer", which was never true — S3 has no notion of the
  handle's access mode. Replaced with an "Access mode enforcement"
  section carrying a rejected/allowed matrix and the errno rationale.

## [0.4.3] - 2026-08-14

### Fixed

- **Data integrity: a shrinking truncate left stale clean-cached
  blocks above the new EOF.** On an open handle, `write` at offset
  X → `flush` → truncate below X → truncate back above X → read at
  X returned the pre-truncate bytes instead of zeros. POSIX requires
  the grown-back region to read as a hole.

  The truncate path discarded only *dirty* cache entries above the
  boundary. The `flush` is what makes a block clean, and a clean
  block lives solely in the clean cache tier, which was never swept
  — while the read path consults that tier first. Only the handle
  that performed the truncate was affected; the persisted state was
  always correct, so a close/reopen hid the problem.

  Both cache tiers are now swept. The local-disk cache additionally
  punches the backing hole for each dropped block, because its
  blocks are unzeroed mmap views and a later partial write to the
  same index could otherwise resurface pre-truncate bytes from the
  cache file.

  Found by `fsx` (xfstests) against a FUSE filesystem layered on
  hyperfile. `write_zero` and the (absent) hole-punch paths were
  audited and have no equivalent gap.

### Changed

- The internal `Cache` trait method `truncate_dirty_blocks_above` is
  renamed `truncate_blocks_above`, reflecting that it now drops clean
  entries too. The trait is `pub(crate)`, so this is not a public API
  change. Its return value still counts dirty removals only, for
  `i_blocks` accounting.

## [0.4.2] - 2026-06-28

### Added

- **Keep-open inode flag** (foundation for opt-in cross-mount open-but-unlinked
  in a filesystem layer):
  - `InodeRaw::FLAG_KEEP_OPEN` (`i_flags` bit) + `is_keep_open()` /
    `set_keep_open(bool)`.
  - `Inode::is_keep_open()` / `set_keep_open(bool)` (marks the inode attr-dirty).
  - `HyperFile::set_keep_open(bool)` / `is_keep_open()` and
    `Hyper::fs_set_keep_open(bool)` / `fs_is_keep_open()` — set (and persist on
    flush) or read the flag on an open file.

### Compatibility

- Additive and on-disk compatible with `0.4.0`/`0.4.1`: no layout change
  (`size_of::<InodeRaw>()` is still 160) and the flag is a previously-unused
  `i_flags` bit.

## [0.4.1] - 2026-06-28

### Added

- **Device-node `rdev` accessors on `InodeRaw`** (so a filesystem can persist
  character/block device nodes):
  - `InodeRaw::set_rdev(rdev)` / `InodeRaw::rdev()` — store/read a device node's
    `rdev`. A device node has no data segments, so the value is kept in the
    otherwise-unused `i_last_cno` slot; call `set_rdev` after `set_inline(&[])`
    (which zeroes the tail).
  - `Inode::to_stat` now reports `st_rdev` from the inode for char/block
    (`S_IFCHR`/`S_IFBLK`) modes, ignoring the passed `rdev`; non-device inodes
    are unchanged (they use the passed value).

### Compatibility

- Additive and on-disk compatible with `0.4.0`: no layout change
  (`size_of::<InodeRaw>()` is still 160) and the new behavior only affects
  device-mode inodes, which `0.4.0` never produced.

## [0.4.0] - 2026-06-27

### Breaking changes

- **`InodeRaw` on-disk layout reordered.** The tail fields are now laid out
  contiguously as `i_size, i_blocks, i_last_seq, i_last_cno, i_bmap` (previously
  `i_blocks`/`i_size` sat near the top). `size_of::<InodeRaw>()` is unchanged
  (160 bytes), but the **serialized byte layout differs**, so inodes written by
  `0.3.x` are not readable by `0.4.0` and vice versa. There is no in-place
  migration; this is intended for fresh trees. Consumers that embed `InodeRaw`
  (e.g. `hyperdir` dirents/scatter) must be rebuilt against the same version so
  both sides agree on the layout.

### Added

- **Inline-small-file format primitives on `InodeRaw`** (foundation for storing
  a tiny file entirely within its inode, with no separate `FILE/<uuid>` object):
  - `InodeRaw::FLAG_INLINE` — `i_flags` bit marking a fully-inlined file.
  - `InodeRaw::inline_offset()` / `inline_cap()` — the inline payload region (the
    contiguous `i_blocks..i_bmap` tail), `80` bytes on the current layout,
    computed via `offset_of!` so they track the layout automatically.
  - `is_inline()`, `inline_data()`, `set_inline(&[u8])`, `clear_inline()` — read
    and populate the inline bytes, with `i_size` as the length.
  - No behavior change on its own: this commit ships the format + accessors only;
    the create/read/write/spill paths that use it land in a later change.

## [0.3.3] - 2026-06-01

### Fixed

- **Precise error kinds for S3 failures**: S3 operations previously
  collapsed most failures into `ErrorKind::Other`. They now map the
  SDK error to the semantically closest `std::io::ErrorKind` (404 →
  `NotFound`, 403 → `PermissionDenied`, 409/412 → `AlreadyExists`,
  429/5xx → `ResourceBusy`, 400-class → `InvalidInput`, timeouts →
  `TimedOut` / `ConnectionReset`), so consumers (e.g. a FUSE layer)
  can react precisely instead of treating every failure as EIO.
  Call sites with bespoke handling (OCC conflict detection on the
  conditional inode write/delete, 404 on inode read) keep their
  explicit checks, so that behavior is unchanged.

### Changed

- During flush, a `429`/`5xx` from the inode write now maps to
  `ResourceBusy` and is retried by the flush retry loop with
  backoff (bounded by the existing max-retries cap) instead of
  failing immediately.

## [0.3.2] - 2026-05-31

### Fixed

- **Create-time timestamps**: a freshly created, never-written file
  or directory left `atime` and `mtime` at epoch 0; only `ctime`
  was set. POSIX requires a newly created object to report `atime`,
  `mtime` and `ctime` all at creation time. `Inode::default_dir` /
  `default_file` now stamp all three (from a single clock reading,
  so they are equal) instead of only `ctime`. No change to
  write / chmod / chown / utimens timestamp semantics.

## [0.3.1] - 2026-05-30

### Added

- **SEEK_DATA / SEEK_HOLE**: `Hyper::fs_seek_data(off)` and
  `Hyper::fs_seek_hole(off)` for sparse-file extent discovery.
  `fs_seek_data` returns the smallest offset `>= off` holding data
  (or `None`, which the caller maps to `ENXIO`, if only holes remain
  to EOF); `fs_seek_hole` returns the smallest offset `>= off` in a
  hole, treating EOF as an implicit hole.
  - A block counts as data if it is an unflushed write in the dirty
    cache or a non-zero block in the bmap; a hole is an absent or
    zero block that is also not in the dirty cache. The seek path is
    therefore consistent with `fs_read`, including for
    written-but-not-yet-flushed blocks.
  - The bmap scan uses `BMap::seek_key` to skip runs of absent keys
    in O(log n), so SEEK_DATA over a large fully-sparse file is
    cheap. SEEK_HOLE over a large fully-dense region remains O(n)
    because zero blocks are present bmap keys indistinguishable from
    data without a per-key lookup.
  - `fs_seek_hole(off == size)` returns `size`; `off > size` returns
    `None`.

## [0.3.0] - 2026-05-27

### Highlights

- **Read coalescing**: contiguous in-segment block reads are merged into a
  single ranged S3 GET, giving **100×+ speedup** on sequential reads
  (4 MiB sequential read: 4290 ms → 41 ms; 0.9 MB/s → ~100 MB/s) and
  ~1000× fewer S3 GET requests on the same workload.
- **Zero-copy flush**: peak transient memory during flush dropped from
  257 MiB to 0.16 MiB on a 1 MiB write (Phase 1 + Phase 2 combined),
  and from 321 MiB to 7.67 MiB on a 64 MiB write.
- **POSIX coverage**: `O_TRUNC`, `O_APPEND`, `O_EXCL`, `O_NOATIME`
  honored at the open / write paths, and `fdatasync` is now a separate
  API from `fsync`. `ctime` semantics fixed to match Linux.
- **WAL hardening**: WAL objects are deleted after successful flush,
  crash-recovery is covered by integration tests, and S3Wal pure-logic
  paths have unit tests.
- **Reactor + concurrency hardening**: handler death surfaces as
  `BrokenPipe` instead of hanging, flush conflicts are now resolved by
  a configurable policy, and `concurrent-segment-build` no longer
  loops forever on small flushes.
- **Test growth**: ~13k lines of new tests across 9 integration
  binaries, 217 unit tests (was ~10).

### Performance

- Coalesce contiguous in-segment block reads into one ranged GET on
  both the direct (`HyperFile::read`) and reactor (`spawn_read`)
  paths. Two new safety caps on `HyperFileRuntimeConfig`:
  `read_get_max_bytes` (default 16 MiB) and `read_max_concurrency`
  (default 10).
- Zero-copy flush body via scatter-gather: the segment is built as a
  list of `Bytes` slices and uploaded directly without copying through
  an intermediate buffer.
- Zero-copy cache → flush handoff via `Bytes::from_owner`, so dirty
  data block buffers move ownership into the flush path instead of
  being copied.

### Added

- **POSIX**:
  - `fs_fdatasync` / `fh_fdatasync` on `Hyper` / `HyperFileHandler`,
    distinct from `fsync` (skips inode metadata flush when only data
    blocks are dirty).
  - `O_EXCL` honored on `do_open_or_create`: returns `AlreadyExists`
    when the file already exists.
  - `O_APPEND` honored on `fs_write` / `fs_write_zero`: writes are
    redirected to the current end-of-file regardless of the requested
    offset.
  - `O_TRUNC` honored on open: file size is set to 0 after a
    successful open if requested.
  - `O_NOATIME` honored on open: `atime` is not bumped on read when
    set.
  - `fs_rename` / `fh_rename` reserved as APIs returning `Unsupported`
    (not yet implemented; reserved so users can plan around it).
- **API**:
  - `HyperFileRuntimeConfig::flush_conflict_policy` (`FailFast` |
    `RetryWithBackoff`) controls how the flush retry loop reacts to
    a `PreconditionFailed` from the conditional inode write.
  - `HyperFileHandler::fh_from_hyper` to construct a handler from an
    existing `Hyper`.
  - `Staging::load_range` trait method (parallel to
    `load_data_block`), implemented on `S3Staging` as a single ranged
    `GetObject`.
- **Examples / benches**:
  - `examples/bench_flush.rs`: flush latency baseline harness with
    per-phase breakdown.
  - `examples/bench_flush_mem.rs`: peak-memory measurement during
    flush, used to validate the zero-copy work.
  - `examples/bench_read.rs`: throughput / latency for sequential,
    stride, and random read patterns.
- **Tests**:
  - 9 focused integration binaries (`integration_s3_smoke`,
    `_rollback`, `_contract`, `_concurrent`,
    `integration_reactor_s3_smoke`, `_failure`, `_range_lock`,
    `_wal`, `_all_features`).
  - 217 unit tests covering handler, RangeLock, S3Wal pure-logic
    paths, segment byte-layout, and core modules.
- **Docs**:
  - `docs/posix.md`: POSIX coverage, permissions, rename, fsync /
    fdatasync, timestamps, supported / unsupported open flags.
  - `docs/wal.md`: WAL design and operational guidance.
  - `docs/concurrency.md`: direct vs reactor access modes, locking,
    range-lock semantics, reactor failure modes.
  - `docs/tests.md`: testing guide and feature matrix.
  - Cargo features documented; reactor/blocking exclusivity called
    out.

### Fixed

- **Truncate**:
  - Shrinking to a block boundary preserves the last block (was
    incorrectly dropping it).
  - Dirty cache entries above the new size are evicted (was leaking
    stale dirty data into the next flush).
- **Stat**: `st_blocks` reflects sparse holes (was always reporting
  size / 512).
- **ctime**: bumped on every metadata-affecting operation
  (`set_size`, `set_mode`, `set_uid_gid`, `update_stat`); previously
  `update_mtime` and `update_stat` skipped `ctime` and Linux clients
  saw stale values.
- **Reactor**:
  - Handler-task death surfaces as `BrokenPipe` to the user instead
    of hanging on a closed mpsc channel.
  - `spawn_write` / `spawn_write_zero` use a non-blocking permit
    acquire so a busy reactor returns `ResourceBusy` instead of
    parking the dispatch task.
  - `concurrent-segment-build` no longer loops forever on flushes
    smaller than the segment buffer.
- **Flush correctness**:
  - Transient flush failure (e.g. PreconditionFailed) is rolled back
    cleanly instead of silently committing a partial state.
  - `concurrent-segment-build` cfg gating no longer leaves dead
    `file_off` assignments in segment build.
- **Memory safety**:
  - `DataBlock` flags use `AtomicU64` instead of `write_volatile`
    (latter was UB under the C++20 memory model the compiler now
    targets).
  - `SegmentSum` serialization no longer uses `unsafe transmute`;
    encoded form is now a regular `repr(C)` struct with pinned-byte
    tests.

### Changed

- **Breaking** — see also the dedicated section below.
- `Hyper::last_cno` returns `Result<u64>` (was `u64`) so the reactor
  can surface handler-death symmetrically with read / write APIs.
- `FileResp` is now an `enum` (was a union with manual `Drop`); the
  enum is `Send + Sync`, drops cleanly, and removes one source of
  unsafe code.
- WAL objects are deleted after a successful flush (both reactor and
  direct API paths). Previously they accumulated until manual prune.
- Conditional inode write failure now surfaces as
  `ErrorKind::AlreadyExists` (was a generic `Other`), which the
  retry loop now uses to drive `FlushConflictPolicy`.
- Default integration test layout: one large `integration_s3.rs` was
  split into four focused binaries; reactor counterpart added.

### Breaking changes (from 0.2.0)

If you depend on `hyperfile = "0.2"`:

- `Hyper::last_cno() -> u64` is now `-> Result<u64>`. Callers must
  handle `Result`.
- `FileResp` is now an enum. If you were constructing or matching it
  manually (you shouldn't have been; it's `pub` only by virtue of
  being part of the reactor channel ABI), update accordingly.
- `HyperFileRuntimeConfig` gained three new fields:
  `flush_conflict_policy`, `read_get_max_bytes`,
  `read_max_concurrency`. All have `#[serde(default = ...)]` so
  existing serialized configs continue to deserialize cleanly. Code
  that builds `HyperFileRuntimeConfig` field-by-field via the public
  struct literal needs to add the new fields.
- `Staging` trait gained a required `load_range` method. Custom
  staging backends must implement it (one ranged-read primitive).
- POSIX flag handling is now active. Code that opened with `O_TRUNC`
  or `O_APPEND` and relied on the flag being silently ignored will
  see different behavior. This is intentional and matches Linux.
- `ctime` bumps on more operations than before. If your tests
  hard-code `ctime` values, they may need updating.

### Dependencies

- `btree-ondisk` `0.17` → `0.18` (transitively `0.16`-compatible
  resolution still works for downstream consumers).
- `hyperfile-reactor` `0.2` → `0.3.2`.
- Other AWS SDK / tokio bumps tracked in `Cargo.lock`.

### Removed

- Stale doc note about a `multi-handle-hang` limitation under WAL
  (the underlying race was fixed; the test that exercised it is
  back).
- Stale doc note about a `concurrent-segment-build` known gap (gap
  closed; suite now in `integration_reactor_s3_all_features`).

### Migration notes

For `0.2.0` consumers:

```toml
# Cargo.toml
[dependencies]
- hyperfile = "0.2"
+ hyperfile = "0.3"
```

If you maintain a custom `Staging` implementation, add:

```rust
async fn load_range(&self, segid: SegmentId, s3_off: usize,
                    buf: &mut [u8]) -> Result<()> {
    // single ranged GET into buf
}
```

If you build `HyperFileRuntimeConfig` literally:

```rust
HyperFileRuntimeConfig {
    // ... existing fields ...
+   flush_conflict_policy: FlushConflictPolicy::default(),
+   read_get_max_bytes:    16 * 1024 * 1024,
+   read_max_concurrency:  10,
}
```

Or use one of the constructor methods (`default()`,
`default_middle()`, `default_large()`) which set the new fields.

If you call `Hyper::last_cno`:

```rust
- let cno = hyper.last_cno();
+ let cno = hyper.last_cno()?;
```

---

## [0.2.0] - 2025-09-21

The 0.2 series is where Hyperfile became a usable random-read-write
file abstraction over S3, with WAL durability, optional caches, and
a reactor concurrency model. ~180 commits since 0.1.0.

### Highlights

- **WAL**: optional Write-Ahead Log support to preserve durability of
  unflushed data, including detection of unclean WAL on open and a
  full WAL flush-recovery path on reopen.
- **Reactor**: a dedicated tokio-runtime-based handler model
  (`HyperFileHandler`) that decouples user calls from the underlying
  flush worker via mpsc channels and a high-priority queue.
- **Range lock**: optional `range-lock` feature for fine-grained
  concurrency on disjoint write ranges.
- **Concurrent segment build**: optional `concurrent-segment-build`
  feature that appends dirty data blocks to the segment buffer in
  parallel during flush.
- **Local-disk caches**: optional `local-disk-cache` feature for both
  the data block cache and the bmap node cache, sized via runtime
  config.
- **POSIX surface**: file mode, `fs_chmod` / `fs_chown`,
  `fs_getattr` / `fs_getattr_fast`, default modes for files and
  directories, `fs_unlink_with_interceptor` for testing.

### Added

- **WAL** (`feature = "wal"`):
  - `WalReadWrite` trait with `S3Wal` implementation.
  - `HyperFileWalConfig` plumbed through `HyperFileConfigBuilder`.
  - `wal_flush_recovery` on open; replays WAL chunks if previous
    process crashed mid-flush.
  - WAL chunks listed via `list_segments` / `list_chunks` for
    introspection.
  - Read-through in-memory segments while a flush is in flight, so
    readers can see in-flight writes without waiting for S3.
- **Reactor** (default; switched off by `--no-default-features
  --features blocking`):
  - `HyperFileHandler` constructed from `Hyper`; clonable and
    shareable across tasks.
  - Dedicated tokio multi-thread runtime for spawned read / write /
    flush worker tasks.
  - High-priority queue for absorb requests; permit-controlled
    concurrent open in read-only mode.
  - Spawn-style read / write / write_zero paths with handles
    collected into an `ImmOrJoinSize` aggregate.
- **Range lock** (`feature = "range-lock"`):
  - Per-file rangemap of in-flight write ranges, aligned to data
    block size.
  - `try_lock` / `unlock` for non-blocking acquire on disjoint
    ranges.
- **Concurrent segment build** (`feature =
  "concurrent-segment-build"`):
  - `spawn_append` path for appending dirty data blocks to the
    segment buffer concurrently with flush prep.
- **Local disk cache** (`feature = "local-disk-cache"`):
  - `Cache` trait with `MemCache` (in-memory LRU, default) and
    `LocalDiskCache` (mmap-backed, optional) implementations.
  - `NodeCache` generic on `HyperFile` for the bmap; pluggable.
  - mlock for hot blocks; graceful shutdown to flush pending writes.
- **POSIX**:
  - `FileMode` with default file/dir modes; `fs_chmod`, `fs_chown`,
    `update_stat` APIs.
  - `fs_getattr_fast`: stat without opening the full file.
  - File type bits preserved across chmod (input mode merged
    instead of overwriting).
  - `fs_unlink_with_interceptor` for testing failure paths.
- **Batch I/O**:
  - `write_aligned_batch` / `write_batch` for bulk writes.
  - Sorted-and-deduplicated input blocks.
  - Batch meta-block loader (`feature = "batch"`) for fewer
    round-trips when warming the bmap.
- **Configuration**:
  - `HyperFileConfigBuilder` for building file configs from
    meta / staging / runtime / wal pieces.
  - JSON serialization of `HyperFileConfig` (template generation
    via `hypercli config gen-template`).
  - Meta config now stored in the inode, so reopens use the file's
    own configuration instead of caller-provided defaults.
  - Cache config: `data_cache_blocks` / `bmap_cache_limit` runtime
    fields.

### Fixed

- bmap insert failure now panics (the dirty cache was already
  polluted at this point and there is no path back to a consistent
  state); previously the error surfaced as a regular `io::Error`
  and let the caller continue with corrupt state.
- `update_cache` corner cases: throw away the cached entry on a full
  block overwrite; ensure block is locked when moved from cache list
  to dirty list; do not unlock if the block is still dirty.
- Truncate edge cases: handle `NotFound` on the highest key when
  extending; correct handling when shrinking from 0 with -1 diff;
  recompute target block index after split.
- Segid encoding bug in flat block-pointer format.
- Mul-overflow guard when computing block addresses on 32-bit
  targets.
- Flush lock flow under WAL: defer `set_last_flush` until
  `wal_flush_done`; correct flushing-state clear on metadata-only
  flushes.
- Reactor write/return convention: pass write task results through
  consistently so 0-byte writes don't panic.
- Strong-count trick to extend in-memory segment lifetime until
  `wal_clear_mem_segment` (was racing with reader teardown).

### Changed

- **Breaking**: `HyperTrait` reshaped multiple times; reactor /
  blocking exclusivity is now enforced via cargo features.
- Cache layer factored behind a `Cache` trait; concrete
  implementations select via cargo features.
- Default runtime config split into `default()`, `default_middle()`,
  `default_large()` profiles.
- Bunch of internal renames: `Block` → `DataBlock`, `AlignedBlock`
  → `AlignedDataBlock` → `AllocDataBlock`; `Writer` lifetime
  annotation removed.

### Tooling

- `examples/submit_batch.rs`: programmatic batch submission demo.
- `staging benchmark` code base for read/write performance harness
  (driven from `hypercli staging benchmark`).

---

## [0.1.0] - 2025-03-15

The 0.1 series imported the original prototype and turned it into a
working library with a tokio wrapper, basic POSIX-shaped APIs, and
the first wave of bug fixes. ~85 commits.

### Highlights

- File-like API over S3 with random read / write / truncate, all
  block-aligned and backed by a B+tree-indexed log-structured
  segment format.
- Tokio-friendly wrapper and runnable examples.

### Added

- **Core architecture**:
  - `HyperFile`, `Hyper`, `HyperFileHandler` (`fh`), and the
    handler-based async dispatch shape.
  - `HyperFileConfig` / `HyperFileConfigBuilder` to assemble meta,
    staging, and runtime configs.
  - `StagingConfig::new_s3_uri` / `new_s3_staging` constructors.
- **File ops**:
  - `read`, `write`, `write_zero`, `truncate`, `flush`,
    `flush_ext` (returns last cno), `last_cno`.
  - Random read/write demo example, write-zero example, batch-write
    example.
- **Cache basics**:
  - Dirty data blocks reflected into bmap on flush.
  - Read path: `NotFound` from bmap → zero block (sparse files
    work).
- **Tokio integration**:
  - `HyperFileTokio` wrapper to use `Hyper` from
    `tokio::io::AsyncRead` / `AsyncWrite` consumers.
  - Tokio-friendly examples.
- **Default profiles**:
  - `HyperFileRuntimeConfig::default_large()` for large-file
    workloads.

### Fixed

- Several truncate bugs: hardcoded data block size; last-block trim
  edge cases; handling of "not found" during the truncate-extend
  path; reverse split for dir/filename in unlink.
- MD5 mismatch now panics (was silently accepted).
- Incorrect nanosecond multiplier in time math.
- Various conditional-compile mistakes around staging features.
- Removed unsafe transmute on inode struct.
- Self-reference address bug in lifetime cheat.

### Changed

- `Block` renamed to `DataBlock`; `AlignedBlock` to
  `AlignedDataBlock`.
- HyperTrait simplified; inode ops merged into `Hyper`.
- Removed the cached `s3_client` field from `HyperFile` (moved to
  staging layer).
