# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

> Pre-1.0 caveat: while we are still on `0.x`, every minor version (`0.x.0`)
> may contain breaking API or on-disk changes. Read the **Breaking changes**
> section before upgrading.

## [0.5.1] - 2026-08-19

> **If you used `fs_block_mut` or `fh_with_block_mut` on 0.5.0, some of
> those edits may never have been persisted.** See the first entry
> below. Data written through the byte API is unaffected. There is no
> way to recover a lost edit after the fact — the segment holding it was
> written, but nothing points at it — so re-apply anything you cannot
> verify.

### Fixed

- **`fs_block_mut` silently lost an in-place edit of an already-cached
  block, for any reader that had not made the edit.** The flush returned
  `Ok` and wrote a segment, a read on the same handle returned the new
  bytes, and a fresh open returned the *previous* version. No error was
  reported anywhere.

  `block_mut` had a fast path for a block already in the cache: promote
  it into the dirty tier, mark it dirty, return — without touching the
  block map. `flush_process_build_segment` collects the set of dirty
  meta nodes *before* it assigns pointers to data blocks, so a map node
  that is not already dirty when the flush starts is never written: the
  pointer the flush stored existed only in memory, and the persisted map
  still named the old segment. The byte write paths avoid this by
  inserting a placeholder for every block they dirty, which is what
  marks the containing node dirty; the fast path skipped exactly that.

  Two things hid it. While the whole map still fitted in the inode's
  inline root the update rode along with the inode, which every flush
  writes, so it only appeared past the spill threshold — 7 blocks from
  index 0, 4 from a large index. And any byte write in the same flush
  window masked it, because that write marked the node for every
  block-API edit sitting beside it. A caller that happens to write one
  structure of its own through the byte API on every commit would never
  have seen it.

  Reported against 0.5.0 with a reproduction; the block API was
  introduced in 0.5.0, so no earlier release is affected.

- **A shrinking `truncate` within the current last block lost its tail
  zeroing** the same way, and for the same reason:
  `truncate_last_data_block` returned early when the block was already
  cached, leaving it dirty but the map node clean. The discarded region
  read back as the pre-truncate bytes after a reopen.

  This one predates the block API and is reachable through the byte API
  alone. It produces the same symptom as the truncate bug fixed in
  0.4.3, by a different route: 0.4.3 was a cross-block shrink going
  through the bmap sweep, this is a shrink that stays inside one block.
  Existing truncate coverage missed it because every case wrote few
  enough blocks to keep the map inline.

- `fs_block_mut` did not update `mtime` when the block was already
  cached.

### Changed

- The ordering constraint behind both defects is now recorded at both
  `bmap_lookup_dirty` call sites in the flush path: any path that puts a
  block into the dirty tier must insert into the bmap for that index in
  the same flush window. Neither of the two places that violated it
  looked wrong on its own, and one of them carried a comment asserting
  the opposite.

### Tests

- `block_mut_edit_survives_after_the_bmap_spills`: 8 and 30 blocks from
  index 0, and 8 from index 33554432; every block edited, every block
  checked after a reopen.
- `block_mut_and_byte_writes_interoperate_after_a_spill`: a block first
  written by `fs_write` then edited in place, and the reverse.
- `block_mut_updates_mtime`.
- `smoke_truncate_same_block_tail_persists_after_bmap_spill`: byte API
  only.
- Each was verified to fail with its fix reverted.

## [0.5.0] - 2026-08-19

### Added

- **Block API.** A caller that uses hyperfile as block storage rather
  than as a file can now borrow the cached block instead of copying
  through it. On `Hyper`:

  ```rust
  fs_block(idx)              -> Option<BlockRef>   // read borrow
  fs_block_mut(idx, create)  -> Option<BlockMut>   // in-place write borrow
  fs_block_state(idx)        -> BlockState         // how idx is mapped
  ```

  `BlockMut` needs no write-back call: the block is dirty from
  acquisition, so an early return between acquire and drop cannot lose
  the modification, and repeated borrows inside one flush window
  collapse to a single version, matching `write`. The point is to remove
  the reason such callers keep a shadow copy of every block they intend
  to modify, which is a recurring source of
  copy-diverges-from-original bugs.

  `BlockState` separates `Unmapped` / `Zero` / `Mapped`. Through the
  byte API all three read back as zeros, so a caller holding its own
  index could not tell "never written" from "real data that happens to
  be zeros" — the difference between a consistent index and a lost
  block.

  Neither borrow moves `i_size`; these callers manage their own address
  space. A block dirtied above EOF is still durable, but `read` stops at
  `i_size`, so it is reachable only through this API. `i_blocks` is
  updated either way.

- **Closure-scoped block access on the reactor surface**:
  `fh_with_block(idx, f)` and `fh_with_block_mut(idx, create, f)`.

  The reactor cannot hand out borrow guards. The `Hyper` lives inside
  the reactor task, so a guard would reach the caller only once the
  response arrives, after which the reactor is free to serve the next
  request and invalidate it. So the action travels to the block: `f`
  runs inside the reactor task while it holds the real borrow, and only
  owned values cross the channel.

- `docs/block-api.md`, covering both surfaces, the three hole states,
  why `i_size` is left alone, and why the reactor surface takes a
  closure.

### Fixed

- **The local-disk *data* cache aborted the process on release.**
  `close` called `libc::close` on a descriptor owned by a
  `std::fs::File`, and both `Cache::shutdown` — the normal path from
  `release` — and `Drop` called it:

      fatal runtime error: IO Safety violation: owned file descriptor
      already closed, aborting

- **The same tier corrupted memory on an extending write.** It mapped
  the file's own address space and located a block at
  `addr + blk_idx * data_block_size`, but the cache is told the new size
  only *after* the blocks are created, so a write past EOF minted a view
  outside the mapping and wrote through it. Observed as `SIGSEGV` and as
  dynamic-loader assertion failures. Three related consequences of the
  same addressing scheme: a newly created file has size zero and `mmap`
  rejects a zero length, so the tier could not create a file at all; the
  mapping could not grow, since `mremap` was called without
  `MREMAP_MAYMOVE` and its result asserted unmoved, which it must be
  because views already handed out point into it; and a sparse file
  could not be mapped, because the mapping had to span the address space
  rather than the resident blocks.

  The mapping is now a fixed pool of block-sized slots, sized from the
  cache capacity and the flush threshold, and a block takes whichever
  slot is free. A block index is never used as an offset. A block's slot
  is derived from its address rather than tracked separately, so the two
  cannot disagree. When the pool is exhausted — one large write dirties
  every block it touches before the flush check runs — blocks fall back
  to heap allocations, costing memory rather than correctness.

- **`write`, `flush`, `read` within one open panicked on the same tier**
  with `assertion failed: !block.is_locked()`. `plan_read` probed the
  cache with `Cache::get`, which mlocks the block it hands out of the
  clean tier and asserts on the next `get` that it was not already
  locked; the read executor then called `get` for real. Reopening the
  file masked it, since the clean tier starts empty and reads miss.

  None of these three were reachable through the default (in-memory)
  data cache, and no test selected the local-disk tier, which is why
  they went unnoticed.

### Changed

- Blocks newly created in the local-disk cache are zeroed explicitly.
  Releasing a slot punches its hole, so a recycled slot did read as
  zeros, but depending on that made every release path load-bearing for
  correctness rather than only for reclaiming space. Partially-filled
  new blocks have caused two stale-data bugs already (0.4.3, 0.4.5).
- The local-disk cache file's size now follows the cache's capacity
  instead of the file's size. It is scratch space with no persistent
  contents — nothing repopulates the cache at open — so this is not an
  on-disk format change, but the file on disk will be a different size
  than before.

### Tests

- `integration_s3_block_api`: 9 cases, run under all three data cache
  configurations (in-memory, local-disk, disabled), covering the borrow
  contents, hole/zero/data discrimination, in-place persistence,
  `create`, one-version-per-flush-window, `i_size` being untouched, and
  access-mode enforcement.
- `integration_s3_local_disk_cache`: 7 cases, the first suite to select
  that tier. Creating an empty file, releasing repeatedly without
  aborting, writing far past EOF, a 4 TiB sparse address space, slot
  recycling under constant eviction, truncate releasing slots without
  leaving stale bytes, and pool exhaustion.
- `integration_reactor_s3_block_api`: 6 cases, including a check that
  the same edits applied through `fs_block_mut` and through
  `fh_with_block_mut` produce byte-identical files.
- All three should be run under debug assertions as well as release:
  much of what they cover only fails with `debug_assertions` on. See
  `tests/README.md`.

## [0.4.6] - 2026-08-17

### Fixed

- **A segment smaller than 512 KiB could not be opened.**
  `SegmentReadWrite::open` speculatively read `SEGMENT_HEADER_FETCH_SIZE`
  (512 KiB) into a pre-sized buffer, and the underlying getter rejects a
  response shorter than the buffer ("feched size 28672 less than input
  buffer size 524288"). A short read is expected here, since the range
  deliberately asks for more than the object may hold.

  New `S3Ops::do_get_object_speculative` returns the body as `Bytes`
  rather than filling a caller-supplied buffer, which removes the
  length mismatch by construction; the exact-length getter is unchanged
  for its other callers, which all request an exact byte count. It also
  maps `NoSuchKey` to `ErrorKind::NotFound`.

  `open` has no caller inside hyperfile — reads go through the block map
  and the block loaders — so this only affected segment tooling:
  `hyperfile-cleaner` uses it, which broke `prune` (and hypercli
  `file prune` / `file du`) on files written with small flushes, i.e. one
  flush per filesystem commit.

- **Unbounded allocation and an integer underflow in the same path.** The
  top-up read for a summary larger than the speculative fetch sized its
  buffer from `s_bytes`, a `u32` off the segment header, so a corrupted
  value could dictate an allocation of up to ~4 GiB. The size was also
  computed before the guard that is its only consumer, so it underflowed
  for any summary below 512 KiB — the normal case, since `s_bytes` covers
  the summary rather than the segment (240 bytes for an 8 KiB segment,
  4304 for a 1 MiB one). That panicked under debug assertions and was
  silently discarded in release.

  The top-up now uses the speculative read as well, so it is bounded by
  data that actually exists and no length is derived from the header. A
  summary still short afterwards is reported as `InvalidData` naming both
  sizes, instead of over-allocating or parsing a truncated summary.

- The speculative range end was off by one: `bytes=0-524288` is
  inclusive, so it requested 524289 bytes.

### Tests

- New `integration_s3_segment_open` suite covering single-flush segments
  of 4 KiB, 8 KiB, 512 KiB, 1 MiB and 16 MiB — below, at and above the
  speculative fetch size. `open` previously had no coverage because it
  has no in-crate caller. The suite must also be run under debug
  assertions, since the underflow above was invisible in release; see
  `tests/README.md`.

## [0.4.5] - 2026-08-15

### Fixed

- **Panic on every data-cache lookup when the clean-block cache is
  disabled.** The guard
  `(data_cache_blocks > 0).then(|| cache.op()).unwrap()` is inverted:
  `bool::then` yields `None` for a false condition, so the `.unwrap()`
  panicked exactly when the cache was *disabled*.

  This was reachable from hyperfile itself, not just from a bad config
  value: both the create and open paths force `data_cache_blocks` to 0
  when `O_DIRECT` is set and the `wal` feature is off, so **`O_DIRECT`
  was unusable without `wal`** — the first read after a flush aborted
  the process. For a FUSE daemon that means the mount dies and
  applications see `ENOTCONN`.

  Fixed at all 12 sites across `mem_cache` and `local_disk_cache`. The
  ten `get` / `pop` sites now use `.flatten()`. The two `clear_dirty`
  sites are restructured, because there the guard also decides the fate
  of a block that cannot be cached: the in-memory cache drops it (the
  data is already persisted and the buffer is a heap allocation), and
  the local-disk cache drops it **and punches the backing hole** — its
  blocks are unzeroed mmap views, so leaving the bytes behind would let
  a later write to the same index observe pre-drop content, the same
  failure class as the stale-clean-block bug fixed in 0.4.3.

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
