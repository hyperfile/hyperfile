# POSIX Semantics

Hyperfile aims to follow POSIX file semantics as closely as the
underlying object-storage substrate allows. This doc enumerates,
flag by flag and operation by operation, what Hyperfile actually
does today.

When Hyperfile diverges from POSIX, the divergence is called out
explicitly with a "Divergence" note. Anything not mentioned here is
unimplemented or undefined; treat it as a known gap.

## Open flags (`FileFlags` / `O_*`)

`FileFlags::from(libc::c_int)` accepts any combination of the flags
below. The crate-internal `HyperFileFlags::from_flags` translates
the bitset into structured fields and feeds them through `Hyper::
open` / `Hyper::create`.

### Access mode (mutually exclusive)

| Flag | Behaviour | Notes |
| --- | --- | --- |
| `O_RDONLY` | Read-only handle. | Every write-side operation (`fs_write`, `fs_write_zero`, `fs_write_batch`, `fs_write_aligned_batch`, `fs_truncate`, and the `fh_*` / tokio equivalents) fails with `EBADF`. See [Write access enforcement](#write-access-enforcement). |
| `O_WRONLY` | Write-only handle. | Writes behave as with `O_RDWR`. Reads are **not** rejected — hyperfile does not yet return `EBADF` for a read on a write-only handle, which POSIX requires. |
| `O_RDWR` | Read+write handle. | Recommended for any mutation flow. |

### Write access enforcement

A handle opened without write access rejects every write-side
operation with `EBADF`:

* POSIX `write()` lists `[EBADF] The fildes argument is not a valid
  file descriptor open for writing` as a mandatory ("shall fail")
  error.
* POSIX `ftruncate()` permits either `[EBADF]` or `[EINVAL]` for the
  same condition. Hyperfile uses `EBADF` so that all write-side
  operations report one errno.

The check keys off the access mode only: `O_APPEND` does not grant
write access on its own, matching Linux, where
`open(O_RDONLY | O_APPEND)` followed by a `write` fails with `EBADF`.

Because `std::io::ErrorKind` has no `EBADF` variant, the error is
constructed with [`std::io::Error::from_raw_os_error`]. Callers should
test it with `err.raw_os_error() == Some(libc::EBADF)`; `err.kind()`
is the unmatchable `Uncategorized` and must not be used. (The
alternatives were rejected as inaccurate: `PermissionDenied` maps to
`EACCES`, which POSIX reserves for permission-bit failures at `open`
time, and `InvalidInput` maps to `EINVAL`, conformant for `ftruncate`
but not for `write`.)

Not covered yet: a read on an `O_WRONLY` handle should also fail with
`EBADF` and currently does not.

### File-creation flags

| Flag | Implemented | Behaviour |
| --- | --- | --- |
| `O_CREAT` | ✅ | Use `Hyper::fs_open_or_create_*` (or the reactor / tokio equivalents). The function performs an `open`, and on `NotFound` falls back to `create`. The bare `Hyper::fs_open` does **not** honour `O_CREAT`. |
| `O_EXCL` | ✅ | Honoured on the `fs_open_or_create_*` family of entry points: when paired with `O_CREAT`, the open MUST create a new file and must fail with `ErrorKind::AlreadyExists` if one already exists. Implementation: `Hyper::do_open_or_create` short-circuits to `Hyper::create()` when `O_EXCL` is set, skipping the try-open dance, so the create-or-fail attempt is a single S3 round trip (a HEAD on the inode, then a conditional PUT on miss). On the bare `fs_open` path (no `O_CREAT`) the bit is silently ignored, matching Linux behaviour where POSIX leaves the combination undefined. The `fs_create` entry point is always exclusive regardless of the flag — hyperfile has no "open existing or create new" mode that lacks an exclusivity check on missing-file. |
| `O_TRUNC` | ✅ | Honoured by `Hyper::open` (and therefore `fs_open`, `fs_open_opt`, `fh_open*`, `HyperFileTokio::open*`, plus the existing-file path of `fs_open_or_create_*`). When set together with `O_WRONLY` or `O_RDWR`, the file's length is set to 0 immediately after the open succeeds, before the handle is returned. The truncation runs through the normal `truncate(0)` path: `mtime`/`ctime` are updated and the change is staged for the next flush. **Divergence**: the truncation is not durable until you flush or release the file. POSIX requires `O_TRUNC` to be effectively immediate; Hyperfile defers persistence the same way it defers writes. |
| `O_TRUNC` (without write access) | ✅ no-op | If the access mode is `O_RDONLY`, `O_TRUNC` is silently ignored, matching glibc / Linux. |

### Access pattern flags

| Flag | Implemented | Behaviour |
| --- | --- | --- |
| `O_APPEND` | ✅ | The offset argument to `fs_write` / `fh_write` / `fs_write_zero` / `fh_write_zero` is ignored; the call writes at the current `i_size` and atomically advances `i_size` by the number of bytes written. Atomicity is enforced through the per-file write serializer: in the direct API, `&mut self` enforces it; in the reactor without `range-lock`, the handler dispatches one ctx at a time; in the reactor with `range-lock`, concurrent appenders compute identical-or-overlapping byte ranges starting at the current `i_size` and only one wins the range lock per turn — the loser is re-queued via send_highprio and re-evaluates `i_size` on its next dispatch. The result is whole, contiguous payloads laid down in some serial order; never a torn interleave. **Divergence**: cross-instance / cross-process append (two `Hyper`s opened against the same URI) is **not** atomic — it is governed by the same conflict policy as ordinary writes, not by O_APPEND. |
| `O_APPEND` (batch APIs) | n/a | `fs_write_aligned_batch` / `fs_write_batch` accept an explicit `offset` per data block; that is the entire point of the batch APIs. They ignore O_APPEND on the open flag set. Use the single-write entry points if you want POSIX append semantics. |
| `O_DIRECT` | ✅ | Disables the in-memory data block cache. Without WAL: every write triggers an immediate flush. With WAL: writes still go to the WAL synchronously but the data-block cache is sized to zero so there is no in-memory accumulation. |
| `O_SYNC` / `O_DSYNC` | ✅ | Triggers a flush on every write. With WAL enabled, the WAL persistence already provides the same crash-consistency guarantee, so the explicit flush is skipped. The two flags are treated identically at write time: every write in hyperfile already updates `i_size` (which `fdatasync` is required to persist), so there is no work that O_DSYNC could legitimately skip but O_SYNC must do. The fsync vs. fdatasync **distinction is exposed at the syscall-equivalent layer** instead — see `fs_flush` (= `fsync`) and `fs_fdatasync` (= `fdatasync`) below. |
| `O_NOATIME` | ✅ | Read paths skip `update_atime` on the in-memory inode. Other timestamps (`mtime`, `ctime`) are unaffected. |
| `O_NONBLOCK` / `O_NDELAY` | ❌ unsupported | These flags govern the read/write blocking discipline of file descriptors in the kernel, where a non-blocking read on an empty pipe returns `EAGAIN` instead of suspending. Hyperfile has no equivalent state machine: every async fn already returns control to the runtime when waiting on S3 I/O, so there is nothing to flip on. Parsed for display only; setting the bit changes no behaviour and there is no plan to add semantics for it. |
| `O_ASYNC` | ❌ unsupported | Requests SIGIO / SIGURG signal-driven I/O on POSIX file descriptors. Hyperfile is a library, not a process running under a kernel fd; signal delivery is outside the model. Parsed for display only and explicitly **not** going to be implemented. Use the standard async/await flow against the existing `fs_*` / `fh_*` / `HyperFileTokio` APIs instead. |

### Path resolution flags

`O_DIRECTORY`, `O_NOFOLLOW`, `O_NOCTTY`, `O_PATH`, `O_LARGEFILE`,
`O_CLOEXEC` are all parsed for display but Hyperfile does not
operate on directories or file descriptors in the kernel sense, so
they are no-ops.

## Sync mode quick reference

The same information as the table above, presented as a behaviour
matrix for the three flush-affecting flags. WAL changes the
"persistence on write" column.

### WAL disabled

| Flag | Data block cache size | Immediate flush after write | Persistence on `fs_write` ack |
| --- | --- | --- | --- |
| (none) | from config | False | None until flush |
| `O_DIRECT` | 0 | True | Yes (flush completed) |
| `O_SYNC` / `O_DSYNC` | from config | True | Yes (flush completed) |

### WAL enabled

| Flag | Data block cache size | Immediate flush after write | Persistence on `fs_write` ack |
| --- | --- | --- | --- |
| (none) | from config | False | Yes (WAL PUT completed; full flush deferred) |
| `O_DIRECT` | 0 | False | Yes (WAL PUT completed) |
| `O_SYNC` / `O_DSYNC` | from config | False | Yes (WAL PUT completed) |

## Inode timestamps

Hyperfile stores `atime`, `mtime`, `ctime` in the on-disk inode
with second + nanosecond resolution. They update as follows:

| Operation | atime | mtime | ctime |
| --- | --- | --- | --- |
| `read` (cache hit, `bytes_read > 0`) | updated unless `O_NOATIME` | — | — |
| `read` (cache miss) | updated unless `O_NOATIME` | — | — |
| `write` / `write_zero` / `write_aligned_batch` / `write_batch` | — | updated | updated |
| `truncate` (size changed) | — | updated | updated |
| `chmod` | — | — | updated |
| `chown` | — | — | updated |
| `setattr` | from caller (if differs) | from caller (if differs) | updated to NOW (caller's `st_ctime` is ignored — POSIX says `ctime` is not user-settable) |
| `flush` / `release` | — | — | — (timestamps are persisted as-is) |

**Divergences from POSIX**:

- `atime` updates only land on the in-memory inode. They are
  flushed lazily as part of the next data-or-metadata flush. A
  read on a read-only handle that gets dropped without flushing
  will lose the `atime` advance. Linux behaves the same way under
  default `relatime` mounts but differs from `strictatime`.

## Sync APIs (`fsync` vs `fdatasync`)

POSIX distinguishes two flush primitives:

- `fsync(fd)` — persist data and **all** metadata (data, file
  size, atime, mtime, ctime, mode, uid, gid).
- `fdatasync(fd)` — persist data and metadata that's required
  for the data to be read correctly, allowed to skip pure
  attribute updates that don't affect retrieval (atime, mtime,
  ctime, mode, uid, gid).

Hyperfile exposes both:

| Hyperfile API | POSIX equivalent | Behaviour |
| --- | --- | --- |
| `Hyper::fs_flush` / `HyperFileHandler::fh_flush` / `HyperFileTokio::flush_ext` | `fsync` | Always persists. If only attrs are dirty, issues an inode-only PUT (overwrites the same S3 key, no new segment, `last_cno` unchanged). If data or bmap is dirty, builds and uploads a new segment. |
| `Hyper::fs_fdatasync` / `HyperFileHandler::fh_fdatasync` / `HyperFileTokio::fdatasync_ext` | `fdatasync` | Skips when only attrs are dirty (no S3 PUT, no `last_cno` change). When data or bmap is dirty, falls through to the same path as `fs_flush`: hyperfile keeps `i_size` in the inode, so any data flush already needs a new segment that carries the inode along. |

Observable difference: a read on an otherwise-clean file dirties
`atime` in memory. `fs_flush` writes the inode out; subsequent
opens see the advanced atime. `fs_fdatasync` skips; the atime
change is lost if the handle is dropped without an `fs_flush` /
`fs_release`. This matches the POSIX rule.

When data is dirty, both calls do the same work, including
issuing a new segment. There is no "data-only segment" path;
`i_size`, mtime, and the bmap travel together with data in the
on-disk format.

## Truncate behaviour

`fs_truncate(new_size)`:

| Case | Effect |
| --- | --- |
| `new_size == cur_size` | No-op (returns immediately). |
| `new_size < cur_size` (shrink, mid-block) | Data blocks beyond `new_size` are dropped from the cache and from the persisted block index on next flush. The block containing byte `new_size - 1` is the **last partial block**; its bytes `[new_size % block_size, block_size)` are zero-padded so a future extending write or read across that boundary cannot expose stale data. |
| `new_size < cur_size` (shrink, exact block boundary) | Same as above except no partial-block zeroing happens — the last fully-retained block (`new_size / block_size - 1`) is kept untouched. (Pre-fix bug: the code used to call the partial-block path with `offset_to_discard = 0`, wiping the block.) |
| `new_size > cur_size` (extend) | The file size is extended; the new range reads as zeros. No data blocks are allocated for the extended range — they are sparse until written. `st_blocks` does not change. |
| `new_size == 0` | All data blocks discarded; bmap fully truncated; `i_blocks` goes to 0. |

Truncate updates `mtime`. It does **not** itself flush; pair with
`fs_flush` or `fs_release` to persist.

## Read on a zero-length file

A `fs_read` issued on a file whose `i_size` is 0 returns `Ok(0)`
immediately, before consulting the bmap or staging. The same
applies for an offset at or past `i_size`.

## Sparse holes (write past EOF without O_APPEND)

`fs_write(off, buf)` where `off > i_size` (and the handle was not
opened with O_APPEND) is allowed. The intervening range is sparse: a subsequent `fs_read` of that range
returns zeros, and the persisted block index records no allocation
for the gap. `i_size` becomes `off + buf.len()`. This is the
standard POSIX "sparse hole" behaviour.

## `stat()` field semantics

`fs_getattr` / `fh_getattr` populate a `libc::stat`. Field rules:

| Field | Source / behaviour |
| --- | --- |
| `st_dev`, `st_rdev` | Always 0. Not modelled — hyperfile is not a device-backed filesystem. (Tracked TODO.) |
| `st_ino` | The inode number from the on-disk inode. |
| `st_nlink` | Always 1; hyperfile has no concept of hard links. |
| `st_mode` | File-type bits (`S_IFREG` / `S_IFDIR`) plus permission bits as last set by `chmod` / `setattr`. |
| `st_uid`, `st_gid` | As last set by `chown` / `setattr`. Default: 1000 / 1000 at creation. Not enforced by the library; see "Permissions and ownership" below. |
| `st_size` | Logical file length in bytes (`i_size`). For sparse files this is the **virtual** size, not the storage footprint. |
| `st_blksize` | The data-block size from the file's `HyperFileMetaConfig` (default 4096). Acts as the I/O hint that POSIX intends. |
| `st_blocks` | Number of 512-byte units actually allocated. **Sparse-aware**: a 1 GiB sparse file with one 4 KiB written block reports `st_blocks = 8` (= 4096 / 512), not 2 097 152. Maintained incrementally: each newly-allocated bmap entry adds `data_block_size / 512` to the counter; truncate-shrink subtracts the units of the entries it discards. |
| `st_atime`, `st_mtime`, `st_ctime` (+ `_nsec`) | See the "Inode timestamps" section. |

**Divergence**: `st_dev` and `st_rdev` are not yet plumbed; both
report 0. If you need a stable device id (e.g. for a FUSE adapter)
you must inject one in the layer above.

## Rename

Hyperfile exposes `Hyper::fs_rename(client, src_uri, dst_uri)`
and the `HyperFileHandler::fh_rename` mirror, but **the
operation is not yet implemented** — the call returns
[`ErrorKind::Unsupported`] today.

The interface is reserved at the API layer so that callers can
write code today that targets the eventual rename without
reaching for a different namespace later. Any such code will
fail with `Unsupported` until the implementation lands; treat
this as a hard signal, not a transient error.

### Why it isn't done yet

A POSIX-faithful `rename(2)` requires:

1. **Atomic move of every backing object.** A hyperfile lives
   as several S3 objects under a shared URI prefix (the inode
   key, segment keys, optional WAL chunks, optional local cache
   files). S3 has no native cross-key atomic-rename primitive;
   the natural implementation is a copy-each-object,
   then-delete-the-originals loop, which is **not** atomic.
2. **Atomic replacement of the destination.** POSIX requires
   that if `dst_uri` already exists, the old destination is
   atomically replaced. Under copy-then-delete, a concurrent
   reader on either prefix can observe an intermediate state.
3. **Coordination with open handles.** An in-flight
   `fs_write` / `fs_flush` against either URI would race the
   rename. Either rename has to fail when handles are open, or
   the handles need to be re-pointed atomically (currently they
   bind to a fixed URI at construction).
4. **Feature interactions.** With WAL on, the WAL prefix lives
   at a sibling URI and would need to move in lock-step with
   the file. With a local cache, cached blocks under the source
   URI need to be invalidated or rewritten under the destination
   URI.

Each item is solvable on its own; the open question is which
non-strict atomicity guarantees are acceptable, and that's a
design decision rather than a coding task.

### Recommended workaround until rename lands

The closest hand-rolled alternative is application-driven:

1. Open the source URI with `Hyper::fs_open`, read the data,
   close.
2. Create the destination URI with
   `Hyper::fs_open_or_create_with_default_opt`, write the data,
   release.
3. Call `Hyper::fs_unlink(source_uri)` once the destination is
   safely persisted.

This is **not atomic**, makes a full data copy, and breaks any
open handles to the source. It is fine for offline /
single-tenant relocations of small-to-medium files; do not
build "atomic rename" semantics on top of it.

## Permissions and ownership

Hyperfile **does not enforce** the POSIX permission model. The
`mode`, `uid`, and `gid` fields stored in the inode are
**opaque metadata** from the library's perspective: hyperfile
records what callers ask it to record, persists it across
flushes, and surfaces it back through `fs_getattr`, but every
read / write / truncate / chmod / chown / setattr succeeds
**regardless of the bits stored**. There is no `EACCES` /
`PermissionDenied` code path anywhere in the read/write
pipeline.

Concretely:

- A handle opened with `FileFlags::rdonly()` will accept
  `fs_write` calls (the access-mode bits influence cache
  sizing and sync-flush behaviour, but they do not gate
  mutation operations).
- A file with `mode = 0o000` and `uid = 0` is just as
  read/writable through the hyperfile API as `0o777`.
- `fs_chmod` / `fs_chown` succeed even when the caller is not
  the file's nominal owner.

This is by design. Hyperfile is a Rust library, not a process
running as a specific user; it has no notion of a "current
user" to check the bits against, and inventing one
(thread-local "set-uid" or per-call credentials) would only
provide self-attestation, not security.

### How to enforce, if you need to

Build the access-control layer **outside** hyperfile:

- **FUSE adapters**: rely on the kernel's `default_permissions`
  mode or its FUSE-protocol equivalent. The kernel does the
  uid/gid comparison against the inode bits hyperfile returns
  via `fs_getattr` before any read/write reaches hyperfile.
- **Multi-tenant services**: use IAM / app-level auth to
  decide whether a request gets to call hyperfile at all. The
  hyperfile mode bits then become a documentation /
  reflection-only field, not a security boundary.
- **Single-tenant tools**: typically don't need enforcement;
  the process owns its files and the POSIX bits are
  cosmetic.

If you build a layer that DOES check, remember:

- Hyperfile has no `O_EXEC`-equivalent gate. A reader-only
  layer must reject writes itself; hyperfile won't.
- `fs_chmod` on a file you "shouldn't be able to chmod" still
  succeeds at the hyperfile level. Either don't surface the
  call, or check before forwarding.
- `umask`, supplementary groups, set-uid/set-gid bits,
  capabilities, ACLs, and POSIX file capabilities are all
  out of scope.

## Related reading

- [`docs/concurrency.md`](concurrency.md) — multi-handle / multi-process consistency and the `range-lock` feature.
- [`docs/wal.md`](wal.md) — the durability contract added by `--features wal`.
- [`tests/README.md`](../tests/README.md) — test layout including
  the smoke suite that exercises `O_TRUNC` and `O_NOATIME`.
