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
| `O_RDONLY` | Read-only handle. | `fs_write` / `fs_truncate` return `EBADF`-shaped errors from the underlying staging layer. |
| `O_WRONLY` | Write-only handle. | Reads return zeros / errors from the underlying layer; not commonly tested. |
| `O_RDWR` | Read+write handle. | Recommended for any mutation flow. |

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
| `O_SYNC` / `O_DSYNC` | ✅ | Triggers a flush on every write. With WAL enabled, the WAL persistence already provides the same crash-consistency guarantee, so the explicit flush is skipped. The two flags are treated identically; Hyperfile does not distinguish data-only from data+metadata sync. |
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
| `write` / `write_zero` / `write_aligned_batch` / `write_batch` | — | updated | — |
| `truncate` (size changed) | — | updated | — (see Divergence) |
| `flush` / `release` | — | — | — (timestamps are persisted as-is) |
| `setattr` / `chmod` / `chown` | — | — | not currently auto-bumped |

**Divergences from POSIX**:

- POSIX says `ctime` updates on every metadata-affecting operation
  (write, truncate, chmod, chown, link/unlink). Hyperfile's
  `update_mtime` does not also bump `ctime`. This is a tracked gap;
  fix is straightforward but not yet applied to avoid breaking
  consumers that compare `ctime` between snapshots.
- `atime` updates only land on the in-memory inode. They are
  flushed lazily as part of the next data-or-metadata flush. A
  read on a read-only handle that gets dropped without flushing
  will lose the `atime` advance. Linux behaves the same way under
  default `relatime` mounts but differs from `strictatime`.

## Truncate behaviour

`fs_truncate(new_size)`:

| Case | Effect |
| --- | --- |
| `new_size == cur_size` | No-op (returns immediately). |
| `new_size < cur_size` (shrink) | Data blocks beyond `new_size` are dropped from the cache and from the persisted block index on next flush. The last partial block (if `new_size` is not block-aligned) is zero-padded from `new_size % block_size` to the end of the block. |
| `new_size > cur_size` (extend) | The file size is extended; the new range reads as zeros. No data blocks are allocated for the extended range — they are sparse until written. |
| `new_size == 0` | Equivalent to `O_TRUNC` at open time. All data blocks discarded. |

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

## Permissions and ownership

`fs_chmod(mode)`, `fs_chown(uid, gid)`, and `fs_setattr(stat)` mutate
the in-memory inode. Hyperfile does **not** enforce permission bits
on subsequent reads/writes — there is no `EACCES` path. The bits
exist for callers that layer their own access control on top
(e.g. a FUSE mount layer asking the kernel to enforce). Callers
relying on permission bits should be aware that hyperfile-the-library
treats them as opaque metadata.

## Related reading

- [`docs/concurrency.md`](concurrency.md) — multi-handle / multi-process consistency and the `range-lock` feature.
- [`docs/wal.md`](wal.md) — the durability contract added by `--features wal`.
- [`tests/README.md`](../tests/README.md) — test layout including
  the smoke suite that exercises `O_TRUNC` and `O_NOATIME`.
