# Concurrency Semantics

This doc describes how Hyperfile behaves when multiple writers, or a
writer and a reader, access the same file concurrently. It complements
the README's short "Consistency" note with the full policy surface and
rationale.

## Model

A `Hyper` instance is the unit of concurrency. Each instance owns its
own in-memory inode, bmap, and data cache. When two instances open the
same S3 URI they are **independent**: there is no shared memory and no
in-process coordination between them. All synchronization happens
through S3's conditional write semantics.

Within a single `Hyper` instance, writes are serialized by an internal
semaphore (permits = 1) unless the `range-lock` feature is enabled.
Concurrency bugs at this layer are not the subject of this doc; here we
focus on the multi-instance case.

## S3 optimistic concurrency control (OCC)

On flush, Hyperfile persists two kinds of objects:

1. Segment files (`0000000001`, `0000000002`, ...) — the append-only log
   of data and meta blocks.
2. The `inode` file — the pointer to the current "head" of the log.

Both writes go through `S3Ops::do_put_object`, which uses the ETag of
the last-known on-disk state as an `If-Match` precondition:

```
PUT s3://bucket/key
    If-Match: <etag from last load>
```

If another writer has modified the object since our load, S3 rejects
the request with `412 Precondition Failed` (or `409 Conflict` in
some Express One Zone cases). Hyperfile surfaces this as
`std::io::ErrorKind::AlreadyExists`.

The error message always contains the phrase `concurrent modification`
for easy grep/log filtering, but callers should match on the `kind()`,
not the message string.

## `FlushConflictPolicy`

`HyperFileRuntimeConfig::flush_conflict_policy` controls how `flush`
handles `AlreadyExists` conflicts. It has two values:

### `RetryLastWriterWins` (default)

When a flush hits `AlreadyExists`, Hyperfile:

1. Refreshes local bmap / inode state from the persisted S3 state
   (via `refresh_bmap`).
2. Re-runs `flush_process`: allocates a new segment id, writes a new
   segment file, and attempts the inode write again (now with the
   updated `If-Match` precondition).
3. Retries up to `DEFAULT_FLUSH_RETRIES` (3) times with exponential
   backoff.

From the caller's perspective `flush` returns `Ok` as long as some
retry attempt succeeds. The caller never observes the conflict. In a
multi-writer scenario this means **the later writer's data overwrites
the earlier writer's data** — in effect, last-writer-wins semantics.

This is the pre-existing behavior and is the default to preserve
backward compatibility. It is appropriate when:

- The workload is effectively single-writer (multi-writer races are
  rare and acceptable when they happen), or
- The user's semantics treat any recent write as equally valid (e.g.
  log aggregation where only the most recent snapshot matters).

### `FailFast`

When a flush hits `AlreadyExists`, Hyperfile returns the error
immediately without retrying. `fs_flush` (which wraps `flush` with
`flush_with_rollback`) rolls back in-memory state to match the
now-updated persisted state, so the caller can observe the latest
committed data by reading again.

This mode is appropriate when:

- The user cannot tolerate silent overwrites.
- The user wants to implement a read-modify-write loop with explicit
  conflict handling.
- Multiple writers are expected and correctness requires at most one
  of them to succeed per conflicting flush.

Under `FailFast`, exactly one of N concurrent writers on the same URI
will see `Ok(segid)`; the others will see
`Err(ErrorKind::AlreadyExists)`.

## Behavior matrix

| Scenario | `RetryLastWriterWins` (default) | `FailFast` |
| --- | --- | --- |
| Two concurrent writers on same URI | Both report `Ok`; one overwrites the other | Exactly one `Ok`, others `Err(AlreadyExists)` |
| Single writer, transient S3 5xx | S3 SDK retries internally; Hyperfile returns `Ok` | Same |
| Single writer, internal lock busy (`ResourceBusy`) | Retry up to 3x | Same (policy only affects `AlreadyExists`) |
| Non-retryable error (`Other`, `NotFound`, etc.) | Return immediately | Return immediately |
| `flush` retries exhausted | Return `Err(ResourceBusy)` with "max retries" message | N/A (FailFast doesn't retry `AlreadyExists`) |

## Reader semantics

Readers (`FileFlags::rdonly`) do not participate in OCC; they simply
load the latest persisted inode at open time and read from whichever
segment the inode points to. A reader always sees a self-consistent
checkpoint; it cannot observe a partially-committed segment.

If a writer is mid-flush, the reader either sees the state before the
flush started or the state after it completed — never a mix.
Concurrent readers and writers have no mutual exclusion requirement.

## Choosing a policy

Start with the default (`RetryLastWriterWins`). Switch to `FailFast`
only if you actually need strict conflict detection, because `FailFast`
requires the caller to handle retries explicitly.

```rust
use hyperfile::config::{FlushConflictPolicy, HyperFileRuntimeConfig};
use hyperfile::file::hyper::Hyper;
use hyperfile::file::flags::FileFlags;

let runtime = HyperFileRuntimeConfig {
    flush_conflict_policy: FlushConflictPolicy::FailFast,
    ..HyperFileRuntimeConfig::default()
};

let mut hyper = Hyper::fs_open_opt(&client, uri, FileFlags::rdwr(), &runtime).await?;
```

A typical `FailFast` retry loop looks like:

```rust
use std::io::ErrorKind;

loop {
    // Re-read state before attempting the modification.
    let mut hyper = Hyper::fs_open_opt(&client, uri, FileFlags::rdwr(), &runtime).await?;
    // ... make modifications ...
    match hyper.fs_flush().await {
        Ok(_) => break,
        Err(e) if e.kind() == ErrorKind::AlreadyExists => {
            // Conflict: another writer committed first. Re-read and retry,
            // possibly with application-level conflict resolution.
            continue;
        }
        Err(e) => return Err(e),
    }
}
```

## Implementation notes

- `AlreadyExists` is emitted from three call sites in `s3commons.rs`:
  `do_put_object` (412/409), `do_delete_object` (412), and
  `do_mp_upload`'s `CompleteMultipartUpload` (412/409).
- The retry loop lives in `HyperTrait::flush` in `src/file/mod.rs`.
- `ErrorKind::ResourceBusy` is reserved for non-OCC busy conditions
  (internal flush lock, range-lock contention, reactor flushing state).
  It is always retried regardless of policy.
- The policy does not cross `Hyper` instance boundaries: opening a
  second `Hyper` on the same URI with a different policy is allowed
  and each instance's flushes follow its own policy independently.

## Related tests

- `concurrent_two_writers_optimistic_cc` — default policy, two
  writers, both report success; persisted content is consistent with
  one writer.
- `concurrent_two_writers_fail_fast_policy` — `FailFast` policy,
  two writers, exactly one returns `Ok` and the other returns
  `Err(AlreadyExists)`.
- `concurrent_writer_a_fails_writer_b_succeeds` — one writer has a
  fault-injected interceptor, the other commits normally.
- `concurrent_read_while_writer_flushes` — concurrent reader always
  observes a self-consistent checkpoint.
- `contract_flush_retries_on_resource_busy` — verifies `ResourceBusy`
  still retries regardless of `FlushConflictPolicy`.
