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

Within a single `Hyper` instance, how writes are serialized depends on
the access mode (direct vs reactor) and whether the `range-lock`
feature is enabled. See the next section.

## Access modes and locking

Hyperfile supports two programming models against the same core engine:

- **Direct API** — `Hyper::fs_write`, `fs_truncate`, `fs_write_zero`,
  etc. Each method takes `&mut self`. Concurrent invocations from a
  single `Hyper` are prevented at compile time by the Rust borrow
  checker.
- **Reactor API** — `HyperFileHandler` / `HyperFileTokio`. A
  `LocalSpawner` runs a handler loop that receives `FileReq` messages
  through a channel. Multiple tasks can send requests into the handler
  concurrently.

These two modes use different runtime-locking strategies.

### Direct API locking

Every write/truncate path acquires an `OwnedSemaphorePermit` from
`HyperFile::sema` before touching bmap or cache. The permit count is:

| File flags | `range-lock` feature | Semaphore permits | Effective serialization |
| --- | --- | --- | --- |
| `rdonly` | either | `MAX_PERMITS` | None (reads are safe to parallelize) |
| `rdwr` / `wronly` | off (default) | **1** | Writes inside a `Hyper` are serialized |
| `rdwr` / `wronly` | on | `MAX_PERMITS` | None from the semaphore |

In the direct API the `&mut self` borrow checker already prevents
concurrent invocation of mutating methods from a single `Hyper`, so
serialization is mostly redundant; the semaphore exists for the cases
where `acquire_owned()` is held across an await point that yields to
another task in the same runtime.

When the `range-lock` feature is on, the semaphore is intentionally
loosened to `MAX_PERMITS` because the reactor API (below) takes over
serialization at the range level. **The direct API does not itself
acquire a range-lock** — the `range_lock` field on `HyperFile` is only
consulted by the reactor handler.

### Reactor API locking

The reactor handler processes one request at a time in the per-handler
task, but the request itself can spawn additional work
(`spawn_read` / `spawn_write` / `spawn_write_zero`) that runs as
independent tokio tasks. These spawned tasks can be in flight
concurrently for non-overlapping byte ranges, and concurrency is bounded
by the `range-lock` feature:

- **`range-lock` off (default)**: the per-file semaphore has 1 permit,
  so spawned writes are serialized even though they run as separate
  tasks. No range-level parallelism.
- **`range-lock` on**: the semaphore has `MAX_PERMITS`, but every
  spawned read/write path first calls `RangeLock::try_lock` on the
  byte range it will touch (aligned to `data_block_size`). If the
  range overlaps an in-flight lock the request is requeued onto the
  high-priority queue and retried later. Non-overlapping ranges run in
  parallel.

Flush interacts with this: the handler checks
`range_lock.is_locked()` before kicking a flush so flush never races
with an in-flight write.

### Why two modes, why two strategies

The direct API is the simplest integration point: call a method, await,
check the result. It benefits from Rust's borrow rules; an additional
runtime lock would add overhead without catching new bugs.

The reactor API exists to let independent callers (e.g. the FUSE
kernel pushing many outstanding requests in parallel, or an NFS-style
server) share one `Hyper` instance without having to serialize at the
user-code layer. Range-level locking is what enables that sharing —
the range-lock feature is effectively *the* reason the reactor API is
useful for highly parallel workloads.

### Summary

- If you program against `Hyper` directly: leave `range-lock` off. The
  borrow checker + default semaphore already give you safe semantics.
- If you use `HyperFileHandler` / `HyperFileTokio` and want parallel
  per-range writes, enable `range-lock`. Otherwise leave it off for
  simpler serialization.

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

## Reactor failure modes

This section describes how errors propagate in the reactor API
(`HyperFileHandler`, `HyperFileTokio`). The direct API is simpler —
each `fs_*` call returns a `Result` and that is the whole story —
but the reactor introduces a request/response channel and a handler
task, and a few of its error paths are not obvious from the method
signatures.

### Three classes of response

The reactor handler in `src/file/handler.rs` follows a three-way
contract documented at `src/file/reactor.rs`:

```text
Ok(bytes)         => send Ok(bytes) back on the response channel
Err(ResourceBusy) => task was internally requeued; do NOT send
                     anything on the response channel yet
Err(e)            => unlock the range (if range-lock is on) and
                     send Err(e) back
```

The practical consequence is that `ResourceBusy` inside the reactor
does **not** mean the same thing as `ResourceBusy` from the direct
API:

- **Direct API**: `ErrorKind::ResourceBusy` is a real error returned
  to the caller; the operation did not complete.
- **Reactor API**: `ResourceBusy` inside the handler means "the
  request was requeued into the handler's own callback queue (via
  `TaskHandler::send_cb`) to be retried later"; the caller does not
  observe it. Callers of `fh_write` etc. only ever see the final
  `Ok` or the final non-busy `Err`.

### Causes of reactor-internal `ResourceBusy`

Grepping `src/file/reactor.rs` for `ResourceBusy` lists the triggers
for an internal requeue:

- A flush is ongoing and the incoming operation cannot proceed
  against the flushing state.
- With `range-lock` enabled, the request's byte range overlaps an
  already-locked range.
- With `wal` enabled, the write needs to be routed through the WAL
  stage (`"op resubmit to exec write wal"` /
  `"op resubmit to exec write zero wal"`).

Each of these requeues is invisible to the caller and does not show
up in the response `Result`.

### What `fh_*` callers actually observe

Every `HyperFileHandler::fh_*` method ends with

```rust
rx.await.expect("task channel closed")
```

on the oneshot/mpsc receiver returned by the handler. The
consequences are:

- **Normal completion**: returns whatever the handler sent
  (`Ok` / `Err(e)`). Application error handling is just `match
  res { ... }` like any other async API.
- **Handler task died**: the handler's async task crashes (for
  example because of a `panic!` in code running on its behalf).
  When the task frame unwinds, the response `Sender` held in the
  dropped `FileResp` enum variant is dropped too, which closes the
  channel. The caller's `rx.await` resolves with a recv error, and
  the `fh_*` / `HyperFileTokio` wrappers translate that into
  `Err(ErrorKind::BrokenPipe, "reactor handler task died")`.
  Observed triggers:
    - A `panic!` inside a `StagingIntercept` hook (validated by
      `tests/integration_reactor_s3_failure.rs::reactor_handler_panic_via_interceptor_returns_broken_pipe`).
    - `panic!("wal flush recovery failed ...")` at
      `src/file/handler.rs:847` — same pattern, not separately
      validated in tests yet.

Historical note: before the `FileResp` union was rewritten to an
`enum`, the `ManuallyDrop<Sender>` inside the union leaked on
unwind, so the caller's `rx.await` never resolved and the caller
hung indefinitely. The enum form makes Drop propagate correctly.

### Interaction with `LocalSpawner`

`LocalSpawner::new_current` spawns a dedicated OS thread running a
current-thread tokio runtime. The thread lives until:

1. All `UnboundedSender` clones tied to the spawner are dropped
   (that is, every `HyperFileHandler` that exists has been
   dropped), and
2. All tasks spawned on that runtime have completed.

Practical consequence: **dropping the `LocalSpawner` while still
holding a `HyperFileHandler` does not kill the worker thread**.
Each `HyperFileHandler` owns a sender clone that keeps the channel,
and therefore the thread, alive. Subsequent `fh_*` calls still
work. Validated by
`tests/integration_reactor_s3_failure.rs::reactor_spawner_dropped_handle_still_works`.

There is no explicit `shutdown` / `join_thread` API. For a test or
a short-lived tool, dropping the last `HyperFileHandler` followed
by dropping the `LocalSpawner` lets the thread exit on its own.

One subtle point: `Hyper::Drop` calls `rt.shutdown_background()` on
its inner tokio runtime; that runs inside the spawner's thread. It
is not valid to drop a `Hyper` from a context that does not already
own a tokio runtime (for example, plain synchronous code), and the
reactor is what makes it work. This is also why `Hyper` objects
flow one-way into `fh_from_hyper` — the handler takes ownership so
the drop happens inside the runtime it was spawned on.

### Summary table

| Event | Observed by caller |
| --- | --- |
| Operation succeeds | `Ok(...)` from the `fh_*` call |
| Operation fails with a domain error (I/O, permission, OCC conflict) | `Err(e)` from the `fh_*` call; `e.kind()` meaningful |
| Request internally requeued (`ResourceBusy` inside handler) | Invisible; caller still awaits the final result |
| Handler task panics (e.g. panic in an interceptor) | `Err(ErrorKind::BrokenPipe, "reactor handler task died")` from the `fh_*` call |
| Range lock held elsewhere (`range-lock` on) | Invisible; caller awaits until the range is released |
| WAL write required (`wal` on) | Invisible; caller awaits until WAL stage completes |

### Practical guidance

- Treat every `fh_*` / `HyperFileTokio` call as a fallible async
  operation: match on its `Result`. Both domain errors and handler
  death surface as `Err(_)`, so a single `?` / `match` covers both.
- `ErrorKind::BrokenPipe` on a `fh_*` call means the reactor handler
  is gone and will not come back. Retrying on the same handler will
  fail the same way. Construct a new `HyperFileHandler` (which
  spawns a fresh handler task) to recover.
- Do not rely on `ErrorKind::ResourceBusy` surfacing from `fh_*`.
  The reactor swallows it for its own retry; the direct API can
  still surface it, but with different provenance (real in-progress
  flush, not an internal requeue).

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
- `reactor_fail_path_domain_error_returns_err` — reactor caller sees
  `Err(_)` (not panic) when an interceptor returns an I/O error;
  handler survives for subsequent calls.
- `reactor_handler_panic_via_interceptor_returns_broken_pipe` — reactor
  caller's `fh_*` returns `Err(ErrorKind::BrokenPipe)` when the
  handler task panics; enforced with a safety-net `tokio::time::timeout`.
- `reactor_spawner_dropped_handle_still_works` — dropping the
  `LocalSpawner` while still holding a `HyperFileHandler` leaves
  the worker thread alive, because the handler owns a sender clone.
