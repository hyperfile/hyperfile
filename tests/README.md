# Testing Guide

This directory holds Hyperfile's test suite. Tests are organized by what
they exercise and what dependencies they require.

## Layout

```
tests/
├── README.md                                ← this file
├── common/
│   └── mod.rs                               ← direct-API fixtures, interceptors
├── common_reactor/
│   └── mod.rs                               ← reactor spawner helper
├── integration_s3_smoke.rs                  ← happy-path create/write/read/truncate
├── integration_s3_rollback.rs               ← rollback (exposure + correctness)
├── integration_s3_contract.rs               ← flush contract (invariant) tests
├── integration_s3_concurrent.rs             ← multi-instance concurrency
├── integration_s3_segment_open.rs           ← segment summary read (`open`)
├── integration_s3_block_api.rs              ← block borrow API (`fs_block*`)
├── integration_s3_local_disk_cache.rs       ← local-disk data cache tier
├── integration_s3_read_timing.rs            ← read-side counters (`read_timing`)
├── integration_reactor_s3_smoke.rs          ← reactor smoke (default features)
├── integration_reactor_s3_block_api.rs      ← reactor block access (`fh_with_block*`)
├── integration_reactor_s3_range_lock.rs     ← reactor + range-lock
├── integration_reactor_s3_wal.rs            ← reactor + wal
└── integration_reactor_s3_all_features.rs   ← reactor + wal + range-lock
```

`common/mod.rs` and `common_reactor/mod.rs` are not themselves test
binaries. They are consumed by each `integration_*_s3_*.rs` file via
`mod common; use common::*;` (and optionally `mod common_reactor; use
common_reactor::*;`). Cargo compiles each integration_*_s3_*.rs as its
own test binary; the common modules are shared source among them.

## Test categories

| Category | Location | Dependencies | Default run |
| --- | --- | --- | --- |
| Unit tests | inline in `src/**/*.rs` (`#[cfg(test)]` modules) | None | Yes |
| S3 integration tests | `tests/integration_s3_*.rs` | Real S3 bucket + AWS credentials | No (opt-in via `--ignored`) |

All unit tests must pass on every build. Integration tests are marked
`#[ignore]` so they don't run under plain `cargo test`; they must be
explicitly invoked.

## Running unit tests

```bash
cargo test --release --lib
```

No credentials, no network. Should complete in well under a second.

As of the latest commit there are approximately 195 unit tests across
modules like `meta_format`, `ondisk`, `segment`, `config`, `buffer`,
`inode`, `data_cache::mem_cache`, `file::flags`, `file::mode`,
`file::handler`, `file::lock` (under the `range-lock` feature), etc.

## Running S3 integration tests

Integration tests run against a real S3 bucket (or S3 Express One Zone
directory bucket). They exercise end-to-end scenarios that cannot be
meaningfully mocked: optimistic concurrency, conditional writes,
partial-flush recovery, and MPU semantics.

### Prerequisites

- A writable S3 bucket in some region. Tests create / delete objects
  under a unique `hyperfile-test/<YYYYMMDD>/<ULID>/` prefix and best-
  effort clean up at the end of each test.
- AWS credentials resolvable via the AWS SDK default chain:
  environment variables, `~/.aws/credentials`, EC2 IMDS, ECS task
  role, or SSO session.

### Configuration (environment variables)

Both variables are **required** — the integration suites panic with a
clear message if either is unset, because picking a default bucket
in library code would either silently hit someone else's account or
hard-code a specific environment's identifier into the repository.

| Variable | Notes |
| --- | --- |
| `HYPERFILE_TEST_BUCKET` | Name of the S3 bucket to write to. Must be an S3 general-purpose bucket or S3 Express One Zone directory bucket you own. |
| `HYPERFILE_TEST_REGION` | AWS region of the bucket. |

The tests use the AWS SDK's default credential provider chain
(env vars → `~/.aws/credentials` → IMDS → SSO). The identity in
use must have `s3:*` on the configured bucket.

### Running all integration suites

```bash
HYPERFILE_TEST_BUCKET=<your-bucket> \
HYPERFILE_TEST_REGION=<your-region> \
cargo test --release --tests -- --ignored --test-threads=1
```

`--test-threads=1` is important: the tests share a bucket prefix and
concurrent execution can cause listing / cleanup interference. Several
tests also deliberately race themselves; adding extra parallelism
obscures their state.

### Running one suite

Each file is its own `--test` target:

```bash
HYPERFILE_TEST_BUCKET=<your-bucket> \
HYPERFILE_TEST_REGION=<your-region> \
cargo test --release --test integration_s3_smoke -- --ignored --test-threads=1
```

Substitute `integration_s3_rollback`, `integration_s3_contract`, or
`integration_s3_concurrent` for the other suites.

### Running one test

Pass the test name (or a substring) before the `--`:

```bash
HYPERFILE_TEST_BUCKET=<your-bucket> \
HYPERFILE_TEST_REGION=<your-region> \
cargo test --release --test integration_s3_concurrent \
    concurrent_two_writers_fail_fast_policy \
    -- --ignored --test-threads=1 --nocapture
```

`--nocapture` is handy for diagnosing failures — it lets `println!`
output from the test show up even when the assertion passes.

### Compiling only (no run)

```bash
cargo test --release --tests --no-run
```

Useful in CI or when iterating on a test without paying for S3 calls.

### Enabling debug logs

Use `RUST_LOG` to see Hyperfile's internal debug traces — especially
useful when a concurrency / rollback test exposes a subtle ordering
issue.

```bash
RUST_LOG=hyperfile=debug \
HYPERFILE_TEST_BUCKET=... HYPERFILE_TEST_REGION=... \
cargo test --release --test integration_s3_concurrent <name> \
    -- --ignored --test-threads=1 --nocapture
```

## Integration test suites

### `integration_s3_smoke`

Happy-path end-to-end round trips. Create, write, flush, release,
reopen, read, truncate (extend and shrink). Runs in ~0.5 s.

### `integration_s3_rollback`

Two groups:

1. **Rollback exposure** (`rollback_exposure_*`): always-fail
   interceptor; verifies that when flush permanently fails, the
   persisted state is unchanged and reopen recovers cleanly.
2. **Rollback correctness** (`rollback_correctness_*`): fail-once
   interceptor; verifies that a single transient flush failure is not
   silently committed by subsequent retries or release-time flushes.
   Covers every mutation API (write / truncate extend / truncate
   shrink / write_zero / write_aligned_batch / write_batch).

Runs in ~45 s (multiple FlushOnce retry cycles that each take a few
seconds of AWS timeouts).

### `integration_s3_contract`

Uses `#[doc(hidden)]` getters on `Hyper` (dirty_block_count,
is_attr_dirty, is_bmap_dirty, cno tracking) to assert that flush
invariants hold after success, after rollback, after segment-upload
failure, and along the retry path (retries `ResourceBusy`, does not
retry `ErrorKind::Other`).

Runs in ~7 s.

### `integration_s3_concurrent`

Multi-instance concurrency:

- `concurrent_two_writers_optimistic_cc`: default
  `RetryLastWriterWins` policy, two writers race on the same URI.
- `concurrent_read_while_writer_flushes`: reader observes
  self-consistent checkpoints while writer churns.
- `concurrent_writer_a_fails_writer_b_succeeds`: writer A injected
  failure, writer B succeeds.
- `concurrent_two_writers_fail_fast_policy`: `FlushConflictPolicy::
  FailFast`; exactly one writer wins, the other returns
  `ErrorKind::AlreadyExists`.

See `docs/concurrency.md` for the full behavior model.

Runs in ~27 s.

### `integration_s3_segment_open`

Covers `SegmentReadWrite::open`, which reads a segment's summary. It
speculatively fetches `SEGMENT_HEADER_FETCH_SIZE` (512 KiB) and tops up
only if `s_bytes` says the summary is larger, so its behavior depends on
how the object's real size compares to that constant. The suite writes a
single flush of 4 KiB, 8 KiB, 512 KiB, 1 MiB and 16 MiB and opens the
resulting segment in each case.

`open` has no caller inside hyperfile — reads go through the block map
and the block loaders, which issue exact-length range reads — so no other
suite exercises it. `hyperfile-cleaner` is the consumer.

**Run this one under debug assertions as well as release.** It previously
regressed on an integer underflow that wrapped harmlessly in release and
only panicked with `debug_assertions` on:

```bash
cargo test --test integration_s3_segment_open -- --ignored --test-threads=1
```

Runs in ~1 s.

### `integration_s3_block_api`

Covers `fs_block` / `fs_block_mut` / `fs_block_state`, which let a
caller borrow a cached data block instead of copying it through
`fs_read` / `fs_write`. Asserts the semantics a block-storage
consumer relies on: the borrow is the cache's own buffer, a hole is
distinguishable from a block of zeros (which `fs_read` cannot do),
an in-place modification is persisted by the next flush with no
write-back call, repeated borrows inside one flush window produce a
single checkpoint, `i_size` is untouched, and access mode is
enforced in both directions.

Runs the same round trip under all three data cache configurations,
because the borrow API does materially different work in each:

- default in-memory cache;
- local-disk cache, where clean blocks are views into a backing file,
  `Cache::get` mlocks the block it hands out, and eviction punches a
  hole;
- data cache disabled (`data_cache_blocks = 0`, which `O_DIRECT`
  without `wal` forces), where there is nothing to borrow and
  `fs_block` falls back to owning the block it loaded.

**Run this one under debug assertions as well as release.** Both
cache tiers carry `debug_assert!`s about block dirty state, and the
local-disk tier asserts outright that a clean block handed out by
`get` was not already locked — which is what makes the borrow
guards' `Drop` load-bearing.

Note the local-disk case creates a block in a hole *inside* `i_size`.
That tier addresses cached blocks as offsets into a mapping sized
from `i_size` and cannot grow the mapping in place, so a block above
EOF is not representable there; that case runs on the in-memory
cache instead.

Runs in ~2 s.

### `integration_s3_local_disk_cache`

The only suite that selects `HyperFileDataCacheConfig::LocalDisk` for
the **data** cache. That tier keeps blocks in a memory-mapped file;
because nothing exercised it, four separate defects lived there
undetected, including memory corruption on an extending write and a
process abort on release. See the suite's module docs.

Covers: creating an empty file on that tier, releasing it repeatedly
without aborting, writing far past EOF, a 4 TiB sparse address space,
slot recycling under constant eviction, truncate releasing slots
without leaving stale bytes, and exhausting the slot pool so blocks
fall back to heap allocations.

Runs in ~2 s.

### `integration_s3_read_timing`

Checks that the read-side counters count what they claim to. The read
path's cost is dominated by object-store round trips, and wall-clock
timing cannot tell one request for a coalesced range from many, nor a
cache hit from a fetch — so these are the assertions that keep the
counters trustworthy.

Covers: a read spanning 64 contiguous blocks in one segment costing a
single request; a byte read not populating the data cache, so a second
read of the same block fetches again; a block borrow populating it, so
a second borrow is a hit and a later byte read is too; index requests
counted separately from data requests, including that the batched
index fetch reports both of the requests it makes; holes and unflushed
writes costing nothing; and `read_timing_reset` zeroing every counter.

Runs in ~2 s.

### `integration_reactor_s3_smoke`

Reactor-mode smoke (`HyperFileHandler` and `HyperFileTokio`). Same
shape as `integration_s3_smoke` but each test uses a `LocalSpawner`
and goes through the request/response channel. Covers read, write,
truncate extend/shrink, flush, getattr, setattr, as well as the
tokio `AsyncRead`/`AsyncWrite`/`AsyncSeek` surface.

Feature requirement: `reactor` (default).

Runs in ~1 s.

### `integration_reactor_s3_block_api`

Closure-scoped block access on the reactor surface,
`fh_with_block` / `fh_with_block_mut`. The reactor cannot hand out
borrow guards — see [docs/block-api.md](../docs/block-api.md#why-the-reactor-surface-takes-a-closure)
— so the caller's action is sent to the block instead.

Covers values moving in and out of the closure, the closure not
running for a hole, in-place modification persisting without a
write-back call, `create`, repeated edits collapsing to one
checkpoint, access-mode errors surfacing through the channel without
killing the reactor task, and a byte-for-byte equivalence check
between the closure form and the direct guard form.

Runs in ~1 s.

### `integration_reactor_s3_range_lock`

Multi-task concurrency on a **single** file handle (via
`HyperFileHandler::clone`). Validates that under `range-lock` the
reactor serializes overlapping byte ranges and lets disjoint ranges
proceed in parallel, and that flush waits for in-flight writes.

Feature requirement: `reactor` + `range-lock`.

Run with:
```bash
cargo test --features range-lock --test integration_reactor_s3_range_lock \
    -- --ignored --test-threads=1
```

### `integration_reactor_s3_wal`

Smokes the WAL path: create + write + flush + reopen round trip with
`HyperFileWalConfig` pointing at a sibling `<uri>/wal/` prefix. Also
verifies that reopen after a second flush does not observe stale WAL
state.

Feature requirement: `reactor` + `wal`.

**Note**: tests use `#[tokio::test(flavor = "multi_thread")]` because
`S3Wal::from_uri` calls `tokio::task::block_in_place`, which is not
valid on a current-thread runtime.

Run with:
```bash
cargo test --features wal --test integration_reactor_s3_wal \
    -- --ignored --test-threads=1
```

### `integration_reactor_s3_all_features`

Maximum-feature smoke: reactor + wal + range-lock +
concurrent-segment-build enabled together. Covers the full feature
matrix in a single binary.

Feature requirement: `reactor` + `wal` + `range-lock` +
`concurrent-segment-build`.

Run with:
```bash
cargo test --features "wal range-lock concurrent-segment-build" \
    --test integration_reactor_s3_all_features \
    -- --ignored --test-threads=1
```

## Feature matrix

The reactor suites exercise different feature combinations. When
iterating on a feature flag, run only the relevant suite:

| Feature combination | Suite |
| --- | --- |
| `reactor` only (default) | `integration_reactor_s3_smoke` |
| `reactor` + `range-lock` | `integration_reactor_s3_range_lock` |
| `reactor` + `wal` | `integration_reactor_s3_wal` |
| `reactor` + `wal` + `range-lock` + `concurrent-segment-build` | `integration_reactor_s3_all_features` |

To validate a release candidate, run every combination once:

```bash
# default
cargo test --test integration_reactor_s3_smoke -- --ignored --test-threads=1

# + range-lock
cargo test --features range-lock --test integration_reactor_s3_range_lock \
    -- --ignored --test-threads=1

# + wal
cargo test --features wal --test integration_reactor_s3_wal \
    -- --ignored --test-threads=1

# + all features
cargo test --features "wal range-lock concurrent-segment-build" \
    --test integration_reactor_s3_all_features \
    -- --ignored --test-threads=1
```

Plus all the direct-API suites (`integration_s3_*`).

## Adding a new interceptor

Shared interceptors live in `tests/common/mod.rs`. To inject a fault
not yet covered, implement `StagingIntercept<S3Staging>` with overrides
only for the hook you need. All methods have default no-op
implementations, so a minimal interceptor is a few lines:

```rust
#[derive(Clone)]
pub struct FailReadInode {
    calls: Arc<AtomicUsize>,
}

impl StagingIntercept<S3Staging> for FailReadInode {
    fn after_flush_inode(&self, _: &S3Staging, _: &[u8], _: FlushInodeFlag)
        -> Pin<Box<dyn Future<Output = Result<()>> + '_ + Send>>
    { Box::pin(async { Ok(()) }) }

    fn after_remove_inode(&self, _: &S3Staging)
        -> Pin<Box<dyn Future<Output = Result<()>> + '_ + Send>>
    { Box::pin(async { Ok(()) }) }

    // override the specific hook you want
}
```

Available hook points (see `src/staging/mod.rs`):

- `before_flush_inode` — can return `Err` to make the PUT fail before
  it hits S3.
- `after_flush_inode` — post-success hook, can't cause the flush to
  fail but can observe or trigger side effects.
- `after_remove_inode`
- `before_segment_done` — can return `Err` to make a segment upload
  fail.

Put the new interceptor in `tests/common/mod.rs` so every suite can
use it. Install in a test with `Hyper::with_staging_interceptor(i.clone())`
after opening.

## Adding a new integration test

1. Pick the right suite or add a new `integration_s3_<name>.rs` file
   (and update this README's Layout table).
2. Top of file: `mod common; use common::*;` plus whatever hyperfile
   imports you need.
3. Use `#[tokio::test]` + `#[ignore]` so the test is excluded from the
   default `cargo test` run.
4. Start with a `TestFile::new(&client).await` and call
   `tf.cleanup(&client).await` at the very end (do NOT rely on Drop —
   see the comment on `TestFile` for why).
5. For tests that need a specific runtime config (e.g. a
   `FlushConflictPolicy`), construct a `HyperFileRuntimeConfig` and
   open via `Hyper::fs_open_opt`.
6. Always call `let _ = hyper.fs_release().await` before dropping the
   `Hyper`, even on the error path — `release` runs internal flush
   cleanup.
7. If the test is concurrent, use `tokio::join!` over plain futures,
   not `tokio::spawn` — `Hyper` is `Send` but not `Sync`, so its
   futures can't be scheduled across tokio worker threads.
8. Prefer printing diagnostic info with `println!` and `--nocapture`
   over stuffing detail into assertion messages.

## Adding a new suite

If a new category of tests doesn't fit into the existing four,
create `tests/integration_s3_<category>.rs`:

1. Start with the same header pattern as existing suites (module-level
   doc comment explaining the group, `mod common; use common::*;`,
   imports).
2. Add a row to the "Layout" table and a section under "Integration
   test suites" describing what it covers.
3. A new test binary is automatically picked up by `cargo test --tests`;
   no Cargo.toml changes required.

## Cleanup and orphaned objects

`TestFile::cleanup` uses `Hyper::fs_unlink`, which issues a prefix list
plus batch delete. If a test crashes between `TestFile::new` and
`cleanup`, the created objects remain in the bucket. They are harmless
(each run uses a unique ULID-based prefix) but can accumulate. Nuke
them with:

```bash
aws s3 rm s3://$HYPERFILE_TEST_BUCKET/hyperfile-test/ --recursive \
    --region $HYPERFILE_TEST_REGION
```

## Related reading

- [`docs/concurrency.md`](../docs/concurrency.md) — how
  `FlushConflictPolicy` and S3 OCC interact; reference for the
  concurrency suite.
- [`docs/posix.md`](../docs/posix.md) — POSIX semantics, sync mode
  behavior.
