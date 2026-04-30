# Testing Guide

This directory holds Hyperfile's test suite. Tests are organized by what
they exercise and what dependencies they require.

## Test categories

| Category | Location | Dependencies | Default run |
| --- | --- | --- | --- |
| Unit tests | inline in `src/**/*.rs` (under `#[cfg(test)]` modules) | None | Yes |
| S3 integration tests | `tests/integration_s3.rs` | Real S3 bucket + AWS credentials | No (opt-in via `--ignored`) |

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
`file::handler`, etc.

## Running S3 integration tests

Integration tests run against a real S3 bucket (or S3 Express One Zone
directory bucket). They exercise end-to-end scenarios that cannot be
meaningfully mocked, such as optimistic concurrency, conditional writes,
partial-flush recovery, and MPU semantics.

### Prerequisites

- A writable S3 bucket in some region. Tests create / delete objects
  under a unique `hyperfile-test/<YYYYMMDD>/<ULID>/` prefix and best-
  effort clean up at the end of each test.
- AWS credentials resolvable via the AWS SDK default chain:
  environment variables, `~/.aws/credentials`, EC2 IMDS, ECS task
  role, or SSO session.

### Configuration (environment variables)

| Variable | Default | Notes |
| --- | --- | --- |
| `HYPERFILE_TEST_BUCKET` | `<your-bucket>` | Name of the bucket to write to. |
| `HYPERFILE_TEST_REGION` | `<your-region>` | AWS region for the client. |

### Running the whole suite

```bash
HYPERFILE_TEST_BUCKET=<your-bucket> \
HYPERFILE_TEST_REGION=<your-region> \
cargo test --release --test integration_s3 \
    -- --ignored --test-threads=1
```

`--test-threads=1` is important: the tests share a bucket prefix and
concurrent execution can cause listing / cleanup interference. Plus
several tests deliberately race themselves, so running multiple tests
in parallel adds noise that is hard to diagnose.

### Running a single test

Pass the test name (or a substring) before the `--`:

```bash
HYPERFILE_TEST_BUCKET=<your-bucket> \
HYPERFILE_TEST_REGION=<your-region> \
cargo test --release --test integration_s3 \
    concurrent_two_writers_fail_fast_policy \
    -- --ignored --test-threads=1 --nocapture
```

`--nocapture` is handy for diagnosing failures — it lets `println!`
output from the test show up even when the assertion passes.

### Compiling only (no run)

```bash
cargo test --release --test integration_s3 --no-run
```

Useful in CI or when iterating on a test without paying for S3 calls.

### Enabling debug logs

Use `RUST_LOG` to see Hyperfile's internal debug traces — this is
especially useful when a concurrency / rollback test exposes a subtle
ordering issue.

```bash
RUST_LOG=hyperfile=debug \
HYPERFILE_TEST_BUCKET=... HYPERFILE_TEST_REGION=... \
cargo test --release --test integration_s3 <name> \
    -- --ignored --test-threads=1 --nocapture
```

## Integration test organization (`tests/integration_s3.rs`)

The file is split into sections, each focused on one behavior area.
When adding a new group, prefer adding to an existing section if it
fits; otherwise create a new section with a banner comment that
explains what invariants it covers.

Current sections:

1. **Test fixture and setup** — `TestFile` RAII struct, `make_client`,
   env-driven bucket/region config.
2. **Fault-injection helpers** — `StagingIntercept` implementations
   like `FailOnFlushInode`, `AlwaysFailFlushInode`,
   `AlwaysFailSegmentDone`, `ResourceBusyOnceFlushInode`,
   `FailOtherNoRetry`. See "Adding a new interceptor" below.
3. **Smoke tests** — happy path end-to-end round trips.
4. **Rollback exposure tests** — flush always fails, verifies the
   pre-flush persisted state is preserved.
5. **Rollback correctness tests** — flush fails once (transient);
   verifies that retries or release-time flushes do not silently
   commit.
6. **Contract (invariant) tests** — use `#[doc(hidden)]` getters on
   `Hyper` (e.g. `dirty_block_count`, `is_bmap_dirty`) to assert the
   internal state is consistent after flush success / failure /
   rollback.
7. **Concurrent-writer / concurrent-reader tests (A group)** — two
   `Hyper` instances on the same URI, exercising S3 OCC semantics
   under the default `RetryLastWriterWins` policy. One test covers the
   `FailFast` policy.

## Adding a new interceptor

To inject a fault not yet covered, implement
`StagingIntercept<S3Staging>` with overrides only for the hook you
need. All methods have default no-op implementations, so a minimal
interceptor is a few lines:

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

Install with `Hyper::with_staging_interceptor(interceptor.clone())`
after opening, before performing the operation.

## Adding a new integration test

1. Pick the right section or create a new one.
2. Use `#[tokio::test]` + `#[ignore]` so the test is excluded from the
   default `cargo test` run.
3. Start with a `TestFile::new(&client).await` and call
   `tf.cleanup(&client).await` at the very end (do NOT rely on Drop —
   see the comment on `TestFile` for why).
4. For tests that need a specific runtime config (e.g. to pick a
   `FlushConflictPolicy`), construct a `HyperFileRuntimeConfig` and
   open via `Hyper::fs_open_opt`.
5. Always call `let _ = hyper.fs_release().await` before dropping the
   `Hyper`, even on the error path — `release` runs internal flush
   cleanup.
6. If the test is concurrent, use `tokio::join!` over plain futures,
   not `tokio::spawn` — `Hyper` is `Send` but not `Sync`, so its
   futures can't be scheduled across tokio worker threads.
7. Prefer printing diagnostic info with `println!` and `--nocapture`
   over stuffing detail into assertion messages.

## Cleanup and orphaned objects

`TestFile::cleanup` uses `Hyper::fs_unlink`, which issues a prefix list
plus batch delete. If a test crashes between `TestFile::new` and
`cleanup`, the created objects will remain in the bucket. They are
harmless (each run uses a unique ULID-based prefix) but if they
accumulate you can nuke them with:

```bash
aws s3 rm s3://$HYPERFILE_TEST_BUCKET/hyperfile-test/ --recursive \
    --region $HYPERFILE_TEST_REGION
```

## Related reading

- [`docs/concurrency.md`](../docs/concurrency.md) — how
  `FlushConflictPolicy` and S3 OCC interact; reference for the
  concurrency tests.
- [`docs/posix.md`](../docs/posix.md) — POSIX semantics, sync mode
  behavior.
