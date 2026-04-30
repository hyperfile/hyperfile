//! Integration tests against a real S3 bucket.
//!
//! These tests are marked `#[ignore]` by default. To run them:
//!
//! ```bash
//! HYPERFILE_TEST_BUCKET=<your-bucket> \
//! HYPERFILE_TEST_REGION=<your-region> \
//! cargo test --test integration_s3 -- --ignored --test-threads=1
//! ```
//!
//! Each test generates a unique ULID-based key prefix and the `TestFile`
//! fixture cleans up all objects under that prefix on drop.
//!
//! The tests are organized into three groups:
//!   1. Smoke tests — verify the happy path works end-to-end.
//!   2. Rollback exposure tests — document current flush-failure behavior.
//!   3. (later) Regression tests covering segment checksum, etc.

use std::io::ErrorKind;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use aws_sdk_s3::Client;
use hyperfile::file::hyper::Hyper;
use hyperfile::file::flags::FileFlags;
use hyperfile::file::mode::FileMode;
use hyperfile::inode::FlushInodeFlag;
use hyperfile::staging::StagingIntercept;
use hyperfile::staging::s3::S3Staging;

const DEFAULT_BUCKET: &str = "<your-bucket>";
const DEFAULT_REGION: &str = "<your-region>";

fn test_bucket() -> String {
    std::env::var("HYPERFILE_TEST_BUCKET").unwrap_or_else(|_| DEFAULT_BUCKET.to_string())
}

fn test_region() -> String {
    std::env::var("HYPERFILE_TEST_REGION").unwrap_or_else(|_| DEFAULT_REGION.to_string())
}

/// Build an S3 client configured for the test region.
async fn make_client() -> Client {
    let region = test_region();
    let config = aws_config::from_env()
        .region(aws_config::Region::new(region))
        .load()
        .await;
    Client::new(&config)
}

/// A test fixture that owns a unique S3 URI.
///
/// Cleanup is the responsibility of each test (call `Hyper::fs_unlink` before
/// returning). We intentionally avoid Drop-based cleanup because:
///   1. We can't `await` in Drop, so we'd need to block on a new runtime —
///      which deadlocks in some cases when the AWS SDK has background tasks.
///   2. The `Hyper` object itself owns a tokio runtime that shuts down in
///      its own Drop; stacking more async cleanup on top makes teardown
///      unpredictable.
///
/// Tests should structure themselves as:
/// ```no_run
/// let tf = TestFile::new(&client).await;
/// // ... use tf.uri() ...
/// tf.cleanup(&client).await;  // explicit, at the end of the test
/// ```
pub struct TestFile {
    uri: String,
}

impl TestFile {
    pub async fn new(_client: &Client) -> Self {
        let prefix = format!(
            "hyperfile-test/{}/{}",
            chrono::Utc::now().format("%Y%m%d"),
            ulid::Ulid::new()
        );
        let uri = format!("s3://{}/{}", test_bucket(), prefix);
        Self { uri }
    }

    pub fn uri(&self) -> &str {
        &self.uri
    }

    /// Best-effort cleanup. Safe to call even if the file doesn't exist yet.
    pub async fn cleanup(&self, client: &Client) {
        match Hyper::fs_unlink(client, &self.uri).await {
            Ok(()) => {}
            Err(e) if e.kind() == ErrorKind::NotFound => {}
            Err(e) => eprintln!("TestFile cleanup failed for {}: {}", self.uri, e),
        }
    }
}

// --------------------------------------------------------------------
// Smoke tests
// --------------------------------------------------------------------

/// Verify basic create → write → flush → release → reopen → read round-trip.
#[tokio::test]
#[ignore]
async fn smoke_write_read_round_trip() {
    let _ = env_logger::try_init();
    let client = make_client().await;
    let tf = TestFile::new(&client).await;

    // Create and write some bytes.
    let payload: Vec<u8> = (0u8..=127u8).collect();
    {
        let mut hyper = Hyper::fs_open_or_create_with_default_opt(
            &client,
            tf.uri(),
            FileFlags::rdwr(),
            FileMode::default_file(),
        )
        .await
        .expect("create failed");
        let n = hyper.fs_write(0, &payload).await.expect("write failed");
        assert_eq!(n, payload.len());
        let _last_cno = hyper.fs_release().await.expect("release failed");
    }

    // Reopen and read back.
    {
        let mut hyper = Hyper::fs_open(&client, tf.uri(), FileFlags::rdonly())
            .await
            .expect("open failed");
        let stat = hyper.fs_getattr().expect("getattr failed");
        assert_eq!(stat.st_size as usize, payload.len());

        let mut buf = vec![0u8; payload.len()];
        let n = hyper.fs_read(0, &mut buf).await.expect("read failed");
        assert_eq!(n, payload.len());
        assert_eq!(buf, payload);
        let _ = hyper.fs_release().await;
    }

    tf.cleanup(&client).await;
}

/// Truncate to extend an existing file — verify new size is reflected and
/// previously written data is preserved, newly extended range reads as zeros.
#[tokio::test]
#[ignore]
async fn smoke_truncate_extend() {
    let _ = env_logger::try_init();
    let client = make_client().await;
    let tf = TestFile::new(&client).await;

    // Seed the file with 1KiB of data.
    let payload: Vec<u8> = (0..1024u16).map(|v| (v & 0xFF) as u8).collect();
    {
        let mut hyper = Hyper::fs_open_or_create_with_default_opt(
            &client,
            tf.uri(),
            FileFlags::rdwr(),
            FileMode::default_file(),
        )
        .await
        .expect("create failed");
        let n = hyper.fs_write(0, &payload).await.expect("write failed");
        assert_eq!(n, payload.len());
        let _ = hyper.fs_release().await.expect("release failed");
    }

    // Extend to 8KiB via truncate.
    let new_size = 8 * 1024;
    {
        let mut hyper = Hyper::fs_open(&client, tf.uri(), FileFlags::rdwr())
            .await
            .expect("open failed");
        hyper.fs_truncate(new_size).await.expect("truncate extend failed");
        let _ = hyper.fs_release().await.expect("release failed");
    }

    // Reopen read-only, verify size and content.
    {
        let mut hyper = Hyper::fs_open(&client, tf.uri(), FileFlags::rdonly())
            .await
            .expect("open failed");
        let stat = hyper.fs_getattr().expect("getattr");
        assert_eq!(stat.st_size as usize, new_size);

        let mut buf = vec![0u8; new_size];
        let n = hyper.fs_read(0, &mut buf).await.expect("read");
        assert_eq!(n, new_size);

        // First 1KiB is the original payload.
        assert_eq!(&buf[..payload.len()], &payload[..]);
        // The rest should be zeros (sparse extension).
        assert!(buf[payload.len()..].iter().all(|&b| b == 0));
        let _ = hyper.fs_release().await;
    }

    tf.cleanup(&client).await;
}

/// Truncate to shrink an existing file — verify new size, content preserved
/// up to the new size, and reading beyond new size fails (or returns 0 bytes).
#[tokio::test]
#[ignore]
async fn smoke_truncate_shrink() {
    let _ = env_logger::try_init();
    let client = make_client().await;
    let tf = TestFile::new(&client).await;

    // Seed the file with 16KiB (4 blocks of 4KiB).
    let payload: Vec<u8> = (0..16 * 1024).map(|i| (i & 0xFF) as u8).collect();
    {
        let mut hyper = Hyper::fs_open_or_create_with_default_opt(
            &client,
            tf.uri(),
            FileFlags::rdwr(),
            FileMode::default_file(),
        )
        .await
        .expect("create failed");
        let n = hyper.fs_write(0, &payload).await.expect("write failed");
        assert_eq!(n, payload.len());
        let _ = hyper.fs_release().await.expect("release");
    }

    // Shrink to 5000 bytes (mid-block).
    let new_size = 5000;
    {
        let mut hyper = Hyper::fs_open(&client, tf.uri(), FileFlags::rdwr())
            .await
            .expect("open failed");
        hyper.fs_truncate(new_size).await.expect("truncate shrink failed");
        let _ = hyper.fs_release().await.expect("release");
    }

    // Verify.
    {
        let mut hyper = Hyper::fs_open(&client, tf.uri(), FileFlags::rdonly())
            .await
            .expect("open failed");
        let stat = hyper.fs_getattr().expect("getattr");
        assert_eq!(stat.st_size as usize, new_size);

        let mut buf = vec![0u8; new_size];
        let n = hyper.fs_read(0, &mut buf).await.expect("read");
        assert_eq!(n, new_size);
        assert_eq!(&buf[..], &payload[..new_size]);
        let _ = hyper.fs_release().await;
    }

    tf.cleanup(&client).await;
}

// --------------------------------------------------------------------
// Fault-injection helpers
// --------------------------------------------------------------------

/// Interceptor that causes the N-th call to `before_flush_inode` to fail.
/// Call count is 1-based; `FailOnFlushInode::at(1)` fails the very first call.
/// Subsequent calls after the targeted one succeed.
///
/// This is useful for exposing behavior where the hyperfile flush retry
/// machinery might paper over a single failure.
///
/// The interceptor is `Clone` (uses `Arc`) so callers can hold a reference
/// to inspect the call counter after the operation under test.
#[derive(Clone)]
pub struct FailOnFlushInode {
    target_count: usize,
    calls: Arc<AtomicUsize>,
}

impl FailOnFlushInode {
    pub fn at(target_count: usize) -> Self {
        Self {
            target_count,
            calls: Arc::new(AtomicUsize::new(0)),
        }
    }

    pub fn call_count(&self) -> usize {
        self.calls.load(Ordering::SeqCst)
    }
}

impl StagingIntercept<S3Staging> for FailOnFlushInode {
    fn before_flush_inode(
        &self,
        _staging: &S3Staging,
        _payload: &[u8],
        _flag: FlushInodeFlag,
    ) -> std::pin::Pin<Box<dyn Future<Output = std::io::Result<()>> + '_ + Send>> {
        let current = self.calls.fetch_add(1, Ordering::SeqCst) + 1;
        let should_fail = current == self.target_count;
        Box::pin(async move {
            if should_fail {
                Err(std::io::Error::new(
                    ErrorKind::Other,
                    "FailOnFlushInode: injected failure",
                ))
            } else {
                Ok(())
            }
        })
    }

    fn after_flush_inode(
        &self,
        _staging: &S3Staging,
        _payload: &[u8],
        _flag: FlushInodeFlag,
    ) -> std::pin::Pin<Box<dyn Future<Output = std::io::Result<()>> + '_ + Send>> {
        Box::pin(async { Ok(()) })
    }

    fn after_remove_inode(
        &self,
        _staging: &S3Staging,
    ) -> std::pin::Pin<Box<dyn Future<Output = std::io::Result<()>> + '_ + Send>> {
        Box::pin(async { Ok(()) })
    }
}

/// Interceptor that causes EVERY call to `before_flush_inode` to fail.
/// Useful to test scenarios where flush cannot possibly succeed.
#[derive(Clone)]
pub struct AlwaysFailFlushInode {
    calls: Arc<AtomicUsize>,
}

impl AlwaysFailFlushInode {
    pub fn new() -> Self {
        Self {
            calls: Arc::new(AtomicUsize::new(0)),
        }
    }

    pub fn call_count(&self) -> usize {
        self.calls.load(Ordering::SeqCst)
    }
}

impl StagingIntercept<S3Staging> for AlwaysFailFlushInode {
    fn before_flush_inode(
        &self,
        _staging: &S3Staging,
        _payload: &[u8],
        _flag: FlushInodeFlag,
    ) -> std::pin::Pin<Box<dyn Future<Output = std::io::Result<()>> + '_ + Send>> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Box::pin(async move {
            Err(std::io::Error::new(
                ErrorKind::Other,
                "AlwaysFailFlushInode: injected failure",
            ))
        })
    }

    fn after_flush_inode(
        &self,
        _staging: &S3Staging,
        _payload: &[u8],
        _flag: FlushInodeFlag,
    ) -> std::pin::Pin<Box<dyn Future<Output = std::io::Result<()>> + '_ + Send>> {
        Box::pin(async { Ok(()) })
    }

    fn after_remove_inode(
        &self,
        _staging: &S3Staging,
    ) -> std::pin::Pin<Box<dyn Future<Output = std::io::Result<()>> + '_ + Send>> {
        Box::pin(async { Ok(()) })
    }
}

// --------------------------------------------------------------------
// Rollback exposure tests
// --------------------------------------------------------------------
//
// These tests document the CURRENT behavior when `flush_inode` fails
// during a truncate/write operation. They are documenting (not testing
// for correctness yet) the divergence between in-memory inode and
// persisted inode.
//
// Observations discovered by the initial version of these tests:
//   1. hyperfile has a built-in flush retry (DEFAULT_FLUSH_RETRIES=3,
//      `HyperTrait::flush()` in file/mod.rs). When a single flush_inode
//      fails, the retry loop may succeed on a subsequent attempt,
//      masking the failure from the caller's perspective after release.
//   2. `fs_release()` also performs a flush as part of teardown. Even
//      if the operation under test returns Err, a subsequent release
//      can still successfully persist the (already-mutated) in-memory
//      state, causing the "failed" operation to effectively commit.
//   3. For this reason we use `AlwaysFailFlushInode` below — it ensures
//      every flush_inode attempt fails, so retries and release flushes
//      cannot paper over the injected failure.
//
// When the rollback issue is eventually fixed, these tests will need
// to be updated to reflect the corrected behavior.

/// Truncate-extend on an already-persisted file, with flush_inode ALWAYS failing.
/// Documents that:
///   1. `fs_truncate()` returns Err.
///   2. `fs_release()` also returns Err (cannot clean up).
///   3. The PERSISTED size remains the original size — the inode write
///      really never succeeded on disk.
#[tokio::test]
#[ignore]
async fn rollback_exposure_truncate_extend_flush_fails() {
    let _ = env_logger::try_init();
    let client = make_client().await;
    let tf = TestFile::new(&client).await;

    let original_size = 1024usize;
    let payload: Vec<u8> = (0..original_size).map(|i| (i & 0xFF) as u8).collect();

    // 1. Create a file and flush some data.
    {
        let mut hyper = Hyper::fs_open_or_create_with_default_opt(
            &client,
            tf.uri(),
            FileFlags::rdwr(),
            FileMode::default_file(),
        )
        .await
        .expect("create");
        let _ = hyper.fs_write(0, &payload).await.expect("write");
        let _ = hyper.fs_release().await.expect("release");
    }

    // 2. Reopen with an interceptor that fails ALL flush_inode calls.
    let interceptor = AlwaysFailFlushInode::new();
    let truncate_result = {
        let mut hyper = Hyper::fs_open(&client, tf.uri(), FileFlags::rdwr())
            .await
            .expect("open");
        hyper.with_staging_interceptor(interceptor.clone());

        let result = hyper.fs_truncate(8192).await;
        // Best-effort release — expected to also fail because flush will retry.
        let _release_result = hyper.fs_release().await;
        result
    };

    assert!(
        truncate_result.is_err(),
        "expected truncate to fail due to injected flush_inode failure, got {:?}",
        truncate_result
    );
    assert!(
        interceptor.call_count() >= 1,
        "interceptor was never invoked"
    );
    println!(
        "[truncate_extend_flush_fails] truncate_result={:?} interceptor_calls={}",
        truncate_result,
        interceptor.call_count()
    );

    // 3. Reopen and inspect what was actually persisted.
    let persisted_size = {
        let hyper = Hyper::fs_open(&client, tf.uri(), FileFlags::rdonly())
            .await
            .expect("reopen");
        let stat = hyper.fs_getattr().expect("getattr");
        stat.st_size as usize
    };
    println!(
        "[truncate_extend_flush_fails] persisted_size={}",
        persisted_size
    );

    // Document current behavior: persisted size should still be the original,
    // since all flush attempts failed.
    assert_eq!(
        persisted_size, original_size,
        "persisted size diverged: expected original {}, got {}",
        original_size, persisted_size
    );

    tf.cleanup(&client).await;
}

/// Truncate-shrink with flush ALWAYS failing. Most dangerous case: bmap.truncate()
/// has already discarded mappings for blocks beyond the new size before the
/// flush is attempted. This test exposes whether reopening the file still
/// has access to the original data (bmap is reloaded from the persisted
/// inode, so theoretically the data should still be readable).
#[tokio::test]
#[ignore]
async fn rollback_exposure_truncate_shrink_flush_fails() {
    let _ = env_logger::try_init();
    let client = make_client().await;
    let tf = TestFile::new(&client).await;

    // 1. Seed a 16 KiB file (4 data blocks of 4 KiB).
    let original_size = 16 * 1024usize;
    let payload: Vec<u8> = (0..original_size).map(|i| (i & 0xFF) as u8).collect();
    {
        let mut hyper = Hyper::fs_open_or_create_with_default_opt(
            &client,
            tf.uri(),
            FileFlags::rdwr(),
            FileMode::default_file(),
        )
        .await
        .expect("create");
        let _ = hyper.fs_write(0, &payload).await.expect("write");
        let _ = hyper.fs_release().await.expect("release");
    }

    // 2. Reopen, install always-failing interceptor.
    let interceptor = AlwaysFailFlushInode::new();
    let truncate_result = {
        let mut hyper = Hyper::fs_open(&client, tf.uri(), FileFlags::rdwr())
            .await
            .expect("open");
        hyper.with_staging_interceptor(interceptor.clone());

        // Shrink to 1 block worth of data.
        let result = hyper.fs_truncate(4096).await;
        let _release = hyper.fs_release().await;
        result
    };

    assert!(
        truncate_result.is_err(),
        "expected truncate to fail, got {:?}",
        truncate_result
    );
    println!(
        "[truncate_shrink_flush_fails] truncate_result={:?} interceptor_calls={}",
        truncate_result,
        interceptor.call_count()
    );

    // 3. Reopen and read everything to see what was preserved.
    let (persisted_size, first_block_ok, last_block_ok) = {
        let mut hyper = Hyper::fs_open(&client, tf.uri(), FileFlags::rdonly())
            .await
            .expect("reopen");
        let stat = hyper.fs_getattr().expect("getattr");
        let size = stat.st_size as usize;

        // Read the first block (offset 0, 4KiB) — should match payload[..4096]
        let mut buf0 = vec![0u8; 4096];
        let r0 = hyper.fs_read(0, &mut buf0).await;
        let first_block_ok = r0.is_ok() && buf0 == payload[..4096];

        // If size is still original, try reading the last block too.
        let last_block_ok = if size >= 16 * 1024 {
            let mut buf3 = vec![0u8; 4096];
            let r3 = hyper.fs_read(12 * 1024, &mut buf3).await;
            Some(r3.is_ok() && buf3 == payload[12 * 1024..16 * 1024])
        } else {
            None
        };

        (size, first_block_ok, last_block_ok)
    };

    println!(
        "[truncate_shrink_flush_fails] persisted_size={} first_block_ok={} last_block_ok={:?}",
        persisted_size, first_block_ok, last_block_ok
    );

    // Document current behavior: persisted size should still be 16 KiB
    // because all flush attempts failed.
    assert_eq!(
        persisted_size, original_size,
        "persisted size diverged after failed shrink: expected {}, got {}",
        original_size, persisted_size
    );
    // First block must always be readable — it's unaffected by shrink.
    assert!(first_block_ok, "first block corrupted after failed shrink");
    // Last block readability after a failed shrink: because bmap is reloaded
    // from the persisted inode on reopen, the in-memory bmap truncation
    // should be undone.
    if let Some(ok) = last_block_ok {
        assert!(ok, "last block unreadable after failed shrink — data lost");
    }

    tf.cleanup(&client).await;
}

/// Write followed by ALL flush attempts failing. Documents whether the
/// written data is visible after reopening the file.
#[tokio::test]
#[ignore]
async fn rollback_exposure_write_flush_fails() {
    let _ = env_logger::try_init();
    let client = make_client().await;
    let tf = TestFile::new(&client).await;

    // 1. Create an empty file, flush to get initial inode persisted.
    {
        let mut hyper = Hyper::fs_open_or_create_with_default_opt(
            &client,
            tf.uri(),
            FileFlags::rdwr(),
            FileMode::default_file(),
        )
        .await
        .expect("create");
        // Release without writing — creates an empty persisted file.
        let _ = hyper.fs_release().await.expect("release");
    }

    // 2. Reopen, install always-failing interceptor, write and try to flush.
    let interceptor = AlwaysFailFlushInode::new();
    let (write_result, flush_result) = {
        let mut hyper = Hyper::fs_open(&client, tf.uri(), FileFlags::rdwr())
            .await
            .expect("open");
        hyper.with_staging_interceptor(interceptor.clone());

        let data = b"hello world";
        let wr = hyper.fs_write(0, data).await;
        let fl = hyper.fs_flush().await;
        let _release = hyper.fs_release().await;
        (wr, fl)
    };

    // write() returns Ok even when subsequent flush fails, because write
    // only modifies in-memory state. flush() is where the failure surfaces.
    println!(
        "[write_flush_fails] write_result={:?} flush_result={:?} interceptor_calls={}",
        write_result,
        flush_result,
        interceptor.call_count()
    );
    assert!(
        flush_result.is_err(),
        "expected flush to fail, got write={:?} flush={:?}",
        write_result,
        flush_result
    );

    // 3. Reopen and check persisted state.
    let persisted_size = {
        let hyper = Hyper::fs_open(&client, tf.uri(), FileFlags::rdonly())
            .await
            .expect("reopen");
        let stat = hyper.fs_getattr().expect("getattr");
        stat.st_size as usize
    };
    println!(
        "[write_flush_fails] persisted_size={}",
        persisted_size
    );

    // The persisted file should still be empty because all flush attempts failed.
    assert_eq!(
        persisted_size, 0,
        "persisted size diverged after failed write+flush: expected 0, got {}",
        persisted_size
    );

    tf.cleanup(&client).await;
}
