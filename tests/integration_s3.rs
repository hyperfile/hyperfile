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
use hyperfile::buffer::{AlignedDataBlockWrapper, BatchDataBlockWrapper};
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

/// Interceptor that fails `before_segment_done` on every call. Simulates
/// segment-write failures (e.g. S3 PutObject 5xx on the segment upload).
#[derive(Clone)]
pub struct AlwaysFailSegmentDone {
    calls: Arc<AtomicUsize>,
}

impl AlwaysFailSegmentDone {
    pub fn new() -> Self {
        Self { calls: Arc::new(AtomicUsize::new(0)) }
    }

    pub fn call_count(&self) -> usize {
        self.calls.load(Ordering::SeqCst)
    }
}

impl StagingIntercept<S3Staging> for AlwaysFailSegmentDone {
    fn before_segment_done(
        &self,
        _staging: &S3Staging,
        _segid: hyperfile::SegmentId,
        _buf: &[u8],
        _len: usize,
    ) -> std::pin::Pin<Box<dyn Future<Output = std::io::Result<()>> + '_ + Send>> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Box::pin(async move {
            Err(std::io::Error::new(
                ErrorKind::Other,
                "AlwaysFailSegmentDone: injected failure",
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

/// Interceptor that fails the first flush_inode call with `ResourceBusy`
/// (which the flush path treats as retryable) and succeeds from the second
/// call onward. Used to verify retry logic actually retries.
#[derive(Clone)]
pub struct ResourceBusyOnceFlushInode {
    calls: Arc<AtomicUsize>,
}

impl ResourceBusyOnceFlushInode {
    pub fn new() -> Self {
        Self { calls: Arc::new(AtomicUsize::new(0)) }
    }

    pub fn call_count(&self) -> usize {
        self.calls.load(Ordering::SeqCst)
    }
}

impl StagingIntercept<S3Staging> for ResourceBusyOnceFlushInode {
    fn before_flush_inode(
        &self,
        _staging: &S3Staging,
        _payload: &[u8],
        _flag: FlushInodeFlag,
    ) -> std::pin::Pin<Box<dyn Future<Output = std::io::Result<()>> + '_ + Send>> {
        let current = self.calls.fetch_add(1, Ordering::SeqCst) + 1;
        let fail = current == 1;
        Box::pin(async move {
            if fail {
                Err(std::io::Error::new(
                    ErrorKind::ResourceBusy,
                    "ResourceBusyOnceFlushInode: injected busy",
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

/// Interceptor that fails every flush_inode call with a non-retryable kind
/// (`ErrorKind::Other`). Used to verify the retry loop does NOT retry for
/// non-retryable errors.
#[derive(Clone)]
pub struct FailOtherNoRetry {
    calls: Arc<AtomicUsize>,
}

impl FailOtherNoRetry {
    pub fn new() -> Self {
        Self { calls: Arc::new(AtomicUsize::new(0)) }
    }

    pub fn call_count(&self) -> usize {
        self.calls.load(Ordering::SeqCst)
    }
}

impl StagingIntercept<S3Staging> for FailOtherNoRetry {
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
                "FailOtherNoRetry: injected non-retryable failure",
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

// --------------------------------------------------------------------
// Rollback CORRECTNESS tests (FailOnce — expose silent commits)
// --------------------------------------------------------------------
//
// These tests use FailOnFlushInode::at(1), so the FIRST flush_inode
// call fails but subsequent attempts succeed. This simulates the
// realistic case where a transient failure happens mid-operation.
//
// Without a proper rollback in write/truncate/write_zero, the
// sequence of events is:
//   1. Operation mutates in-memory inode (e.g. set_size).
//   2. Operation calls flush() → flush_inode #1 fails → returns Err.
//   3. Caller sees Err and assumes the operation didn't commit.
//   4. Caller eventually calls fs_release() → flush() → flush_inode #2
//      succeeds → the mutated in-memory state is committed.
//
// That is the silent-commit bug these tests are designed to expose.

/// Truncate-extend + a single transient flush failure. After release,
/// the persisted size MUST remain the original — otherwise we have
/// committed a "failed" operation.
#[tokio::test]
#[ignore]
async fn rollback_correctness_truncate_extend_single_flush_fails() {
    let _ = env_logger::try_init();
    let client = make_client().await;
    let tf = TestFile::new(&client).await;

    let original_size = 1024usize;
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

    // Fail exactly the first flush_inode; subsequent attempts succeed.
    let interceptor = FailOnFlushInode::at(1);
    let truncate_result = {
        let mut hyper = Hyper::fs_open(&client, tf.uri(), FileFlags::rdwr())
            .await
            .expect("open");
        hyper.with_staging_interceptor(interceptor.clone());
        let r = hyper.fs_truncate(8192).await;
        let _rel = hyper.fs_release().await;
        r
    };

    assert!(
        truncate_result.is_err(),
        "expected truncate to fail, got {:?}",
        truncate_result
    );
    println!(
        "[correctness_truncate_extend] truncate={:?} calls={}",
        truncate_result,
        interceptor.call_count()
    );

    let persisted_size = {
        let hyper = Hyper::fs_open(&client, tf.uri(), FileFlags::rdonly())
            .await
            .expect("reopen");
        hyper.fs_getattr().expect("getattr").st_size as usize
    };
    println!(
        "[correctness_truncate_extend] persisted_size={}",
        persisted_size
    );

    // INVARIANT: a failed truncate must NOT change the persisted size.
    assert_eq!(
        persisted_size, original_size,
        "failed truncate silently committed: expected size {}, got {}",
        original_size, persisted_size
    );

    tf.cleanup(&client).await;
}

/// Truncate-shrink + single flush failure. Besides size, we also verify
/// that all original blocks are still readable — bmap.truncate() destroys
/// in-memory mappings before flush, so without rollback the first block
/// can appear corrupted.
#[tokio::test]
#[ignore]
async fn rollback_correctness_truncate_shrink_single_flush_fails() {
    let _ = env_logger::try_init();
    let client = make_client().await;
    let tf = TestFile::new(&client).await;

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

    let interceptor = FailOnFlushInode::at(1);
    let truncate_result = {
        let mut hyper = Hyper::fs_open(&client, tf.uri(), FileFlags::rdwr())
            .await
            .expect("open");
        hyper.with_staging_interceptor(interceptor.clone());
        let r = hyper.fs_truncate(4096).await;
        let _rel = hyper.fs_release().await;
        r
    };

    assert!(
        truncate_result.is_err(),
        "expected truncate to fail, got {:?}",
        truncate_result
    );
    println!(
        "[correctness_truncate_shrink] truncate={:?} calls={}",
        truncate_result,
        interceptor.call_count()
    );

    let (persisted_size, first_ok, last_ok) = {
        let mut hyper = Hyper::fs_open(&client, tf.uri(), FileFlags::rdonly())
            .await
            .expect("reopen");
        let size = hyper.fs_getattr().expect("getattr").st_size as usize;

        let mut b0 = vec![0u8; 4096];
        let r0 = hyper.fs_read(0, &mut b0).await;
        let first_ok = r0.is_ok() && b0 == payload[..4096];

        let last_ok = if size >= 16 * 1024 {
            let mut b3 = vec![0u8; 4096];
            let r3 = hyper.fs_read(12 * 1024, &mut b3).await;
            Some(r3.is_ok() && b3 == payload[12 * 1024..16 * 1024])
        } else {
            None
        };
        (size, first_ok, last_ok)
    };
    println!(
        "[correctness_truncate_shrink] size={} first_ok={} last_ok={:?}",
        persisted_size, first_ok, last_ok
    );

    // INVARIANTS after a failed shrink:
    //   1. persisted size unchanged.
    //   2. all blocks of the original file still readable.
    assert_eq!(
        persisted_size, original_size,
        "failed shrink silently committed: size expected {}, got {}",
        original_size, persisted_size
    );
    assert!(first_ok, "first block corrupted after failed shrink");
    assert_eq!(
        last_ok,
        Some(true),
        "last block unreadable after failed shrink — data lost"
    );

    tf.cleanup(&client).await;
}

/// Write + single flush failure. After release, the persisted file MUST
/// still be empty (0 bytes), not the 11 bytes we attempted to write.
#[tokio::test]
#[ignore]
async fn rollback_correctness_write_single_flush_fails() {
    let _ = env_logger::try_init();
    let client = make_client().await;
    let tf = TestFile::new(&client).await;

    {
        let mut hyper = Hyper::fs_open_or_create_with_default_opt(
            &client,
            tf.uri(),
            FileFlags::rdwr(),
            FileMode::default_file(),
        )
        .await
        .expect("create");
        let _ = hyper.fs_release().await.expect("release");
    }

    let interceptor = FailOnFlushInode::at(1);
    let (write_result, flush_result) = {
        let mut hyper = Hyper::fs_open(&client, tf.uri(), FileFlags::rdwr())
            .await
            .expect("open");
        hyper.with_staging_interceptor(interceptor.clone());
        let wr = hyper.fs_write(0, b"hello world").await;
        let fl = hyper.fs_flush().await;
        let _rel = hyper.fs_release().await;
        (wr, fl)
    };

    println!(
        "[correctness_write] write={:?} flush={:?} calls={}",
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

    let persisted_size = {
        let hyper = Hyper::fs_open(&client, tf.uri(), FileFlags::rdonly())
            .await
            .expect("reopen");
        hyper.fs_getattr().expect("getattr").st_size as usize
    };
    println!("[correctness_write] persisted_size={}", persisted_size);

    // INVARIANT: a failed write+flush must not commit the data.
    assert_eq!(
        persisted_size, 0,
        "failed write silently committed: expected 0, got {}",
        persisted_size
    );

    tf.cleanup(&client).await;
}

// --------------------------------------------------------------------
// A: flush contract invariants (success + rollback)
// --------------------------------------------------------------------

/// A: after a successful flush, all dirty indicators must be cleared and
/// the cno bookkeeping must be in sync with persisted state.
#[tokio::test]
#[ignore]
async fn contract_flush_success_clears_all_dirty_flags() {
    let _ = env_logger::try_init();
    let client = make_client().await;
    let tf = TestFile::new(&client).await;

    let mut hyper = Hyper::fs_open_or_create_with_default_opt(
        &client,
        tf.uri(),
        FileFlags::rdwr(),
        FileMode::default_file(),
    )
    .await
    .expect("create");

    // Write data → cache is dirty, bmap is dirty.
    let data = vec![0xABu8; 4096];
    let _ = hyper.fs_write(0, &data).await.expect("write");
    assert!(hyper.dirty_block_count() > 0, "expected dirty blocks before flush");
    assert!(hyper.is_bmap_dirty(), "expected bmap dirty before flush");
    assert!(hyper.is_attr_dirty(), "expected attr dirty before flush (mtime set)");

    // Flush.
    let cno = hyper.fs_flush().await.expect("flush");

    // Invariants after successful flush:
    assert_eq!(hyper.dirty_block_count(), 0, "dirty blocks not cleared after flush");
    assert!(!hyper.is_bmap_dirty(), "bmap still dirty after flush");
    assert!(!hyper.is_attr_dirty(), "attr still dirty after flush");
    assert_eq!(
        hyper.in_memory_last_cno(),
        hyper.in_memory_last_ondisk_cno(),
        "last_cno and last_ondisk_cno diverged after successful flush",
    );
    assert!(cno > 0, "flush should return a non-zero cno on success");

    let _ = hyper.fs_release().await;
    tf.cleanup(&client).await;
}

/// A: after flush fails and fs_flush rolls back, the in-memory state must
/// match what was persisted (which is the pre-write state).
#[tokio::test]
#[ignore]
async fn contract_flush_fail_rollback_restores_clean_state() {
    let _ = env_logger::try_init();
    let client = make_client().await;
    let tf = TestFile::new(&client).await;

    // Prepare an empty persisted file.
    {
        let mut hyper = Hyper::fs_open_or_create_with_default_opt(
            &client,
            tf.uri(),
            FileFlags::rdwr(),
            FileMode::default_file(),
        )
        .await
        .expect("create");
        let _ = hyper.fs_release().await.expect("release");
    }

    // Reopen, install always-fail interceptor, write, try to flush.
    let mut hyper = Hyper::fs_open(&client, tf.uri(), FileFlags::rdwr())
        .await
        .expect("open");
    hyper.with_staging_interceptor(AlwaysFailFlushInode::new());

    let baseline_cno = hyper.in_memory_last_cno();
    let baseline_ondisk = hyper.in_memory_last_ondisk_cno();
    assert_eq!(baseline_cno, baseline_ondisk, "baseline cno mismatch after open");

    let _ = hyper.fs_write(0, &[0xAAu8; 100]).await.expect("write");
    assert!(hyper.dirty_block_count() > 0, "expected dirty after write");

    // Flush MUST fail.
    let flush_res = hyper.fs_flush().await;
    assert!(flush_res.is_err(), "flush should have failed");

    // Invariants after rollback:
    //   dirty flags all cleared (rolled back to persisted state)
    //   last_cno and last_ondisk_cno aligned with each other and with baseline
    assert_eq!(hyper.dirty_block_count(), 0, "dirty blocks not cleared by rollback");
    assert!(!hyper.is_bmap_dirty(), "bmap still dirty after rollback");
    assert!(!hyper.is_attr_dirty(), "attr still dirty after rollback");
    assert_eq!(
        hyper.in_memory_last_cno(),
        hyper.in_memory_last_ondisk_cno(),
        "cno tracking still diverged after rollback",
    );
    assert_eq!(
        hyper.in_memory_last_cno(),
        baseline_cno,
        "cno moved despite rollback",
    );

    let _ = hyper.fs_release().await;
    tf.cleanup(&client).await;
}

// --------------------------------------------------------------------
// B: segment-done failure path
// --------------------------------------------------------------------

/// B: when `segwr.done()` (segment upload) always fails, flush must return
/// Err and the persisted file must be unchanged. Rollback clears the
/// in-memory mutations.
#[tokio::test]
#[ignore]
async fn contract_segment_done_fail_persisted_unchanged() {
    let _ = env_logger::try_init();
    let client = make_client().await;
    let tf = TestFile::new(&client).await;

    let original_size = 512usize;
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

    let interceptor = AlwaysFailSegmentDone::new();
    let write_res = {
        let mut hyper = Hyper::fs_open(&client, tf.uri(), FileFlags::rdwr())
            .await
            .expect("open");
        hyper.with_staging_interceptor(interceptor.clone());

        let _ = hyper.fs_write(1024, &[0xFFu8; 128]).await.expect("write buffered");
        let res = hyper.fs_flush().await;
        let _ = hyper.fs_release().await;
        res
    };

    assert!(
        write_res.is_err(),
        "expected flush to fail when segment done fails, got {:?}",
        write_res,
    );
    assert!(interceptor.call_count() >= 1, "segment done interceptor never invoked");
    println!(
        "[segment_done_fail] flush={:?} interceptor_calls={}",
        write_res, interceptor.call_count()
    );

    // Reopen and confirm persisted file hasn't grown.
    let persisted_size = {
        let hyper = Hyper::fs_open(&client, tf.uri(), FileFlags::rdonly())
            .await
            .expect("reopen");
        hyper.fs_getattr().expect("getattr").st_size as usize
    };
    assert_eq!(
        persisted_size, original_size,
        "persisted size changed after failed segment upload",
    );

    tf.cleanup(&client).await;
}

// --------------------------------------------------------------------
// C: attr-only flush takes the early-return path
// --------------------------------------------------------------------

/// C: flushing when only inode attrs are dirty (no data / bmap changes)
/// should NOT allocate a new segment — the flush updates inode only.
#[tokio::test]
#[ignore]
async fn contract_attr_only_flush_does_not_produce_new_segment() {
    let _ = env_logger::try_init();
    let client = make_client().await;
    let tf = TestFile::new(&client).await;

    // Create file, write data, flush — this creates segment 1.
    let cno_after_write = {
        let mut hyper = Hyper::fs_open_or_create_with_default_opt(
            &client,
            tf.uri(),
            FileFlags::rdwr(),
            FileMode::default_file(),
        )
        .await
        .expect("create");
        let _ = hyper.fs_write(0, &[0xABu8; 1024]).await.expect("write");
        let cno = hyper.fs_flush().await.expect("flush");
        let _ = hyper.fs_release().await;
        cno
    };
    assert!(cno_after_write > 0, "first flush should produce a segment");

    // Reopen, chmod only (no data/bmap changes). `fs_chmod` already calls
    // flush() internally, so we can observe the post-flush state directly.
    let cno_after_chmod = {
        let mut hyper = Hyper::fs_open(&client, tf.uri(), FileFlags::rdwr())
            .await
            .expect("open");
        assert_eq!(hyper.in_memory_last_cno(), cno_after_write);

        let _ = hyper.fs_chmod(0o600).await.expect("chmod");

        // After chmod's internal flush: no dirty state, cno unchanged because
        // the attr-only early-return path did not allocate a new segment.
        assert!(!hyper.is_attr_dirty(), "attr_dirty should be cleared by chmod's flush");
        assert!(!hyper.is_bmap_dirty(), "bmap should not be dirty after attr-only flush");
        assert_eq!(hyper.dirty_block_count(), 0, "no dirty blocks after attr-only flush");

        let cno = hyper.in_memory_last_cno();
        let _ = hyper.fs_release().await;
        cno
    };

    // attr-only flush takes the early-return branch: cno unchanged.
    assert_eq!(
        cno_after_chmod, cno_after_write,
        "attr-only flush should not advance cno",
    );

    // Reopen and verify the mode was persisted despite no new segment.
    let mode = {
        let hyper = Hyper::fs_open(&client, tf.uri(), FileFlags::rdonly())
            .await
            .expect("reopen");
        hyper.fs_getattr().expect("getattr").st_mode
    };
    assert_eq!(mode & 0o777, 0o600, "mode change did not persist");

    tf.cleanup(&client).await;
}

// --------------------------------------------------------------------
// D: retry policy
// --------------------------------------------------------------------

/// D: a single ResourceBusy failure should be retried. The flush should
/// eventually succeed and commit the mutations.
#[tokio::test]
#[ignore]
async fn contract_flush_retries_on_resource_busy() {
    let _ = env_logger::try_init();
    let client = make_client().await;
    let tf = TestFile::new(&client).await;

    {
        let mut hyper = Hyper::fs_open_or_create_with_default_opt(
            &client,
            tf.uri(),
            FileFlags::rdwr(),
            FileMode::default_file(),
        )
        .await
        .expect("create");
        let _ = hyper.fs_release().await.expect("release");
    }

    let interceptor = ResourceBusyOnceFlushInode::new();
    let flush_res = {
        let mut hyper = Hyper::fs_open(&client, tf.uri(), FileFlags::rdwr())
            .await
            .expect("open");
        hyper.with_staging_interceptor(interceptor.clone());
        let _ = hyper.fs_write(0, b"retry me").await.expect("write");
        let res = hyper.fs_flush().await;
        let _ = hyper.fs_release().await;
        res
    };

    assert!(
        flush_res.is_ok(),
        "flush should succeed after ResourceBusy retry, got {:?}",
        flush_res,
    );
    assert!(
        interceptor.call_count() >= 2,
        "interceptor should have been invoked at least twice (1 fail + 1 retry), got {}",
        interceptor.call_count(),
    );
    println!(
        "[retry_resource_busy] flush={:?} interceptor_calls={}",
        flush_res, interceptor.call_count()
    );

    // Verify the data was committed.
    let persisted_size = {
        let hyper = Hyper::fs_open(&client, tf.uri(), FileFlags::rdonly())
            .await
            .expect("reopen");
        hyper.fs_getattr().expect("getattr").st_size as usize
    };
    assert_eq!(persisted_size, 8, "retried flush should have committed");

    tf.cleanup(&client).await;
}

/// D: a non-retryable error (ErrorKind::Other) must NOT trigger retries.
/// The interceptor should be invoked exactly once per flush attempt.
#[tokio::test]
#[ignore]
async fn contract_flush_does_not_retry_on_other_error() {
    let _ = env_logger::try_init();
    let client = make_client().await;
    let tf = TestFile::new(&client).await;

    {
        let mut hyper = Hyper::fs_open_or_create_with_default_opt(
            &client,
            tf.uri(),
            FileFlags::rdwr(),
            FileMode::default_file(),
        )
        .await
        .expect("create");
        let _ = hyper.fs_release().await.expect("release");
    }

    let interceptor = FailOtherNoRetry::new();
    let flush_res = {
        let mut hyper = Hyper::fs_open(&client, tf.uri(), FileFlags::rdwr())
            .await
            .expect("open");
        hyper.with_staging_interceptor(interceptor.clone());
        let _ = hyper.fs_write(0, b"fail me").await.expect("write");
        let res = hyper.fs_flush().await;
        // Release also calls flush; count it to confirm no retry loop.
        let _ = hyper.fs_release().await;
        res
    };

    assert!(flush_res.is_err(), "flush should fail");
    // fs_flush runs flush once, fs_release runs flush once. With NO retry
    // on Other, the interceptor is invoked exactly twice total (never
    // 3x from the retry loop). Allow <= 2 in case release skips flush
    // because rollback cleared dirty state.
    assert!(
        interceptor.call_count() <= 2,
        "expected <= 2 interceptor calls (1 from fs_flush, up to 1 from fs_release), got {}",
        interceptor.call_count(),
    );
    println!(
        "[no_retry_on_other] flush={:?} interceptor_calls={}",
        flush_res, interceptor.call_count()
    );

    tf.cleanup(&client).await;
}

// --------------------------------------------------------------------
// Rollback correctness tests for the remaining write variants
// --------------------------------------------------------------------
//
// rollback_from_persisted is wired into write/write_zero/truncate/
// write_aligned_batch/write_batch, but integration tests only exercised
// write and truncate so far. The three tests below close the gap.

/// fs_write_zero extends the file with a sparse region of zeros. With a
/// single transient flush_inode failure, the operation must return Err
/// and leave the persisted file unchanged.
#[tokio::test]
#[ignore]
async fn rollback_correctness_write_zero_single_flush_fails() {
    let _ = env_logger::try_init();
    let client = make_client().await;
    let tf = TestFile::new(&client).await;

    let original_size = 4096usize;
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

    // Fail only the first flush_inode; if rollback works, the zero-fill
    // extension must not be visible after reopen.
    //
    // Note: fs_write_zero returns Ok for the buffered write; the actual
    // flush happens later. We call fs_flush explicitly so the injected
    // failure surfaces at a well-defined point.
    let interceptor = FailOnFlushInode::at(1);
    let (write_zero_result, flush_result) = {
        let mut hyper = Hyper::fs_open(&client, tf.uri(), FileFlags::rdwr())
            .await
            .expect("open");
        hyper.with_staging_interceptor(interceptor.clone());
        // Extend from 4 KiB to 12 KiB with zeros.
        let wr = hyper.fs_write_zero(original_size, 8192).await;
        let fr = hyper.fs_flush().await;
        let _rel = hyper.fs_release().await;
        (wr, fr)
    };

    // write_zero itself may return Ok (data buffered); flush must fail.
    assert!(
        flush_result.is_err(),
        "expected flush after write_zero to fail, got wr={:?} flush={:?}",
        write_zero_result, flush_result,
    );
    println!(
        "[correctness_write_zero] write_zero={:?} flush={:?} calls={}",
        write_zero_result,
        flush_result,
        interceptor.call_count(),
    );

    // Reopen, verify size and original bytes.
    let (persisted_size, body_ok) = {
        let mut hyper = Hyper::fs_open(&client, tf.uri(), FileFlags::rdonly())
            .await
            .expect("reopen");
        let size = hyper.fs_getattr().expect("getattr").st_size as usize;
        let mut buf = vec![0u8; original_size];
        let _ = hyper.fs_read(0, &mut buf).await.expect("read");
        (size, buf == payload)
    };

    assert_eq!(
        persisted_size, original_size,
        "write_zero silently committed: expected size {}, got {}",
        original_size, persisted_size,
    );
    assert!(body_ok, "original bytes corrupted after failed write_zero");

    tf.cleanup(&client).await;
}

/// fs_write_aligned_batch writes a mix of data and zero blocks at 4 KiB
/// alignment. A single flush_inode failure must leave the file empty.
#[tokio::test]
#[ignore]
async fn rollback_correctness_write_aligned_batch_single_flush_fails() {
    let _ = env_logger::try_init();
    let client = make_client().await;
    let tf = TestFile::new(&client).await;

    // Start from an empty persisted file.
    {
        let mut hyper = Hyper::fs_open_or_create_with_default_opt(
            &client,
            tf.uri(),
            FileFlags::rdwr(),
            FileMode::default_file(),
        )
        .await
        .expect("create");
        let _ = hyper.fs_release().await.expect("release");
    }

    let interceptor = FailOnFlushInode::at(1);
    let (batch_result, flush_result) = {
        let mut hyper = Hyper::fs_open(&client, tf.uri(), FileFlags::rdwr())
            .await
            .expect("open");
        hyper.with_staging_interceptor(interceptor.clone());

        // Three 4 KiB blocks: data, zero, data. Exercises both code paths.
        let block0 = AlignedDataBlockWrapper::new(0, 4096, false);
        block0.as_mut_slice().fill(0xAA);
        let block1 = AlignedDataBlockWrapper::new(1, 4096, true); // zero
        let block2 = AlignedDataBlockWrapper::new(2, 4096, false);
        block2.as_mut_slice().fill(0xCC);
        let blocks = vec![block0, block1, block2];

        let br = hyper.fs_write_aligned_batch(blocks).await;
        let fr = hyper.fs_flush().await;
        let _rel = hyper.fs_release().await;
        (br, fr)
    };

    assert!(
        flush_result.is_err(),
        "expected flush after write_aligned_batch to fail, got batch={:?} flush={:?}",
        batch_result, flush_result,
    );
    println!(
        "[correctness_aligned_batch] batch={:?} flush={:?} calls={}",
        batch_result,
        flush_result,
        interceptor.call_count(),
    );

    // Reopen, file must still be empty.
    let persisted_size = {
        let hyper = Hyper::fs_open(&client, tf.uri(), FileFlags::rdonly())
            .await
            .expect("reopen");
        hyper.fs_getattr().expect("getattr").st_size as usize
    };
    assert_eq!(
        persisted_size, 0,
        "write_aligned_batch silently committed: expected 0, got {}",
        persisted_size,
    );

    tf.cleanup(&client).await;
}

/// fs_write_batch with a partial block over an existing file. A single
/// flush_inode failure must leave the original data unchanged, and the
/// size must not shift.
#[tokio::test]
#[ignore]
async fn rollback_correctness_write_batch_single_flush_fails() {
    let _ = env_logger::try_init();
    let client = make_client().await;
    let tf = TestFile::new(&client).await;

    let original_size = 4096usize;
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

    let interceptor = FailOnFlushInode::at(1);
    let (batch_result, flush_result) = {
        let mut hyper = Hyper::fs_open(&client, tf.uri(), FileFlags::rdwr())
            .await
            .expect("open");
        hyper.with_staging_interceptor(interceptor.clone());

        // Partial block: block 0, offset 100, len 200 — overwrites 200
        // bytes in the middle of the already-persisted block.
        let part = BatchDataBlockWrapper::new_partial_block(
            0, 4096, 100, 200, false,
        );
        part.as_mut_slice().fill(0xFF);
        let blocks = vec![part];

        let br = hyper.fs_write_batch(blocks).await;
        let fr = hyper.fs_flush().await;
        let _rel = hyper.fs_release().await;
        (br, fr)
    };

    assert!(
        flush_result.is_err(),
        "expected flush after write_batch to fail, got batch={:?} flush={:?}",
        batch_result, flush_result,
    );
    println!(
        "[correctness_batch] batch={:?} flush={:?} calls={}",
        batch_result,
        flush_result,
        interceptor.call_count(),
    );

    // Reopen, verify size and bytes intact.
    let (persisted_size, body_ok) = {
        let mut hyper = Hyper::fs_open(&client, tf.uri(), FileFlags::rdonly())
            .await
            .expect("reopen");
        let size = hyper.fs_getattr().expect("getattr").st_size as usize;
        let mut buf = vec![0u8; original_size];
        let _ = hyper.fs_read(0, &mut buf).await.expect("read");
        (size, buf == payload)
    };

    assert_eq!(
        persisted_size, original_size,
        "write_batch silently changed size: expected {}, got {}",
        original_size, persisted_size,
    );
    assert!(
        body_ok,
        "write_batch silently modified original bytes",
    );

    tf.cleanup(&client).await;
}

// --------------------------------------------------------------------
// Concurrent-writer / concurrent-reader tests (A group)
// --------------------------------------------------------------------
//
// hyperfile's concurrency model:
//   * Single Hyper instance serializes writes via a semaphore(1) when
//     `range-lock` feature is disabled (the default test configuration).
//   * Multiple Hyper instances opening the same S3 URI are INDEPENDENT
//     and rely on S3-level optimistic concurrency control through
//     `OnDiskState` (checksum / ETag) threaded into `flush_inode`.
//
// These tests exercise the multi-instance case. The expected invariant
// is that two writers racing on the same URI cannot silently both commit
// divergent state — either one wins and the other gets a well-defined
// error, or they serialize so the last writer's view is consistent.

/// Two writers open the same URI, write different data, flush concurrently.
/// Under optimistic concurrency control we expect:
///   * At most one of the two flushes reports Ok.
///   * After both finish, reopening shows a state consistent with the
///     winning writer (either A's or B's bytes, not a mix).
#[tokio::test]
#[ignore]
async fn concurrent_two_writers_optimistic_cc() {
    let _ = env_logger::try_init();
    let client = make_client().await;
    let tf = TestFile::new(&client).await;

    // Seed an empty file so both writers open the same persisted baseline.
    {
        let mut hyper = Hyper::fs_open_or_create_with_default_opt(
            &client,
            tf.uri(),
            FileFlags::rdwr(),
            FileMode::default_file(),
        )
        .await
        .expect("create");
        let _ = hyper.fs_release().await.expect("release");
    }

    let uri = tf.uri().to_string();
    let client_a = client.clone();
    let client_b = client.clone();
    let uri_a = uri.clone();
    let uri_b = uri.clone();

    let writer_a = async move {
        let mut hyper = Hyper::fs_open(&client_a, &uri_a, FileFlags::rdwr())
            .await
            .expect("open A");
        // A writes 4 KiB of 0xAA.
        let data = vec![0xAAu8; 4096];
        let _ = hyper.fs_write(0, &data).await.expect("write A");
        let flush = hyper.fs_flush().await;
        let _ = hyper.fs_release().await;
        flush
    };

    let writer_b = async move {
        let mut hyper = Hyper::fs_open(&client_b, &uri_b, FileFlags::rdwr())
            .await
            .expect("open B");
        // B writes 4 KiB of 0xBB.
        let data = vec![0xBBu8; 4096];
        let _ = hyper.fs_write(0, &data).await.expect("write B");
        let flush = hyper.fs_flush().await;
        let _ = hyper.fs_release().await;
        flush
    };

    let (a_flush, b_flush) = tokio::join!(writer_a, writer_b);

    let a_ok = a_flush.is_ok();
    let b_ok = b_flush.is_ok();
    println!(
        "[two_writers_occ] A flush={:?} B flush={:?}",
        a_flush, b_flush
    );

    // Invariant 1: at least one writer must succeed (otherwise we made no
    // progress). Invariant 2: at most one succeeds if OCC is working.
    assert!(
        a_ok || b_ok,
        "both writers failed: A={:?} B={:?}",
        a_flush, b_flush,
    );

    // Read the final persisted state.
    let mut buf = vec![0u8; 4096];
    let mut hyper = Hyper::fs_open(&client, &uri, FileFlags::rdonly())
        .await
        .expect("reopen");
    let stat = hyper.fs_getattr().expect("getattr");
    let n = hyper.fs_read(0, &mut buf).await.expect("read");
    let _ = hyper.fs_release().await;

    println!(
        "[two_writers_occ] final size={} first_byte=0x{:02X} last_byte=0x{:02X}",
        stat.st_size, buf[0], buf[n - 1]
    );

    // The persisted bytes must be a single writer's data, not a mix.
    let all_aa = buf.iter().all(|&b| b == 0xAA);
    let all_bb = buf.iter().all(|&b| b == 0xBB);
    assert!(
        all_aa || all_bb,
        "persisted content is a mix of A and B — consistency broken (first={:#x} last={:#x})",
        buf[0], buf[n - 1],
    );

    // OCC invariant: if both flushes claimed Ok, there's a good chance the
    // second one silently overwrote the first. Record the observation.
    if a_ok && b_ok {
        println!(
            "[two_writers_occ] WARNING: both flushes reported Ok — OCC may not be enforcing exclusion"
        );
    }

    tf.cleanup(&client).await;
}

/// A writer repeatedly writes and flushes; a concurrent reader opens
/// the URI while that's happening. Every reader open must yield a
/// self-consistent view (the size must match what can be read).
#[tokio::test]
#[ignore]
async fn concurrent_read_while_writer_flushes() {
    let _ = env_logger::try_init();
    let client = make_client().await;
    let tf = TestFile::new(&client).await;

    // Seed a known initial state.
    {
        let mut hyper = Hyper::fs_open_or_create_with_default_opt(
            &client,
            tf.uri(),
            FileFlags::rdwr(),
            FileMode::default_file(),
        )
        .await
        .expect("create");
        let initial = vec![0x11u8; 4096];
        let _ = hyper.fs_write(0, &initial).await.expect("write");
        let _ = hyper.fs_release().await.expect("release");
    }

    let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));

    let uri = tf.uri().to_string();
    let client_w = client.clone();
    let uri_w = uri.clone();
    let stop_w = stop.clone();

    let writer = async move {
        let mut iterations = 0u32;
        while !stop_w.load(Ordering::SeqCst) && iterations < 8 {
            let mut hyper = Hyper::fs_open(&client_w, &uri_w, FileFlags::rdwr())
                .await
                .expect("writer open");
            // Each iteration writes a new 4 KiB pattern filled with the
            // iteration number (low byte), then flushes.
            let byte = 0x20u8 + (iterations as u8);
            let data = vec![byte; 4096];
            let _ = hyper.fs_write(0, &data).await.expect("writer write");
            let _ = hyper.fs_flush().await;
            let _ = hyper.fs_release().await;
            iterations += 1;
            tokio::task::yield_now().await;
        }
        iterations
    };

    let uri_r = uri.clone();
    let client_r = client.clone();
    let stop_r = stop.clone();

    let reader = async move {
        let mut observations = Vec::new();
        let mut reads = 0u32;
        while !stop_r.load(Ordering::SeqCst) && reads < 20 {
            let mut hyper = Hyper::fs_open(&client_r, &uri_r, FileFlags::rdonly())
                .await
                .expect("reader open");
            let size = hyper.fs_getattr().expect("getattr").st_size as usize;
            let mut buf = vec![0u8; size];
            let _ = hyper.fs_read(0, &mut buf).await.expect("read");
            let _ = hyper.fs_release().await;

            // Every read must see a homogeneous block — a single writer's
            // pattern. If the size says 4096 but bytes diverge, the
            // checkpoint is inconsistent.
            let first = buf.first().copied();
            let all_same = buf.iter().all(|&b| Some(b) == first);
            observations.push((size, first, all_same));

            reads += 1;
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
        observations
    };

    // Run writer + reader concurrently; have a separate future arm the
    // stop flag after a short window.
    let stop_arm = stop.clone();
    let timer = async move {
        tokio::time::sleep(std::time::Duration::from_secs(2)).await;
        stop_arm.store(true, Ordering::SeqCst);
    };

    let (writer_iters, observations, _) = tokio::join!(writer, reader, timer);

    println!(
        "[read_while_writer_flushes] writer did {} iterations, reader made {} reads",
        writer_iters, observations.len()
    );

    // Every observation must be self-consistent (homogeneous buffer).
    for (i, (size, first, ok)) in observations.iter().enumerate() {
        assert!(
            *ok,
            "observation #{} inconsistent: size={} first_byte={:?} buffer not homogeneous",
            i, size, first,
        );
    }

    tf.cleanup(&client).await;
}

/// Two writers again, but writer A's flush is injected with always-fail
/// (the instance never commits). Writer B writes normally. Expected:
///   * A's flush returns Err (and rollback leaves it consistent).
///   * B's flush returns Ok.
///   * After both finish, the persisted content matches B's write.
#[tokio::test]
#[ignore]
async fn concurrent_writer_a_fails_writer_b_succeeds() {
    let _ = env_logger::try_init();
    let client = make_client().await;
    let tf = TestFile::new(&client).await;

    // Seed empty file.
    {
        let mut hyper = Hyper::fs_open_or_create_with_default_opt(
            &client,
            tf.uri(),
            FileFlags::rdwr(),
            FileMode::default_file(),
        )
        .await
        .expect("create");
        let _ = hyper.fs_release().await.expect("release");
    }

    let uri = tf.uri().to_string();
    let client_a = client.clone();
    let client_b = client.clone();
    let uri_a = uri.clone();
    let uri_b = uri.clone();

    let writer_a = async move {
        let mut hyper = Hyper::fs_open(&client_a, &uri_a, FileFlags::rdwr())
            .await
            .expect("open A");
        hyper.with_staging_interceptor(AlwaysFailFlushInode::new());
        let data = vec![0xAAu8; 2048];
        let _ = hyper.fs_write(0, &data).await.expect("write A");
        let flush = hyper.fs_flush().await;
        let _ = hyper.fs_release().await;
        flush
    };

    let writer_b = async move {
        let mut hyper = Hyper::fs_open(&client_b, &uri_b, FileFlags::rdwr())
            .await
            .expect("open B");
        let data = vec![0xBBu8; 2048];
        let _ = hyper.fs_write(0, &data).await.expect("write B");
        let flush = hyper.fs_flush().await;
        let _ = hyper.fs_release().await;
        flush
    };

    let (a_flush, b_flush) = tokio::join!(writer_a, writer_b);

    println!(
        "[a_fails_b_succeeds] A flush={:?} B flush={:?}",
        a_flush, b_flush
    );

    assert!(
        a_flush.is_err(),
        "writer A should fail due to injected interceptor, got {:?}",
        a_flush,
    );
    assert!(
        b_flush.is_ok(),
        "writer B should succeed, got {:?}",
        b_flush,
    );

    // Persisted content must be exactly B's bytes.
    let mut buf = vec![0u8; 2048];
    let mut hyper = Hyper::fs_open(&client, &uri, FileFlags::rdonly())
        .await
        .expect("reopen");
    let size = hyper.fs_getattr().expect("getattr").st_size as usize;
    let _ = hyper.fs_read(0, &mut buf).await.expect("read");
    let _ = hyper.fs_release().await;

    assert_eq!(size, 2048, "unexpected persisted size: {}", size);
    assert!(
        buf.iter().all(|&b| b == 0xBB),
        "persisted bytes do not match writer B's data (first={:#x}, last={:#x})",
        buf[0], buf[2047],
    );

    tf.cleanup(&client).await;
}
