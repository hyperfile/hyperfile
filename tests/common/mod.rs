//! Shared helpers for S3 integration tests.
//!
//! Every `tests/integration_s3_*.rs` binary re-exports what it needs from
//! this module via:
//!
//! ```ignore
//! #[allow(dead_code)]
//! mod common;
//! use common::*;
//! ```
//!
//! The `#[allow(dead_code)]` suppresses warnings because each test
//! binary will only use a subset of the helpers in this module.

#![allow(dead_code)]

use std::io::ErrorKind;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use aws_sdk_s3::Client;
use hyperfile::file::hyper::Hyper;
use hyperfile::inode::FlushInodeFlag;
use hyperfile::staging::StagingIntercept;
use hyperfile::staging::s3::S3Staging;
use hyperfile::SegmentId;

pub const DEFAULT_BUCKET: &str = "<your-bucket>";
pub const DEFAULT_REGION: &str = "<your-region>";

pub fn test_bucket() -> String {
    std::env::var("HYPERFILE_TEST_BUCKET").unwrap_or_else(|_| DEFAULT_BUCKET.to_string())
}

pub fn test_region() -> String {
    std::env::var("HYPERFILE_TEST_REGION").unwrap_or_else(|_| DEFAULT_REGION.to_string())
}

/// Build an S3 client configured for the test region.
pub async fn make_client() -> Client {
    let region = test_region();
    let config = aws_config::from_env()
        .region(aws_config::Region::new(region))
        .load()
        .await;
    Client::new(&config)
}

/// A test fixture that owns a unique S3 URI.
///
/// Cleanup is the responsibility of each test (call `cleanup` before
/// returning). We intentionally avoid Drop-based cleanup because:
///   1. We can't `await` in Drop, so we'd need to block on a new runtime —
///      which deadlocks in some cases when the AWS SDK has background tasks.
///   2. The `Hyper` object itself owns a tokio runtime that shuts down in
///      its own Drop; stacking more async cleanup on top makes teardown
///      unpredictable.
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

// ---------------------------------------------------------------------
// Fault-injection interceptors
// ---------------------------------------------------------------------

/// Interceptor that causes the N-th call to `before_flush_inode` to fail
/// (1-based). Subsequent calls succeed. Useful for exercising "transient
/// failure" scenarios where the caller must not silently commit.
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
/// Use this to simulate persistent failures where no retry can succeed.
#[derive(Clone)]
pub struct AlwaysFailFlushInode {
    calls: Arc<AtomicUsize>,
}

impl AlwaysFailFlushInode {
    pub fn new() -> Self {
        Self { calls: Arc::new(AtomicUsize::new(0)) }
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
/// segment-upload failures (e.g. S3 PutObject 5xx on the segment itself
/// rather than on the inode).
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
        _segid: SegmentId,
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

/// Interceptor that fails the first `before_flush_inode` call with
/// `ResourceBusy` (which the flush path treats as retryable) and
/// succeeds from the second call onward. Used to verify retry logic.
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

/// Interceptor that fails every `before_flush_inode` call with a
/// non-retryable kind (`ErrorKind::Other`). Verifies the retry loop
/// does NOT retry for non-retryable errors.
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
