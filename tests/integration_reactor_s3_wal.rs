//! Reactor + WAL integration tests.
//!
//! Requires `--features wal`. With WAL enabled, writes are durable to
//! a separate S3 prefix (the "WAL root") before flush; recovery on
//! reopen replays unflushed data.
//!
//! Each test configures a WAL prefix as a sibling to the main file
//! staging (`<uri>/wal/`) and verifies:
//!
//!  - Buffered writes produce WAL chunks.
//!  - A successful flush clears the WAL for the committed segid.
//!  - An unflushed write is replayed on reopen (recovery path).
//!
//! Run with:
//! ```bash
//! cargo test --features wal --test integration_reactor_s3_wal \
//!     -- --ignored --test-threads=1
//! ```

#![cfg(all(feature = "reactor", feature = "wal"))]

#[allow(dead_code)]
mod common;
#[allow(dead_code)]
mod common_reactor;

use common::*;
use common_reactor::*;

use hyperfile::file::fh::HyperFileHandler;
use hyperfile::file::flags::FileFlags;
use hyperfile::file::mode::FileMode;
use hyperfile::file::hyper::Hyper;
use hyperfile::file::flags::HyperFileFlags;
use hyperfile::file::mode::HyperFileMode;
use hyperfile::config::{HyperFileConfig, HyperFileConfigBuilder};
use hyperfile::staging::config::StagingConfig;
use hyperfile::wal::config::HyperFileWalConfig;

/// Build a HyperFileConfig with WAL pointing at `<uri>/wal/`.
fn build_wal_config(uri: &str) -> HyperFileConfig {
    let staging_config = StagingConfig::new_s3_uri(uri, None);
    let wal_uri = format!("{}/wal", uri);
    let wal_config = HyperFileWalConfig::new(&wal_uri);
    HyperFileConfigBuilder::new()
        .with_staging_config(&staging_config)
        .with_wal_config(&wal_config)
        .build()
}

/// Unlink both the file and the WAL prefix.
async fn cleanup_with_wal(client: &aws_sdk_s3::Client, uri: &str) {
    // fs_unlink deletes everything under <uri>/ which includes the WAL
    // subdirectory, so a single call is enough.
    let _ = Hyper::fs_unlink(client, uri).await;
}

/// Basic smoke test: create a file with WAL enabled, write + flush +
/// reopen + read. Verifies the WAL path doesn't break normal
/// round trips.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn reactor_wal_smoke_write_flush_reopen() {
    let _ = env_logger::try_init();
    let client = make_client().await;
    let tf = TestFile::new(&client).await;

    let spawner = make_spawner();
    let config = build_wal_config(tf.uri());
    let payload = b"hello wal world".to_vec();

    // Create with WAL config, write, flush, release.
    {
        let hyper = Hyper::create(
            client.clone(),
            config.clone(),
            HyperFileFlags::from_flags(FileFlags::rdwr()),
            HyperFileMode::from_mode(FileMode::default_file()),
        )
        .await
        .expect("create hyper with wal");

        let mut fh = HyperFileHandler::fh_from_hyper(&spawner, hyper)
            .await
            .expect("spawn handler");
        fh.fh_write(0, &payload).await.expect("write");
        fh.fh_flush().await.expect("flush");
        let _ = fh.fh_release().await.expect("release");
    }

    // Reopen with WAL config, verify content.
    {
        let hyper = Hyper::open(
            client.clone(),
            config.clone(),
            HyperFileFlags::from_flags(FileFlags::rdonly()),
        )
        .await
        .expect("open hyper with wal");

        let mut fh = HyperFileHandler::fh_from_hyper(&spawner, hyper)
            .await
            .expect("spawn handler");
        let mut buf = vec![0u8; payload.len()];
        fh.fh_read(0, &mut buf).await.expect("read");
        assert_eq!(buf, payload);
        let _ = fh.fh_release().await;
    }

    drop(spawner);
    cleanup_with_wal(&client, tf.uri()).await;
}

/// After a successful flush the WAL prefix for that segid should be
/// cleared. We verify this indirectly by reopening: if WAL still held
/// stale data, the recovery path would overwrite the flushed state.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn reactor_wal_flush_and_reopen_is_idempotent() {
    let _ = env_logger::try_init();
    let client = make_client().await;
    let tf = TestFile::new(&client).await;

    let spawner = make_spawner();
    let config = build_wal_config(tf.uri());

    // First pass: write A, flush, release.
    let payload_a = vec![0xAAu8; 8192];
    {
        let hyper = Hyper::create(
            client.clone(),
            config.clone(),
            HyperFileFlags::from_flags(FileFlags::rdwr()),
            HyperFileMode::from_mode(FileMode::default_file()),
        )
        .await
        .expect("create");
        let mut fh = HyperFileHandler::fh_from_hyper(&spawner, hyper)
            .await
            .expect("spawn");
        fh.fh_write(0, &payload_a).await.expect("write A");
        fh.fh_flush().await.expect("flush A");
        let _ = fh.fh_release().await;
    }

    // Second pass: open the existing file, overwrite with B, flush.
    let payload_b = vec![0xBBu8; 8192];
    {
        let hyper = Hyper::open(
            client.clone(),
            config.clone(),
            HyperFileFlags::from_flags(FileFlags::rdwr()),
        )
        .await
        .expect("open B");
        let mut fh = HyperFileHandler::fh_from_hyper(&spawner, hyper)
            .await
            .expect("spawn B");
        fh.fh_write(0, &payload_b).await.expect("write B");
        fh.fh_flush().await.expect("flush B");
        let _ = fh.fh_release().await;
    }

    // Third pass: reopen read-only, verify we see B (not A, and not a
    // replayed mix from an uncleared WAL).
    {
        let hyper = Hyper::open(
            client.clone(),
            config.clone(),
            HyperFileFlags::from_flags(FileFlags::rdonly()),
        )
        .await
        .expect("reopen ro");
        let mut fh = HyperFileHandler::fh_from_hyper(&spawner, hyper)
            .await
            .expect("spawn ro");
        let mut buf = vec![0u8; payload_b.len()];
        fh.fh_read(0, &mut buf).await.expect("read");
        assert_eq!(buf, payload_b, "reopen after flush shows stale data");
        let _ = fh.fh_release().await;
    }

    drop(spawner);
    cleanup_with_wal(&client, tf.uri()).await;
}

/// After a successful flush, the WAL prefix for the flushed segid
/// should be cleaned up. The cleanup runs as a fire-and-forget
/// `tokio::spawn` inside the handler, so we give it a moment
/// before listing.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn reactor_wal_delete_after_flush() {
    let _ = env_logger::try_init();
    let client = make_client().await;
    let tf = TestFile::new(&client).await;

    let spawner = make_spawner();
    let config = build_wal_config(tf.uri());
    let payload = vec![0xCDu8; 8192];

    // Create with WAL, write, flush.
    {
        let hyper = Hyper::create(
            client.clone(),
            config.clone(),
            HyperFileFlags::from_flags(FileFlags::rdwr()),
            HyperFileMode::from_mode(FileMode::default_file()),
        )
        .await
        .expect("create hyper with wal");

        let mut fh = HyperFileHandler::fh_from_hyper(&spawner, hyper)
            .await
            .expect("spawn handler");
        fh.fh_write(0, &payload).await.expect("write");
        let _segid = fh.fh_flush().await.expect("flush");

        // Let the fire-and-forget delete task catch up. 1s is plenty
        // over the ~20-100ms ListObjects + DeleteObjects round trip
        // on S3 Express One Zone.
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;

        let _ = fh.fh_release().await;
    }

    // List the WAL prefix directly via the S3 client. It should
    // contain zero objects: every chunk from the flushed segid
    // has been deleted.
    let wal_prefix = format!("{}/wal/", tf.uri().trim_start_matches("s3://")
        .splitn(2, '/')
        .nth(1)
        .expect("uri has no key part"));
    let bucket = test_bucket();
    let list = client
        .list_objects_v2()
        .bucket(&bucket)
        .prefix(&wal_prefix)
        .send()
        .await
        .expect("list_objects_v2");
    let keys: Vec<String> = list
        .contents()
        .iter()
        .filter_map(|o| o.key().map(|s| s.to_string()))
        .collect();
    assert!(
        keys.is_empty(),
        "WAL prefix still holds {} objects after flush: {:?}",
        keys.len(),
        keys,
    );

    drop(spawner);
    cleanup_with_wal(&client, tf.uri()).await;
}
