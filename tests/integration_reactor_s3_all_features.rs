//! Reactor + all opt-in features smoke test.
//!
//! Requires `--features "wal range-lock concurrent-segment-build"`.
//! Verifies that the maximum feature combination does not introduce
//! cross-feature conflicts.
//!
//! Run with:
//! ```bash
//! cargo test --features "wal range-lock concurrent-segment-build" \
//!     --test integration_reactor_s3_all_features \
//!     -- --ignored --test-threads=1
//! ```

#![cfg(all(
    feature = "reactor",
    feature = "wal",
    feature = "range-lock",
    feature = "concurrent-segment-build",
))]

#[allow(dead_code)]
mod common;
#[allow(dead_code)]
mod common_reactor;

use common::*;
use common_reactor::*;

use hyperfile::file::fh::HyperFileHandler;
use hyperfile::file::flags::{FileFlags, HyperFileFlags};
use hyperfile::file::mode::{FileMode, HyperFileMode};
use hyperfile::file::hyper::Hyper;
use hyperfile::config::HyperFileConfigBuilder;
use hyperfile::staging::config::StagingConfig;
use hyperfile::wal::config::HyperFileWalConfig;

/// Smoke: reactor + wal + range-lock + concurrent-segment-build
/// together. Two concurrent disjoint writes, flush, reopen, verify
/// persisted content.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn all_features_write_read_round_trip() {
    let _ = env_logger::try_init();
    let client = make_client().await;
    let tf = TestFile::new(&client).await;

    let staging_config = StagingConfig::new_s3_uri(tf.uri(), None);
    let wal_config = HyperFileWalConfig::new(&format!("{}/wal", tf.uri()));
    let config = HyperFileConfigBuilder::new()
        .with_staging_config(&staging_config)
        .with_wal_config(&wal_config)
        .build();

    let reactor = make_reactor();

    {
        let hyper = Hyper::create(
            client.clone(),
            config.clone(),
            HyperFileFlags::from_flags(FileFlags::rdwr()),
            HyperFileMode::from_mode(FileMode::default_file()),
        )
        .await
        .expect("create");
        let fh = HyperFileHandler::fh_from_hyper(&reactor, hyper)
            .await
            .expect("spawn");

        let mut fh_a = fh.clone();
        let mut fh_b = fh.clone();
        drop(fh);

        let writer_a = async move {
            let data = vec![0xAAu8; 4096];
            fh_a.fh_write(0, &data).await.expect("A write");
            fh_a.fh_flush().await.expect("A flush");
            let _ = fh_a.fh_release().await;
        };
        let writer_b = async move {
            let data = vec![0xBBu8; 4096];
            fh_b.fh_write(8192, &data).await.expect("B write");
            fh_b.fh_flush().await.expect("B flush");
            let _ = fh_b.fh_release().await;
        };
        tokio::join!(writer_a, writer_b);
    }

    {
        let hyper = Hyper::open(
            client.clone(),
            config.clone(),
            HyperFileFlags::from_flags(FileFlags::rdonly()),
        )
        .await
        .expect("reopen");
        let mut fh = HyperFileHandler::fh_from_hyper(&reactor, hyper)
            .await
            .expect("spawn");

        let mut buf_a = vec![0u8; 4096];
        fh.fh_read(0, &mut buf_a).await.expect("read A");
        assert!(buf_a.iter().all(|&b| b == 0xAA), "A's range lost");

        let mut buf_b = vec![0u8; 4096];
        fh.fh_read(8192, &mut buf_b).await.expect("read B");
        assert!(buf_b.iter().all(|&b| b == 0xBB), "B's range lost");

        let _ = fh.fh_release().await;
    }

    let _ = Hyper::fs_unlink(&client, tf.uri()).await;
}
