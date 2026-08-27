//! Opening a published checkpoint read-only (`Hyper::open_cno`).
//!
//! Every flush publishes a checkpoint, and the container never overwrites what
//! it published, so each checkpoint remains a whole-file snapshot. This opens
//! one as a second, read-only view while the container carries on — which is
//! what `hypercli file rollback` does *not* do: that publishes an old inode as
//! the current one, moving the container and every reader with it.
//!
//! The tests that matter most here are the ones asserting that a checkpoint
//! view cannot write. It holds a historical inode, so a write reaching storage
//! through it would publish the past as the present.
//!
//! ```bash
//! HYPERFILE_TEST_BUCKET=<your-bucket> HYPERFILE_TEST_REGION=<your-region> \
//!     cargo test --test integration_s3_open_cno -- --ignored --test-threads=1
//! ```

#[allow(dead_code)]
mod common;

use common::*;

use hyperfile::config::HyperFileConfigBuilder;
use hyperfile::file::hyper::Hyper;
use hyperfile::file::flags::{FileFlags, HyperFileFlags};
use hyperfile::file::mode::FileMode;
use hyperfile::staging::config::StagingConfig;

const BLK: usize = 4096;

/// The config `fs_open` would build for this uri.
fn config_for(uri: &str) -> hyperfile::config::HyperFileConfig {
    let staging_config = StagingConfig::new_s3_uri(uri, None);
    HyperFileConfigBuilder::new()
        .with_staging_config(&staging_config)
        .build()
}

async fn open_cno(client: &aws_sdk_s3::Client, uri: &str, cno: u64) -> std::io::Result<Hyper<'static>> {
    Hyper::open_cno(
        client.clone(),
        config_for(uri),
        HyperFileFlags::from_flags(FileFlags::rdonly()),
        cno,
    ).await
}

/// Write `byte` over the first block and flush, returning the checkpoint.
async fn checkpoint(client: &aws_sdk_s3::Client, uri: &str, byte: u8, len: usize) -> u64 {
    let mut h = Hyper::fs_open_or_create_with_default_opt(
        client, uri, FileFlags::rdwr(), FileMode::default_file(),
    ).await.expect("open rw");
    let _ = h.fs_write(0, &vec![byte; len]).await.expect("write");
    let cno = h.fs_flush().await.expect("flush");
    let _ = h.fs_release().await.expect("release");
    cno
}

/// Each checkpoint shows the file as it stood then, not as it stands now.
#[tokio::test]
#[ignore]
async fn each_checkpoint_shows_its_own_contents() {
    let _ = env_logger::try_init();
    let client = make_client().await;
    let tf = TestFile::new(&client).await;

    let c1 = checkpoint(&client, tf.uri(), 0xA1, BLK).await;
    let c2 = checkpoint(&client, tf.uri(), 0xB2, 2 * BLK).await;
    let c3 = checkpoint(&client, tf.uri(), 0xC3, 3 * BLK).await;
    assert!(c1 < c2 && c2 < c3, "checkpoints must advance: {} {} {}", c1, c2, c3);

    for (cno, byte, len) in [(c1, 0xA1u8, BLK), (c2, 0xB2, 2 * BLK), (c3, 0xC3, 3 * BLK)] {
        let mut h = open_cno(&client, tf.uri(), cno).await.expect("open_cno");

        let st = h.fs_getattr().expect("getattr");
        assert_eq!(st.st_size as usize, len, "checkpoint {} should be {} bytes", cno, len);

        let mut buf = vec![0u8; len];
        let n = h.fs_read(0, &mut buf).await.expect("read");
        assert_eq!(n, len);
        assert!(buf.iter().all(|b| *b == byte),
            "checkpoint {} should read back {:#x}", cno, byte);

        let _ = h.fs_release().await.expect("release");
    }

    tf.cleanup(&client).await;
}

/// The live container carries on while a checkpoint is open, and the checkpoint
/// does not move. This is the point of the whole thing.
#[tokio::test]
#[ignore]
async fn a_checkpoint_view_does_not_move_while_the_container_advances() {
    let _ = env_logger::try_init();
    let client = make_client().await;
    let tf = TestFile::new(&client).await;

    let old = checkpoint(&client, tf.uri(), 0xA1, BLK).await;

    let mut view = open_cno(&client, tf.uri(), old).await.expect("open_cno");
    let mut before = vec![0u8; BLK];
    let _ = view.fs_read(0, &mut before).await.expect("read");
    assert!(before.iter().all(|b| *b == 0xA1));

    // Two more checkpoints, written and published while the view is open.
    let mut live = Hyper::fs_open_or_create_with_default_opt(
        &client, tf.uri(), FileFlags::rdwr(), FileMode::default_file(),
    ).await.expect("open rw");
    let _ = live.fs_write(0, &vec![0xD4; 4 * BLK]).await.expect("write");
    let _ = live.fs_flush().await.expect("flush");
    let _ = live.fs_write(0, &vec![0xE5; 5 * BLK]).await.expect("write");
    let _ = live.fs_flush().await.expect("flush");

    // The view still reads what it read before.
    let mut after = vec![0u8; BLK];
    let _ = view.fs_read(0, &mut after).await.expect("read");
    assert_eq!(after, before, "the checkpoint view moved");
    let st = view.fs_getattr().expect("getattr");
    assert_eq!(st.st_size as usize, BLK, "the checkpoint view's size moved");

    // And the live handle sees the latest.
    let live_st = live.fs_getattr().expect("getattr");
    assert_eq!(live_st.st_size as usize, 5 * BLK);

    let _ = view.fs_release().await.expect("release view");
    let _ = live.fs_release().await.expect("release live");
    tf.cleanup(&client).await;
}

/// A checkpoint view cannot write. It holds a historical inode, so anything
/// reaching storage through it would publish the past as the present.
#[tokio::test]
#[ignore]
async fn a_checkpoint_view_cannot_write() {
    let _ = env_logger::try_init();
    let client = make_client().await;
    let tf = TestFile::new(&client).await;

    let old = checkpoint(&client, tf.uri(), 0xA1, BLK).await;
    let latest = checkpoint(&client, tf.uri(), 0xB2, 7 * BLK).await;

    {
        let mut h = open_cno(&client, tf.uri(), old).await.expect("open_cno");

        assert!(h.fs_write(0, &[0xFF; BLK]).await.is_err(), "fs_write succeeded");
        assert!(h.fs_write_zero(0, BLK).await.is_err(), "fs_write_zero succeeded");
        assert!(h.fs_truncate(0).await.is_err(), "fs_truncate succeeded");
        assert!(h.fs_block_mut(0, false).await.is_err(), "fs_block_mut succeeded");
        assert!(h.fs_chmod(0o600).await.is_err(), "fs_chmod succeeded");

        // Reading is of course fine.
        let mut buf = vec![0u8; BLK];
        let _ = h.fs_read(0, &mut buf).await.expect("read");
        assert!(buf.iter().all(|b| *b == 0xA1));

        let _ = h.fs_release().await.expect("release");
    }

    // Nothing the view did — including its release — published anything: the
    // container's current state is still the latest checkpoint.
    let mut h = Hyper::fs_open(&client, tf.uri(), FileFlags::rdonly()).await.expect("reopen");
    let st = h.fs_getattr().expect("getattr");
    assert_eq!(st.st_size as usize, 7 * BLK, "the container moved back to the checkpoint");
    assert_eq!(h.fs_last_cno(), latest, "the published checkpoint changed");
    let mut buf = vec![0u8; 7 * BLK];
    let _ = h.fs_read(0, &mut buf).await.expect("read");
    assert!(buf.iter().all(|b| *b == 0xB2), "container contents changed");
    let _ = h.fs_release().await.expect("release");

    tf.cleanup(&client).await;
}

/// Write access is refused up front rather than failing later, per operation.
#[tokio::test]
#[ignore]
async fn opening_a_checkpoint_for_write_is_refused() {
    let _ = env_logger::try_init();
    let client = make_client().await;
    let tf = TestFile::new(&client).await;

    let cno = checkpoint(&client, tf.uri(), 0xA1, BLK).await;

    for flags in [FileFlags::rdwr(), FileFlags::wronly()] {
        let shown = format!("{}", flags);
        let res = Hyper::open_cno(
            client.clone(), config_for(tf.uri()),
            HyperFileFlags::from_flags(flags), cno).await;
        match res {
            Ok(_) => panic!("open_cno accepted write access with {}", shown),
            Err(e) => assert_eq!(e.kind(), std::io::ErrorKind::ReadOnlyFilesystem,
                "expected ReadOnlyFilesystem for {}, got {:?}", shown, e.kind()),
        }
    }

    tf.cleanup(&client).await;
}

/// A checkpoint that was never published is an error, not a panic and not an
/// empty file.
#[tokio::test]
#[ignore]
async fn an_unpublished_checkpoint_is_an_error() {
    let _ = env_logger::try_init();
    let client = make_client().await;
    let tf = TestFile::new(&client).await;

    let cno = checkpoint(&client, tf.uri(), 0xA1, BLK).await;

    match open_cno(&client, tf.uri(), cno + 1000).await {
        Ok(_) => panic!("open_cno accepted a checkpoint that was never published"),
        Err(e) => assert!(
            e.kind() == std::io::ErrorKind::NotFound || e.kind() == std::io::ErrorKind::InvalidData,
            "expected NotFound or InvalidData, got {:?}: {}", e.kind(), e),
    }

    tf.cleanup(&client).await;
}
