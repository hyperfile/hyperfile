mod settings;
use std::io::{Result, ErrorKind};
use std::time::Instant;
use rand::Rng;
use aws_sdk_s3::Client;
use human_bytes::human_bytes;
use hyperfile::file::hyper::Hyper;
use hyperfile::file::flags::FileFlags;
use settings::*;

async fn random_write(client: &Client, uri: &str, data: &mut Vec<u8>) -> Result<()> {
    let rand_data_offset = rand::random_range(DEFAULT_RANDOM_WRITE_OFFSET);
    let mut rand_data_bytes = rand::random_range(DEFAULT_RANDOM_WRITE_BYTES);
    if rand_data_offset + rand_data_bytes >= DEFAULT_MAX_FILE_SIZE {
        rand_data_bytes = DEFAULT_MAX_FILE_SIZE - rand_data_offset;
    }
    let total_len = rand_data_offset + rand_data_bytes;
    // only resize buffer for data extend case
    if total_len > data.len() {
        data.resize(total_len, 0);
    }
    print!("random write offset {} bytes {} ..", rand_data_offset, rand_data_bytes);

    let flags = FileFlags::rdwr();
    let mut hyper = Hyper::fs_open_or_create(client, uri, flags).await?;
    // write content
    let filled_buf = &mut data[rand_data_offset..total_len];
    let write_bytes = hyper.fs_write(rand_data_offset, filled_buf).await?;
    assert!(write_bytes == rand_data_bytes);
    let last_cno = hyper.fs_release().await?;
    println!(". Done with last cno {}", last_cno);
    Ok(())
}

async fn random_truncate(client: &Client, uri: &str, data: &mut Vec<u8>) -> Result<()> {
    let curr_len = data.len();
    // ratio to truncate only one byte
    let new_file_len = if curr_len == 0 {
        rand::random_range(0..DEFAULT_MAX_FILE_SIZE)
    } else if rand::rng().random_ratio(2, 10) {
        curr_len - 1
    } else if rand::rng().random_ratio(2, 10) {
        curr_len + 1
    } else {
        rand::random_range(0..DEFAULT_MAX_FILE_SIZE)
    };
    print!("random truncate file to new size {} ..", new_file_len);
    // open file for read
    let flags = FileFlags::rdwr();
    let mut hyper = Hyper::fs_open_or_create(&client, &uri, flags).await?;
    // get stat
    let stat = hyper.fs_getattr()?;
    assert!(stat.st_size == data.len() as i64);
    if new_file_len > stat.st_size as usize {
        print!(".[EXPEND]..");
    } else if new_file_len < stat.st_size as usize {
        print!(".[SHRINK]..");
    } else {
        print!(".[NO CHANGE]..");
    }

    // extend file by truncate
    hyper.fs_truncate(new_file_len).await?;
    let last_cno = hyper.fs_release().await?;
    data.resize(new_file_len, 0);
    println!(". Done with last cno {}", last_cno);
    Ok(())
}

async fn read_check(client: &Client, uri: &str, data: &mut Vec<u8>) -> Result<()> {
    let total_bytes = data.len();
    // open file for read
    let flags = FileFlags::rdonly();
    let mut hyper = Hyper::fs_open_or_create(client, uri, flags).await?;
    // get stat
    let stat = hyper.fs_getattr()?;
    assert!(stat.st_size == total_bytes as i64);

    let mut buf = Vec::new();
    buf.resize(total_bytes, 0);
    let start = Instant::now();
    let read_bytes = hyper.fs_read(0, &mut buf).await?;
    let millis = start.elapsed().as_millis();
    println!("real data in memory md5: {:?} - read data from file md5: {:?}", md5::compute(&data), md5::compute(&buf));
    assert!(read_bytes == total_bytes);
    let throughput = (read_bytes as u128 * 1000) / millis;
    println!("bytes read {} throughput {}/s", human_bytes(read_bytes as f64), human_bytes(throughput as f64));
    let _last_cno = hyper.fs_release().await?;
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();
	let uri = std::env::var("HYPERFILE_STAGING_ROOT_URI").unwrap();

    // build s3 client
	let config = aws_config::load_from_env().await;
    let client = aws_sdk_s3::Client::new(&config);

    // clear any existing on staging
    let _ = Hyper::fs_unlink(&client, &uri).await
                        .or_else(|e| {
                            if e.kind() == ErrorKind::NotFound {
                                return Ok(());
                            }
                            Err(e)
                        })?;

    let mut data = Vec::new();

    for _ in 0..NUM_ITER {
        if rand::rng().random_ratio(7, 10) {
            random_write(&client, &uri, &mut data).await?;
        } else {
            random_truncate(&client, &uri, &mut data).await?;
        }
    }

    println!("Final read check...");
    read_check(&client, &uri, &mut data).await?;

    Ok(())
}
