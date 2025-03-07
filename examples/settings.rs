use std::ops::Range;

pub(crate) const NUM_ITER: usize = 100;
pub(crate) const DEFAULT_BLOCK_SIZE: usize = 4096;
pub(crate) const DEFAULT_MAX_FILE_SIZE: usize = 50 * 1024 * 1024; // 50MiB
pub(crate) const DEFAULT_RANDOM_WRITE_OFFSET: Range<usize> = 0..DEFAULT_MAX_FILE_SIZE;
pub(crate) const DEFAULT_RANDOM_WRITE_BYTES: Range<usize> = 1..DEFAULT_BLOCK_SIZE*1024; // 1Byte to 4MiB
