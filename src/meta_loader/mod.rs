#[cfg(not(feature = "meta_loader_batch"))]
pub mod s3;
#[cfg(feature = "meta_loader_batch")]
pub mod s3_batch;
#[cfg(feature = "meta_loader_batch")]
pub use s3_batch as s3;
