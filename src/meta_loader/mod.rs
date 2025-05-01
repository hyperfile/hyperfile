#[cfg(not(feature = "meta_loader_batch"))]
pub(crate) mod s3;
#[cfg(feature = "meta_loader_batch")]
pub(crate) mod s3_batch;
#[cfg(feature = "meta_loader_batch")]
pub(crate) use s3_batch as s3;
