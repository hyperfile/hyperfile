use std::borrow::Cow;
use std::error::Error;
use std::fmt::{Display, Formatter};

#[derive(Debug, PartialEq)]
pub struct InvalidS3Uri {
    message: Cow<'static, str>,
}

impl InvalidS3Uri {
    fn from_static(message: &'static str) -> InvalidS3Uri {
        Self { message: Cow::Borrowed(message), }
    }
}

impl Display for InvalidS3Uri {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.message)
    }
}

impl Error for InvalidS3Uri {}

#[derive(Debug, Eq, PartialEq)]
pub struct S3Uri<'a> {
    pub bucket: &'a str,
    pub key: &'a str,
}

impl<'a> S3Uri<'a> {
    pub fn parse(uri: &'a str) -> Result<Self, InvalidS3Uri> {
        if !uri.starts_with("s3://") && !uri.starts_with("S3://") {
            return Err(InvalidS3Uri::from_static("S3 Uri must start with s3:// or S3://"));
        }
        if let Some((bucket, key)) = uri[5..].split_once('/') {
            if key == "" {
                return Err(InvalidS3Uri::from_static("Missing key from S3 Uri"));
            }
            return Ok(Self {
                    bucket: bucket,
                    key: key,
            });
        }
        return Err(InvalidS3Uri::from_static("Incomplete S3 Uri"));
    }
}

