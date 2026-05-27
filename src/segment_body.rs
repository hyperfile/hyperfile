//! Scatter-gather body for segment uploads.
//!
//! `SegmentBody` is the data-plane analogue of `Vec<u8>` for a flush
//! segment, except the bytes never live in one contiguous allocation.
//! It accumulates `Bytes` pieces — one for the segment header /
//! summary, one per dirty meta block, one per dirty data block — and
//! produces an `aws_smithy_types::body::SdkBody` that streams those
//! pieces directly to the S3 SDK without a final memcpy into a
//! single buffer.
//!
//! Why this matters: with the previous `Vec<u8>` segment buffer,
//! every flush of a *D*-byte payload allocated a contiguous
//! `segment_buffer_size` (typically 256 MiB or 1 GiB) buffer up
//! front, regardless of *D*. The actual data was then memcpy'd in,
//! and the AWS SDK's `SdkBody::from(&[u8])` did a second copy via
//! `Bytes::copy_from_slice`. Memory peak during flush was
//! `segment_buffer_size + payload`, dominated by the upfront
//! allocation.
//!
//! With `SegmentBody`, the only allocation is the sum of the
//! pieces themselves (≈ payload + a few KiB of header). The SDK
//! pulls one frame at a time via `http_body::Body::poll_data`, so
//! it does not need to materialize the full body in its own
//! buffers either.

use std::convert::Infallible;
use std::pin::Pin;
use std::task::{Context, Poll};

use bytes::Bytes;
use http::HeaderMap;
use http_body::{Body, SizeHint};

/// A growable list of `Bytes` pieces that together form the body
/// of one S3 segment. Push pieces in upload order; cheap to clone
/// (each piece is refcounted).
#[derive(Clone, Default)]
pub struct SegmentBody {
    pieces: Vec<Bytes>,
    total_len: usize,
}

impl SegmentBody {
    pub(crate) fn new() -> Self {
        Self {
            pieces: Vec::new(),
            total_len: 0,
        }
    }

    pub(crate) fn with_capacity(n_pieces: usize) -> Self {
        Self {
            pieces: Vec::with_capacity(n_pieces),
            total_len: 0,
        }
    }

    pub(crate) fn push(&mut self, b: Bytes) {
        self.total_len += b.len();
        self.pieces.push(b);
    }

    /// Total payload length across all pieces. Equivalent to the
    /// final segment file size on S3.
    pub fn len(&self) -> usize {
        self.total_len
    }

    pub fn is_empty(&self) -> bool {
        self.total_len == 0
    }

    pub(crate) fn pieces(&self) -> &[Bytes] {
        &self.pieces
    }

    /// Take a sub-body covering byte range `[start, start + len)`
    /// of the concatenation of pieces, walking them and slicing
    /// individual `Bytes` at the boundaries (each slice is a
    /// refcounted view, no copy).
    ///
    /// Used by the multipart-upload path to slice the full body
    /// into part-sized sub-bodies.
    pub(crate) fn slice(&self, start: usize, len: usize) -> Self {
        assert!(
            start + len <= self.total_len,
            "SegmentBody::slice out of range: start={} len={} total={}",
            start, len, self.total_len,
        );
        if len == 0 {
            return Self::new();
        }
        let mut out = Self::with_capacity(self.pieces.len());
        let mut cursor = 0usize;
        let mut remaining = len;
        for piece in &self.pieces {
            if remaining == 0 {
                break;
            }
            let piece_start = cursor;
            let piece_end = cursor + piece.len();
            cursor = piece_end;

            // Skip pieces entirely before `start`.
            if piece_end <= start {
                continue;
            }
            // Trim the head of the first overlapping piece.
            let piece_off = if piece_start < start {
                start - piece_start
            } else {
                0
            };
            let avail = piece.len() - piece_off;
            let take = avail.min(remaining);
            let chunk = piece.slice(piece_off..piece_off + take);
            out.push(chunk);
            remaining -= take;
        }
        out
    }

    /// Convert into an SdkBody that streams the pieces one frame
    /// at a time. The body is wrapped in `SdkBody::retryable` so
    /// the SDK can rebuild a fresh body for retries (cloning the
    /// `Vec<Bytes>` is cheap — each piece is refcounted).
    pub(crate) fn into_sdk_body(self) -> aws_smithy_types::body::SdkBody {
        let pieces = self.pieces;
        aws_smithy_types::body::SdkBody::retryable(move || {
            aws_smithy_types::body::SdkBody::from_body_0_4(ScatterBody::new(pieces.clone()))
        })
    }
}

/// `http_body::Body` implementation backing `SegmentBody`. One
/// `Bytes` is yielded per `poll_data` call.
struct ScatterBody {
    iter: std::vec::IntoIter<Bytes>,
    remaining: u64,
}

impl ScatterBody {
    fn new(pieces: Vec<Bytes>) -> Self {
        let remaining: u64 = pieces.iter().map(|b| b.len() as u64).sum();
        Self {
            iter: pieces.into_iter(),
            remaining,
        }
    }
}

impl Body for ScatterBody {
    type Data = Bytes;
    type Error = Infallible;

    fn poll_data(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Bytes, Self::Error>>> {
        let me = self.get_mut();
        match me.iter.next() {
            Some(b) => {
                me.remaining = me.remaining.saturating_sub(b.len() as u64);
                Poll::Ready(Some(Ok(b)))
            }
            None => Poll::Ready(None),
        }
    }

    fn poll_trailers(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
    ) -> Poll<Result<Option<HeaderMap>, Self::Error>> {
        Poll::Ready(Ok(None))
    }

    fn size_hint(&self) -> SizeHint {
        SizeHint::with_exact(self.remaining)
    }

    fn is_end_stream(&self) -> bool {
        self.remaining == 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn segment_body_len_tracks_pieces() {
        let mut b = SegmentBody::new();
        assert_eq!(b.len(), 0);
        assert!(b.is_empty());
        b.push(Bytes::from_static(b"hello "));
        b.push(Bytes::from_static(b"world"));
        assert_eq!(b.len(), 11);
        assert!(!b.is_empty());
        assert_eq!(b.pieces().len(), 2);
    }

    #[test]
    fn segment_body_slice_within_one_piece() {
        let mut b = SegmentBody::new();
        b.push(Bytes::from_static(b"abcdefgh"));
        let s = b.slice(2, 4);
        assert_eq!(s.len(), 4);
        assert_eq!(s.pieces()[0].as_ref(), b"cdef");
    }

    #[test]
    fn segment_body_slice_across_pieces() {
        let mut b = SegmentBody::new();
        b.push(Bytes::from_static(b"abc"));
        b.push(Bytes::from_static(b"defghi"));
        b.push(Bytes::from_static(b"jkl"));
        // request bytes 1..10 = "bcdefghij"
        let s = b.slice(1, 9);
        let collected: Vec<u8> = s.pieces().iter().flat_map(|p| p.iter().copied()).collect();
        assert_eq!(collected.as_slice(), b"bcdefghij");
        assert_eq!(s.len(), 9);
    }

    #[test]
    fn segment_body_slice_full() {
        let mut b = SegmentBody::new();
        b.push(Bytes::from_static(b"abc"));
        b.push(Bytes::from_static(b"def"));
        let s = b.slice(0, 6);
        assert_eq!(s.len(), 6);
    }

    #[test]
    fn segment_body_slice_empty() {
        let mut b = SegmentBody::new();
        b.push(Bytes::from_static(b"abc"));
        let s = b.slice(2, 0);
        assert_eq!(s.len(), 0);
        assert!(s.is_empty());
    }

    #[test]
    #[should_panic(expected = "SegmentBody::slice out of range")]
    fn segment_body_slice_panics_out_of_range() {
        let mut b = SegmentBody::new();
        b.push(Bytes::from_static(b"abc"));
        let _ = b.slice(2, 5);
    }

    #[tokio::test]
    async fn scatter_body_polls_pieces_in_order() {
        let pieces = vec![
            Bytes::from_static(b"part-1"),
            Bytes::from_static(b"part-2"),
            Bytes::from_static(b"part-3"),
        ];
        let body = ScatterBody::new(pieces);
        let mut body = Box::pin(body);
        let mut collected = Vec::new();
        while let Some(frame) = std::future::poll_fn(|cx| body.as_mut().poll_data(cx)).await {
            let b = frame.expect("infallible");
            collected.extend_from_slice(&b);
        }
        assert_eq!(&collected, b"part-1part-2part-3");
    }
}
