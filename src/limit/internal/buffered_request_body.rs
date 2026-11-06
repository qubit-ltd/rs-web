// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Replays a buffered request body and its optional trailers.

use std::convert::Infallible;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;

use axum::body::Bytes;
use axum::http::HeaderMap;
use hyper::body::Body;
use hyper::body::Frame;
use hyper::body::SizeHint;

/// Buffered data followed by its original trailers.
pub(in crate::limit) struct BufferedRequestBody {
    /// The buffered data frame until it is consumed; `None` means it was
    /// consumed, and empty data is skipped.
    data: Option<Bytes>,
    /// The trailers frame until it is consumed after the data frame; `None`
    /// means the trailers were consumed.
    trailers: Option<HeaderMap>,
}

impl BufferedRequestBody {
    /// Creates a body that replays buffered data followed by its trailers.
    ///
    /// # Parameters
    ///
    /// - `data`: The buffered data bytes to replay before the trailers.
    /// - `trailers`: The trailers to replay after the data.
    ///
    /// # Returns
    ///
    /// A body containing the data and trailers for replay.
    pub(in crate::limit) fn new(data: Bytes, trailers: HeaderMap) -> Self {
        Self {
            data: Some(data),
            trailers: Some(trailers),
        }
    }
}

impl Body for BufferedRequestBody {
    /// Buffered data frame payload type.
    type Data = Bytes;
    /// This replay body does not produce errors.
    type Error = Infallible;

    /// Returns the next buffered frame, skipping empty data and replaying
    /// trailers after data has been consumed.
    ///
    /// # Returns
    ///
    /// A data frame, then a trailers frame, or `Poll::Ready(None)` after both
    /// have been consumed. This body returns a ready result on every call.
    /// # Parameters
    ///
    /// * `self` - Pinned body whose next buffered frame is requested.
    /// * `_` - Task context used to register the current waker.
    fn poll_frame(
        mut self: Pin<&mut Self>,
        _: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        if let Some(data) = self.data.take()
            && !data.is_empty()
        {
            return Poll::Ready(Some(Ok(Frame::data(data))));
        }
        if let Some(trailers) = self.trailers.take() {
            return Poll::Ready(Some(Ok(Frame::trailers(trailers))));
        }
        Poll::Ready(None)
    }

    /// Reports whether both the buffered data and trailers have been consumed.
    fn is_end_stream(&self) -> bool {
        self.data.is_none() && self.trailers.is_none()
    }

    /// Returns an exact size hint for the remaining data bytes, excluding
    /// trailers. A zero size hint does not imply end of stream because trailers
    /// may still remain.
    fn size_hint(&self) -> SizeHint {
        let remaining = self.data.as_ref().map_or(0, Bytes::len) as u64;
        SizeHint::with_exact(remaining)
    }
}

#[cfg(test)]
mod tests {
    use axum::body::Bytes;
    use axum::http::HeaderMap;
    use http_body_util::BodyExt;
    use hyper::body::Body;

    use super::BufferedRequestBody;

    #[tokio::test]
    async fn test_replays_data_then_trailers_and_tracks_remaining_data_size() {
        let mut trailers = HeaderMap::new();
        trailers.insert("x-check", "preserved".parse().unwrap());
        let mut body = BufferedRequestBody::new(Bytes::from_static(b"abc"), trailers);
        assert_eq!(body.size_hint().exact(), Some(3));
        assert!(!body.is_end_stream());
        let frame = body.frame().await.unwrap().unwrap();
        assert_eq!(frame.into_data().unwrap(), Bytes::from_static(b"abc"));
        assert_eq!(body.size_hint().exact(), Some(0));
        assert!(!body.is_end_stream());
        let frame = body.frame().await.unwrap().unwrap();
        assert_eq!(frame.into_trailers().unwrap()["x-check"], "preserved");
        assert!(body.is_end_stream());
        assert!(body.frame().await.is_none());
    }

    #[tokio::test]
    async fn test_replays_trailers_without_data_and_excludes_them_from_size_hint() {
        let mut trailers = HeaderMap::new();
        trailers.insert("x-check", "only".parse().unwrap());
        let mut body = BufferedRequestBody::new(Bytes::new(), trailers);
        assert_eq!(body.size_hint().exact(), Some(0));
        assert!(!body.is_end_stream());
        assert!(body.frame().await.unwrap().unwrap().is_trailers());
        assert!(body.is_end_stream());
        assert!(body.frame().await.is_none());
    }
}
