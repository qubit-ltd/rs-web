// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Buffers a JSON body under its byte budget.

use axum::body::Body;
use axum::body::Bytes;
use http_body_util::BodyExt;
use http_body_util::Limited;
use hyper::body::Body as HttpBody;

use crate::json::JsonRejection;

/// Buffers a JSON body while reusing a single data frame when it ends the body.
///
/// All other data frames are copied into a growing vector and released before
/// polling the body again. The byte budget applies to JSON payload bytes; it
/// does not bound allocation capacity or process memory.
///
/// # Parameters
///
/// - `body`: request body to buffer.
/// - `maximum`: maximum number of JSON payload bytes to accept.
///
/// # Returns
///
/// The buffered JSON bytes, or a rejection describing a body read or budget
/// failure.
pub(in crate::json) async fn buffer_json_body(body: Body, maximum: usize) -> Result<Bytes, JsonRejection> {
    let mut body = Limited::new(body, maximum);
    let mut buffer = Vec::new();
    let mut first = true;
    while let Some(frame) = body.frame().await {
        let frame = frame.map_err(|error| {
            if error.is::<http_body_util::LengthLimitError>() {
                JsonRejection::BudgetExceeded
            } else {
                JsonRejection::BodyReadFailed
            }
        })?;
        if let Ok(data) = frame.into_data() {
            if first && body.is_end_stream() {
                return Ok(data);
            }
            buffer.extend_from_slice(&data);
        }
        first = false;
    }
    Ok(Bytes::from(buffer))
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::io;
    use std::pin::Pin;
    use std::sync::Arc;
    use std::sync::atomic::AtomicBool;
    use std::sync::atomic::Ordering;
    use std::task::Context;
    use std::task::Poll;

    use axum::body::Body;
    use axum::body::Bytes;
    use axum::http::HeaderMap;
    use hyper::body::Body as HttpBody;

    use super::buffer_json_body;

    enum Frame {
        Data(Bytes),
        Trailers,
        Error,
    }

    struct FramesBody(VecDeque<Frame>);

    impl HttpBody for FramesBody {
        type Data = Bytes;
        type Error = io::Error;

        fn poll_frame(
            mut self: Pin<&mut Self>,
            _: &mut Context<'_>,
        ) -> Poll<Option<Result<hyper::body::Frame<Self::Data>, Self::Error>>> {
            let Some(frame) = self.0.pop_front() else {
                return Poll::Ready(None);
            };
            Poll::Ready(Some(match frame {
                Frame::Data(data) => Ok(hyper::body::Frame::data(data)),
                Frame::Trailers => Ok(hyper::body::Frame::trailers(HeaderMap::new())),
                Frame::Error => Err(io::Error::other("scripted body error")),
            }))
        }
    }

    struct TrackedOwner {
        data: Vec<u8>,
        dropped: Arc<AtomicBool>,
    }

    struct DropBeforeNextPollBody {
        state: u8,
        dropped: Arc<AtomicBool>,
    }

    impl HttpBody for DropBeforeNextPollBody {
        type Data = Bytes;
        type Error = io::Error;

        fn poll_frame(
            mut self: Pin<&mut Self>,
            _: &mut Context<'_>,
        ) -> Poll<Option<Result<hyper::body::Frame<Self::Data>, Self::Error>>> {
            if self.state == 0 {
                self.state = 1;
                let owner = TrackedOwner {
                    data: b"4".to_vec(),
                    dropped: self.dropped.clone(),
                };
                return Poll::Ready(Some(Ok(hyper::body::Frame::data(Bytes::from_owner(owner)))));
            }

            assert!(self.dropped.load(Ordering::SeqCst));
            self.state = 2;
            Poll::Ready(None)
        }

        fn is_end_stream(&self) -> bool {
            self.state == 2
        }
    }

    impl AsRef<[u8]> for TrackedOwner {
        fn as_ref(&self) -> &[u8] {
            &self.data
        }
    }

    impl Drop for TrackedOwner {
        fn drop(&mut self) {
            self.dropped.store(true, Ordering::SeqCst);
        }
    }

    #[tokio::test]
    async fn test_reuses_bytes_allocation_for_one_complete_data_frame() {
        let input = Bytes::from_static(b"{\"answer\":42}");
        let input_ptr = input.as_ptr();
        let input_len = input.len();

        let output = buffer_json_body(Body::from(input), input_len).await.unwrap();

        assert_eq!(output.as_ptr(), input_ptr);
        assert_eq!(output.len(), input_len);
        assert_eq!(&output[..], b"{\"answer\":42}");
    }

    #[tokio::test]
    async fn test_empty_body_returns_empty_bytes() {
        assert!(buffer_json_body(Body::empty(), 0).await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn test_empty_first_frame_then_data_uses_copy_path() {
        let data = Bytes::from_static(b"42");
        let data_ptr = data.as_ptr();
        let body = Body::new(FramesBody(VecDeque::from([
            Frame::Data(Bytes::new()),
            Frame::Data(data),
        ])));

        let output = buffer_json_body(body, 2).await.unwrap();

        assert_eq!(&output[..], b"42");
        assert_ne!(output.as_ptr(), data_ptr);
    }

    #[tokio::test]
    async fn test_single_frame_exactly_at_limit_is_accepted() {
        let output = buffer_json_body(Body::from("42"), 2).await.unwrap();
        assert_eq!(&output[..], b"42");
    }

    #[tokio::test]
    async fn test_single_frame_over_limit_is_rejected() {
        let result = buffer_json_body(Body::from("42"), 1).await;
        assert!(matches!(result, Err(crate::json::JsonRejection::BudgetExceeded)));
    }

    #[tokio::test]
    async fn test_multiframe_cumulative_over_limit_is_rejected() {
        let body = Body::new(FramesBody(VecDeque::from([
            Frame::Data(Bytes::from_static(b"1")),
            Frame::Data(Bytes::from_static(b"2")),
        ])));
        let result = buffer_json_body(body, 1).await;
        assert!(matches!(result, Err(crate::json::JsonRejection::BudgetExceeded)));
    }

    #[tokio::test]
    async fn test_trailers_do_not_hide_a_later_body_read_error() {
        let body = Body::new(FramesBody(VecDeque::from([
            Frame::Data(Bytes::from_static(b"42")),
            Frame::Trailers,
            Frame::Error,
        ])));
        let result = buffer_json_body(body, 2).await;
        assert!(matches!(result, Err(crate::json::JsonRejection::BodyReadFailed)));
    }

    #[tokio::test]
    async fn test_prior_frame_owner_is_dropped_before_next_poll() {
        let dropped = Arc::new(AtomicBool::new(false));
        let body = Body::new(DropBeforeNextPollBody {
            state: 0,
            dropped: dropped.clone(),
        });

        let output = buffer_json_body(body, 1).await.unwrap();

        assert_eq!(&output[..], b"4");
        assert!(dropped.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn test_single_frame_keeps_large_backing_allocation_until_drop() {
        let dropped = Arc::new(AtomicBool::new(false));
        let backing = Bytes::from_owner(TrackedOwner {
            data: vec![b'x'; 1024 * 1024],
            dropped: dropped.clone(),
        });
        let input = backing.slice(0..8);
        let input_ptr = input.as_ptr();
        drop(backing);
        assert!(!dropped.load(Ordering::SeqCst));

        let output = buffer_json_body(Body::from(input), 8).await.unwrap();

        assert_eq!(output.as_ptr(), input_ptr);
        assert_eq!(output.len(), 8);
        assert!(!dropped.load(Ordering::SeqCst));
        drop(output);
        assert!(dropped.load(Ordering::SeqCst));
    }
}
