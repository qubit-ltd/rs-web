// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Internal request and response body wrappers for request limits.

use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;

use axum::body::Body;
use axum::body::Bytes;

/// Request body wrapper that rejects data after the configured byte budget.
struct LimitedRequestBody {
    /// Underlying body polled by the extractor.
    inner: Body,
    /// Maximum total data-frame size accepted.
    max_bytes: usize,
    /// Bytes already delivered to the extractor.
    consumed_bytes: usize,
    /// Shared signal used to turn extractor failures into a stable rejection.
    exceeded: Arc<AtomicBool>,
}

impl hyper::body::Body for LimitedRequestBody {
    type Data = Bytes;
    type Error = std::io::Error;

    /// Polls one frame and rejects data frames that exceed the cumulative
    /// budget.
    ///
    /// # Parameters
    ///
    /// - `context`: task context used to register the next body wake-up.
    ///
    /// # Returns
    ///
    /// The next body frame, an I/O rejection, end of stream, or `Pending`.
    fn poll_frame(
        mut self: Pin<&mut Self>,
        context: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Result<hyper::body::Frame<Self::Data>, Self::Error>>> {
        let this = self.as_mut().get_mut();
        if this.exceeded.load(Ordering::Acquire) {
            return std::task::Poll::Ready(None);
        }
        match Pin::new(&mut this.inner).poll_frame(context) {
            std::task::Poll::Ready(Some(Ok(frame))) => match frame.into_data() {
                Ok(data) => {
                    let next_size = this.consumed_bytes.saturating_add(data.len());
                    if next_size > this.max_bytes {
                        this.exceeded.store(true, Ordering::Release);
                        std::task::Poll::Ready(Some(Err(std::io::Error::new(
                            std::io::ErrorKind::InvalidData,
                            "request body exceeds configured limit",
                        ))))
                    } else {
                        this.consumed_bytes = next_size;
                        std::task::Poll::Ready(Some(Ok(hyper::body::Frame::data(data))))
                    }
                }
                Err(trailers) => std::task::Poll::Ready(Some(Ok(trailers))),
            },
            std::task::Poll::Ready(Some(Err(error))) => std::task::Poll::Ready(Some(Err(std::io::Error::other(error)))),
            std::task::Poll::Ready(None) => std::task::Poll::Ready(None),
            std::task::Poll::Pending => std::task::Poll::Pending,
        }
    }

    /// Reports whether the body ended or the byte budget was exceeded.
    ///
    /// # Returns
    ///
    /// `true` when no further frames can be delivered.
    fn is_end_stream(&self) -> bool {
        self.exceeded.load(Ordering::Acquire) || self.inner.is_end_stream()
    }

    /// Preserves the underlying body's size estimate for downstream extractors.
    ///
    /// # Returns
    ///
    /// The underlying body's current size hint.
    fn size_hint(&self) -> hyper::body::SizeHint {
        self.inner.size_hint()
    }
}

/// Wraps a request body with streamed byte accounting.
///
/// # Parameters
///
/// - `inner`: body whose data frames are checked.
/// - `max_bytes`: cumulative data byte budget.
/// - `exceeded`: flag shared with middleware after the handler completes.
///
/// # Returns
///
/// A body that rejects the first data frame exceeding the configured budget.
pub(in crate::limit) fn limit_request_body(inner: Body, max_bytes: usize, exceeded: Arc<AtomicBool>) -> Body {
    Body::new(LimitedRequestBody {
        inner,
        max_bytes,
        consumed_bytes: 0,
        exceeded,
    })
}
