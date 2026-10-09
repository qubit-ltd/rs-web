// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Internal request and response body wrappers for request limits.

use std::io;
use std::pin::Pin;

use axum::body::Body;
use axum::body::Bytes;
use tokio::sync::OwnedSemaphorePermit;

/// Response body wrapper that holds capacity until streaming finishes or times
/// out.
struct PermitBody {
    /// Handler response body polled while the permit is held.
    inner: Body,
    /// Capacity lease released when this wrapper is dropped or times out.
    permit: Option<OwnedSemaphorePermit>,
    /// Absolute request deadline shared with the handler phase.
    deadline: Pin<Box<tokio::time::Sleep>>,
    /// Whether a timeout error has already been emitted.
    timed_out: bool,
}

impl hyper::body::Body for PermitBody {
    type Data = Bytes;
    type Error = axum::Error;

    /// Polls the response body until completion or emits one deadline error.
    ///
    /// # Parameters
    ///
    /// - `context`: task context used to register body and deadline wake-ups.
    ///
    /// # Returns
    ///
    /// The next response frame, a timeout error, end of stream, or `Pending`.
    fn poll_frame(
        mut self: Pin<&mut Self>,
        context: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Result<hyper::body::Frame<Self::Data>, Self::Error>>> {
        let this = self.as_mut().get_mut();
        if this.timed_out {
            return std::task::Poll::Ready(None);
        }
        if this.inner.is_end_stream() {
            this.permit.take();
            return std::task::Poll::Ready(None);
        }
        if this.deadline.as_mut().poll(context).is_ready() {
            this.timed_out = true;
            this.permit.take();
            return std::task::Poll::Ready(Some(Err(axum::Error::new(io::Error::new(
                io::ErrorKind::TimedOut,
                "short response exceeded its configured deadline",
            )))));
        }
        match Pin::new(&mut this.inner).poll_frame(context) {
            std::task::Poll::Ready(Some(Err(error))) => {
                this.permit.take();
                std::task::Poll::Ready(Some(Err(error)))
            }
            std::task::Poll::Ready(None) => {
                this.permit.take();
                std::task::Poll::Ready(None)
            }
            result => result,
        }
    }

    /// Reports whether the response ended or a timeout has stopped its body.
    ///
    /// # Returns
    ///
    /// `true` when no further response frames can be delivered.
    fn is_end_stream(&self) -> bool {
        self.timed_out || self.inner.is_end_stream()
    }

    /// Returns an empty size hint after timeout, otherwise delegates to the
    /// body.
    ///
    /// # Returns
    ///
    /// The body size hint, or the default empty hint after timeout.
    fn size_hint(&self) -> hyper::body::SizeHint {
        if self.timed_out {
            hyper::body::SizeHint::default()
        } else {
            self.inner.size_hint()
        }
    }
}

/// Wraps a response body with its capacity permit and absolute deadline.
///
/// # Parameters
///
/// - `inner`: handler response body to stream.
/// - `permit`: capacity lease retained until completion, timeout, or drop.
/// - `deadline`: absolute deadline shared with handler processing.
///
/// # Returns
///
/// A response body that releases capacity when streaming ends or times out.
pub(in crate::limit) fn permit_body(
    inner: Body,
    permit: Option<OwnedSemaphorePermit>,
    deadline: tokio::time::Instant,
) -> Body {
    Body::new(PermitBody {
        inner,
        permit,
        deadline: Box::pin(tokio::time::sleep_until(deadline)),
        timed_out: false,
    })
}
