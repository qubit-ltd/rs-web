// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Transparent response-body wrapper that observes termination.

use std::pin::Pin;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;

use axum::body::Body;
use axum::body::Bytes;
use hyper::body::Body as HttpBody;
use hyper::body::Frame;
use hyper::body::SizeHint;

use super::body_observation::BodyObservation;

/// Body wrapper that forwards frames and records exactly one terminal outcome.
pub(crate) struct DiagnosticBody {
    /// Response body whose frames and size hint are forwarded unchanged.
    inner: Body,
    /// Shared observation state that records one terminal outcome across the
    /// body wrapper and its owning request context.
    observation: Arc<BodyObservation>,
    /// Whether completion or a body error has already been reported. This
    /// prevents `Drop` from reporting a second outcome after polling ends.
    terminal: bool,
}

impl DiagnosticBody {
    /// Wraps a response body and its shared observation state.
    pub(crate) fn new(inner: Body, observation: Arc<BodyObservation>) -> Self {
        Self {
            inner,
            observation,
            terminal: false,
        }
    }
}

impl Drop for DiagnosticBody {
    fn drop(&mut self) {
        // Dropping an unfinished response is terminal too; the shared
        // observation makes this idempotent with other lifecycle observers.
        if !self.terminal {
            self.observation.terminate("dropped");
        }
    }
}

impl HttpBody for DiagnosticBody {
    type Data = Bytes;
    type Error = axum::Error;

    /// Polls and forwards one body frame while observing terminal outcomes.
    ///
    /// A completed body and a body error are recorded once before their
    /// original result is returned. `Pending` is forwarded without marking
    /// the body terminal.
    fn poll_frame(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Result<Frame<Bytes>, Self::Error>>> {
        let this = self.as_mut().get_mut();
        match Pin::new(&mut this.inner).poll_frame(cx) {
            Poll::Ready(Some(Ok(frame))) => {
                // Some bodies signal their final data frame before returning
                // `None`; observe that terminal state without consuming the
                // frame or waiting for another poll.
                if this.inner.is_end_stream() {
                    this.terminal = true;
                    this.observation.terminate("complete");
                }
                Poll::Ready(Some(Ok(frame)))
            }
            Poll::Ready(Some(Err(error))) => {
                // Forward the original error after recording the terminal
                // outcome; the observation does not replace body semantics.
                this.terminal = true;
                this.observation.terminate("body_error");
                Poll::Ready(Some(Err(error)))
            }
            Poll::Ready(None) => {
                this.terminal = true;
                this.observation.terminate("complete");
                Poll::Ready(None)
            }
            Poll::Pending => Poll::Pending,
        }
    }

    /// Reports whether the wrapped body has no more frames, as reported by
    /// the inner body.
    fn is_end_stream(&self) -> bool {
        self.inner.is_end_stream()
    }

    /// Returns the inner body's size hint without altering its bounds.
    fn size_hint(&self) -> SizeHint {
        self.inner.size_hint()
    }
}

#[cfg(test)]
mod tests {
    use std::pin::Pin;
    use std::sync::Arc;
    use std::task::Context;
    use std::task::Poll;
    use std::task::Waker;
    use std::time::Instant;

    use axum::body::Body;
    use axum::body::Bytes;
    use axum::http::Method;
    use hyper::body::Body as HttpBody;
    use hyper::body::Frame;
    use hyper::body::SizeHint;

    use super::DiagnosticBody;
    use crate::diagnostic::internal::BodyObservation;

    struct PendingBody;

    impl HttpBody for PendingBody {
        type Data = Bytes;
        type Error = std::io::Error;

        fn poll_frame(
            self: Pin<&mut Self>,
            _context: &mut Context<'_>,
        ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
            Poll::Pending
        }

        fn is_end_stream(&self) -> bool {
            false
        }

        fn size_hint(&self) -> SizeHint {
            SizeHint::default()
        }
    }

    #[test]
    fn forwards_pending_and_nonterminal_state() {
        let observation = Arc::new(BodyObservation::new(
            Method::GET,
            "/pending".to_owned(),
            "connection".to_owned(),
            None,
            Instant::now(),
        ));
        let mut body = DiagnosticBody::new(Body::new(PendingBody), observation);

        assert!(!HttpBody::is_end_stream(&body));
        let mut context = Context::from_waker(Waker::noop());
        assert!(matches!(
            std::pin::Pin::new(&mut body).poll_frame(&mut context),
            Poll::Pending
        ));
        assert!(!HttpBody::is_end_stream(&body));
    }
}
