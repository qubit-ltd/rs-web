// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Internal request and response body wrappers for request limits.

use std::io;
use std::sync::Arc;
use std::sync::Mutex;

use axum::body::Body;
use axum::body::Bytes;
use hyper::body::Body as HttpBody;
use tokio::sync::OwnedSemaphorePermit;
use tokio::task::JoinHandle;

use super::body_state::BodyState;
use super::phase::Phase;

/// Response body wrapper that holds capacity until streaming finishes or times
/// out.
struct PermitBody {
    /// State coordinated with the independent deadline task.
    state: Arc<Mutex<BodyState>>,
    /// Absolute request deadline shared with the handler phase.
    deadline: tokio::time::Instant,
    /// Deadline task cancelled when this wrapper finishes or is dropped.
    watcher: Option<JoinHandle<()>>,
}

impl PermitBody {
    /// Returns the shared state even if another poll panicked while holding it.
    fn lock_state(&self) -> std::sync::MutexGuard<'_, BodyState> {
        self.state.lock().unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Marks the body finished and takes resources for destruction outside the
    /// state lock.
    fn finish(&self) -> (Option<Body>, Option<OwnedSemaphorePermit>) {
        let mut state = self.lock_state();
        state.phase = Phase::Finished;
        state.waker = None;
        (state.inner.take(), state.permit.take())
    }

    /// Cancels the deadline task after the body has reached a terminal state.
    fn cancel_watcher(&mut self) {
        if let Some(watcher) = self.watcher.take() {
            watcher.abort();
        }
    }
}

impl Drop for PermitBody {
    fn drop(&mut self) {
        let (inner, permit) = self.finish();
        drop(inner);
        drop(permit);
        self.cancel_watcher();
    }
}

impl hyper::body::Body for PermitBody {
    type Data = Bytes;
    type Error = axum::Error;

    /// Polls the application body until completion or emits one deadline error.
    ///
    /// # Parameters
    ///
    /// - `context`: task context used to register for another body frame.
    ///
    /// # Returns
    ///
    /// The next response frame, one timeout error, end of stream, or `Pending`.
    fn poll_frame(
        mut self: std::pin::Pin<&mut Self>,
        context: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Result<hyper::body::Frame<Self::Data>, Self::Error>>> {
        let this = self.as_mut().get_mut();
        let mut state = this.lock_state();
        match state.phase {
            Phase::TimedOut => {
                state.phase = Phase::Finished;
                drop(state);
                this.cancel_watcher();
                return std::task::Poll::Ready(Some(Err(axum::Error::new(io::Error::new(
                    io::ErrorKind::TimedOut,
                    "short response exceeded its configured deadline",
                )))));
            }
            Phase::Finished => return std::task::Poll::Ready(None),
            Phase::Active => {}
        }

        if tokio::time::Instant::now() >= this.deadline {
            state.phase = Phase::TimedOut;
            let inner = state.inner.take();
            let permit = state.permit.take();
            let waker = state.waker.take();
            drop(state);
            this.cancel_watcher();
            drop(inner);
            drop(permit);
            if let Some(waker) = waker {
                waker.wake();
            }
            let mut state = this.lock_state();
            state.phase = Phase::Finished;
            drop(state);
            return std::task::Poll::Ready(Some(Err(axum::Error::new(io::Error::new(
                io::ErrorKind::TimedOut,
                "short response exceeded its configured deadline",
            )))));
        }

        let Some(inner) = state.inner.as_mut() else {
            state.phase = Phase::Finished;
            drop(state);
            this.cancel_watcher();
            return std::task::Poll::Ready(None);
        };
        if inner.is_end_stream() {
            let inner = state.inner.take();
            let permit = state.permit.take();
            state.phase = Phase::Finished;
            state.waker = None;
            drop(state);
            this.cancel_watcher();
            drop(inner);
            drop(permit);
            return std::task::Poll::Ready(None);
        }

        let result = std::pin::Pin::new(inner).poll_frame(context);
        match result {
            std::task::Poll::Pending => {
                state.waker = Some(context.waker().clone());
                std::task::Poll::Pending
            }
            std::task::Poll::Ready(Some(Ok(frame))) => {
                state.waker = None;
                std::task::Poll::Ready(Some(Ok(frame)))
            }
            std::task::Poll::Ready(Some(Err(error))) => {
                let inner = state.inner.take();
                let permit = state.permit.take();
                state.phase = Phase::Finished;
                state.waker = None;
                drop(state);
                this.cancel_watcher();
                drop(inner);
                drop(permit);
                std::task::Poll::Ready(Some(Err(error)))
            }
            std::task::Poll::Ready(None) => {
                let inner = state.inner.take();
                let permit = state.permit.take();
                state.phase = Phase::Finished;
                state.waker = None;
                drop(state);
                this.cancel_watcher();
                drop(inner);
                drop(permit);
                std::task::Poll::Ready(None)
            }
        }
    }

    /// Reports whether the wrapper can still deliver a timeout error or body
    /// frames.
    ///
    /// # Returns
    ///
    /// `true` only after the body is finished or its timeout error was emitted.
    fn is_end_stream(&self) -> bool {
        let state = self.lock_state();
        match state.phase {
            Phase::Active => state.inner.as_ref().is_none_or(Body::is_end_stream),
            Phase::TimedOut => false,
            Phase::Finished => true,
        }
    }

    /// Returns an empty size hint after timeout or completion.
    ///
    /// # Returns
    ///
    /// The application body's size hint while active, or an empty hint once
    /// no application frames can be produced.
    fn size_hint(&self) -> hyper::body::SizeHint {
        let state = self.lock_state();
        match state.phase {
            Phase::Active => state
                .inner
                .as_ref()
                .map_or_else(hyper::body::SizeHint::default, Body::size_hint),
            Phase::TimedOut | Phase::Finished => hyper::body::SizeHint::default(),
        }
    }
}

/// Takes the body and permit at the deadline, then wakes a waiting transport.
///
/// The lock decides whether timeout or normal completion wins. The application
/// body and permit are dropped after releasing the lock so their destructors
/// cannot block other lifecycle transitions.
async fn watch_deadline(state: Arc<Mutex<BodyState>>, deadline: tokio::time::Instant) {
    tokio::time::sleep_until(deadline).await;
    let (inner, permit, waker) = {
        let mut state = state.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        if !matches!(state.phase, Phase::Active) {
            return;
        }
        state.phase = Phase::TimedOut;
        (state.inner.take(), state.permit.take(), state.waker.take())
    };
    drop(inner);
    drop(permit);
    if let Some(waker) = waker {
        waker.wake();
    }
}

/// Wraps a response body with its capacity permit and absolute deadline.
///
/// The body producer and capacity permit are released at the deadline even if
/// the transport stops polling the body. If response headers have already been
/// sent, the next poll receives one timeout body error; the layer cannot change
/// the status or guarantee when the transport observes that error.
///
/// # Parameters
///
/// - `inner`: handler response body to stream.
/// - `permit`: capacity lease retained until completion, timeout, or drop.
/// - `deadline`: absolute deadline shared with handler processing.
///
/// # Returns
///
/// A response body that releases its producer and capacity when streaming ends
/// or the deadline expires.
pub(in crate::limit) fn permit_body(
    inner: Body,
    permit: Option<OwnedSemaphorePermit>,
    deadline: tokio::time::Instant,
) -> Body {
    let is_end_stream = inner.is_end_stream();
    let state = Arc::new(Mutex::new(BodyState {
        phase: Phase::Active,
        inner: Some(inner),
        permit,
        waker: None,
    }));
    let watcher = (!is_end_stream).then(|| {
        let watcher_state = state.clone();
        tokio::spawn(watch_deadline(watcher_state, deadline))
    });
    Body::new(PermitBody {
        state,
        deadline,
        watcher,
    })
}
