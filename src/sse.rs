// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Server-sent event connection limits, keep-alive, and cancellation.

use std::num::NonZeroUsize;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::task::Context;
use std::task::Poll;
use std::time::Duration;

use axum::response::IntoResponse;
use axum::response::sse::Event;
use axum::response::sse::KeepAlive;
use axum::response::sse::Sse;
use futures_core::Stream;

use crate::ServerContext;
use crate::SessionGuard;

const DEFAULT_MAX_CONNECTIONS: usize = 128;
const DEFAULT_KEEP_ALIVE_INTERVAL: Duration = Duration::from_secs(15);

/// Independent limits for server-sent event connections.
///
/// Event IDs, `Last-Event-ID`, replay, and application event buffering remain
/// application responsibilities. A successful write only means that the
/// transport accepted the data; it does not prove that the client consumed it.
#[derive(Clone, Debug)]
pub struct SseConnectionPolicy {
    max_connections: NonZeroUsize,
    keep_alive_interval: Duration,
    active_connections: Arc<AtomicUsize>,
}

impl Default for SseConnectionPolicy {
    fn default() -> Self {
        Self {
            max_connections: NonZeroUsize::new(DEFAULT_MAX_CONNECTIONS)
                .expect("default SSE connection count is non-zero"),
            keep_alive_interval: DEFAULT_KEEP_ALIVE_INTERVAL,
            active_connections: Arc::new(AtomicUsize::new(0)),
        }
    }
}

impl SseConnectionPolicy {
    /// Creates a policy with the given maximum number of concurrent streams.
    pub fn new(max_connections: NonZeroUsize) -> Self {
        Self {
            max_connections,
            ..Self::default()
        }
    }

    /// Sets the interval between SSE keep-alive comment frames.
    ///
    /// A zero interval is rejected to prevent a busy loop.
    pub fn with_keep_alive_interval(mut self, interval: Duration) -> Result<Self, crate::WebServerError> {
        if interval.is_zero() {
            return Err(crate::WebServerError::InvalidConfig);
        }
        self.keep_alive_interval = interval;
        Ok(self)
    }

    /// Returns the number of currently active SSE responses.
    pub fn active_connections(&self) -> usize {
        self.active_connections.load(Ordering::Acquire)
    }

    /// Reserves an SSE connection and returns its event-source cancellation
    /// token.
    ///
    /// Call this before starting the event producer. Pass the returned token to
    /// producer tasks so they can stop on client disconnect or server shutdown.
    /// A full connection budget is rejected immediately with HTTP 503.
    pub fn begin(&self, context: &ServerContext) -> Result<SseConnection, SseCapacityExceeded> {
        let mut active = self.active_connections.load(Ordering::Acquire);
        loop {
            if active >= self.max_connections.get() {
                return Err(SseCapacityExceeded);
            }
            match self
                .active_connections
                .compare_exchange_weak(active, active + 1, Ordering::AcqRel, Ordering::Acquire)
            {
                Ok(_) => break,
                Err(current) => active = current,
            }
        }

        let cancellation = context.cancellation_token().child_token();
        Ok(SseConnection {
            keep_alive_interval: self.keep_alive_interval,
            guard: Some(SseConnectionGuard {
                active_connections: self.active_connections.clone(),
                session: Some(context.register_session()),
                cancellation: cancellation.clone(),
            }),
            cancellation,
        })
    }
}

/// Returned when the configured number of simultaneous SSE streams is already
/// active.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SseCapacityExceeded;

impl IntoResponse for SseCapacityExceeded {
    fn into_response(self) -> axum::response::Response {
        crate::limit::WebRejection::CapacityExceeded.into_response()
    }
}

/// A reserved SSE slot that owns producer cancellation until its response ends.
pub struct SseConnection {
    keep_alive_interval: Duration,
    guard: Option<SseConnectionGuard>,
    cancellation: tokio_util::sync::CancellationToken,
}

impl SseConnection {
    /// Returns the token the event source should observe for
    /// disconnect/shutdown.
    pub fn cancellation_token(&self) -> tokio_util::sync::CancellationToken {
        self.cancellation.clone()
    }

    /// Wraps an application event stream with connection lifetime and
    /// keep-alive.
    ///
    /// The token is cancelled when the stream ends, the client disconnects, or
    /// the server context begins shutdown. On shutdown, the source may emit a
    /// final event before ending; the server's shutdown deadline bounds sources
    /// that do not finish. Keep-alive frames are comments without event IDs.
    pub fn into_sse<S, E>(mut self, stream: S) -> impl IntoResponse
    where
        S: Stream<Item = Result<Event, E>> + Send + 'static,
        E: Into<Box<dyn std::error::Error + Send + Sync>> + Send + 'static,
    {
        let guard = self.guard.take().expect("SSE connection guard is present");
        Sse::new(SessionStream::new(stream, guard))
            .keep_alive(KeepAlive::new().interval(self.keep_alive_interval).text("keep-alive"))
    }
}

struct SseConnectionGuard {
    active_connections: Arc<AtomicUsize>,
    session: Option<SessionGuard>,
    cancellation: tokio_util::sync::CancellationToken,
}

impl Drop for SseConnectionGuard {
    fn drop(&mut self) {
        self.active_connections.fetch_sub(1, Ordering::AcqRel);
        self.session.take();
        self.cancellation.cancel();
    }
}

struct SessionStream<S> {
    stream: Pin<Box<S>>,
    guard: Option<SseConnectionGuard>,
}

impl<S> SessionStream<S> {
    fn new(stream: S, guard: SseConnectionGuard) -> Self {
        Self {
            stream: Box::pin(stream),
            guard: Some(guard),
        }
    }
}

impl<S, E> Stream for SessionStream<S>
where
    S: Stream<Item = Result<Event, E>>,
{
    type Item = Result<Event, E>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        match this.stream.as_mut().poll_next(cx) {
            Poll::Ready(None) => {
                this.guard.take();
                Poll::Ready(None)
            }
            other => other,
        }
    }
}

impl<S> Drop for SessionStream<S> {
    fn drop(&mut self) {
        self.guard.take();
    }
}
