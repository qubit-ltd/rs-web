// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Server-sent event connection limits, keep-alive, and cancellation.

use std::error::Error;
use std::num::NonZeroUsize;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::task::Context;
use std::task::Poll;
use std::time::Duration;

use axum::http::StatusCode;
use axum::http::header::CONTENT_TYPE;
use axum::response::IntoResponse;
use axum::response::Response;
use axum::response::sse::Event;
use axum::response::sse::KeepAlive;
use axum::response::sse::Sse;
use futures_core::Stream;
use tokio_util::sync::CancellationToken;

use crate::ServerContext;
use crate::SessionGuard;
use crate::SessionRegistrationError;
use crate::WebServerError;
use crate::limit::WebRejection;

const DEFAULT_MAX_CONNECTIONS: usize = 128;
const DEFAULT_KEEP_ALIVE_INTERVAL: Duration = Duration::from_secs(15);

/// Independent limits for server-sent event connections.
///
/// Event IDs, `Last-Event-ID`, replay, and application event buffering remain
/// application responsibilities. A successful write only means that the
/// transport accepted the data; it does not prove that the client consumed it.
///
/// # Examples
///
/// ```
/// use std::num::NonZeroUsize;
///
/// use qubit_web::SseConnectionPolicy;
///
/// let policy = SseConnectionPolicy::new(NonZeroUsize::new(32).expect("positive limit"));
/// assert_eq!(policy.active_connections(), 0);
/// ```
#[derive(Clone, Debug)]
#[must_use]
pub struct SseConnectionPolicy {
    /// Maximum number of simultaneous SSE connections admitted by this policy.
    max_connections: NonZeroUsize,
    /// Delay between comment frames used to keep an idle connection open.
    keep_alive_interval: Duration,
    /// Shared count of active connections reserved through this policy.
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
    ///
    /// # Parameters
    ///
    /// * `max_connections` - Positive simultaneous connection capacity.
    ///
    /// # Returns
    ///
    /// A policy with the default keep-alive interval and no active streams.
    pub fn new(max_connections: NonZeroUsize) -> Self {
        Self {
            max_connections,
            ..Self::default()
        }
    }

    /// Returns the number of currently active SSE responses.
    ///
    /// # Returns
    ///
    /// The number of connection reservations not yet released.
    #[must_use]
    #[inline]
    pub fn active_connections(&self) -> usize {
        self.active_connections.load(Ordering::Acquire)
    }

    /// Sets the interval between SSE keep-alive comment frames.
    ///
    /// A zero interval is rejected to prevent a busy loop.
    ///
    /// # Parameters
    ///
    /// * `interval` - Positive delay between keep-alive comment frames.
    ///
    /// # Returns
    ///
    /// The updated policy.
    ///
    /// # Errors
    ///
    /// Returns [`WebServerError::InvalidConfig`] if the interval is
    /// zero.
    pub fn with_keep_alive_interval(mut self, interval: Duration) -> Result<Self, WebServerError> {
        if interval.is_zero() {
            return Err(WebServerError::InvalidConfig);
        }
        self.keep_alive_interval = interval;
        Ok(self)
    }

    /// Reserves an SSE connection and returns its event-source cancellation
    /// token.
    ///
    /// Call this before starting the event producer. Pass the returned token to
    /// producer tasks so they can stop on client disconnect or server shutdown.
    /// A full connection budget is rejected immediately with HTTP 503.
    ///
    /// # Parameters
    ///
    /// * `context` - Server lifecycle state used for cancellation and session
    ///   accounting.
    ///
    /// # Returns
    ///
    /// A reserved connection with a cancellation token for the event producer.
    ///
    /// # Errors
    ///
    /// Returns [`SseAdmissionError`] when capacity is full or shutdown has
    /// begun.
    pub fn begin(&self, context: &ServerContext) -> Result<SseConnection, SseAdmissionError> {
        let mut active = self.active_connections.load(Ordering::Acquire);
        loop {
            if active >= self.max_connections.get() {
                return Err(SseAdmissionError::CapacityExceeded);
            }
            match self
                .active_connections
                .compare_exchange_weak(active, active + 1, Ordering::AcqRel, Ordering::Acquire)
            {
                Ok(_) => break,
                Err(current) => active = current,
            }
        }

        let session = match context.try_register_session() {
            Ok(session) => session,
            Err(SessionRegistrationError::ShuttingDown) => {
                self.active_connections.fetch_sub(1, Ordering::AcqRel);
                return Err(SseAdmissionError::ShuttingDown);
            }
        };

        let cancellation = context.cancellation_token().child_token();
        Ok(SseConnection {
            keep_alive_interval: self.keep_alive_interval,
            guard: Some(SseConnectionGuard {
                active_connections: self.active_connections.clone(),
                session: Some(session),
                cancellation: cancellation.clone(),
            }),
            cancellation,
        })
    }
}

/// Reports why an SSE connection reservation was rejected.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[must_use]
pub enum SseAdmissionError {
    /// The configured number of simultaneous SSE streams is already active.
    CapacityExceeded,
    /// The server has begun shutting down and no longer accepts SSE sessions.
    ShuttingDown,
}

impl IntoResponse for SseAdmissionError {
    fn into_response(self) -> Response {
        match self {
            Self::CapacityExceeded => WebRejection::CapacityExceeded.into_response(),
            Self::ShuttingDown => (
                StatusCode::SERVICE_UNAVAILABLE,
                [(CONTENT_TYPE, "application/problem+json")],
                r#"{"type":"about:blank","title":"Service Unavailable","status":503,"code":"server_shutting_down"}"#,
            )
                .into_response(),
        }
    }
}

/// Returned when the configured number of simultaneous SSE streams is already
/// active. Prefer [`SseAdmissionError`] for new code.
///
/// # Examples
///
/// ```
/// use qubit_web::sse::SseCapacityExceeded;
///
/// let error = SseCapacityExceeded;
/// assert_eq!(format!("{error:?}"), "SseCapacityExceeded");
/// ```
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[must_use]
pub struct SseCapacityExceeded;

impl IntoResponse for SseCapacityExceeded {
    fn into_response(self) -> Response {
        WebRejection::CapacityExceeded.into_response()
    }
}

/// A reserved SSE slot that owns producer cancellation until its response ends.
///
/// # Examples
///
/// ```
/// # #[tokio::main]
/// # async fn main() -> Result<(), qubit_web::WebServerError> {
/// use std::num::NonZeroUsize;
///
/// use qubit_web::sse::SseConnectionPolicy;
/// use qubit_web::ServerOptions;
/// use qubit_web::WebServer;
///
/// let server = WebServer::bind_http(ServerOptions::new(
///     "127.0.0.1:0".parse().expect("socket address"),
/// )).await?;
/// let connection = SseConnectionPolicy::new(NonZeroUsize::new(8).expect("positive limit"))
///     .begin(&server.context())
///     .expect("available connection slot");
/// let _cancellation = connection.cancellation_token();
/// # Ok(())
/// # }
/// ```
#[must_use]
pub struct SseConnection {
    /// Keep-alive interval retained until the response stream is constructed.
    keep_alive_interval: Duration,
    /// Guard that releases the policy slot after the response ends or drops.
    guard: Option<SseConnectionGuard>,
    /// Token cancelled on stream completion, disconnect, or server shutdown.
    cancellation: CancellationToken,
}

impl SseConnection {
    /// Returns the token the event source should observe for
    /// disconnect/shutdown.
    ///
    /// # Returns
    ///
    /// A clone that producer tasks can await independently.
    #[must_use]
    #[inline]
    pub fn cancellation_token(&self) -> CancellationToken {
        self.cancellation.clone()
    }

    /// Wraps an application event stream with connection lifetime and
    /// keep-alive.
    ///
    /// The token is cancelled when the stream ends, the client disconnects, or
    /// the server context begins shutdown. On shutdown, the source may emit a
    /// final event before ending; the server's shutdown deadline bounds sources
    /// that do not finish. Keep-alive frames are comments without event IDs.
    ///
    /// # Type Parameters
    ///
    /// * `S` - Sendable event stream with a static lifetime.
    /// * `E` - Stream error convertible to a boxed sendable standard error.
    ///
    /// # Parameters
    ///
    /// * `stream` - Application event source wrapped with session lifetime
    ///   tracking.
    ///
    /// # Returns
    ///
    /// An Axum response that emits keep-alive comments and releases the slot
    /// when done.
    ///
    /// # Panics
    ///
    /// Panics only if the internal reservation guard was already moved out,
    /// which cannot occur through the public API.
    #[must_use]
    pub fn into_sse<S, E>(mut self, stream: S) -> impl IntoResponse
    where
        S: Stream<Item = Result<Event, E>> + Send + 'static,
        E: Into<Box<dyn Error + Send + Sync>> + Send + 'static,
    {
        let guard = self.guard.take().expect("SSE connection guard is present");
        Sse::new(SessionStream::new(stream, guard))
            .keep_alive(KeepAlive::new().interval(self.keep_alive_interval).text("keep-alive"))
    }
}

/// Owns the counters and cancellation state for one admitted SSE response.
struct SseConnectionGuard {
    /// Shared policy connection count decremented on drop.
    active_connections: Arc<AtomicUsize>,
    /// Server session count guard released when this reservation ends.
    session: Option<SessionGuard>,
    /// Producer token cancelled after the response stream ends or drops.
    cancellation: CancellationToken,
}

impl Drop for SseConnectionGuard {
    fn drop(&mut self) {
        self.active_connections.fetch_sub(1, Ordering::AcqRel);
        self.session.take();
        self.cancellation.cancel();
    }
}

/// Pins the application stream and retains its reservation for the response
/// lifetime.
struct SessionStream<S> {
    /// Pinned source polled by Axum's SSE body.
    stream: Pin<Box<S>>,
    /// Reservation released when the source completes or the response is
    /// dropped.
    guard: Option<SseConnectionGuard>,
}

impl<S> SessionStream<S> {
    /// Pins a source and attaches its lifecycle guard.
    ///
    /// # Parameters
    ///
    /// * `stream` - Application event stream to poll.
    /// * `guard` - Reservation retained until stream completion or drop.
    ///
    /// # Returns
    ///
    /// A pinned stream wrapper that owns the reservation.
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
