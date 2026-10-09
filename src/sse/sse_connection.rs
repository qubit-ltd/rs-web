// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use std::error::Error;
use std::time::Duration;

use axum::response::IntoResponse;
use axum::response::sse::Event;
use axum::response::sse::KeepAlive;
use axum::response::sse::Sse;
use futures_core::Stream;
use tokio_util::sync::CancellationToken;

use super::internal::SessionStream;
use super::internal::SseConnectionGuard;

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
    pub(in crate::sse) fn new(
        keep_alive_interval: Duration,
        guard: SseConnectionGuard,
        cancellation: CancellationToken,
    ) -> Self {
        Self {
            keep_alive_interval,
            guard: Some(guard),
            cancellation,
        }
    }

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
