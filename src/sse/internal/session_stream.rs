// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;

use axum::response::sse::Event;
use futures_core::Stream;

use super::sse_connection_guard::SseConnectionGuard;

/// Pins the application stream and retains its reservation for the response
/// lifetime.
pub(in crate::sse) struct SessionStream<S> {
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
    pub(in crate::sse) fn new(stream: S, guard: SseConnectionGuard) -> Self {
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
