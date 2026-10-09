// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! WebSocket close-handshake helpers.

use std::sync::Arc;
use std::sync::Mutex;
use std::sync::PoisonError;
use std::time::Duration;

use axum::extract::ws::CloseFrame;
use axum::extract::ws::Message;
use axum::extract::ws::WebSocket;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::response::Response;
use futures_util::SinkExt;
use futures_util::stream::SplitSink;
use tokio::sync::oneshot;
use tokio::time::Instant;
use tokio::time::timeout_at;

/// Stores and returns the earliest applicable close deadline.
///
/// # Parameters
///
/// * `deadline` - Mutable per-session deadline slot.
/// * `server_deadline` - Optional shared server deadline, which takes
///   precedence.
/// * `timeout` - Fallback grace period when the server has no deadline.
///
/// # Returns
///
/// The earlier of the current and newly computed deadlines.
///
/// # Panics
///
/// Panics only if the internal deadline invariant is broken after storing a
/// value.
#[must_use]
pub(in crate::ws) fn get_close_deadline(
    deadline: &Mutex<Option<Instant>>,
    server_deadline: Option<&Arc<Mutex<Option<Instant>>>>,
    timeout: Duration,
) -> Instant {
    let mut deadline = deadline.lock().unwrap_or_else(PoisonError::into_inner);
    let candidate = server_deadline
        .and_then(|shared| *shared.lock().unwrap_or_else(PoisonError::into_inner))
        .unwrap_or_else(|| Instant::now() + timeout);
    let current = *deadline;
    *deadline = Some(current.map_or(candidate, |current| current.min(candidate)));
    deadline.expect("close deadline is set")
}

/// Drains queued messages and performs the WebSocket close handshake on
/// shutdown.
///
/// Builds the stable problem response used when server shutdown closes WS
/// admission.
///
/// # Returns
///
/// An HTTP 503 response with the `server_shutting_down` problem code.
pub(in crate::ws) fn server_shutting_down_response() -> Response {
    (
        StatusCode::SERVICE_UNAVAILABLE,
        [(axum::http::header::CONTENT_TYPE, "application/problem+json")],
        r#"{"type":"about:blank","title":"Service Unavailable","status":503,"code":"server_shutting_down"}"#,
    )
        .into_response()
}

/// Sends a close frame and waits for the reader's peer-acknowledgement signal.
///
/// # Parameters
///
/// * `sink` - WebSocket sink used to send and flush the close frame.
/// * `close_ack` - Signal receiver completed when the reader observes a close.
/// * `code` - Protocol close code to send.
/// * `deadline` - Absolute latest time for sending and acknowledging close.
pub(in crate::ws) async fn send_close_and_wait(
    sink: &mut SplitSink<WebSocket, Message>,
    close_ack: &mut oneshot::Receiver<()>,
    code: u16,
    deadline: Instant,
) {
    match timeout_at(deadline, sink.send(server_shutdown_close(code))).await {
        Ok(Ok(())) => {}
        Ok(Err(_)) | Err(_) => return,
    }
    if matches!(timeout_at(deadline, close_ack).await, Ok(Ok(()))) {
        let _ = timeout_at(deadline, sink.flush()).await;
    }
}

/// Constructs the close frame used for server shutdown or protocol rejection.
///
/// # Parameters
///
/// * `code` - WebSocket close status code.
///
/// # Returns
///
/// A close message with a reason corresponding to the selected code.
#[must_use]
#[inline]
fn server_shutdown_close(code: u16) -> Message {
    Message::Close(Some(CloseFrame {
        code,
        reason: match code {
            1001 => "server shutdown",
            1013 => "inbound backpressure",
            _ => "protocol limit",
        }
        .into(),
    }))
}
