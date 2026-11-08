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
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::response::Response;
use futures_util::Sink;
use futures_util::SinkExt;
use tokio::sync::oneshot;
use tokio::time::Instant;
use tokio::time::timeout_at;

use super::TerminationState;
use crate::ws::WsTermination;

/// Stores and returns the earliest applicable close deadline.
///
/// # Parameters
///
/// * `deadline` - Mutable per-session deadline slot.
/// * `server_deadline` - Optional shared server shutdown deadline. When set, it
///   is the candidate deadline; otherwise the fallback is used.
/// * `timeout` - Fallback grace period measured from now when no server
///   deadline is set. If adding it overflows, the candidate is immediate.
/// # Returns
///
/// The earlier of the deadline already stored and the candidate deadline.
#[must_use]
pub(in crate::ws) fn get_close_deadline(
    deadline: &Mutex<Option<Instant>>,
    server_deadline: Option<&Arc<Mutex<Option<Instant>>>>,
    timeout: Duration,
) -> Instant {
    let mut deadline = deadline.lock().unwrap_or_else(PoisonError::into_inner);
    let now = Instant::now();
    let candidate = server_deadline
        .and_then(|shared| *shared.lock().unwrap_or_else(PoisonError::into_inner))
        .unwrap_or_else(|| now.checked_add(timeout).unwrap_or(now));
    let current = *deadline;
    *deadline = Some(current.map_or(candidate, |current| current.min(candidate)));
    deadline.expect("close deadline is set")
}

/// Builds the stable problem response returned when server shutdown rejects a
/// WebSocket request.
///
/// # Returns
///
/// An HTTP 503 response with the `application/problem+json` content type and
/// the `server_shutting_down` problem code.
pub(in crate::ws) fn server_shutting_down_response() -> Response {
    (
        StatusCode::SERVICE_UNAVAILABLE,
        [(axum::http::header::CONTENT_TYPE, "application/problem+json")],
        r#"{"type":"about:blank","title":"Service Unavailable","status":503,"code":"server_shutting_down"}"#,
    )
        .into_response()
}

/// Sends a close frame through a sink and waits for the reader's
/// peer-acknowledgement signal.
///
/// # Parameters
///
/// * `sink` - WebSocket sink used to send and flush the close frame.
/// * `close_ack` - Signal receiver completed when the reader observes a close.
/// * `code` - Protocol close code to send.
/// * `deadline` - Absolute latest time for sending and acknowledging close.
/// # Type Parameters
///
/// * `S` - WebSocket sink type used to send frames.
pub(in crate::ws) async fn send_close_and_wait<S>(
    sink: &mut S,
    close_ack: &mut oneshot::Receiver<()>,
    code: u16,
    deadline: Instant,
    termination: &TerminationState,
) where
    S: Sink<Message> + Unpin,
{
    match timeout_at(deadline, sink.send(shutdown_close(code, termination.candidate()))).await {
        Ok(Ok(())) => {}
        Ok(Err(_)) => {
            termination.record(WsTermination::WriteError);
            tracing::warn!(event = "websocket_io_failure", direction = "write", operation = "send");
            return;
        }
        Err(_) => return,
    }
    if matches!(timeout_at(deadline, close_ack).await, Ok(Ok(())))
        && matches!(timeout_at(deadline, sink.flush()).await, Ok(Err(_)))
    {
        termination.record(WsTermination::WriteError);
        tracing::warn!(event = "websocket_io_failure", direction = "write", operation = "flush");
    }
}

/// Constructs the close frame used for server shutdown or protocol rejection.
///
/// # Parameters
///
/// * `code` - WebSocket close status code.
/// # Returns
///
/// A close message with a reason corresponding to the selected code.
#[must_use]
#[inline]
fn shutdown_close(code: u16, termination: Option<WsTermination>) -> Message {
    Message::Close(Some(CloseFrame {
        code,
        reason: match code {
            1001 => match termination {
                Some(WsTermination::IdleTimeout) => "idle timeout",
                Some(WsTermination::ServerShutdown) => "server shutdown",
                _ => "session closed",
            },
            1013 => "inbound backpressure",
            _ => "protocol limit",
        }
        .into(),
    }))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::Mutex;
    use std::time::Duration;

    use tokio::time::Instant;

    use super::get_close_deadline;

    #[test]
    fn test_fallback_overflow_returns_an_immediate_deadline() {
        let deadline = Mutex::new(None);
        let result = get_close_deadline(&deadline, None, Duration::MAX);

        assert!(result <= Instant::now());
    }

    #[test]
    fn test_shared_server_deadline_takes_precedence_over_fallback() {
        let server_deadline = Arc::new(Mutex::new(Some(Instant::now())));
        let expected = *server_deadline.lock().unwrap();
        let deadline = Mutex::new(None);

        assert_eq!(
            get_close_deadline(&deadline, Some(&server_deadline), Duration::MAX),
            expected.unwrap()
        );
    }
}
