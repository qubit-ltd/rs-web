// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Outbound WebSocket writer loop.

use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use axum::extract::ws::Message;
use futures_util::Sink;
use futures_util::SinkExt;
use tokio::select;
use tokio::sync::Notify;
use tokio::sync::oneshot;
use tokio::time::Instant;
use tokio::time::timeout_at;
use tokio_util::sync::CancellationToken;

#[cfg(test)]
use self::tests::support::TestObservedSink;
#[cfg(test)]
use self::tests::support::current_test_pending_observer;
use super::SessionLifecycle;
use super::SessionResources;
use super::close::get_close_deadline;
use super::close::send_close_and_wait;
use super::control_flush_outcome::ControlFlushOutcome;
use super::flush_outcome::FlushOutcome;
use super::send_outcome::SendOutcome;
use crate::ws::WsTermination;
use crate::ws::internal::CloseQueueOnDrop;
use crate::ws::ws_send_queue::WsSendQueue;
use crate::ws::ws_send_queue::message_size;

/// Drains the session queue through a sink until shutdown or a terminal error.
///
/// The sink is generic so tests can exercise actual writer-loop behavior with
/// deterministic readiness and flush results; production uses Axum's split
/// WebSocket sink.
/// # Type Parameters
///
/// * `S` - WebSocket sink type used to send frames.
/// # Parameters
///
/// * `sink` - WebSocket sink that receives or flushes frames.
/// * `queue` - Queue whose messages or state are being processed.
/// * `notify` - Notification used to wake the waiting task.
/// * `write_timeout` - Maximum duration for one ordinary send or flush attempt.
/// * `lifecycle` - Shared cancellation, close-code, and deadline state.
/// * `close_ack` - Signal indicating that the peer acknowledged the close
/// * `_resources` - Connection permit and session guard retained for this task.
pub(in crate::ws) async fn writer_loop<S>(
    #[allow(unused_mut)] mut sink: S,
    queue: WsSendQueue,
    notify: Arc<Notify>,
    write_timeout: Duration,
    lifecycle: SessionLifecycle,
    mut close_ack: oneshot::Receiver<()>,
    _resources: Arc<SessionResources>,
) where
    S: Sink<Message> + Unpin,
{
    #[cfg(test)]
    let mut sink = TestObservedSink::new(sink, current_test_pending_observer());
    let SessionLifecycle {
        shutdown,
        close_code,
        close_deadline,
        server_deadline,
        shutdown_timeout,
        server_shutdown,
        termination,
        ..
    } = lifecycle;
    let _close_queue_on_drop = CloseQueueOnDrop::new(queue.clone());
    loop {
        if shutdown.is_cancelled() {
            let deadline = get_close_deadline(&close_deadline, server_deadline.as_ref(), shutdown_timeout);
            record_server_shutdown(&server_shutdown, &termination);
            send_close_and_wait(
                &mut sink,
                &mut close_ack,
                close_code.load(Ordering::Acquire),
                deadline,
                &termination,
            )
            .await;
            return;
        }
        let message = loop {
            let message = queue.take_next();
            if message.is_some() {
                break message;
            }
            select! {
                biased;
                _ = shutdown.cancelled() => {
                    let deadline = get_close_deadline(&close_deadline, server_deadline.as_ref(), shutdown_timeout);
                    record_server_shutdown(&server_shutdown, &termination);
                    send_close_and_wait(&mut sink, &mut close_ack, close_code.load(Ordering::Acquire), deadline, &termination).await;
                    return;
                }
                _ = &mut close_ack => {
                    let deadline = get_close_deadline(&close_deadline, server_deadline.as_ref(), shutdown_timeout);
                    if matches!(flush_until(&mut sink, &shutdown, Some(deadline)).await, FlushOutcome::Failed) {
                        tracing::warn!(event = "websocket_io_failure", direction = "write", operation = "flush");
                    }
                    return;
                }
                _ = notify.notified() => {
                    let flush_deadline = operation_deadline(write_timeout);
                    let outcome = select! {
                        biased;
                        _ = &mut close_ack => ControlFlushOutcome::PeerCloseAcknowledged,
                        result = flush_until(&mut sink, &shutdown, Some(flush_deadline)) => ControlFlushOutcome::Flushed(result),
                    };
                    match outcome {
                        ControlFlushOutcome::PeerCloseAcknowledged => {
                            let deadline = get_close_deadline(&close_deadline, server_deadline.as_ref(), shutdown_timeout);
                            if matches!(flush_until(&mut sink, &shutdown, Some(deadline)).await, FlushOutcome::Failed) {
                                tracing::warn!(event = "websocket_io_failure", direction = "write", operation = "flush");
                            }
                            return;
                        }
                        ControlFlushOutcome::Flushed(FlushOutcome::Flushed) => {}
                        ControlFlushOutcome::Flushed(FlushOutcome::Cancelled) => {
                            let deadline = get_close_deadline(&close_deadline, server_deadline.as_ref(), shutdown_timeout);
                            record_server_shutdown(&server_shutdown, &termination);
                            send_close_and_wait(&mut sink, &mut close_ack, close_code.load(Ordering::Acquire), deadline, &termination).await;
                            return;
                        }
                        ControlFlushOutcome::Flushed(FlushOutcome::Failed) => {
                            termination.record(WsTermination::WriteError);
                            tracing::warn!(event = "websocket_io_failure", direction = "write", operation = "flush");
                            shutdown.cancel();
                            return;
                        }
                        ControlFlushOutcome::Flushed(FlushOutcome::TimedOut) => {
                            termination.record(WsTermination::WriteTimeout);
                            queue.close();
                            let _ = get_close_deadline(&close_deadline, server_deadline.as_ref(), shutdown_timeout);
                            shutdown.cancel();
                            return;
                        }
                    }
                }
            }
        };
        let Some(message) = message else { continue };
        let message_bytes = message_size(&message);
        if matches!(message, Message::Close(_)) {
            record_local_close(&server_shutdown, &termination);
            // A queued application Close starts the protocol close phase when
            // the writer reaches it. Reject later sends immediately and have
            // the reader use the same absolute deadline while waiting for the
            // peer's acknowledgement.
            queue.close();
            let deadline = get_close_deadline(&close_deadline, server_deadline.as_ref(), shutdown_timeout);
            shutdown.cancel();
            let (sent_ok, send_failed) = match timeout_at(deadline, sink.send(message)).await {
                Ok(Ok(())) => (true, false),
                Ok(Err(_)) => (false, true),
                Err(_) => (false, false),
            };
            queue.finish_message(message_bytes);
            if sent_ok {
                if matches!(timeout_at(deadline, &mut close_ack).await, Ok(Ok(())))
                    && matches!(timeout_at(deadline, sink.flush()).await, Ok(Err(_)))
                {
                    tracing::warn!(event = "websocket_io_failure", direction = "write", operation = "flush");
                }
            } else if send_failed {
                termination.record(WsTermination::WriteError);
                tracing::warn!(event = "websocket_io_failure", direction = "write", operation = "send");
            }
            return;
        }
        let send_deadline = operation_deadline(write_timeout);
        let sent = select! {
            biased;
            _ = shutdown.cancelled() => SendOutcome::Shutdown,
            _ = &mut close_ack => SendOutcome::PeerCloseAcknowledged,
            result = timeout_at(send_deadline, sink.send(message)) => match result {
                Ok(Ok(())) => SendOutcome::Sent,
                Ok(Err(_)) => SendOutcome::SinkError,
                Err(_) => SendOutcome::TimedOut,
            },
        };
        queue.finish_message(message_bytes);
        match sent {
            SendOutcome::Sent => {}
            SendOutcome::SinkError => {
                termination.record(WsTermination::WriteError);
                tracing::warn!(event = "websocket_io_failure", direction = "write", operation = "send");
                let deadline = get_close_deadline(&close_deadline, server_deadline.as_ref(), shutdown_timeout);
                shutdown.cancel();
                queue.close();
                // Preserve the bounded best-effort flush after cancellation;
                // `flush_until` would return immediately for the cancelled token.
                let _ = timeout_at(deadline, sink.flush()).await;
                return;
            }
            SendOutcome::PeerCloseAcknowledged => {
                let deadline = get_close_deadline(&close_deadline, server_deadline.as_ref(), shutdown_timeout);
                if matches!(
                    flush_until(&mut sink, &shutdown, Some(deadline)).await,
                    FlushOutcome::Failed
                ) {
                    tracing::warn!(event = "websocket_io_failure", direction = "write", operation = "flush");
                }
                return;
            }
            SendOutcome::Shutdown => {
                let deadline = get_close_deadline(&close_deadline, server_deadline.as_ref(), shutdown_timeout);
                record_server_shutdown(&server_shutdown, &termination);
                send_close_and_wait(
                    &mut sink,
                    &mut close_ack,
                    close_code.load(Ordering::Acquire),
                    deadline,
                    &termination,
                )
                .await;
                return;
            }
            SendOutcome::TimedOut => {
                termination.record(WsTermination::WriteTimeout);
                queue.close();
                let _ = get_close_deadline(&close_deadline, server_deadline.as_ref(), shutdown_timeout);
                shutdown.cancel();
                return;
            }
        }
    }
}

/// Records server shutdown when the parent token has been cancelled.
///
/// # Parameters
///
/// * `server_shutdown` - Parent server cancellation token.
/// * `termination` - Shared first-cause state for the session.
fn record_server_shutdown(server_shutdown: &CancellationToken, termination: &super::TerminationState) {
    if server_shutdown.is_cancelled() {
        termination.record(WsTermination::ServerShutdown);
    }
}

/// Records a local close unless server shutdown has already begun.
///
/// # Parameters
///
/// * `server_shutdown` - Parent server cancellation token.
/// * `termination` - Shared first-cause state for the session.
fn record_local_close(server_shutdown: &CancellationToken, termination: &super::TerminationState) {
    if server_shutdown.is_cancelled() {
        termination.record(WsTermination::ServerShutdown);
    } else {
        termination.record(WsTermination::LocalClose);
    }
}

/// Computes one operation's absolute write deadline, using an immediate
/// deadline if the configured duration cannot be represented from now.
/// # Parameters
///
/// * `write_timeout` - Maximum duration allowed for the ordinary write
///   operation.
/// # Returns
///
/// The absolute deadline, or the current instant if checked addition overflows.
fn operation_deadline(write_timeout: Duration) -> Instant {
    Instant::now().checked_add(write_timeout).unwrap_or_else(Instant::now)
}
/// Flushes the sink until completion, cancellation, or the optional deadline.
/// # Parameters
///
/// * `sink` - WebSocket sink whose buffered frames are flushed.
/// * `shutdown` - Cancellation token that can end an unbounded flush.
/// * `deadline` - Optional absolute deadline bounding the flush.
/// # Type Parameters
///
/// * `S` - Sink used to write WebSocket messages.
async fn flush_until<S>(sink: &mut S, shutdown: &CancellationToken, deadline: Option<Instant>) -> FlushOutcome
where
    S: Sink<Message> + Unpin,
{
    let flushing = sink.flush();
    tokio::pin!(flushing);
    match deadline {
        Some(deadline) => tokio::select! {
            biased;
            _ = shutdown.cancelled() => FlushOutcome::Cancelled,
            result = timeout_at(deadline, &mut flushing) => match result {
                Ok(Ok(())) => FlushOutcome::Flushed,
                Ok(Err(_)) => FlushOutcome::Failed,
                Err(_) => FlushOutcome::TimedOut,
            },
        },
        None => tokio::select! {
            biased;
            _ = shutdown.cancelled() => FlushOutcome::Cancelled,
            result = &mut flushing => if result.is_ok() {
                FlushOutcome::Flushed
            } else {
                FlushOutcome::Failed
            },
        },
    }
}

#[cfg(test)]
mod tests;
