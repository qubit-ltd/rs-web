// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Inbound WebSocket reader loop.

use std::sync::atomic::Ordering;
use std::time::Duration;

use axum::Error;
use axum::extract::ws::Message;
use axum::extract::ws::WebSocket;
use futures_util::StreamExt;
use futures_util::stream::SplitStream;
use tokio::select;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio::time::timeout_at;
use tokio_util::sync::CancellationToken;

use super::DeliveryResult;
use super::SessionLifecycle;
use super::close::get_close_deadline;
use super::reader_context::ReaderContext;
use super::reader_wait_result::ReaderWaitResult;
use crate::ws::WsTermination;
use crate::ws::internal::CloseQueueOnDrop;
use crate::ws::ws_send_queue::WsSendQueue;
/// Selects the shutdown deadline when present, otherwise computes an idle
/// deadline. `None` means the idle deadline overflowed and was not created.
/// # Parameters
///
/// * `shutdown_deadline` - Absolute server-shutdown deadline, when one is
/// * `idle_timeout` - Maximum duration allowed without inbound activity.
fn reader_deadline(
    shutdown_deadline: Option<tokio::time::Instant>,
    idle_timeout: Duration,
) -> Option<tokio::time::Instant> {
    shutdown_deadline.or_else(|| tokio::time::Instant::now().checked_add(idle_timeout))
}

/// Receives frames, applies idle/shutdown deadlines, and forwards input to the
/// session.
///
/// # Parameters
///
/// * `stream` - Socket read half from which peer frames are received.
/// * `incoming` - Bounded channel receiving budgeted frames for the
///   application.
/// * `queue` - Outbound queue closed on reader exit and notified for close or
///   ping handling.
/// * `lifecycle` - Shared cancellation, close code, and shutdown deadlines.
/// * `close_ack` - One-shot signal sent when a peer close frame is observed.
/// * `idle_timeout` - Maximum idle interval and maximum wait to deliver a
///   frame.
/// * `context` - Inbound byte accounting and shared session resource lifetime.
pub(in crate::ws) async fn reader_loop(
    mut stream: SplitStream<WebSocket>,
    incoming: mpsc::Sender<super::InboundMessage>,
    queue: WsSendQueue,
    lifecycle: SessionLifecycle,
    close_ack: oneshot::Sender<()>,
    idle_timeout: Duration,
    context: ReaderContext,
) {
    let ReaderContext {
        inbound_state,
        _resources,
    } = context;
    let _close_queue_on_drop = CloseQueueOnDrop::new(queue.clone());
    let mut close_ack = Some(close_ack);
    let mut deadline = None;
    loop {
        let next = select! {
            biased;
            _ = lifecycle.shutdown.cancelled(), if deadline.is_none() => {
                if lifecycle.server_shutdown.is_cancelled() {
                    lifecycle.termination.record(WsTermination::ServerShutdown);
                }
                deadline = Some(get_close_deadline(
                    &lifecycle.close_deadline,
                    lifecycle.server_deadline.as_ref(),
                    lifecycle.shutdown_timeout,
                ));
                continue;
            }
            result = async {
                match reader_deadline(deadline, idle_timeout) {
                    Some(deadline) => ReaderWaitResult::Timed(timeout_at(deadline, stream.next()).await),
                    None => ReaderWaitResult::IdleDeadlineUnrepresentable,
                }
            } => result,
        };
        let next = match next {
            ReaderWaitResult::Timed(result) => result,
            ReaderWaitResult::IdleDeadlineUnrepresentable => {
                lifecycle.termination.record(WsTermination::IdleTimeout);
                lifecycle.close_code.store(1001, Ordering::Release);
                deadline = Some(get_close_deadline(
                    &lifecycle.close_deadline,
                    lifecycle.server_deadline.as_ref(),
                    lifecycle.shutdown_timeout,
                ));
                lifecycle.shutdown.cancel();
                continue;
            }
        };
        match next {
            Ok(Some(Ok(message @ Message::Ping(_)))) => {
                queue.notify.notify_one();
                let Ok(message) = inbound_state.try_wrap(message) else {
                    lifecycle.termination.record(WsTermination::InboundBackpressure);
                    lifecycle.close_code.store(1013, Ordering::Release);
                    lifecycle.shutdown.cancel();
                    deadline.get_or_insert_with(|| {
                        get_close_deadline(
                            &lifecycle.close_deadline,
                            lifecycle.server_deadline.as_ref(),
                            lifecycle.shutdown_timeout,
                        )
                    });
                    continue;
                };
                if handle_delivery(
                    deliver_incoming(&incoming, &lifecycle.shutdown, message, idle_timeout).await,
                    &queue,
                    &lifecycle,
                    &mut deadline,
                ) {
                    continue;
                }
                break;
            }
            Ok(Some(Ok(message @ Message::Close(_)))) => {
                let code = match &message {
                    Message::Close(Some(frame)) => Some(frame.code),
                    _ => None,
                };
                lifecycle.termination.record(WsTermination::PeerClose { code });
                queue.close();
                queue.notify.notify_one();
                if let Some(ack) = close_ack.take() {
                    let _ = ack.send(());
                }
                if let Ok(message) = inbound_state.try_wrap(message) {
                    let _ = incoming.try_send(message);
                }
                break;
            }
            Ok(Some(Ok(message))) => {
                let Ok(message) = inbound_state.try_wrap(message) else {
                    lifecycle.termination.record(WsTermination::InboundBackpressure);
                    lifecycle.close_code.store(1013, Ordering::Release);
                    lifecycle.shutdown.cancel();
                    deadline.get_or_insert_with(|| {
                        get_close_deadline(
                            &lifecycle.close_deadline,
                            lifecycle.server_deadline.as_ref(),
                            lifecycle.shutdown_timeout,
                        )
                    });
                    continue;
                };
                if handle_delivery(
                    deliver_incoming(&incoming, &lifecycle.shutdown, message, idle_timeout).await,
                    &queue,
                    &lifecycle,
                    &mut deadline,
                ) {
                    continue;
                }
                break;
            }
            Ok(Some(Err(error))) => {
                lifecycle.termination.record(WsTermination::ReadError);
                queue.close();
                lifecycle
                    .close_code
                    .store(close_code_for_read_error(&error), Ordering::Release);
                inbound_state.record_read_error(error);
                lifecycle.shutdown.cancel();
                if deadline.is_none() {
                    deadline = Some(get_close_deadline(
                        &lifecycle.close_deadline,
                        lifecycle.server_deadline.as_ref(),
                        lifecycle.shutdown_timeout,
                    ));
                }
            }
            Ok(None) => {
                queue.close();
                if let Some(ack) = close_ack.take() {
                    let _ = ack.send(());
                }
                break;
            }
            Err(_) if deadline.is_none() => {
                lifecycle.termination.record(WsTermination::IdleTimeout);
                lifecycle.close_code.store(1001, Ordering::Release);
                deadline = Some(get_close_deadline(
                    &lifecycle.close_deadline,
                    lifecycle.server_deadline.as_ref(),
                    lifecycle.shutdown_timeout,
                ));
                lifecycle.shutdown.cancel();
            }
            Err(_) => break,
        }
    }
    drop(close_ack);
}

/// Maps a WebSocket read error to the protocol close code.
///
/// # Parameters
///
/// * `error` - Read failure wrapped by Axum.
/// # Returns
///
/// `1009` for an oversized message, or `1002` otherwise.
fn close_code_for_read_error(error: &Error) -> u16 {
    let source = std::error::Error::source(error).and_then(|source| source.downcast_ref::<tungstenite::Error>());
    if matches!(
        source,
        Some(tungstenite::Error::Capacity(
            tungstenite::error::CapacityError::MessageTooLong { .. }
        ))
    ) {
        1009
    } else {
        1002
    }
}

/// Applies the state transition for one attempted inbound delivery.
/// # Parameters
///
/// * `delivery` - Result of attempting to forward one inbound frame.
/// * `queue` - Outbound queue closed when the receiver is gone or delivery
///   cannot continue.
/// * `lifecycle` - Shared shutdown and close state for this session.
/// * `deadline` - Close deadline set when cancellation or timeout starts close
///   handling; unchanged for delivery or receiver closure.
fn handle_delivery(
    delivery: DeliveryResult,
    queue: &WsSendQueue,
    lifecycle: &SessionLifecycle,
    deadline: &mut Option<tokio::time::Instant>,
) -> bool {
    match delivery {
        DeliveryResult::Delivered => true,
        DeliveryResult::ReceiverClosed => {
            queue.close();
            false
        }
        DeliveryResult::Cancelled => {
            if lifecycle.server_shutdown.is_cancelled() {
                lifecycle.termination.record(WsTermination::ServerShutdown);
            }
            *deadline = Some(get_close_deadline(
                &lifecycle.close_deadline,
                lifecycle.server_deadline.as_ref(),
                lifecycle.shutdown_timeout,
            ));
            true
        }
        DeliveryResult::TimedOut => {
            lifecycle.termination.record(WsTermination::InboundBackpressure);
            queue.close();
            lifecycle.close_code.store(1013, Ordering::Release);
            lifecycle.shutdown.cancel();
            *deadline = Some(get_close_deadline(
                &lifecycle.close_deadline,
                lifecycle.server_deadline.as_ref(),
                lifecycle.shutdown_timeout,
            ));
            true
        }
    }
}

/// Delivers one inbound frame unless cancellation wins the race.
///
/// # Parameters
///
/// * `incoming` - Bounded channel consumed by the application session.
/// * `shutdown` - Token that interrupts delivery during shutdown.
/// * `message` - Frame or read error to deliver.
/// * `idle_timeout` - Maximum time allowed for the bounded channel send.
/// # Returns
///
/// The outcome of delivery, cancellation, receiver closure, or timeout.
async fn deliver_incoming(
    incoming: &mpsc::Sender<super::InboundMessage>,
    shutdown: &CancellationToken,
    message: super::InboundMessage,
    idle_timeout: Duration,
) -> DeliveryResult {
    select! {
        biased;
        _ = shutdown.cancelled() => DeliveryResult::Cancelled,
        result = async {
            let deadline = tokio::time::Instant::now().checked_add(idle_timeout)?;
            Some(timeout_at(deadline, incoming.send(message)).await)
        } => match result {
            None => DeliveryResult::TimedOut,
            Some(Ok(Ok(()))) => DeliveryResult::Delivered,
            Some(Ok(Err(_))) => DeliveryResult::ReceiverClosed,
            Some(Err(_)) => DeliveryResult::TimedOut,
        },
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use axum::Error;
    use axum::extract::ws::Message;
    use tokio::sync::mpsc;
    use tokio_util::sync::CancellationToken;
    use tungstenite::Error as WebSocketError;
    use tungstenite::error::CapacityError;

    use super::close_code_for_read_error;
    use super::deliver_incoming;
    use super::reader_deadline;
    use crate::ws::internal::DeliveryResult;
    use crate::ws::internal::InboundState;

    #[test]
    fn test_close_code_for_read_error_uses_capacity_type() {
        let oversized = Error::new(WebSocketError::Capacity(CapacityError::MessageTooLong {
            size: 5,
            max_size: 4,
        }));
        assert_eq!(close_code_for_read_error(&oversized), 1009);

        let misleading_text = Error::new(std::io::Error::other("message too long"));
        assert_eq!(close_code_for_read_error(&misleading_text), 1002);

        let other_websocket_error = Error::new(WebSocketError::ConnectionClosed);
        assert_eq!(close_code_for_read_error(&other_websocket_error), 1002);
    }

    #[test]
    fn test_unrepresentable_reader_idle_deadline_is_detected() {
        assert!(reader_deadline(None, Duration::MAX).is_none());
    }

    #[tokio::test]
    async fn test_unrepresentable_delivery_deadline_uses_the_timeout_path() {
        let state = Arc::new(InboundState::new(8));
        let message = state
            .try_wrap(Message::Text("payload".into()))
            .expect("the test message fits the inbound budget");
        let (incoming, _receiver) = mpsc::channel(1);

        let result = deliver_incoming(&incoming, &CancellationToken::new(), message, Duration::MAX).await;

        assert!(matches!(result, DeliveryResult::TimedOut));
    }
}
