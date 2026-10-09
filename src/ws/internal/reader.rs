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
use tokio::time::timeout;
use tokio::time::timeout_at;
use tokio_util::sync::CancellationToken;

use super::SessionLifecycle;
use super::close::get_close_deadline;
use crate::ws::ws_send_queue::CloseQueueOnDrop;
use crate::ws::ws_send_queue::WsSendQueue;

/// Receives frames, applies idle/shutdown deadlines, and forwards input to the
/// session.
///
/// # Parameters
///
/// * `stream` - Socket half read by the session task.
/// * `incoming` - Bounded channel used to deliver frames to the application.
/// * `queue` - Shared outbound queue used to wake the writer and reject sends
///   after the reader exits.
/// * `lifecycle` - Shared cancellation, close-code and deadline policy.
/// * `close_ack` - Signal used to coordinate peer close acknowledgement.
/// * `idle_timeout` - Maximum time without a received frame or successful
///   delivery to the application before shutdown.
pub(in crate::ws) async fn reader_loop(
    mut stream: SplitStream<WebSocket>,
    incoming: mpsc::Sender<Result<Message, Error>>,
    queue: WsSendQueue,
    lifecycle: SessionLifecycle,
    close_ack: oneshot::Sender<()>,
    idle_timeout: Duration,
) {
    let _close_queue_on_drop = CloseQueueOnDrop::new(queue.clone());
    let mut close_ack = Some(close_ack);
    let mut deadline = None;
    loop {
        let next = select! {
            biased;
            _ = lifecycle.shutdown.cancelled(), if deadline.is_none() => {
                deadline = Some(get_close_deadline(
                    &lifecycle.close_deadline,
                    lifecycle.server_deadline.as_ref(),
                    lifecycle.shutdown_timeout,
                ));
                continue;
            }
            result = async {
                if let Some(deadline) = deadline {
                    timeout_at(deadline, stream.next()).await
                } else {
                    timeout(idle_timeout, stream.next()).await
                }
            } => result,
        };
        match next {
            Ok(Some(Ok(message @ Message::Ping(_)))) => {
                queue.notify.notify_one();
                if handle_delivery(
                    deliver_incoming(&incoming, &lifecycle.shutdown, Ok(message), idle_timeout).await,
                    &queue,
                    &lifecycle,
                    &mut deadline,
                ) {
                    continue;
                }
                break;
            }
            Ok(Some(Ok(message @ Message::Close(_)))) => {
                queue.close();
                queue.notify.notify_one();
                if let Some(ack) = close_ack.take() {
                    let _ = ack.send(());
                }
                let _ = incoming.try_send(Ok(message));
                break;
            }
            Ok(Some(Ok(message))) => {
                if handle_delivery(
                    deliver_incoming(&incoming, &lifecycle.shutdown, Ok(message), idle_timeout).await,
                    &queue,
                    &lifecycle,
                    &mut deadline,
                ) {
                    continue;
                }
                break;
            }
            Ok(Some(Err(error))) => {
                queue.close();
                let display = error.to_string().to_ascii_lowercase();
                lifecycle.close_code.store(
                    if display.contains("too long") { 1009 } else { 1002 },
                    Ordering::Release,
                );
                let _ = incoming.try_send(Err(error));
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

/// Applies the state transition for one attempted inbound delivery.
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
            *deadline = Some(get_close_deadline(
                &lifecycle.close_deadline,
                lifecycle.server_deadline.as_ref(),
                lifecycle.shutdown_timeout,
            ));
            true
        }
        DeliveryResult::TimedOut => {
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

/// Result of forwarding a frame to the bounded application channel.
enum DeliveryResult {
    Delivered,
    ReceiverClosed,
    Cancelled,
    TimedOut,
}

/// Delivers one inbound frame unless cancellation wins the race.
///
/// # Parameters
///
/// * `incoming` - Bounded channel consumed by the application session.
/// * `shutdown` - Token that interrupts delivery during shutdown.
/// * `message` - Frame or read error to deliver.
/// * `idle_timeout` - Maximum time the application channel may remain full.
///
/// # Returns
///
/// The outcome of delivery, cancellation, receiver closure, or timeout.
async fn deliver_incoming(
    incoming: &mpsc::Sender<Result<Message, Error>>,
    shutdown: &CancellationToken,
    message: Result<Message, Error>,
    idle_timeout: Duration,
) -> DeliveryResult {
    select! {
        biased;
        _ = shutdown.cancelled() => DeliveryResult::Cancelled,
        result = timeout(idle_timeout, incoming.send(message)) => match result {
            Ok(Ok(())) => DeliveryResult::Delivered,
            Ok(Err(_)) => DeliveryResult::ReceiverClosed,
            Err(_) => DeliveryResult::TimedOut,
        },
    }
}
