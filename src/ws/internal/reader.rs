// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Inbound WebSocket reader loop.

use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use axum::Error;
use axum::extract::ws::Message;
use axum::extract::ws::WebSocket;
use futures_util::StreamExt;
use futures_util::stream::SplitStream;
use tokio::select;
use tokio::sync::Notify;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio::time::timeout;
use tokio::time::timeout_at;
use tokio_util::sync::CancellationToken;

use super::SessionLifecycle;
use super::close::get_close_deadline;

/// Receives frames, applies idle/shutdown deadlines, and forwards input to the
/// session.
///
/// # Parameters
///
/// * `stream` - Socket half read by the session task.
/// * `incoming` - Bounded channel used to deliver frames to the application.
/// * `writer_notify` - Signal waking the writer for ping/close handling.
/// * `lifecycle` - Shared cancellation, close-code and deadline policy.
/// * `close_ack` - Signal used to coordinate peer close acknowledgement.
/// * `idle_timeout` - Maximum time without inbound frames before shutdown.
pub(in crate::ws) async fn reader_loop(
    mut stream: SplitStream<WebSocket>,
    incoming: mpsc::Sender<Result<Message, Error>>,
    writer_notify: Arc<Notify>,
    lifecycle: SessionLifecycle,
    close_ack: oneshot::Sender<()>,
    idle_timeout: Duration,
) {
    let SessionLifecycle {
        shutdown,
        close_code,
        close_deadline,
        server_deadline,
        shutdown_timeout,
    } = lifecycle;
    let mut close_ack = Some(close_ack);
    let mut deadline = None;
    loop {
        let next = select! {
            biased;
            _ = shutdown.cancelled(), if deadline.is_none() => {
                deadline = Some(get_close_deadline(&close_deadline, server_deadline.as_ref(), shutdown_timeout));
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
                writer_notify.notify_one();
                if !deliver_incoming(&incoming, &shutdown, Ok(message)).await {
                    if shutdown.is_cancelled() {
                        deadline = Some(get_close_deadline(
                            &close_deadline,
                            server_deadline.as_ref(),
                            shutdown_timeout,
                        ));
                        continue;
                    }
                    break;
                }
            }
            Ok(Some(Ok(message @ Message::Close(_)))) => {
                writer_notify.notify_one();
                if let Some(ack) = close_ack.take() {
                    let _ = ack.send(());
                }
                let _ = incoming.try_send(Ok(message));
                break;
            }
            Ok(Some(Ok(message))) => {
                if !deliver_incoming(&incoming, &shutdown, Ok(message)).await {
                    if shutdown.is_cancelled() {
                        deadline = Some(get_close_deadline(
                            &close_deadline,
                            server_deadline.as_ref(),
                            shutdown_timeout,
                        ));
                        continue;
                    }
                    break;
                }
            }
            Ok(Some(Err(error))) => {
                let display = error.to_string().to_ascii_lowercase();
                close_code.store(
                    if display.contains("too long") { 1009 } else { 1002 },
                    Ordering::Release,
                );
                let _ = incoming.try_send(Err(error));
                shutdown.cancel();
                if deadline.is_none() {
                    deadline = Some(get_close_deadline(
                        &close_deadline,
                        server_deadline.as_ref(),
                        shutdown_timeout,
                    ));
                }
            }
            Ok(None) => {
                if let Some(ack) = close_ack.take() {
                    let _ = ack.send(());
                }
                break;
            }
            Err(_) if deadline.is_none() => {
                close_code.store(1001, Ordering::Release);
                deadline = Some(get_close_deadline(
                    &close_deadline,
                    server_deadline.as_ref(),
                    shutdown_timeout,
                ));
                shutdown.cancel();
            }
            Err(_) => break,
        }
    }
    drop(close_ack);
}

/// Delivers one inbound frame unless cancellation wins the race.
///
/// # Parameters
///
/// * `incoming` - Bounded channel consumed by the application session.
/// * `shutdown` - Token that interrupts delivery during shutdown.
/// * `message` - Frame or read error to deliver.
///
/// # Returns
///
/// `true` when the channel accepts the item, otherwise `false`.
async fn deliver_incoming(
    incoming: &mpsc::Sender<Result<Message, Error>>,
    shutdown: &CancellationToken,
    message: Result<Message, Error>,
) -> bool {
    select! {
        biased;
        _ = shutdown.cancelled() => false,
        result = incoming.send(message) => result.is_ok(),
    }
}
