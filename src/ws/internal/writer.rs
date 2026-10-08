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

use axum::extract::ws::Message;
use axum::extract::ws::WebSocket;
use futures_util::SinkExt;
use futures_util::stream::SplitSink;
use tokio::select;
use tokio::sync::Notify;
use tokio::sync::OwnedSemaphorePermit;
use tokio::sync::oneshot;

use super::SessionLifecycle;
use super::close::get_close_deadline;
use super::close::send_close_and_wait;
use crate::server::SessionGuard;
use crate::ws::ws_send_queue::WsSendQueue;
use crate::ws::ws_send_queue::message_size;

pub(in crate::ws) async fn writer_loop(
    mut sink: SplitSink<WebSocket, Message>,
    queue: WsSendQueue,
    notify: Arc<Notify>,
    lifecycle: SessionLifecycle,
    mut close_ack: oneshot::Receiver<()>,
    _permit: OwnedSemaphorePermit,
    _session: SessionGuard,
) {
    let SessionLifecycle {
        shutdown,
        close_code,
        close_deadline,
        server_deadline,
        shutdown_timeout,
    } = lifecycle;
    loop {
        if shutdown.is_cancelled() {
            let deadline = get_close_deadline(&close_deadline, server_deadline.as_ref(), shutdown_timeout);
            send_close_and_wait(&mut sink, &mut close_ack, close_code.load(Ordering::Acquire), deadline).await;
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
                    send_close_and_wait(&mut sink, &mut close_ack, close_code.load(Ordering::Acquire), deadline).await;
                    return;
                }
                _ = &mut close_ack => {
                    let _ = sink.flush().await;
                    return;
                }
                _ = notify.notified() => {
                    if sink.flush().await.is_err() {
                        shutdown.cancel();
                        return;
                    }
                }
            }
        };
        let Some(message) = message else { continue };
        let message_bytes = message_size(&message);
        let sent = select! {
            biased;
            _ = shutdown.cancelled() => None,
            _ = &mut close_ack => Some(Err(())),
            result = sink.send(message) => Some(result.map_err(|_| ())),
        };
        queue.finish_message(message_bytes);
        match sent {
            Some(Ok(())) => {}
            Some(Err(())) => {
                let _ = sink.flush().await;
                return;
            }
            None => {
                let deadline = get_close_deadline(&close_deadline, server_deadline.as_ref(), shutdown_timeout);
                send_close_and_wait(&mut sink, &mut close_ack, close_code.load(Ordering::Acquire), deadline).await;
                return;
            }
        }
    }
}
