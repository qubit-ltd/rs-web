// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Application-facing WebSocket session handle.

use std::time::Duration;

use axum::Error;
use axum::extract::ws::Message;
use tokio::join;
use tokio::select;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio::time::timeout;
use tokio_util::sync::CancellationToken;

use super::ws_send_error::WsSendError;
use super::ws_send_queue::WsSendQueue;

/// One upgraded connection with bounded outbound buffering and coordinated
/// reader/writer shutdown.
///
/// # Examples
///
/// ```
/// use qubit_web::ws::WsSession;
///
/// async fn consume_messages(mut session: WsSession) {
///     while let Some(message) = session.recv().await {
///         let _ = message;
///     }
/// }
///
/// let _handler = consume_messages;
/// ```
pub struct WsSession {
    /// Channel carrying inbound frames from the reader task.
    pub(super) incoming: mpsc::Receiver<Result<Message, Error>>,
    /// Shared bounded queue drained by the writer task.
    pub(super) queue: WsSendQueue,
    /// Cancellation token shared by the application and both tasks.
    pub(super) shutdown: CancellationToken,
    /// Maximum interval without an inbound application message.
    pub(super) idle_timeout: Duration,
    /// Maximum time allowed for close acknowledgement during shutdown.
    pub(super) shutdown_timeout: Duration,
    /// Dedicated task that writes queued messages and close frames.
    pub(super) writer: JoinHandle<()>,
    /// Dedicated task that reads frames and sends them to the application.
    pub(super) reader: JoinHandle<()>,
}

impl WsSession {
    /// Receives the next application message.
    ///
    /// A peer close frame may be returned as `Some(Ok(Message::Close(...)))`
    /// when the inbound channel has capacity. A later call returns `None` after
    /// the channel closes. If the close frame cannot be queued, or cancellation
    /// or the idle deadline wins, the method can return `None` without first
    /// returning a close frame.
    ///
    /// # Returns
    ///
    /// An inbound message or read error when delivered to the application;
    /// `None` after close, cancellation, or the idle deadline.
    ///
    /// # Errors
    ///
    /// The inner error reports a WebSocket read failure observed by the reader
    /// task.
    pub async fn recv(&mut self) -> Option<Result<Message, Error>> {
        select! {
            biased;
            _ = self.shutdown.cancelled() => {
                self.close().await;
                None
            }
            message = timeout(self.idle_timeout, self.incoming.recv()) => {
                match message {
                    Ok(message) => message,
                    Err(_) => {
                        self.close().await;
                        None
                    }
                }
            }
        }
    }

    /// Adds a message to the bounded outbound queue.
    ///
    /// # Parameters
    ///
    /// * `message` - Outbound WebSocket message to queue.
    ///
    /// # Returns
    ///
    /// `Ok(())` when the message is accepted.
    ///
    /// # Errors
    ///
    /// Returns [`WsSendError::Closed`] after shutdown or
    /// [`WsSendError::Backpressure`] when queue capacity is exhausted. A
    /// message accepted before shutdown may be discarded if it is still
    /// pending when the session closes.
    #[inline]
    pub fn try_send(&self, message: Message) -> Result<(), WsSendError> {
        if self.shutdown.is_cancelled() {
            return Err(WsSendError::Closed);
        }
        self.queue.try_send(message)
    }

    /// Sends a close frame and discards queued messages.
    ///
    /// The close operation waits at most the configured session shutdown
    /// timeout, then aborts reader and writer tasks. It immediately closes the
    /// outbound queue, discarding pending messages and rejecting future sends.
    pub async fn close(&mut self) {
        self.queue.close();
        self.shutdown.cancel();
        let wait_for_tasks = async {
            let _ = join!(&mut self.writer, &mut self.reader);
        };
        if timeout(self.shutdown_timeout, wait_for_tasks).await.is_err() {
            self.writer.abort();
            self.reader.abort();
            let _ = join!(&mut self.writer, &mut self.reader);
        }
    }
}

impl Drop for WsSession {
    fn drop(&mut self) {
        self.queue.close();
        self.shutdown.cancel();
    }
}
