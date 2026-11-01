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
    pub(super) writer: Option<JoinHandle<()>>,
    /// Dedicated task that reads frames and sends them to the application.
    pub(super) reader: Option<JoinHandle<()>>,
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
    /// Repeated calls are safe. If this future is cancelled while waiting, a
    /// later call continues waiting for any task whose result was not consumed.
    /// During server shutdown, cancellation can also make the writer exit
    /// without the application calling this method.
    pub async fn close(&mut self) {
        self.queue.close();
        self.shutdown.cancel();
        let wait_for_tasks = async {
            join!(wait_task(&mut self.writer), wait_task(&mut self.reader));
        };
        if timeout(self.shutdown_timeout, wait_for_tasks).await.is_err() {
            if let Some(writer) = self.writer.as_ref() {
                writer.abort();
            }
            if let Some(reader) = self.reader.as_ref() {
                reader.abort();
            }
            join!(wait_task(&mut self.writer), wait_task(&mut self.reader));
        }
    }
}

/// Waits for one session task without losing its handle if the caller is
/// cancelled.
async fn wait_task(slot: &mut Option<JoinHandle<()>>) {
    if let Some(task) = slot.as_mut() {
        let _ = task.await;
        *slot = None;
    }
}

impl Drop for WsSession {
    fn drop(&mut self) {
        self.queue.close();
        self.shutdown.cancel();
    }
}

#[cfg(test)]
mod tests {
    use std::future::Future;
    use std::future::pending;
    use std::task::Poll;
    use std::time::Duration;

    use axum::Error;
    use axum::extract::ws::Message;
    use tokio::sync::mpsc;
    use tokio::sync::oneshot;
    use tokio::task::yield_now;
    use tokio::time::timeout;
    use tokio_util::sync::CancellationToken;

    use super::WsSession;
    use crate::ws::WsSendQueue;

    fn session(writer: tokio::task::JoinHandle<()>, reader: tokio::task::JoinHandle<()>) -> WsSession {
        let (_incoming_tx, incoming) = mpsc::channel::<Result<Message, Error>>(1);
        WsSession {
            incoming,
            queue: WsSendQueue::new(1, 1024),
            shutdown: CancellationToken::new(),
            idle_timeout: Duration::from_secs(1),
            shutdown_timeout: Duration::from_millis(20),
            writer: Some(writer),
            reader: Some(reader),
        }
    }

    #[tokio::test]
    async fn close_timeout_aborts_and_consumes_only_pending_task() {
        let (started_tx, started_rx) = oneshot::channel();
        let writer = tokio::spawn(async move {
            let _ = started_tx.send(());
        });
        started_rx.await.expect("writer should start before close");
        while !writer.is_finished() {
            yield_now().await;
        }
        let reader = tokio::spawn(pending::<()>());
        let mut session = session(writer, reader);

        timeout(Duration::from_secs(1), session.close())
            .await
            .expect("close should respect its bounded wait");

        assert!(session.writer.is_none(), "completed writer handle should be consumed");
        assert!(session.reader.is_none(), "aborted reader handle should be consumed");
    }

    #[tokio::test]
    async fn cancelled_close_can_be_retried_after_one_task_was_consumed() {
        let (writer_done_tx, writer_done_rx) = oneshot::channel();
        let writer = tokio::spawn(async move {
            let _ = writer_done_tx.send(());
        });
        writer_done_rx.await.expect("writer should finish before close");
        while !writer.is_finished() {
            yield_now().await;
        }
        let (reader_release_tx, reader_release_rx) = oneshot::channel();
        let reader = tokio::spawn(async move {
            let _ = reader_release_rx.await;
        });
        let mut session = session(writer, reader);

        let mut close = Box::pin(session.close());
        std::future::poll_fn(|cx| {
            assert!(close.as_mut().poll(cx).is_pending());
            Poll::Ready(())
        })
        .await;
        drop(close);
        assert!(session.writer.is_none(), "ready writer handle should be consumed");
        assert!(
            session.reader.is_some(),
            "pending reader handle should remain retryable"
        );

        reader_release_tx.send(()).unwrap();
        timeout(Duration::from_secs(1), session.close())
            .await
            .expect("a later close call should finish remaining tasks");
        assert!(session.writer.is_none());
        assert!(session.reader.is_none());
        yield_now().await;
    }
}
