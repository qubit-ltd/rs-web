// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Application-facing WebSocket session handle.

use std::sync::Arc;
use std::time::Duration;

use axum::Error;
use axum::extract::ws::Message;
use tokio::join;
use tokio::select;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio::time::timeout;
use tokio::time::timeout_at;
use tokio_util::sync::CancellationToken;

use super::internal::InboundMessage;
use super::internal::InboundState;
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
/// async fn stop_cancellable_work(mut session: WsSession) {
///     let ended = session.closed();
///     tokio::select! {
///         _ = ended => session.close().await,
///         _ = std::future::pending::<()>() => {},
///     }
/// }
///
/// let _handler = consume_messages;
/// let _stop_work = stop_cancellable_work;
/// ```
pub struct WsSession {
    /// Channel carrying inbound frames from the reader task.
    pub(super) incoming: mpsc::Receiver<InboundMessage>,
    /// Shared budget and first read failure from the reader task.
    pub(super) inbound_state: Arc<InboundState>,
    /// Distinguishes application close from reader-triggered cancellation.
    pub(super) explicitly_closed: bool,
    /// Shared bounded queue drained by the writer task.
    pub(super) queue: WsSendQueue,
    /// Cancellation token shared by the application and both tasks.
    pub(super) shutdown: CancellationToken,
    /// Completes after both I/O tasks release their managed resources.
    pub(super) closed: CancellationToken,
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
    /// Returns an owned future that completes after the reader and writer have
    /// both released this session's managed resources.
    ///
    /// The future can be created before or after completion, cloned through
    /// multiple calls, and safely cancelled without affecting either I/O task.
    /// Completion means the connection permit and server session registration
    /// have been released. It does not mean a Close frame reached the peer,
    /// application work or the handler finished, or queued inbound messages
    /// were consumed. This notification does not cancel an existing handler.
    ///
    /// # Returns
    ///
    /// A `Send + 'static` future that can be selected alongside other work
    /// without borrowing this session.
    pub fn closed(&self) -> impl std::future::Future<Output = ()> + Send + 'static {
        let closed = self.closed.clone();
        async move { closed.cancelled().await }
    }

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
        if self.explicitly_closed {
            return None;
        }
        let message = match tokio::time::Instant::now().checked_add(self.idle_timeout) {
            Some(deadline) => select! {
                biased;
                _ = self.shutdown.cancelled() => None,
                message = timeout_at(deadline, self.incoming.recv()) => {
                    match message {
                        Ok(message) => message.map(InboundMessage::into_message),
                        Err(_) => None,
                    }
                }
            },
            None => None,
        };
        if message.is_some() {
            return message.map(Ok);
        }
        self.cleanup().await;
        if self.explicitly_closed {
            self.inbound_state.clear_read_error();
            None
        } else {
            self.inbound_state.take_read_error().map(Err)
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
    /// Returns [`WsSendError::Closed`] after shutdown,
    /// [`WsSendError::MessageTooLarge`] if the message exceeds the total byte
    /// budget, or [`WsSendError::Backpressure`] if the queue lacks message
    /// count or remaining byte capacity. A message accepted before shutdown
    /// may be discarded if it is still pending when the session closes;
    /// queue checks follow the order listed, after the session shutdown check.
    /// Success does not confirm network delivery.
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
        self.explicitly_closed = true;
        self.inbound_state.clear_read_error();
        self.cleanup().await;
    }

    async fn cleanup(&mut self) {
        self.queue.close();
        self.shutdown.cancel();
        self.incoming.close();
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
        while self.incoming.try_recv().is_ok() {}
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
    use std::sync::Arc;
    use std::task::Poll;
    use std::time::Duration;

    use axum::extract::ws::Message;
    use tokio::sync::mpsc;
    use tokio::sync::oneshot;
    use tokio::task::yield_now;
    use tokio::time::timeout;
    use tokio_util::sync::CancellationToken;

    use super::InboundMessage;
    use super::InboundState;
    use super::WsSession;
    use crate::ws::WsSendError;
    use crate::ws::WsSendQueue;

    fn session(writer: tokio::task::JoinHandle<()>, reader: tokio::task::JoinHandle<()>) -> WsSession {
        let (_incoming_tx, incoming) = mpsc::channel::<InboundMessage>(1);
        WsSession {
            incoming,
            inbound_state: Arc::new(InboundState::new(1024)),
            explicitly_closed: false,
            queue: WsSendQueue::new(1, 1024),
            shutdown: CancellationToken::new(),
            closed: CancellationToken::new(),
            idle_timeout: Duration::from_secs(1),
            shutdown_timeout: Duration::from_millis(20),
            writer: Some(writer),
            reader: Some(reader),
        }
    }

    #[tokio::test]
    async fn try_send_forwards_queue_error_and_prefers_session_shutdown() {
        let closed = session(tokio::spawn(pending::<()>()), tokio::spawn(pending::<()>()));
        closed.queue.close();
        assert_eq!(closed.try_send(Message::text("oversized")), Err(WsSendError::Closed));

        let mut active = session(tokio::spawn(pending::<()>()), tokio::spawn(pending::<()>()));
        active.queue = WsSendQueue::new(1, 3);
        assert_eq!(
            active.try_send(Message::text("four")),
            Err(WsSendError::MessageTooLarge)
        );
        active.shutdown.cancel();
        assert_eq!(active.try_send(Message::text("four")), Err(WsSendError::Closed));
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

    #[tokio::test]
    async fn cancelled_recv_cleanup_can_be_retried_to_deliver_read_error() {
        let writer = tokio::spawn(async {});
        while !writer.is_finished() {
            yield_now().await;
        }
        let (reader_release_tx, reader_release_rx) = oneshot::channel();
        let reader = tokio::spawn(async move {
            let _ = reader_release_rx.await;
        });
        let mut session = session(writer, reader);
        session
            .inbound_state
            .record_read_error(axum::Error::new(std::io::Error::other("read failed")));
        session.shutdown.cancel();

        let mut recv = Box::pin(session.recv());
        std::future::poll_fn(|cx| {
            assert!(recv.as_mut().poll(cx).is_pending());
            Poll::Ready(())
        })
        .await;
        drop(recv);
        assert!(
            session.reader.is_some(),
            "unfinished reader handle must remain retryable"
        );

        reader_release_tx.send(()).unwrap();
        let result = timeout(Duration::from_secs(1), session.recv())
            .await
            .expect("retried recv should finish reader cleanup");
        assert!(
            matches!(result, Some(Err(_))),
            "first read error should survive cancellation"
        );
        assert!(session.recv().await.is_none(), "read error should be consumed once");
    }

    #[tokio::test]
    async fn unrepresentable_recv_deadline_returns_none_and_cleans_up() {
        let (incoming_tx, incoming) = mpsc::channel(1);
        let mut session = WsSession {
            incoming,
            inbound_state: Arc::new(InboundState::new(1024)),
            explicitly_closed: false,
            queue: WsSendQueue::new(1, 1024),
            shutdown: CancellationToken::new(),
            closed: CancellationToken::new(),
            idle_timeout: Duration::MAX,
            shutdown_timeout: Duration::from_millis(20),
            writer: Some(tokio::spawn(async {})),
            reader: Some(tokio::spawn(async {})),
        };

        let message = timeout(Duration::from_millis(100), session.recv())
            .await
            .expect("unrepresentable recv deadline must return promptly");

        assert!(message.is_none());
        assert!(session.writer.is_none());
        assert!(session.reader.is_none());
        drop(incoming_tx);
    }
}
