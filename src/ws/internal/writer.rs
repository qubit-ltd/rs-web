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
use futures_util::Sink;
use futures_util::SinkExt;
use tokio::select;
use tokio::sync::Notify;
use tokio::sync::oneshot;
use tokio::time::Instant;
use tokio::time::timeout_at;
use tokio_util::sync::CancellationToken;

use super::SessionLifecycle;
use super::SessionResources;
use super::close::get_close_deadline;
use super::close::send_close_and_wait;
use super::control_flush_outcome::ControlFlushOutcome;
use super::flush_outcome::FlushOutcome;
use super::send_outcome::SendOutcome;
use crate::ws::internal::CloseQueueOnDrop;
use crate::ws::ws_send_queue::WsSendQueue;
use crate::ws::ws_send_queue::message_size;

/// Drains the session queue through a sink until shutdown or a terminal error.
///
/// The sink is generic so tests can exercise actual writer-loop behavior with
/// deterministic readiness and flush results; production uses Axum's split
/// WebSocket sink.
pub(in crate::ws) async fn writer_loop<S>(
    mut sink: S,
    queue: WsSendQueue,
    notify: Arc<Notify>,
    lifecycle: SessionLifecycle,
    mut close_ack: oneshot::Receiver<()>,
    _resources: Arc<SessionResources>,
) where
    S: Sink<Message> + Unpin,
{
    let SessionLifecycle {
        shutdown,
        close_code,
        close_deadline,
        server_deadline,
        shutdown_timeout,
    } = lifecycle;
    let _close_queue_on_drop = CloseQueueOnDrop::new(queue.clone());
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
                    let deadline = get_close_deadline(&close_deadline, server_deadline.as_ref(), shutdown_timeout);
                    let _ = flush_until(&mut sink, &shutdown, Some(deadline)).await;
                    return;
                }
                _ = notify.notified() => {
                    let outcome = select! {
                        biased;
                        _ = &mut close_ack => ControlFlushOutcome::PeerCloseAcknowledged,
                        result = flush_until(&mut sink, &shutdown, None) => ControlFlushOutcome::Flushed(result),
                    };
                    match outcome {
                        ControlFlushOutcome::PeerCloseAcknowledged => {
                            let deadline = get_close_deadline(&close_deadline, server_deadline.as_ref(), shutdown_timeout);
                            let _ = flush_until(&mut sink, &shutdown, Some(deadline)).await;
                            return;
                        }
                        ControlFlushOutcome::Flushed(FlushOutcome::Flushed) => {}
                        ControlFlushOutcome::Flushed(FlushOutcome::Cancelled) => {
                            let deadline = get_close_deadline(&close_deadline, server_deadline.as_ref(), shutdown_timeout);
                            send_close_and_wait(&mut sink, &mut close_ack, close_code.load(Ordering::Acquire), deadline).await;
                            return;
                        }
                        ControlFlushOutcome::Flushed(FlushOutcome::Failed) => {
                            tracing::warn!(
                                event = "websocket_io_failure",
                                direction = "write",
                                operation = "flush"
                            );
                            shutdown.cancel();
                            return;
                        }
                        ControlFlushOutcome::Flushed(FlushOutcome::TimedOut) => {
                            shutdown.cancel();
                            return;
                        }
                    }
                }
            }
        };
        let Some(message) = message else { continue };
        let message_bytes = message_size(&message);
        let sent = select! {
            biased;
            _ = shutdown.cancelled() => SendOutcome::Shutdown,
            _ = &mut close_ack => SendOutcome::PeerCloseAcknowledged,
            result = sink.send(message) => {
                if result.is_ok() {
                    SendOutcome::Sent
                } else {
                    SendOutcome::SinkError
                }
            },
        };
        queue.finish_message(message_bytes);
        match sent {
            SendOutcome::Sent => {}
            SendOutcome::SinkError => {
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
                let _ = flush_until(&mut sink, &shutdown, Some(deadline)).await;
                return;
            }
            SendOutcome::Shutdown => {
                let deadline = get_close_deadline(&close_deadline, server_deadline.as_ref(), shutdown_timeout);
                send_close_and_wait(&mut sink, &mut close_ack, close_code.load(Ordering::Acquire), deadline).await;
                return;
            }
        }
    }
}

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
mod tests {
    use std::io::Write;
    use std::pin::Pin;
    use std::sync::Arc;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;
    use std::task::Context;
    use std::task::Poll;
    use std::time::Duration;

    use axum::extract::ws::Message;
    use futures_util::Sink;
    use futures_util::task::noop_waker_ref;
    use tokio::sync::Notify;
    use tokio::sync::Semaphore;
    use tokio::sync::oneshot;
    use tokio::time::timeout;
    use tokio_util::sync::CancellationToken;
    use tracing::Level;
    use tracing::instrument::WithSubscriber;
    use tracing_subscriber::fmt::MakeWriter;

    use super::FlushOutcome;
    use super::flush_until;
    use super::writer_loop;
    use crate::ServerOptions;
    use crate::WebServer;
    use crate::ws::WsSendQueue;
    use crate::ws::internal::SessionLifecycle;
    use crate::ws::internal::SessionResources;
    use crate::ws::ws_send_error::WsSendError;

    #[derive(Clone, Default)]
    struct SharedLog(Arc<std::sync::Mutex<Vec<u8>>>);

    struct LogWriter(Arc<std::sync::Mutex<Vec<u8>>>);

    impl Write for LogWriter {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(bytes);
            Ok(bytes.len())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    impl<'a> MakeWriter<'a> for SharedLog {
        type Writer = LogWriter;

        fn make_writer(&'a self) -> Self::Writer {
            LogWriter(self.0.clone())
        }
    }

    impl SharedLog {
        fn contents(&self) -> String {
            String::from_utf8_lossy(&self.0.lock().unwrap()).into_owned()
        }
    }

    #[derive(Clone, Copy)]
    enum FailurePoint {
        Ready,
        Send,
        Flush,
    }

    struct FailingSink {
        point: FailurePoint,
        failed: bool,
        notified: Arc<Notify>,
    }

    impl Sink<Message> for FailingSink {
        type Error = std::io::Error;

        fn poll_ready(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            if !self.failed && matches!(self.point, FailurePoint::Ready) {
                self.failed = true;
                self.notified.notify_one();
                return Poll::Ready(Err(std::io::Error::other("secret-sentinel ready failure")));
            }
            Poll::Ready(Ok(()))
        }

        fn start_send(mut self: Pin<&mut Self>, _item: Message) -> Result<(), Self::Error> {
            if !self.failed && matches!(self.point, FailurePoint::Send) {
                self.failed = true;
                self.notified.notify_one();
                return Err(std::io::Error::other("secret-sentinel send failure"));
            }
            Ok(())
        }

        fn poll_flush(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            if !self.failed && matches!(self.point, FailurePoint::Flush) {
                self.failed = true;
                self.notified.notify_one();
                return Poll::Ready(Err(std::io::Error::other("secret-sentinel flush failure")));
            }
            Poll::Ready(Ok(()))
        }

        fn poll_close(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }
    }

    async fn assert_writer_failure(point: FailurePoint) {
        let log = SharedLog::default();
        let dispatch = tracing::Dispatch::new(
            tracing_subscriber::fmt()
                .with_max_level(Level::DEBUG)
                .with_ansi(false)
                .with_writer(log.clone())
                .finish(),
        );
        let server = WebServer::bind_http(ServerOptions::new("127.0.0.1:0".parse().unwrap()))
            .await
            .expect("test server should bind");
        let context = server.context();
        let session = context.try_register_session().expect("session should register");
        let permits = Arc::new(Semaphore::new(1));
        let permit = permits
            .clone()
            .acquire_owned()
            .await
            .expect("permit should be available");
        let queue = WsSendQueue::new(2, 1024);
        queue
            .try_send(Message::text("queued"))
            .expect("initial message should be queued");
        let reached_failure = Arc::new(Notify::new());
        let shutdown = CancellationToken::new();
        let lifecycle = SessionLifecycle {
            shutdown: shutdown.clone(),
            close_code: Arc::new(std::sync::atomic::AtomicU16::new(1011)),
            close_deadline: Arc::new(std::sync::Mutex::new(None)),
            server_deadline: None,
            shutdown_timeout: Duration::from_millis(100),
        };
        let notify = queue.notify.clone();
        let sink = FailingSink {
            point,
            failed: false,
            notified: reached_failure.clone(),
        };
        let resources = Arc::new(SessionResources::new(permit, session));
        let (ack_tx, ack_rx) = oneshot::channel();
        let writer = tokio::spawn(
            async move {
                writer_loop(sink, queue.clone(), notify, lifecycle, ack_rx, resources).await;
                queue
            }
            .with_subscriber(dispatch),
        );

        timeout(Duration::from_secs(1), reached_failure.notified())
            .await
            .expect("writer must reach injected failure");
        let queue = timeout(Duration::from_secs(1), writer)
            .await
            .expect("writer must exit after sink failure")
            .expect("writer task must not panic");
        assert!(shutdown.is_cancelled(), "writer failure must cancel the session");
        let output = log.contents();
        let failures: Vec<_> = output
            .lines()
            .filter(|line| line.contains("websocket_io_failure"))
            .collect();
        assert_eq!(failures.len(), 1, "expected exactly one write failure event: {output}");
        assert!(
            failures[0].contains("direction=\"write\""),
            "unexpected direction: {output}"
        );
        assert!(
            failures[0].contains("operation=\"send\""),
            "unexpected operation: {output}"
        );
        assert!(
            !output.contains("secret-sentinel"),
            "underlying error text must be redacted: {output}"
        );
        assert_eq!(queue.try_send(Message::text("closed")), Err(WsSendError::Closed));
        let state = queue.state.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        assert!(state.messages.is_empty(), "pending messages must be discarded");
        assert_eq!(state.queued_bytes, 0, "queued byte budget must be returned");
        assert_eq!(state.in_flight_messages, 0, "in-flight message budget must be returned");
        drop(state);
        assert_eq!(context.active_sessions(), 0, "session guard must be released");
        assert_eq!(permits.available_permits(), 1, "connection permit must be released");
        drop(ack_tx);
        drop(server);
    }

    #[tokio::test]
    async fn test_writer_send_error_cancels_session() {
        for point in [FailurePoint::Ready, FailurePoint::Send, FailurePoint::Flush] {
            assert_writer_failure(point).await;
        }
    }

    #[tokio::test]
    async fn test_writer_control_flush_error_reports_flush_once() {
        let log = SharedLog::default();
        let dispatch = tracing::Dispatch::new(
            tracing_subscriber::fmt()
                .with_max_level(Level::DEBUG)
                .with_ansi(false)
                .with_writer(log.clone())
                .finish(),
        );
        let server = WebServer::bind_http(ServerOptions::new("127.0.0.1:0".parse().unwrap()))
            .await
            .expect("test server should bind");
        let session = server
            .context()
            .try_register_session()
            .expect("session should register");
        let permits = Arc::new(Semaphore::new(1));
        let permit = permits
            .clone()
            .acquire_owned()
            .await
            .expect("permit should be available");
        let queue = WsSendQueue::new(1, 128);
        let notify = queue.notify.clone();
        let writer_notify = notify.clone();
        let lifecycle = SessionLifecycle {
            shutdown: CancellationToken::new(),
            close_code: Arc::new(std::sync::atomic::AtomicU16::new(1000)),
            close_deadline: Arc::new(std::sync::Mutex::new(None)),
            server_deadline: None,
            shutdown_timeout: Duration::from_millis(100),
        };
        let resources = Arc::new(SessionResources::new(permit, session));
        let (ack_tx, ack_rx) = oneshot::channel();
        let writer = tokio::spawn(
            async move {
                writer_loop(
                    ReadyFlush(Some(Err(std::io::Error::other("secret-sentinel flush failure")))),
                    queue,
                    writer_notify,
                    lifecycle,
                    ack_rx,
                    resources,
                )
                .await;
            }
            .with_subscriber(dispatch),
        );
        notify.notify_one();
        timeout(Duration::from_secs(1), writer)
            .await
            .expect("writer should exit after control flush error")
            .expect("writer should not panic");
        let output = log.contents();
        let failures: Vec<_> = output
            .lines()
            .filter(|line| line.contains("websocket_io_failure"))
            .collect();
        assert_eq!(failures.len(), 1, "expected exactly one flush failure event: {output}");
        assert!(
            failures[0].contains("operation=\"flush\""),
            "unexpected operation: {output}"
        );
        assert!(
            failures[0].contains("direction=\"write\""),
            "unexpected direction: {output}"
        );
        assert!(
            !output.contains("secret-sentinel"),
            "underlying error text must be redacted: {output}"
        );
        drop(ack_tx);
        drop(server);
    }

    #[tokio::test]
    async fn test_writer_clean_shutdown_does_not_report_failure() {
        let log = SharedLog::default();
        let dispatch = tracing::Dispatch::new(
            tracing_subscriber::fmt()
                .with_max_level(Level::DEBUG)
                .with_ansi(false)
                .with_writer(log.clone())
                .finish(),
        );
        let server = WebServer::bind_http(ServerOptions::new("127.0.0.1:0".parse().unwrap()))
            .await
            .expect("test server should bind");
        let session = server
            .context()
            .try_register_session()
            .expect("session should register");
        let permits = Arc::new(Semaphore::new(1));
        let permit = permits
            .clone()
            .acquire_owned()
            .await
            .expect("permit should be available");
        let queue = WsSendQueue::new(1, 128);
        let notify = queue.notify.clone();
        let shutdown = CancellationToken::new();
        shutdown.cancel();
        let lifecycle = SessionLifecycle {
            shutdown,
            close_code: Arc::new(std::sync::atomic::AtomicU16::new(1001)),
            close_deadline: Arc::new(std::sync::Mutex::new(None)),
            server_deadline: None,
            shutdown_timeout: Duration::from_millis(100),
        };
        let resources = Arc::new(SessionResources::new(permit, session));
        let (ack_tx, ack_rx) = oneshot::channel();
        ack_tx.send(()).expect("writer should retain acknowledgement receiver");
        tokio::spawn(
            async move {
                writer_loop(SuccessfulSink, queue, notify, lifecycle, ack_rx, resources).await;
            }
            .with_subscriber(dispatch),
        )
        .await
        .expect("writer should not panic");
        let output = log.contents();
        assert!(
            !output.contains("websocket_io_failure"),
            "clean shutdown must not log a failure: {output}"
        );
        drop(server);
    }

    struct CloseFailureSink {
        fail_send: bool,
        fail_post_ack_flush: bool,
        flush_count: usize,
    }

    impl Sink<Message> for CloseFailureSink {
        type Error = std::io::Error;

        fn poll_ready(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }

        fn start_send(self: Pin<&mut Self>, item: Message) -> Result<(), Self::Error> {
            if self.fail_send && matches!(item, Message::Close(_)) {
                return Err(std::io::Error::other("secret-sentinel close send error"));
            }
            Ok(())
        }

        fn poll_flush(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            let flush_count = self.flush_count;
            self.flush_count += 1;
            if self.fail_post_ack_flush && flush_count > 0 {
                return Poll::Ready(Err(std::io::Error::other("secret-sentinel close flush error")));
            }
            Poll::Ready(Ok(()))
        }

        fn poll_close(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }
    }

    async fn assert_close_failure_reports(operation: &str, fail_send: bool, fail_post_ack_flush: bool) {
        let log = SharedLog::default();
        let dispatch = tracing::Dispatch::new(
            tracing_subscriber::fmt()
                .with_max_level(Level::DEBUG)
                .with_ansi(false)
                .with_writer(log.clone())
                .finish(),
        );
        let server = WebServer::bind_http(ServerOptions::new("127.0.0.1:0".parse().unwrap()))
            .await
            .expect("test server should bind");
        let session = server
            .context()
            .try_register_session()
            .expect("session should register");
        let permits = Arc::new(Semaphore::new(1));
        let permit = permits
            .clone()
            .acquire_owned()
            .await
            .expect("permit should be available");
        let queue = WsSendQueue::new(1, 128);
        let notify = queue.notify.clone();
        let shutdown = CancellationToken::new();
        shutdown.cancel();
        let lifecycle = SessionLifecycle {
            shutdown,
            close_code: Arc::new(std::sync::atomic::AtomicU16::new(1001)),
            close_deadline: Arc::new(std::sync::Mutex::new(None)),
            server_deadline: None,
            shutdown_timeout: Duration::from_millis(100),
        };
        let resources = Arc::new(SessionResources::new(permit, session));
        let (ack_tx, ack_rx) = oneshot::channel();
        ack_tx.send(()).expect("writer should retain acknowledgement receiver");
        let writer = tokio::spawn(
            async move {
                writer_loop(
                    CloseFailureSink {
                        fail_send,
                        fail_post_ack_flush,
                        flush_count: 0,
                    },
                    queue,
                    notify,
                    lifecycle,
                    ack_rx,
                    resources,
                )
                .await;
            }
            .with_subscriber(dispatch),
        );
        timeout(Duration::from_secs(1), writer)
            .await
            .expect("writer should exit after close handshake")
            .expect("writer should not panic");
        let output = log.contents();
        let failures: Vec<_> = output
            .lines()
            .filter(|line| line.contains("websocket_io_failure"))
            .collect();
        assert_eq!(failures.len(), 1, "expected exactly one close failure event: {output}");
        assert!(
            failures[0].contains(&format!("operation=\"{operation}\"")),
            "unexpected operation: {output}"
        );
        assert!(
            failures[0].contains("direction=\"write\""),
            "unexpected direction: {output}"
        );
        assert!(
            !output.contains("secret-sentinel"),
            "underlying error must be redacted: {output}"
        );
        drop(server);
    }

    #[tokio::test]
    async fn test_writer_close_send_error_reports_send_once() {
        assert_close_failure_reports("send", true, false).await;
    }

    #[tokio::test]
    async fn test_writer_post_ack_close_flush_error_reports_flush_once() {
        assert_close_failure_reports("flush", false, true).await;
    }

    #[tokio::test]
    async fn test_writer_close_ack_timeout_does_not_report_failure() {
        let log = SharedLog::default();
        let dispatch = tracing::Dispatch::new(
            tracing_subscriber::fmt()
                .with_max_level(Level::DEBUG)
                .with_ansi(false)
                .with_writer(log.clone())
                .finish(),
        );
        let server = WebServer::bind_http(ServerOptions::new("127.0.0.1:0".parse().unwrap()))
            .await
            .expect("test server should bind");
        let session = server
            .context()
            .try_register_session()
            .expect("session should register");
        let permits = Arc::new(Semaphore::new(1));
        let permit = permits
            .clone()
            .acquire_owned()
            .await
            .expect("permit should be available");
        let queue = WsSendQueue::new(1, 128);
        let notify = queue.notify.clone();
        let shutdown = CancellationToken::new();
        shutdown.cancel();
        let lifecycle = SessionLifecycle {
            shutdown,
            close_code: Arc::new(std::sync::atomic::AtomicU16::new(1001)),
            close_deadline: Arc::new(std::sync::Mutex::new(None)),
            server_deadline: None,
            shutdown_timeout: Duration::from_millis(20),
        };
        let resources = Arc::new(SessionResources::new(permit, session));
        let (_ack_tx, ack_rx) = oneshot::channel();
        let writer = tokio::spawn(
            async move {
                writer_loop(SuccessfulSink, queue, notify, lifecycle, ack_rx, resources).await;
            }
            .with_subscriber(dispatch),
        );
        timeout(Duration::from_secs(1), writer)
            .await
            .expect("writer should stop after acknowledgement timeout")
            .expect("writer should not panic");
        let output = log.contents();
        assert!(
            !output.contains("websocket_io_failure"),
            "waiting for close acknowledgement must not log a failure: {output}"
        );
        drop(server);
    }

    struct PendingFlush {
        polled: std::sync::Arc<Notify>,
    }

    impl Sink<Message> for PendingFlush {
        type Error = std::io::Error;

        fn poll_ready(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }

        fn start_send(self: Pin<&mut Self>, _item: Message) -> Result<(), Self::Error> {
            Ok(())
        }

        fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            self.get_mut().polled.notify_one();
            Poll::Pending
        }

        fn poll_close(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }
    }

    struct ReadyFlush(Option<Result<(), std::io::Error>>);

    impl Sink<Message> for ReadyFlush {
        type Error = std::io::Error;

        fn poll_ready(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }

        fn start_send(self: Pin<&mut Self>, _item: Message) -> Result<(), Self::Error> {
            Ok(())
        }

        fn poll_flush(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(self.0.take().expect("poll_flush should be called once"))
        }

        fn poll_close(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }
    }

    struct SuccessfulSink;

    impl Sink<Message> for SuccessfulSink {
        type Error = std::io::Error;

        fn poll_ready(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }

        fn start_send(self: Pin<&mut Self>, _item: Message) -> Result<(), Self::Error> {
            Ok(())
        }

        fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }

        fn poll_close(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }
    }

    struct PendingSend {
        pending_flush: Arc<Notify>,
        flush_polls: Arc<AtomicUsize>,
    }

    impl Sink<Message> for PendingSend {
        type Error = std::io::Error;

        fn poll_ready(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }

        fn start_send(self: Pin<&mut Self>, _item: Message) -> Result<(), Self::Error> {
            Ok(())
        }

        fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            if self.flush_polls.fetch_add(1, Ordering::SeqCst) == 0 {
                self.pending_flush.notify_one();
            }
            Poll::Pending
        }

        fn poll_close(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }
    }

    #[tokio::test]
    async fn pending_flush_is_cancelled() {
        let polled = std::sync::Arc::new(Notify::new());
        let sink = PendingFlush { polled: polled.clone() };
        let token = CancellationToken::new();
        let task_token = token.clone();
        let task = tokio::spawn(async move {
            let mut sink = sink;
            flush_until(&mut sink, &task_token, None).await
        });

        polled.notified().await;
        assert!(!task.is_finished(), "flush should remain pending before cancellation");
        token.cancel();
        let result = timeout(Duration::from_secs(1), task)
            .await
            .expect("flush task should stop promptly")
            .expect("flush task should not panic");
        assert_eq!(result, FlushOutcome::Cancelled);
    }

    #[tokio::test]
    async fn pending_flush_obeys_deadline() {
        let mut sink = PendingFlush {
            polled: std::sync::Arc::new(Notify::new()),
        };
        let token = CancellationToken::new();
        let result = flush_until(&mut sink, &token, Some(tokio::time::Instant::now())).await;
        assert_eq!(result, FlushOutcome::TimedOut);
    }

    #[tokio::test]
    async fn ready_flush_succeeds() {
        let mut sink = ReadyFlush(Some(Ok(())));
        let token = CancellationToken::new();
        let result = flush_until(&mut sink, &token, None).await;
        assert_eq!(result, FlushOutcome::Flushed);
    }

    #[tokio::test]
    async fn failed_flush_is_reported() {
        let mut sink = ReadyFlush(Some(Err(std::io::Error::other("injected"))));
        let token = CancellationToken::new();
        let result = flush_until(&mut sink, &token, None).await;
        assert_eq!(result, FlushOutcome::Failed);
    }

    #[tokio::test]
    async fn writer_pending_send_shutdown_releases_resources() {
        let server = WebServer::bind_http(ServerOptions::new("127.0.0.1:0".parse().unwrap()))
            .await
            .expect("test server should bind");
        let context = server.context();
        let session = context.try_register_session().expect("session should register");
        let permits = Arc::new(Semaphore::new(1));
        let permit = permits
            .clone()
            .acquire_owned()
            .await
            .expect("permit should be available");
        let queue = WsSendQueue::new(2, 1024);
        queue
            .try_send(Message::binary(vec![1; 128]))
            .expect("message should be queued");
        let pending_flush = Arc::new(Notify::new());
        let flush_polls = Arc::new(AtomicUsize::new(0));
        let sink = PendingSend {
            pending_flush: pending_flush.clone(),
            flush_polls: flush_polls.clone(),
        };
        let shutdown = CancellationToken::new();
        let close_deadline = Arc::new(std::sync::Mutex::new(None));
        let resources = Arc::new(SessionResources::new(permit, session));
        let lifecycle = SessionLifecycle {
            shutdown: shutdown.clone(),
            close_code: Arc::new(std::sync::atomic::AtomicU16::new(1001)),
            close_deadline,
            server_deadline: None,
            shutdown_timeout: Duration::from_millis(100),
        };
        let notify = queue.notify.clone();
        let writer = tokio::spawn(async move {
            let (_ack_tx, ack_rx) = oneshot::channel();
            writer_loop(sink, queue.clone(), notify, lifecycle, ack_rx, resources).await;
            queue
        });

        timeout(Duration::from_secs(1), pending_flush.notified())
            .await
            .expect("actual writer send must reach pending poll_flush");
        assert!(flush_polls.load(Ordering::SeqCst) > 0);
        shutdown.cancel();
        let queue = timeout(Duration::from_secs(2), writer)
            .await
            .expect("writer should stop within the close deadline")
            .expect("writer task should not panic");

        assert!(queue.is_empty(), "in-flight queue budget should be returned");
        assert_eq!(queue.try_send(Message::text("closed")), Err(WsSendError::Closed));
        assert_eq!(context.active_sessions(), 0, "session guard should be released");
        assert_eq!(permits.available_permits(), 1, "connection permit should be released");
        drop(server);
    }

    #[tokio::test]
    async fn writer_pending_send_peer_close_ack_does_not_cancel_session() {
        let server = WebServer::bind_http(ServerOptions::new("127.0.0.1:0".parse().unwrap()))
            .await
            .expect("test server should bind");
        let context = server.context();
        let session = context.try_register_session().expect("session should register");
        let permits = Arc::new(Semaphore::new(1));
        let permit = permits
            .clone()
            .acquire_owned()
            .await
            .expect("permit should be available");
        let queue = WsSendQueue::new(2, 1024);
        queue
            .try_send(Message::binary(vec![1; 128]))
            .expect("message should be queued");
        let pending_flush = Arc::new(Notify::new());
        let flush_polls = Arc::new(AtomicUsize::new(0));
        let sink = PendingSend {
            pending_flush: pending_flush.clone(),
            flush_polls: flush_polls.clone(),
        };
        let shutdown = CancellationToken::new();
        let resources = Arc::new(SessionResources::new(permit, session));
        let lifecycle = SessionLifecycle {
            shutdown: shutdown.clone(),
            close_code: Arc::new(std::sync::atomic::AtomicU16::new(1000)),
            close_deadline: Arc::new(std::sync::Mutex::new(None)),
            server_deadline: None,
            shutdown_timeout: Duration::from_millis(100),
        };
        let notify = queue.notify.clone();
        let (ack_tx, ack_rx) = oneshot::channel();
        let writer = tokio::spawn(async move {
            writer_loop(sink, queue.clone(), notify, lifecycle, ack_rx, resources).await;
            queue
        });

        timeout(Duration::from_secs(1), pending_flush.notified())
            .await
            .expect("writer must reach pending send flush before peer acknowledgement");
        assert!(flush_polls.load(Ordering::SeqCst) > 0);
        ack_tx.send(()).expect("peer acknowledgement receiver remains alive");
        let queue = timeout(Duration::from_secs(2), writer)
            .await
            .expect("writer should stop within the close deadline")
            .expect("writer task should not panic");

        assert!(
            !shutdown.is_cancelled(),
            "peer acknowledgement must not cancel the session"
        );
        assert_eq!(queue.try_send(Message::text("closed")), Err(WsSendError::Closed));
        let state = queue.state.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        assert!(state.messages.is_empty(), "pending messages must be discarded");
        assert_eq!(state.queued_bytes, 0, "queued byte budget must be returned");
        assert_eq!(state.in_flight_messages, 0, "in-flight message budget must be returned");
        drop(state);
        assert_eq!(context.active_sessions(), 0, "session guard should be released");
        assert_eq!(permits.available_permits(), 1, "connection permit should be released");
        drop(server);
    }

    #[tokio::test]
    async fn test_writer_notify_flush_peer_close_releases_resources() {
        assert_notify_flush_peer_close(None, false, false).await;
    }

    #[tokio::test]
    async fn writer_notify_flush_close_ack_sender_drop_releases_resources() {
        assert_notify_flush_peer_close(None, true, false).await;
    }

    #[tokio::test]
    async fn writer_notify_flush_close_ack_uses_expired_server_deadline() {
        assert_notify_flush_peer_close(Some(tokio::time::Instant::now()), false, false).await;
    }

    #[tokio::test]
    async fn writer_notify_flush_close_ack_and_cancellation_simultaneously_ready() {
        assert_notify_flush_peer_close(None, false, true).await;
    }

    async fn assert_notify_flush_peer_close(
        server_deadline: Option<tokio::time::Instant>,
        drop_ack_sender: bool,
        cancel_simultaneously: bool,
    ) {
        let server = WebServer::bind_http(ServerOptions::new("127.0.0.1:0".parse().unwrap()))
            .await
            .expect("test server should bind");
        let context = server.context();
        let session = context.try_register_session().expect("session should register");
        let permits = Arc::new(Semaphore::new(1));
        let permit = permits
            .clone()
            .acquire_owned()
            .await
            .expect("permit should be available");
        let queue = WsSendQueue::new(2, 1024);
        let polled = Arc::new(Notify::new());
        let sink = PendingFlush { polled: polled.clone() };
        let shutdown = CancellationToken::new();
        let lifecycle = SessionLifecycle {
            shutdown: shutdown.clone(),
            close_code: Arc::new(std::sync::atomic::AtomicU16::new(1000)),
            close_deadline: Arc::new(std::sync::Mutex::new(None)),
            server_deadline: server_deadline.map(|deadline| Arc::new(std::sync::Mutex::new(Some(deadline)))),
            shutdown_timeout: Duration::from_millis(100),
        };
        let notify = queue.notify.clone();
        let wake_writer = notify.clone();
        let resources = Arc::new(SessionResources::new(permit, session));
        let (ack_tx, ack_rx) = oneshot::channel();
        let writer = tokio::spawn(async move {
            writer_loop(sink, queue.clone(), notify, lifecycle, ack_rx, resources).await;
            queue
        });
        wake_writer.notify_one();
        timeout(Duration::from_secs(1), polled.notified())
            .await
            .expect("writer should start a notify-triggered flush");
        assert!(!writer.is_finished(), "pending flush should keep writer alive");
        if cancel_simultaneously {
            shutdown.cancel();
        }
        if drop_ack_sender {
            drop(ack_tx);
        } else {
            ack_tx.send(()).expect("ack receiver should still be alive");
        }

        let queue = timeout(Duration::from_millis(250), writer)
            .await
            .expect("writer should exit after peer close acknowledgement")
            .expect("writer should not panic");
        assert_eq!(
            shutdown.is_cancelled(),
            cancel_simultaneously,
            "writer must not add cancellation"
        );
        assert!(queue.is_empty(), "queue budgets should be returned");
        assert_eq!(queue.try_send(Message::text("closed")), Err(WsSendError::Closed));
        let state = queue.state.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        assert_eq!(state.queued_bytes, 0, "queued byte budget should be returned");
        assert_eq!(
            state.in_flight_messages, 0,
            "in-flight message budget should be returned"
        );
        drop(state);
        assert_eq!(permits.available_permits(), 1, "policy permit should be returned");
        assert_eq!(context.active_sessions(), 0, "session guard should be released");
        drop(server);
    }

    #[test]
    fn test_sinks_accept_and_close_messages() {
        let mut context = Context::from_waker(noop_waker_ref());
        let mut pending = PendingFlush {
            polled: std::sync::Arc::new(Notify::new()),
        };
        let mut ready = ReadyFlush(Some(Ok(())));

        assert!(Pin::new(&mut pending).poll_ready(&mut context).is_ready());
        Pin::new(&mut pending).start_send(Message::Text("test".into())).unwrap();
        assert!(Pin::new(&mut pending).poll_close(&mut context).is_ready());

        assert!(Pin::new(&mut ready).poll_ready(&mut context).is_ready());
        Pin::new(&mut ready).start_send(Message::Text("test".into())).unwrap();
        assert!(Pin::new(&mut ready).poll_close(&mut context).is_ready());
    }
}
