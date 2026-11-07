// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Outbound WebSocket writer loop.

#[cfg(test)]
use std::pin::Pin;
use std::sync::Arc;
#[cfg(test)]
use std::sync::Mutex;
#[cfg(test)]
use std::sync::OnceLock;
use std::sync::atomic::Ordering;
#[cfg(test)]
use std::task::Context;
#[cfg(test)]
use std::task::Poll;
use std::time::Duration;

use axum::extract::ws::Message;
use futures_util::Sink;
use futures_util::SinkExt;
use tokio::select;
use tokio::sync::Notify;
#[cfg(test)]
use tokio::sync::mpsc::UnboundedSender;
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

#[cfg(test)]
#[derive(Clone)]
struct TestPendingObserver {
    marker: Arc<[u8]>,
    events: UnboundedSender<TestWriterEvent>,
}

#[cfg(test)]
#[derive(Clone, Copy, Debug)]
enum TestWriterEvent {
    SendStarted(Instant),
    FlushPending(Instant),
    FlushReady(Instant),
}

#[cfg(test)]
static TEST_PENDING_OBSERVER: OnceLock<Mutex<Option<TestPendingObserver>>> = OnceLock::new();

#[cfg(test)]
fn current_test_pending_observer() -> Option<TestPendingObserver> {
    TEST_PENDING_OBSERVER
        .get_or_init(|| Mutex::new(None))
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .clone()
}

#[cfg(test)]
struct TestObservedSink<S> {
    inner: S,
    observer: Option<TestPendingObserver>,
    target_send: bool,
}

#[cfg(test)]
impl<S> TestObservedSink<S> {
    fn new(inner: S, observer: Option<TestPendingObserver>) -> Self {
        Self {
            inner,
            observer,
            target_send: false,
        }
    }
}

#[cfg(test)]
impl<S> Sink<Message> for TestObservedSink<S>
where
    S: Sink<Message> + Unpin,
{
    type Error = S::Error;

    fn poll_ready(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Pin::new(&mut self.inner).poll_ready(cx)
    }

    fn start_send(mut self: Pin<&mut Self>, message: Message) -> Result<(), Self::Error> {
        let observer = self.observer.clone();
        if let (Some(observer), Message::Binary(payload)) = (observer, &message)
            && payload.starts_with(&observer.marker)
        {
            self.target_send = true;
            let _ = observer.events.send(TestWriterEvent::SendStarted(Instant::now()));
        }
        Pin::new(&mut self.inner).start_send(message)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let result = Pin::new(&mut self.inner).poll_flush(cx);
        if self.target_send
            && let Some(observer) = &self.observer
        {
            let event = match &result {
                Poll::Pending => TestWriterEvent::FlushPending(Instant::now()),
                Poll::Ready(_) => TestWriterEvent::FlushReady(Instant::now()),
            };
            let _ = observer.events.send(event);
        }
        result
    }

    fn poll_close(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Pin::new(&mut self.inner).poll_close(cx)
    }
}

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
/// * `idle_timeout` - Maximum duration for one ordinary send or flush attempt.
/// * `lifecycle` - Shared cancellation, close-code, and deadline state.
/// * `close_ack` - Signal indicating that the peer acknowledged the close
/// * `_resources` - Connection permit and session guard retained for this task.
pub(in crate::ws) async fn writer_loop<S>(
    #[allow(unused_mut)] mut sink: S,
    queue: WsSendQueue,
    notify: Arc<Notify>,
    idle_timeout: Duration,
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
                    if matches!(flush_until(&mut sink, &shutdown, Some(deadline)).await, FlushOutcome::Failed) {
                        tracing::warn!(event = "websocket_io_failure", direction = "write", operation = "flush");
                    }
                    return;
                }
                _ = notify.notified() => {
                    let flush_deadline = operation_deadline(idle_timeout);
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
                            send_close_and_wait(&mut sink, &mut close_ack, close_code.load(Ordering::Acquire), deadline).await;
                            return;
                        }
                        ControlFlushOutcome::Flushed(FlushOutcome::Failed) => {
                            tracing::warn!(event = "websocket_io_failure", direction = "write", operation = "flush");
                            shutdown.cancel();
                            return;
                        }
                        ControlFlushOutcome::Flushed(FlushOutcome::TimedOut) => {
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
                tracing::warn!(event = "websocket_io_failure", direction = "write", operation = "send");
            }
            return;
        }
        let send_deadline = operation_deadline(idle_timeout);
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
                send_close_and_wait(&mut sink, &mut close_ack, close_code.load(Ordering::Acquire), deadline).await;
                return;
            }
            SendOutcome::TimedOut => {
                queue.close();
                let _ = get_close_deadline(&close_deadline, server_deadline.as_ref(), shutdown_timeout);
                shutdown.cancel();
                return;
            }
        }
    }
}

/// Computes one operation's absolute write deadline, using an immediate
/// deadline if the configured duration cannot be represented from now.
/// # Parameters
///
/// * `idle_timeout` - Maximum duration allowed for the ordinary write
///   operation.
/// # Returns
///
/// The absolute deadline, or the current instant if checked addition overflows.
fn operation_deadline(idle_timeout: Duration) -> Instant {
    Instant::now().checked_add(idle_timeout).unwrap_or_else(Instant::now)
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
mod tests {
    use std::io::Write;
    use std::net::SocketAddr;
    use std::pin::Pin;
    use std::sync::Arc;
    use std::sync::Mutex;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;
    use std::task::Context;
    use std::task::Poll;
    use std::time::Duration;

    use axum::Router;
    use axum::extract::State;
    use axum::extract::WebSocketUpgrade;
    use axum::extract::ws::Message;
    use axum::http::HeaderMap;
    use axum::http::StatusCode;
    use axum::response::Response;
    use axum::routing::get;
    use futures_util::Sink;
    use futures_util::task::noop_waker_ref;
    use tokio::io::AsyncReadExt;
    use tokio::io::AsyncWriteExt;
    use tokio::net::TcpStream;
    use tokio::sync::Notify;
    use tokio::sync::Semaphore;
    use tokio::sync::oneshot;
    use tokio::time::timeout;
    use tokio_util::sync::CancellationToken;
    use tracing::Level;
    use tracing::instrument::WithSubscriber;
    use tracing_subscriber::fmt::MakeWriter;

    use super::FlushOutcome;
    use super::TEST_PENDING_OBSERVER;
    use super::TestPendingObserver;
    use super::TestWriterEvent;
    use super::flush_until;
    use super::operation_deadline;
    use super::writer_loop;
    use crate::ServerOptions;
    use crate::WebServer;
    use crate::ws::WsSendQueue;
    use crate::ws::WsUpgradePolicy;
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

    struct RecordingSink(Arc<std::sync::Mutex<Vec<Message>>>);

    impl Sink<Message> for RecordingSink {
        type Error = std::io::Error;

        fn poll_ready(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }

        fn start_send(self: Pin<&mut Self>, message: Message) -> Result<(), Self::Error> {
            self.0.lock().unwrap().push(message);
            Ok(())
        }

        fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }

        fn poll_close(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }
    }

    #[tokio::test]
    async fn test_writer_close_preserves_frame_drops_pending_data_and_times_out_without_ack() {
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
        let queue = WsSendQueue::new(2, 128);
        queue
            .try_send(Message::Close(Some(axum::extract::ws::CloseFrame {
                code: 4001,
                reason: "custom reason".into(),
            })))
            .expect("custom Close should be queued");
        queue
            .try_send(Message::text("discard after Close"))
            .expect("following data should fit before the writer starts Close");
        let recorded = Arc::new(std::sync::Mutex::new(Vec::new()));
        let shutdown = CancellationToken::new();
        let lifecycle = SessionLifecycle {
            shutdown: shutdown.clone(),
            close_code: Arc::new(std::sync::atomic::AtomicU16::new(1001)),
            close_deadline: Arc::new(std::sync::Mutex::new(None)),
            server_deadline: None,
            shutdown_timeout: Duration::from_millis(25),
        };
        let resources = Arc::new(SessionResources::new(permit, session));
        let (ack_tx, ack_rx) = oneshot::channel();
        let writer = tokio::spawn({
            let queue = queue.clone();
            let recorded = recorded.clone();
            async move {
                writer_loop(
                    RecordingSink(recorded),
                    queue.clone(),
                    queue.notify.clone(),
                    Duration::from_secs(60),
                    lifecycle,
                    ack_rx,
                    resources,
                )
                .await;
            }
        });
        timeout(Duration::from_secs(1), writer)
            .await
            .expect("writer should finish at the shared close deadline")
            .expect("writer should not panic");
        drop(ack_tx);

        assert!(shutdown.is_cancelled(), "queued Close should start session shutdown");
        assert_eq!(queue.try_send(Message::text("late")), Err(WsSendError::Closed));
        assert!(queue.is_empty(), "queued and in-flight capacity should be returned");
        let sent = recorded.lock().unwrap();
        assert_eq!(sent.len(), 1, "writer should send only the queued Close frame");
        let Message::Close(Some(frame)) = &sent[0] else {
            panic!("writer should preserve the queued Close frame");
        };
        assert_eq!(frame.code, 4001);
        assert_eq!(frame.reason, "custom reason");
        drop(sent);
        assert_eq!(context.active_sessions(), 0, "session registration should be released");
        assert_eq!(permits.available_permits(), 1, "policy permit should be released");
        drop(server);
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
                writer_loop(
                    sink,
                    queue.clone(),
                    notify,
                    Duration::from_secs(60),
                    lifecycle,
                    ack_rx,
                    resources,
                )
                .await;
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
    async fn test_writer_failure_terminates_active_session() {
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
                    Duration::from_secs(60),
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
    async fn test_writer_peer_close_ack_while_idle_flush_failure_reports_once() {
        assert_peer_close_flush_failure(PeerCloseFlushBranch::AckWhileIdle).await;
    }

    #[tokio::test]
    async fn test_writer_peer_close_notify_flush_failure_reports_once() {
        assert_peer_close_flush_failure(PeerCloseFlushBranch::Notify).await;
    }

    #[tokio::test]
    async fn test_writer_peer_close_ack_during_send_flush_failure_reports_once() {
        assert_peer_close_flush_failure(PeerCloseFlushBranch::AckWhileSending).await;
    }

    #[test]
    fn test_peer_close_failing_sink_accepts_frames_and_closes() {
        let mut sink = PeerCloseFailingSink::Ready;
        let waker = std::task::Waker::noop();
        let mut context = Context::from_waker(waker);

        assert!(matches!(
            Sink::poll_ready(Pin::new(&mut sink), &mut context),
            Poll::Ready(Ok(()))
        ));
        assert!(Sink::start_send(Pin::new(&mut sink), Message::Text("test".into())).is_ok());
        assert!(matches!(
            Sink::poll_close(Pin::new(&mut sink), &mut context),
            Poll::Ready(Ok(()))
        ));
    }

    #[derive(Clone, Copy)]
    enum PeerCloseFlushBranch {
        AckWhileIdle,
        Notify,
        AckWhileSending,
    }

    async fn assert_peer_close_flush_failure(branch: PeerCloseFlushBranch) {
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
        let queue = WsSendQueue::new(1, 128);
        if matches!(branch, PeerCloseFlushBranch::AckWhileSending) {
            queue
                .try_send(Message::text("queued"))
                .expect("message should be queued");
        }
        let notify = queue.notify.clone();
        let writer_notify = notify.clone();
        let shutdown = CancellationToken::new();
        let lifecycle = SessionLifecycle {
            shutdown: shutdown.clone(),
            close_code: Arc::new(std::sync::atomic::AtomicU16::new(1000)),
            close_deadline: Arc::new(std::sync::Mutex::new(None)),
            server_deadline: None,
            shutdown_timeout: Duration::from_secs(1),
        };
        let resources = Arc::new(SessionResources::new(permit, session));
        let (ack_tx, ack_rx) = oneshot::channel();
        let reached_send = Arc::new(Notify::new());
        let control_flush_started = Arc::new(Notify::new());
        let sink = if matches!(branch, PeerCloseFlushBranch::AckWhileSending) {
            PeerCloseFailingSink::Sending {
                reached: reached_send.clone(),
            }
        } else if matches!(branch, PeerCloseFlushBranch::Notify) {
            PeerCloseFailingSink::NotifyThenFail {
                started: control_flush_started.clone(),
                polls: AtomicUsize::new(0),
            }
        } else {
            PeerCloseFailingSink::Ready
        };
        let writer = tokio::spawn(
            async move {
                writer_loop(
                    sink,
                    queue.clone(),
                    writer_notify,
                    Duration::from_secs(60),
                    lifecycle,
                    ack_rx,
                    resources,
                )
                .await;
                queue
            }
            .with_subscriber(dispatch),
        );
        match branch {
            PeerCloseFlushBranch::AckWhileIdle => {
                ack_tx.send(()).expect("ack receiver should be live");
            }
            PeerCloseFlushBranch::Notify => {
                notify.notify_one();
                timeout(Duration::from_secs(1), control_flush_started.notified())
                    .await
                    .expect("writer should enter control flush");
                ack_tx.send(()).expect("ack receiver should be live");
            }
            PeerCloseFlushBranch::AckWhileSending => {
                timeout(Duration::from_secs(1), reached_send.notified())
                    .await
                    .expect("writer should enter send");
                ack_tx.send(()).expect("ack receiver should be live");
            }
        }
        let queue = timeout(Duration::from_secs(1), writer)
            .await
            .expect("writer should exit after flush error")
            .expect("writer should not panic");
        let output = log.contents();
        let failures: Vec<_> = output
            .lines()
            .filter(|line| line.contains("websocket_io_failure"))
            .collect();
        assert_eq!(failures.len(), 1, "expected one flush warning: {output}");
        assert!(
            failures[0].contains("direction=\"write\"") && failures[0].contains("operation=\"flush\""),
            "unexpected warning: {output}"
        );
        assert!(
            !output.contains("secret-sentinel"),
            "sink error must be redacted: {output}"
        );
        assert_eq!(
            queue.state.lock().unwrap().in_flight_messages,
            0,
            "in-flight budget should be released"
        );
        assert_eq!(context.active_sessions(), 0, "session registration should be released");
        assert_eq!(permits.available_permits(), 1, "connection permit should be released");
        drop(server);
    }

    enum PeerCloseFailingSink {
        Ready,
        Sending { reached: Arc<Notify> },
        NotifyThenFail { started: Arc<Notify>, polls: AtomicUsize },
    }

    impl Sink<Message> for PeerCloseFailingSink {
        type Error = std::io::Error;
        fn poll_ready(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            if let Self::Sending { reached } = &*self {
                reached.notify_one();
                return Poll::Pending;
            }
            Poll::Ready(Ok(()))
        }
        fn start_send(self: Pin<&mut Self>, _item: Message) -> Result<(), Self::Error> {
            Ok(())
        }
        fn poll_flush(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            if let Self::NotifyThenFail { started, polls } = &mut *self
                && polls.fetch_add(1, Ordering::SeqCst) == 0
            {
                started.notify_one();
                return Poll::Pending;
            }
            Poll::Ready(Err(std::io::Error::other("secret-sentinel peer-close flush failure")))
        }
        fn poll_close(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }
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
                writer_loop(
                    SuccessfulSink,
                    queue,
                    notify,
                    Duration::from_secs(60),
                    lifecycle,
                    ack_rx,
                    resources,
                )
                .await;
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
                    Duration::from_secs(60),
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
                writer_loop(
                    SuccessfulSink,
                    queue,
                    notify,
                    Duration::from_secs(60),
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

    struct PendingReady {
        pending_ready: Arc<Notify>,
        ready_polls: Arc<AtomicUsize>,
    }

    impl Sink<Message> for PendingReady {
        type Error = std::io::Error;

        fn poll_ready(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            if self.ready_polls.fetch_add(1, Ordering::SeqCst) == 0 {
                self.pending_ready.notify_one();
            }
            Poll::Pending
        }

        fn start_send(self: Pin<&mut Self>, _item: Message) -> Result<(), Self::Error> {
            unreachable!("start_send must not run while readiness is pending");
        }

        fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }

        fn poll_close(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }
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

    async fn assert_pending_send_times_out_after_one_idle_window<S>(
        sink: S,
        pending: Arc<Notify>,
        poll_count: Arc<AtomicUsize>,
    ) where
        S: Sink<Message> + Unpin + Send + 'static,
    {
        const IDLE_WINDOW: Duration = Duration::from_millis(500);

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
        let shutdown = CancellationToken::new();
        let lifecycle = SessionLifecycle {
            shutdown: shutdown.clone(),
            close_code: Arc::new(std::sync::atomic::AtomicU16::new(1001)),
            close_deadline: Arc::new(std::sync::Mutex::new(None)),
            server_deadline: None,
            shutdown_timeout: Duration::from_millis(100),
        };
        let resources = Arc::new(SessionResources::new(permit, session));
        let (ack_tx, ack_rx) = oneshot::channel();
        let writer = tokio::spawn({
            let queue = queue.clone();
            async move {
                writer_loop(
                    sink,
                    queue.clone(),
                    queue.notify.clone(),
                    IDLE_WINDOW,
                    lifecycle,
                    ack_rx,
                    resources,
                )
                .await;
            }
        });

        pending.notified().await;
        assert_eq!(
            poll_count.load(Ordering::SeqCst),
            1,
            "sink reached its controlled Pending point"
        );
        assert_eq!(queue.len(), 1, "the taken message remains in flight");
        assert!(!shutdown.is_cancelled(), "the test did not initiate shutdown");
        assert_eq!(
            context.active_sessions(),
            1,
            "the pending writer retains session registration"
        );
        assert_eq!(
            permits.available_permits(),
            0,
            "the pending writer retains connection capacity"
        );

        for _ in 0..4 {
            tokio::time::advance(IDLE_WINDOW / 5).await;
            queue.notify.notify_one();
            tokio::task::yield_now().await;
            assert!(
                !writer.is_finished(),
                "a pending write remains alive before its absolute deadline"
            );
            assert_eq!(queue.len(), 1, "the write has made no completion progress");
            assert_eq!(
                poll_count.load(Ordering::SeqCst),
                1,
                "no new sink poll or completion occurred"
            );
            assert!(
                !shutdown.is_cancelled(),
                "pending write did not cancel before its deadline"
            );
        }
        tokio::time::advance(IDLE_WINDOW / 5).await;
        tokio::task::yield_now().await;
        drop(ack_tx);
        timeout(Duration::from_secs(1), writer)
            .await
            .expect("writer should time out without external closure")
            .expect("writer should not panic");
        assert!(shutdown.is_cancelled(), "write timeout cancels the session token");
        assert!(queue.is_empty(), "write timeout releases the in-flight queue charge");
        assert_eq!(
            context.active_sessions(),
            0,
            "write timeout releases session registration"
        );
        assert_eq!(
            permits.available_permits(),
            1,
            "write timeout releases connection capacity"
        );
        drop(server);
    }

    #[tokio::test(start_paused = true)]
    async fn test_pending_ready_times_out_after_one_idle_window_without_external_close() {
        let pending = Arc::new(Notify::new());
        let polls = Arc::new(AtomicUsize::new(0));
        assert_pending_send_times_out_after_one_idle_window(
            PendingReady {
                pending_ready: pending.clone(),
                ready_polls: polls.clone(),
            },
            pending,
            polls,
        )
        .await;
    }

    #[tokio::test(start_paused = true)]
    async fn test_pending_flush_times_out_after_one_idle_window_without_external_close() {
        let pending = Arc::new(Notify::new());
        let polls = Arc::new(AtomicUsize::new(0));
        assert_pending_send_times_out_after_one_idle_window(
            PendingSend {
                pending_flush: pending.clone(),
                flush_polls: polls.clone(),
            },
            pending,
            polls,
        )
        .await;
    }

    #[tokio::test(start_paused = true)]
    async fn test_control_flush_times_out_after_one_idle_window_despite_new_notifications() {
        const IDLE_WINDOW: Duration = Duration::from_millis(500);

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
        let queue = WsSendQueue::new(1, 128);
        let notify = queue.notify.clone();
        let pending_flush = Arc::new(Notify::new());
        let shutdown = CancellationToken::new();
        let lifecycle = SessionLifecycle {
            shutdown: shutdown.clone(),
            close_code: Arc::new(std::sync::atomic::AtomicU16::new(1001)),
            close_deadline: Arc::new(std::sync::Mutex::new(None)),
            server_deadline: None,
            shutdown_timeout: Duration::from_millis(100),
        };
        let resources = Arc::new(SessionResources::new(permit, session));
        let (ack_tx, ack_rx) = oneshot::channel();
        let writer = tokio::spawn({
            let queue = queue.clone();
            let sink = PendingFlush {
                polled: pending_flush.clone(),
            };
            async move {
                writer_loop(sink, queue.clone(), notify, IDLE_WINDOW, lifecycle, ack_rx, resources).await;
            }
        });
        queue.notify.notify_one();
        pending_flush.notified().await;
        assert!(!writer.is_finished(), "control flush should begin pending");

        for _ in 0..4 {
            tokio::time::advance(IDLE_WINDOW / 5).await;
            queue.notify.notify_one();
            tokio::task::yield_now().await;
            assert!(
                !writer.is_finished(),
                "unrelated notifications do not shorten the flush deadline"
            );
            assert!(
                !shutdown.is_cancelled(),
                "control flush remains active before its deadline"
            );
        }
        tokio::time::advance(IDLE_WINDOW / 5).await;
        tokio::task::yield_now().await;
        drop(ack_tx);
        timeout(Duration::from_secs(1), writer)
            .await
            .expect("control flush should time out without external closure")
            .expect("writer should not panic");

        assert!(shutdown.is_cancelled(), "flush timeout cancels the session token");
        assert!(queue.is_empty(), "flush timeout closes the outbound queue");
        assert_eq!(
            context.active_sessions(),
            0,
            "flush timeout releases session registration"
        );
        assert_eq!(
            permits.available_permits(),
            1,
            "flush timeout releases connection capacity"
        );
        drop(server);
    }

    #[tokio::test]
    async fn test_real_network_write_stall_times_out_and_releases_capacity() {
        const IDLE_TIMEOUT: Duration = Duration::from_millis(500);
        const MESSAGE_BYTES: usize = 32 * 1024 * 1024;
        const STALL_MARKER: &[u8] = b"rs-web-write-stall-20261107";

        type HandlerState = (
            WsUpgradePolicy,
            crate::server::ServerContext,
            Arc<Notify>,
            Arc<std::sync::atomic::AtomicUsize>,
            Arc<std::sync::atomic::AtomicUsize>,
        );

        async fn upgrade(
            State((policy, context, admitted, inbound, connection_count)): State<HandlerState>,
            headers: HeaderMap,
            ws: WebSocketUpgrade,
        ) -> Response {
            policy.on_upgrade(ws, &headers, context, move |mut session| async move {
                if connection_count.fetch_add(1, Ordering::SeqCst) == 0 {
                    let mut payload = vec![0x5a; MESSAGE_BYTES];
                    payload[..STALL_MARKER.len()].copy_from_slice(STALL_MARKER);
                    session
                        .try_send(Message::Binary(payload.into()))
                        .expect("large server message should enter the bounded queue");
                    admitted.notify_one();
                } else {
                    session
                        .try_send(Message::text("second-session"))
                        .expect("second session message should enter the bounded queue");
                }
                while let Some(Ok(message)) = session.recv().await {
                    if matches!(message, Message::Text(_)) {
                        inbound.fetch_add(1, Ordering::SeqCst);
                    }
                }
            })
        }

        let (event_tx, mut event_rx) = tokio::sync::mpsc::unbounded_channel();
        let observer = TestPendingObserver {
            marker: Arc::from(STALL_MARKER),
            events: event_tx,
        };
        let observer_slot = TEST_PENDING_OBSERVER.get_or_init(|| Mutex::new(None));
        *observer_slot.lock().unwrap_or_else(std::sync::PoisonError::into_inner) = Some(observer);
        let _observer_guard = TestPendingObserverGuard(observer_slot);

        let policy = WsUpgradePolicy::new()
            .max_connections(1)
            .max_frame_bytes(MESSAGE_BYTES)
            .max_message_bytes(MESSAGE_BYTES)
            .queue_limits(1, MESSAGE_BYTES)
            .idle_timeout(IDLE_TIMEOUT);
        let server = WebServer::bind_http(
            ServerOptions::new("127.0.0.1:0".parse().unwrap()).with_shutdown_timeout(Duration::from_millis(200)),
        )
        .await
        .expect("test WebServer should bind");
        let address = server.local_addr();
        let context = server.context();
        let shutdown = CancellationToken::new();
        let admitted = Arc::new(Notify::new());
        let inbound = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let connection_count = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let app = Router::new().route(
            "/ws",
            get(upgrade).with_state((
                policy.clone(),
                context.clone(),
                admitted.clone(),
                inbound.clone(),
                connection_count,
            )),
        );
        let serve_shutdown = shutdown.clone();
        let serving = tokio::spawn(async move { server.serve(app, serve_shutdown.cancelled_owned()).await });

        let (client, status) = test_handshake(address).await;
        assert_eq!(status, StatusCode::SWITCHING_PROTOCOLS);
        admitted.notified().await;
        let (client_read, client_write) = client.into_split();
        let inbound_writer = tokio::spawn(async move {
            let mut client_write = client_write;
            let mut interval = tokio::time::interval(Duration::from_millis(50));
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                interval.tick().await;
                test_send_masked_text(&mut client_write, b"inbound-progress").await;
            }
        });

        let started = tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                match event_rx.recv().await {
                    Some(TestWriterEvent::SendStarted(at)) => break at,
                    Some(_) => {}
                    None => panic!("writer observer channel closed before target send"),
                }
            }
        })
        .await
        .expect("writer should start the marked network send");
        let first_pending = tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                match event_rx.recv().await {
                    Some(TestWriterEvent::FlushPending(at)) => break at,
                    Some(TestWriterEvent::FlushReady(_)) => {
                        panic!("large marked send completed before the stall window")
                    }
                    Some(TestWriterEvent::SendStarted(_)) => {}
                    None => panic!("writer observer channel closed before Pending"),
                }
            }
        })
        .await
        .expect("actual WebSocket sink should reach poll_flush Pending");
        assert!(first_pending >= started);

        let mut inbound_timeline = Vec::new();
        let previous_inbound = inbound.load(Ordering::SeqCst);
        tokio::time::timeout(Duration::from_secs(15), async {
            let deadline = started + IDLE_TIMEOUT;
            let midpoint = first_pending + (deadline - first_pending) / 2;
            let mut sampled_midpoint = false;
            loop {
                tokio::select! {
                    _ = tokio::time::sleep_until(deadline) => break,
                    _ = tokio::time::sleep_until(midpoint), if !sampled_midpoint => {
                        let count = inbound.load(Ordering::SeqCst);
                        assert!(count > previous_inbound, "application inbound count must advance while the write is pending");
                        assert_eq!(policy.active_connections(), 1, "pending writer retains its policy permit before timeout");
                        assert_eq!(context.active_sessions(), 1, "session remains registered before timeout");
                        sampled_midpoint = true;
                    }
                    event = event_rx.recv() => match event {
                        Some(TestWriterEvent::FlushPending(_)) => {},
                        Some(TestWriterEvent::FlushReady(at)) => {
                            panic!("large send completed before its write deadline at {at:?}")
                        }
                        Some(TestWriterEvent::SendStarted(_)) => {},
                        None => panic!("writer observer channel closed during observation"),
                    }
                }
            }
            let count = inbound.load(Ordering::SeqCst);
            assert!(count > previous_inbound, "application inbound count must advance during the write deadline");
            inbound_timeline.push((1, tokio::time::Instant::now(), count));
            assert!(
                !shutdown.is_cancelled(),
                "the server was not shut down during observation"
            );
        })
        .await
        .expect("write timeout observation must remain within the 15 second bound");

        tokio::time::timeout(Duration::from_secs(3), async {
            while policy.active_connections() != 0 || context.active_sessions() != 0 {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("write timeout should release both I/O tasks and session capacity");

        let (second_client, second_status) = test_handshake(address).await;
        assert_eq!(second_status, StatusCode::SWITCHING_PROTOCOLS);
        drop(second_client);

        inbound_writer.abort();
        let _ = inbound_writer.await;
        drop(client_read);
        shutdown.cancel();
        let report = tokio::time::timeout(Duration::from_secs(3), serving)
            .await
            .expect("server cleanup should be bounded")
            .expect("server task should not panic")
            .expect("server should report successful shutdown");
        assert!(report.graceful, "server should finish cleanup within shutdown_timeout");
        assert_eq!(
            policy.active_connections(),
            0,
            "shutdown should release policy capacity"
        );
        assert_eq!(
            context.active_sessions(),
            0,
            "shutdown should release session registration"
        );
        println!("writer_send_started={started:?}");
        println!("writer_flush_pending={first_pending:?}");
        println!("inbound_windows={inbound_timeline:?}");
        println!("second_upgrade_status={second_status}");
        println!("cleanup_active_connections={}", policy.active_connections());
        println!("cleanup_active_sessions={}", context.active_sessions());
    }

    struct TestPendingObserverGuard(&'static Mutex<Option<TestPendingObserver>>);

    impl Drop for TestPendingObserverGuard {
        fn drop(&mut self) {
            *self.0.lock().unwrap_or_else(std::sync::PoisonError::into_inner) = None;
        }
    }

    async fn test_handshake(address: SocketAddr) -> (TcpStream, StatusCode) {
        let mut stream = TcpStream::connect(address).await.expect("test client should connect");
        let request = format!(
            "GET /ws HTTP/1.1\r\nHost: {address}\r\nConnection: Upgrade\r\nUpgrade: websocket\r\nSec-WebSocket-Version: 13\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n\r\n"
        );
        stream
            .write_all(request.as_bytes())
            .await
            .expect("handshake request should write");
        let mut response = Vec::new();
        let mut byte = [0; 1];
        while !response.ends_with(b"\r\n\r\n") {
            tokio::time::timeout(Duration::from_secs(2), stream.read_exact(&mut byte))
                .await
                .expect("handshake response should arrive")
                .expect("handshake response should be readable");
            response.push(byte[0]);
        }
        let status = String::from_utf8_lossy(&response)
            .split_whitespace()
            .nth(1)
            .expect("response status should exist")
            .parse::<u16>()
            .expect("response status should be numeric");
        (
            stream,
            StatusCode::from_u16(status).expect("response status should be valid"),
        )
    }

    async fn test_send_masked_text(stream: &mut tokio::net::tcp::OwnedWriteHalf, text: &[u8]) {
        assert!(text.len() < 126);
        let mask = [7, 9, 3, 1];
        let mut frame = vec![0x81, 0x80 | text.len() as u8];
        frame.extend_from_slice(&mask);
        frame.extend(text.iter().enumerate().map(|(i, byte)| byte ^ mask[i % 4]));
        stream
            .write_all(&frame)
            .await
            .expect("masked client frame should write");
    }

    #[test]
    fn test_operation_deadline_is_immediate_when_duration_cannot_be_added() {
        let deadline = operation_deadline(Duration::MAX);

        assert!(deadline <= tokio::time::Instant::now());
    }

    #[tokio::test]
    async fn test_writer_close_send_obeys_shared_deadline_when_sink_blocks() {
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
        let queue = WsSendQueue::new(1, 128);
        queue.try_send(Message::Close(None)).expect("Close should be queued");
        let pending_flush = Arc::new(Notify::new());
        let flush_polls = Arc::new(AtomicUsize::new(0));
        let lifecycle = SessionLifecycle {
            shutdown: CancellationToken::new(),
            close_code: Arc::new(std::sync::atomic::AtomicU16::new(1001)),
            close_deadline: Arc::new(std::sync::Mutex::new(None)),
            server_deadline: None,
            shutdown_timeout: Duration::from_millis(25),
        };
        let resources = Arc::new(SessionResources::new(permit, session));
        let (ack_tx, ack_rx) = oneshot::channel();
        let writer = tokio::spawn({
            let queue = queue.clone();
            let sink = PendingSend {
                pending_flush: pending_flush.clone(),
                flush_polls,
            };
            async move {
                writer_loop(
                    sink,
                    queue.clone(),
                    queue.notify.clone(),
                    Duration::from_secs(60),
                    lifecycle,
                    ack_rx,
                    resources,
                )
                .await;
            }
        });

        timeout(Duration::from_secs(1), pending_flush.notified())
            .await
            .expect("Close send should reach the intentionally pending flush");
        assert!(
            !writer.is_finished(),
            "writer must remain bounded by its close deadline"
        );
        timeout(Duration::from_secs(1), writer)
            .await
            .expect("blocked Close send should stop at the shared deadline")
            .expect("writer should not panic");
        drop(ack_tx);

        assert!(
            queue.is_empty(),
            "in-flight Close budget should be released exactly once"
        );
        assert_eq!(queue.try_send(Message::text("late")), Err(WsSendError::Closed));
        assert_eq!(context.active_sessions(), 0);
        assert_eq!(permits.available_permits(), 1);
        drop(server);
    }

    #[tokio::test]
    async fn test_pending_flush_is_cancelled() {
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
    async fn test_pending_flush_obeys_deadline() {
        let mut sink = PendingFlush {
            polled: std::sync::Arc::new(Notify::new()),
        };
        let token = CancellationToken::new();
        let result = flush_until(&mut sink, &token, Some(tokio::time::Instant::now())).await;
        assert_eq!(result, FlushOutcome::TimedOut);
    }

    #[tokio::test]
    async fn test_ready_flush_succeeds() {
        let mut sink = ReadyFlush(Some(Ok(())));
        let token = CancellationToken::new();
        let result = flush_until(&mut sink, &token, None).await;
        assert_eq!(result, FlushOutcome::Flushed);
    }

    #[tokio::test]
    async fn test_failed_flush_is_reported() {
        let mut sink = ReadyFlush(Some(Err(std::io::Error::other("injected"))));
        let token = CancellationToken::new();
        let result = flush_until(&mut sink, &token, None).await;
        assert_eq!(result, FlushOutcome::Failed);
    }

    #[tokio::test]
    async fn test_writer_pending_send_shutdown_releases_resources() {
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
            writer_loop(
                sink,
                queue.clone(),
                notify,
                Duration::from_secs(60),
                lifecycle,
                ack_rx,
                resources,
            )
            .await;
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
    async fn test_writer_pending_send_peer_close_ack_does_not_cancel_session() {
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
            writer_loop(
                sink,
                queue.clone(),
                notify,
                Duration::from_secs(60),
                lifecycle,
                ack_rx,
                resources,
            )
            .await;
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
    async fn test_writer_notify_flush_close_ack_sender_drop_releases_resources() {
        assert_notify_flush_peer_close(None, true, false).await;
    }

    #[tokio::test]
    async fn test_writer_notify_flush_close_ack_uses_expired_server_deadline() {
        assert_notify_flush_peer_close(Some(tokio::time::Instant::now()), false, false).await;
    }

    #[tokio::test]
    async fn test_writer_notify_flush_close_ack_and_cancellation_simultaneously_ready() {
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
            writer_loop(
                sink,
                queue.clone(),
                notify,
                Duration::from_secs(60),
                lifecycle,
                ack_rx,
                resources,
            )
            .await;
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
