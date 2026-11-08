// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::io::Write;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::OnceLock;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::task::Context;
use std::task::Poll;
use std::time::Duration;

use axum::extract::ws::Message;
use axum::http::StatusCode;
use futures_util::Sink;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use tokio::sync::Notify;
use tokio::sync::Semaphore;
use tokio::sync::mpsc::UnboundedSender;
use tokio::sync::oneshot;
use tokio::time::Instant;
use tokio::time::timeout;
use tokio_util::sync::CancellationToken;
use tracing::Level;
use tracing::instrument::WithSubscriber;
use tracing_subscriber::fmt::MakeWriter;

use super::super::writer_loop;
use crate::ServerOptions;
use crate::WebServer;
use crate::ws::WsSendQueue;
use crate::ws::WsTermination;
use crate::ws::internal::SessionLifecycle;
use crate::ws::internal::SessionResources;
use crate::ws::ws_send_error::WsSendError;

#[cfg(test)]
#[derive(Clone)]
pub(in crate::ws::internal::writer) struct TestPendingObserver {
    pub(super) marker: Arc<[u8]>,
    pub(super) events: UnboundedSender<TestWriterEvent>,
}

#[cfg(test)]
#[derive(Clone, Copy, Debug)]
pub(super) enum TestWriterEvent {
    SendStarted(Instant),
    FlushPending(Instant),
    FlushReady(Instant),
}

#[cfg(test)]
pub(in crate::ws::internal::writer) static TEST_PENDING_OBSERVER: OnceLock<Mutex<Option<TestPendingObserver>>> =
    OnceLock::new();

#[cfg(test)]
pub(in crate::ws::internal::writer) fn current_test_pending_observer() -> Option<TestPendingObserver> {
    TEST_PENDING_OBSERVER
        .get_or_init(|| Mutex::new(None))
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .clone()
}

#[cfg(test)]
pub(in crate::ws::internal::writer) struct TestObservedSink<S> {
    inner: S,
    observer: Option<TestPendingObserver>,
    target_send: bool,
}

#[cfg(test)]
impl<S> TestObservedSink<S> {
    pub(in crate::ws::internal::writer) fn new(inner: S, observer: Option<TestPendingObserver>) -> Self {
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

#[derive(Clone, Default)]
pub(super) struct SharedLog(pub(super) Arc<std::sync::Mutex<Vec<u8>>>);

pub(super) struct LogWriter(pub(super) Arc<std::sync::Mutex<Vec<u8>>>);

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
    pub(super) fn contents(&self) -> String {
        String::from_utf8_lossy(&self.0.lock().unwrap()).into_owned()
    }
}

#[derive(Clone, Copy)]
pub(super) enum FailurePoint {
    Ready,
    Send,
    Flush,
}

pub(super) struct FailingSink {
    pub(super) point: FailurePoint,
    pub(super) failed: bool,
    pub(super) notified: Arc<Notify>,
}

pub(super) struct RecordingSink(pub(super) Arc<std::sync::Mutex<Vec<Message>>>);

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

pub(super) async fn assert_writer_failure(point: FailurePoint) {
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
    let resources = Arc::new(SessionResources::new(permit, session));
    let termination = resources.termination_state();
    let lifecycle = SessionLifecycle {
        shutdown: shutdown.clone(),
        server_shutdown: CancellationToken::new(),
        termination: termination.clone(),
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
    assert_eq!(termination.snapshot(), Some(WsTermination::WriteError));
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

#[derive(Clone, Copy)]
pub(super) enum PeerCloseFlushBranch {
    AckWhileIdle,
    Notify,
    AckWhileSending,
}

pub(super) async fn assert_peer_close_flush_failure(branch: PeerCloseFlushBranch) {
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
    let resources = Arc::new(SessionResources::new(permit, session));
    let termination = resources.termination_state();
    let lifecycle = SessionLifecycle {
        shutdown: shutdown.clone(),
        server_shutdown: CancellationToken::new(),
        termination: termination.clone(),
        close_code: Arc::new(std::sync::atomic::AtomicU16::new(1000)),
        close_deadline: Arc::new(std::sync::Mutex::new(None)),
        server_deadline: None,
        shutdown_timeout: Duration::from_secs(1),
    };
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

pub(super) enum PeerCloseFailingSink {
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

pub(super) struct CloseFailureSink {
    pub(super) fail_send: bool,
    pub(super) fail_post_ack_flush: bool,
    pub(super) flush_count: usize,
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

pub(super) async fn assert_close_failure_reports(operation: &str, fail_send: bool, fail_post_ack_flush: bool) {
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
        server_shutdown: shutdown.clone(),
        termination: Arc::new(crate::ws::internal::TerminationState::default()),
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

pub(super) struct PendingFlush {
    pub(super) polled: std::sync::Arc<Notify>,
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

pub(super) struct ReadyFlush(pub(super) Option<Result<(), std::io::Error>>);

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

pub(super) struct SuccessfulSink;

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

pub(super) struct PendingSend {
    pub(super) pending_flush: Arc<Notify>,
    pub(super) flush_polls: Arc<AtomicUsize>,
}

pub(super) struct PendingReady {
    pub(super) pending_ready: Arc<Notify>,
    pub(super) ready_polls: Arc<AtomicUsize>,
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

pub(super) async fn assert_pending_send_times_out_after_one_idle_window<S>(
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
    let resources = Arc::new(SessionResources::new(permit, session));
    let termination = resources.termination_state();
    let lifecycle = SessionLifecycle {
        shutdown: shutdown.clone(),
        server_shutdown: CancellationToken::new(),
        termination: termination.clone(),
        close_code: Arc::new(std::sync::atomic::AtomicU16::new(1001)),
        close_deadline: Arc::new(std::sync::Mutex::new(None)),
        server_deadline: None,
        shutdown_timeout: Duration::from_millis(100),
    };
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
    assert_eq!(termination.snapshot(), Some(WsTermination::WriteTimeout));
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

pub(super) struct TestPendingObserverGuard(pub(super) &'static Mutex<Option<TestPendingObserver>>);

impl Drop for TestPendingObserverGuard {
    fn drop(&mut self) {
        *self.0.lock().unwrap_or_else(std::sync::PoisonError::into_inner) = None;
    }
}

pub(super) async fn test_handshake(address: SocketAddr) -> (TcpStream, StatusCode) {
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

pub(super) async fn test_send_masked_text(stream: &mut tokio::net::tcp::OwnedWriteHalf, text: &[u8]) {
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

pub(super) async fn assert_notify_flush_peer_close(
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
        server_shutdown: shutdown.clone(),
        termination: Arc::new(crate::ws::internal::TerminationState::default()),
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
