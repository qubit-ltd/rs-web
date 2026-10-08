// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Bounded WebSocket upgrades and per-session outbound backpressure.
//!
//! Applications retain responsibility for authentication and their message
//! protocol. Use [`WsUpgradePolicy::on_upgrade`] after authentication to apply
//! origin, connection, frame, and message limits. Selecting Axum's native
//! `WebSocketUpgrade::on_upgrade` directly bypasses this module's queue and
//! lifecycle policy.

use std::collections::VecDeque;
use std::future::Future;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicU16;
use std::sync::atomic::Ordering;
use std::time::Duration;

use axum::extract::ws::Message;
use axum::extract::ws::WebSocket;
use axum::extract::ws::WebSocketUpgrade;
use axum::http::HeaderMap;
use axum::http::StatusCode;
use axum::http::header::ORIGIN;
use axum::response::IntoResponse;
use axum::response::Response;
use futures_util::SinkExt;
use futures_util::StreamExt;
use tokio::sync::Notify;
use tokio::sync::OwnedSemaphorePermit;
use tokio::sync::Semaphore;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::server::ServerContext;

const DEFAULT_CONNECTIONS: usize = 128;
const DEFAULT_FRAME_BYTES: usize = 64 * 1024;
const DEFAULT_MESSAGE_BYTES: usize = 1024 * 1024;
const DEFAULT_QUEUE_MESSAGES: usize = 64;
const DEFAULT_QUEUE_BYTES: usize = 1024 * 1024;
const DEFAULT_IDLE_TIMEOUT: Duration = Duration::from_secs(60);
const DEFAULT_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Clone, Debug)]
struct PolicyInner {
    connections: Arc<Semaphore>,
    max_connections: usize,
    max_frame_bytes: usize,
    max_message_bytes: usize,
    queue_messages: usize,
    queue_bytes: usize,
    idle_timeout: Duration,
    shutdown_timeout: Duration,
    allowed_origins: Option<Vec<String>>,
    invalid_limits: bool,
}

/// Limits and lifecycle policy for WebSocket sessions.
#[derive(Clone, Debug)]
pub struct WsUpgradePolicy {
    inner: Arc<PolicyInner>,
}

impl Default for WsUpgradePolicy {
    fn default() -> Self {
        Self::new()
    }
}

impl WsUpgradePolicy {
    /// Creates a policy with finite defaults: 128 sessions, 64 KiB frames,
    /// 1 MiB messages, a 64-message/1 MiB queue, and a 60-second idle timeout.
    pub fn new() -> Self {
        Self {
            inner: Arc::new(PolicyInner {
                connections: Arc::new(Semaphore::new(DEFAULT_CONNECTIONS)),
                max_connections: DEFAULT_CONNECTIONS,
                max_frame_bytes: DEFAULT_FRAME_BYTES,
                max_message_bytes: DEFAULT_MESSAGE_BYTES,
                queue_messages: DEFAULT_QUEUE_MESSAGES,
                queue_bytes: DEFAULT_QUEUE_BYTES,
                idle_timeout: DEFAULT_IDLE_TIMEOUT,
                shutdown_timeout: DEFAULT_SHUTDOWN_TIMEOUT,
                allowed_origins: None,
                invalid_limits: false,
            }),
        }
    }

    fn map_inner(mut self, f: impl FnOnce(&mut PolicyInner)) -> Self {
        f(Arc::make_mut(&mut self.inner));
        self
    }

    /// Sets the maximum number of simultaneous upgraded connections.
    pub fn max_connections(self, limit: usize) -> Self {
        self.map_inner(|inner| {
            let valid = (1..=Semaphore::MAX_PERMITS).contains(&limit);
            let capacity = if valid { limit } else { 0 };
            inner.invalid_limits |= !valid;
            inner.connections = Arc::new(Semaphore::new(capacity));
            inner.max_connections = capacity;
        })
    }

    /// Sets the accepted browser origins. `None` means no Origin is allowed.
    /// Requests without an Origin still proceed to the application's auth.
    pub fn allowed_origins<I, S>(self, origins: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.map_inner(|inner| inner.allowed_origins = Some(origins.into_iter().map(Into::into).collect()))
    }

    /// Sets the independent maximum WebSocket frame size.
    pub fn max_frame_bytes(self, limit: usize) -> Self {
        self.map_inner(|inner| {
            inner.invalid_limits |= limit == 0;
            inner.max_frame_bytes = limit;
        })
    }

    /// Sets the reassembled WebSocket message size limit.
    pub fn max_message_bytes(self, limit: usize) -> Self {
        self.map_inner(|inner| {
            inner.invalid_limits |= limit == 0;
            inner.max_message_bytes = limit;
        })
    }

    /// Sets the outbound queue's simultaneous item and byte limits.
    pub fn queue_limits(self, messages: usize, bytes: usize) -> Self {
        self.map_inner(|inner| {
            inner.invalid_limits |= messages == 0 || bytes == 0;
            inner.queue_messages = messages;
            inner.queue_bytes = bytes;
        })
    }

    /// Sets the maximum interval without an inbound message before closing.
    pub fn idle_timeout(self, timeout: Duration) -> Self {
        self.map_inner(|inner| {
            inner.invalid_limits |= timeout.is_zero();
            inner.idle_timeout = timeout;
        })
    }

    /// Sets how long a session waits for the peer's close acknowledgement.
    ///
    /// The server has its own shutdown deadline. Set this no greater than the
    /// server deadline; `on_upgrade` receives its cancellation token but not
    /// the server's deadline value.
    pub fn shutdown_timeout(self, timeout: Duration) -> Self {
        self.map_inner(|inner| {
            inner.invalid_limits |= timeout.is_zero();
            inner.shutdown_timeout = timeout;
        })
    }

    /// Checks that all configured bounds are positive.
    pub fn validate(&self) -> Result<(), WsPolicyError> {
        if self.inner.invalid_limits {
            Err(WsPolicyError::ZeroLimit)
        } else {
            Ok(())
        }
    }

    /// Returns the number of active upgraded connections.
    pub fn active_connections(&self) -> usize {
        self.inner.max_connections - self.inner.connections.available_permits()
    }

    /// Creates a separately usable send queue with this policy's limits.
    pub fn send_queue(&self) -> WsSendQueue {
        WsSendQueue::new(self.inner.queue_messages, self.inner.queue_bytes)
    }

    fn origin_allowed(&self, headers: &HeaderMap) -> bool {
        let Some(origin) = headers.get(ORIGIN) else {
            return true;
        };
        let Ok(origin) = origin.to_str() else {
            return false;
        };
        self.inner
            .allowed_origins
            .as_ref()
            .is_some_and(|allowed| allowed.iter().any(|candidate| candidate == origin))
    }

    /// Validates Origin and reserves capacity before writing the upgrade
    /// response. The application must perform authentication before calling it.
    pub fn on_upgrade<F, Fut>(
        &self,
        ws: WebSocketUpgrade,
        headers: &HeaderMap,
        shutdown: CancellationToken,
        handler: F,
    ) -> Response
    where
        F: FnOnce(WsSession) -> Fut + Send + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        self.on_upgrade_with_timeout(ws, headers, shutdown, self.inner.shutdown_timeout, None, handler)
    }

    /// Validates and upgrades a WebSocket using the server context's shared
    /// cancellation token and shutdown deadline.
    pub fn on_upgrade_with_context<F, Fut>(
        &self,
        ws: WebSocketUpgrade,
        headers: &HeaderMap,
        context: ServerContext,
        handler: F,
    ) -> Response
    where
        F: FnOnce(WsSession) -> Fut + Send + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        self.on_upgrade_with_timeout(
            ws,
            headers,
            context.cancellation_token(),
            context.shutdown_timeout(),
            Some(context.shutdown_deadline_handle()),
            handler,
        )
    }

    fn on_upgrade_with_timeout<F, Fut>(
        &self,
        ws: WebSocketUpgrade,
        headers: &HeaderMap,
        shutdown: CancellationToken,
        shutdown_timeout: Duration,
        server_deadline: Option<Arc<Mutex<Option<tokio::time::Instant>>>>,
        handler: F,
    ) -> Response
    where
        F: FnOnce(WsSession) -> Fut + Send + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        if self.validate().is_err() {
            return StatusCode::INTERNAL_SERVER_ERROR.into_response();
        }
        if !self.origin_allowed(headers) {
            return StatusCode::FORBIDDEN.into_response();
        }
        let Ok(permit) = self.inner.connections.clone().try_acquire_owned() else {
            return crate::limit::WebRejection::CapacityExceeded.into_response();
        };

        let policy = self.clone();
        ws.max_frame_size(self.inner.max_frame_bytes)
            .max_message_size(self.inner.max_message_bytes)
            .on_upgrade(move |socket| async move {
                let queue = policy.send_queue();
                let session_shutdown = shutdown.child_token();
                let close_code = Arc::new(AtomicU16::new(1001));
                let close_deadline = Arc::new(Mutex::new(None));
                let writer_close_code = close_code.clone();
                let writer_close_deadline = close_deadline.clone();
                let writer_server_deadline = server_deadline.clone();
                let writer_shutdown = session_shutdown.clone();
                let writer_queue = queue.clone();
                let writer_notify = queue.notify.clone();
                let (close_ack_sender, close_ack_receiver) = tokio::sync::oneshot::channel();
                let (sink, stream) = socket.split();
                let writer = tokio::spawn(async move {
                    writer_loop(
                        sink,
                        writer_queue,
                        writer_notify,
                        writer_shutdown,
                        writer_close_code,
                        writer_close_deadline,
                        writer_server_deadline,
                        close_ack_receiver,
                        shutdown_timeout,
                        permit,
                    )
                    .await;
                });
                let (incoming_tx, incoming_rx) = mpsc::channel(64);
                let reader_shutdown = session_shutdown.clone();
                let reader_close_code = close_code.clone();
                let reader_close_deadline = close_deadline.clone();
                let reader_server_deadline = server_deadline.clone();
                let reader_notify = queue.notify.clone();
                let idle_timeout = policy.inner.idle_timeout;
                let reader = tokio::spawn(async move {
                    reader_loop(
                        stream,
                        incoming_tx,
                        reader_notify,
                        reader_shutdown,
                        reader_close_code,
                        reader_close_deadline,
                        reader_server_deadline,
                        close_ack_sender,
                        idle_timeout,
                        shutdown_timeout,
                    )
                    .await;
                });
                let session = WsSession {
                    incoming: incoming_rx,
                    queue,
                    shutdown: session_shutdown,
                    idle_timeout: policy.inner.idle_timeout,
                    shutdown_timeout,
                    writer,
                    reader,
                };
                tokio::select! {
                    biased;
                    _ = shutdown.cancelled() => {},
                    _ = handler(session) => {},
                }
            })
    }
}

/// Invalid WebSocket policy configuration.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WsPolicyError {
    /// A configured connection, frame, message, queue, or timeout limit was
    /// zero.
    ZeroLimit,
}

/// Error returned when an outbound message cannot be queued.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WsSendError {
    /// The queue reached either its item or byte limit.
    Backpressure,
    /// The session has started shutting down.
    Closed,
}

#[derive(Debug)]
struct QueueState {
    messages: VecDeque<Message>,
    queued_bytes: usize,
    in_flight_messages: usize,
}

/// A nonblocking bounded queue for outbound WebSocket messages.
#[derive(Clone, Debug)]
pub struct WsSendQueue {
    state: Arc<Mutex<QueueState>>,
    notify: Arc<Notify>,
    max_messages: usize,
    max_bytes: usize,
}

impl WsSendQueue {
    /// Creates a queue whose item and byte limits must both be satisfied.
    pub fn new(max_messages: usize, max_bytes: usize) -> Self {
        Self {
            state: Arc::new(Mutex::new(QueueState {
                messages: VecDeque::new(),
                queued_bytes: 0,
                in_flight_messages: 0,
            })),
            notify: Arc::new(Notify::new()),
            max_messages,
            max_bytes,
        }
    }

    /// Enqueues a message without waiting; full queues return backpressure.
    pub fn try_send(&self, message: Message) -> Result<(), WsSendError> {
        let bytes = message_size(&message);
        let mut state = self.state.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.messages.len() + state.in_flight_messages >= self.max_messages
            || bytes > self.max_bytes.saturating_sub(state.queued_bytes)
        {
            return Err(WsSendError::Backpressure);
        }
        state.queued_bytes += bytes;
        state.messages.push_back(message);
        drop(state);
        self.notify.notify_one();
        Ok(())
    }

    /// Returns the number of queued messages.
    pub fn len(&self) -> usize {
        let state = self.state.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        state.messages.len() + state.in_flight_messages
    }

    /// Returns whether no message is queued.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn finish_message(&self, bytes: usize) {
        let mut state = self.state.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        state.in_flight_messages = state.in_flight_messages.saturating_sub(1);
        state.queued_bytes = state.queued_bytes.saturating_sub(bytes);
    }

    fn take_next(&self) -> Option<Message> {
        let mut state = self.state.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        let message = state.messages.pop_front();
        if message.is_some() {
            state.in_flight_messages += 1;
        }
        message
    }
}

fn message_size(message: &Message) -> usize {
    match message {
        Message::Text(text) => text.as_str().len(),
        Message::Binary(bytes) | Message::Ping(bytes) | Message::Pong(bytes) => bytes.len(),
        Message::Close(Some(frame)) => frame.reason.len() + 2,
        Message::Close(None) => 0,
    }
}

/// One upgraded connection. Call `recv` from one application read loop and
/// `try_send` to preserve bounded outbound buffering. A dedicated writer loop
/// drains queued messages while one reader loop delivers inbound messages.
pub struct WsSession {
    incoming: mpsc::Receiver<Result<Message, axum::Error>>,
    queue: WsSendQueue,
    shutdown: CancellationToken,
    idle_timeout: Duration,
    shutdown_timeout: Duration,
    writer: tokio::task::JoinHandle<()>,
    reader: tokio::task::JoinHandle<()>,
}

impl WsSession {
    /// Receives the next application message, returning `None` on close,
    /// shutdown, or the configured idle deadline.
    pub async fn recv(&mut self) -> Option<Result<Message, axum::Error>> {
        tokio::select! {
            biased;
            _ = self.shutdown.cancelled() => {
                self.close().await;
                None
            }
            message = tokio::time::timeout(self.idle_timeout, self.incoming.recv()) => {
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
    pub fn try_send(&self, message: Message) -> Result<(), WsSendError> {
        if self.shutdown.is_cancelled() {
            return Err(WsSendError::Closed);
        }
        self.queue.try_send(message)
    }

    /// Sends a close frame and discards queued messages.
    pub async fn close(&mut self) {
        self.shutdown.cancel();
        let wait_for_tasks = async {
            let _ = tokio::join!(&mut self.writer, &mut self.reader);
        };
        if tokio::time::timeout(self.shutdown_timeout, wait_for_tasks)
            .await
            .is_err()
        {
            self.writer.abort();
            self.reader.abort();
            let _ = tokio::join!(&mut self.writer, &mut self.reader);
        }
        self.clear_queue();
    }

    fn clear_queue(&self) {
        let mut state = self
            .queue
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let queued_bytes = state.messages.iter().map(message_size).sum::<usize>();
        state.messages.clear();
        state.queued_bytes = state.queued_bytes.saturating_sub(queued_bytes);
    }
}

impl Drop for WsSession {
    fn drop(&mut self) {
        self.shutdown.cancel();
        self.clear_queue();
    }
}

#[allow(clippy::too_many_arguments)]
async fn reader_loop(
    mut stream: futures_util::stream::SplitStream<WebSocket>,
    incoming: mpsc::Sender<Result<Message, axum::Error>>,
    writer_notify: Arc<Notify>,
    shutdown: CancellationToken,
    close_code: Arc<AtomicU16>,
    close_deadline: Arc<Mutex<Option<tokio::time::Instant>>>,
    server_deadline: Option<Arc<Mutex<Option<tokio::time::Instant>>>>,
    close_ack: tokio::sync::oneshot::Sender<()>,
    idle_timeout: Duration,
    shutdown_timeout: Duration,
) {
    let mut close_ack = Some(close_ack);
    let mut deadline = None;
    loop {
        let next = tokio::select! {
            biased;
            _ = shutdown.cancelled(), if deadline.is_none() => {
                deadline = Some(get_close_deadline(&close_deadline, server_deadline.as_ref(), shutdown_timeout));
                continue;
            }
            result = async {
                if let Some(deadline) = deadline {
                    tokio::time::timeout_at(deadline, stream.next()).await
                } else {
                    tokio::time::timeout(idle_timeout, stream.next()).await
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

async fn deliver_incoming(
    incoming: &mpsc::Sender<Result<Message, axum::Error>>,
    shutdown: &CancellationToken,
    message: Result<Message, axum::Error>,
) -> bool {
    tokio::select! {
        biased;
        _ = shutdown.cancelled() => false,
        result = incoming.send(message) => result.is_ok(),
    }
}

fn get_close_deadline(
    deadline: &Mutex<Option<tokio::time::Instant>>,
    server_deadline: Option<&Arc<Mutex<Option<tokio::time::Instant>>>>,
    timeout: Duration,
) -> tokio::time::Instant {
    let mut deadline = deadline.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
    let candidate = server_deadline
        .and_then(|shared| *shared.lock().unwrap_or_else(std::sync::PoisonError::into_inner))
        .unwrap_or_else(|| tokio::time::Instant::now() + timeout);
    let current = *deadline;
    *deadline = Some(current.map_or(candidate, |current| current.min(candidate)));
    deadline.expect("close deadline is set")
}

#[allow(clippy::too_many_arguments)]
async fn writer_loop(
    mut sink: futures_util::stream::SplitSink<WebSocket, Message>,
    queue: WsSendQueue,
    notify: Arc<Notify>,
    shutdown: CancellationToken,
    close_code: Arc<AtomicU16>,
    close_deadline: Arc<Mutex<Option<tokio::time::Instant>>>,
    server_deadline: Option<Arc<Mutex<Option<tokio::time::Instant>>>>,
    mut close_ack: tokio::sync::oneshot::Receiver<()>,
    shutdown_timeout: Duration,
    _permit: OwnedSemaphorePermit,
) {
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
            tokio::select! {
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
        let sent = tokio::select! {
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

async fn send_close_and_wait(
    sink: &mut futures_util::stream::SplitSink<WebSocket, Message>,
    close_ack: &mut tokio::sync::oneshot::Receiver<()>,
    code: u16,
    deadline: tokio::time::Instant,
) {
    match tokio::time::timeout_at(deadline, sink.send(server_shutdown_close(code))).await {
        Ok(Ok(())) => {}
        Ok(Err(_)) | Err(_) => return,
    }
    if matches!(tokio::time::timeout_at(deadline, close_ack).await, Ok(Ok(()))) {
        let _ = tokio::time::timeout_at(deadline, sink.flush()).await;
    }
}

fn server_shutdown_close(code: u16) -> Message {
    Message::Close(Some(axum::extract::ws::CloseFrame {
        code,
        reason: if code == 1001 {
            "server shutdown"
        } else {
            "protocol limit"
        }
        .into(),
    }))
}

#[cfg(test)]
mod tests {
    use axum::extract::ws::Message;

    use super::WsSendError;
    use super::WsSendQueue;
    use super::message_size;

    #[test]
    fn in_flight_messages_keep_queue_capacity_until_send_finishes() {
        let queue = WsSendQueue::new(1, 3);
        queue.try_send(Message::text("one")).unwrap();
        let in_flight = queue.take_next().unwrap();

        assert_eq!(queue.len(), 1);
        assert_eq!(queue.try_send(Message::text("two")), Err(WsSendError::Backpressure));

        queue.finish_message(message_size(&in_flight));
        assert!(queue.is_empty());
        queue.try_send(Message::text("two")).unwrap();
    }
}
