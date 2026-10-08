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
use std::sync::PoisonError;
use std::sync::atomic::AtomicU16;
use std::sync::atomic::Ordering;
use std::time::Duration;

use axum::Error;
use axum::extract::ws::CloseFrame;
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
use futures_util::stream::SplitSink;
use futures_util::stream::SplitStream;
use tokio::join;
use tokio::select;
use tokio::spawn;
use tokio::sync::Notify;
use tokio::sync::OwnedSemaphorePermit;
use tokio::sync::Semaphore;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tokio::time::Instant;
use tokio::time::timeout;
use tokio::time::timeout_at;
use tokio_util::sync::CancellationToken;

use crate::limit::WebRejection;
use crate::server::ServerContext;
use crate::server::SessionGuard;

const DEFAULT_CONNECTIONS: usize = 128;
const DEFAULT_FRAME_BYTES: usize = 64 * 1024;
const DEFAULT_MESSAGE_BYTES: usize = 1024 * 1024;
const DEFAULT_QUEUE_MESSAGES: usize = 64;
const DEFAULT_QUEUE_BYTES: usize = 1024 * 1024;
const DEFAULT_IDLE_TIMEOUT: Duration = Duration::from_secs(60);
const DEFAULT_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(30);

/// Holds the cancellation and accounting values for one upgrade lifetime.
struct UpgradeLifecycle {
    /// Cancellation inherited by the upgraded session.
    shutdown: CancellationToken,
    /// Grace period allowed for the peer's close acknowledgement.
    shutdown_timeout: Duration,
    /// Shared absolute shutdown deadline, when a server context is available.
    server_deadline: Option<Arc<Mutex<Option<Instant>>>>,
    /// Context that accounts for this session after a successful upgrade.
    session_context: Option<ServerContext>,
}

/// Immutable connection, frame, queue, and timeout settings shared by policy
/// clones.
#[derive(Clone, Debug)]
struct PolicyInner {
    /// Semaphore tracking concurrent upgraded connections.
    connections: Arc<Semaphore>,
    /// Configured connection capacity used alongside available permits.
    max_connections: usize,
    /// Maximum size of one WebSocket frame.
    max_frame_bytes: usize,
    /// Maximum size of one reassembled WebSocket message.
    max_message_bytes: usize,
    /// Maximum number of outbound messages queued or currently being sent.
    queue_messages: usize,
    /// Maximum total bytes held by queued or in-flight outbound messages.
    queue_bytes: usize,
    /// Maximum idle interval before a session is closed.
    idle_timeout: Duration,
    /// Maximum time allowed for the peer to acknowledge a shutdown close frame.
    shutdown_timeout: Duration,
    /// Exact allowed Origin values; `None` rejects requests that provide
    /// Origin.
    allowed_origins: Option<Vec<String>>,
    /// Whether any configured finite limit was set to an invalid value.
    invalid_limits: bool,
}

/// Limits and lifecycle policy for WebSocket sessions.
///
/// # Examples
///
/// ```
/// use qubit_web::WsUpgradePolicy;
///
/// let policy = WsUpgradePolicy::new()
///     .max_connections(32)
///     .max_message_bytes(256 * 1024);
/// assert!(policy.validate().is_ok());
/// ```
#[derive(Clone, Debug)]
#[must_use]
pub struct WsUpgradePolicy {
    /// Shared immutable policy configuration and connection semaphore.
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
    ///
    /// # Returns
    ///
    /// A policy that accepts any Origin and applies finite default limits.
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

    /// Creates a separately usable send queue with this policy's limits.
    ///
    /// # Returns
    ///
    /// An independent outbound queue configured with this policy's item and
    /// byte limits.
    pub fn send_queue(&self) -> WsSendQueue {
        WsSendQueue::new(self.inner.queue_messages, self.inner.queue_bytes)
    }

    /// Returns the number of active upgraded connections.
    ///
    /// # Returns
    ///
    /// The configured capacity minus permits currently available for upgrades.
    #[must_use]
    #[inline]
    pub fn active_connections(&self) -> usize {
        self.inner.max_connections - self.inner.connections.available_permits()
    }

    /// Sets the maximum number of simultaneous upgraded connections.
    ///
    /// # Parameters
    ///
    /// * `limit` - Positive connection capacity within Semaphore's maximum.
    ///
    /// # Returns
    ///
    /// The updated policy; invalid values are reported by `validate`.
    pub fn max_connections(self, limit: usize) -> Self {
        self.map_inner(|inner| {
            let valid = (1..=Semaphore::MAX_PERMITS).contains(&limit);
            let capacity = if valid { limit } else { 0 };
            inner.invalid_limits |= !valid;
            inner.connections = Arc::new(Semaphore::new(capacity));
            inner.max_connections = capacity;
        })
    }

    /// Sets the exact accepted browser Origin values.
    ///
    /// Requests without an Origin still proceed to application authentication;
    /// requests with an Origin must match one of these values exactly.
    ///
    /// # Type Parameters
    ///
    /// * `I` - Into-iterator of allowed origin values.
    /// * `S` - Origin item convertible into an owned string.
    ///
    /// # Parameters
    ///
    /// * `origins` - Exact origin strings accepted by the upgrade policy.
    ///
    /// # Returns
    ///
    /// The updated policy.
    pub fn allowed_origins<I, S>(self, origins: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.map_inner(|inner| inner.allowed_origins = Some(origins.into_iter().map(Into::into).collect()))
    }

    /// Sets the independent maximum WebSocket frame size.
    ///
    /// # Parameters
    ///
    /// * `limit` - Positive maximum number of bytes in one frame.
    ///
    /// # Returns
    ///
    /// The updated policy; zero is reported by `validate`.
    pub fn max_frame_bytes(self, limit: usize) -> Self {
        self.map_inner(|inner| {
            inner.invalid_limits |= limit == 0;
            inner.max_frame_bytes = limit;
        })
    }

    /// Sets the reassembled WebSocket message size limit.
    ///
    /// # Parameters
    ///
    /// * `limit` - Positive maximum bytes in a complete message.
    ///
    /// # Returns
    ///
    /// The updated policy; zero is reported by `validate`.
    pub fn max_message_bytes(self, limit: usize) -> Self {
        self.map_inner(|inner| {
            inner.invalid_limits |= limit == 0;
            inner.max_message_bytes = limit;
        })
    }

    /// Sets the outbound queue's simultaneous item and byte limits.
    ///
    /// # Parameters
    ///
    /// * `messages` - Positive queue item limit, including in-flight sends.
    /// * `bytes` - Positive aggregate byte limit for queued and in-flight
    ///   sends.
    ///
    /// # Returns
    ///
    /// The updated policy; a zero value is reported by `validate`.
    pub fn queue_limits(self, messages: usize, bytes: usize) -> Self {
        self.map_inner(|inner| {
            inner.invalid_limits |= messages == 0 || bytes == 0;
            inner.queue_messages = messages;
            inner.queue_bytes = bytes;
        })
    }

    /// Sets the maximum interval without an inbound message before closing.
    ///
    /// # Parameters
    ///
    /// * `timeout` - Positive inbound idle interval.
    ///
    /// # Returns
    ///
    /// The updated policy; zero is reported by `validate`.
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
    ///
    /// # Parameters
    ///
    /// * `timeout` - Positive wait for a peer close acknowledgement.
    ///
    /// # Returns
    ///
    /// The updated policy; zero is reported by `validate`.
    pub fn shutdown_timeout(self, timeout: Duration) -> Self {
        self.map_inner(|inner| {
            inner.invalid_limits |= timeout.is_zero();
            inner.shutdown_timeout = timeout;
        })
    }

    /// Checks that all configured bounds are positive.
    ///
    /// # Returns
    ///
    /// `Ok(())` for a valid policy or [`WsPolicyError::ZeroLimit`] if a limit
    /// is zero.
    ///
    /// # Errors
    ///
    /// Returns [`WsPolicyError::ZeroLimit`] for zero or otherwise invalid
    /// bounds.
    pub fn validate(&self) -> Result<(), WsPolicyError> {
        if self.inner.invalid_limits {
            Err(WsPolicyError::ZeroLimit)
        } else {
            Ok(())
        }
    }

    /// Validates Origin and reserves capacity before writing the upgrade
    /// response. The application must perform authentication before calling it.
    /// Sessions created through this token-only entry point are not included in
    /// [`ServerContext::active_sessions`].
    ///
    /// # Type Parameters
    ///
    /// * `F` - Handler that consumes the admitted WebSocket session.
    /// * `Fut` - Sendable future returned by the handler.
    ///
    /// # Parameters
    ///
    /// * `ws` - Axum upgrade extractor for the accepted request.
    /// * `headers` - Request headers used for Origin validation.
    /// * `shutdown` - Cancellation token inherited by the upgraded session.
    /// * `handler` - Application logic that reads and writes the session.
    ///
    /// # Returns
    ///
    /// An upgrade response or an HTTP rejection for invalid policy, Origin, or
    /// capacity.
    #[must_use]
    #[inline]
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
        self.on_upgrade_with_timeout(
            ws,
            headers,
            UpgradeLifecycle {
                shutdown,
                shutdown_timeout: self.inner.shutdown_timeout,
                server_deadline: None,
                session_context: None,
            },
            handler,
        )
    }

    /// Validates and upgrades a WebSocket using the server context's shared
    /// cancellation token and shutdown deadline. After a successful upgrade,
    /// the session is included in [`ServerContext::active_sessions`] until its
    /// writer task finishes. Rejected or failed upgrades are not counted.
    ///
    /// # Type Parameters
    ///
    /// * `F` - Handler that consumes the admitted WebSocket session.
    /// * `Fut` - Sendable future returned by the handler.
    ///
    /// # Parameters
    ///
    /// * `ws` - Axum upgrade extractor for the accepted request.
    /// * `headers` - Request headers used for Origin validation.
    /// * `context` - Server cancellation and shutdown-deadline state.
    /// * `handler` - Application logic that reads and writes the session.
    ///
    /// # Returns
    ///
    /// An upgrade response or an HTTP rejection for invalid policy, Origin, or
    /// capacity.
    #[must_use]
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
            UpgradeLifecycle {
                shutdown: context.cancellation_token(),
                shutdown_timeout: context.shutdown_timeout(),
                server_deadline: Some(context.shutdown_deadline_handle()),
                session_context: Some(context.clone()),
            },
            handler,
        )
    }

    /// Applies one change to the shared configuration, cloning only when
    /// needed.
    ///
    /// # Parameters
    ///
    /// * `f` - One-shot update to the policy configuration.
    ///
    /// # Returns
    ///
    /// The policy with the update applied.
    fn map_inner(mut self, f: impl FnOnce(&mut PolicyInner)) -> Self {
        f(Arc::make_mut(&mut self.inner));
        self
    }

    /// Checks whether a request Origin exactly matches an allowed value.
    ///
    /// # Parameters
    ///
    /// * `headers` - Request headers that may contain an Origin field.
    ///
    /// # Returns
    ///
    /// `true` when no Origin is present or one exact allowed value matches.
    #[must_use]
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

    /// Validates a request and arranges the reader/writer lifecycle for an
    /// upgrade.
    ///
    /// # Type Parameters
    ///
    /// * `F` - Handler for the upgraded session.
    /// * `Fut` - Sendable future returned by the handler.
    ///
    /// # Parameters
    ///
    /// * `ws` - Axum upgrade extractor.
    /// * `headers` - Request headers inspected for Origin.
    /// * `lifecycle` - Cancellation, timeout, deadline, and session accounting
    ///   values for the upgraded session.
    /// * `handler` - Application session logic.
    ///
    /// # Returns
    ///
    /// The upgrade response or a rejection response.
    #[must_use]
    fn on_upgrade_with_timeout<F, Fut>(
        &self,
        ws: WebSocketUpgrade,
        headers: &HeaderMap,
        lifecycle: UpgradeLifecycle,
        handler: F,
    ) -> Response
    where
        F: FnOnce(WsSession) -> Fut + Send + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        let UpgradeLifecycle {
            shutdown,
            shutdown_timeout,
            server_deadline,
            session_context,
        } = lifecycle;

        if self.validate().is_err() {
            return StatusCode::INTERNAL_SERVER_ERROR.into_response();
        }
        if !self.origin_allowed(headers) {
            return StatusCode::FORBIDDEN.into_response();
        }
        let Ok(permit) = self.inner.connections.clone().try_acquire_owned() else {
            return WebRejection::CapacityExceeded.into_response();
        };

        let policy = self.clone();
        ws.max_frame_size(self.inner.max_frame_bytes)
            .max_message_size(self.inner.max_message_bytes)
            .on_upgrade(move |socket| async move {
                let session = session_context.map(|context| context.register_session());
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
                let (close_ack_sender, close_ack_receiver) = oneshot::channel();
                let (sink, stream) = socket.split();
                let writer = spawn(async move {
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
                        session,
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
                let reader = spawn(async move {
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
                select! {
                    biased;
                    _ = shutdown.cancelled() => {},
                    _ = handler(session) => {},
                }
            })
    }
}

/// Invalid WebSocket policy configuration.
///
/// # Examples
///
/// ```
/// use qubit_web::ws::WsPolicyError;
///
/// let error = WsPolicyError::ZeroLimit;
/// assert_eq!(format!("{error:?}"), "ZeroLimit");
/// ```
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[must_use]
pub enum WsPolicyError {
    /// A configured connection, frame, message, queue, or timeout limit was
    /// zero.
    ZeroLimit,
}

/// Error returned when an outbound message cannot be queued.
///
/// # Examples
///
/// ```
/// use qubit_web::ws::WsSendError;
///
/// let error = WsSendError::Backpressure;
/// assert_eq!(format!("{error:?}"), "Backpressure");
/// ```
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[must_use]
pub enum WsSendError {
    /// The queue reached either its item or byte limit.
    Backpressure,
    /// The session has started shutting down.
    Closed,
}

/// Tracks queued and in-flight message counts and byte charges under one lock.
#[derive(Debug)]
struct QueueState {
    /// Messages accepted but not yet removed by the writer task.
    messages: VecDeque<Message>,
    /// Bytes retained in queued messages and current in-flight sends.
    queued_bytes: usize,
    /// Messages removed from the queue but not yet completed by the sink.
    in_flight_messages: usize,
}

/// A nonblocking bounded queue for outbound WebSocket messages.
///
/// # Examples
///
/// ```
/// use axum::extract::ws::Message;
/// use qubit_web::ws::WsSendQueue;
///
/// let queue = WsSendQueue::new(4, 1024);
/// queue.try_send(Message::text("ready")).expect("capacity available");
/// assert_eq!(queue.len(), 1);
/// ```
#[derive(Clone, Debug)]
#[must_use]
pub struct WsSendQueue {
    /// Mutex-protected pending and in-flight queue accounting.
    state: Arc<Mutex<QueueState>>,
    /// Wake-up signal consumed by the dedicated writer task.
    notify: Arc<Notify>,
    /// Maximum pending and in-flight message count.
    max_messages: usize,
    /// Maximum bytes retained by pending and in-flight messages.
    max_bytes: usize,
}

impl WsSendQueue {
    /// Creates a queue whose item and byte limits must both be satisfied.
    ///
    /// # Parameters
    ///
    /// * `max_messages` - Maximum message capacity, including in-flight sends;
    ///   zero prevents every send.
    /// * `max_bytes` - Maximum total byte capacity; zero prevents every send.
    ///
    /// # Returns
    ///
    /// An empty queue with the supplied capacity bounds.
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

    /// Returns the number of queued messages.
    ///
    /// # Returns
    ///
    /// The queued plus currently in-flight message count.
    #[must_use]
    #[inline]
    pub fn len(&self) -> usize {
        let state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        state.messages.len() + state.in_flight_messages
    }

    /// Returns whether no message is queued.
    ///
    /// # Returns
    ///
    /// `true` when no queued or in-flight messages remain.
    #[must_use]
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Enqueues a message without waiting; full queues return backpressure.
    ///
    /// # Parameters
    ///
    /// * `message` - Text, binary, ping, pong, or close message to enqueue.
    ///
    /// # Returns
    ///
    /// `Ok(())` when accepted into the queue.
    ///
    /// # Errors
    ///
    /// Returns [`WsSendError::Backpressure`] when either capacity bound would
    /// be exceeded.
    pub fn try_send(&self, message: Message) -> Result<(), WsSendError> {
        let bytes = message_size(&message);
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
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

    /// Releases byte and item capacity after a writer finishes sending a
    /// message.
    ///
    /// # Parameters
    ///
    /// * `bytes` - Size previously charged when the message entered flight.
    fn finish_message(&self, bytes: usize) {
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        state.in_flight_messages = state.in_flight_messages.saturating_sub(1);
        state.queued_bytes = state.queued_bytes.saturating_sub(bytes);
    }

    /// Moves the next queued message into the in-flight count for the writer.
    ///
    /// # Returns
    ///
    /// The oldest queued message, or `None` when the queue is empty.
    fn take_next(&self) -> Option<Message> {
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        let message = state.messages.pop_front();
        if message.is_some() {
            state.in_flight_messages += 1;
        }
        message
    }
}

/// Measures the payload bytes charged against the outbound queue budget.
///
/// # Parameters
///
/// * `message` - WebSocket frame whose queue cost is being calculated.
///
/// # Returns
///
/// Payload size in bytes, including the close code when present.
#[must_use]
#[inline]
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
#[must_use]
pub struct WsSession {
    /// Channel carrying inbound frames from the reader task.
    incoming: mpsc::Receiver<Result<Message, Error>>,
    /// Shared bounded queue drained by the writer task.
    queue: WsSendQueue,
    /// Cancellation token shared by the application and both tasks.
    shutdown: CancellationToken,
    /// Maximum interval without an inbound application message.
    idle_timeout: Duration,
    /// Maximum time allowed for close acknowledgement during shutdown.
    shutdown_timeout: Duration,
    /// Dedicated task that writes queued messages and close frames.
    writer: JoinHandle<()>,
    /// Dedicated task that reads frames and sends them to the application.
    reader: JoinHandle<()>,
}

impl WsSession {
    /// Receives the next application message, returning `None` on close,
    /// shutdown, or the configured idle deadline.
    ///
    /// # Returns
    ///
    /// `Some(Ok(message))` for an inbound frame, `Some(Err(error))` for a
    /// transport error, and `None` after close, cancellation, or idle timeout.
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
    /// [`WsSendError::Backpressure`] when queue capacity is exhausted.
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
    /// timeout, then aborts reader and writer tasks and clears queued data.
    pub async fn close(&mut self) {
        self.shutdown.cancel();
        let wait_for_tasks = async {
            let _ = join!(&mut self.writer, &mut self.reader);
        };
        if timeout(self.shutdown_timeout, wait_for_tasks).await.is_err() {
            self.writer.abort();
            self.reader.abort();
            let _ = join!(&mut self.writer, &mut self.reader);
        }
        self.clear_queue();
    }

    /// Drops queued messages while preserving accounting for any in-flight
    /// send.
    fn clear_queue(&self) {
        let mut state = self.queue.state.lock().unwrap_or_else(PoisonError::into_inner);
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

/// Receives frames, applies idle/shutdown deadlines, and forwards input to the
/// session.
///
/// # Parameters
///
/// * `stream` - Socket half read by the session task.
/// * `incoming` - Bounded channel used to deliver frames to the application.
/// * `writer_notify` - Signal waking the writer for ping/close handling.
/// * `shutdown` - Shared token that starts the close handshake.
/// * `close_code` - Atomic close code selected for protocol and shutdown cases.
/// * `close_deadline` - Per-session close deadline shared with the writer.
/// * `server_deadline` - Optional absolute server shutdown deadline.
/// * `close_ack` - Signal used to coordinate peer close acknowledgement.
/// * `idle_timeout` - Maximum time without inbound frames before shutdown.
/// * `shutdown_timeout` - Fallback grace period when no server deadline exists.
#[allow(clippy::too_many_arguments)]
async fn reader_loop(
    mut stream: SplitStream<WebSocket>,
    incoming: mpsc::Sender<Result<Message, Error>>,
    writer_notify: Arc<Notify>,
    shutdown: CancellationToken,
    close_code: Arc<AtomicU16>,
    close_deadline: Arc<Mutex<Option<Instant>>>,
    server_deadline: Option<Arc<Mutex<Option<Instant>>>>,
    close_ack: oneshot::Sender<()>,
    idle_timeout: Duration,
    shutdown_timeout: Duration,
) {
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

/// Stores and returns the earliest applicable close deadline.
///
/// # Parameters
///
/// * `deadline` - Mutable per-session deadline slot.
/// * `server_deadline` - Optional shared server deadline, which takes
///   precedence.
/// * `timeout` - Fallback grace period when the server has no deadline.
///
/// # Returns
///
/// The earlier of the current and newly computed deadlines.
///
/// # Panics
///
/// Panics only if the internal deadline invariant is broken after storing a
/// value.
#[must_use]
fn get_close_deadline(
    deadline: &Mutex<Option<Instant>>,
    server_deadline: Option<&Arc<Mutex<Option<Instant>>>>,
    timeout: Duration,
) -> Instant {
    let mut deadline = deadline.lock().unwrap_or_else(PoisonError::into_inner);
    let candidate = server_deadline
        .and_then(|shared| *shared.lock().unwrap_or_else(PoisonError::into_inner))
        .unwrap_or_else(|| Instant::now() + timeout);
    let current = *deadline;
    *deadline = Some(current.map_or(candidate, |current| current.min(candidate)));
    deadline.expect("close deadline is set")
}

/// Drains queued messages and performs the WebSocket close handshake on
/// shutdown.
///
/// # Parameters
///
/// * `sink` - Socket half used to write frames.
/// * `queue` - Bounded outbound message queue.
/// * `notify` - Wake-up signal for newly queued messages.
/// * `shutdown` - Token that starts close handling.
/// * `close_code` - Close code selected by the reader or server.
/// * `close_deadline` - Per-session shutdown deadline slot.
/// * `server_deadline` - Optional absolute server shutdown deadline.
/// * `close_ack` - Receiver for the peer close acknowledgement signal.
/// * `shutdown_timeout` - Fallback close grace period.
/// * `_permit` - Connection capacity retained until the writer exits.
/// * `_session` - Server session registration retained until the writer exits.
#[allow(clippy::too_many_arguments)]
async fn writer_loop(
    mut sink: SplitSink<WebSocket, Message>,
    queue: WsSendQueue,
    notify: Arc<Notify>,
    shutdown: CancellationToken,
    close_code: Arc<AtomicU16>,
    close_deadline: Arc<Mutex<Option<Instant>>>,
    server_deadline: Option<Arc<Mutex<Option<Instant>>>>,
    mut close_ack: oneshot::Receiver<()>,
    shutdown_timeout: Duration,
    _permit: OwnedSemaphorePermit,
    _session: Option<SessionGuard>,
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

/// Sends a close frame and waits for the reader's peer-acknowledgement signal.
///
/// # Parameters
///
/// * `sink` - WebSocket sink used to send and flush the close frame.
/// * `close_ack` - Signal receiver completed when the reader observes a close.
/// * `code` - Protocol close code to send.
/// * `deadline` - Absolute latest time for sending and acknowledging close.
async fn send_close_and_wait(
    sink: &mut SplitSink<WebSocket, Message>,
    close_ack: &mut oneshot::Receiver<()>,
    code: u16,
    deadline: Instant,
) {
    match timeout_at(deadline, sink.send(server_shutdown_close(code))).await {
        Ok(Ok(())) => {}
        Ok(Err(_)) | Err(_) => return,
    }
    if matches!(timeout_at(deadline, close_ack).await, Ok(Ok(()))) {
        let _ = timeout_at(deadline, sink.flush()).await;
    }
}

/// Constructs the close frame used for server shutdown or protocol rejection.
///
/// # Parameters
///
/// * `code` - WebSocket close status code.
///
/// # Returns
///
/// A close message with a reason corresponding to the selected code.
#[must_use]
#[inline]
fn server_shutdown_close(code: u16) -> Message {
    Message::Close(Some(CloseFrame {
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
    fn test_in_flight_messages_keep_queue_capacity_until_send_finishes() {
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
