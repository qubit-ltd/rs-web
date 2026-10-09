// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! WebSocket admission policy and upgrade lifecycle.

use std::future::Future;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicU16;
use std::time::Duration;

use axum::extract::ws::WebSocketUpgrade;
use axum::http::HeaderMap;
use axum::http::StatusCode;
use axum::http::header::ORIGIN;
use axum::response::IntoResponse;
use axum::response::Response;
use futures_util::StreamExt;
use tokio::select;
use tokio::spawn;
use tokio::sync::Semaphore;
use tokio::sync::mpsc;
use tokio::sync::oneshot;

use super::internal::PolicyInner;
use super::internal::SessionLifecycle;
use super::internal::UpgradeLifecycle;
use super::internal::reader_loop;
use super::internal::server_shutting_down_response;
use super::internal::writer_loop;
use super::ws_policy_error::WsPolicyError;
use super::ws_send_queue::WsSendQueue;
use super::ws_session::WsSession;
use crate::limit::WebRejection;
use crate::server::ServerContext;

const DEFAULT_CONNECTIONS: usize = 128;
const DEFAULT_FRAME_BYTES: usize = 64 * 1024;
const DEFAULT_MESSAGE_BYTES: usize = 1024 * 1024;
const DEFAULT_QUEUE_MESSAGES: usize = 64;
const DEFAULT_QUEUE_BYTES: usize = 1024 * 1024;
const DEFAULT_IDLE_TIMEOUT: Duration = Duration::from_secs(60);

/// Limits and lifecycle policy for WebSocket sessions.
///
/// Each call to [`new`](Self::new) or [`default`](Self::default) creates an
/// independent connection-capacity domain. Cloning a policy shares its domain.
/// To apply one limit across routes or handlers, create the policy once during
/// application startup, store it in application state, and use clones obtained
/// from that state (for example, through Axum's
/// [`axum::extract::State`]). [`active_connections`](Self::active_connections)
/// counts upgrades in this domain, including its clones; it does not count
/// connections admitted by other policies or all server sessions.
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
    /// The returned policy starts an independent connection-capacity domain.
    /// Cloning it shares that domain. Create it once at application startup and
    /// reuse clones from application state when routes or handlers must share
    /// the same limit.
    ///
    /// # Returns
    ///
    /// A policy that accepts requests without an Origin, rejects requests
    /// carrying an Origin until an allowlist is configured, and applies finite
    /// default limits.
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
    /// This counts only upgrades using this policy's capacity domain,
    /// including clones of this policy.
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
    /// The returned policy uses a new connection-capacity domain with the
    /// requested limit. Because configuration updates use copy-on-write,
    /// existing policy clones keep their previous configuration and domain.
    /// Already admitted sessions also retain permits in the previous domain;
    /// they are not included in this policy's
    /// [`active_connections`](Self::active_connections) count.
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

    /// Sets the maximum interval without inbound progress before closing.
    ///
    /// # Parameters
    ///
    /// * `timeout` - Positive maximum interval without a received frame or
    ///   successful delivery of a frame to the application.
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

    /// Validates Origin and reserves policy and server-session capacity before
    /// writing the upgrade response. The application must perform
    /// authentication before calling it. The session is counted while the
    /// upgrade response is pending; failed upgrades release both reservations
    /// when Axum drops the callback, while successful sessions retain them
    /// until the writer exits. Registrations are rejected after shutdown
    /// starts.
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
    /// capacity, or a server that is shutting down.
    #[must_use]
    #[inline]
    pub fn on_upgrade<F, Fut>(
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
        let lifecycle = UpgradeLifecycle {
            shutdown: context.cancellation_token(),
            shutdown_timeout: context.shutdown_timeout(),
            server_deadline: Some(context.shutdown_deadline_handle()),
        };
        self.on_upgrade_with_timeout(ws, headers, context, lifecycle, handler)
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
        context: ServerContext,
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
        let Ok(session_guard) = context.try_register_session() else {
            return server_shutting_down_response();
        };

        let policy = self.clone();
        ws.max_frame_size(self.inner.max_frame_bytes)
            .max_message_size(self.inner.max_message_bytes)
            .on_upgrade(move |socket| async move {
                let queue = policy.send_queue();
                let session_shutdown = shutdown.child_token();
                let close_code = Arc::new(AtomicU16::new(1001));
                let close_deadline = Arc::new(Mutex::new(None));
                let writer_lifecycle = SessionLifecycle {
                    shutdown: session_shutdown.clone(),
                    close_code: close_code.clone(),
                    close_deadline: close_deadline.clone(),
                    server_deadline: server_deadline.clone(),
                    shutdown_timeout,
                };
                let writer_queue = queue.clone();
                let writer_notify = queue.notify.clone();
                let (close_ack_sender, close_ack_receiver) = oneshot::channel();
                let (sink, stream) = socket.split();
                let writer = spawn(async move {
                    writer_loop(
                        sink,
                        writer_queue,
                        writer_notify,
                        writer_lifecycle,
                        close_ack_receiver,
                        permit,
                        session_guard,
                    )
                    .await;
                });
                let (incoming_tx, incoming_rx) = mpsc::channel(64);
                let reader_lifecycle = SessionLifecycle {
                    shutdown: session_shutdown.clone(),
                    close_code: close_code.clone(),
                    close_deadline: close_deadline.clone(),
                    server_deadline: server_deadline.clone(),
                    shutdown_timeout,
                };
                let reader_queue = queue.clone();
                let idle_timeout = policy.inner.idle_timeout;
                let reader = spawn(async move {
                    reader_loop(
                        stream,
                        incoming_tx,
                        reader_queue,
                        reader_lifecycle,
                        close_ack_sender,
                        idle_timeout,
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

#[cfg(test)]
mod tests {
    use super::WsUpgradePolicy;

    #[test]
    fn default_policy_has_valid_finite_limits() {
        assert!(WsUpgradePolicy::default().validate().is_ok());
    }
}
