// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use std::future::Future;
use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::time::Duration;

use axum::Router;
use axum::body::Body;
use hyper::service::service_fn;
use hyper_util::rt::TokioIo;
use hyper_util::rt::TokioTimer;
use hyper_util::server::conn::auto::Builder;
use hyper_util::server::graceful::GracefulShutdown;
use tokio::net::TcpListener;
use tokio::sync::OwnedSemaphorePermit;
use tokio::sync::Semaphore;
use tokio::task::JoinSet;
#[cfg(feature = "tls")]
use tokio_rustls::TlsAcceptor;
use tower::ServiceExt;

#[path = "internal/accept_retry.rs"]
mod accept_retry;
#[path = "internal/connection_activity.rs"]
mod connection_activity;
#[path = "internal/connection_completion.rs"]
mod connection_completion;
#[path = "internal/connection_completion_guard.rs"]
mod connection_completion_guard;
#[path = "internal/connection_completion_state.rs"]
mod connection_completion_state;
#[path = "internal/connection_task_executor.rs"]
mod connection_task_executor;
#[path = "internal/serve_cancellation_guard.rs"]
mod serve_cancellation_guard;
#[path = "server_context.rs"]
mod server_context;
#[path = "session_guard.rs"]
mod session_guard;
#[path = "session_registration_error.rs"]
mod session_registration_error;
#[path = "internal/session_tracker.rs"]
mod session_tracker;
#[path = "shutdown_report.rs"]
mod shutdown_report;
use accept_retry::AcceptRetry;
use connection_activity::ConnectionActivity;
use connection_completion::ConnectionCompletion;
use connection_task_executor::ConnectionTaskExecutor;
use serve_cancellation_guard::ServeCancellationGuard;
pub use server_context::ServerContext;
pub use session_guard::SessionGuard;
pub use session_registration_error::SessionRegistrationError;
pub use shutdown_report::ShutdownReport;

use crate::ServerOptions;
use crate::WebServerError;

/// A bound HTTP server whose lifecycle remains under host control.
///
/// # Examples
///
/// ```
/// # #[tokio::main]
/// # async fn main() -> Result<(), qubit_web::WebServerError> {
/// use qubit_web::ServerOptions;
/// use qubit_web::WebServer;
///
/// let options = ServerOptions::new("127.0.0.1:0".parse().expect("socket address"));
/// let server = WebServer::bind_http(options).await?;
/// assert_ne!(server.local_addr().port(), 0);
/// # Ok(())
/// # }
/// ```
#[must_use]
pub struct WebServer {
    /// Bound TCP listener accepting incoming transport sockets.
    listener: TcpListener,
    /// Actual bound address, including any operating-system allocated port.
    local_addr: SocketAddr,
    /// Validated timeouts and connection-cap policy used while serving.
    options: ServerOptions,
    /// Shared shutdown and session-tracking state.
    context: ServerContext,
    /// TLS acceptor when this server was created from HTTPS configuration.
    #[cfg(feature = "tls")]
    tls_acceptor: Option<TlsAcceptor>,
}

impl std::fmt::Debug for WebServer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WebServer")
            .field("local_addr", &self.local_addr)
            .finish_non_exhaustive()
    }
}

impl WebServer {
    /// Binds an HTTP socket after validating options.
    ///
    /// # Parameters
    ///
    /// * `options` - Validated address, timeouts, and connection-cap policy.
    ///
    /// # Returns
    ///
    /// A server owning the bound listener and shared shutdown context.
    ///
    /// # Errors
    ///
    /// Returns [`WebServerError::InvalidConfig`] for invalid options or
    /// [`WebServerError::BindFailed`] when binding fails or
    /// [`WebServerError::LocalAddressFailed`] when querying the address fails.
    pub async fn bind_http(options: ServerOptions) -> Result<Self, WebServerError> {
        options.validate()?;
        let context = ServerContext::new(options.shutdown_timeout);
        let listener = TcpListener::bind(options.addr)
            .await
            .map_err(|source| WebServerError::BindFailed { source })?;
        let local_addr = local_address_result(listener.local_addr())?;
        Ok(Self {
            listener,
            local_addr,
            options,
            context,
            #[cfg(feature = "tls")]
            tls_acceptor: None,
        })
    }

    /// Binds an HTTPS socket after validating options and TLS credentials.
    ///
    /// # Parameters
    ///
    /// * `options` - Address and connection policy for the HTTPS listener.
    /// * `config` - TLS credentials and protocol configuration.
    ///
    /// # Returns
    ///
    /// A server bound to the configured HTTPS address.
    ///
    /// # Errors
    ///
    /// Returns [`WebServerError::InvalidConfig`] for invalid options or
    /// [`WebServerError::BindFailed`] when binding fails or
    /// [`WebServerError::LocalAddressFailed`] when address lookup fails.
    #[cfg(feature = "tls")]
    pub async fn bind_https(options: ServerOptions, config: crate::tls::TlsConfig) -> Result<Self, WebServerError> {
        options.validate()?;
        let listener = TcpListener::bind(options.addr)
            .await
            .map_err(|source| WebServerError::BindFailed { source })?;
        Self::from_tls_listener(options, listener, config)
    }

    /// Constructs a server from a listener prepared by the TLS module.
    ///
    /// # Parameters
    ///
    /// * `options` - Validated server policy.
    /// * `listener` - Bound listener prepared for HTTPS.
    /// * `config` - TLS configuration used for incoming handshakes.
    ///
    /// # Returns
    ///
    /// A server that applies TLS to accepted transport connections.
    ///
    /// # Errors
    ///
    /// Returns [`WebServerError::InvalidConfig`] or
    /// [`WebServerError::LocalAddressFailed`] if address lookup fails.
    #[cfg(feature = "tls")]
    pub(crate) fn from_tls_listener(
        options: ServerOptions,
        listener: TcpListener,
        config: crate::tls::TlsConfig,
    ) -> Result<Self, WebServerError> {
        options.validate()?;
        let context = ServerContext::new(options.shutdown_timeout);
        let local_addr = local_address_result(listener.local_addr())?;
        Ok(Self {
            listener,
            local_addr,
            options,
            context,
            tls_acceptor: Some(TlsAcceptor::from(config.rustls_config().get_inner())),
        })
    }

    /// Returns the actual listening address, including an allocated port.
    ///
    /// # Returns
    ///
    /// The bound address, including an operating-system allocated port when
    /// used.
    #[must_use]
    #[inline]
    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    /// Returns the context shared with long-lived application sessions.
    ///
    /// # Returns
    ///
    /// A clone sharing cancellation and session-count state with this server.
    #[inline]
    pub fn context(&self) -> ServerContext {
        self.context.clone()
    }

    /// Runs the application Router until the host's shutdown future completes.
    ///
    /// Dropping this future after it has started serving closes managed-session
    /// admission and notifies active sessions. The drop path cannot wait for
    /// asynchronous draining or return a [`ShutdownReport`]; hosts should
    /// resolve the shutdown future and await this method for graceful shutdown.
    ///
    /// # Type Parameters
    ///
    /// * `F` - Sendable future that resolves when the host requests shutdown.
    ///
    /// # Parameters
    ///
    /// * `app` - Axum router serving accepted HTTP and upgraded connections.
    /// * `shutdown` - Host-controlled shutdown notification future.
    ///
    /// # Returns
    ///
    /// A report describing graceful completion or deadline-forced shutdown.
    ///
    /// # Errors
    ///
    /// Retries transient accept failures up to eight consecutive times, with
    /// an interruptible exponential delay; a successful accept resets the
    /// count. Fatal failures and the ninth consecutive failure end serving
    /// after shutdown cleanup. The underlying I/O error is retained as the
    /// source of [`WebServerError::ServeFailed`]. Returns
    /// [`WebServerError::InvalidConfig`] if the shutdown deadline cannot be
    /// represented; the listener is closed and spawned connection tasks are
    /// aborted and reaped before returning.
    pub async fn serve<F>(self, app: Router<()>, shutdown: F) -> Result<ShutdownReport, WebServerError>
    where
        F: Future<Output = ()> + Send,
    {
        let Self {
            listener,
            options,
            context,
            #[cfg(feature = "tls")]
            tls_acceptor,
            ..
        } = self;
        let mut cancellation_guard = ServeCancellationGuard::new(context.clone());
        let request_header_timeout = options.request_header_timeout;
        let transport_idle_timeout = options.transport_idle_timeout;
        let connection_limit = Arc::new(Semaphore::new(options.max_transport_connections.get()));
        let graceful = GracefulShutdown::new();
        let completion = ConnectionCompletion::new();
        let connection_tasks = ConnectionTaskExecutor::new();
        let listener = Arc::new(listener);
        let mut retry = AcceptRetry::default();
        tokio::pin!(shutdown);

        let accept_listener = Arc::clone(&listener);
        let accept_limit = connection_limit.clone();
        let accept_completion = completion.clone();
        let (accept_error, connections) = run_accept_loop(
            JoinSet::new(),
            shutdown,
            &mut retry,
            move |retry_at| {
                let listener = Arc::clone(&accept_listener);
                let connection_limit = accept_limit.clone();
                async move {
                    if let Some(deadline) = retry_at {
                        tokio::time::sleep_until(deadline).await;
                    }
                    let permit = connection_limit.acquire_owned().await.ok()?;
                    Some((permit, listener.accept().await))
                }
            },
            |(stream, _), permit, connections| {
                let app = app.clone();
                let watcher = graceful.watcher();
                let completion = accept_completion.clone();
                let connection_tasks = connection_tasks.clone();
                #[cfg(feature = "tls")]
                let tls_acceptor = tls_acceptor.clone();
                #[cfg(feature = "tls")]
                let cancellation = context.cancellation_token();
                let completion_guard = completion.register();
                connections.spawn(async move {
                    let _completion_guard = completion_guard;
                    let _permit = permit;
                    #[cfg(feature = "tls")]
                    if let Some(acceptor) = tls_acceptor {
                        let accepted = tokio::select! {
                            biased;
                            _ = cancellation.cancelled() => return,
                            result = tokio::time::timeout(
                                request_header_timeout,
                                acceptor.accept(stream),
                            ) => result,
                        };
                        let Ok(Ok(stream)) = accepted else { return };
                        serve_connection(
                            stream,
                            app,
                            watcher,
                            request_header_timeout,
                            transport_idle_timeout,
                            connection_tasks,
                        )
                        .await;
                        return;
                    }
                    serve_connection(
                        stream,
                        app,
                        watcher,
                        request_header_timeout,
                        transport_idle_timeout,
                        connection_tasks,
                    )
                    .await;
                });
            },
        )
        .await;

        let result = finish_serve(
            listener,
            context,
            graceful,
            completion,
            connection_tasks,
            connections,
            accept_error,
        )
        .await;
        cancellation_guard.disarm();
        result
    }
}

enum AcceptResult<T> {
    Accepted(T),
    Retry,
    Terminal(io::Error),
}

async fn run_accept_loop<T, MakeAccept, AcceptFuture, OnAccepted, Shutdown>(
    mut connections: JoinSet<()>,
    mut shutdown: Shutdown,
    retry: &mut AcceptRetry,
    mut make_accept: MakeAccept,
    mut on_accepted: OnAccepted,
) -> (Option<io::Error>, JoinSet<()>)
where
    MakeAccept: FnMut(Option<tokio::time::Instant>) -> AcceptFuture,
    AcceptFuture: Future<Output = Option<(OwnedSemaphorePermit, io::Result<T>)>>,
    OnAccepted: FnMut(T, OwnedSemaphorePermit, &mut JoinSet<()>),
    Shutdown: Future<Output = ()> + Unpin,
{
    let accept_error = loop {
        let event = select_server_event(&mut connections, &mut shutdown, make_accept(retry.retry_at())).await;
        match event {
            ServerEvent::Shutdown => break None,
            ServerEvent::Joined => {}
            ServerEvent::Accept(None) => break None,
            ServerEvent::Accept(Some((permit, result))) => match handle_accept_result(retry, result) {
                AcceptResult::Accepted(accepted) => on_accepted(accepted, permit, &mut connections),
                AcceptResult::Retry => drop(permit),
                AcceptResult::Terminal(source) => {
                    drop(permit);
                    break Some(source);
                }
            },
        }
    };
    (accept_error, connections)
}

fn handle_accept_result<T>(retry: &mut AcceptRetry, result: io::Result<T>) -> AcceptResult<T> {
    match result {
        Ok(accepted) => {
            retry.record_success();
            AcceptResult::Accepted(accepted)
        }
        Err(source) => match retry.record_error(source) {
            Some(_) => AcceptResult::Retry,
            None => AcceptResult::Terminal(retry.take_terminal_error().expect("terminal accept error is retained")),
        },
    }
}

async fn finish_serve(
    listener: Arc<TcpListener>,
    context: ServerContext,
    graceful: GracefulShutdown,
    completion: ConnectionCompletion,
    connection_tasks: ConnectionTaskExecutor,
    mut connections: JoinSet<()>,
    accept_error: Option<io::Error>,
) -> Result<ShutdownReport, WebServerError> {
    drop(listener);
    let started = tokio::time::Instant::now();
    let shutdown_deadline = match context.begin_shutdown() {
        Ok(deadline) => deadline,
        Err(error) => {
            let shutdown_deadline = context.shutdown_deadline().unwrap_or_else(tokio::time::Instant::now);
            let tracked_tasks = connection_tasks.close();
            connections.abort_all();
            tokio::join!(tracked_tasks.wait_until(shutdown_deadline), async {
                while connections.join_next().await.is_some() {}
            },);
            return Err(error);
        }
    };
    let tracked_tasks = connection_tasks.close();
    let (_wait_result, managed_task_report) = tokio::join!(
        tokio::time::timeout_at(shutdown_deadline, async {
            tokio::join!(graceful.shutdown(), context.wait_for_sessions())
        }),
        tracked_tasks.wait_until(shutdown_deadline),
    );
    let unfinished_managed_sessions = context.active_sessions_at(shutdown_deadline);
    let transport_completed_by_deadline = completion.completed_by(shutdown_deadline);
    let graceful = unfinished_managed_sessions == 0
        && transport_completed_by_deadline
        && managed_task_report.completed_by_deadline;
    if !graceful {
        connections.abort_all();
    }
    while connections.join_next().await.is_some() {}
    let report = ShutdownReport {
        graceful,
        forced_connections: None,
        unfinished_managed_sessions,
        elapsed: started.elapsed(),
    };
    if let Some(source) = accept_error {
        Err(serve_error(source))
    } else {
        Ok(report)
    }
}

enum ServerEvent<A> {
    Shutdown,
    Accept(A),
    Joined,
}

async fn select_server_event<A, S>(connections: &mut JoinSet<()>, shutdown: S, accept: A) -> ServerEvent<A::Output>
where
    A: Future,
    S: Future<Output = ()>,
{
    tokio::select! {
        biased;
        _ = shutdown => ServerEvent::Shutdown,
        joined = connections.join_next(), if !connections.is_empty() => {
            let _ = joined;
            ServerEvent::Joined
        }
        accepted = accept => ServerEvent::Accept(accepted),
    }
}

/// Preserves an injected or real local-address lookup failure as a public
/// error.
///
/// # Parameters
///
/// * `result` - The result returned by the listener's address query.
///
/// # Returns
///
/// The local address, or an error retaining the original I/O source.
fn local_address_result(result: io::Result<SocketAddr>) -> Result<SocketAddr, WebServerError> {
    result.map_err(|source| WebServerError::LocalAddressFailed { source })
}

/// Wraps an accept-loop error after the server has completed shutdown cleanup.
///
/// # Parameters
///
/// * `source` - The original error returned by `TcpListener::accept`.
///
/// # Returns
///
/// A serving error that exposes the original I/O error through `source()`.
fn serve_error(source: io::Error) -> WebServerError {
    WebServerError::ServeFailed { source }
}

/// Adapts one TCP or TLS stream into an HTTP connection with upgrades enabled.
///
/// # Type Parameters
///
/// * `S` - Asynchronous bidirectional stream used by Hyper.
///
/// # Parameters
///
/// * `stream` - Accepted transport stream, optionally after TLS negotiation.
/// * `app` - Router cloned into each request service call.
/// * `watcher` - Graceful shutdown watcher for this connection.
/// * `request_header_timeout` - Deadline for receiving HTTP/1 request headers.
/// * `transport_idle_timeout` - Maximum time without an active request or
///   response body.
async fn serve_connection<S>(
    stream: S,
    app: Router<()>,
    watcher: hyper_util::server::graceful::Watcher,
    request_header_timeout: Duration,
    transport_idle_timeout: Duration,
    connection_tasks: ConnectionTaskExecutor,
) where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
{
    static NEXT_CONNECTION_ID: AtomicU64 = AtomicU64::new(1);
    let connection_id = crate::diagnostic::ConnectionId(NEXT_CONNECTION_ID.fetch_add(1, Ordering::Relaxed));
    let activity = ConnectionActivity::new();
    let service_activity = activity.clone();
    let service = service_fn(move |request: hyper::Request<hyper::body::Incoming>| {
        let app = app.clone();
        let activity = service_activity.clone();
        async move {
            let lease = connection_activity::request_lease(&activity);
            let (mut parts, body) = request.into_parts();
            parts.extensions.insert(connection_id);
            app.oneshot(hyper::Request::from_parts(parts, Body::new(body)))
                .await
                .map(|response| response.map(|body| connection_activity::retain_response_activity(body, lease)))
        }
    });
    let mut builder = Builder::new(connection_tasks);
    builder
        .http1()
        .timer(TokioTimer::new())
        .header_read_timeout(Some(request_header_timeout));
    let connection = builder
        .serve_connection_with_upgrades(TokioIo::new(stream), service)
        .into_owned();
    tokio::select! {
        _ = watcher.watch(connection) => {}
        _ = activity.wait_until_idle_for(transport_idle_timeout) => {}
    }
}

#[cfg(test)]
mod error_tests {
    use std::error::Error;
    use std::io;

    use super::local_address_result;
    use super::serve_error;
    use crate::WebServerError;

    #[test]
    fn test_preserves_injected_local_address_failure() {
        let injected = io::Error::new(io::ErrorKind::PermissionDenied, "address sentinel");
        let error = local_address_result(Err(injected)).expect_err("address lookup should fail");

        assert!(matches!(&error, WebServerError::LocalAddressFailed { .. }));
        assert_eq!(error.code(), "local_address_failed");
        assert_eq!(
            Error::source(&error)
                .and_then(|source| source.downcast_ref::<io::Error>())
                .map(io::Error::kind),
            Some(io::ErrorKind::PermissionDenied)
        );
    }

    #[test]
    fn test_accept_terminal_error_preserves_original_source() {
        let injected = io::Error::new(io::ErrorKind::ConnectionAborted, "accept sentinel");
        let error = serve_error(injected);

        assert!(matches!(&error, WebServerError::ServeFailed { .. }));
        assert_eq!(error.code(), "serve_failed");
        assert_eq!(
            Error::source(&error)
                .and_then(|source| source.downcast_ref::<io::Error>())
                .map(io::Error::kind),
            Some(io::ErrorKind::ConnectionAborted)
        );
    }
}

#[cfg(test)]
mod server_event_tests {
    use std::future::Future;
    use std::future::pending;
    use std::future::ready;
    use std::pin::pin;
    use std::task::Context;
    use std::task::Poll;
    use std::task::Waker;

    use tokio::sync::oneshot;
    use tokio::task::JoinSet;

    use super::ServerEvent;
    use super::select_server_event;

    #[tokio::test(flavor = "current_thread")]
    async fn test_accept_readiness_does_not_starve_completed_join_set_entries() {
        let mut connections = JoinSet::new();
        let (completed_tx, completed_rx) = oneshot::channel();
        connections.spawn(async move {
            completed_tx.send(()).expect("completion receiver remains open");
        });
        completed_rx.await.expect("connection task completed");
        tokio::task::yield_now().await;

        let mut reaped = 0;
        for accepted in 0..8 {
            match select_server_event(&mut connections, pending(), ready(accepted)).await {
                ServerEvent::Accept(value) => assert_eq!(value, accepted),
                ServerEvent::Joined => reaped += 1,
                ServerEvent::Shutdown => panic!("shutdown is pending"),
            }
        }

        let completed_entry_retained = connections.try_join_next().is_some();
        assert!(
            reaped > 0,
            "ready accept won every biased selection; reaped={reaped}, completed entry retained={completed_entry_retained}"
        );
    }

    #[tokio::test]
    async fn test_accept_is_selected_when_join_set_has_no_completed_entries() {
        let mut connections = JoinSet::new();

        let event = select_server_event(&mut connections, pending(), ready("accepted")).await;

        assert!(matches!(event, ServerEvent::Accept("accepted")));
        assert!(connections.is_empty());
    }

    #[test]
    fn test_empty_join_set_does_not_make_selector_ready() {
        let mut connections = JoinSet::new();
        let mut event = pin!(select_server_event(&mut connections, pending::<()>(), pending::<()>(),));
        let mut context = Context::from_waker(Waker::noop());

        assert!(matches!(event.as_mut().poll(&mut context), Poll::Pending));
    }

    #[tokio::test]
    async fn test_shutdown_is_selected_before_ready_accept() {
        let mut connections = JoinSet::new();

        let event = select_server_event(&mut connections, ready(()), ready("accepted")).await;

        assert!(matches!(event, ServerEvent::Shutdown));
    }
}

#[cfg(test)]
mod accept_retry_tests {
    use std::collections::VecDeque;
    use std::future::pending;
    use std::io;
    use std::sync::Arc;
    use std::sync::atomic::AtomicBool;
    use std::sync::atomic::Ordering;
    use std::time::Duration;

    use hyper_util::server::graceful::GracefulShutdown;
    use tokio::net::TcpListener;
    use tokio::sync::Semaphore;
    use tokio::sync::mpsc;
    use tokio::sync::oneshot;
    use tokio::task::JoinSet;
    use tokio::time::Instant;
    use tokio::time::timeout;

    use super::AcceptResult;
    use super::accept_retry::AcceptRetry;
    use super::connection_completion::ConnectionCompletion;
    use super::connection_task_executor::ConnectionTaskExecutor;
    use super::finish_serve;
    use super::handle_accept_result;
    use super::run_accept_loop;
    use super::server_context::ServerContext;

    #[tokio::test]
    async fn test_finish_serve_reaps_hyper_tasks_when_deadline_overflows() {
        struct SignalOnDrop(Arc<AtomicBool>);

        impl Drop for SignalOnDrop {
            fn drop(&mut self) {
                self.0.store(true, Ordering::SeqCst);
            }
        }

        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind test listener");
        let context = ServerContext::new(Duration::MAX);
        let completion = ConnectionCompletion::new();
        let connection_tasks = ConnectionTaskExecutor::new();
        let dropped = Arc::new(AtomicBool::new(false));
        let task_signal = SignalOnDrop(dropped.clone());
        connection_tasks
            .spawn(async move {
                let _signal = task_signal;
                pending::<()>().await;
            })
            .expect("executor should accept tasks before shutdown");

        let result = timeout(
            Duration::from_secs(1),
            finish_serve(
                Arc::new(listener),
                context,
                GracefulShutdown::new(),
                completion,
                connection_tasks,
                JoinSet::new(),
                None,
            ),
        )
        .await
        .expect("overflow cleanup should finish after aborting tracked Hyper tasks");

        assert!(matches!(result, Err(crate::WebServerError::InvalidConfig)));
        assert!(dropped.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn test_finish_serve_waits_for_hyper_tasks_and_marks_forced_shutdown() {
        struct SignalOnDrop(Arc<AtomicBool>);

        impl Drop for SignalOnDrop {
            fn drop(&mut self) {
                self.0.store(true, Ordering::SeqCst);
            }
        }

        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind test listener");
        let context = ServerContext::new(Duration::from_millis(20));
        let completion = ConnectionCompletion::new();
        let connection_tasks = ConnectionTaskExecutor::new();
        let dropped = Arc::new(AtomicBool::new(false));
        let task_signal = SignalOnDrop(dropped.clone());
        connection_tasks
            .spawn(async move {
                let _signal = task_signal;
                pending::<()>().await;
            })
            .expect("executor should accept tasks before shutdown");

        let report = timeout(
            Duration::from_secs(1),
            finish_serve(
                Arc::new(listener),
                context,
                GracefulShutdown::new(),
                completion,
                connection_tasks,
                JoinSet::new(),
                None,
            ),
        )
        .await
        .expect("shutdown cleanup should finish after aborting tracked Hyper tasks")
        .expect("no accept error should be returned");

        assert!(!report.graceful);
        assert_eq!(report.unfinished_managed_sessions, 0);
        assert!(report.elapsed >= Duration::from_millis(20));
        assert!(dropped.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn test_finish_serve_reports_late_transport_completion_as_not_graceful() {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind test listener");
        let context = ServerContext::new(Duration::from_millis(20));
        let deadline = context.begin_shutdown().expect("shutdown deadline is representable");
        let completion = ConnectionCompletion::new();
        let late_completion = deadline + Duration::from_millis(1);
        completion.record_completion(late_completion);
        tokio::time::sleep_until(late_completion).await;

        let report = finish_serve(
            Arc::new(listener),
            context,
            GracefulShutdown::new(),
            completion,
            ConnectionTaskExecutor::new(),
            JoinSet::new(),
            None,
        )
        .await
        .expect("no accept error should be returned");

        assert!(!report.graceful);
        assert_eq!(report.unfinished_managed_sessions, 0);
    }

    #[tokio::test]
    async fn test_finish_serve_keeps_on_time_transport_graceful_when_observed_late() {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind test listener");
        let context = ServerContext::new(Duration::from_millis(20));
        let deadline = context.begin_shutdown().expect("shutdown deadline is representable");
        let completion = ConnectionCompletion::new();
        completion.record_completion(deadline - Duration::from_millis(1));
        tokio::time::sleep_until(deadline + Duration::from_millis(1)).await;

        let report = finish_serve(
            Arc::new(listener),
            context,
            GracefulShutdown::new(),
            completion,
            ConnectionTaskExecutor::new(),
            JoinSet::new(),
            None,
        )
        .await
        .expect("no accept error should be returned");

        assert!(report.graceful);
        assert_eq!(report.unfinished_managed_sessions, 0);
    }

    #[test]
    fn test_accept_transient_error_then_success_keeps_server_running() {
        let mut retry = AcceptRetry::default();
        let before = Instant::now();
        let retry_at =
            match handle_accept_result::<()>(&mut retry, Err(io::Error::from(io::ErrorKind::ConnectionAborted))) {
                AcceptResult::Retry => retry.retry_at().expect("retry state retains its deadline"),
                _ => panic!("transient failure must schedule retry"),
            };
        assert!(retry_at.duration_since(before) >= Duration::from_millis(10));
        assert!(retry_at.duration_since(before) < Duration::from_millis(11));
        assert!(matches!(
            handle_accept_result(&mut retry, Ok(())),
            AcceptResult::Accepted(())
        ));
        assert!(retry.retry_at().is_none());
        let before = Instant::now();
        let retry_at =
            match handle_accept_result::<()>(&mut retry, Err(io::Error::from(io::ErrorKind::ConnectionAborted))) {
                AcceptResult::Retry => retry.retry_at().expect("retry state retains its deadline"),
                _ => panic!("failure after success starts a new retry series"),
            };
        assert!(retry_at.duration_since(before) >= Duration::from_millis(10));
        assert!(retry_at.duration_since(before) < Duration::from_millis(11));
    }

    #[tokio::test]
    async fn test_accept_loop_retries_controlled_error_then_accepts_connection() {
        let results = Arc::new(std::sync::Mutex::new(VecDeque::from([
            Err(io::Error::from(io::ErrorKind::ConnectionAborted)),
            Ok(23),
        ])));
        let permits = Arc::new(Semaphore::new(1));
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let (accepted_tx, accepted_rx) = oneshot::channel();
        let mut shutdown_tx = Some(shutdown_tx);
        let mut accepted_tx = Some(accepted_tx);
        let mut retry = AcceptRetry::default();
        let shutdown = async move {
            let _ = shutdown_rx.await;
        };
        tokio::pin!(shutdown);

        let (error, connections) = run_accept_loop(
            JoinSet::new(),
            shutdown,
            &mut retry,
            {
                let results = results.clone();
                let permits = permits.clone();
                move |retry_at| {
                    let results = results.clone();
                    let permits = permits.clone();
                    async move {
                        if let Some(deadline) = retry_at {
                            tokio::time::sleep_until(deadline).await;
                        }
                        let permit = permits.acquire_owned().await.ok()?;
                        let result = results
                            .lock()
                            .expect("scripted results lock")
                            .pop_front()
                            .expect("script contains error and then success");
                        Some((permit, result))
                    }
                }
            },
            move |accepted, _permit, _connections| {
                accepted_tx
                    .take()
                    .expect("only one scripted accept succeeds")
                    .send(accepted)
                    .expect("acceptance receiver remains open");
                shutdown_tx
                    .take()
                    .expect("shutdown is requested once")
                    .send(())
                    .expect("shutdown receiver remains open");
            },
        )
        .await;

        assert!(error.is_none(), "a transient error must not terminate accept loop");
        assert_eq!(accepted_rx.await.expect("successful accept is delivered"), 23);
        assert_eq!(retry.consecutive_failures(), 0, "success resets retry count");
        assert!(connections.is_empty());
    }

    #[tokio::test]
    async fn test_accept_loop_fatal_error_stops_after_one_accept_attempt() {
        let permits = Arc::new(Semaphore::new(1));
        let accept_calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let mut retry = AcceptRetry::default();
        let (error, connections) = run_accept_loop(
            JoinSet::new(),
            std::future::pending::<()>(),
            &mut retry,
            {
                let permits = permits.clone();
                let accept_calls = accept_calls.clone();
                move |_retry_at| {
                    let permits = permits.clone();
                    let accept_calls = accept_calls.clone();
                    async move {
                        accept_calls.fetch_add(1, Ordering::SeqCst);
                        let permit = permits.acquire_owned().await.ok()?;
                        Some((
                            permit,
                            Err::<(), io::Error>(io::Error::new(io::ErrorKind::PermissionDenied, "fatal sentinel")),
                        ))
                    }
                }
            },
            |_accepted, _permit, _connections| {},
        )
        .await;

        assert_eq!(accept_calls.load(Ordering::SeqCst), 1, "fatal error is not retried");
        assert_eq!(
            error.expect("fatal accept error terminates loop").to_string(),
            "fatal sentinel"
        );
        assert!(connections.is_empty());
    }

    #[tokio::test]
    async fn test_accept_loop_releases_permit_before_retry_backoff() {
        let permits = Arc::new(Semaphore::new(1));
        let permit_observer = permits.clone();
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let (retried_tx, retried_rx) = oneshot::channel();
        let mut retried_tx = Some(retried_tx);
        let mut accept_count = 0;
        let mut accept_loop = tokio::spawn(async move {
            let shutdown = async move {
                let _ = shutdown_rx.await;
            };
            tokio::pin!(shutdown);
            let mut retry = AcceptRetry::default();
            run_accept_loop(
                JoinSet::new(),
                shutdown,
                &mut retry,
                move |retry_at| {
                    let permits = permits.clone();
                    let current_attempt = accept_count;
                    accept_count += 1;
                    let retried_tx = if current_attempt == 0 { None } else { retried_tx.take() };
                    async move {
                        if let Some(deadline) = retry_at {
                            tokio::time::sleep_until(deadline).await;
                        }
                        let permit = permits.acquire_owned().await.ok()?;
                        if current_attempt == 0 {
                            return Some((permit, Err(io::Error::from(io::ErrorKind::ConnectionAborted))));
                        }
                        retried_tx
                            .expect("second accept receives the retry notification")
                            .send(())
                            .expect("test observes reacquired permit");
                        let _permit = permit;
                        std::future::pending::<Option<(tokio::sync::OwnedSemaphorePermit, io::Result<()>)>>().await
                    }
                },
                |_accepted, _permit, _connections| {},
            )
            .await
        });

        let retry_result = tokio::time::timeout(Duration::from_secs(1), retried_rx).await;
        if let Err(error) = retry_result {
            shutdown_tx.send(()).expect("shutdown receiver remains open");
            accept_loop.abort();
            let _ = accept_loop.await;
            panic!("retry must reacquire the only transport permit: {error}");
        }
        retry_result
            .expect("retry notification arrived")
            .expect("accept loop reacquired the permit");
        shutdown_tx.send(()).expect("shutdown receiver remains open");
        let loop_result = tokio::time::timeout(Duration::from_secs(1), &mut accept_loop).await;
        if let Err(error) = loop_result {
            accept_loop.abort();
            let _ = accept_loop.await;
            panic!("shutdown cancels the pending retry accept: {error}");
        }
        let (_error, connections) = loop_result
            .expect("loop result was checked")
            .expect("accept loop task completes");
        assert!(connections.is_empty());
        assert_eq!(
            permit_observer.available_permits(),
            1,
            "cancelled accept releases its permit"
        );
    }

    #[tokio::test]
    async fn test_accept_shutdown_cancels_rebuilt_backoff_future() {
        let permits = Arc::new(Semaphore::new(1));
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let (retry_started_tx, retry_started_rx) = oneshot::channel();
        let mut retry_started_tx = Some(retry_started_tx);
        let mut accept_count = 0;
        let mut accept_loop = tokio::spawn(async move {
            let shutdown = async move {
                let _ = shutdown_rx.await;
            };
            tokio::pin!(shutdown);
            let mut retry = AcceptRetry::default();
            run_accept_loop(
                JoinSet::new(),
                shutdown,
                &mut retry,
                move |retry_at| {
                    let permits = permits.clone();
                    let this_accept = accept_count;
                    accept_count += 1;
                    let retry_started_tx = if this_accept == 0 {
                        None
                    } else {
                        retry_started_tx.take()
                    };
                    async move {
                        if this_accept == 0 {
                            let permit = permits.acquire_owned().await.ok()?;
                            return Some((permit, Err(io::Error::from(io::ErrorKind::ConnectionAborted))));
                        }
                        let deadline = retry_at.expect("retry future receives its saved deadline");
                        retry_started_tx
                            .expect("retry future is built once")
                            .send(())
                            .expect("test observes retry future");
                        tokio::time::sleep_until(deadline).await;
                        let permit = permits.acquire_owned().await.ok()?;
                        let _permit = permit;
                        std::future::pending::<Option<(tokio::sync::OwnedSemaphorePermit, io::Result<()>)>>().await
                    }
                },
                |_accepted, _permit, _connections| {},
            )
            .await
        });

        retry_started_rx
            .await
            .expect("loop rebuilt the accept future during backoff");
        shutdown_tx.send(()).expect("shutdown receiver remains open");
        let loop_result = tokio::time::timeout(Duration::from_secs(1), &mut accept_loop).await;
        if let Err(error) = loop_result {
            accept_loop.abort();
            let _ = accept_loop.await;
            panic!("shutdown must cancel the pending retry future: {error}");
        }
        let (_error, connections) = loop_result
            .expect("loop result was checked")
            .expect("accept loop task completes");
        assert!(connections.is_empty());
    }

    #[tokio::test]
    async fn test_accept_join_rebuilds_future_without_resetting_backoff_deadline() {
        let permits = Arc::new(Semaphore::new(1));
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let (join_tx, join_rx) = oneshot::channel();
        let (deadlines_tx, mut deadlines_rx) = mpsc::unbounded_channel();
        let mut accept_count = 0;
        let mut connections = JoinSet::new();
        connections.spawn(async move {
            let _ = join_rx.await;
        });

        let mut accept_loop = tokio::spawn(async move {
            let shutdown = async move {
                let _ = shutdown_rx.await;
            };
            tokio::pin!(shutdown);
            let mut retry = AcceptRetry::default();
            run_accept_loop(
                connections,
                shutdown,
                &mut retry,
                move |retry_at| {
                    let permits = permits.clone();
                    let deadlines_tx = deadlines_tx.clone();
                    let this_accept = accept_count;
                    accept_count += 1;
                    async move {
                        if this_accept == 0 {
                            let permit = permits.acquire_owned().await.ok()?;
                            return Some((permit, Err(io::Error::from(io::ErrorKind::ConnectionAborted))));
                        }
                        let deadline = retry_at.expect("retry future receives its saved deadline");
                        deadlines_tx.send(deadline).expect("deadline observer remains open");
                        tokio::time::sleep_until(deadline).await;
                        let permit = permits.acquire_owned().await.ok()?;
                        let _permit = permit;
                        std::future::pending::<Option<(tokio::sync::OwnedSemaphorePermit, io::Result<()>)>>().await
                    }
                },
                |_accepted, _permit, _connections| {},
            )
            .await
        });

        let first_deadline = deadlines_rx.recv().await.expect("first retry future is built");
        join_tx.send(()).expect("test connection task remains pending");
        let second_deadline = tokio::time::timeout(Duration::from_secs(1), deadlines_rx.recv())
            .await
            .expect("join event rebuilds accept future")
            .expect("rebuilt future reports its deadline");
        assert_eq!(first_deadline, second_deadline, "join must not restart backoff");
        shutdown_tx.send(()).expect("shutdown receiver remains open");
        let loop_result = tokio::time::timeout(Duration::from_secs(1), &mut accept_loop).await;
        if let Err(error) = loop_result {
            accept_loop.abort();
            let _ = accept_loop.await;
            panic!("shutdown must interrupt rebuilt retry wait: {error}");
        }
        loop_result
            .expect("loop result was checked")
            .expect("accept loop task completes");
    }

    #[test]
    fn test_accept_success_resets_consecutive_failure_limit() {
        let mut retry = AcceptRetry::default();
        for _ in 0..8 {
            assert!(matches!(
                handle_accept_result::<()>(&mut retry, Err(io::Error::from(io::ErrorKind::ConnectionAborted))),
                AcceptResult::Retry
            ));
        }
        assert!(matches!(
            handle_accept_result(&mut retry, Ok(())),
            AcceptResult::Accepted(())
        ));
        for _ in 0..8 {
            assert!(matches!(
                handle_accept_result::<()>(&mut retry, Err(io::Error::from(io::ErrorKind::ConnectionAborted))),
                AcceptResult::Retry
            ));
        }
        let final_result = handle_accept_result::<()>(
            &mut retry,
            Err(io::Error::new(io::ErrorKind::ConnectionAborted, "last source")),
        );
        let AcceptResult::Terminal(final_error) = final_result else {
            panic!("ninth consecutive failure must terminate")
        };
        assert_eq!(final_error.to_string(), "last source");
    }

    #[test]
    fn test_accept_fatal_error_terminates_without_retry() {
        let mut retry = AcceptRetry::default();
        let result = handle_accept_result::<()>(
            &mut retry,
            Err(io::Error::new(io::ErrorKind::PermissionDenied, "fatal source")),
        );
        let AcceptResult::Terminal(error) = result else {
            panic!("fatal failure must terminate immediately")
        };
        assert_eq!(error.to_string(), "fatal source");
    }

    #[test]
    fn test_accept_backoff_is_exponential_and_capped() {
        let delays: Vec<_> = (1..=8).map(super::accept_retry::accept_retry_delay).collect();
        assert_eq!(delays, [10, 20, 40, 80, 160, 320, 640, 1000].map(Duration::from_millis));
    }

    #[tokio::test]
    async fn test_accept_terminal_error_closes_listener_and_reaps_connection_task() {
        struct DropSignal(Arc<AtomicBool>);

        impl Drop for DropSignal {
            fn drop(&mut self) {
                self.0.store(true, Ordering::SeqCst);
            }
        }

        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind test listener");
        let address = listener.local_addr().expect("read test listener address");
        let dropped = Arc::new(AtomicBool::new(false));
        let session_cancelled = Arc::new(AtomicBool::new(false));
        let context = ServerContext::new(Duration::from_millis(50));
        let cancellation = context.cancellation_token();
        let session_guard = context
            .try_register_session()
            .expect("session registers before shutdown");
        let mut connections = JoinSet::new();
        let watcher = GracefulShutdown::new();
        let task_dropped = dropped.clone();
        connections.spawn(async move {
            let _drop_signal = DropSignal(task_dropped);
            std::future::pending::<()>().await;
        });
        let was_cancelled = session_cancelled.clone();
        connections.spawn(async move {
            let _session_guard = session_guard;
            cancellation.cancelled().await;
            was_cancelled.store(true, Ordering::SeqCst);
            std::future::pending::<()>().await;
        });

        let mut failures: VecDeque<io::Result<()>> = VecDeque::new();
        for _ in 0..8 {
            failures.push_back(Err(io::Error::from(io::ErrorKind::ConnectionAborted)));
        }
        failures.push_back(Err(io::Error::new(
            io::ErrorKind::ConnectionAborted,
            "ninth accept sentinel",
        )));
        let failures = Arc::new(std::sync::Mutex::new(failures));
        let permits = Arc::new(Semaphore::new(1));
        let mut retry = AcceptRetry::default();
        let (accept_error, connections) = run_accept_loop(
            connections,
            std::future::pending::<()>(),
            &mut retry,
            {
                let failures = failures.clone();
                let permits = permits.clone();
                move |_retry_at| {
                    let failures = failures.clone();
                    let permits = permits.clone();
                    async move {
                        let permit = permits.acquire_owned().await.ok()?;
                        let result = failures
                            .lock()
                            .expect("scripted failures lock")
                            .pop_front()
                            .expect("script provides nine accept failures");
                        Some((permit, result))
                    }
                }
            },
            |_accepted, _permit, _connections| {},
        )
        .await;
        let accept_error = accept_error.expect("ninth transient failure terminates the accept loop");
        assert_eq!(accept_error.to_string(), "ninth accept sentinel");

        let result = finish_serve(
            Arc::new(listener),
            context,
            watcher,
            ConnectionCompletion::new(),
            ConnectionTaskExecutor::new(),
            connections,
            Some(accept_error),
        )
        .await;

        let error = result.expect_err("terminal accept error is returned after cleanup");
        assert!(matches!(&error, crate::WebServerError::ServeFailed { .. }));
        let source = std::error::Error::source(&error)
            .and_then(|source| source.downcast_ref::<io::Error>())
            .expect("the original accept error remains the source");
        assert_eq!(source.kind(), io::ErrorKind::ConnectionAborted);
        assert_eq!(source.to_string(), "ninth accept sentinel");
        assert!(
            dropped.load(Ordering::SeqCst),
            "connection task must exit and be reaped"
        );
        assert!(
            session_cancelled.load(Ordering::SeqCst),
            "terminal accept error initiates managed session cancellation"
        );
        assert!(
            TcpListener::bind(address).await.is_ok(),
            "terminal accept error closes listener"
        );
    }
}
