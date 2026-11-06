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

use axum::Router;
use axum::body::Body;
use hyper::service::service_fn;
use hyper_util::rt::TokioIo;
use hyper_util::rt::TokioTimer;
use hyper_util::server::conn::auto::Builder;
use hyper_util::server::graceful::GracefulShutdown;
use tokio::net::TcpListener;
use tokio::sync::Semaphore;
use tokio::task::JoinSet;
use tower::ServiceExt;
use tracing::Instrument;
use tracing::instrument::WithSubscriber;

// Accept-loop responsibilities stay private to this server implementation.
#[path = "internal/accept_loop.rs"]
mod accept_loop;
#[path = "internal/accept_result.rs"]
mod accept_result;
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
#[path = "internal/connection_diagnostics.rs"]
mod connection_diagnostics;
#[path = "internal/connection_settings.rs"]
mod connection_settings;
#[path = "internal/connection_task_executor.rs"]
mod connection_task_executor;
// Shutdown cleanup is kept separate from the public WebServer API.
#[path = "internal/finish_serve.rs"]
mod finish_serve;
#[path = "internal/serve_cancellation_guard.rs"]
mod serve_cancellation_guard;
#[path = "server_context.rs"]
mod server_context;
#[path = "internal/server_event.rs"]
mod server_event;
#[path = "session_guard.rs"]
mod session_guard;
#[path = "session_registration_error.rs"]
mod session_registration_error;
#[path = "internal/session_tracker.rs"]
mod session_tracker;
#[path = "shutdown_report.rs"]
mod shutdown_report;
#[cfg(feature = "tls")]
#[path = "internal/tls_acceptor_source.rs"]
mod tls_acceptor_source;
use accept_loop::run_accept_loop;
use accept_retry::AcceptRetry;
use connection_activity::ConnectionActivity;
use connection_completion::ConnectionCompletion;
use connection_diagnostics::ConnectionDiagnostics;
use connection_settings::ConnectionSettings;
use connection_task_executor::ConnectionTaskExecutor;
use finish_serve::finish_serve;
use serve_cancellation_guard::ServeCancellationGuard;
pub use server_context::ServerContext;
pub use session_guard::SessionGuard;
pub use session_registration_error::SessionRegistrationError;
pub use shutdown_report::ShutdownReport;
#[cfg(feature = "tls")]
use tls_acceptor_source::TlsAcceptorSource;

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
    tls_acceptor: Option<TlsAcceptorSource>,
}

impl std::fmt::Debug for WebServer {
    /// Formats the server's stable identifying state.
    ///
    /// # Parameters
    ///
    /// * `f` - Formatter receiving the server debug representation.
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
    /// `TlsConfig::from_rustls` snapshots its configuration at bind time;
    /// `TlsConfig::from_reloadable_rustls` takes one current snapshot before
    /// each new connection handshake.
    ///
    /// # Parameters
    ///
    /// * `options` - Address and connection policy for the HTTPS listener.
    /// * `config` - TLS credentials and protocol configuration.
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
            tls_acceptor: Some(if config.is_reloadable() {
                TlsAcceptorSource::Reloadable(config.rustls_config())
            } else {
                TlsAcceptorSource::Snapshot(tokio_rustls::TlsAcceptor::from(config.rustls_config().get_inner()))
            }),
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
    /// Deadlines use cooperative Tokio cancellation. Synchronous work that does
    /// not yield can delay timeout checks and cleanup, and may finish
    /// successfully after the deadline. Cancelling a wait does not stop an
    /// already running `spawn_blocking` task or application-spawned work.
    ///
    /// Each request received over HTTP or HTTPS includes Axum's
    /// `ConnectInfo<SocketAddr>` for the accepted TCP peer. This is the direct
    /// peer (which may be a proxy), not a verified end-user identity; forwarded
    /// headers are not trusted. A Router invoked on its own, such as with
    /// `oneshot`, has no transport peer and tests must insert the extension:
    ///
    /// ```
    /// use axum::extract::ConnectInfo;
    /// use std::net::SocketAddr;
    ///
    /// async fn handler(ConnectInfo(peer): ConnectInfo<SocketAddr>) -> String {
    ///     peer.to_string()
    /// }
    /// ```
    ///
    /// # Type Parameters
    ///
    /// * `F` - Sendable future that resolves when the host requests shutdown.
    /// # Parameters
    ///
    /// * `app` - Axum router serving accepted HTTP and upgraded connections.
    /// * `shutdown` - Host-controlled shutdown notification future.
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
        let connection_settings = ConnectionSettings {
            request_header_timeout: options.request_header_timeout,
            transport_idle_timeout: options.transport_idle_timeout,
            http2_max_concurrent_streams: options.http2_max_concurrent_streams.get(),
            http1_max_buffer_bytes: options.http1_max_buffer_bytes,
            http2_max_header_list_bytes: options.http2_max_header_list_bytes,
        };
        let connection_limit = Arc::new(Semaphore::new(options.max_transport_connections.get()));
        let graceful = GracefulShutdown::new();
        let completion = ConnectionCompletion::new();
        let connection_tasks = ConnectionTaskExecutor::new();
        let diagnostics = Arc::new(ConnectionDiagnostics::default());
        let connection_dispatch = tracing::dispatcher::get_default(Clone::clone);
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
            |(stream, peer_addr), permit, connections| {
                let app = app.clone();
                let watcher = graceful.watcher();
                let completion = accept_completion.clone();
                let connection_tasks = connection_tasks.clone();
                let diagnostics = Arc::clone(&diagnostics);
                let connection_dispatch = connection_dispatch.clone();
                #[cfg(feature = "tls")]
                let tls_acceptor = tls_acceptor.clone();
                #[cfg(feature = "tls")]
                let cancellation = context.cancellation_token();
                let completion_guard = completion.register();
                connections.spawn(
                    async move {
                        let _completion_guard = completion_guard;
                        let _permit = permit;
                        #[cfg(feature = "tls")]
                        if let Some(source) = tls_acceptor {
                            let acceptor = source.for_handshake();
                            let accepted = tokio::select! {
                                biased;
                                _ = cancellation.cancelled() => return,
                                result = tokio::time::timeout(
                                    connection_settings.request_header_timeout,
                                    acceptor.accept(stream),
                                ) => result,
                            };
                            let stream = match accepted {
                                Ok(Ok(stream)) => stream,
                                Ok(Err(_)) => {
                                    diagnostics.record_tls_handshake_failure();
                                    return;
                                }
                                Err(_) => {
                                    diagnostics.record_tls_handshake_timeout();
                                    return;
                                }
                            };
                            serve_connection(
                                stream,
                                peer_addr,
                                app,
                                watcher,
                                connection_settings,
                                connection_tasks,
                                diagnostics,
                            )
                            .await;
                            return;
                        }
                        serve_connection(
                            stream,
                            peer_addr,
                            app,
                            watcher,
                            connection_settings,
                            connection_tasks,
                            diagnostics,
                        )
                        .await;
                    }
                    .instrument(tracing::Span::current())
                    .with_subscriber(connection_dispatch),
                );
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

/// Preserves an injected or real local-address lookup failure as a public
/// error.
///
/// # Parameters
///
/// * `result` - The result returned by the listener's address query.
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
/// # Parameters
///
/// * `stream` - Accepted transport stream, optionally after TLS negotiation.
/// * `peer_addr` - Remote socket address of the accepted connection.
/// * `app` - Router cloned into each request service call.
/// * `watcher` - Graceful shutdown watcher for this connection.
/// * `settings` - Per-connection timeouts and protocol limits.
/// * `connection_tasks` - Executor registering Hyper tasks so shutdown can wait
/// * `diagnostics` - Per-serve sampled failure counters.
async fn serve_connection<S>(
    stream: S,
    peer_addr: SocketAddr,
    app: Router<()>,
    watcher: hyper_util::server::graceful::Watcher,
    settings: ConnectionSettings,
    connection_tasks: ConnectionTaskExecutor,
    diagnostics: Arc<ConnectionDiagnostics>,
) where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
{
    let ConnectionSettings {
        request_header_timeout,
        transport_idle_timeout,
        http2_max_concurrent_streams,
        http1_max_buffer_bytes,
        http2_max_header_list_bytes,
    } = settings;
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
            parts.extensions.insert(axum::extract::ConnectInfo(peer_addr));
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
    if let Some(bytes) = http1_max_buffer_bytes {
        builder.http1().max_buf_size(bytes);
    }
    builder
        .http2()
        .max_concurrent_streams(Some(http2_max_concurrent_streams));
    if let Some(bytes) = http2_max_header_list_bytes {
        builder.http2().max_header_list_size(bytes);
    }
    let connection = builder
        .serve_connection_with_upgrades(TokioIo::new(stream), service)
        .into_owned();
    tokio::select! {
        result = watcher.watch(connection) => {
            if result.is_err() {
                diagnostics.record_http_connection_failure();
            }
        }
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
