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
use hyper_util::rt::TokioExecutor;
use hyper_util::rt::TokioIo;
use hyper_util::rt::TokioTimer;
use hyper_util::server::conn::auto::Builder;
use hyper_util::server::graceful::GracefulShutdown;
use tokio::net::TcpListener;
use tokio::sync::Semaphore;
use tokio::task::JoinSet;
#[cfg(feature = "tls-rustls")]
use tokio_rustls::TlsAcceptor;
use tower::ServiceExt;

#[path = "internal/connection_activity.rs"]
mod connection_activity;
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
use connection_activity::ConnectionActivity;
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
    #[cfg(feature = "tls-rustls")]
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
            #[cfg(feature = "tls-rustls")]
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
    #[cfg(feature = "tls-rustls")]
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
    #[cfg(feature = "tls-rustls")]
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
    /// Returns [`WebServerError::ServeFailed`] when accepting a connection
    /// fails. The underlying I/O error is retained as its source and is only
    /// shown when a caller explicitly inspects that source.
    pub async fn serve<F>(self, app: Router<()>, shutdown: F) -> Result<ShutdownReport, WebServerError>
    where
        F: Future<Output = ()> + Send,
    {
        let Self {
            listener,
            options,
            context,
            #[cfg(feature = "tls-rustls")]
            tls_acceptor,
            ..
        } = self;
        let request_header_timeout = options.request_header_timeout;
        let transport_idle_timeout = options.transport_idle_timeout;
        let connection_limit = Arc::new(Semaphore::new(options.max_transport_connections.get()));
        let graceful = GracefulShutdown::new();
        let mut connections = JoinSet::new();
        tokio::pin!(shutdown);

        let accept_error = loop {
            tokio::select! {
                biased;
                _ = &mut shutdown => break None,
                accepted = async {
                    let permit = connection_limit.clone().acquire_owned().await.ok()?;
                    let accepted = listener.accept().await;
                    Some((permit, accepted))
                } => {
                    let Some((permit, accepted)) = accepted else {
                        break None;
                    };
                    let (stream, _) = match accepted {
                        Ok(accepted) => accepted,
                        Err(source) => break Some(source),
                    };
                    let app = app.clone();
                    let watcher = graceful.watcher();
                    #[cfg(feature = "tls-rustls")]
                    let tls_acceptor = tls_acceptor.clone();
                    #[cfg(feature = "tls-rustls")]
                    let cancellation = context.cancellation_token();
                    connections.spawn(async move {
                        let _permit = permit;
                        #[cfg(feature = "tls-rustls")]
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
                        )
                        .await;
                    });
                }
                joined = connections.join_next(), if !connections.is_empty() => {
                    let _ = joined;
                }
            }
        };

        drop(listener);
        let started = tokio::time::Instant::now();
        let shutdown_deadline = context.begin_shutdown();
        let report = match tokio::time::timeout_at(shutdown_deadline, async {
            tokio::join!(graceful.shutdown(), context.wait_for_sessions())
        })
        .await
        {
            Ok(((), ())) => {
                while connections.join_next().await.is_some() {}
                ShutdownReport {
                    graceful: true,
                    forced_connections: None,
                    unfinished_managed_sessions: 0,
                    elapsed: started.elapsed(),
                }
            }
            Err(_) => {
                let unfinished_managed_sessions = context.active_sessions_at(shutdown_deadline);
                connections.abort_all();
                while connections.join_next().await.is_some() {}
                ShutdownReport {
                    graceful: false,
                    forced_connections: None,
                    unfinished_managed_sessions,
                    elapsed: started.elapsed(),
                }
            }
        };
        if let Some(source) = accept_error {
            Err(serve_error(source))
        } else {
            Ok(report)
        }
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
/// * `request_header_timeout` - Deadline for receiving HTTP request headers.
/// * `transport_idle_timeout` - Maximum time without an active request or
///   response body.
async fn serve_connection<S>(
    stream: S,
    app: Router<()>,
    watcher: hyper_util::server::graceful::Watcher,
    request_header_timeout: Duration,
    transport_idle_timeout: Duration,
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
    let mut builder = Builder::new(TokioExecutor::new());
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
    fn test_preserves_injected_accept_failure_after_shutdown_cleanup() {
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
