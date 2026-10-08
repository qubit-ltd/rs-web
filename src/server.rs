// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use std::future::Future;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::AtomicUsize;
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
use tokio_util::sync::CancellationToken;
use tower::ServiceExt;

use crate::ServerOptions;
use crate::WebServerError;

/// Classification shared by route registration and request policies.
///
/// # Examples
///
/// ```
/// use qubit_web::RouteKind;
///
/// let kind = RouteKind::WebSocket;
/// assert_ne!(kind, RouteKind::Short);
/// ```
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
#[must_use]
pub enum RouteKind {
    /// A bounded, short-lived HTTP request.
    Short,
    /// A server-sent event stream.
    Sse,
    /// A WebSocket session.
    WebSocket,
}

/// Shared cancellation state for application-owned long-lived sessions.
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
/// let context = server.context();
/// let _shutdown = context.cancellation_token();
/// # Ok(())
/// # }
/// ```
#[derive(Clone, Debug)]
#[must_use]
pub struct ServerContext {
    /// Token cancelled when graceful shutdown starts.
    cancellation: CancellationToken,
    /// Atomic count of guards currently retained by long-lived sessions.
    active_sessions: Arc<AtomicUsize>,
    /// Grace period allowed after shutdown begins.
    shutdown_timeout: Duration,
    /// Shared deadline initialized once when shutdown starts.
    shutdown_deadline: Arc<Mutex<Option<tokio::time::Instant>>>,
}

impl ServerContext {
    /// Creates empty session state with the configured shutdown grace period.
    ///
    /// # Parameters
    ///
    /// * `shutdown_timeout` - Grace period applied after cancellation begins.
    ///
    /// # Returns
    ///
    /// A context with no registered sessions and no shutdown deadline.
    fn new(shutdown_timeout: Duration) -> Self {
        Self {
            cancellation: CancellationToken::new(),
            active_sessions: Arc::new(AtomicUsize::new(0)),
            shutdown_timeout,
            shutdown_deadline: Arc::new(Mutex::new(None)),
        }
    }

    /// Returns a cloneable token cancelled when server shutdown begins.
    ///
    /// # Returns
    ///
    /// A token that is cancelled when the shared shutdown process starts.
    #[must_use]
    #[inline]
    pub fn cancellation_token(&self) -> CancellationToken {
        self.cancellation.clone()
    }

    /// Returns the server's graceful-shutdown deadline for long-lived sessions.
    ///
    /// # Returns
    ///
    /// The configured grace period used to calculate the absolute deadline.
    #[must_use]
    #[inline]
    pub const fn shutdown_timeout(&self) -> Duration {
        self.shutdown_timeout
    }

    /// Returns the absolute graceful-shutdown deadline after shutdown starts.
    ///
    /// # Returns
    ///
    /// Returns `None` before shutdown begins and the shared deadline afterward.
    pub fn shutdown_deadline(&self) -> Option<tokio::time::Instant> {
        *self
            .shutdown_deadline
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Returns the number of currently registered sessions.
    ///
    /// The count covers long-lived sessions whose owners registered a guard,
    /// including SSE and WebSocket sessions that use the context-aware upgrade
    /// API. It does not count ordinary HTTP requests, pending handshakes, or
    /// WebSocket upgrades made with only a cancellation token.
    ///
    /// # Returns
    ///
    /// The number of registered session guards that have not yet been dropped.
    #[must_use]
    #[inline]
    pub fn active_sessions(&self) -> usize {
        self.active_sessions.load(Ordering::Acquire)
    }

    /// Registers an active long-lived session until the returned guard drops.
    ///
    /// # Returns
    ///
    /// A guard that decrements the active count when dropped.
    pub fn register_session(&self) -> SessionGuard {
        self.active_sessions.fetch_add(1, Ordering::AcqRel);
        SessionGuard {
            active_sessions: self.active_sessions.clone(),
        }
    }

    /// Returns the shared deadline storage used by WebSocket lifecycle
    /// tracking.
    ///
    /// # Returns
    ///
    /// A shared handle updated when the server begins shutdown.
    #[cfg(feature = "ws")]
    pub(crate) fn shutdown_deadline_handle(&self) -> Arc<Mutex<Option<tokio::time::Instant>>> {
        self.shutdown_deadline.clone()
    }

    /// Starts cancellation once and returns the deadline shared with all
    /// sessions.
    ///
    /// # Returns
    ///
    /// The first shutdown deadline, or the existing deadline if shutdown began
    /// earlier.
    #[must_use]
    fn begin_shutdown(&self) -> tokio::time::Instant {
        let deadline = tokio::time::Instant::now() + self.shutdown_timeout;
        let mut state = self
            .shutdown_deadline
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let deadline = *state.get_or_insert(deadline);
        drop(state);
        self.cancellation.cancel();
        deadline
    }
}

/// RAII registration for one active SSE or WebSocket session.
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
/// let context = server.context();
/// let guard = context.register_session();
/// assert_eq!(context.active_sessions(), 1);
/// drop(guard);
/// # Ok(())
/// # }
/// ```
#[derive(Debug)]
#[must_use]
pub struct SessionGuard {
    /// Shared count decremented when this guard is dropped.
    active_sessions: Arc<AtomicUsize>,
}

impl Drop for SessionGuard {
    fn drop(&mut self) {
        self.active_sessions.fetch_sub(1, Ordering::AcqRel);
    }
}

/// Summary of a completed graceful shutdown.
///
/// # Examples
///
/// ```
/// use std::time::Duration;
///
/// use qubit_web::ShutdownReport;
///
/// let report = ShutdownReport {
///     graceful: true,
///     forced_connections: None,
///     elapsed: Duration::from_millis(25),
/// };
/// assert!(report.graceful);
/// ```
#[derive(Clone, Debug, Eq, PartialEq)]
#[must_use]
pub struct ShutdownReport {
    /// Whether all connections finished before the shutdown deadline.
    pub graceful: bool,
    /// Number of forcibly closed connections, when the transport can prove it.
    /// This implementation returns `None` because Axum does not expose this
    /// count.
    pub forced_connections: Option<usize>,
    /// Time from shutdown notification until the server task terminated.
    pub elapsed: Duration,
}

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
    /// [`WebServerError::BindFailed`] when binding or querying the address
    /// fails.
    pub async fn bind_http(options: ServerOptions) -> Result<Self, WebServerError> {
        options.validate()?;
        let context = ServerContext::new(options.shutdown_timeout);
        let listener = TcpListener::bind(options.addr)
            .await
            .map_err(|_| WebServerError::BindFailed)?;
        let local_addr = listener.local_addr().map_err(|_| WebServerError::BindFailed)?;
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
    /// [`WebServerError::BindFailed`] when binding or address lookup fails.
    #[cfg(feature = "tls-rustls")]
    pub async fn bind_https(options: ServerOptions, config: crate::tls::TlsConfig) -> Result<Self, WebServerError> {
        options.validate()?;
        let listener = TcpListener::bind(options.addr)
            .await
            .map_err(|_| WebServerError::BindFailed)?;
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
    /// [`WebServerError::BindFailed`] if validation or address lookup fails.
    #[cfg(feature = "tls-rustls")]
    pub(crate) fn from_tls_listener(
        options: ServerOptions,
        listener: TcpListener,
        config: crate::tls::TlsConfig,
    ) -> Result<Self, WebServerError> {
        options.validate()?;
        let context = ServerContext::new(options.shutdown_timeout);
        let local_addr = listener.local_addr().map_err(|_| WebServerError::BindFailed)?;
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
    /// Returns [`WebServerError::ServeFailed`] when the accept loop fails.
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
        let connection_limit = Arc::new(Semaphore::new(options.max_transport_connections.get()));
        let graceful = GracefulShutdown::new();
        let mut connections = JoinSet::new();
        tokio::pin!(shutdown);

        let accept_failed = loop {
            tokio::select! {
                biased;
                _ = &mut shutdown => break false,
                accepted = async {
                    let permit = connection_limit.clone().acquire_owned().await.ok()?;
                    let accepted = listener.accept().await;
                    Some((permit, accepted))
                } => {
                    let Some((permit, accepted)) = accepted else {
                        break true;
                    };
                    let (stream, _) = match accepted {
                        Ok(accepted) => accepted,
                        Err(_) => break true,
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
                            serve_connection(stream, app, watcher, request_header_timeout).await;
                            return;
                        }
                        serve_connection(stream, app, watcher, request_header_timeout).await;
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
        let report = match tokio::time::timeout_at(shutdown_deadline, graceful.shutdown()).await {
            Ok(()) => {
                while connections.join_next().await.is_some() {}
                ShutdownReport {
                    graceful: true,
                    forced_connections: None,
                    elapsed: started.elapsed(),
                }
            }
            Err(_) => {
                connections.abort_all();
                while connections.join_next().await.is_some() {}
                ShutdownReport {
                    graceful: false,
                    forced_connections: None,
                    elapsed: started.elapsed(),
                }
            }
        };
        if accept_failed {
            Err(WebServerError::ServeFailed)
        } else {
            Ok(report)
        }
    }
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
async fn serve_connection<S>(
    stream: S,
    app: Router<()>,
    watcher: hyper_util::server::graceful::Watcher,
    request_header_timeout: Duration,
) where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
{
    static NEXT_CONNECTION_ID: AtomicU64 = AtomicU64::new(1);
    let connection_id = crate::diagnostic::ConnectionId(NEXT_CONNECTION_ID.fetch_add(1, Ordering::Relaxed));
    let service = service_fn(move |request: hyper::Request<hyper::body::Incoming>| {
        let app = app.clone();
        async move {
            let (mut parts, body) = request.into_parts();
            parts.extensions.insert(connection_id);
            app.oneshot(hyper::Request::from_parts(parts, Body::new(body))).await
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
    let _ = watcher.watch(connection).await;
}
