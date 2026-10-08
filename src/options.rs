// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use std::net::SocketAddr;
use std::num::NonZeroUsize;
use std::time::Duration;

use crate::WebServerError;

/// Validated settings used before a server starts accepting connections.
///
/// # Examples
///
/// ```
/// use qubit_web::ServerOptions;
///
/// let options = ServerOptions::new("127.0.0.1:8080".parse().expect("valid socket address"))
///     .with_max_transport_connections(256)
///     .expect("positive transport connection limit");
/// assert_eq!(options.max_transport_connections(), 256);
/// ```
#[derive(Clone, Debug)]
pub struct ServerOptions {
    pub(crate) addr: SocketAddr,
    pub(crate) shutdown_timeout: Duration,
    pub(crate) request_header_timeout: Duration,
    /// Positive per-server cap covering TLS handshakes and HTTP connection futures.
    pub(crate) max_transport_connections: NonZeroUsize,
}

impl ServerOptions {
    /// Creates server options for the explicit listening address.
    pub fn new(addr: SocketAddr) -> Self {
        Self {
            addr,
            shutdown_timeout: Duration::from_secs(30),
            request_header_timeout: Duration::from_secs(10),
            max_transport_connections: NonZeroUsize::new(1024).expect("default transport limit is non-zero"),
        }
    }

    /// Sets the maximum time allowed for graceful shutdown.
    pub fn with_shutdown_timeout(mut self, timeout: Duration) -> Self {
        self.shutdown_timeout = timeout;
        self
    }

    /// Sets the maximum time allowed to receive an HTTP request header block.
    ///
    /// The default is 10 seconds, which also bounds an HTTP/1 WebSocket
    /// upgrade request before Axum accepts the upgrade.
    pub fn with_request_header_timeout(mut self, timeout: Duration) -> Self {
        self.request_header_timeout = timeout;
        self
    }

    /// Sets the maximum number of accepted transport connections owned by this server instance.
    ///
    /// The limit includes TLS handshakes and HTTP connection futures. It does not include
    /// WebSocket sessions after Hyper completes an upgrade; those are governed by the
    /// WebSocket policy. When the limit is full, the server stops accepting sockets and
    /// clients may wait in the operating system backlog or fail to connect; no HTTP 503 is
    /// generated.
    ///
    /// # Errors
    ///
    /// Returns [`WebServerError::InvalidConfig`] when `limit` is zero.
    pub fn with_max_transport_connections(mut self, limit: usize) -> Result<Self, WebServerError> {
        self.max_transport_connections = NonZeroUsize::new(limit).ok_or(WebServerError::InvalidConfig)?;
        Ok(self)
    }

    /// Returns the configured listening address.
    pub const fn address(&self) -> SocketAddr {
        self.addr
    }

    /// Returns the graceful shutdown deadline.
    pub const fn shutdown_timeout(&self) -> Duration {
        self.shutdown_timeout
    }

    /// Returns the maximum time allowed to receive request headers.
    pub const fn request_header_timeout(&self) -> Duration {
        self.request_header_timeout
    }

    /// Returns the maximum number of transport connections for this server instance.
    pub const fn max_transport_connections(&self) -> usize {
        self.max_transport_connections.get()
    }

    /// Checks that all configured limits are finite and positive.
    pub fn validate(&self) -> Result<(), WebServerError> {
        if self.shutdown_timeout.is_zero() || self.request_header_timeout.is_zero() {
            return Err(WebServerError::InvalidConfig);
        }
        Ok(())
    }
}
