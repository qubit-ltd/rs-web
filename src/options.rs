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
/// assert_eq!(options.transport_idle_timeout(), std::time::Duration::from_secs(30));
/// let options = options.with_transport_idle_timeout(std::time::Duration::from_secs(45));
/// assert_eq!(options.transport_idle_timeout(), std::time::Duration::from_secs(45));
/// ```
#[derive(Clone, Debug)]
#[must_use]
pub struct ServerOptions {
    /// Socket address where the server accepts connections.
    pub(crate) addr: SocketAddr,
    /// Maximum duration allowed for graceful server shutdown.
    pub(crate) shutdown_timeout: Duration,
    /// Maximum duration allowed to receive an HTTP request header block.
    pub(crate) request_header_timeout: Duration,
    /// Maximum duration a transport connection may remain without an active
    /// request or response body.
    pub(crate) transport_idle_timeout: Duration,
    /// Positive per-server cap covering TLS handshakes and HTTP connection
    /// futures.
    pub(crate) max_transport_connections: NonZeroUsize,
}

impl ServerOptions {
    /// Creates server options for the explicit listening address.
    ///
    /// # Parameters
    ///
    /// * `addr` - Socket address on which the server will listen.
    ///
    /// # Returns
    ///
    /// Options with the supplied address and default timeouts and connection
    /// cap.
    pub fn new(addr: SocketAddr) -> Self {
        Self {
            addr,
            shutdown_timeout: Duration::from_secs(30),
            request_header_timeout: Duration::from_secs(10),
            transport_idle_timeout: Duration::from_secs(30),
            max_transport_connections: NonZeroUsize::new(1024).expect("default transport limit is non-zero"),
        }
    }

    /// Returns the configured listening address.
    #[must_use]
    #[inline]
    pub const fn address(&self) -> SocketAddr {
        self.addr
    }

    /// Returns the graceful shutdown deadline.
    #[must_use]
    #[inline]
    pub const fn shutdown_timeout(&self) -> Duration {
        self.shutdown_timeout
    }

    /// Returns the maximum time allowed to receive request headers.
    #[must_use]
    #[inline]
    pub const fn request_header_timeout(&self) -> Duration {
        self.request_header_timeout
    }

    /// Returns the maximum duration a transport may remain without request
    /// activity.
    #[must_use]
    #[inline]
    pub const fn transport_idle_timeout(&self) -> Duration {
        self.transport_idle_timeout
    }

    /// Returns the maximum number of transport connections for this server
    /// instance.
    #[must_use]
    #[inline]
    pub const fn max_transport_connections(&self) -> usize {
        self.max_transport_connections.get()
    }

    /// Sets the maximum time allowed for graceful shutdown.
    ///
    /// # Parameters
    ///
    /// * `timeout` - Graceful shutdown deadline; zero is rejected during
    ///   validation.
    ///
    /// # Returns
    ///
    /// The updated options.
    pub fn with_shutdown_timeout(mut self, timeout: Duration) -> Self {
        self.shutdown_timeout = timeout;
        self
    }

    /// Sets the maximum time allowed to receive an HTTP request header block.
    ///
    /// The default is 10 seconds, which also bounds an HTTP/1 WebSocket
    /// upgrade request before Axum accepts the upgrade.
    ///
    /// # Parameters
    ///
    /// * `timeout` - Header receive deadline; zero is rejected during
    ///   validation.
    ///
    /// # Returns
    ///
    /// The updated options.
    pub fn with_request_header_timeout(mut self, timeout: Duration) -> Self {
        self.request_header_timeout = timeout;
        self
    }

    /// Sets the maximum idle duration for an HTTP transport connection.
    ///
    /// Active request handlers and response bodies pause this timeout. A zero
    /// duration is rejected during validation.
    ///
    /// # Parameters
    ///
    /// * `timeout` - Idle duration before an inactive transport is closed.
    ///
    /// # Returns
    ///
    /// The updated options.
    pub fn with_transport_idle_timeout(mut self, timeout: Duration) -> Self {
        self.transport_idle_timeout = timeout;
        self
    }

    /// Sets the maximum number of accepted transport connections owned by this
    /// server instance.
    ///
    /// The limit includes TLS handshakes and HTTP connection futures. It does
    /// not include WebSocket sessions after Hyper completes an upgrade;
    /// those are governed by the WebSocket policy. When the limit is full,
    /// the server stops accepting sockets and clients may wait in the
    /// operating system backlog or fail to connect; no HTTP 503 is
    /// generated.
    ///
    /// # Parameters
    ///
    /// * `limit` - Positive maximum number of accepted transport connections.
    ///
    /// # Returns
    ///
    /// The updated options.
    ///
    /// # Errors
    ///
    /// Returns [`WebServerError::InvalidConfig`] when `limit` is zero.
    pub fn with_max_transport_connections(mut self, limit: usize) -> Result<Self, WebServerError> {
        self.max_transport_connections = NonZeroUsize::new(limit).ok_or(WebServerError::InvalidConfig)?;
        Ok(self)
    }

    /// Checks that all configured limits are finite and positive.
    ///
    /// # Returns
    ///
    /// `Ok(())` when all timeouts and connection limits are positive.
    ///
    /// # Errors
    ///
    /// Returns [`WebServerError::InvalidConfig`] if any timeout is zero.
    pub fn validate(&self) -> Result<(), WebServerError> {
        if self.shutdown_timeout.is_zero()
            || self.request_header_timeout.is_zero()
            || self.transport_idle_timeout.is_zero()
        {
            return Err(WebServerError::InvalidConfig);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::ServerOptions;

    #[test]
    fn defaults_and_builders_expose_all_server_settings() {
        let address = "127.0.0.1:8080".parse().unwrap();
        let defaults = ServerOptions::new(address);
        assert_eq!(defaults.address(), address);
        assert_eq!(defaults.shutdown_timeout(), Duration::from_secs(30));
        assert_eq!(defaults.request_header_timeout(), Duration::from_secs(10));
        assert_eq!(defaults.transport_idle_timeout(), Duration::from_secs(30));
        assert_eq!(defaults.max_transport_connections(), 1024);
        defaults.validate().unwrap();

        let configured = defaults
            .with_shutdown_timeout(Duration::from_secs(5))
            .with_request_header_timeout(Duration::from_secs(6))
            .with_transport_idle_timeout(Duration::from_secs(7))
            .with_max_transport_connections(8)
            .unwrap();
        assert_eq!(configured.shutdown_timeout(), Duration::from_secs(5));
        assert_eq!(configured.request_header_timeout(), Duration::from_secs(6));
        assert_eq!(configured.transport_idle_timeout(), Duration::from_secs(7));
        assert_eq!(configured.max_transport_connections(), 8);
        configured.validate().unwrap();
    }

    #[test]
    fn rejects_zero_timeouts_and_transport_connection_limit() {
        let address = "127.0.0.1:8080".parse().unwrap();
        assert!(
            ServerOptions::new(address)
                .with_shutdown_timeout(Duration::ZERO)
                .validate()
                .is_err()
        );
        assert!(
            ServerOptions::new(address)
                .with_request_header_timeout(Duration::ZERO)
                .validate()
                .is_err()
        );
        assert!(
            ServerOptions::new(address)
                .with_transport_idle_timeout(Duration::ZERO)
                .validate()
                .is_err()
        );
        assert!(ServerOptions::new(address).with_max_transport_connections(0).is_err());
    }
}
