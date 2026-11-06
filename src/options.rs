// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use std::net::SocketAddr;
use std::num::NonZeroU32;
use std::num::NonZeroUsize;
use std::time::Duration;

use crate::WebServerError;

/// Detailed validation error for [`ServerOptions`].
pub mod server_options_validation_error;
pub use server_options_validation_error::ServerOptionsValidationError;

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
/// let h2_options = ServerOptions::new("127.0.0.1:8080".parse().expect("valid socket address"))
///     .with_http2_max_concurrent_streams(32)
///     .expect("positive HTTP/2 stream limit");
/// assert_eq!(h2_options.http2_max_concurrent_streams(), 32);
/// ```
#[derive(Clone, Debug)]
#[must_use]
pub struct ServerOptions {
    /// Socket address where the server accepts connections.
    pub(crate) addr: SocketAddr,
    /// Maximum duration allowed for graceful server shutdown.
    pub(crate) shutdown_timeout: Duration,
    /// Maximum duration allowed to receive HTTP/1 request headers. HTTPS TLS
    /// handshakes use the same duration; HTTP/2 request headers are not timed
    /// by the HTTP/1 header timer. The timeout must form a representable Tokio
    /// deadline. Defaults to 10 seconds.
    pub(crate) request_header_timeout: Duration,
    /// Maximum duration a transport connection may remain without an active
    /// request or response body.
    pub(crate) transport_idle_timeout: Duration,
    /// Positive per-server cap covering TLS handshakes and HTTP connection
    /// futures.
    pub(crate) max_transport_connections: NonZeroUsize,
    /// Maximum concurrent HTTP/2 streams on each connection.
    pub(crate) http2_max_concurrent_streams: NonZeroU32,
    /// Optional maximum HTTP/1 parser buffer size. `None` keeps Hyper's
    /// default.
    pub(crate) http1_max_buffer_bytes: Option<usize>,
    /// Optional HTTP/2 decoded header-list budget. `None` keeps Hyper's
    /// default.
    pub(crate) http2_max_header_list_bytes: Option<u32>,
}

impl ServerOptions {
    /// Creates server options for the explicit listening address.
    ///
    /// # Parameters
    ///
    /// * `addr` - Socket address on which the server will listen.
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
            http2_max_concurrent_streams: NonZeroU32::new(200).expect("default HTTP/2 stream limit is non-zero"),
            http1_max_buffer_bytes: None,
            http2_max_header_list_bytes: None,
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

    /// Returns the maximum time allowed to receive HTTP/1 request headers.
    /// HTTPS TLS handshakes use the same duration; HTTP/2 request headers are
    /// not timed by the HTTP/1 header timer.
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

    /// Returns the maximum number of concurrent HTTP/2 streams per connection.
    ///
    /// The default is 200. This connection-level protocol limit is independent
    /// of the server's transport connection, route concurrency, and session
    /// limits; it is not a process-wide request or memory limit.
    #[must_use]
    #[inline]
    pub const fn http2_max_concurrent_streams(&self) -> u32 {
        self.http2_max_concurrent_streams.get()
    }

    /// Returns the optional maximum HTTP/1 parser buffer size in bytes.
    /// This bounds parser buffering, not an exact HTTP header-list byte count.
    ///
    /// # Returns
    ///
    /// `Some(bytes)` when an explicit parser-buffer limit is configured, or
    /// `None` when Hyper's default is preserved.
    #[must_use]
    #[inline]
    pub fn http1_max_buffer_bytes(&self) -> Option<usize> {
        self.http1_max_buffer_bytes
    }

    /// Returns the optional HTTP/2 decoded header-list limit in bytes.
    /// This is independent of connection count, stream count, dynamic-table
    /// state, and body limits.
    ///
    /// # Returns
    ///
    /// `Some(bytes)` when an explicit header-list limit is configured, or
    /// `None` when Hyper's default is preserved.
    #[must_use]
    #[inline]
    pub fn http2_max_header_list_bytes(&self) -> Option<u32> {
        self.http2_max_header_list_bytes
    }

    /// Sets the HTTP/1 parser buffer limit. Values below 8192 are invalid.
    ///
    /// # Parameters
    ///
    /// * `bytes` - Maximum parser-buffer size in bytes, at least 8192.
    /// # Returns
    ///
    /// Updated server options containing the explicit buffer limit.
    ///
    /// # Errors
    ///
    /// Returns [`WebServerError::InvalidConfig`] when `bytes` is below 8192.
    pub fn with_http1_max_buffer_bytes(mut self, bytes: usize) -> Result<Self, WebServerError> {
        if bytes < 8192 {
            return Err(WebServerError::InvalidConfig);
        }
        self.http1_max_buffer_bytes = Some(bytes);
        Ok(self)
    }

    /// Sets the HTTP/2 decoded header-list limit in bytes.
    ///
    /// # Parameters
    ///
    /// * `bytes` - Positive decoded header-list limit in bytes.
    /// # Returns
    ///
    /// Updated server options containing the explicit header-list limit.
    ///
    /// # Errors
    ///
    /// Returns [`WebServerError::InvalidConfig`] when `bytes` is zero.
    pub fn with_http2_max_header_list_bytes(mut self, bytes: u32) -> Result<Self, WebServerError> {
        if bytes == 0 {
            return Err(WebServerError::InvalidConfig);
        }
        self.http2_max_header_list_bytes = Some(bytes);
        Ok(self)
    }

    /// Sets the maximum number of concurrent HTTP/2 streams per connection.
    ///
    /// # Parameters
    ///
    /// * `streams` - Positive concurrent stream limit advertised to each HTTP/2
    ///   connection.
    /// # Errors
    ///
    /// Returns [`WebServerError::InvalidConfig`] when `streams` is zero.
    pub fn with_http2_max_concurrent_streams(mut self, streams: u32) -> Result<Self, WebServerError> {
        self.http2_max_concurrent_streams = NonZeroU32::new(streams).ok_or(WebServerError::InvalidConfig)?;
        Ok(self)
    }

    /// Sets the maximum time allowed for graceful shutdown.
    ///
    /// # Parameters
    ///
    /// * `timeout` - Graceful shutdown deadline. The value is stored as-is;
    ///   zero or an unrepresentable Tokio deadline is rejected by
    ///   [`validate`](Self::validate).
    /// # Returns
    ///
    /// The updated options.
    pub fn with_shutdown_timeout(mut self, timeout: Duration) -> Self {
        self.shutdown_timeout = timeout;
        self
    }

    /// Sets the maximum time allowed to receive HTTP/1 request headers.
    ///
    /// The default is 10 seconds. HTTPS TLS handshakes use this same duration,
    /// and HTTP/1 WebSocket upgrade requests are bounded before Axum accepts
    /// the upgrade. HTTP/2 request headers are not timed by the HTTP/1 header
    /// timer.
    ///
    /// # Parameters
    ///
    /// * `timeout` - Header receive deadline. The value is stored as-is; zero
    ///   or an unrepresentable Tokio deadline is rejected by
    ///   [`validate`](Self::validate).
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
    /// duration or a duration that cannot form a Tokio deadline is rejected
    /// during validation.
    ///
    /// # Parameters
    ///
    /// * `timeout` - Idle duration before an inactive transport is closed. The
    ///   value is stored as-is; zero or an unrepresentable Tokio deadline is
    ///   rejected by [`validate`](Self::validate).
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
    /// * `limit` - Maximum accepted transport connections for this server
    ///   instance, from 1 through `Semaphore::MAX_PERMITS`.
    /// # Returns
    ///
    /// The updated options.
    ///
    /// # Errors
    ///
    /// Returns [`WebServerError::InvalidConfig`] when `limit` is zero or
    /// exceeds `tokio::sync::Semaphore::MAX_PERMITS`.
    pub fn with_max_transport_connections(mut self, limit: usize) -> Result<Self, WebServerError> {
        if !(1..=tokio::sync::Semaphore::MAX_PERMITS).contains(&limit) {
            return Err(WebServerError::InvalidConfig);
        }
        self.max_transport_connections = NonZeroUsize::new(limit).ok_or(WebServerError::InvalidConfig)?;
        Ok(self)
    }

    /// Checks that all configured limits are finite and positive.
    ///
    /// # Returns
    ///
    /// `Ok(())` when all timeouts are positive and form representable Tokio
    /// deadlines, and connection limits are valid.
    ///
    /// # Errors
    ///
    /// Returns [`WebServerError::InvalidConfig`] if a timeout is zero, any
    /// timeout deadline cannot be represented by Tokio, or the transport
    /// capacity exceeds `tokio::sync::Semaphore::MAX_PERMITS`.
    pub fn validate(&self) -> Result<(), WebServerError> {
        self.validate_detailed().map_err(|_| WebServerError::InvalidConfig)
    }

    /// Checks the configured limits and reports the first invalid field.
    ///
    /// Fields are checked in this order: shutdown timeout, request-header
    /// timeout, transport idle timeout, transport connection capacity, HTTP/1
    /// parser buffer, then HTTP/2 header-list budget. Timeouts share one clock
    /// reading for this validation call. A successful result does not replace
    /// the runtime deadline checks performed when the server operates.
    ///
    /// # Returns
    ///
    /// `Ok(())` when all configured limits are valid.
    ///
    /// # Errors
    ///
    /// Returns the first invalid field and a stable reason code without
    /// including the configured value.
    pub fn validate_detailed(&self) -> Result<(), ServerOptionsValidationError> {
        let now = tokio::time::Instant::now();
        for (field, timeout) in [
            ("shutdown_timeout", self.shutdown_timeout),
            ("request_header_timeout", self.request_header_timeout),
            ("transport_idle_timeout", self.transport_idle_timeout),
        ] {
            if timeout.is_zero() {
                return Err(ServerOptionsValidationError::new(field, "zero"));
            }
            if now.checked_add(timeout).is_none() {
                return Err(ServerOptionsValidationError::new(field, "deadline_unrepresentable"));
            }
        }
        if self.max_transport_connections.get() > tokio::sync::Semaphore::MAX_PERMITS {
            return Err(ServerOptionsValidationError::new(
                "max_transport_connections",
                "capacity_too_large",
            ));
        }
        if self.http1_max_buffer_bytes.is_some_and(|bytes| bytes < 8192) {
            return Err(ServerOptionsValidationError::new(
                "http1_max_buffer_bytes",
                "below_minimum",
            ));
        }
        if self.http2_max_header_list_bytes == Some(0) {
            return Err(ServerOptionsValidationError::new("http2_max_header_list_bytes", "zero"));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::ServerOptions;

    #[test]
    fn test_invalid_private_protocol_header_budgets_fail_validation() {
        let address = "127.0.0.1:8080".parse().unwrap();
        let defaults = ServerOptions::new(address);
        let mut invalid = defaults.clone();
        invalid.http1_max_buffer_bytes = Some(8191);
        assert!(matches!(invalid.validate(), Err(crate::WebServerError::InvalidConfig)));
        let mut invalid = defaults.clone();
        invalid.http2_max_header_list_bytes = Some(0);
        assert!(matches!(invalid.validate(), Err(crate::WebServerError::InvalidConfig)));
    }

    #[test]
    fn test_detailed_validation_reports_all_fields_and_reason_codes() {
        let address = "127.0.0.1:8080".parse().unwrap();
        let defaults = ServerOptions::new(address);
        let cases = [
            (
                defaults.clone().with_shutdown_timeout(Duration::ZERO),
                "shutdown_timeout",
                "zero",
            ),
            (
                defaults.clone().with_shutdown_timeout(Duration::MAX),
                "shutdown_timeout",
                "deadline_unrepresentable",
            ),
            (
                defaults.clone().with_request_header_timeout(Duration::ZERO),
                "request_header_timeout",
                "zero",
            ),
            (
                defaults.clone().with_request_header_timeout(Duration::MAX),
                "request_header_timeout",
                "deadline_unrepresentable",
            ),
            (
                defaults.clone().with_transport_idle_timeout(Duration::ZERO),
                "transport_idle_timeout",
                "zero",
            ),
            (
                defaults.clone().with_transport_idle_timeout(Duration::MAX),
                "transport_idle_timeout",
                "deadline_unrepresentable",
            ),
        ];
        for (options, field, code) in cases {
            let error = options.validate_detailed().unwrap_err();
            assert_eq!((error.field(), error.code()), (field, code));
        }

        let mut invalid = defaults.clone();
        invalid.max_transport_connections =
            std::num::NonZeroUsize::new(tokio::sync::Semaphore::MAX_PERMITS + 1).unwrap();
        let error = invalid.validate_detailed().unwrap_err();
        assert_eq!(
            (error.field(), error.code()),
            ("max_transport_connections", "capacity_too_large")
        );

        let mut invalid = defaults.clone();
        invalid.http1_max_buffer_bytes = Some(8191);
        let error = invalid.validate_detailed().unwrap_err();
        assert_eq!(
            (error.field(), error.code()),
            ("http1_max_buffer_bytes", "below_minimum")
        );

        let mut invalid = defaults.clone();
        invalid.http2_max_header_list_bytes = Some(0);
        let error = invalid.validate_detailed().unwrap_err();
        assert_eq!((error.field(), error.code()), ("http2_max_header_list_bytes", "zero"));
    }

    #[test]
    fn test_detailed_validation_accepts_defaults_and_boundaries() {
        let address = "127.0.0.1:8080".parse().unwrap();
        ServerOptions::new(address).validate_detailed().unwrap();
        ServerOptions::new(address)
            .with_shutdown_timeout(Duration::from_nanos(1))
            .with_request_header_timeout(Duration::from_nanos(1))
            .with_transport_idle_timeout(Duration::from_nanos(1))
            .with_max_transport_connections(tokio::sync::Semaphore::MAX_PERMITS)
            .unwrap()
            .with_http1_max_buffer_bytes(8192)
            .unwrap()
            .with_http2_max_header_list_bytes(1)
            .unwrap()
            .validate_detailed()
            .unwrap();
    }
}
