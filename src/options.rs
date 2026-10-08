// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use std::net::SocketAddr;
use std::time::Duration;

use crate::WebServerError;
use crate::limit::HttpLimits;

/// Validated settings used before a server starts accepting connections.
#[derive(Clone, Debug)]
pub struct ServerOptions {
    pub(crate) addr: SocketAddr,
    pub(crate) shutdown_timeout: Duration,
    pub(crate) request_header_timeout: Duration,
    pub(crate) http_limits: HttpLimits,
}

impl ServerOptions {
    /// Creates server options for the explicit listening address.
    pub fn new(addr: SocketAddr) -> Self {
        Self {
            addr,
            shutdown_timeout: Duration::from_secs(30),
            request_header_timeout: Duration::from_secs(10),
            http_limits: HttpLimits::default(),
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

    /// Sets the shared finite limits available to short-request router
    /// branches.
    pub fn with_http_limits(mut self, limits: HttpLimits) -> Self {
        self.http_limits = limits;
        self
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

    /// Returns the configured short-request limit defaults.
    ///
    /// `WebServer` does not rewrite a caller-built `Router` to classify its
    /// routes. Pass this value to [`crate::ControllerRoutes::with_http_limits`]
    /// or [`crate::RequestLimitLayer::new`] for the branches that should use
    /// these defaults; explicitly classified SSE and WebSocket branches skip
    /// the short-request layer.
    pub const fn http_limits(&self) -> HttpLimits {
        self.http_limits
    }

    /// Checks that all configured limits are finite and positive.
    pub fn validate(&self) -> Result<(), WebServerError> {
        if self.shutdown_timeout.is_zero() || self.request_header_timeout.is_zero() {
            return Err(WebServerError::InvalidConfig);
        }
        Ok(())
    }
}
