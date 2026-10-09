// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use std::net::SocketAddr;
use std::time::Duration;

use qubit_config::Config;

use super::ConfigOptionsError;
use crate::ServerOptions;
use crate::WebServerError;
use crate::limit::HttpLimits;

/// Separates listener settings from policies that callers install on selected
/// routes.
///
/// `WebServer` consumes only [`ServerOptions`]. The HTTP limits remain inert
/// until they are explicitly passed to
/// [`crate::ControllerRoutes::with_http_limits`] or
/// [`crate::RequestLimitLayer::new`].
///
/// # Examples
///
/// ```
/// # #[cfg(feature = "config")]
/// # {
/// use qubit_config::Config;
/// use qubit_web::ConfiguredWeb;
///
/// let mut config = Config::new();
/// config.set("address", "127.0.0.1:8080").unwrap();
/// let configured = ConfiguredWeb::from_config(&config).unwrap();
/// let (server_options, http_limits) = configured.into_parts();
/// assert_eq!(server_options.max_transport_connections(), 1024);
/// assert_eq!(http_limits.max_body_bytes(), 1024 * 1024);
/// # }
/// ```
#[must_use]
#[derive(Clone, Debug)]
pub struct ConfiguredWeb {
    /// Listener address and shutdown settings used to start the server.
    server_options: ServerOptions,
    /// HTTP limits retained for explicit installation on short routes.
    http_limits: HttpLimits,
}

impl ConfiguredWeb {
    /// Parses listener settings and route limit defaults from explicit values.
    ///
    /// The required `address` value is a socket address. The optional
    /// `shutdown_timeout_ms` value defaults to 30 seconds,
    /// `transport.max_connections` defaults to 1024, and
    /// `transport.idle_timeout_ms` defaults to 30 seconds. HTTP limits are
    /// returned separately so they can be installed only on appropriate
    /// short routes. This function does not discover or load a global
    /// configuration source.
    ///
    /// # Parameters
    ///
    /// - `config`: explicit values containing the listener and optional policy
    ///   settings.
    ///
    /// # Returns
    ///
    /// The listener options and route limits as independent policies.
    ///
    /// # Errors
    ///
    /// Returns a field-specific error when a value is absent, has the wrong
    /// type, cannot be parsed, or fails validation. Rejected values are never
    /// included in the error text.
    pub fn from_config(config: &Config) -> Result<Self, ConfigOptionsError> {
        let address = config
            .get_strict::<String>("address")
            .map_err(|_| ConfigOptionsError::new("address"))?
            .parse::<SocketAddr>()
            .map_err(|_| ConfigOptionsError::new("address"))?;

        let shutdown_timeout_ms = config
            .get_optional::<u64>("shutdown_timeout_ms")
            .map_err(|_| ConfigOptionsError::new("shutdown_timeout_ms"))?
            .unwrap_or(30_000);
        let max_transport_connections = config
            .get_optional::<u64>("transport.max_connections")
            .map_err(|_| ConfigOptionsError::new("transport.max_connections"))?
            .unwrap_or(1024);
        let transport_idle_timeout_ms = config
            .get_optional::<u64>("transport.idle_timeout_ms")
            .map_err(|_| ConfigOptionsError::new("transport.idle_timeout_ms"))?
            .unwrap_or(30_000);
        if transport_idle_timeout_ms == 0 {
            return Err(ConfigOptionsError::new("transport.idle_timeout_ms"));
        }

        let max_body_bytes = config
            .get_optional::<u64>("http.max_body_bytes")
            .map_err(|_| ConfigOptionsError::new("http.max_body_bytes"))?;
        let max_concurrent_requests = config
            .get_optional::<u64>("http.max_concurrent_requests")
            .map_err(|_| ConfigOptionsError::new("http.max_concurrent_requests"))?;
        let request_timeout_ms = config
            .get_optional::<u64>("http.request_timeout_ms")
            .map_err(|_| ConfigOptionsError::new("http.request_timeout_ms"))?;

        let mut http_limits = HttpLimits::default();
        if let Some(bytes) = max_body_bytes {
            http_limits = http_limits
                .with_max_body_bytes(
                    usize::try_from(bytes).map_err(|_| ConfigOptionsError::new("http.max_body_bytes"))?,
                )
                .map_err(|_| ConfigOptionsError::new("http.max_body_bytes"))?;
        }
        if let Some(requests) = max_concurrent_requests {
            http_limits = http_limits
                .with_max_concurrent_requests(
                    usize::try_from(requests).map_err(|_| ConfigOptionsError::new("http.max_concurrent_requests"))?,
                )
                .map_err(|_| ConfigOptionsError::new("http.max_concurrent_requests"))?;
        }
        if let Some(timeout_ms) = request_timeout_ms {
            http_limits = http_limits
                .with_request_timeout(Duration::from_millis(timeout_ms))
                .map_err(|_| ConfigOptionsError::new("http.request_timeout_ms"))?;
        }

        let server_options = ServerOptions::new(address)
            .with_shutdown_timeout(Duration::from_millis(shutdown_timeout_ms))
            .with_transport_idle_timeout(Duration::from_millis(transport_idle_timeout_ms))
            .with_max_transport_connections(
                usize::try_from(max_transport_connections)
                    .map_err(|_| ConfigOptionsError::new("transport.max_connections"))?,
            )
            .map_err(|_| ConfigOptionsError::new("transport.max_connections"))?;
        server_options.validate().map_err(|error| match error {
            WebServerError::InvalidConfig => ConfigOptionsError::new("shutdown_timeout_ms"),
            WebServerError::BindFailed { .. }
            | WebServerError::LocalAddressFailed { .. }
            | WebServerError::ServeFailed { .. }
            | WebServerError::TlsInvalid => ConfigOptionsError::new("address"),
        })?;

        Ok(Self {
            server_options,
            http_limits,
        })
    }

    /// Borrows the listener and shutdown settings without exposing route
    /// policy.
    ///
    /// # Returns
    ///
    /// The listener settings used to bind and operate the server.
    #[inline]
    pub const fn server_options(&self) -> &ServerOptions {
        &self.server_options
    }

    /// Copies the route limit defaults for explicit installation on short
    /// routes.
    ///
    /// # Returns
    ///
    /// The HTTP limits value, independent from the listener settings.
    #[inline]
    pub const fn http_limits(&self) -> HttpLimits {
        self.http_limits
    }

    /// Consumes the configuration result and returns its independent policies.
    ///
    /// # Returns
    ///
    /// The server options and HTTP limits as a pair.
    #[inline]
    pub fn into_parts(self) -> (ServerOptions, HttpLimits) {
        (self.server_options, self.http_limits)
    }
}
