// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Allowlisted HTTP diagnostics that never include request content or
//! credentials.

use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::time::Duration;
use std::time::Instant;

use axum::extract::MatchedPath;
use axum::http::Method;
use axum::http::Request;
use axum::http::header::CONTENT_LENGTH;
use axum::middleware;
use axum::middleware::Next;
use axum::response::Response;

/// Opaque connection identifier attached to a request while serving it.
#[derive(Clone, Copy)]
pub(crate) struct ConnectionId(
    /// Numeric value propagated through request extensions.
    pub(crate) u64,
);

/// Boxed, sendable future returned by diagnostic middleware.
#[doc(hidden)]
pub type DiagnosticFuture = Pin<Box<dyn Future<Output = Response> + Send>>;
/// Function signature accepted by Axum's diagnostic `from_fn` layer.
#[doc(hidden)]
pub type DiagnosticMiddleware = fn(Request<axum::body::Body>, Next) -> DiagnosticFuture;

/// A safe diagnostic record containing only route-level request metadata.
///
/// # Examples
///
/// ```
/// use axum::http::Method;
/// use qubit_web::diagnostic::RequestDiagnostic;
/// use std::time::Duration;
///
/// let diagnostic = RequestDiagnostic::new(
///     Method::GET,
///     "/health",
///     200,
///     Duration::from_millis(3),
///     "connection-7",
///     Some(12),
/// ).unwrap();
/// assert!(diagnostic.to_string().contains("route=/health"));
/// ```
#[must_use]
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RequestDiagnostic {
    /// HTTP method observed on the request.
    method: Method,
    /// Matched route template, without query or fragment data.
    route_template: String,
    /// Response status code returned by the handler.
    status: u16,
    /// Elapsed handler time measured by the middleware.
    elapsed: Duration,
    /// Safe identifier used to correlate this request with a connection.
    connection_id: String,
    /// Declared request content length, when it was a valid integer.
    bytes: Option<u64>,
}

impl RequestDiagnostic {
    /// Builds a record after validating the route, connection ID, and status.
    ///
    /// Request content and credentials are not accepted as inputs, preventing
    /// them from being retained in diagnostic output.
    ///
    /// # Parameters
    ///
    /// - `method`: HTTP method observed on the request.
    /// - `route_template`: normalized route template, without query/fragment.
    /// - `status`: HTTP response status in the range 100 through 599.
    /// - `elapsed`: time spent handling the request.
    /// - `connection_id`: correlation identifier without control characters.
    /// - `bytes`: optional declared request content length.
    ///
    /// # Returns
    ///
    /// A safe diagnostic record containing only the supplied route metadata.
    ///
    /// # Errors
    ///
    /// Returns a static reason if the route is not normalized, the connection
    /// ID contains control characters, or the status is outside the HTTP range.
    pub fn new(
        method: Method,
        route_template: impl Into<String>,
        status: u16,
        elapsed: Duration,
        connection_id: impl Into<String>,
        bytes: Option<u64>,
    ) -> Result<Self, &'static str> {
        let route_template = route_template.into();
        if !route_template.starts_with('/')
            || route_template.contains('?')
            || route_template.contains('#')
            || route_template.contains("://")
            || route_template.contains("..")
            || route_template.chars().any(char::is_control)
        {
            return Err("route must be a normalized template");
        }
        let connection_id = connection_id.into();
        if connection_id.chars().any(char::is_control) {
            return Err("connection ID contains control characters");
        }
        if !(100..=599).contains(&status) {
            return Err("invalid HTTP status");
        }
        Ok(Self {
            method,
            route_template,
            status,
            elapsed,
            connection_id,
            bytes,
        })
    }
}

impl fmt::Display for RequestDiagnostic {
    /// Formats the allowlisted metadata as a single log-safe line.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "method={} route={} status={} elapsed_ms={} connection_id={}",
            self.method,
            self.route_template,
            self.status,
            self.elapsed.as_millis(),
            self.connection_id
        )?;
        if let Some(bytes) = self.bytes {
            write!(f, " bytes={bytes}")?;
        }
        Ok(())
    }
}

/// Creates default tracing diagnostics for an explicitly selected router
/// branch.
///
/// # Examples
///
/// ```
/// use qubit_web::diagnostic::DiagnosticLayer;
///
/// let _layer = DiagnosticLayer::new::<()>();
/// ```
#[must_use]
#[derive(Clone, Copy, Debug, Default)]
pub struct DiagnosticLayer;

impl DiagnosticLayer {
    /// Creates a layer that emits only method, matched route template, status,
    /// elapsed time, connection ID, and declared request size.
    ///
    /// # Type Parameters
    ///
    /// - `S`: router state type required by Axum's middleware layer.
    ///
    /// # Returns
    ///
    /// An Axum layer that records safe request metadata after the handler runs.
    #[allow(clippy::new_ret_no_self)]
    pub fn new<S>() -> middleware::FromFnLayer<DiagnosticMiddleware, (), S> {
        middleware::from_fn::<_, S>(record_request as DiagnosticMiddleware)
    }
}

/// Records one request's allowlisted metadata after calling the next handler.
///
/// # Parameters
///
/// - `request`: incoming request whose route, method, ID, and content length
///   are inspected.
/// - `next`: Axum continuation that produces the response.
///
/// # Returns
///
/// A future that runs the continuation, emits a safe diagnostic when valid,
/// and yields the original response.
fn record_request(request: Request<axum::body::Body>, next: Next) -> DiagnosticFuture {
    Box::pin(async move {
        let method = request.method().clone();
        let route = request
            .extensions()
            .get::<MatchedPath>()
            .map(|matched| matched.as_str().to_owned())
            .unwrap_or_else(|| "<unmatched>".to_owned());
        let connection_id = request
            .extensions()
            .get::<ConnectionId>()
            .map(|id| id.0.to_string())
            .unwrap_or_else(|| "unavailable".to_owned());
        let bytes = request
            .headers()
            .get(CONTENT_LENGTH)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.parse::<u64>().ok());
        let started = Instant::now();
        let response = next.run(request).await;
        if let Ok(diagnostic) = RequestDiagnostic::new(
            method,
            route,
            response.status().as_u16(),
            started.elapsed(),
            connection_id,
            bytes,
        ) {
            tracing::info!(diagnostic = %diagnostic);
        }
        response
    })
}
