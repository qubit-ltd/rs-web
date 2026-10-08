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

#[derive(Clone, Copy)]
pub(crate) struct ConnectionId(pub(crate) u64);

#[doc(hidden)]
pub type DiagnosticFuture = Pin<Box<dyn Future<Output = Response> + Send>>;
#[doc(hidden)]
pub type DiagnosticMiddleware = fn(Request<axum::body::Body>, Next) -> DiagnosticFuture;

/// A safe diagnostic record containing only route-level request metadata.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RequestDiagnostic {
    method: Method,
    route_template: String,
    status: u16,
    elapsed: Duration,
    connection_id: String,
    bytes: Option<u64>,
}

impl RequestDiagnostic {
    /// Builds a diagnostic record, rejecting values that are not route
    /// templates.
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
#[derive(Clone, Copy, Debug, Default)]
pub struct DiagnosticLayer;

impl DiagnosticLayer {
    /// Creates a layer that emits only method, matched route template, status,
    /// elapsed time, connection ID, and declared request size.
    #[allow(clippy::new_ret_no_self)]
    pub fn new<S>() -> middleware::FromFnLayer<DiagnosticMiddleware, (), S> {
        middleware::from_fn::<_, S>(record_request as DiagnosticMiddleware)
    }
}

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
