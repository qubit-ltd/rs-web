// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use std::future::Future;
use std::pin::Pin;
use std::time::Instant;

use axum::extract::MatchedPath;
use axum::http::Request;
use axum::http::header::CONTENT_LENGTH;
use axum::middleware;
use axum::middleware::Next;
use axum::response::Response;

use super::RequestDiagnostic;
use super::internal::ConnectionId;

/// Boxed, sendable future returned by diagnostic middleware.
#[doc(hidden)]
pub type DiagnosticFuture = Pin<Box<dyn Future<Output = Response> + Send>>;
/// Function signature accepted by Axum's diagnostic `from_fn` layer.
#[doc(hidden)]
pub type DiagnosticMiddleware = fn(Request<axum::body::Body>, Next) -> DiagnosticFuture;

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
