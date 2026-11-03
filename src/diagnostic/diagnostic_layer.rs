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
use hyper::body::Body as HttpBody;

use super::RequestDiagnostic;
use super::internal::BodyObservation;
use super::internal::ConnectionId;
use super::internal::DiagnosticBody;

/// Boxed, sendable future returned by diagnostic middleware.
#[doc(hidden)]
pub type DiagnosticFuture = Pin<Box<dyn Future<Output = Response> + Send>>;
/// Function signature accepted by Axum's diagnostic `from_fn` layer.
#[doc(hidden)]
pub type DiagnosticMiddleware = fn(Request<axum::body::Body>, Next) -> DiagnosticFuture;

/// Creates default tracing diagnostics for an Axum router branch.
///
/// # Examples
///
/// ```
/// use axum::body::Body;
/// use axum::http::Request;
/// use axum::routing::get;
/// use axum::Router;
/// use qubit_web::diagnostic::DiagnosticLayer;
/// use tower::ServiceExt;
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let app: Router = Router::new()
///     .route("/", get(|| async { "ok" }))
///     .layer(DiagnosticLayer::new());
/// let response = app
///     .oneshot(Request::get("/").body(Body::empty())?)
///     .await?;
/// assert_eq!(response.status(), axum::http::StatusCode::OK);
/// # Ok(())
/// # }
/// ```
#[must_use]
#[derive(Clone, Copy, Debug, Default)]
pub struct DiagnosticLayer;

impl DiagnosticLayer {
    /// Creates a layer that emits only method, matched route template, status,
    /// elapsed time, connection ID, and declared request size.
    ///
    /// # Returns
    ///
    /// An Axum layer that records safe request metadata after the handler runs.
    #[allow(clippy::new_ret_no_self)]
    pub fn new() -> middleware::FromFnLayer<DiagnosticMiddleware, (), (Request<axum::body::Body>,)> {
        middleware::from_fn(record_request as DiagnosticMiddleware)
    }

    /// Creates a layer that also records response-body termination.
    ///
    /// The layer records `complete`, `timeout`, `body_error`, or `dropped`
    /// once the response body terminates. `dropped` does not establish that a
    /// client disconnected, and `complete` does not establish network receipt.
    /// Install this layer outside [`crate::RequestLimitLayer`] to observe its
    /// independent response deadline watcher. Avoid installing it around a
    /// controller that already logs the same response if duplicate header logs
    /// are undesirable.
    ///
    /// # Returns
    ///
    /// An Axum layer with the same router compatibility as [`Self::new`].
    pub fn with_body_diagnostics() -> middleware::FromFnLayer<DiagnosticMiddleware, (), (Request<axum::body::Body>,)> {
        middleware::from_fn(record_request_body as DiagnosticMiddleware)
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

/// Records allowlisted request data and wraps the response body observer.
fn record_request_body(request: Request<axum::body::Body>, next: Next) -> DiagnosticFuture {
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
        let observation = BodyObservation::shared(method.clone(), route.clone(), connection_id.clone(), bytes, started);
        let mut request = request;
        request.extensions_mut().insert(observation.clone());
        let mut response = next.run(request).await;
        let status = response.status().as_u16();
        if let Ok(diagnostic) = RequestDiagnostic::new(method, route, status, started.elapsed(), connection_id, bytes) {
            tracing::info!(diagnostic = %diagnostic);
        }
        observation.set_status(status);
        if response.body().is_end_stream() {
            observation.terminate("complete");
        } else {
            let body = std::mem::replace(response.body_mut(), axum::body::Body::empty());
            *response.body_mut() = axum::body::Body::new(DiagnosticBody::new(body, observation));
        }
        response
    })
}
