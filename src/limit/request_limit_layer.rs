// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Middleware enforcing request body, concurrency, and deadline budgets.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;

use axum::body::Body;
use axum::extract::State;
use axum::http::Request;
use axum::http::header::CONTENT_LENGTH;
use axum::middleware;
use axum::middleware::Next;
use axum::response::IntoResponse;
use axum::response::Response;
use hyper::body::Body as HttpBody;
use tokio::sync::OwnedSemaphorePermit;

use super::http_limits::HttpLimits;
use super::internal;
use super::limit_state::LimitState;
use super::web_rejection::WebRejection;
use crate::diagnostic::internal::BodyObservation;

/// Boxed future produced by the request-limit middleware.
#[doc(hidden)]
pub type LimitFuture = Pin<Box<dyn Future<Output = Response> + Send>>;
/// Axum middleware function signature for shared request-limit state.
#[doc(hidden)]
pub type LimitMiddleware = fn(State<LimitState>, Request<Body>, Next) -> LimitFuture;

/// Creates the request limit middleware for a selected Axum router branch.
/// Calling [`new`](Self::new) independently creates an independent capacity
/// domain. To share body, deadline, and concurrency budgets across branches,
/// create one [`LimitState`] and pass its clones to [`Self::from_state`].
///
/// The deadline uses cooperative Tokio cancellation. Synchronous work that
/// does not yield can delay timeout checks and cleanup, and may finish
/// successfully after the deadline. Cancelling a wait does not stop an
/// already running `spawn_blocking` task or application-spawned work.
/// # Examples
///
/// ```
/// use axum::Router;
/// use axum::routing::get;
/// use qubit_web::{HttpLimits, RequestLimitLayer};
/// use qubit_web::limit::LimitState;
///
/// async fn handler() -> &'static str { "ok" }
///
/// let app: Router<()> = Router::new()
///     .route("/", get(handler))
///     .layer(RequestLimitLayer::new(HttpLimits::default()));
/// # let _ = app;
///
/// // Cloned state makes both branches share the same capacity budget.
/// let shared = LimitState::new(HttpLimits::default());
/// let app: Router = Router::new()
///     .route("/a", get(|| async { "a" })
///         .layer(RequestLimitLayer::from_state(shared.clone())))
///     .route("/b", get(|| async { "b" })
///         .layer(RequestLimitLayer::from_state(shared)));
/// # let _ = app;
///
/// #[derive(Clone)]
/// struct AppState;
/// async fn stateful(_: axum::extract::State<AppState>) -> &'static str { "ok" }
///
/// let app: Router = Router::<AppState>::new()
///     .route("/stateful", get(stateful))
///     .with_state(AppState)
///     .layer(RequestLimitLayer::new(HttpLimits::default()));
/// # let _ = app;
/// ```
#[must_use]
pub struct RequestLimitLayer;

impl RequestLimitLayer {
    /// Creates a middleware with a body budget, short-request capacity, and
    /// deadline.
    ///
    /// Each call creates a fresh, independent capacity domain. Use
    /// [`from_state`](Self::from_state) with cloned [`LimitState`] values to
    /// share budgets across route branches.
    ///
    /// The middleware counts request data frames as the selected route consumes
    /// them and stops polling the body after the first over-limit frame. An
    /// extractor error caused by that frame becomes a stable 413 rejection.
    /// Unconsumed request bodies are not read solely to enforce the limit.
    /// The layer converts a streamed over-limit signal to 413 only when it is
    /// observed before the handler returns. If a handler returns an unread body
    /// for later consumption, the initial response status cannot be changed;
    /// that response body instead reports a read error when it reaches the
    /// over-limit frame. Applications that require a stable rejection status
    /// must read and validate the complete request body before returning the
    /// response.
    /// For a streamed body without an oversized declared `Content-Length`, a
    /// handler may perform side effects before consuming the frame that makes
    /// the body exceed its budget. A later 413 does not roll those effects
    /// back; applications needing validation before side effects must consume
    /// and validate the complete body first.
    /// The deadline covers handler work and response-body streaming. If it
    /// expires after response headers are sent, the application body and
    /// capacity permit are released even when the transport stops polling;
    /// the next body poll reports one timeout error. The layer cannot change
    /// the sent status or control when the transport observes that error.
    /// If the configured duration can no longer be represented as an absolute
    /// deadline at request time, the layer returns HTTP 500 with code
    /// `invalid_configuration` and releases the acquired capacity permit.
    /// Long-lived SSE and WebSocket routes must not use this layer.
    ///
    /// # Parameters
    ///
    /// - `limits`: validated body, concurrency, and timeout budgets.
    ///
    /// # Returns
    ///
    /// A middleware layer carrying a fresh shared state for this branch.
    #[allow(clippy::new_ret_no_self)]
    pub fn new(
        limits: HttpLimits,
    ) -> middleware::FromFnLayer<LimitMiddleware, LimitState, (State<LimitState>, Request<Body>)> {
        Self::from_state(LimitState::new(limits))
    }

    /// Creates a middleware using a previously shared branch state.
    ///
    /// Pass clones of the same [`LimitState`] to multiple branches to make
    /// those branches share body and deadline budgets and concurrent-request
    /// capacity. Independently created states remain independent.
    ///
    /// # Parameters
    ///
    /// - `state`: shared limits and semaphore installed on the branch.
    ///
    /// # Returns
    ///
    /// A middleware layer that reuses the supplied branch state.
    pub fn from_state(
        state: LimitState,
    ) -> middleware::FromFnLayer<LimitMiddleware, LimitState, (State<LimitState>, Request<Body>)> {
        middleware::from_fn_with_state::<_, LimitState, (State<LimitState>, Request<Body>)>(
            state,
            enforce_limits as LimitMiddleware,
        )
    }
}

/// Enforces size, capacity, and deadline policies around one request.
///
/// # Parameters
///
/// - `state`: shared limits and capacity for the selected route.
/// - `request`: incoming request whose body is wrapped for streamed accounting.
/// - `next`: Axum continuation that runs the handler.
///
/// # Returns
///
/// A future yielding the handler response or a stable infrastructure rejection.
fn enforce_limits(State(state): State<LimitState>, request: Request<Body>, next: Next) -> LimitFuture {
    Box::pin(async move {
        if body_exceeds_limit(&request, state.max_body_bytes) {
            return WebRejection::BodyTooLarge.into_response();
        }
        let permit = match state.permits.clone().try_acquire_owned() {
            Ok(permit) => permit,
            Err(_) => return WebRejection::CapacityExceeded.into_response(),
        };
        let observation = request.extensions().get::<Arc<BodyObservation>>().cloned();
        let exceeded = Arc::new(AtomicBool::new(false));
        let request = limit_request_body(request, state.max_body_bytes, exceeded.clone());
        let deadline = state.request_timeout;
        let Some(deadline_at) = tokio::time::Instant::now().checked_add(deadline) else {
            return WebRejection::InvalidConfiguration.into_response();
        };
        let processing = run_limited_request(next, request, permit, exceeded, deadline_at, observation);
        match tokio::time::timeout_at(deadline_at, processing).await {
            Ok(Ok(response)) => response,
            Ok(Err(rejection)) => rejection.into_response(),
            Err(_) => WebRejection::RequestTimedOut.into_response(),
        }
    })
}

/// Detects requests whose declared body size already exceeds the branch budget.
///
/// # Parameters
///
/// - `request`: request headers to inspect without consuming the body.
/// - `maximum`: configured byte limit for the body.
///
/// # Returns
///
/// `true` when a valid declared length is larger than the configured maximum.
#[must_use]
fn body_exceeds_limit(request: &Request<Body>, maximum: usize) -> bool {
    request
        .headers()
        .get(CONTENT_LENGTH)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<usize>().ok())
        .is_some_and(|length| length > maximum)
}

/// Wraps a request body so consumed data frames are counted against its limit.
///
/// # Parameters
///
/// - `request`: request whose body will be consumed by the route extractor.
/// - `maximum`: maximum cumulative data bytes passed to the extractor.
/// - `exceeded`: shared signal read after the handler returns.
///
/// # Returns
///
/// The same request with a body wrapper that reports an over-limit frame.
fn limit_request_body(request: Request<Body>, maximum: usize, exceeded: Arc<AtomicBool>) -> Request<Body> {
    request.map(|inner| internal::limit_request_body(inner, maximum, exceeded))
}

/// Runs the handler, checks streamed body accounting, and retains capacity for
/// the response.
///
/// # Parameters
///
/// - `next`: continuation that runs the selected route handler.
/// - `request`: request carrying the limited body wrapper.
/// - `permit`: capacity lease held through response streaming.
/// - `exceeded`: shared body-budget result set by the wrapper.
/// - `deadline`: absolute deadline for both handler and response body.
///
/// # Returns
///
/// The wrapped response, or `BodyTooLarge` when an extractor consumed an
/// over-limit frame.
async fn run_limited_request(
    next: Next,
    request: Request<Body>,
    permit: OwnedSemaphorePermit,
    exceeded: Arc<AtomicBool>,
    deadline: tokio::time::Instant,
    observation: Option<Arc<BodyObservation>>,
) -> Result<Response, WebRejection> {
    let response = next.run(request).await;
    if exceeded.load(Ordering::Acquire) {
        return Err(WebRejection::BodyTooLarge);
    }
    Ok(response.map(|body| {
        let permit = if body.is_end_stream() { None } else { Some(permit) };
        internal::permit_body(body, permit, deadline, observation)
    }))
}
