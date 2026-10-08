// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Finite HTTP request limits for explicitly protected router branches.

use std::future::Future;
use std::num::NonZeroUsize;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::time::Duration;

use axum::body::Body;
use axum::extract::State;
use axum::http::Request;
use axum::http::StatusCode;
use axum::http::header::CONTENT_LENGTH;
use axum::middleware;
use axum::middleware::Next;
use axum::response::IntoResponse;
use axum::response::Response;
use hyper::body::Body as HttpBody;
use tokio::sync::OwnedSemaphorePermit;
use tokio::sync::Semaphore;

mod internal;
/// Boxed future produced by the request-limit middleware.
#[doc(hidden)]
pub type LimitFuture = Pin<Box<dyn Future<Output = Response> + Send>>;
/// Axum middleware function signature for shared request-limit state.
#[doc(hidden)]
pub type LimitMiddleware = fn(State<LimitState>, Request<Body>, Next) -> LimitFuture;

/// Finite limits applied to ordinary, short HTTP requests.
///
/// # Examples
///
/// ```
/// use qubit_web::HttpLimits;
/// use std::time::Duration;
///
/// let limits = HttpLimits::default()
///     .with_max_concurrent_requests(8).unwrap()
///     .with_request_timeout(Duration::from_secs(2)).unwrap();
/// assert_eq!(limits.max_concurrent_requests(), 8);
/// ```
#[must_use]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct HttpLimits {
    /// Maximum request body size admitted by the middleware.
    max_body_bytes: NonZeroUsize,
    /// Maximum simultaneous short requests protected by one branch.
    max_concurrent_requests: NonZeroUsize,
    /// Deadline for handler work and response body streaming.
    request_timeout: Duration,
}

impl Default for HttpLimits {
    /// Creates finite defaults for body size, concurrency, and request time.
    ///
    /// # Returns
    ///
    /// Limits of 1 MiB per body, 256 in-flight requests, and 30 seconds.
    fn default() -> Self {
        Self {
            max_body_bytes: NonZeroUsize::new(1024 * 1024).expect("positive constant"),
            max_concurrent_requests: NonZeroUsize::new(256).expect("positive constant"),
            request_timeout: Duration::from_secs(30),
        }
    }
}

impl HttpLimits {
    /// Returns the maximum accepted request body size.
    ///
    /// # Returns
    ///
    /// The body budget in bytes.
    #[must_use]
    #[inline]
    pub const fn max_body_bytes(self) -> usize {
        self.max_body_bytes.get()
    }

    /// Returns the maximum number of in-flight short requests.
    ///
    /// # Returns
    ///
    /// The per-branch concurrent request capacity.
    #[must_use]
    #[inline]
    pub const fn max_concurrent_requests(self) -> usize {
        self.max_concurrent_requests.get()
    }

    /// Returns the ordinary request processing deadline.
    ///
    /// # Returns
    ///
    /// The configured request and response-stream duration.
    #[must_use]
    #[inline]
    pub const fn request_timeout(self) -> Duration {
        self.request_timeout
    }

    /// Sets the maximum number of request body bytes.
    ///
    /// # Parameters
    ///
    /// - `bytes`: positive byte budget for each request body.
    ///
    /// # Returns
    ///
    /// Updated HTTP limits.
    ///
    /// # Errors
    ///
    /// Returns `InvalidConfig` when the budget is zero.
    pub fn with_max_body_bytes(mut self, bytes: usize) -> Result<Self, crate::WebServerError> {
        self.max_body_bytes = NonZeroUsize::new(bytes).ok_or(crate::WebServerError::InvalidConfig)?;
        Ok(self)
    }

    /// Sets the maximum number of concurrent short requests.
    ///
    /// # Parameters
    ///
    /// - `requests`: positive count of in-flight requests.
    ///
    /// # Returns
    ///
    /// Updated HTTP limits.
    ///
    /// # Errors
    ///
    /// Returns `InvalidConfig` when the capacity is zero.
    pub fn with_max_concurrent_requests(mut self, requests: usize) -> Result<Self, crate::WebServerError> {
        self.max_concurrent_requests = NonZeroUsize::new(requests).ok_or(crate::WebServerError::InvalidConfig)?;
        Ok(self)
    }

    /// Sets the maximum processing time for an ordinary short request.
    ///
    /// # Parameters
    ///
    /// - `timeout`: positive duration covering handler and response streaming.
    ///
    /// # Returns
    ///
    /// Updated HTTP limits.
    ///
    /// # Errors
    ///
    /// Returns `InvalidConfig` for a zero duration.
    pub fn with_request_timeout(mut self, timeout: Duration) -> Result<Self, crate::WebServerError> {
        if timeout.is_zero() {
            return Err(crate::WebServerError::InvalidConfig);
        }
        self.request_timeout = timeout;
        Ok(self)
    }
}

/// Shared body, deadline, and capacity state for one selected route branch.
#[derive(Clone)]
#[doc(hidden)]
#[must_use]
pub struct LimitState {
    /// Body size threshold applied while a handler consumes request data.
    max_body_bytes: usize,
    /// Deadline applied to processing and response streaming.
    request_timeout: Duration,
    /// Shared capacity semaphore for concurrent short requests.
    permits: Arc<Semaphore>,
}

impl LimitState {
    /// Creates the shared state used by selected short-request branches.
    ///
    /// # Parameters
    ///
    /// - `limits`: validated budgets copied into the branch state.
    ///
    /// # Returns
    ///
    /// State that shares one capacity semaphore across the branch's requests.
    #[doc(hidden)]
    pub fn new(limits: HttpLimits) -> Self {
        Self {
            max_body_bytes: limits.max_body_bytes(),
            request_timeout: limits.request_timeout(),
            permits: Arc::new(Semaphore::new(limits.max_concurrent_requests())),
        }
    }
}

/// Creates the request limit middleware for a selected Axum router branch.
///
/// # Examples
///
/// ```
/// use qubit_web::{HttpLimits, RequestLimitLayer};
///
/// let _layer = RequestLimitLayer::new::<()>(HttpLimits::default());
/// ```
#[must_use]
pub struct RequestLimitLayer;

impl RequestLimitLayer {
    /// Creates a middleware with a body budget, short-request capacity, and
    /// deadline.
    ///
    /// The middleware counts request data frames as the selected route consumes
    /// them and stops polling the body after the first over-limit frame. An
    /// extractor error caused by that frame becomes a stable 413 rejection.
    /// Unconsumed request bodies are not read solely to enforce the limit.
    /// The deadline covers handler work and response-body streaming; if it
    /// expires after response headers are sent, the body ends with a timeout
    /// error. Long-lived SSE and WebSocket routes must not use this layer.
    ///
    /// # Type Parameters
    ///
    /// - `S`: Axum router state type for the selected branch.
    ///
    /// # Parameters
    ///
    /// - `limits`: validated body, concurrency, and timeout budgets.
    ///
    /// # Returns
    ///
    /// A middleware layer carrying a fresh shared state for this branch.
    #[allow(clippy::new_ret_no_self)]
    pub fn new<S>(limits: HttpLimits) -> middleware::FromFnLayer<LimitMiddleware, LimitState, S> {
        Self::from_state(LimitState::new(limits))
    }

    /// Creates a middleware using a previously shared branch state.
    ///
    /// # Type Parameters
    ///
    /// - `S`: Axum router state type for the selected branch.
    ///
    /// # Parameters
    ///
    /// - `state`: shared limits and semaphore installed on the branch.
    ///
    /// # Returns
    ///
    /// A middleware layer that reuses the supplied branch state.
    #[doc(hidden)]
    pub fn from_state<S>(state: LimitState) -> middleware::FromFnLayer<LimitMiddleware, LimitState, S> {
        middleware::from_fn_with_state::<_, LimitState, S>(state, enforce_limits as LimitMiddleware)
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
        let exceeded = Arc::new(AtomicBool::new(false));
        let request = limit_request_body(request, state.max_body_bytes, exceeded.clone());
        let deadline = state.request_timeout;
        let deadline_at = tokio::time::Instant::now() + deadline;
        let processing = run_limited_request(next, request, permit, exceeded, deadline_at);
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
) -> Result<Response, WebRejection> {
    let response = next.run(request).await;
    if exceeded.load(Ordering::Acquire) {
        return Err(WebRejection::BodyTooLarge);
    }
    Ok(response.map(|body| {
        let permit = if body.is_end_stream() { None } else { Some(permit) };
        internal::permit_body(body, permit, deadline)
    }))
}

/// A stable, request-independent rejection generated by the infrastructure.
///
/// # Examples
///
/// ```
/// use qubit_web::WebRejection;
///
/// let rejection = WebRejection::BodyTooLarge;
/// assert_eq!(rejection.status(), axum::http::StatusCode::PAYLOAD_TOO_LARGE);
/// assert_eq!(rejection.code(), "body_too_large");
/// ```
#[must_use]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WebRejection {
    /// The declared or streamed request body exceeded its byte budget.
    BodyTooLarge,
    /// No short-request capacity was available.
    CapacityExceeded,
    /// An ordinary short request exceeded its processing deadline.
    RequestTimedOut,
}

impl WebRejection {
    /// Returns the stable HTTP status for this rejection.
    ///
    /// # Returns
    ///
    /// The status to use in the corresponding problem response.
    #[must_use]
    #[inline]
    pub const fn status(self) -> StatusCode {
        match self {
            Self::BodyTooLarge => StatusCode::PAYLOAD_TOO_LARGE,
            Self::CapacityExceeded => StatusCode::SERVICE_UNAVAILABLE,
            Self::RequestTimedOut => StatusCode::GATEWAY_TIMEOUT,
        }
    }

    /// Returns the stable machine-readable code.
    ///
    /// # Returns
    ///
    /// A stable code that does not include request data.
    #[must_use]
    #[inline]
    pub const fn code(self) -> &'static str {
        match self {
            Self::BodyTooLarge => "body_too_large",
            Self::CapacityExceeded => "capacity_exceeded",
            Self::RequestTimedOut => "request_timeout",
        }
    }
}

impl IntoResponse for WebRejection {
    /// Converts the rejection into its safe problem response.
    ///
    /// # Returns
    ///
    /// An HTTP response with the stable status and machine-readable code.
    fn into_response(self) -> Response {
        let title = match self {
            Self::BodyTooLarge => "Payload Too Large",
            Self::CapacityExceeded => "Service Unavailable",
            Self::RequestTimedOut => "Gateway Timeout",
        };
        let payload = format!(
            "{{\"type\":\"about:blank\",\"title\":\"{title}\",\"status\":{},\"code\":\"{}\"}}",
            self.status().as_u16(),
            self.code()
        );
        (
            self.status(),
            [(axum::http::header::CONTENT_TYPE, "application/problem+json")],
            payload,
        )
            .into_response()
    }
}
