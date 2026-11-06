// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Axum middleware that admits complete, bounded long-lived requests.

use std::future::Future;
use std::pin::Pin;

use axum::body::Body;
use axum::extract::State;
use axum::http::Request;
use axum::http::header::CONTENT_LENGTH;
use axum::middleware;
use axum::middleware::Next;
use axum::response::IntoResponse;
use axum::response::Response;
use http_body_util::BodyExt;
use http_body_util::Limited;
use tokio::sync::OwnedSemaphorePermit;

use super::internal::BufferedRequestBody;
use super::stream_admission_state::StreamAdmissionState;
use super::web_rejection::WebRejection;

/// Boxed future for the stream admission middleware.
#[doc(hidden)]
pub type StreamAdmissionFuture = Pin<Box<dyn Future<Output = Response> + Send>>;

/// Axum middleware function signature for shared stream admission state.
#[doc(hidden)]
pub type StreamAdmissionMiddleware = fn(State<StreamAdmissionState>, Request<Body>, Next) -> StreamAdmissionFuture;

/// Creates middleware that buffers request bodies before invoking a long-lived
/// route handler. The deadline ends when response headers are produced; it
/// does not cover streaming the response body or a WebSocket session.
/// Request trailers are preserved. Buffered data frames may be merged, and the
/// configured body limit counts data bytes; it does not impose a separate
/// trailers memory limit.
/// The deadline uses cooperative Tokio cancellation. Synchronous work that
/// does not yield can delay timeout checks and cleanup, and may finish
/// successfully after the deadline. Cancelling a wait does not stop an
/// already running `spawn_blocking` task or application-spawned work.
///
/// # Example
///
/// ```
/// use axum::Router;
/// use axum::routing::post;
/// use qubit_web::{StreamAdmissionLayer, StreamAdmissionLimits};
///
/// async fn handler() -> &'static str { "ok" }
///
/// let app: Router<()> = Router::new()
///     .route("/", post(handler))
///     .layer(StreamAdmissionLayer::new(StreamAdmissionLimits::default()));
/// # let _ = app;
///
/// #[derive(Clone)]
/// struct AppState;
/// async fn stateful(_: axum::extract::State<AppState>) -> &'static str { "ok" }
///
/// let app: Router = Router::<AppState>::new()
///     .route("/stateful", post(stateful))
///     .with_state(AppState)
///     .layer(StreamAdmissionLayer::new(StreamAdmissionLimits::default()));
/// # let _ = app;
/// ```
#[must_use]
pub struct StreamAdmissionLayer;

impl StreamAdmissionLayer {
    /// Creates middleware with a fresh shared capacity domain.
    ///
    /// Each independent layer created this way has its own semaphore. Clone a
    /// [`StreamAdmissionState`] and use [`Self::from_state`] when multiple
    /// branches need to share a capacity budget.
    ///
    /// # Parameters
    ///
    /// - `limits`: validated limits for buffered body size, concurrent
    ///   admissions, and admission deadline.
    ///
    /// # Returns
    ///
    /// An Axum middleware layer that buffers the complete request body before
    /// running the handler and holds a permit until response headers exist.
    /// With default limits, the body is at most 1 MiB and up to 256 admissions
    /// share the layer's state domain, for an estimated concurrent payload of
    /// 256 MiB. This is not a bound on buffer allocations or process peak
    /// memory. Incremental buffering releases each prior data-frame owner after
    /// copying it, while vector growth, temporary coexistence during growth,
    /// the current frame, trailers, allocator behavior, and transport buffers
    /// remain additional costs. Independent layers have independent budgets.
    #[allow(clippy::new_ret_no_self)]
    pub fn new(
        limits: super::stream_admission_limits::StreamAdmissionLimits,
    ) -> middleware::FromFnLayer<
        StreamAdmissionMiddleware,
        StreamAdmissionState,
        (State<StreamAdmissionState>, Request<Body>),
    > {
        Self::from_state(StreamAdmissionState::new(limits))
    }

    /// Creates middleware from a state shared with other route branches.
    ///
    /// # Parameters
    ///
    /// - `state`: shared limits and semaphore for this capacity domain.
    ///
    /// # Returns
    ///
    /// An Axum middleware layer reusing the supplied capacity domain.
    ///
    /// Oversized bodies return HTTP 413 (`body_too_large`), body read failures
    /// return HTTP 400 (`body_read_failed`), exhausted capacity returns HTTP
    /// 503 (`capacity_exceeded`), and an admission timeout returns HTTP 504
    /// (`request_timeout`). The timeout covers body buffering and the handler
    /// up to response headers only; the response body is not timed by this
    /// layer. If the absolute deadline cannot be represented at runtime, the
    /// layer returns HTTP 500 (`invalid_configuration`).
    pub fn from_state(
        state: StreamAdmissionState,
    ) -> middleware::FromFnLayer<
        StreamAdmissionMiddleware,
        StreamAdmissionState,
        (State<StreamAdmissionState>, Request<Body>),
    > {
        middleware::from_fn_with_state::<_, StreamAdmissionState, (State<StreamAdmissionState>, Request<Body>)>(
            state,
            admit_stream_request as StreamAdmissionMiddleware,
        )
    }
}

/// Rejects an oversized declared body before admission, then acquires a
/// non-waiting concurrency permit and applies one deadline from admission
/// through body buffering and handler completion up to response headers.
/// Capacity, size, configuration, read, and timeout failures map to stable
/// [`WebRejection`] responses.
/// # Parameters
///
/// * `State(state)` - Shared admission limits and capacity semaphore.
/// * `request` - Incoming request whose body and declared length are checked.
/// * `next` - Axum continuation that invokes the route handler.
fn admit_stream_request(
    State(state): State<StreamAdmissionState>,
    request: Request<Body>,
    next: Next,
) -> StreamAdmissionFuture {
    Box::pin(async move {
        if body_exceeds_limit(&request, state.max_body_bytes) {
            return WebRejection::BodyTooLarge.into_response();
        }
        let permit = match state.permits.clone().try_acquire_owned() {
            Ok(permit) => permit,
            Err(_) => return WebRejection::CapacityExceeded.into_response(),
        };
        let Some(deadline) = tokio::time::Instant::now().checked_add(state.admission_timeout) else {
            return WebRejection::InvalidConfiguration.into_response();
        };
        match tokio::time::timeout_at(deadline, buffer_then_run(request, next, permit, state.max_body_bytes)).await {
            Ok(Ok(response)) => response,
            Ok(Err(rejection)) => rejection.into_response(),
            Err(_) => WebRejection::RequestTimedOut.into_response(),
        }
    })
}

/// Buffers the complete request body before invoking the handler. The owned
/// permit remains held through the handler future and is released when that
/// future returns or is cancelled. The admission deadline ends when response
/// headers are produced; response body streaming is outside this layer's
/// deadline.
/// # Parameters
///
/// * `request` - Request whose body is buffered before the handler runs.
/// * `next` - Axum continuation that invokes the route handler.
/// * `_permit` - Capacity permit held while the handler runs.
/// * `maximum` - Maximum number of body data bytes to buffer.
async fn buffer_then_run(
    request: Request<Body>,
    next: Next,
    _permit: OwnedSemaphorePermit,
    maximum: usize,
) -> Result<Response, WebRejection> {
    let (parts, body) = request.into_parts();
    let mut body = Limited::new(body, maximum);
    let mut buffer = Vec::new();
    let mut trailers: Option<axum::http::HeaderMap> = None;
    while let Some(frame) = body.frame().await {
        let frame = frame.map_err(|error| {
            if error.is::<http_body_util::LengthLimitError>() {
                WebRejection::BodyTooLarge
            } else {
                WebRejection::BodyReadFailed
            }
        })?;
        match frame.into_data() {
            Ok(data) => buffer.extend_from_slice(&data),
            Err(frame) => {
                if let Ok(next_trailers) = frame.into_trailers() {
                    if let Some(current) = &mut trailers {
                        current.extend(next_trailers);
                    } else {
                        trailers = Some(next_trailers);
                    }
                }
            }
        }
    }
    let bytes = axum::body::Bytes::from(buffer);
    let replay = match trailers {
        Some(trailers) => Body::new(BufferedRequestBody::new(bytes, trailers)),
        None => Body::from(bytes),
    };
    Ok(next.run(Request::from_parts(parts, replay)).await)
}

/// Checks only a parseable `Content-Length` declaration without consuming the
/// body. This is a fast rejection path; `buffer_then_run` separately accounts
/// for streamed bodies whose length is absent or cannot be parsed.
/// # Parameters
///
/// * `request` - Request whose `Content-Length` header is inspected.
/// * `maximum` - Maximum allowed body size in bytes.
fn body_exceeds_limit(request: &Request<Body>, maximum: usize) -> bool {
    request
        .headers()
        .get(CONTENT_LENGTH)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<usize>().ok())
        .is_some_and(|length| length > maximum)
}
