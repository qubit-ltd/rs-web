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
use axum::body::to_bytes;
use axum::extract::State;
use axum::http::Request;
use axum::http::header::CONTENT_LENGTH;
use axum::middleware;
use axum::middleware::Next;
use axum::response::IntoResponse;
use axum::response::Response;
use tokio::sync::OwnedSemaphorePermit;

use super::stream_admission_state::StreamAdmissionState;
use super::web_rejection::WebRejection;

/// Boxed future for the stream admission middleware.
#[doc(hidden)]
pub type StreamAdmissionFuture = Pin<Box<dyn Future<Output = Response> + Send>>;

/// Axum middleware function signature for shared stream admission state.
#[doc(hidden)]
pub type StreamAdmissionMiddleware = fn(State<StreamAdmissionState>, Request<Body>, Next) -> StreamAdmissionFuture;

/// Creates middleware that buffers request bodies before invoking a long-lived
/// route handler. The deadline ends when response headers are produced.
#[must_use]
pub struct StreamAdmissionLayer;

impl StreamAdmissionLayer {
    /// Creates middleware with a fresh shared capacity domain.
    ///
    /// Each independent layer created this way has its own semaphore. Clone a
    /// [`StreamAdmissionState`] and use [`Self::from_state`] when multiple
    /// branches need to share a capacity budget.
    ///
    /// # Type Parameters
    ///
    /// - `S`: router state type used when applying the layer.
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
    #[allow(clippy::new_ret_no_self)]
    pub fn new<S>(
        limits: super::stream_admission_limits::StreamAdmissionLimits,
    ) -> middleware::FromFnLayer<
        StreamAdmissionMiddleware,
        StreamAdmissionState,
        (State<StreamAdmissionState>, Request<Body>),
    > {
        Self::from_state::<S>(StreamAdmissionState::new(limits))
    }

    /// Creates middleware from a state shared with other route branches.
    ///
    /// # Type Parameters
    ///
    /// - `S`: router state type used when applying the layer.
    ///
    /// # Parameters
    ///
    /// - `state`: shared limits and semaphore for this capacity domain.
    ///
    /// # Returns
    ///
    /// An Axum middleware layer reusing the supplied capacity domain.
    pub fn from_state<S>(
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

async fn buffer_then_run(
    request: Request<Body>,
    next: Next,
    _permit: OwnedSemaphorePermit,
    maximum: usize,
) -> Result<Response, WebRejection> {
    let (parts, body) = request.into_parts();
    let bytes = to_bytes(body, maximum).await.map_err(|error| {
        if std::error::Error::source(&error).is_some_and(|source| source.is::<http_body_util::LengthLimitError>()) {
            WebRejection::BodyTooLarge
        } else {
            WebRejection::BodyReadFailed
        }
    })?;
    Ok(next.run(Request::from_parts(parts, Body::from(bytes))).await)
}

fn body_exceeds_limit(request: &Request<Body>, maximum: usize) -> bool {
    request
        .headers()
        .get(CONTENT_LENGTH)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<usize>().ok())
        .is_some_and(|length| length > maximum)
}
