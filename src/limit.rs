// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Finite HTTP request limits for explicitly protected router branches.

use std::future::Future;
use std::io;
use std::num::NonZeroUsize;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::time::Duration;

use axum::body::Body;
use axum::body::Bytes;
use axum::extract::State;
use axum::http::Request;
use axum::http::StatusCode;
use axum::http::header::CONTENT_LENGTH;
use axum::middleware;
use axum::middleware::Next;
use axum::response::IntoResponse;
use axum::response::Response;
use tokio::sync::OwnedSemaphorePermit;
use tokio::sync::Semaphore;

#[doc(hidden)]
pub type LimitFuture = Pin<Box<dyn Future<Output = Response> + Send>>;
#[doc(hidden)]
pub type LimitMiddleware = fn(State<LimitState>, Request<Body>, Next) -> LimitFuture;

/// Finite limits applied to ordinary, short HTTP requests.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct HttpLimits {
    max_body_bytes: NonZeroUsize,
    max_concurrent_requests: NonZeroUsize,
    request_timeout: Duration,
}

impl Default for HttpLimits {
    fn default() -> Self {
        Self {
            max_body_bytes: NonZeroUsize::new(1024 * 1024).expect("positive constant"),
            max_concurrent_requests: NonZeroUsize::new(256).expect("positive constant"),
            request_timeout: Duration::from_secs(30),
        }
    }
}

impl HttpLimits {
    /// Sets the maximum number of request body bytes.
    pub fn with_max_body_bytes(mut self, bytes: usize) -> Result<Self, crate::WebServerError> {
        self.max_body_bytes = NonZeroUsize::new(bytes).ok_or(crate::WebServerError::InvalidConfig)?;
        Ok(self)
    }

    /// Sets the maximum number of concurrent short requests.
    pub fn with_max_concurrent_requests(mut self, requests: usize) -> Result<Self, crate::WebServerError> {
        self.max_concurrent_requests = NonZeroUsize::new(requests).ok_or(crate::WebServerError::InvalidConfig)?;
        Ok(self)
    }

    /// Sets the maximum processing time for an ordinary short request.
    pub fn with_request_timeout(mut self, timeout: Duration) -> Result<Self, crate::WebServerError> {
        if timeout.is_zero() {
            return Err(crate::WebServerError::InvalidConfig);
        }
        self.request_timeout = timeout;
        Ok(self)
    }

    /// Returns the maximum accepted request body size.
    pub const fn max_body_bytes(self) -> usize {
        self.max_body_bytes.get()
    }

    /// Returns the maximum number of in-flight short requests.
    pub const fn max_concurrent_requests(self) -> usize {
        self.max_concurrent_requests.get()
    }

    /// Returns the ordinary request processing deadline.
    pub const fn request_timeout(self) -> Duration {
        self.request_timeout
    }
}

#[derive(Clone)]
#[doc(hidden)]
pub struct LimitState {
    max_body_bytes: usize,
    request_timeout: Duration,
    permits: Arc<Semaphore>,
}

impl LimitState {
    /// Creates the shared state used by selected short-request branches.
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
    #[allow(clippy::new_ret_no_self)]
    pub fn new<S>(limits: HttpLimits) -> middleware::FromFnLayer<LimitMiddleware, LimitState, S> {
        Self::from_state(LimitState::new(limits))
    }

    /// Creates a middleware using a previously shared branch state.
    #[doc(hidden)]
    pub fn from_state<S>(state: LimitState) -> middleware::FromFnLayer<LimitMiddleware, LimitState, S> {
        middleware::from_fn_with_state::<_, LimitState, S>(state, enforce_limits as LimitMiddleware)
    }
}

fn enforce_limits(State(state): State<LimitState>, request: Request<Body>, next: Next) -> LimitFuture {
    Box::pin(async move {
        if request
            .headers()
            .get(CONTENT_LENGTH)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.parse::<usize>().ok())
            .is_some_and(|length| length > state.max_body_bytes)
        {
            return WebRejection::BodyTooLarge.into_response();
        }
        let permit = match state.permits.clone().try_acquire_owned() {
            Ok(permit) => permit,
            Err(_) => return WebRejection::CapacityExceeded.into_response(),
        };
        let exceeded = Arc::new(AtomicBool::new(false));
        let request = request.map(|inner| {
            Body::new(LimitedRequestBody {
                inner,
                max_bytes: state.max_body_bytes,
                consumed_bytes: 0,
                exceeded: exceeded.clone(),
            })
        });
        let deadline = state.request_timeout;
        let deadline_at = tokio::time::Instant::now() + deadline;
        let processing = async move {
            let response = next.run(request).await;
            if exceeded.load(Ordering::Acquire) {
                return Err(WebRejection::BodyTooLarge);
            }
            Ok::<_, WebRejection>(response.map(|body| {
                Body::new(PermitBody {
                    inner: body,
                    permit: Some(permit),
                    deadline: Box::pin(tokio::time::sleep_until(deadline_at)),
                    timed_out: false,
                })
            }))
        };
        match tokio::time::timeout_at(deadline_at, processing).await {
            Ok(Ok(response)) => response,
            Ok(Err(rejection)) => rejection.into_response(),
            Err(_) => WebRejection::RequestTimedOut.into_response(),
        }
    })
}

struct LimitedRequestBody {
    inner: Body,
    max_bytes: usize,
    consumed_bytes: usize,
    exceeded: Arc<AtomicBool>,
}

impl hyper::body::Body for LimitedRequestBody {
    type Data = Bytes;
    type Error = std::io::Error;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        context: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Result<hyper::body::Frame<Self::Data>, Self::Error>>> {
        let this = self.as_mut().get_mut();
        if this.exceeded.load(Ordering::Acquire) {
            return std::task::Poll::Ready(None);
        }
        match Pin::new(&mut this.inner).poll_frame(context) {
            std::task::Poll::Ready(Some(Ok(frame))) => match frame.into_data() {
                Ok(data) => {
                    let next_size = this.consumed_bytes.saturating_add(data.len());
                    if next_size > this.max_bytes {
                        this.exceeded.store(true, Ordering::Release);
                        std::task::Poll::Ready(Some(Err(std::io::Error::new(
                            std::io::ErrorKind::InvalidData,
                            "request body exceeds configured limit",
                        ))))
                    } else {
                        this.consumed_bytes = next_size;
                        std::task::Poll::Ready(Some(Ok(hyper::body::Frame::data(data))))
                    }
                }
                Err(trailers) => std::task::Poll::Ready(Some(Ok(trailers))),
            },
            std::task::Poll::Ready(Some(Err(error))) => std::task::Poll::Ready(Some(Err(std::io::Error::other(error)))),
            std::task::Poll::Ready(None) => std::task::Poll::Ready(None),
            std::task::Poll::Pending => std::task::Poll::Pending,
        }
    }

    fn is_end_stream(&self) -> bool {
        self.exceeded.load(Ordering::Acquire) || self.inner.is_end_stream()
    }

    fn size_hint(&self) -> hyper::body::SizeHint {
        self.inner.size_hint()
    }
}

struct PermitBody {
    inner: Body,
    permit: Option<OwnedSemaphorePermit>,
    deadline: Pin<Box<tokio::time::Sleep>>,
    timed_out: bool,
}

impl hyper::body::Body for PermitBody {
    type Data = Bytes;
    type Error = axum::Error;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        context: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Result<hyper::body::Frame<Self::Data>, Self::Error>>> {
        let this = self.as_mut().get_mut();
        if this.timed_out {
            return std::task::Poll::Ready(None);
        }
        if this.deadline.as_mut().poll(context).is_ready() {
            this.timed_out = true;
            this.permit.take();
            return std::task::Poll::Ready(Some(Err(axum::Error::new(io::Error::new(
                io::ErrorKind::TimedOut,
                "short response exceeded its configured deadline",
            )))));
        }
        Pin::new(&mut this.inner).poll_frame(context)
    }

    fn is_end_stream(&self) -> bool {
        self.timed_out || self.inner.is_end_stream()
    }

    fn size_hint(&self) -> hyper::body::SizeHint {
        if self.timed_out {
            hyper::body::SizeHint::default()
        } else {
            self.inner.size_hint()
        }
    }
}

/// A stable, request-independent rejection generated by the infrastructure.
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
    pub const fn status(self) -> StatusCode {
        match self {
            Self::BodyTooLarge => StatusCode::PAYLOAD_TOO_LARGE,
            Self::CapacityExceeded => StatusCode::SERVICE_UNAVAILABLE,
            Self::RequestTimedOut => StatusCode::GATEWAY_TIMEOUT,
        }
    }

    /// Returns the stable machine-readable code.
    pub const fn code(self) -> &'static str {
        match self {
            Self::BodyTooLarge => "body_too_large",
            Self::CapacityExceeded => "capacity_exceeded",
            Self::RequestTimedOut => "request_timeout",
        }
    }
}

impl IntoResponse for WebRejection {
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
