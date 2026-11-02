// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Finite per-branch budgets for ordinary HTTP requests.

use std::num::NonZeroUsize;
use std::time::Duration;

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
    /// Deadline for handler work and response body streaming, including active
    /// disposal of the application body and capacity permit at expiry.
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
    /// This budget is enforced by
    /// [`RequestLimitLayer`](super::RequestLimitLayer) and is independent
    /// of Axum's `DefaultBodyLimit`, which native extractors
    /// such as `Bytes`, `String`, and `Json` may enforce separately. This
    /// setter does not install or disable Axum's limit. When a route should
    /// accept a body larger than Axum's default, configure
    /// `DefaultBodyLimit::max` for that route as well, or disable that
    /// extractor limit only when the request limit layer remains in place.
    /// Both limits must admit the body.
    ///
    /// The example checks Axum's default rejection, a matching 4 MiB Axum
    /// limit, and a 5 MiB request rejected by rs-web when Axum's limit is
    /// disabled:
    ///
    /// ```
    /// use axum::Router;
    /// use axum::body::{Body, Bytes};
    /// use axum::extract::DefaultBodyLimit;
    /// use axum::http::Request;
    /// use axum::routing::post;
    /// use qubit_web::{HttpLimits, RequestLimitLayer};
    /// use tower::ServiceExt;
    ///
    /// # #[tokio::main]
    /// # async fn main() {
    /// const BUDGET: usize = 4 * 1024 * 1024;
    /// let request = |size| Request::post("/")
    ///     .body(Body::from(vec![b'x'; size]))
    ///     .unwrap();
    /// let limits = || HttpLimits::default().with_max_body_bytes(BUDGET).unwrap();
    /// let route = || post(|body: Bytes| async move { body.len().to_string() });
    ///
    /// let default_limit = Router::new()
    ///     .route("/", route())
    ///     .layer(RequestLimitLayer::new(limits()));
    /// let rejected = default_limit
    ///     .oneshot(request(3 * 1024 * 1024))
    ///     .await
    ///     .unwrap();
    /// assert_eq!(rejected.status(), axum::http::StatusCode::PAYLOAD_TOO_LARGE);
    ///
    /// let explicit_limit = Router::new()
    ///     .route("/", route())
    ///     .layer(DefaultBodyLimit::max(BUDGET))
    ///     .layer(RequestLimitLayer::new(limits()));
    /// let accepted = explicit_limit
    ///     .oneshot(request(3 * 1024 * 1024))
    ///     .await
    ///     .unwrap();
    /// assert_eq!(accepted.status(), axum::http::StatusCode::OK);
    /// let body = axum::body::to_bytes(accepted.into_body(), 128).await.unwrap();
    /// assert_eq!(
    ///     std::str::from_utf8(&body).unwrap(),
    ///     (3 * 1024 * 1024).to_string(),
    /// );
    ///
    /// let disabled_limit = Router::new()
    ///     .route("/", route())
    ///     .layer(DefaultBodyLimit::disable())
    ///     .layer(RequestLimitLayer::new(limits()));
    /// let rejected = disabled_limit
    ///     .oneshot(request(5 * 1024 * 1024))
    ///     .await
    ///     .unwrap();
    /// assert_eq!(rejected.status(), axum::http::StatusCode::PAYLOAD_TOO_LARGE);
    /// let body = axum::body::to_bytes(rejected.into_body(), 1024).await.unwrap();
    /// assert!(std::str::from_utf8(&body).unwrap().contains("body_too_large"));
    /// # }
    /// ```
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
    /// - `requests`: in-flight request capacity in
    ///   `1..=tokio::sync::Semaphore::MAX_PERMITS`.
    ///
    /// # Returns
    ///
    /// Updated HTTP limits.
    ///
    /// # Errors
    ///
    /// Returns `InvalidConfig` when the capacity is zero or exceeds
    /// `tokio::sync::Semaphore::MAX_PERMITS`.
    pub fn with_max_concurrent_requests(mut self, requests: usize) -> Result<Self, crate::WebServerError> {
        if !(1..=tokio::sync::Semaphore::MAX_PERMITS).contains(&requests) {
            return Err(crate::WebServerError::InvalidConfig);
        }
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
    /// Updated HTTP limits. The deadline includes handler work and response
    /// streaming; after headers are sent, expiry releases the application body
    /// and permit even if the transport stops polling. A later body poll
    /// reports one timeout error, but the response status cannot be changed
    /// and the transport controls when it observes that error.
    ///
    /// # Errors
    ///
    /// Returns `InvalidConfig` for a zero duration or when the duration cannot
    /// be represented as an absolute deadline from the current monotonic time.
    pub fn with_request_timeout(mut self, timeout: Duration) -> Result<Self, crate::WebServerError> {
        if timeout.is_zero() || tokio::time::Instant::now().checked_add(timeout).is_none() {
            return Err(crate::WebServerError::InvalidConfig);
        }
        self.request_timeout = timeout;
        Ok(self)
    }
}
