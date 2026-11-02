// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Finite budgets for admitting long-lived HTTP requests.

use std::num::NonZeroUsize;
use std::time::Duration;

/// Finite body, concurrency, and admission-time limits for SSE and WebSocket
/// request entry points.
///
/// The defaults are 1 MiB per body, 256 simultaneous admissions, and 10
/// seconds. Limits apply per shared [`super::StreamAdmissionState`] domain.
/// Bodies are buffered before the handler starts; active response bodies and
/// WebSocket sessions are outside the admission deadline. The default
/// theoretical request-body buffer budget is therefore 256 MiB per independent
/// state domain, excluding request and runtime overhead; reduce either limit
/// to fit the application's memory budget.
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
/// let limits = StreamAdmissionLimits::default()
///     .with_max_concurrent_admissions(8)?;
/// let app: Router<()> = Router::new()
///     .route("/events", post(handler))
///     .layer(StreamAdmissionLayer::new(limits));
/// # let _ = app;
/// # Ok::<(), qubit_web::WebServerError>(())
/// ```
#[must_use]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct StreamAdmissionLimits {
    max_body_bytes: NonZeroUsize,
    max_concurrent_admissions: NonZeroUsize,
    admission_timeout: Duration,
}

impl Default for StreamAdmissionLimits {
    /// Creates finite defaults for long-lived request admission.
    fn default() -> Self {
        Self {
            max_body_bytes: NonZeroUsize::new(1024 * 1024).expect("positive constant"),
            max_concurrent_admissions: NonZeroUsize::new(256).expect("positive constant"),
            admission_timeout: Duration::from_secs(10),
        }
    }
}

impl StreamAdmissionLimits {
    /// Returns the maximum buffered request body size in bytes.
    #[must_use]
    pub const fn max_body_bytes(self) -> usize {
        self.max_body_bytes.get()
    }

    /// Returns the number of concurrent admissions in this state domain.
    #[must_use]
    pub const fn max_concurrent_admissions(self) -> usize {
        self.max_concurrent_admissions.get()
    }

    /// Returns the deadline for buffering the request and creating response
    /// headers.
    #[must_use]
    pub const fn admission_timeout(self) -> Duration {
        self.admission_timeout
    }

    /// Sets the maximum request body size.
    ///
    /// This budget is enforced while
    /// [`StreamAdmissionLayer`](super::StreamAdmissionLayer)
    /// buffers a request, independently of Axum's `DefaultBodyLimit` for native
    /// extractors such as `Bytes`, `String`, and `Json`. This setter does not
    /// install or disable Axum's limit. When a route should accept a body
    /// larger than Axum's default, configure `DefaultBodyLimit::max` for
    /// that route as well, or disable that extractor limit only when stream
    /// admission remains in place. Both limits must admit the body.
    ///
    /// The example checks Axum's default rejection, a matching 4 MiB Axum
    /// limit, and a 5 MiB request rejected by stream admission when Axum's
    /// limit is disabled:
    ///
    /// ```
    /// use axum::Router;
    /// use axum::body::{Body, Bytes};
    /// use axum::extract::DefaultBodyLimit;
    /// use axum::http::Request;
    /// use axum::routing::post;
    /// use qubit_web::{StreamAdmissionLayer, StreamAdmissionLimits};
    /// use tower::ServiceExt;
    ///
    /// # #[tokio::main]
    /// # async fn main() {
    /// const BUDGET: usize = 4 * 1024 * 1024;
    /// let request = |size| Request::post("/")
    ///     .body(Body::from(vec![b'x'; size]))
    ///     .unwrap();
    /// let limits = || {
    ///     StreamAdmissionLimits::default()
    ///         .with_max_body_bytes(BUDGET)
    ///         .unwrap()
    /// };
    /// let route = || post(|body: Bytes| async move { body.len().to_string() });
    ///
    /// let default_limit = Router::new()
    ///     .route("/", route())
    ///     .layer(StreamAdmissionLayer::new(limits()));
    /// let rejected = default_limit
    ///     .oneshot(request(3 * 1024 * 1024))
    ///     .await
    ///     .unwrap();
    /// assert_eq!(rejected.status(), axum::http::StatusCode::PAYLOAD_TOO_LARGE);
    ///
    /// let explicit_limit = Router::new()
    ///     .route("/", route())
    ///     .layer(DefaultBodyLimit::max(BUDGET))
    ///     .layer(StreamAdmissionLayer::new(limits()));
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
    ///     .layer(StreamAdmissionLayer::new(limits()));
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
    /// - `bytes`: positive per-request body budget.
    ///
    /// # Returns
    ///
    /// Updated admission limits.
    ///
    /// # Errors
    ///
    /// Returns `WebServerError::InvalidConfig` when `bytes` is zero.
    pub fn with_max_body_bytes(mut self, bytes: usize) -> Result<Self, crate::WebServerError> {
        self.max_body_bytes = NonZeroUsize::new(bytes).ok_or(crate::WebServerError::InvalidConfig)?;
        Ok(self)
    }

    /// Sets the maximum simultaneous admissions. The value must be within
    /// Tokio's semaphore permit range.
    ///
    /// # Parameters
    ///
    /// - `admissions`: positive capacity shared by this state domain.
    ///
    /// # Returns
    ///
    /// Updated admission limits.
    ///
    /// # Errors
    ///
    /// Returns `WebServerError::InvalidConfig` when the value is zero or
    /// exceeds `Semaphore::MAX_PERMITS`.
    pub fn with_max_concurrent_admissions(mut self, admissions: usize) -> Result<Self, crate::WebServerError> {
        if !(1..=tokio::sync::Semaphore::MAX_PERMITS).contains(&admissions) {
            return Err(crate::WebServerError::InvalidConfig);
        }
        self.max_concurrent_admissions = NonZeroUsize::new(admissions).ok_or(crate::WebServerError::InvalidConfig)?;
        Ok(self)
    }

    /// Sets the positive request admission deadline.
    ///
    /// # Parameters
    ///
    /// - `timeout`: maximum time for buffering the request and creating
    ///   response headers.
    ///
    /// # Returns
    ///
    /// Updated admission limits.
    ///
    /// # Errors
    ///
    /// Returns `WebServerError::InvalidConfig` when `timeout` is zero or
    /// cannot be represented as an absolute monotonic deadline.
    pub fn with_admission_timeout(mut self, timeout: Duration) -> Result<Self, crate::WebServerError> {
        if timeout.is_zero() || tokio::time::Instant::now().checked_add(timeout).is_none() {
            return Err(crate::WebServerError::InvalidConfig);
        }
        self.admission_timeout = timeout;
        Ok(self)
    }
}
