// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use std::convert::Infallible;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::task::Context;
use std::task::Poll;
use std::time::Duration;

use axum::Router;
use axum::body::Body;
use axum::body::Bytes;
use axum::extract::State;
use axum::http::HeaderMap;
use axum::http::Request;
use axum::http::StatusCode;
use axum::response::Response;
use axum::routing::post;
use futures_core::Stream;
use http_body_util::BodyExt;
use hyper::body::Body as HttpBody;
use qubit_web::StreamAdmissionLayer;
use qubit_web::StreamAdmissionLimits;
use qubit_web::WebRejection;
use tower::ServiceExt;

const DEFAULT_MAX_BODY_BYTES: usize = 1024 * 1024;

struct DataAndTrailersBody {
    state: u8,
    data: Bytes,
    fail_after_data: bool,
}

impl HttpBody for DataAndTrailersBody {
    type Data = Bytes;
    type Error = std::io::Error;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        _: &mut Context<'_>,
    ) -> Poll<Option<Result<hyper::body::Frame<Self::Data>, Self::Error>>> {
        match self.state {
            0 => {
                self.state = 1;
                Poll::Ready(Some(Ok(hyper::body::Frame::data(self.data.clone()))))
            }
            1 => {
                self.state = 2;
                if self.fail_after_data {
                    return Poll::Ready(Some(Err(std::io::Error::other("failed after data"))));
                }
                let mut trailers = HeaderMap::new();
                trailers.insert("x-check", "preserved".parse().unwrap());
                Poll::Ready(Some(Ok(hyper::body::Frame::trailers(trailers))))
            }
            _ => Poll::Ready(None),
        }
    }
}

#[tokio::test]
async fn admission_preserves_request_trailers() {
    async fn inspect(_: State<()>, request: Request<Body>) -> StatusCode {
        let collected = request.into_body().collect().await.unwrap();
        assert_eq!(collected.trailers().unwrap()["x-check"], "preserved");
        assert_eq!(collected.to_bytes(), Bytes::from_static(b"request"));
        StatusCode::OK
    }
    let router = Router::new()
        .route("/", post(inspect))
        .with_state(())
        .layer(StreamAdmissionLayer::new(StreamAdmissionLimits::default()));
    let response = router
        .oneshot(
            Request::post("/")
                .body(Body::new(DataAndTrailersBody {
                    state: 0,
                    data: Bytes::from_static(b"request"),
                    fail_after_data: false,
                }))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
}

#[tokio::test]
async fn trailer_body_limit_and_read_failure_keep_admission_rejections() {
    let calls = Arc::new(AtomicUsize::new(0));
    async fn inspect(State(calls): State<Arc<AtomicUsize>>, request: Request<Body>) -> StatusCode {
        calls.fetch_add(1, Ordering::SeqCst);
        let collected = request.into_body().collect().await.unwrap();
        assert_eq!(collected.trailers().unwrap()["x-check"], "preserved");
        assert_eq!(collected.to_bytes(), Bytes::from_static(b"request"));
        StatusCode::OK
    }
    let limits = StreamAdmissionLimits::default().with_max_body_bytes(7).unwrap();
    let router = Router::new()
        .route("/", post(inspect))
        .with_state(calls.clone())
        .layer(StreamAdmissionLayer::new(limits));
    let exact = router
        .clone()
        .oneshot(
            Request::post("/")
                .body(Body::new(DataAndTrailersBody {
                    state: 0,
                    data: Bytes::from_static(b"request"),
                    fail_after_data: false,
                }))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(exact.status(), StatusCode::OK);
    assert_eq!(calls.load(Ordering::SeqCst), 1);

    let over = router
        .clone()
        .oneshot(
            Request::post("/")
                .body(Body::new(DataAndTrailersBody {
                    state: 0,
                    data: Bytes::from_static(b"request!"),
                    fail_after_data: false,
                }))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(over.status(), StatusCode::PAYLOAD_TOO_LARGE);
    assert_eq!(calls.load(Ordering::SeqCst), 1);

    let failed = router
        .oneshot(
            Request::post("/")
                .body(Body::new(DataAndTrailersBody {
                    state: 0,
                    data: Bytes::from_static(b"request"),
                    fail_after_data: true,
                }))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(failed.status(), StatusCode::BAD_REQUEST);
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}

#[test]
fn defaults_and_invalid_values_are_stable() {
    let limits = StreamAdmissionLimits::default();
    assert_eq!(limits.max_body_bytes(), DEFAULT_MAX_BODY_BYTES);
    assert_eq!(limits.max_concurrent_admissions(), 256);
    assert_eq!(limits.admission_timeout(), Duration::from_secs(10));
    assert!(limits.with_max_body_bytes(0).is_err());
    assert!(limits.with_max_concurrent_admissions(0).is_err());
    assert!(
        limits
            .with_max_concurrent_admissions(tokio::sync::Semaphore::MAX_PERMITS + 1)
            .is_err()
    );
    assert!(limits.with_admission_timeout(Duration::ZERO).is_err());
    assert!(limits.with_admission_timeout(Duration::MAX).is_err());
}

async fn accepted_handler(State(calls): State<Arc<AtomicUsize>>, body: Bytes) -> &'static str {
    calls.fetch_add(1, Ordering::SeqCst);
    assert!(body.len() <= DEFAULT_MAX_BODY_BYTES);
    "accepted"
}

fn app(limits: StreamAdmissionLimits, calls: Arc<AtomicUsize>) -> Router {
    Router::new()
        .route("/", post(accepted_handler))
        .with_state(calls)
        .layer(StreamAdmissionLayer::new(limits))
}

async fn body(response: Response) -> String {
    let bytes = axum::body::to_bytes(response.into_body(), 4096).await.unwrap();
    String::from_utf8(bytes.to_vec()).unwrap()
}

#[tokio::test]
async fn declared_and_streamed_body_limits_are_checked_before_handler() {
    let calls = Arc::new(AtomicUsize::new(0));
    let router = app(StreamAdmissionLimits::default(), calls.clone());
    let declared = router
        .clone()
        .oneshot(
            Request::post("/")
                .header("content-length", DEFAULT_MAX_BODY_BYTES + 1)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(declared.status(), StatusCode::PAYLOAD_TOO_LARGE);

    let streamed = router
        .clone()
        .oneshot(
            Request::post("/")
                .body(Body::from(vec![b'x'; DEFAULT_MAX_BODY_BYTES + 1]))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(streamed.status(), StatusCode::PAYLOAD_TOO_LARGE);
    assert_eq!(calls.load(Ordering::SeqCst), 0);

    let exact = router
        .oneshot(
            Request::post("/")
                .body(Body::from(vec![b'x'; DEFAULT_MAX_BODY_BYTES]))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(exact.status(), StatusCode::OK);
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn over_limit_body_is_rejected_even_when_handler_would_not_read_it() {
    async fn ignore_body(State(calls): State<Arc<AtomicUsize>>) -> &'static str {
        calls.fetch_add(1, Ordering::SeqCst);
        "ignored"
    }
    let calls = Arc::new(AtomicUsize::new(0));
    let router = Router::new()
        .route("/", post(ignore_body))
        .with_state(calls.clone())
        .layer(StreamAdmissionLayer::new(StreamAdmissionLimits::default()));
    let response = router
        .oneshot(
            Request::post("/")
                .body(Body::from(vec![b'x'; DEFAULT_MAX_BODY_BYTES + 1]))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
    assert_eq!(calls.load(Ordering::SeqCst), 0);
}

struct FailedBody;

impl HttpBody for FailedBody {
    type Data = Bytes;
    type Error = std::io::Error;

    fn poll_frame(
        self: Pin<&mut Self>,
        _: &mut Context<'_>,
    ) -> Poll<Option<Result<hyper::body::Frame<Self::Data>, Self::Error>>> {
        Poll::Ready(Some(Err(std::io::Error::other("private body failure detail"))))
    }
}

struct PendingBody;

impl HttpBody for PendingBody {
    type Data = Bytes;
    type Error = Infallible;

    fn poll_frame(
        self: Pin<&mut Self>,
        _: &mut Context<'_>,
    ) -> Poll<Option<Result<hyper::body::Frame<Self::Data>, Self::Error>>> {
        Poll::Pending
    }
}

struct PendingStream;

impl Stream for PendingStream {
    type Item = Result<Bytes, Infallible>;

    fn poll_next(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Poll::Pending
    }
}

#[tokio::test]
async fn body_read_failure_returns_safe_400_problem() {
    let calls = Arc::new(AtomicUsize::new(0));
    let response = app(StreamAdmissionLimits::default(), calls.clone())
        .oneshot(Request::post("/").body(Body::new(FailedBody)).unwrap())
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let payload = body(response).await;
    assert_eq!(
        payload,
        "{\"type\":\"about:blank\",\"title\":\"Bad Request\",\"status\":400,\"code\":\"body_read_failed\"}"
    );
    assert!(!payload.contains("private body failure detail"));
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert_eq!(WebRejection::BodyReadFailed.code(), "body_read_failed");
}

#[tokio::test]
async fn pending_request_body_times_out_before_handler_runs() {
    let calls = Arc::new(AtomicUsize::new(0));
    let limits = StreamAdmissionLimits::default()
        .with_max_concurrent_admissions(1)
        .unwrap()
        .with_admission_timeout(Duration::from_millis(10))
        .unwrap();
    let router = app(limits, calls.clone());
    let response = router
        .clone()
        .oneshot(Request::post("/").body(Body::new(PendingBody)).unwrap())
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::GATEWAY_TIMEOUT);
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    let next = router
        .oneshot(Request::post("/").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(next.status(), StatusCode::OK);
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}

#[derive(Clone)]
struct Gate {
    entered: Arc<tokio::sync::Notify>,
    release: Arc<tokio::sync::Semaphore>,
    calls: Arc<AtomicUsize>,
}

async fn blocked(State(gate): State<Gate>) -> &'static str {
    gate.calls.fetch_add(1, Ordering::SeqCst);
    gate.entered.notify_one();
    let permit = gate.release.acquire().await.unwrap();
    permit.forget();
    "ok"
}

async fn blocked_with_pending_response(State(gate): State<Gate>) -> Response {
    gate.calls.fetch_add(1, Ordering::SeqCst);
    gate.entered.notify_one();
    let permit = gate.release.acquire().await.unwrap();
    permit.forget();
    Response::new(Body::from_stream(PendingStream))
}

#[tokio::test]
async fn admission_permit_is_released_after_response_headers() {
    let gate = Gate {
        entered: Arc::new(tokio::sync::Notify::new()),
        release: Arc::new(tokio::sync::Semaphore::new(0)),
        calls: Arc::new(AtomicUsize::new(0)),
    };
    let limits = StreamAdmissionLimits::default()
        .with_max_concurrent_admissions(1)
        .unwrap();
    let router = Router::new()
        .route("/", post(blocked_with_pending_response))
        .with_state(gate.clone())
        .layer(StreamAdmissionLayer::new(limits));
    let first = tokio::spawn(router.clone().oneshot(Request::post("/").body(Body::empty()).unwrap()));
    gate.entered.notified().await;
    let rejected = router
        .clone()
        .oneshot(Request::post("/").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(rejected.status(), StatusCode::SERVICE_UNAVAILABLE);
    gate.release.add_permits(1);
    let first_response = first.await.unwrap().unwrap();
    assert_eq!(first_response.status(), StatusCode::OK);
    assert!(!first_response.body().is_end_stream());
    gate.release.add_permits(1);
    let third = tokio::time::timeout(
        Duration::from_secs(1),
        router.oneshot(Request::post("/").body(Body::empty()).unwrap()),
    )
    .await
    .expect("third admission should complete while the first response body is pending")
    .unwrap();
    assert_eq!(third.status(), StatusCode::OK);
    drop(first_response);
    drop(third);
}

#[tokio::test]
async fn shared_state_limits_admissions_across_router_branches() {
    let gate = Gate {
        entered: Arc::new(tokio::sync::Notify::new()),
        release: Arc::new(tokio::sync::Semaphore::new(0)),
        calls: Arc::new(AtomicUsize::new(0)),
    };
    let limits = StreamAdmissionLimits::default()
        .with_max_concurrent_admissions(1)
        .unwrap();
    let state = qubit_web::StreamAdmissionState::new(limits);
    let router = Router::new()
        .route(
            "/first",
            post(blocked).layer(StreamAdmissionLayer::from_state(state.clone())),
        )
        .route("/second", post(blocked).layer(StreamAdmissionLayer::from_state(state)))
        .with_state(gate.clone());

    let first = tokio::spawn(
        router
            .clone()
            .oneshot(Request::post("/first").body(Body::empty()).unwrap()),
    );
    gate.entered.notified().await;
    let rejected = router
        .clone()
        .oneshot(Request::post("/second").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(rejected.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(gate.calls.load(Ordering::SeqCst), 1);

    gate.release.add_permits(1);
    assert_eq!(first.await.unwrap().unwrap().status(), StatusCode::OK);
}

const LARGE_BODY_BUDGET: usize = 4 * 1024 * 1024;

#[derive(Clone, Copy)]
enum AxumBodyPolicy {
    Default,
    Max(usize),
    Disabled,
}

async fn count_native_bytes(State(calls): State<Arc<AtomicUsize>>, body: Bytes) -> String {
    calls.fetch_add(1, Ordering::SeqCst);
    body.len().to_string()
}

async fn count_native_string(State(calls): State<Arc<AtomicUsize>>, body: String) -> String {
    calls.fetch_add(1, Ordering::SeqCst);
    body.len().to_string()
}

fn native_body_router(policy: AxumBodyPolicy, calls: Arc<AtomicUsize>) -> Router {
    let limits = StreamAdmissionLimits::default()
        .with_max_body_bytes(LARGE_BODY_BUDGET)
        .unwrap();
    let router = Router::new()
        .route("/bytes", post(count_native_bytes))
        .route("/string", post(count_native_string))
        .with_state(calls);
    let router = match policy {
        AxumBodyPolicy::Default => router,
        AxumBodyPolicy::Max(bytes) => router.layer(axum::extract::DefaultBodyLimit::max(bytes)),
        AxumBodyPolicy::Disabled => router.layer(axum::extract::DefaultBodyLimit::disable()),
    };
    router.layer(StreamAdmissionLayer::new(limits))
}

async fn assert_native_stream_limit_matrix(path: &str, request_body: fn(&str, usize) -> Request<Body>) {
    let default_calls = Arc::new(AtomicUsize::new(0));
    let default_response = native_body_router(AxumBodyPolicy::Default, default_calls.clone())
        .oneshot(request_body(path, 3 * 1024 * 1024))
        .await
        .unwrap();
    assert_eq!(default_response.status(), StatusCode::PAYLOAD_TOO_LARGE);
    assert_eq!(default_calls.load(Ordering::SeqCst), 0);

    let max_calls = Arc::new(AtomicUsize::new(0));
    let max_router = native_body_router(AxumBodyPolicy::Max(LARGE_BODY_BUDGET), max_calls.clone());
    for size in [3 * 1024 * 1024, LARGE_BODY_BUDGET] {
        let response = max_router.clone().oneshot(request_body(path, size)).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let payload = axum::body::to_bytes(response.into_body(), 128).await.unwrap();
        assert_eq!(std::str::from_utf8(&payload).unwrap(), size.to_string());
    }
    assert_eq!(max_calls.load(Ordering::SeqCst), 2);

    let disabled_calls = Arc::new(AtomicUsize::new(0));
    let disabled_response = native_body_router(AxumBodyPolicy::Disabled, disabled_calls.clone())
        .oneshot(request_body(path, 5 * 1024 * 1024))
        .await
        .unwrap();
    assert_eq!(disabled_response.status(), StatusCode::PAYLOAD_TOO_LARGE);
    assert_eq!(disabled_calls.load(Ordering::SeqCst), 0);
    let payload = axum::body::to_bytes(disabled_response.into_body(), 1024).await.unwrap();
    assert!(std::str::from_utf8(&payload).unwrap().contains("body_too_large"));
    assert_eq!(WebRejection::BodyTooLarge.code(), "body_too_large");
}

fn native_bytes_request(path: &str, size: usize) -> Request<Body> {
    Request::post(path).body(Body::from(vec![b'x'; size])).unwrap()
}

fn native_string_request(path: &str, size: usize) -> Request<Body> {
    Request::post(path).body(Body::from(vec![b'x'; size])).unwrap()
}

#[tokio::test]
async fn stream_admission_preserves_axum_and_native_bytes_limits() {
    assert_native_stream_limit_matrix("/bytes", native_bytes_request).await;
}

#[tokio::test]
async fn stream_admission_preserves_axum_and_native_string_limits() {
    assert_native_stream_limit_matrix("/string", native_string_request).await;
}

#[cfg(feature = "json")]
async fn count_native_json(State(calls): State<Arc<AtomicUsize>>, axum::Json(value): axum::Json<String>) -> String {
    calls.fetch_add(1, Ordering::SeqCst);
    value.len().to_string()
}

#[cfg(feature = "json")]
fn native_json_router(policy: AxumBodyPolicy, calls: Arc<AtomicUsize>) -> Router {
    let limits = StreamAdmissionLimits::default()
        .with_max_body_bytes(LARGE_BODY_BUDGET)
        .unwrap();
    let router = Router::new().route("/json", post(count_native_json)).with_state(calls);
    let router = match policy {
        AxumBodyPolicy::Default => router,
        AxumBodyPolicy::Max(bytes) => router.layer(axum::extract::DefaultBodyLimit::max(bytes)),
        AxumBodyPolicy::Disabled => router.layer(axum::extract::DefaultBodyLimit::disable()),
    };
    router.layer(StreamAdmissionLayer::new(limits))
}

#[cfg(feature = "json")]
fn native_json_request(size: usize) -> Request<Body> {
    Request::post("/json")
        .header(axum::http::header::CONTENT_TYPE, "application/json")
        .body(Body::from(format!("\"{}\"", "x".repeat(size - 2))))
        .unwrap()
}

#[cfg(feature = "json")]
#[tokio::test]
async fn stream_admission_preserves_axum_and_native_json_limits() {
    let default_calls = Arc::new(AtomicUsize::new(0));
    let default_response = native_json_router(AxumBodyPolicy::Default, default_calls.clone())
        .oneshot(native_json_request(3 * 1024 * 1024))
        .await
        .unwrap();
    assert_eq!(default_response.status(), StatusCode::PAYLOAD_TOO_LARGE);
    assert_eq!(default_calls.load(Ordering::SeqCst), 0);

    let max_calls = Arc::new(AtomicUsize::new(0));
    let max_router = native_json_router(AxumBodyPolicy::Max(LARGE_BODY_BUDGET), max_calls.clone());
    for size in [3 * 1024 * 1024, LARGE_BODY_BUDGET] {
        let response = max_router.clone().oneshot(native_json_request(size)).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let payload = axum::body::to_bytes(response.into_body(), 128).await.unwrap();
        assert_eq!(std::str::from_utf8(&payload).unwrap(), (size - 2).to_string());
    }
    assert_eq!(max_calls.load(Ordering::SeqCst), 2);

    let disabled_calls = Arc::new(AtomicUsize::new(0));
    let disabled_response = native_json_router(AxumBodyPolicy::Disabled, disabled_calls.clone())
        .oneshot(native_json_request(5 * 1024 * 1024))
        .await
        .unwrap();
    assert_eq!(disabled_response.status(), StatusCode::PAYLOAD_TOO_LARGE);
    assert_eq!(disabled_calls.load(Ordering::SeqCst), 0);
    let payload = axum::body::to_bytes(disabled_response.into_body(), 1024).await.unwrap();
    assert!(std::str::from_utf8(&payload).unwrap().contains("body_too_large"));
}

#[tokio::test]
async fn admission_deadline_covers_handler_but_not_response_body() {
    async fn slow() -> &'static str {
        tokio::time::sleep(Duration::from_millis(50)).await;
        "late"
    }
    let limits = StreamAdmissionLimits::default()
        .with_admission_timeout(Duration::from_millis(10))
        .unwrap();
    let response = Router::new()
        .route("/", post(slow))
        .layer(StreamAdmissionLayer::new(limits))
        .oneshot(Request::post("/").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::GATEWAY_TIMEOUT);

    async fn streaming() -> Response {
        Response::new(Body::from_stream(futures_util::stream::once(async {
            tokio::time::sleep(Duration::from_millis(40)).await;
            Ok::<_, Infallible>(Bytes::from_static(b"still alive"))
        })))
    }
    let response = Router::new()
        .route("/", post(streaming))
        .layer(StreamAdmissionLayer::new(limits))
        .oneshot(Request::post("/").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        axum::body::to_bytes(response.into_body(), 1024).await.unwrap(),
        "still alive"
    );
}
