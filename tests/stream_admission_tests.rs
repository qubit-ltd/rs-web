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
use axum::http::Request;
use axum::http::StatusCode;
use axum::response::Response;
use axum::routing::post;
use hyper::body::Body as HttpBody;
use qubit_web::StreamAdmissionLayer;
use qubit_web::StreamAdmissionLimits;
use qubit_web::WebRejection;
use tower::ServiceExt;

const DEFAULT_MAX_BODY_BYTES: usize = 1024 * 1024;

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
        .layer(StreamAdmissionLayer::new::<()>(limits))
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
        .layer(StreamAdmissionLayer::new::<()>(StreamAdmissionLimits::default()));
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
        .route("/", post(blocked))
        .with_state(gate.clone())
        .layer(StreamAdmissionLayer::new::<()>(limits));
    let first = tokio::spawn(router.clone().oneshot(Request::post("/").body(Body::empty()).unwrap()));
    gate.entered.notified().await;
    let rejected = router
        .clone()
        .oneshot(Request::post("/").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(rejected.status(), StatusCode::SERVICE_UNAVAILABLE);
    gate.release.add_permits(1);
    assert_eq!(first.await.unwrap().unwrap().status(), StatusCode::OK);
    gate.release.add_permits(1);
    let third = router
        .oneshot(Request::post("/").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(third.status(), StatusCode::OK);
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
        .layer(StreamAdmissionLayer::new::<()>(limits))
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
        .layer(StreamAdmissionLayer::new::<()>(limits))
        .oneshot(Request::post("/").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        axum::body::to_bytes(response.into_body(), 1024).await.unwrap(),
        "still alive"
    );
}
