// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use std::collections::VecDeque;
use std::convert::Infallible;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::task::Context;
use std::task::Poll;

use axum::Router;
use axum::body::Body;
use axum::body::Bytes;
use axum::http::Request;
use axum::http::StatusCode;
use axum::http::header::CONTENT_LENGTH;
use axum::response::Response;
use axum::routing::post;
use hyper::body::Body as HttpBody;
use qubit_web::limit::HttpLimits;
use qubit_web::limit::RequestLimitLayer;
use qubit_web::limit::WebRejection;
use tower::ServiceExt;

struct ObservedChunks {
    chunks: VecDeque<Bytes>,
    poll_count: Arc<AtomicUsize>,
}

impl hyper::body::Body for ObservedChunks {
    type Data = Bytes;
    type Error = Infallible;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        _: &mut Context<'_>,
    ) -> Poll<Option<Result<hyper::body::Frame<Self::Data>, Self::Error>>> {
        self.poll_count.fetch_add(1, Ordering::SeqCst);
        Poll::Ready(self.chunks.pop_front().map(|chunk| Ok(hyper::body::Frame::data(chunk))))
    }
}

async fn consume_body(_: Bytes) -> &'static str {
    "ok"
}

#[tokio::test]
async fn request_limit_stops_polling_after_the_first_over_limit_frame() {
    let poll_count = Arc::new(AtomicUsize::new(0));
    let body = Body::new(ObservedChunks {
        chunks: VecDeque::from([
            Bytes::from_static(b"12"),
            Bytes::from_static(b"345"),
            Bytes::from_static(b"must-not-be-read"),
        ]),
        poll_count: poll_count.clone(),
    });
    let app = Router::new()
        .route("/", post(consume_body))
        .layer(RequestLimitLayer::new(
            HttpLimits::default().with_max_body_bytes(4).unwrap(),
        ));

    let response = app.oneshot(Request::post("/").body(body).unwrap()).await.unwrap();

    assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
    assert_eq!(poll_count.load(Ordering::SeqCst), 2);
}

struct PendingAfterFirst {
    yielded_first: bool,
    poll_count: Arc<AtomicUsize>,
}

impl hyper::body::Body for PendingAfterFirst {
    type Data = Bytes;
    type Error = Infallible;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        _: &mut Context<'_>,
    ) -> Poll<Option<Result<hyper::body::Frame<Self::Data>, Self::Error>>> {
        self.poll_count.fetch_add(1, Ordering::SeqCst);
        if self.yielded_first {
            return Poll::Pending;
        }
        self.yielded_first = true;
        Poll::Ready(Some(Ok(hyper::body::Frame::data(Bytes::from_static(b"12")))))
    }
}

async fn consume_one_frame(mut request: axum::extract::Request) -> &'static str {
    let frame = std::future::poll_fn(|context| Pin::new(request.body_mut()).poll_frame(context))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(frame.into_data().unwrap(), "12");
    "read first frame"
}

#[tokio::test]
async fn handler_starts_before_the_request_body_reaches_eof() {
    let poll_count = Arc::new(AtomicUsize::new(0));
    let body = Body::new(PendingAfterFirst {
        yielded_first: false,
        poll_count: poll_count.clone(),
    });
    let app = Router::new()
        .route("/", post(consume_one_frame))
        .layer(RequestLimitLayer::new(HttpLimits::default()));

    let response = tokio::time::timeout(
        std::time::Duration::from_millis(100),
        app.oneshot(Request::post("/").body(body).unwrap()),
    )
    .await
    .expect("the limit layer must not buffer the complete request body")
    .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(poll_count.load(Ordering::SeqCst), 1);
}

async fn echo(_: Bytes) -> &'static str {
    "ok"
}

#[tokio::test]
async fn declared_and_streamed_body_limits_use_exact_byte_boundaries() {
    let app = Router::new().route("/", post(echo)).layer(RequestLimitLayer::new(
        HttpLimits::default().with_max_body_bytes(4).unwrap(),
    ));
    let exact = Body::new(ObservedChunks {
        chunks: VecDeque::from([Bytes::from_static(b"12"), Bytes::from_static(b"34")]),
        poll_count: Arc::new(AtomicUsize::new(0)),
    });
    let exact_response = app
        .clone()
        .oneshot(Request::post("/").body(exact).unwrap())
        .await
        .unwrap();
    assert_eq!(exact_response.status(), StatusCode::OK);

    let streamed_over_limit = Body::new(ObservedChunks {
        chunks: VecDeque::from([Bytes::from_static(b"12"), Bytes::from_static(b"345")]),
        poll_count: Arc::new(AtomicUsize::new(0)),
    });
    let streamed_response = app
        .clone()
        .oneshot(Request::post("/").body(streamed_over_limit).unwrap())
        .await
        .unwrap();
    assert_eq!(streamed_response.status(), StatusCode::PAYLOAD_TOO_LARGE);

    let declared_response = app
        .oneshot(
            Request::post("/")
                .header(CONTENT_LENGTH, "5")
                .body(Body::from("1"))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(declared_response.status(), StatusCode::PAYLOAD_TOO_LARGE);
}

#[test]
fn zero_byte_limit_is_rejected() {
    assert!(HttpLimits::default().with_max_body_bytes(0).is_err());
    assert!(HttpLimits::default().with_max_concurrent_requests(0).is_err());
    assert!(
        HttpLimits::default()
            .with_request_timeout(std::time::Duration::ZERO)
            .is_err()
    );
}

#[test]
fn infrastructure_rejections_use_stable_problem_json_codes() {
    for (rejection, status, code) in [
        (
            WebRejection::BodyTooLarge,
            StatusCode::PAYLOAD_TOO_LARGE,
            "body_too_large",
        ),
        (
            WebRejection::CapacityExceeded,
            StatusCode::SERVICE_UNAVAILABLE,
            "capacity_exceeded",
        ),
        (
            WebRejection::RequestTimedOut,
            StatusCode::GATEWAY_TIMEOUT,
            "request_timeout",
        ),
    ] {
        let response = axum::response::IntoResponse::into_response(rejection);
        assert_eq!(response.status(), status);
        assert_eq!(response.headers()["content-type"], "application/problem+json");
        assert_eq!(rejection.code(), code);
    }
}

#[derive(Clone)]
struct RequestGate {
    started: Arc<tokio::sync::Notify>,
    release: Arc<tokio::sync::Semaphore>,
}

async fn blocked_handler(axum::extract::State(gate): axum::extract::State<RequestGate>) -> &'static str {
    gate.started.notify_one();
    let permit = gate.release.acquire().await.unwrap();
    permit.forget();
    "ok"
}

#[tokio::test]
async fn capacity_overflow_returns_immediate_503() {
    let gate = RequestGate {
        started: Arc::new(tokio::sync::Notify::new()),
        release: Arc::new(tokio::sync::Semaphore::new(0)),
    };
    let app = Router::new()
        .route("/", axum::routing::get(blocked_handler))
        .with_state(gate.clone())
        .layer(RequestLimitLayer::new(
            HttpLimits::default().with_max_concurrent_requests(1).unwrap(),
        ));
    let first = tokio::spawn(app.clone().oneshot(Request::get("/").body(Body::empty()).unwrap()));
    gate.started.notified().await;
    let rejected = tokio::time::timeout(
        std::time::Duration::from_secs(1),
        app.oneshot(Request::get("/").body(Body::empty()).unwrap()),
    )
    .await
    .expect("over-capacity request should not queue")
    .unwrap();
    assert_eq!(rejected.status(), StatusCode::SERVICE_UNAVAILABLE);
    let payload = axum::body::to_bytes(rejected.into_body(), 1024).await.unwrap();
    assert!(std::str::from_utf8(&payload).unwrap().contains("capacity_exceeded"));
    gate.release.add_permits(1);
    assert_eq!(first.await.unwrap().unwrap().status(), StatusCode::OK);
}

#[tokio::test]
async fn short_request_deadline_returns_gateway_timeout() {
    async fn slow_handler() -> &'static str {
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        "too late"
    }
    let app = Router::new()
        .route("/", axum::routing::get(slow_handler))
        .layer(RequestLimitLayer::new(
            HttpLimits::default()
                .with_request_timeout(std::time::Duration::from_millis(10))
                .unwrap(),
        ));
    let response = app
        .oneshot(Request::get("/").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::GATEWAY_TIMEOUT);
}

#[tokio::test]
async fn permit_is_held_until_response_body_eof() {
    async fn reply() -> &'static str {
        "ok"
    }
    let app = Router::new()
        .route("/", axum::routing::get(reply))
        .layer(RequestLimitLayer::new(
            HttpLimits::default().with_max_concurrent_requests(1).unwrap(),
        ));
    let first = app
        .clone()
        .oneshot(Request::get("/").body(Body::empty()).unwrap())
        .await
        .unwrap();
    let second = app
        .clone()
        .oneshot(Request::get("/").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(first.status(), StatusCode::OK);
    assert_eq!(second.status(), StatusCode::SERVICE_UNAVAILABLE);
    let bytes = axum::body::to_bytes(first.into_body(), 16).await.unwrap();
    assert_eq!(bytes, "ok");
    let third = app
        .oneshot(Request::get("/").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(third.status(), StatusCode::OK);
}

struct NeverEndingResponse;

impl hyper::body::Body for NeverEndingResponse {
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
async fn response_body_deadline_releases_short_request_capacity() {
    let limits = HttpLimits::default()
        .with_max_concurrent_requests(1)
        .unwrap()
        .with_request_timeout(std::time::Duration::from_millis(30))
        .unwrap();
    let app = Router::new()
        .route(
            "/stalled",
            post(|| async { Response::new(Body::new(NeverEndingResponse)) }),
        )
        .route("/ok", post(|| async { "ok" }))
        .layer(RequestLimitLayer::new(limits));

    let response = app
        .clone()
        .oneshot(Request::post("/stalled").body(Body::empty()).unwrap())
        .await
        .unwrap();
    let body_result = tokio::time::timeout(
        std::time::Duration::from_secs(1),
        axum::body::to_bytes(response.into_body(), usize::MAX),
    )
    .await
    .expect("the response body deadline must wake a stalled stream");
    assert!(body_result.is_err());

    let response = app
        .oneshot(Request::post("/ok").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
}
