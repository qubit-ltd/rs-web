// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
#![cfg(feature = "json")]

use std::collections::VecDeque;
use std::io;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::task::Context;
use std::task::Poll;

use axum::Extension;
use axum::Router;
use axum::body::Body;
use axum::body::Bytes;
use axum::body::to_bytes;
use axum::extract::FromRequest;
use axum::http::HeaderMap;
use axum::http::Request;
use axum::http::StatusCode;
use axum::response::Response;
use axum::routing::post;
use futures_util::stream;
use hyper::body::Body as HttpBody;
use qubit_web::StreamAdmissionLayer;
use qubit_web::StreamAdmissionLimits;
use qubit_web::json::BoundedJson;
use qubit_web::json::JsonLimits;
use qubit_web::json::JsonRejection;
use qubit_web::json::json_response;
use serde::Serialize;
use serde::Serializer;
use serde::de::IgnoredAny;
use serde::ser::SerializeStruct;
use tokio::sync::Notify;
use tower::ServiceExt;

struct CountingOwner {
    bytes: Vec<u8>,
    active: Arc<AtomicUsize>,
}

struct MustNotPollBody;

impl HttpBody for MustNotPollBody {
    type Data = Bytes;
    type Error = io::Error;

    fn poll_frame(
        self: Pin<&mut Self>,
        _: &mut Context<'_>,
    ) -> Poll<Option<Result<hyper::body::Frame<Self::Data>, Self::Error>>> {
        panic!("oversized declared body must not be polled");
    }
}

impl CountingOwner {
    fn new(bytes: Vec<u8>, active: Arc<AtomicUsize>) -> Self {
        active.fetch_add(1, Ordering::SeqCst);
        Self { bytes, active }
    }
}

impl AsRef<[u8]> for CountingOwner {
    fn as_ref(&self) -> &[u8] {
        &self.bytes
    }
}

impl Drop for CountingOwner {
    fn drop(&mut self) {
        self.active.fetch_sub(1, Ordering::SeqCst);
    }
}

struct OwnerFramesBody {
    bytes: &'static [u8],
    index: usize,
    active: Arc<AtomicUsize>,
    owners_live_before_frame: Arc<Mutex<Vec<usize>>>,
}

impl HttpBody for OwnerFramesBody {
    type Data = Bytes;
    type Error = io::Error;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        _: &mut Context<'_>,
    ) -> Poll<Option<Result<hyper::body::Frame<Self::Data>, Self::Error>>> {
        if self.index == self.bytes.len() {
            return Poll::Ready(None);
        }
        self.owners_live_before_frame
            .lock()
            .unwrap()
            .push(self.active.load(Ordering::SeqCst));
        let byte = self.bytes[self.index];
        self.index += 1;
        let owner = CountingOwner::new(vec![byte], self.active.clone());
        Poll::Ready(Some(Ok(hyper::body::Frame::data(Bytes::from_owner(owner)))))
    }
}

fn owner_frames_body(
    bytes: &'static [u8],
    active: Arc<AtomicUsize>,
    owners_live_before_frame: Arc<Mutex<Vec<usize>>>,
) -> Body {
    Body::new(OwnerFramesBody {
        bytes,
        index: 0,
        active,
        owners_live_before_frame,
    })
}

enum ScriptedFrame {
    Data(&'static [u8]),
    Error(&'static str),
    Trailers,
}

struct ScriptedBody {
    frames: VecDeque<ScriptedFrame>,
    active: Arc<AtomicUsize>,
}

impl HttpBody for ScriptedBody {
    type Data = Bytes;
    type Error = io::Error;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        _: &mut Context<'_>,
    ) -> Poll<Option<Result<hyper::body::Frame<Self::Data>, Self::Error>>> {
        let Some(frame) = self.frames.pop_front() else {
            return Poll::Ready(None);
        };
        match frame {
            ScriptedFrame::Data(data) => {
                let owner = CountingOwner::new(data.to_vec(), self.active.clone());
                Poll::Ready(Some(Ok(hyper::body::Frame::data(Bytes::from_owner(owner)))))
            }
            ScriptedFrame::Error(message) => Poll::Ready(Some(Err(io::Error::other(message)))),
            ScriptedFrame::Trailers => Poll::Ready(Some(Ok(hyper::body::Frame::trailers(HeaderMap::new())))),
        }
    }
}

struct PendingOwnerBody {
    active: Arc<AtomicUsize>,
    pending: Arc<Notify>,
    dropped: Arc<Notify>,
    state: u8,
}

impl HttpBody for PendingOwnerBody {
    type Data = Bytes;
    type Error = io::Error;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        _: &mut Context<'_>,
    ) -> Poll<Option<Result<hyper::body::Frame<Self::Data>, Self::Error>>> {
        match self.state {
            0 => {
                self.state = 1;
                let owner = CountingOwner::new(b"1".to_vec(), self.active.clone());
                Poll::Ready(Some(Ok(hyper::body::Frame::data(Bytes::from_owner(owner)))))
            }
            _ => {
                self.pending.notify_one();
                Poll::Pending
            }
        }
    }
}

impl Drop for PendingOwnerBody {
    fn drop(&mut self) {
        self.dropped.notify_one();
    }
}

struct FailingPayload;

impl Serialize for FailingPayload {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut value = serializer.serialize_struct("FailingPayload", 2)?;
        value.serialize_field("written", "partial-value")?;
        Err(<S::Error as serde::ser::Error>::custom("private-json-error-sentinel"))
    }
}

async fn echo_number(BoundedJson(value): BoundedJson<u64>) -> String {
    value.to_string()
}

async fn echo_array(BoundedJson(value): BoundedJson<Vec<u64>>) -> String {
    value.len().to_string()
}

async fn ignore_json(BoundedJson(_): BoundedJson<IgnoredAny>) -> &'static str {
    "accepted"
}

fn app() -> Router {
    Router::new().route("/number", post(echo_number))
}

async fn send(uri: &str, content_type: &str, body: &'static str) -> Response {
    app()
        .oneshot(
            Request::post(uri)
                .header("content-type", content_type)
                .body(Body::from(body))
                .unwrap(),
        )
        .await
        .unwrap()
}

async fn send_with_limits(body: &'static str, limits: JsonLimits) -> Response {
    Router::new()
        .route("/", post(ignore_json))
        .layer(Extension(limits))
        .oneshot(
            Request::post("/")
                .header("content-type", "application/json")
                .body(Body::from(body))
                .unwrap(),
        )
        .await
        .unwrap()
}

#[tokio::test]
async fn test_accepts_json_and_vendor_json_media_types() {
    assert_eq!(send("/number", "application/json", "42").await.status(), StatusCode::OK);
    assert_eq!(
        send("/number", "application/vnd.example+json", "42").await.status(),
        StatusCode::OK
    );
    assert_eq!(
        send("/number", "APPLICATION/JSON; Charset=\"UTF-8\"", "42")
            .await
            .status(),
        StatusCode::OK
    );
    assert_eq!(
        send("/number", "application/vnd.example+json; charset=utf-8", "42")
            .await
            .status(),
        StatusCode::OK
    );
    assert_eq!(
        send("/number", " application/vnd.example+json ; charset = utf-8 ", "42")
            .await
            .status(),
        StatusCode::OK
    );
}

#[tokio::test]
async fn test_rejects_non_utf8_duplicate_and_malformed_content_type_parameters() {
    for content_type in [
        "application/json; charset=us-ascii",
        "application/json; profile=example",
        "application/json; charset=utf-8; charset=utf-8",
        "application/json; charset='utf-8'",
        "application/json; charset=\"utf-8",
        "application/json; charset=utf-8;",
    ] {
        assert_eq!(
            send("/number", content_type, "42").await.status(),
            StatusCode::UNSUPPORTED_MEDIA_TYPE,
            "unexpectedly accepted {content_type}"
        );
    }
}

#[tokio::test]
async fn test_rejects_json_subtypes_with_non_tchar_bytes() {
    for content_type in [
        "application/vnd.example name+json",
        "application/vnd.example?name+json",
        "application/vnd.example/name+json",
    ] {
        assert_eq!(
            send("/number", content_type, "42").await.status(),
            StatusCode::UNSUPPORTED_MEDIA_TYPE,
            "unexpectedly accepted {content_type}"
        );
    }
}

#[tokio::test]
async fn test_decodes_u64_max_without_precision_loss() {
    let response = send("/number", "application/json", "18446744073709551615").await;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        to_bytes(response.into_body(), usize::MAX).await.unwrap(),
        "18446744073709551615"
    );
}

#[tokio::test]
async fn test_rejects_unsupported_media_type() {
    assert_eq!(
        send("/number", "text/plain", "42").await.status(),
        StatusCode::UNSUPPORTED_MEDIA_TYPE
    );
}

#[tokio::test]
async fn test_rejects_invalid_json_and_target_type_mismatch() {
    assert_eq!(
        send("/number", "application/json", "42 trailing").await.status(),
        StatusCode::BAD_REQUEST
    );
    assert_eq!(
        send("/number", "application/json", "\"not a number\"").await.status(),
        StatusCode::BAD_REQUEST
    );
}

#[tokio::test]
async fn test_rejects_body_over_input_budget() {
    let limits = JsonLimits::default().with_max_input_bytes(1).unwrap();
    let response = Router::new()
        .route("/", post(echo_number))
        .layer(Extension(limits))
        .oneshot(
            Request::post("/")
                .header("content-type", "application/json")
                .body(Body::from("42"))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
    let response_body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    assert!(
        String::from_utf8(response_body.to_vec())
            .unwrap()
            .contains("json_budget_exceeded")
    );
}

#[tokio::test]
async fn test_body_read_failure_is_not_reported_as_budget_exceeded() {
    let sentinel = "private body read failure detail";
    let body = Body::from_stream(stream::once(async move {
        Err::<&'static str, io::Error>(io::Error::other(sentinel))
    }));
    let response = Router::new()
        .route("/", post(echo_number))
        .oneshot(
            Request::post("/")
                .header("content-type", "application/json")
                .body(body)
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let response_body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let response_body = String::from_utf8(response_body.to_vec()).unwrap();
    assert!(response_body.contains("json_body_read_failed"));
    assert!(!response_body.contains(sentinel));
}

#[tokio::test]
async fn test_accepts_input_exactly_at_byte_budget() {
    let limits = JsonLimits::default().with_max_input_bytes(2).unwrap();
    assert_eq!(send_with_limits("42", limits).await.status(), StatusCode::OK);
}

#[tokio::test]
async fn test_declared_json_length_over_budget_does_not_poll_body() {
    let mut request = Request::post("/")
        .header("content-type", "application/json")
        .header("content-length", "2")
        .body(Body::new(MustNotPollBody))
        .unwrap();
    request
        .extensions_mut()
        .insert(JsonLimits::default().with_max_input_bytes(1).unwrap());
    let rejection = BoundedJson::<u64>::from_request(request, &()).await.unwrap_err();
    assert!(matches!(&rejection, JsonRejection::BudgetExceeded));
    let response = axum::response::IntoResponse::into_response(rejection);
    assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    assert!(
        String::from_utf8(body.to_vec())
            .unwrap()
            .contains("json_budget_exceeded")
    );
}

#[tokio::test]
async fn test_declared_json_length_checks_media_type_first() {
    let mut request = Request::post("/")
        .header("content-type", "text/plain")
        .header("content-length", "2")
        .body(Body::new(MustNotPollBody))
        .unwrap();
    request
        .extensions_mut()
        .insert(JsonLimits::default().with_max_input_bytes(1).unwrap());
    assert!(matches!(
        BoundedJson::<u64>::from_request(request, &()).await,
        Err(JsonRejection::UnsupportedMediaType)
    ));
}

#[tokio::test]
async fn test_declared_json_length_equal_to_budget_is_read() {
    let mut request = Request::post("/")
        .header("content-type", "application/json")
        .header("content-length", "2")
        .body(Body::from("42"))
        .unwrap();
    request
        .extensions_mut()
        .insert(JsonLimits::default().with_max_input_bytes(2).unwrap());
    assert_eq!(BoundedJson::<u64>::from_request(request, &()).await.unwrap().0, 42);
}

#[tokio::test]
async fn test_missing_or_invalid_declared_json_length_uses_body_budget() {
    for declaration in [None, Some("invalid")] {
        let mut builder = Request::post("/").header("content-type", "application/json");
        if let Some(value) = declaration {
            builder = builder.header("content-length", value);
        }
        let mut request = builder.body(Body::from("42")).unwrap();
        request
            .extensions_mut()
            .insert(JsonLimits::default().with_max_input_bytes(2).unwrap());
        assert_eq!(BoundedJson::<u64>::from_request(request, &()).await.unwrap().0, 42);
    }
}

#[tokio::test]
async fn test_actual_oversized_body_still_exceeds_budget() {
    let mut request = Request::post("/")
        .header("content-type", "application/json")
        .header("content-length", "1")
        .body(Body::from("42"))
        .unwrap();
    request
        .extensions_mut()
        .insert(JsonLimits::default().with_max_input_bytes(1).unwrap());
    assert!(matches!(
        BoundedJson::<u64>::from_request(request, &()).await,
        Err(JsonRejection::BudgetExceeded)
    ));
}

#[tokio::test]
async fn test_u64_max_declared_json_length_is_rejected_without_polling() {
    let mut request = Request::post("/")
        .header("content-type", "application/json")
        .header("content-length", "18446744073709551615")
        .body(Body::new(MustNotPollBody))
        .unwrap();
    request
        .extensions_mut()
        .insert(JsonLimits::default().with_max_input_bytes(1).unwrap());
    assert!(matches!(
        BoundedJson::<u64>::from_request(request, &()).await,
        Err(JsonRejection::BudgetExceeded)
    ));
}

#[tokio::test]
async fn test_overflowing_declared_json_length_falls_back_to_body_read() {
    let mut request = Request::post("/")
        .header("content-type", "application/json")
        .header("content-length", "18446744073709551616")
        .body(Body::from("42"))
        .unwrap();
    request
        .extensions_mut()
        .insert(JsonLimits::default().with_max_input_bytes(2).unwrap());
    assert_eq!(BoundedJson::<u64>::from_request(request, &()).await.unwrap().0, 42);
}

#[tokio::test]
async fn test_fragmented_json_releases_prior_frame_owners() {
    const JSON: &[u8] = b"12345678";
    let active = Arc::new(AtomicUsize::new(0));
    let owners_live_before_frame = Arc::new(Mutex::new(Vec::new()));
    let limits = JsonLimits::default().with_max_input_bytes(8).unwrap();
    let response = Router::new()
        .route("/", post(echo_number))
        .layer(Extension(limits))
        .oneshot(
            Request::post("/")
                .header("content-type", "application/json")
                .body(owner_frames_body(
                    JSON,
                    active.clone(),
                    owners_live_before_frame.clone(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(to_bytes(response.into_body(), usize::MAX).await.unwrap(), "12345678");
    assert_eq!(active.load(Ordering::SeqCst), 0);
    assert_eq!(
        *owners_live_before_frame.lock().unwrap(),
        vec![0; JSON.len()],
        "each frame owner should be dropped before polling the next frame"
    );
}

#[tokio::test]
async fn test_input_budget_stops_at_ninth_frame_without_reading_tenth() {
    let active = Arc::new(AtomicUsize::new(0));
    let owners_live_before_frame = Arc::new(Mutex::new(Vec::new()));
    let body = owner_frames_body(b"12345678X!", active.clone(), owners_live_before_frame.clone());
    let limits = JsonLimits::default().with_max_input_bytes(8).unwrap();
    let mut request = Request::post("/")
        .header("content-type", "application/json")
        .body(body)
        .unwrap();
    request.extensions_mut().insert(limits);
    let rejection = BoundedJson::<u64>::from_request(request, &()).await.unwrap_err();
    let response = axum::response::IntoResponse::into_response(rejection);

    assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    assert!(
        String::from_utf8(body.to_vec())
            .unwrap()
            .contains("json_budget_exceeded")
    );
    assert_eq!(owners_live_before_frame.lock().unwrap().len(), 9);
    assert_eq!(active.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn test_body_read_failure_after_data_is_private_and_releases_owners() {
    let sentinel = "private-read-error-sentinel";
    let active = Arc::new(AtomicUsize::new(0));
    let body = Body::new(ScriptedBody {
        frames: VecDeque::from([ScriptedFrame::Data(b"42"), ScriptedFrame::Error(sentinel)]),
        active: active.clone(),
    });
    let response = Router::new()
        .route("/", post(echo_number))
        .oneshot(
            Request::post("/")
                .header("content-type", "application/json")
                .body(body)
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let body = String::from_utf8(body.to_vec()).unwrap();
    assert!(body.contains("json_body_read_failed"));
    assert!(!body.contains(sentinel));
    assert_eq!(active.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn test_empty_data_frames_and_trailers_do_not_change_json_input() {
    let active = Arc::new(AtomicUsize::new(0));
    let body = Body::new(ScriptedBody {
        frames: VecDeque::from([
            ScriptedFrame::Data(b""),
            ScriptedFrame::Data(b"4"),
            ScriptedFrame::Data(b""),
            ScriptedFrame::Data(b"2"),
            ScriptedFrame::Trailers,
        ]),
        active: active.clone(),
    });
    let response = Router::new()
        .route("/", post(echo_number))
        .oneshot(
            Request::post("/")
                .header("content-type", "application/json")
                .body(body)
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(to_bytes(response.into_body(), usize::MAX).await.unwrap(), "42");
    assert_eq!(active.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn test_empty_data_only_is_invalid_json() {
    let active = Arc::new(AtomicUsize::new(0));
    let body = Body::new(ScriptedBody {
        frames: VecDeque::from([ScriptedFrame::Data(b""), ScriptedFrame::Data(b"")]),
        active: active.clone(),
    });
    let response = Router::new()
        .route("/", post(echo_number))
        .oneshot(
            Request::post("/")
                .header("content-type", "application/json")
                .body(body)
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    assert!(String::from_utf8(body.to_vec()).unwrap().contains("invalid_json"));
    assert_eq!(active.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn test_cancelling_fragmented_json_releases_body_and_frame_owner() {
    let active = Arc::new(AtomicUsize::new(0));
    let pending = Arc::new(Notify::new());
    let dropped = Arc::new(Notify::new());
    let body = Body::new(PendingOwnerBody {
        active: active.clone(),
        pending: pending.clone(),
        dropped: dropped.clone(),
        state: 0,
    });
    let request = Request::post("/")
        .header("content-type", "application/json")
        .body(body)
        .unwrap();
    let task = tokio::spawn(async move { BoundedJson::<u64>::from_request(request, &()).await });
    pending.notified().await;
    task.abort();
    let result = task.await;

    assert!(result.unwrap_err().is_cancelled());
    dropped.notified().await;
    assert_eq!(active.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn test_rejects_structure_budget_exceeded() {
    let limits = JsonLimits::default().with_max_sequence_items(1).unwrap();
    let response = Router::new()
        .route("/", post(echo_array))
        .layer(Extension(limits))
        .oneshot(
            Request::post("/")
                .header("content-type", "application/json")
                .body(Body::from("[1,2]"))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
    let response_body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    assert!(
        String::from_utf8(response_body.to_vec())
            .unwrap()
            .contains("json_budget_exceeded")
    );
}

#[tokio::test]
async fn test_stream_admission_replay_works_with_bounded_json() {
    let admission = StreamAdmissionLimits::default().with_max_body_bytes(16).unwrap();
    let valid = Router::new()
        .route("/", post(echo_number))
        .layer(Extension(JsonLimits::default()))
        .layer(StreamAdmissionLayer::new(admission));
    let response = valid
        .clone()
        .oneshot(
            Request::post("/")
                .header("content-type", "application/json")
                .body(Body::from("42"))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(to_bytes(response.into_body(), usize::MAX).await.unwrap(), "42");

    let byte_limited = Router::new()
        .route("/", post(echo_number))
        .layer(Extension(JsonLimits::default().with_max_input_bytes(1).unwrap()))
        .layer(StreamAdmissionLayer::new(admission));
    let response = byte_limited
        .oneshot(
            Request::post("/")
                .header("content-type", "application/json")
                .body(Body::from("42"))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);

    let structure_limited = Router::new()
        .route("/", post(echo_array))
        .layer(Extension(JsonLimits::default().with_max_sequence_items(1).unwrap()))
        .layer(StreamAdmissionLayer::new(admission));
    let response = structure_limited
        .oneshot(
            Request::post("/")
                .header("content-type", "application/json")
                .body(Body::from("[1,2]"))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);

    let invalid = Router::new()
        .route("/", post(echo_number))
        .layer(StreamAdmissionLayer::new(admission));
    let response = invalid
        .oneshot(
            Request::post("/")
                .header("content-type", "application/json")
                .body(Body::from("not-json"))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);

    let trailers = Router::new()
        .route("/", post(echo_number))
        .layer(StreamAdmissionLayer::new(admission));
    let body = Body::new(ScriptedBody {
        frames: VecDeque::from([ScriptedFrame::Data(b"42"), ScriptedFrame::Trailers]),
        active: Arc::new(AtomicUsize::new(0)),
    });
    let response = trailers
        .oneshot(
            Request::post("/")
                .header("content-type", "application/json")
                .body(body)
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(to_bytes(response.into_body(), usize::MAX).await.unwrap(), "42");
}

#[tokio::test]
async fn test_rejects_max_depth_budget() {
    let limits = JsonLimits::default().with_max_depth(1).unwrap();
    assert_eq!(
        send_with_limits("{\"a\":{\"b\":0}}", limits).await.status(),
        StatusCode::PAYLOAD_TOO_LARGE
    );
}

#[tokio::test]
async fn test_rejects_max_nodes_budget() {
    let limits = JsonLimits::default().with_max_nodes(1).unwrap();
    assert_eq!(
        send_with_limits("[1]", limits).await.status(),
        StatusCode::PAYLOAD_TOO_LARGE
    );
}

#[tokio::test]
async fn test_rejects_max_map_entries_budget() {
    let limits = JsonLimits::default().with_max_map_entries(1).unwrap();
    assert_eq!(
        send_with_limits("{\"a\":1,\"b\":2}", limits).await.status(),
        StatusCode::PAYLOAD_TOO_LARGE
    );
}

#[test]
fn test_output_serializer_failure_is_private_and_returns_no_response() {
    let error = json_response(&FailingPayload, &JsonLimits::default()).unwrap_err();
    assert_eq!(error.code(), "json_encode_failed");
    assert_eq!(error.to_string(), "json_encode_failed");
    assert!(!format!("{error}").contains("private-json-error-sentinel"));
    assert!(!format!("{error:?}").contains("private-json-error-sentinel"));
}

#[tokio::test]
async fn test_rejects_max_key_bytes_budget() {
    let limits = JsonLimits::default().with_max_key_bytes(2).unwrap();
    assert_eq!(
        send_with_limits("{\"long\":1}", limits).await.status(),
        StatusCode::PAYLOAD_TOO_LARGE
    );
}

#[tokio::test]
async fn test_rejects_max_string_bytes_budget() {
    let limits = JsonLimits::default().with_max_string_bytes(2).unwrap();
    assert_eq!(
        send_with_limits("\"abc\"", limits).await.status(),
        StatusCode::PAYLOAD_TOO_LARGE
    );
}

#[tokio::test]
async fn test_rejects_max_number_bytes_budget() {
    let limits = JsonLimits::default().with_max_number_bytes(2).unwrap();
    assert_eq!(
        send_with_limits("123", limits).await.status(),
        StatusCode::PAYLOAD_TOO_LARGE
    );
}

#[tokio::test]
async fn test_output_budget_failure_returns_no_partial_json_response() {
    let limits = JsonLimits::default().with_max_output_bytes(1).unwrap();
    let error = json_response(&u64::MAX, &limits).unwrap_err();
    assert_eq!(error.code(), "json_output_budget_exceeded");
}

#[tokio::test]
async fn test_output_structure_budget_failure_returns_encoding_error() {
    let limits = JsonLimits::default().with_max_sequence_items(1).unwrap();
    let error = json_response(&[1_u64, 2], &limits).unwrap_err();
    assert_eq!(error.code(), "json_output_budget_exceeded");
}

#[tokio::test]
async fn test_output_preserves_u64_max_precision() {
    let response = json_response(&u64::MAX, &JsonLimits::default()).unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers()["content-type"], "application/json");
    assert_eq!(
        to_bytes(response.into_body(), usize::MAX).await.unwrap(),
        u64::MAX.to_string()
    );
}
