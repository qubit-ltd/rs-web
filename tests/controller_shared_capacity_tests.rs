// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use std::convert::Infallible;
use std::sync::Arc;
use std::time::Duration;

use axum::Router;
use axum::body::Body;
use axum::body::Bytes;
use axum::http::Request;
use axum::http::StatusCode;
use axum::response::Response;
use axum::routing::get;
use qubit_web::HttpLimits;
use qubit_web::StreamAdmissionLayer;
use qubit_web::StreamAdmissionLimits;
use qubit_web::StreamAdmissionState;
use qubit_web::limit::LimitState;
use qubit_web::mvc::ControllerRoutes;
use qubit_web::rest_controller;
use tokio::sync::Semaphore;
use tower::ServiceExt;

struct LongBodyController;

#[rest_controller("/shared-short")]
impl LongBodyController {
    #[get_mapping("/controller")]
    async fn controller(&self) -> Response {
        Response::new(Body::from_stream(futures_util::stream::pending::<
            Result<Bytes, Infallible>,
        >()))
    }
}

struct EarlierController;
struct LaterController;

#[rest_controller("/earlier")]
impl EarlierController {
    #[get_mapping("")]
    async fn get(&self) -> Response {
        Response::new(Body::from_stream(futures_util::stream::pending::<
            Result<Bytes, Infallible>,
        >()))
    }
}

#[rest_controller("/later")]
impl LaterController {
    #[get_mapping("")]
    async fn get(&self) -> Response {
        Response::new(Body::from_stream(futures_util::stream::pending::<
            Result<Bytes, Infallible>,
        >()))
    }
}

struct AdmissionController {
    started: Arc<Semaphore>,
    release: Arc<Semaphore>,
}

#[rest_controller("/shared-stream")]
impl AdmissionController {
    #[route("/controller", method = "GET", kind = "sse")]
    async fn controller(&self) -> &'static str {
        self.started.add_permits(1);
        self.release.acquire().await.unwrap().forget();
        "headers-ready"
    }
}

fn request(uri: &'static str) -> Request<Body> {
    Request::get(uri).body(Body::empty()).unwrap()
}

async fn checked<T>(future: T) -> T::Output
where
    T: std::future::Future,
{
    tokio::time::timeout(Duration::from_secs(2), future)
        .await
        .expect("request should complete without hanging")
}

#[tokio::test]
async fn test_controller_and_native_short_routes_share_body_lifetime_capacity() {
    let state = LimitState::new(HttpLimits::default().with_max_concurrent_requests(1).unwrap());
    let controller = ControllerRoutes::<()>::new()
        .with_limit_state(state.clone())
        .add(Arc::new(LongBodyController))
        .unwrap()
        .finish();
    let native = Router::new()
        .route("/shared-short/native", get(|| async { "native" }))
        .layer(qubit_web::RequestLimitLayer::from_state(state.clone()));
    let app = controller.clone().merge(native);

    let response = checked(app.clone().oneshot(request("/shared-short/controller")))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let rejected = checked(app.clone().oneshot(request("/shared-short/native")))
        .await
        .unwrap();
    assert_eq!(rejected.status(), StatusCode::SERVICE_UNAVAILABLE);
    drop(response);
    let accepted = checked(app.clone().oneshot(request("/shared-short/native")))
        .await
        .unwrap();
    assert_eq!(accepted.status(), StatusCode::OK);
    drop(accepted);

    // The reverse direction uses the same state after the previous body is dropped.
    let native_pending = Router::new()
        .route(
            "/pending",
            get(|| async {
                Response::new(Body::from_stream(futures_util::stream::pending::<
                    Result<Bytes, Infallible>,
                >()))
            }),
        )
        .layer(qubit_web::RequestLimitLayer::from_state(state));
    let reverse = controller.merge(native_pending);
    let held = checked(reverse.clone().oneshot(request("/pending"))).await.unwrap();
    assert_eq!(held.status(), StatusCode::OK);
    let rejected = checked(reverse.clone().oneshot(request("/shared-short/controller")))
        .await
        .unwrap();
    assert_eq!(rejected.status(), StatusCode::SERVICE_UNAVAILABLE);
    drop(held);
    let accepted = checked(reverse.oneshot(request("/shared-short/controller")))
        .await
        .unwrap();
    assert_eq!(accepted.status(), StatusCode::OK);
    drop(accepted);
}

#[tokio::test]
async fn test_controller_and_native_stream_routes_share_admission_until_headers() {
    let state = StreamAdmissionState::new(
        StreamAdmissionLimits::default()
            .with_max_concurrent_admissions(1)
            .unwrap(),
    );
    let started = Arc::new(Semaphore::new(0));
    let release = Arc::new(Semaphore::new(0));
    let controller = ControllerRoutes::<()>::new()
        .with_stream_admission_state(state.clone())
        .add(Arc::new(AdmissionController {
            started: started.clone(),
            release: release.clone(),
        }))
        .unwrap()
        .finish();
    let native = Router::new()
        .route("/native", get(|| async { "native" }))
        .layer(StreamAdmissionLayer::from_state(state));
    let app = controller.merge(native);

    let first_app = app.clone();
    let first = tokio::spawn(async move { first_app.oneshot(request("/shared-stream/controller")).await.unwrap() });
    let started_permit = checked(started.acquire()).await.unwrap();
    started_permit.forget();
    let rejected = checked(app.clone().oneshot(request("/native"))).await.unwrap();
    assert_eq!(rejected.status(), StatusCode::SERVICE_UNAVAILABLE);

    release.add_permits(1);
    let response = checked(first).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    // Admission capacity ends at response headers, independently of body lifetime.
    let accepted = checked(app.oneshot(request("/native"))).await.unwrap();
    assert_eq!(accepted.status(), StatusCode::OK);
}

#[tokio::test]
async fn test_independent_short_and_stream_states_remain_separate_domains() {
    let short = LimitState::new(HttpLimits::default().with_max_concurrent_requests(1).unwrap());
    let stream = StreamAdmissionState::new(
        StreamAdmissionLimits::default()
            .with_max_concurrent_admissions(1)
            .unwrap(),
    );
    let controller = ControllerRoutes::<()>::new()
        .with_limit_state(short.clone())
        .with_stream_admission_state(stream.clone())
        .add(Arc::new(AdmissionController {
            started: Arc::new(Semaphore::new(0)),
            release: Arc::new(Semaphore::new(1)),
        }))
        .unwrap()
        .finish();
    let native_short = Router::new()
        .route("/native-short", get(|| async { "short" }))
        .layer(qubit_web::RequestLimitLayer::from_state(short));
    let native_stream = Router::new()
        .route("/native-stream", get(|| async { "stream" }))
        .layer(StreamAdmissionLayer::from_state(stream));
    let app = controller.merge(native_short).merge(native_stream);
    let response = checked(app.clone().oneshot(request("/shared-stream/controller")))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        checked(app.oneshot(request("/native-short"))).await.unwrap().status(),
        StatusCode::OK
    );
}

#[tokio::test]
async fn test_replacement_only_affects_later_registrations_and_latest_setter_wins() {
    let first_state = LimitState::new(HttpLimits::default().with_max_concurrent_requests(1).unwrap());
    let second_state = LimitState::new(HttpLimits::default().with_max_concurrent_requests(1).unwrap());
    let routes = ControllerRoutes::<()>::new()
        .with_limit_state(first_state.clone())
        .add(Arc::new(EarlierController))
        .unwrap()
        .with_limit_state(LimitState::new(
            HttpLimits::default().with_max_concurrent_requests(2).unwrap(),
        ))
        .with_http_limits(HttpLimits::default().with_max_concurrent_requests(2).unwrap())
        .with_limit_state(second_state.clone())
        .add(Arc::new(LaterController))
        .unwrap()
        .finish();
    let native_first = Router::new()
        .route("/native-first", get(|| async { "first" }))
        .layer(qubit_web::RequestLimitLayer::from_state(first_state));
    let native_second = Router::new()
        .route("/native-second", get(|| async { "second" }))
        .layer(qubit_web::RequestLimitLayer::from_state(second_state));
    let app = routes.merge(native_first).merge(native_second);

    let earlier = checked(app.clone().oneshot(request("/earlier"))).await.unwrap();
    assert_eq!(earlier.status(), StatusCode::OK);
    assert_eq!(
        checked(app.clone().oneshot(request("/native-first")))
            .await
            .unwrap()
            .status(),
        StatusCode::SERVICE_UNAVAILABLE
    );
    drop(earlier);

    let later = checked(app.clone().oneshot(request("/later"))).await.unwrap();
    assert_eq!(later.status(), StatusCode::OK);
    assert_eq!(
        checked(app.clone().oneshot(request("/native-second")))
            .await
            .unwrap()
            .status(),
        StatusCode::SERVICE_UNAVAILABLE
    );
    drop(later);
}

#[tokio::test]
async fn test_cancelled_stream_admission_releases_its_permit() {
    let state = StreamAdmissionState::new(
        StreamAdmissionLimits::default()
            .with_max_concurrent_admissions(1)
            .unwrap(),
    );
    let started = Arc::new(Semaphore::new(0));
    let release = Arc::new(Semaphore::new(0));
    let controller = ControllerRoutes::<()>::new()
        .with_stream_admission_state(state.clone())
        .add(Arc::new(AdmissionController {
            started: started.clone(),
            release,
        }))
        .unwrap()
        .finish();
    let native = Router::new()
        .route("/native", get(|| async { "native" }))
        .layer(StreamAdmissionLayer::from_state(state));
    let app = controller.merge(native);
    let first_app = app.clone();
    let first = tokio::spawn(async move { first_app.oneshot(request("/shared-stream/controller")).await });
    let started_permit = checked(started.acquire()).await.unwrap();
    started_permit.forget();
    first.abort();
    let _cancelled = checked(first).await;
    let response = checked(app.oneshot(request("/native"))).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
}

#[tokio::test]
async fn test_timed_out_stream_admission_releases_its_permit() {
    let state = StreamAdmissionState::new(
        StreamAdmissionLimits::default()
            .with_max_concurrent_admissions(1)
            .unwrap()
            .with_admission_timeout(Duration::from_millis(30))
            .unwrap(),
    );
    let controller = ControllerRoutes::<()>::new()
        .with_stream_admission_state(state.clone())
        .add(Arc::new(AdmissionController {
            started: Arc::new(Semaphore::new(0)),
            release: Arc::new(Semaphore::new(0)),
        }))
        .unwrap()
        .finish();
    let native = Router::new()
        .route("/native", get(|| async { "native" }))
        .layer(StreamAdmissionLayer::from_state(state));
    let app = controller.merge(native);
    let timed_out = checked(app.clone().oneshot(request("/shared-stream/controller")))
        .await
        .unwrap();
    assert_eq!(timed_out.status(), StatusCode::GATEWAY_TIMEOUT);
    assert_eq!(
        checked(app.oneshot(request("/native"))).await.unwrap().status(),
        StatusCode::OK
    );
}

#[tokio::test]
async fn test_separate_controller_builders_share_the_injected_capacity() {
    let state = LimitState::new(HttpLimits::default().with_max_concurrent_requests(1).unwrap());
    let first = ControllerRoutes::<()>::new()
        .with_limit_state(state.clone())
        .add(Arc::new(EarlierController))
        .unwrap()
        .finish();
    let second = ControllerRoutes::<()>::new()
        .with_limit_state(state)
        .add(Arc::new(LaterController))
        .unwrap()
        .finish();
    let app = first.merge(second);
    let held = checked(app.clone().oneshot(request("/earlier"))).await.unwrap();
    assert_eq!(held.status(), StatusCode::OK);
    let rejected = checked(app.clone().oneshot(request("/later"))).await.unwrap();
    assert_eq!(rejected.status(), StatusCode::SERVICE_UNAVAILABLE);
    drop(held);
    assert_eq!(
        checked(app.oneshot(request("/later"))).await.unwrap().status(),
        StatusCode::OK
    );
}

#[tokio::test]
async fn test_default_controller_state_is_independent_from_native_state() {
    let controller = ControllerRoutes::<()>::new()
        .add(Arc::new(LongBodyController))
        .unwrap()
        .finish();
    let native_state = LimitState::new(HttpLimits::default().with_max_concurrent_requests(1).unwrap());
    let native = Router::new()
        .route("/native-independent", get(|| async { "native" }))
        .layer(qubit_web::RequestLimitLayer::from_state(native_state));
    let app = controller.merge(native);
    let held = checked(app.clone().oneshot(request("/shared-short/controller")))
        .await
        .unwrap();
    assert_eq!(held.status(), StatusCode::OK);
    assert_eq!(
        checked(app.oneshot(request("/native-independent")))
            .await
            .unwrap()
            .status(),
        StatusCode::OK
    );
}

#[tokio::test]
async fn test_stream_setter_last_call_selects_state_for_later_controller() {
    let old_state = StreamAdmissionState::new(
        StreamAdmissionLimits::default()
            .with_max_concurrent_admissions(1)
            .unwrap(),
    );
    let selected_state = StreamAdmissionState::new(
        StreamAdmissionLimits::default()
            .with_max_concurrent_admissions(1)
            .unwrap(),
    );
    let started = Arc::new(Semaphore::new(0));
    let release = Arc::new(Semaphore::new(0));
    let controller = ControllerRoutes::<()>::new()
        .with_stream_admission_state(old_state.clone())
        .with_stream_admission_limits(
            StreamAdmissionLimits::default()
                .with_max_concurrent_admissions(2)
                .unwrap(),
        )
        .with_stream_admission_state(selected_state.clone())
        .add(Arc::new(AdmissionController {
            started: started.clone(),
            release: release.clone(),
        }))
        .unwrap()
        .finish();
    let native_old = Router::new()
        .route("/old", get(|| async { "old" }))
        .layer(StreamAdmissionLayer::from_state(old_state));
    let native_selected = Router::new()
        .route("/selected", get(|| async { "selected" }))
        .layer(StreamAdmissionLayer::from_state(selected_state));
    let app = controller.merge(native_old).merge(native_selected);
    let first_app = app.clone();
    let first = tokio::spawn(async move { first_app.oneshot(request("/shared-stream/controller")).await.unwrap() });
    let started_permit = checked(started.acquire()).await.unwrap();
    started_permit.forget();
    assert_eq!(
        checked(app.clone().oneshot(request("/selected")))
            .await
            .unwrap()
            .status(),
        StatusCode::SERVICE_UNAVAILABLE
    );
    assert_eq!(
        checked(app.clone().oneshot(request("/old"))).await.unwrap().status(),
        StatusCode::OK
    );
    release.add_permits(1);
    let response = checked(first).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
}
