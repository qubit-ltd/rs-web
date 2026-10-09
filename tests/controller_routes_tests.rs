// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::time::Duration;

use axum::Router;
use axum::body::Body;
use axum::body::Bytes;
use axum::extract::Extension;
use axum::extract::Path;
use axum::extract::Query;
use axum::extract::State;
use axum::http::Method;
use axum::http::Request;
use axum::http::StatusCode;
use axum::routing::get;
use qubit_web::HttpLimits;
use qubit_web::mvc::ControllerRouteError;
use qubit_web::mvc::ControllerRoutes;
use qubit_web::rest_controller;
use tokio::sync::Semaphore;
use tower::ServiceExt;

static INVALID_PATH_REGISTER_CALLED: AtomicBool = AtomicBool::new(false);

#[derive(Clone)]
struct AppState(&'static str);

struct Users {
    prefix: String,
}

#[rest_controller("/users")]
impl Users {
    #[get_mapping("/{id}")]
    async fn show(
        &self,
        State(state): State<AppState>,
        Path(id): Path<u64>,
        Query(params): Query<HashMap<String, String>>,
        Extension(extra): Extension<&'static str>,
    ) -> String {
        format!("{}:{}:{id}:{}:{extra}", self.prefix, state.0, params["suffix"])
    }

    #[route("/events", method = "GET", kind = "sse")]
    async fn events(&self) -> &'static str {
        "events"
    }

    #[route("/slow-events", method = "GET", kind = "sse")]
    async fn slow_events(&self) -> &'static str {
        tokio::time::sleep(Duration::from_millis(25)).await;
        "events"
    }

    #[route("/slow-socket", method = "GET", kind = "ws")]
    async fn slow_socket(&self) -> &'static str {
        tokio::time::sleep(Duration::from_millis(25)).await;
        "socket"
    }

    #[post_mapping("/short-payload")]
    async fn short_payload(&self, body: Bytes) -> String {
        String::from_utf8_lossy(&body).into_owned()
    }

    #[route("/event-payload", method = "POST", kind = "sse")]
    async fn event_payload(&self, body: Bytes) -> String {
        String::from_utf8_lossy(&body).into_owned()
    }

    #[route("/socket-payload", method = "POST", kind = "ws")]
    async fn socket_payload(&self, body: Bytes) -> String {
        String::from_utf8_lossy(&body).into_owned()
    }

    #[qubit_web::get("/short-alias")]
    async fn short_alias(&self) -> &'static str {
        "short-alias"
    }
}

struct BlockingController {
    started: Arc<Semaphore>,
    release: Arc<Semaphore>,
}

#[rest_controller("/blocking")]
impl BlockingController {
    async fn block(&self) {
        self.started.add_permits(1);
        self.release.acquire().await.unwrap().forget();
    }

    #[get_mapping("/one")]
    async fn one(&self) -> &'static str {
        self.block().await;
        "one"
    }

    #[get_mapping("/two")]
    async fn two(&self) -> &'static str {
        self.block().await;
        "two"
    }
}

#[tokio::test]
async fn controller_short_routes_enforce_configured_body_limit_but_stream_routes_skip_it() {
    let users = Arc::new(Users {
        prefix: "user".to_owned(),
    });
    let limits = HttpLimits::default().with_max_body_bytes(1).unwrap();
    let controller = ControllerRoutes::new()
        .with_http_limits(limits)
        .add(users)
        .unwrap()
        .finish();
    let app = Router::new().merge(controller).with_state(AppState("state"));

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri("/users/short-payload")
                .body(Body::from("xx"))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);

    for path in ["/users/event-payload", "/users/socket-payload"] {
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri(path)
                    .body(Body::from("xx"))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK, "{path}");
        let body = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
        assert_eq!(&body[..], b"xx", "{path}");
    }
}

#[tokio::test]
async fn controller_stream_routes_skip_short_request_deadline() {
    let controller = ControllerRoutes::new()
        .with_http_limits(
            HttpLimits::default()
                .with_request_timeout(Duration::from_millis(5))
                .unwrap(),
        )
        .add(Arc::new(Users {
            prefix: "user".to_owned(),
        }))
        .unwrap()
        .finish();
    let app = Router::new().merge(controller).with_state(AppState("state"));

    for path in ["/users/slow-events", "/users/slow-socket"] {
        let response = app
            .clone()
            .oneshot(Request::get(path).body(Body::empty()).unwrap())
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK, "{path}");
    }
}

#[tokio::test]
async fn controller_short_routes_apply_default_body_limit() {
    let controller = ControllerRoutes::new()
        .add(Arc::new(Users {
            prefix: "user".to_owned(),
        }))
        .unwrap()
        .finish();
    let app = Router::new().merge(controller).with_state(AppState("state"));
    let body = vec![b'x'; 1024 * 1024 + 1];
    let response = app
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri("/users/short-payload")
                .body(Body::from(body))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
}

#[tokio::test]
async fn short_controller_routes_share_the_configured_concurrency_limit() {
    let started = Arc::new(Semaphore::new(0));
    let release = Arc::new(Semaphore::new(0));
    let controller = Arc::new(BlockingController {
        started: started.clone(),
        release: release.clone(),
    });
    let routes: Router<AppState> = ControllerRoutes::new()
        .with_http_limits(HttpLimits::default().with_max_concurrent_requests(1).unwrap())
        .add(controller)
        .unwrap()
        .finish();
    let app = Router::new().merge(routes).with_state(AppState("state"));

    let first_app = app.clone();
    let first = tokio::spawn(async move {
        first_app
            .oneshot(Request::get("/blocking/one").body(Body::empty()).unwrap())
            .await
            .unwrap()
    });
    let permit = tokio::time::timeout(Duration::from_secs(1), started.acquire())
        .await
        .expect("the first route should enter its handler")
        .unwrap();
    permit.forget();

    let second = tokio::time::timeout(
        Duration::from_secs(1),
        app.oneshot(Request::get("/blocking/two").body(Body::empty()).unwrap()),
    )
    .await
    .expect("the second route should be rejected before its handler runs")
    .unwrap();
    assert_eq!(second.status(), StatusCode::SERVICE_UNAVAILABLE);

    release.add_permits(1);
    let first = tokio::time::timeout(Duration::from_secs(1), first)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(first.status(), StatusCode::OK);
}

#[tokio::test]
async fn controller_routes_mounts_controller_with_native_extractors() {
    let metadata = <Users as qubit_web::mvc::ControllerDefinition<AppState>>::route_metadata();
    assert!(
        metadata
            .iter()
            .any(|route| { route.path() == "/users/events" && route.kind() == qubit_web::RouteKind::Sse })
    );
    assert!(metadata.iter().any(|route| route.path() == "/users/short-alias"));

    let users = Arc::new(Users {
        prefix: "user".to_owned(),
    });
    let controller = ControllerRoutes::new().add(users).unwrap().finish();
    let app = Router::new()
        .route("/native", get(|| async { "native" }))
        .route("/state", get(|State(state): State<AppState>| async move { state.0 }))
        .merge(controller)
        .layer(Extension("extra"))
        .with_state(AppState("state"));

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/users/42?suffix=ok")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
    assert_eq!(&body[..], b"user:state:42:ok:extra");

    let wrong_method = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri("/users/42?suffix=ok")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(wrong_method.status(), StatusCode::METHOD_NOT_ALLOWED);

    let missing = app
        .oneshot(Request::builder().uri("/unknown").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(missing.status(), StatusCode::NOT_FOUND);
}

#[test]
fn route_path_join_normalizes_empty_child_and_root_prefix() {
    assert_eq!(qubit_web::mvc::join_route_path("/users", "/{id}"), "/users/{id}");
    assert_eq!(qubit_web::mvc::join_route_path("/users", ""), "/users");
    assert_eq!(qubit_web::mvc::join_route_path("/", "/health"), "/health");
    assert_eq!(qubit_web::mvc::join_route_path("/", ""), "/");

    let metadata = qubit_web::mvc::RouteMetadata::new("GET", "/health", qubit_web::RouteKind::Short);
    assert_eq!(metadata.method(), "GET");
    assert_eq!(metadata.path(), "/health");
    assert_eq!(metadata.kind(), qubit_web::RouteKind::Short);
}

#[tokio::test]
async fn duplicate_method_and_path_across_controllers_is_reported() {
    struct Duplicate;
    #[rest_controller("/users")]
    impl Duplicate {
        #[get_mapping("/{id}")]
        async fn show(&self, Path(_id): Path<u64>) -> &'static str {
            "duplicate"
        }
    }

    let result = ControllerRoutes::new()
        .add(Arc::new(Users {
            prefix: "one".to_owned(),
        }))
        .unwrap()
        .add(Arc::new(Duplicate));
    assert!(matches!(
        result,
        Err(ControllerRouteError::DuplicateMethod {
            method: "GET",
            path: "/users/{id}",
        })
    ));
}

#[test]
fn parameter_aliases_across_controllers_return_pattern_conflict_without_panicking() {
    struct NamedUser;
    #[rest_controller("/users")]
    impl NamedUser {
        #[get_mapping("/{name}")]
        async fn show(&self, Path(_name): Path<String>) -> &'static str {
            "user"
        }
    }

    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        ControllerRoutes::new()
            .add(Arc::new(Users {
                prefix: "one".to_owned(),
            }))
            .unwrap()
            .add(Arc::new(NamedUser))
    }));
    let Err(ControllerRouteError::PatternConflict { path, existing_path }) =
        result.expect("route addition should return a conflict instead of panicking")
    else {
        panic!("expected a parameter alias conflict");
    };
    assert_eq!(path, "/users/{name}");
    assert_eq!(existing_path, "/users/{id}");
}

#[test]
fn parameter_aliases_within_one_controller_return_pattern_conflict() {
    struct Aliases;
    #[rest_controller("/users")]
    impl Aliases {
        #[get_mapping("/{id}")]
        async fn by_id(&self, Path(_id): Path<String>) -> &'static str {
            "id"
        }
        #[post_mapping("/{name}")]
        async fn by_name(&self, Path(_name): Path<String>) -> &'static str {
            "name"
        }
    }

    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        ControllerRoutes::<()>::new().add(Arc::new(Aliases))
    }));
    let Err(ControllerRouteError::PatternConflict { path, existing_path }) =
        result.expect("route addition should return a conflict instead of panicking")
    else {
        panic!("expected an in-controller parameter alias conflict");
    };
    assert_eq!(path, "/users/{name}");
    assert_eq!(existing_path, "/users/{id}");
}

#[test]
fn wildcard_and_parameter_overlap_returns_pattern_conflict() {
    struct Wildcard;
    #[rest_controller("/files")]
    impl Wildcard {
        #[get_mapping("/{*rest}")]
        async fn rest(&self, Path(_rest): Path<HashMap<String, String>>) -> &'static str {
            "rest"
        }
    }
    struct File;
    #[rest_controller("/files")]
    impl File {
        #[post_mapping("/{id}")]
        async fn one(&self, Path(_id): Path<String>) -> &'static str {
            "file"
        }
    }

    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        ControllerRoutes::<()>::new()
            .add(Arc::new(Wildcard))
            .unwrap()
            .add(Arc::new(File))
    }));
    assert!(matches!(
        result.expect("route addition should not panic"),
        Err(ControllerRouteError::PatternConflict { .. })
    ));
}

#[test]
fn manually_declared_invalid_path_returns_invalid_path_error() {
    struct InvalidPath;

    impl qubit_web::mvc::ControllerDefinition<()> for InvalidPath {
        fn route_metadata() -> &'static [qubit_web::mvc::RouteMetadata] {
            static ROUTES: [qubit_web::mvc::RouteMetadata; 1] = [qubit_web::mvc::RouteMetadata::new(
                "GET",
                "/users/{id",
                qubit_web::RouteKind::Short,
            )];
            &ROUTES
        }

        fn register(self: Arc<Self>, router: Router<()>, _: qubit_web::limit::LimitState) -> Router<()> {
            INVALID_PATH_REGISTER_CALLED.store(true, Ordering::Relaxed);
            router
        }
    }

    INVALID_PATH_REGISTER_CALLED.store(false, Ordering::Relaxed);
    let result = ControllerRoutes::new().add(Arc::new(InvalidPath));
    let Err(ControllerRouteError::InvalidPath { path, reason }) = result else {
        panic!("invalid route metadata should be rejected");
    };
    assert_eq!(path, "/users/{id");
    assert!(!reason.is_empty());
    assert!(!INVALID_PATH_REGISTER_CALLED.load(Ordering::Relaxed));
}

#[test]
fn same_path_different_methods_and_static_parameter_paths_can_coexist() {
    struct PostUser;
    #[rest_controller("/users")]
    impl PostUser {
        #[post_mapping("/{id}")]
        async fn update(&self, Path(_id): Path<String>) -> &'static str {
            "updated"
        }
    }
    struct StaticUser;
    #[rest_controller("/users")]
    impl StaticUser {
        #[get_mapping("/current")]
        async fn current(&self) -> &'static str {
            "current"
        }
    }

    let router = ControllerRoutes::new()
        .add(Arc::new(Users {
            prefix: "one".to_owned(),
        }))
        .unwrap()
        .add(Arc::new(PostUser))
        .unwrap()
        .add(Arc::new(StaticUser))
        .unwrap()
        .finish();
    let _ = router;
}
