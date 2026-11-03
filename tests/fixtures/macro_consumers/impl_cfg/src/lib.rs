// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use qubit_web::rest_controller;

#[cfg(feature = "impl-enabled")]
type MissingWhenDisabled = axum::extract::Extension<()>;

struct CfgBefore;

#[cfg(feature = "impl-enabled")]
#[rest_controller("/cfg-before")]
impl CfgBefore {
    #[get_mapping("")]
    async fn route(&self, _: MissingWhenDisabled) -> &'static str {
        "ok"
    }
    #[get_mapping("/health")]
    async fn health(&self) -> &'static str {
        "healthy"
    }
}

struct CfgAfter;

#[rest_controller("/cfg-after")]
#[cfg(feature = "impl-enabled")]
impl CfgAfter {
    #[get_mapping("")]
    async fn route(&self, _: MissingWhenDisabled) -> &'static str {
        "ok"
    }
    #[get_mapping("/health")]
    async fn health(&self) -> &'static str {
        "healthy"
    }
}

struct CfgAttrBefore;

#[cfg_attr(not(feature = "impl-enabled"), cfg(any()))]
#[rest_controller("/cfg-attr-before")]
impl CfgAttrBefore {
    #[get_mapping("")]
    async fn route(&self, _: MissingWhenDisabled) -> &'static str {
        "ok"
    }
    #[get_mapping("/health")]
    async fn health(&self) -> &'static str {
        "healthy"
    }
}

struct CfgAttrAfter;

#[rest_controller("/cfg-attr-after")]
#[cfg_attr(not(feature = "impl-enabled"), cfg(any()))]
impl CfgAttrAfter {
    #[get_mapping("")]
    async fn route(&self, _: MissingWhenDisabled) -> &'static str {
        "ok"
    }
    #[get_mapping("/health")]
    async fn health(&self) -> &'static str {
        "healthy"
    }
}

#[cfg(all(test, feature = "impl-enabled"))]
mod tests {
    use std::sync::Arc;

    use qubit_web::mvc::ControllerDefinition;
    use tower::ServiceExt;

    use super::CfgAfter;
    use super::CfgAttrAfter;
    use super::CfgAttrBefore;
    use super::CfgBefore;

    #[tokio::test]
    async fn enabled_impl_conditions_produce_requestable_routes() {
        assert_eq!(<CfgBefore as ControllerDefinition<()>>::route_metadata().len(), 2);
        assert_eq!(<CfgAfter as ControllerDefinition<()>>::route_metadata().len(), 2);
        assert_eq!(<CfgAttrBefore as ControllerDefinition<()>>::route_metadata().len(), 2);
        assert_eq!(<CfgAttrAfter as ControllerDefinition<()>>::route_metadata().len(), 2);

        let router = qubit_web::ControllerRoutes::<()>::new()
            .add(Arc::new(CfgBefore))
            .unwrap()
            .add(Arc::new(CfgAfter))
            .unwrap()
            .add(Arc::new(CfgAttrBefore))
            .unwrap()
            .add(Arc::new(CfgAttrAfter))
            .unwrap()
            .finish();
        let response = router
            .oneshot(
                axum::http::Request::get("/cfg-before/health")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), axum::http::StatusCode::OK);
    }
}
