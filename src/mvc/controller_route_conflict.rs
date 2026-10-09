// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use std::fmt;

/// Error returned when two Controllers declare the same method and template.
///
/// # Examples
///
/// A duplicate declaration is returned as a conflict:
///
/// ```
/// use std::sync::Arc;
///
/// use axum::Router;
/// use qubit_web::limit::LimitState;
/// use qubit_web::mvc::{ControllerDefinition, ControllerRoutes, RouteMetadata};
/// use qubit_web::RouteKind;
///
/// struct Health;
///
/// impl ControllerDefinition<()> for Health {
///     fn route_metadata() -> &'static [RouteMetadata] {
///         static ROUTES: [RouteMetadata; 1] = [
///             RouteMetadata::new("GET", "/health", RouteKind::Short),
///         ];
///         &ROUTES
///     }
///
///     fn register(self: Arc<Self>, router: Router<()>, _: LimitState) -> Router<()> {
///         router
///     }
/// }
///
/// let routes = ControllerRoutes::new().add(Arc::new(Health)).unwrap();
/// let conflict = match routes.add(Arc::new(Health)) {
///     Err(conflict) => conflict,
///     Ok(_) => panic!("duplicate route should be rejected"),
/// };
/// assert_eq!(conflict.method(), "GET");
/// ```
#[derive(Clone, Debug, Eq, PartialEq)]
#[must_use]
pub struct ControllerRouteConflict {
    /// HTTP method shared by the conflicting declarations.
    method: &'static str,
    /// Route template shared by the conflicting declarations.
    path: &'static str,
}

impl ControllerRouteConflict {
    pub(super) const fn new(method: &'static str, path: &'static str) -> Self {
        Self { method, path }
    }

    /// Returns the duplicated method.
    #[must_use]
    #[inline]
    pub const fn method(&self) -> &'static str {
        self.method
    }

    /// Returns the duplicated route template.
    #[must_use]
    #[inline]
    pub const fn path(&self) -> &'static str {
        self.path
    }
}

impl fmt::Display for ControllerRouteConflict {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "duplicate Controller route: {} {}", self.method, self.path)
    }
}

impl std::error::Error for ControllerRouteConflict {}
