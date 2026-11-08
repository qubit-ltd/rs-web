// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::Arc;

use axum::Router;

use super::ControllerDefinition;
use super::ControllerRouteError;
use super::internal::validate_route_path;
use crate::limit::HttpLimits;
use crate::limit::LimitState;
use crate::limit::StreamAdmissionLimits;
use crate::limit::StreamAdmissionState;

/// Builds an Axum router from explicitly supplied Controller instances.
///
/// # Type Parameters
///
/// * `S` - Cloneable, thread-safe application state required by the router.
///
/// # Examples
///
/// An empty router can be assembled without declaring any controllers:
///
/// ```
/// use qubit_web::mvc::ControllerRoutes;
///
/// let router = ControllerRoutes::<()>::new().finish();
/// let _ = router;
/// ```
#[must_use]
pub struct ControllerRoutes<S = ()> {
    /// Axum router accumulated during assembly.
    router: Router<S>,
    /// Matcher containing every route template already checked by preflight.
    matcher: matchit::Router<()>,
    /// Methods already declared for each complete route template.
    declared: HashMap<&'static str, HashSet<&'static str>>,
    /// Shared limits passed to each registered controller.
    limit_state: LimitState,
    /// Shared admission budget passed to each SSE and WebSocket route.
    stream_state: StreamAdmissionState,
}

impl<S> Default for ControllerRoutes<S>
where
    S: Clone + Send + Sync + 'static,
{
    /// Returns the documented default policy.
    fn default() -> Self {
        Self::new()
    }
}

impl<S> ControllerRoutes<S>
where
    S: Clone + Send + Sync + 'static,
{
    /// Creates an empty Controller router. Short routes added to it use
    /// `HttpLimits::default()` (1 MiB body, 256 concurrent requests, and a
    /// 30-second timeout) until `with_http_limits` supplies a replacement
    /// policy.
    ///
    /// # Examples
    ///
    /// ```
    /// use qubit_web::mvc::ControllerRoutes;
    ///
    /// let router = ControllerRoutes::<()>::new().finish();
    /// let _ = router;
    /// ```
    pub fn new() -> Self {
        Self {
            router: Router::new(),
            matcher: matchit::Router::new(),
            declared: HashMap::new(),
            limit_state: LimitState::new(HttpLimits::default()),
            stream_state: StreamAdmissionState::new(StreamAdmissionLimits::default()),
        }
    }

    /// Replaces the default finite limits for all short routes added to this
    /// builder afterward.
    ///
    /// SSE and WebSocket routes use their separate stream-admission limits.
    /// Configure this before adding Controllers so their short routes share
    /// one concurrency limit.
    ///
    /// # Parameters
    ///
    /// * `http_limits` - Replacement policy shared by short routes added
    ///   afterward.
    ///
    /// # Returns
    ///
    /// The same builder with the replacement policy installed.
    pub fn with_http_limits(mut self, http_limits: HttpLimits) -> Self {
        self.limit_state = LimitState::new(http_limits);
        self
    }

    /// Uses an existing short-request capacity domain for Controllers added
    /// afterward.
    ///
    /// Clone the state to share its body, deadline, and concurrency budgets
    /// with native Axum routes or another [`ControllerRoutes`] builder. The
    /// state is captured when [`Self::add`] registers each Controller:
    /// replacing it later affects only Controllers added afterward. The
    /// most recently called same-kind setter selects the state for
    /// subsequent registrations. A separately constructed state remains an
    /// independent domain, and this short-request domain is independent of
    /// stream admission.
    ///
    /// The concurrency permit covers handler processing and response-body
    /// streaming. This capacity is not a pool of cumulative request-body bytes
    /// and does not impose a service-wide absolute deadline.
    ///
    /// # Parameters
    ///
    /// * `state` - Existing short-request state, usually cloned for native
    ///   routes using [`crate::RequestLimitLayer::from_state`].
    ///
    /// # Returns
    ///
    /// This builder with `state` installed for Controllers added afterward.
    ///
    /// # Example
    ///
    /// ```
    /// use std::sync::Arc;
    /// use axum::Router;
    /// use axum::routing::get;
    /// use qubit_web::HttpLimits;
    /// use qubit_web::RequestLimitLayer;
    /// use qubit_web::limit::LimitState;
    /// use qubit_web::mvc::ControllerRoutes;
    /// use qubit_web::rest_controller;
    ///
    /// struct Health;
    /// #[rest_controller("/health")]
    /// impl Health {
    ///     #[get_mapping("")]
    ///     async fn get(&self) -> &'static str { "ok" }
    /// }
    ///
    /// let state = LimitState::new(HttpLimits::default());
    /// let controllers = ControllerRoutes::<()>::new()
    ///     .with_limit_state(state.clone())
    ///     .add(Arc::new(Health))?
    ///     .finish();
    /// let native: Router<()> = Router::new()
    ///     .route("/native", get(|| async { "ok" }))
    ///     .layer(RequestLimitLayer::from_state(state));
    /// let app = controllers.merge(native);
    /// # let _ = app;
    /// # Ok::<(), qubit_web::mvc::ControllerRouteError>(())
    /// ```
    pub fn with_limit_state(mut self, state: LimitState) -> Self {
        self.limit_state = state;
        self
    }

    /// Replaces the default admission budgets for SSE and WebSocket routes
    /// added to this builder afterward.
    ///
    /// Each builder has its own shared admission capacity domain. This policy
    /// is independent from the short-route policy configured by
    /// [`Self::with_http_limits`].
    /// # Parameters
    ///
    /// * `limits` - Stream admission capacity and timeout policy for this
    ///   router.
    pub fn with_stream_admission_limits(mut self, limits: StreamAdmissionLimits) -> Self {
        self.stream_state = StreamAdmissionState::new(limits);
        self
    }

    /// Uses an existing stream-admission capacity domain for Controllers added
    /// afterward.
    ///
    /// Clone the state to share its body, deadline, and admission capacity with
    /// native Axum routes or another [`ControllerRoutes`] builder. The state is
    /// captured when [`Self::add`] registers each Controller: replacing it
    /// later affects only Controllers added afterward. The most recently
    /// called same-kind setter selects the state for subsequent
    /// registrations. A separately constructed state remains independent,
    /// and stream admission is separate from short-request capacity.
    ///
    /// A permit covers request buffering and handler execution up to response
    /// headers. It is released when headers are produced or the admission
    /// future is cancelled; it does not count ongoing SSE or WebSocket
    /// sessions.
    ///
    /// # Parameters
    ///
    /// * `state` - Existing admission state, usually cloned for native routes
    ///   using [`crate::StreamAdmissionLayer::from_state`].
    ///
    /// # Returns
    ///
    /// This builder with `state` installed for Controllers added afterward.
    ///
    /// # Example
    ///
    /// ```
    /// use std::sync::Arc;
    /// use axum::Router;
    /// use axum::routing::get;
    /// use qubit_web::StreamAdmissionLayer;
    /// use qubit_web::StreamAdmissionLimits;
    /// use qubit_web::StreamAdmissionState;
    /// use qubit_web::mvc::ControllerRoutes;
    /// use qubit_web::rest_controller;
    ///
    /// struct Events;
    /// #[rest_controller("/events")]
    /// impl Events {
    ///     #[route("", method = "GET", kind = "sse")]
    ///     async fn get(&self) -> &'static str { "event" }
    /// }
    ///
    /// let state = StreamAdmissionState::new(StreamAdmissionLimits::default());
    /// let controllers = ControllerRoutes::<()>::new()
    ///     .with_stream_admission_state(state.clone())
    ///     .add(Arc::new(Events))?
    ///     .finish();
    /// let native: Router<()> = Router::new()
    ///     .route("/native-events", get(|| async { "event" }))
    ///     .layer(StreamAdmissionLayer::from_state(state));
    /// let app = controllers.merge(native);
    /// # let _ = app;
    /// # Ok::<(), qubit_web::mvc::ControllerRouteError>(())
    /// ```
    pub fn with_stream_admission_state(mut self, state: StreamAdmissionState) -> Self {
        self.stream_state = state;
        self
    }

    /// Adds all routes declared by one Controller instance.
    ///
    /// # Parameters
    ///
    /// * `controller` - Shared controller instance used by registered handlers.
    ///
    /// # Type Parameters
    ///
    /// * `C` - Controller type generated by the controller attribute macro.
    ///
    /// # Returns
    ///
    /// The updated builder after every route declaration passes preflight.
    ///
    /// This method consumes the builder, including when preflight returns an
    /// error. Controller routes are registered only after all of that
    /// controller's metadata has passed preflight.
    ///
    /// # Errors
    ///
    /// Returns [`ControllerRouteError::DuplicateMethod`] for an exact duplicate
    /// method and path, [`ControllerRouteError::PatternConflict`] for distinct
    /// templates that match the same paths, or
    /// [`ControllerRouteError::InvalidPath`] for an invalid route template,
    /// including a route with more than 25 non-wildcard parameters. Catch-all
    /// parameters do not count toward that limit. No routes are registered
    /// unless every declaration passes preflight.
    #[allow(clippy::should_implement_trait)]
    pub fn add<C>(mut self, controller: Arc<C>) -> Result<Self, ControllerRouteError>
    where
        C: ControllerDefinition<S>,
    {
        for route in C::route_metadata() {
            validate_route_path(route.path()).map_err(|reason| ControllerRouteError::InvalidPath {
                path: route.path(),
                reason: reason.to_owned(),
            })?;
            if self
                .declared
                .get(route.path())
                .is_some_and(|methods| methods.contains(route.method()))
            {
                return Err(ControllerRouteError::DuplicateMethod {
                    method: route.method(),
                    path: route.path(),
                });
            }
            if !self.declared.contains_key(route.path()) {
                self.matcher.insert(route.path(), ()).map_err(|error| match error {
                    matchit::InsertError::Conflict { with } => ControllerRouteError::PatternConflict {
                        path: route.path(),
                        existing_path: with,
                    },
                    error => ControllerRouteError::InvalidPath {
                        path: route.path(),
                        reason: error.to_string(),
                    },
                })?;
            }
            self.declared.entry(route.path()).or_default().insert(route.method());
        }
        self.router = C::register(
            controller,
            self.router,
            self.limit_state.clone(),
            self.stream_state.clone(),
        );
        Ok(self)
    }

    /// Finishes assembly and returns the Axum router.
    ///
    /// # Returns
    ///
    /// The assembled router.
    pub fn finish(self) -> Router<S> {
        self.router
    }
}
