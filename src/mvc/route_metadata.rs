// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use crate::RouteKind;

/// One statically declared route produced by a Controller implementation.
///
/// # Examples
///
/// ```
/// use qubit_web::mvc::RouteMetadata;
/// use qubit_web::RouteKind;
///
/// let route = RouteMetadata::new("GET", "/health", RouteKind::Short);
/// assert_eq!(route.path(), "/health");
/// ```
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
#[must_use]
pub struct RouteMetadata {
    /// HTTP method used to identify conflicts and register the route.
    method: &'static str,
    /// Normalized route template used by Axum.
    path: &'static str,
    /// Whether the route is long-lived and exempt from short-request limits.
    kind: RouteKind,
}

impl RouteMetadata {
    /// Creates route metadata for generated Controller code.
    ///
    /// # Parameters
    ///
    /// * `method` - Static HTTP method name.
    /// * `path` - Static normalized route template.
    /// * `kind` - Classification controlling request-limit treatment.
    ///
    /// # Examples
    ///
    /// ```
    /// use qubit_web::mvc::RouteMetadata;
    /// use qubit_web::RouteKind;
    ///
    /// let route = RouteMetadata::new("GET", "/health", RouteKind::Short);
    /// assert_eq!(route.method(), "GET");
    /// ```
    pub const fn new(method: &'static str, path: &'static str, kind: RouteKind) -> Self {
        Self { method, path, kind }
    }

    /// Returns the HTTP method name.
    #[must_use]
    #[inline]
    pub const fn method(&self) -> &'static str {
        self.method
    }

    /// Returns the normalized route template.
    #[must_use]
    #[inline]
    pub const fn path(&self) -> &'static str {
        self.path
    }

    /// Returns the route's long-lived or short-request classification.
    #[inline]
    pub const fn kind(&self) -> RouteKind {
        self.kind
    }
}
