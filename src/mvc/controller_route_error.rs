// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use std::fmt;

/// Error returned when Controller route declarations cannot be assembled.
///
/// [`DuplicateMethod`](Self::DuplicateMethod) identifies an identical method
/// and complete path, [`PatternConflict`](Self::PatternConflict) identifies
/// distinct templates that overlap, and [`InvalidPath`](Self::InvalidPath)
/// carries the route matcher's reason for rejecting a template.
#[derive(Clone, Debug, Eq, PartialEq)]
#[must_use]
pub enum ControllerRouteError {
    /// A Controller repeats a method and complete route path.
    DuplicateMethod {
        /// HTTP method repeated for the same complete path.
        method: &'static str,
        /// Complete route path declared more than once for this method.
        path: &'static str,
    },
    /// Two distinct route templates can match the same request path.
    PatternConflict {
        /// Newly declared route template that conflicts.
        path: &'static str,
        /// Previously declared route template reported by the matcher.
        existing_path: String,
    },
    /// A route template is not valid for Axum's route matcher.
    InvalidPath {
        /// Route template rejected by the matcher.
        path: &'static str,
        /// Matcher-provided reason for rejecting the template.
        reason: String,
    },
}

impl fmt::Display for ControllerRouteError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::DuplicateMethod { method, path } => {
                write!(formatter, "duplicate Controller route: {method} {path}")
            }
            Self::PatternConflict { path, existing_path } => {
                write!(
                    formatter,
                    "Controller route pattern {path} conflicts with {existing_path}"
                )
            }
            Self::InvalidPath { path, reason } => {
                write!(formatter, "invalid Controller route path {path}: {reason}")
            }
        }
    }
}

impl std::error::Error for ControllerRouteError {}
