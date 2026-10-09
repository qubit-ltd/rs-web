// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use super::internal::InsertSlash;

/// Joins a Controller prefix and child template with exactly one separator.
///
/// Empty prefixes or children are handled without introducing duplicate
/// separators; a root prefix yields a leading slash for non-empty children.
///
/// # Parameters
///
/// * `prefix` - Controller-level route prefix.
/// * `child` - Method-level route template.
///
/// # Returns
///
/// The joined route template.
///
/// # Examples
///
/// ```
/// use qubit_web::mvc::join_route_path;
///
/// assert_eq!(join_route_path("/api/", "health"), "/api/health");
/// assert_eq!(join_route_path("/", "health"), "/health");
/// ```
#[must_use]
#[inline]
pub fn join_route_path(prefix: &str, child: &str) -> String {
    if prefix == "/" {
        if child.is_empty() {
            "/".to_owned()
        } else {
            child.trim_start_matches('/').to_owned().insert_slash()
        }
    } else if child.is_empty() {
        prefix.to_owned()
    } else {
        format!(
            "{}{}",
            prefix.trim_end_matches('/'),
            if child.starts_with('/') {
                child.to_owned()
            } else {
                format!("/{child}")
            }
        )
    }
}
