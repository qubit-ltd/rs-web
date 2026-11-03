// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use super::internal::InsertSlash;

/// Joins a Controller prefix and child template using boundary-specific slash
/// handling.
///
/// If `prefix` is `/`, all leading slashes are removed from a non-empty
/// `child`, then one leading slash is added. An empty `child` returns `/`.
/// For any other prefix, an empty `child` returns `prefix` unchanged,
/// including any trailing slashes. Otherwise, trailing slashes are removed
/// from `prefix`; a `child` that starts with `/` is appended unchanged, and
/// any other `child` is joined with one slash. This helper does not validate
/// or normalize internal repeated slashes.
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
/// assert_eq!(join_route_path("/api/", "//health"), "/api//health");
/// assert_eq!(join_route_path("/", "//health"), "/health");
/// assert_eq!(join_route_path("/api//", ""), "/api//");
/// assert_eq!(join_route_path("", ""), "");
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
