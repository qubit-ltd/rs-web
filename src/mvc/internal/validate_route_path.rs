// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

/// Maximum supported count of named parameters in one route path.
const MAX_NON_WILDCARD_PARAMETERS: usize = 25;

/// Checks route path constraints shared by all manually declared controllers.
/// # Parameters
///
/// * `path` - Route path to validate for supported syntax and parameter count.
pub(in crate::mvc) fn validate_route_path(path: &str) -> Result<(), &'static str> {
    if path.is_empty() {
        return Err("route path must not be empty");
    }
    if !path.starts_with('/') {
        return Err("route path must be absolute");
    }
    if has_too_many_non_wildcard_parameters(path) {
        return Err("route path supports at most 25 non-wildcard parameters");
    }
    if path
        .split('/')
        .any(|segment| segment.starts_with(':') || segment.starts_with('*'))
    {
        return Err(
            "legacy route parameters are not supported; use `{name}` for parameters and `{*name}` for wildcards",
        );
    }
    Ok(())
}

/// Counts unescaped named parameters without slicing at potentially non-UTF-8
/// boundaries.
/// # Parameters
///
/// * `path` - Route template whose non-wildcard parameters are counted.
fn has_too_many_non_wildcard_parameters(path: &str) -> bool {
    let bytes = path.as_bytes();
    let mut index = 0;
    let mut count = 0;
    while index < bytes.len() {
        match bytes[index] {
            b'{' if bytes.get(index + 1) == Some(&b'{') => index += 2,
            b'}' if bytes.get(index + 1) == Some(&b'}') => index += 2,
            b'{' => {
                if bytes.get(index + 1) != Some(&b'*') {
                    count += 1;
                    if count > MAX_NON_WILDCARD_PARAMETERS {
                        return true;
                    }
                }
                index += 1;
            }
            _ => index += 1,
        }
    }
    false
}
