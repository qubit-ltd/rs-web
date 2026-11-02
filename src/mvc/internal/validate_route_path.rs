// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

/// Checks route path constraints shared by all manually declared controllers.
pub(in crate::mvc) fn validate_route_path(path: &str) -> Result<(), &'static str> {
    if path.is_empty() {
        return Err("route path must not be empty");
    }
    if !path.starts_with('/') {
        return Err("route path must be absolute");
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
