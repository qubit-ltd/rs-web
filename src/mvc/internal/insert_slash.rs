// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
pub(in crate::mvc) trait InsertSlash {
    /// Adds a leading slash when the owned route fragment does not already have
    /// one.
    #[must_use]
    fn insert_slash(self) -> String;
}

impl InsertSlash for String {
    #[inline]
    fn insert_slash(self) -> String {
        if self.starts_with('/') {
            self
        } else {
            format!("/{self}")
        }
    }
}
