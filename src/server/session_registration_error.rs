// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use std::fmt;

/// Reports why a managed session could not be registered.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SessionRegistrationError {
    /// The server has begun shutting down and no longer accepts sessions.
    ShuttingDown,
}

impl fmt::Display for SessionRegistrationError {
    /// Formats the stable registration error message.
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ShuttingDown => formatter.write_str("server is shutting down"),
        }
    }
}

impl std::error::Error for SessionRegistrationError {}
