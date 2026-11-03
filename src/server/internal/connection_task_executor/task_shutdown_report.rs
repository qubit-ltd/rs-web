// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
/// Outcome of waiting for all registered Hyper tasks until the shutdown
/// deadline.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in super::super) struct TaskShutdownReport {
    /// Whether every registered task and its application resources completed
    /// by the deadline without forced abort.
    pub(in super::super) completed_by_deadline: bool,
}
