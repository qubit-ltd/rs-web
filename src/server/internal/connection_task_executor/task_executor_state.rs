// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use tokio::task::JoinSet;
use tokio::time::Instant;

/// Shared mutable admission, task ownership, and completion state.
///
/// Access is serialized by the executor mutex. Callers hold that lock only
/// while changing registration or reading completion counters; task futures
/// and their drop handlers must be destroyed after the lock is released.
pub(super) struct TaskExecutorState {
    /// Whether new Hyper tasks may be registered.
    pub(super) accepting: bool,
    /// The registered tasks; `None` means shutdown has taken ownership.
    pub(super) tasks: Option<JoinSet<()>>,
    /// Number of registered tasks whose completion guard has not dropped.
    pub(super) active: usize,
    /// Latest time an application future and output were released.
    pub(super) last_completed: Option<Instant>,
}
