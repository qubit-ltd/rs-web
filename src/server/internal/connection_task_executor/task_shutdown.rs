// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use std::sync::Arc;
use std::sync::Mutex;

use tokio::task::JoinSet;
use tokio::time::Instant;

use super::super::connection_diagnostics::record_task_result;
use super::TaskShutdownReport;
use super::task_executor_state::TaskExecutorState;

/// Owns the task set removed from an executor when shutdown closes admission.
///
/// Moving the set into this value releases the executor-state lock before
/// tasks are joined or aborted. Repeated executor closes produce an empty set
/// while retaining the same completion state.
pub(in super::super) struct TaskShutdown {
    /// Tasks transferred from the executor when admission closed.
    pub(super) tasks: JoinSet<()>,
    /// Shared completion counters retained after ownership transfer.
    pub(super) state: Arc<Mutex<TaskExecutorState>>,
}

impl TaskShutdown {
    /// Waits for registered tasks until `deadline`, then aborts and reaps any
    /// remaining tasks.
    ///
    /// The report distinguishes completion by the deadline from forced
    /// cancellation. Aborting tasks releases their futures, but does not await
    /// work spawned independently by an application.
    /// # Parameters
    ///
    /// * `deadline` - Optional absolute instant that bounds this operation.
    pub(in super::super) async fn wait_until(self, deadline: Instant) -> TaskShutdownReport {
        let Self { mut tasks, state } = self;
        let mut failed_tasks = Vec::new();
        let wait_result = tokio::time::timeout_at(deadline, async {
            while let Some(result) = tasks.join_next().await {
                if let Err(error) = result {
                    failed_tasks.push(error);
                }
            }
        })
        .await;

        if wait_result.is_err() {
            let active = state.lock().unwrap_or_else(std::sync::PoisonError::into_inner).active;
            if active > 0 {
                tasks.abort_all();
            }
            while let Some(result) = tasks.join_next().await {
                if let Err(error) = result {
                    failed_tasks.push(error);
                }
            }
        }

        for error in failed_tasks {
            record_task_result(Err(error), "hyper");
        }

        let state = state.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        TaskShutdownReport {
            completed_by_deadline: state.active == 0
                && state.last_completed.is_none_or(|completed| completed <= deadline),
        }
    }
}
