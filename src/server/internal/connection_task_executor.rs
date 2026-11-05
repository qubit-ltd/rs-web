// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Tracks the HTTP tasks submitted by Hyper for one server instance.
//!
//! The state, task shutdown operation, shutdown report, tracked future, and
//! completion guard are kept in this module's local submodules. Keeping those
//! ownership boundaries beside the executor makes lock and drop ordering
//! explicit without expanding the server's public API.

use std::future::Future;
use std::sync::Arc;
use std::sync::Mutex;

use hyper::rt::Executor;
use tokio::task::AbortHandle;
use tokio::task::JoinSet;

use super::connection_diagnostics::record_task_result;
#[path = "connection_task_executor/task_completion_guard.rs"]
mod task_completion_guard;
#[path = "connection_task_executor/task_executor_state.rs"]
mod task_executor_state;
#[path = "connection_task_executor/task_shutdown.rs"]
mod task_shutdown;
#[path = "connection_task_executor/task_shutdown_report.rs"]
mod task_shutdown_report;
#[path = "connection_task_executor/tracked_future.rs"]
mod tracked_future;

use task_completion_guard::TaskCompletionGuard;
use task_executor_state::TaskExecutorState;
pub(super) use task_shutdown::TaskShutdown;
pub(super) use task_shutdown_report::TaskShutdownReport;
use tracked_future::TrackedFuture;

/// An executor that registers Hyper tasks with one server instance.
#[derive(Clone)]
pub(super) struct ConnectionTaskExecutor {
    /// Shared admission and task-completion state guarded for short updates.
    state: Arc<Mutex<TaskExecutorState>>,
}

impl ConnectionTaskExecutor {
    /// Creates an executor that accepts tasks until shutdown closes admission.
    pub(super) fn new() -> Self {
        Self {
            state: Arc::new(Mutex::new(TaskExecutorState {
                accepting: true,
                tasks: Some(JoinSet::new()),
                active: 0,
                last_completed: None,
            })),
        }
    }

    /// Submits one Hyper task, returning `None` after shutdown closes
    /// admission.
    ///
    /// Admission and shutdown are serialized by the state lock, so a task is
    /// either registered in the owned `JoinSet` or rejected. Reaped task
    /// futures are destroyed after the registration lock is released.
    /// # Type Parameters
    ///
    /// * `F` - Application future type submitted to the executor.
    /// # Parameters
    ///
    /// * `future` - Application future submitted to the connection executor.
    pub(super) fn spawn<F>(&self, future: F) -> Option<AbortHandle>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        let mut state = self.lock_state();
        if !state.accepting {
            return None;
        }

        let mut failed_tasks = Vec::new();
        if let Some(tasks) = state.tasks.as_mut() {
            while let Some(result) = tasks.try_join_next() {
                if let Err(error) = result {
                    failed_tasks.push(error);
                }
            }
        }
        state.active += 1;

        let tracked = TrackedFuture {
            future: Some(Box::pin(future)),
            completion: Some(TaskCompletionGuard {
                state: Arc::clone(&self.state),
            }),
        };
        let abort_handle = state
            .tasks
            .as_mut()
            .expect("an accepting executor has a task set")
            .spawn(tracked);
        drop(state);
        for error in failed_tasks {
            record_task_result(Err(error), "hyper");
        }
        Some(abort_handle)
    }

    /// Closes task admission and takes ownership of all registered tasks.
    ///
    /// The first call moves the `JoinSet` out of shared state. Later calls
    /// return an empty task set; the `accepting` flag remains closed. The set
    /// is dropped or awaited only after the state lock has been released.
    pub(super) fn close(&self) -> TaskShutdown {
        let mut state = self.lock_state();
        state.accepting = false;
        let tasks = state.tasks.take().unwrap_or_default();
        TaskShutdown {
            tasks,
            state: Arc::clone(&self.state),
        }
    }

    /// Locks executor state for a short state update or snapshot.
    ///
    /// Callers must move task sets out of the guard before destroying or
    /// awaiting them, because dropping a future can run application code.
    fn lock_state(&self) -> std::sync::MutexGuard<'_, TaskExecutorState> {
        self.state.lock().unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

impl<F> Executor<F> for ConnectionTaskExecutor
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    /// Submits the future to the tracked connection executor.
    /// # Parameters
    ///
    /// * `future` - Connection task registered for completion tracking.
    fn execute(&self, future: F) {
        let _ = self.spawn(future);
    }
}

#[cfg(test)]
mod tests {
    use std::future::pending;
    use std::sync::Arc;
    use std::sync::atomic::AtomicBool;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;
    use std::time::Duration;

    use tokio::sync::Barrier;
    use tokio::sync::Notify;
    use tokio::sync::oneshot;
    use tokio::time::timeout;
    use tracing::instrument::WithSubscriber;

    use super::super::connection_diagnostics::tests::SharedLog;
    use super::super::connection_diagnostics::tests::diagnostic_subscriber;
    use super::ConnectionTaskExecutor;

    struct DropSignal(Arc<AtomicBool>);

    impl Drop for DropSignal {
        fn drop(&mut self) {
            self.0.store(true, Ordering::SeqCst);
        }
    }

    #[tokio::test]
    async fn test_normal_completion_is_joined_before_shutdown_returns() {
        let executor = ConnectionTaskExecutor::new();
        let dropped = Arc::new(AtomicBool::new(false));
        let task_dropped = dropped.clone();
        executor
            .spawn(async move {
                let _drop_signal = DropSignal(task_dropped);
            })
            .expect("executor should accept tasks before shutdown");

        let report = executor
            .close()
            .wait_until(tokio::time::Instant::now() + Duration::from_secs(1))
            .await;

        assert!(report.completed_by_deadline);
        assert!(dropped.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn test_panic_is_joined_and_does_not_poison_executor_state() {
        let executor = ConnectionTaskExecutor::new();
        executor
            .spawn(async { panic!("tracked Hyper task panic") })
            .expect("executor should accept tasks before shutdown");

        let report = executor
            .close()
            .wait_until(tokio::time::Instant::now() + Duration::from_secs(1))
            .await;

        assert!(report.completed_by_deadline);
        assert!(executor.spawn(async {}).is_none());
    }

    #[tokio::test]
    async fn test_opportunistic_reaping_reports_panic_once_outside_state_lock() {
        let log = SharedLog::default();
        let dispatch = diagnostic_subscriber(&log);
        async {
            let executor = ConnectionTaskExecutor::new();
            let (panicked_tx, panicked_rx) = oneshot::channel();
            executor
                .spawn(async move {
                    struct SignalOnDrop(Option<oneshot::Sender<()>>);

                    impl Drop for SignalOnDrop {
                        fn drop(&mut self) {
                            let _ = self.0.take().expect("panic signal is sent once").send(());
                        }
                    }

                    let _signal = SignalOnDrop(Some(panicked_tx));
                    panic!("executor-opportunistic-panic-sentinel");
                })
                .expect("executor should accept task before shutdown");
            panicked_rx.await.expect("panic task reached unwinding");

            executor.spawn(async {}).expect("executor should accept task");
            let after_reaping = log.contents();
            assert_eq!(
                after_reaping.matches("connection_task_panic").count(),
                1,
                "{after_reaping}"
            );
            assert!(after_reaping.contains("task_kind=\"hyper\""), "{after_reaping}");

            let _report = executor
                .close()
                .wait_until(tokio::time::Instant::now() + Duration::from_secs(1))
                .await;
            let after_close = log.contents();
            assert_eq!(after_close.matches("connection_task_panic").count(), 1, "{after_close}");
            assert!(
                !after_close.contains("executor-opportunistic-panic-sentinel"),
                "{after_close}"
            );
        }
        .with_subscriber(dispatch)
        .await;
    }

    #[tokio::test]
    async fn test_shutdown_drain_reports_panic_but_not_deadline_cancellation() {
        let log = SharedLog::default();
        let dispatch = diagnostic_subscriber(&log);
        async {
            let executor = ConnectionTaskExecutor::new();
            let (panicked_tx, panicked_rx) = oneshot::channel();
            executor
                .spawn(async move {
                    struct SignalOnDrop(Option<oneshot::Sender<()>>);

                    impl Drop for SignalOnDrop {
                        fn drop(&mut self) {
                            let _ = self.0.take().expect("panic signal is sent once").send(());
                        }
                    }

                    let _signal = SignalOnDrop(Some(panicked_tx));
                    panic!("executor-shutdown-panic-sentinel");
                })
                .expect("executor should accept panicking task");
            executor
                .spawn(pending::<()>())
                .expect("executor should accept pending task");
            panicked_rx.await.expect("panic task reached unwinding");

            let report = executor
                .close()
                .wait_until(tokio::time::Instant::now() + Duration::from_millis(20))
                .await;
            assert!(!report.completed_by_deadline);
            let output = log.contents();
            assert_eq!(output.matches("connection_task_panic").count(), 1, "{output}");
            assert!(output.contains("task_kind=\"hyper\""), "{output}");
            assert!(!output.contains("executor-shutdown-panic-sentinel"), "{output}");
        }
        .with_subscriber(dispatch)
        .await;
    }

    #[tokio::test]
    async fn test_cancelled_task_releases_its_resources_before_shutdown_returns() {
        let executor = ConnectionTaskExecutor::new();
        let entered = Arc::new(Notify::new());
        let task_entered = entered.clone();
        let dropped = Arc::new(AtomicBool::new(false));
        let task_dropped = dropped.clone();
        let handle = executor
            .spawn(async move {
                let _drop_signal = DropSignal(task_dropped);
                task_entered.notify_one();
                pending::<()>().await;
            })
            .expect("executor should accept tasks before shutdown");
        timeout(Duration::from_secs(1), entered.notified())
            .await
            .expect("tracked task should enter");
        handle.abort();

        let report = executor
            .close()
            .wait_until(tokio::time::Instant::now() + Duration::from_secs(1))
            .await;

        assert!(report.completed_by_deadline);
        assert!(dropped.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn test_deadline_aborts_and_joins_only_registered_tasks() {
        let executor = ConnectionTaskExecutor::new();
        let entered = Arc::new(Notify::new());
        let task_entered = entered.clone();
        let dropped = Arc::new(AtomicBool::new(false));
        let task_dropped = dropped.clone();
        executor
            .spawn(async move {
                let _drop_signal = DropSignal(task_dropped);
                task_entered.notify_one();
                pending::<()>().await;
            })
            .expect("executor should accept tasks before shutdown");
        timeout(Duration::from_secs(1), entered.notified())
            .await
            .expect("tracked task should enter");

        let report = executor
            .close()
            .wait_until(tokio::time::Instant::now() + Duration::from_millis(20))
            .await;

        assert!(!report.completed_by_deadline);
        assert!(dropped.load(Ordering::SeqCst));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_registration_racing_shutdown_is_rejected_or_joined() {
        const PRODUCERS: usize = 16;

        struct CountDrop(Arc<AtomicUsize>);

        impl Drop for CountDrop {
            fn drop(&mut self) {
                self.0.fetch_add(1, Ordering::SeqCst);
            }
        }

        let executor = ConnectionTaskExecutor::new();
        let entered = Arc::new(Notify::new());
        let task_entered = entered.clone();
        let dropped = Arc::new(AtomicUsize::new(0));
        let initial_drop = CountDrop(dropped.clone());
        executor
            .spawn(async move {
                let _drop_signal = initial_drop;
                task_entered.notify_one();
                pending::<()>().await;
            })
            .expect("executor should accept the initial task");
        timeout(Duration::from_secs(1), entered.notified())
            .await
            .expect("initial tracked task should enter");

        let barrier = Arc::new(Barrier::new(PRODUCERS + 1));
        let producers: Vec<_> = (0..PRODUCERS)
            .map(|_| {
                let executor = executor.clone();
                let barrier = barrier.clone();
                let dropped = dropped.clone();
                tokio::spawn(async move {
                    barrier.wait().await;
                    let task_drop = CountDrop(dropped.clone());
                    executor
                        .spawn(async move {
                            let _drop_signal = task_drop;
                            pending::<()>().await;
                        })
                        .is_some()
                })
            })
            .collect();
        barrier.wait().await;

        let report = executor
            .close()
            .wait_until(tokio::time::Instant::now() + Duration::from_millis(20))
            .await;
        let mut accepted = 0;
        for producer in producers {
            accepted += usize::from(producer.await.unwrap());
        }

        assert!(!report.completed_by_deadline);
        assert!(accepted <= PRODUCERS);
        assert_eq!(dropped.load(Ordering::SeqCst), PRODUCERS + 1);
        assert!(executor.spawn(async {}).is_none());
    }
}
