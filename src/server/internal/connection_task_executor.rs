// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Tracks the HTTP tasks submitted by Hyper for one server instance.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::Mutex;
use std::task::Context;
use std::task::Poll;

use hyper::rt::Executor;
use tokio::task::AbortHandle;
use tokio::task::JoinSet;
use tokio::time::Instant;

/// An executor that registers Hyper tasks with one server instance.
#[derive(Clone)]
pub(super) struct ConnectionTaskExecutor {
    state: Arc<Mutex<TaskExecutorState>>,
}

struct TaskExecutorState {
    accepting: bool,
    tasks: Option<JoinSet<()>>,
    active: usize,
    last_completed: Option<Instant>,
}

/// The result of waiting for all registered Hyper tasks to stop.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct TaskShutdownReport {
    pub(super) completed_by_deadline: bool,
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
    pub(super) fn spawn<F>(&self, future: F) -> Option<AbortHandle>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        let mut state = self.lock_state();
        if !state.accepting {
            return None;
        }

        if let Some(tasks) = state.tasks.as_mut() {
            while tasks.try_join_next().is_some() {}
        }
        state.active += 1;

        let tracked = TrackedFuture {
            future: Some(Box::pin(future)),
            completion: Some(TaskCompletionGuard {
                state: Arc::clone(&self.state),
            }),
        };
        Some(
            state
                .tasks
                .as_mut()
                .expect("an accepting executor has a task set")
                .spawn(tracked),
        )
    }

    /// Closes task admission and takes ownership of all registered tasks.
    pub(super) fn close(&self) -> TaskShutdown {
        let mut state = self.lock_state();
        state.accepting = false;
        let tasks = state.tasks.take().unwrap_or_default();
        TaskShutdown {
            tasks,
            state: Arc::clone(&self.state),
        }
    }

    fn lock_state(&self) -> std::sync::MutexGuard<'_, TaskExecutorState> {
        self.state.lock().unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

impl<F> Executor<F> for ConnectionTaskExecutor
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    fn execute(&self, future: F) {
        let _ = self.spawn(future);
    }
}

pub(super) struct TaskShutdown {
    tasks: JoinSet<()>,
    state: Arc<Mutex<TaskExecutorState>>,
}

impl TaskShutdown {
    /// Waits until every task exits, aborting remaining tasks at `deadline`.
    pub(super) async fn wait_until(self, deadline: Instant) -> TaskShutdownReport {
        let Self { mut tasks, state } = self;
        let wait_result = tokio::time::timeout_at(deadline, async { while tasks.join_next().await.is_some() {} }).await;

        if wait_result.is_err() {
            let active = state.lock().unwrap_or_else(std::sync::PoisonError::into_inner).active;
            if active > 0 {
                tasks.abort_all();
            }
            while tasks.join_next().await.is_some() {}
        }

        let state = state.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        TaskShutdownReport {
            completed_by_deadline: state.active == 0
                && state.last_completed.is_none_or(|completed| completed <= deadline),
        }
    }
}

struct TrackedFuture<F> {
    future: Option<Pin<Box<F>>>,
    completion: Option<TaskCompletionGuard>,
}

impl<F> Future for TrackedFuture<F>
where
    F: Future,
{
    type Output = ();

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        match this
            .future
            .as_mut()
            .expect("tracked task future is present while it is polled")
            .as_mut()
            .poll(context)
        {
            Poll::Ready(output) => {
                drop(output);
                this.future.take();
                this.completion.take();
                Poll::Ready(())
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

impl<F> Drop for TrackedFuture<F> {
    fn drop(&mut self) {
        self.future.take();
        self.completion.take();
    }
}

struct TaskCompletionGuard {
    state: Arc<Mutex<TaskExecutorState>>,
}

impl Drop for TaskCompletionGuard {
    fn drop(&mut self) {
        let mut state = self.state.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        state.active = state.active.saturating_sub(1);
        let completed = Instant::now();
        state.last_completed = Some(state.last_completed.map_or(completed, |latest| latest.max(completed)));
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
    use tokio::time::timeout;

    use super::ConnectionTaskExecutor;

    struct DropSignal(Arc<AtomicBool>);

    impl Drop for DropSignal {
        fn drop(&mut self) {
            self.0.store(true, Ordering::SeqCst);
        }
    }

    #[tokio::test]
    async fn normal_completion_is_joined_before_shutdown_returns() {
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
    async fn panic_is_joined_and_does_not_poison_executor_state() {
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
    async fn cancelled_task_releases_its_resources_before_shutdown_returns() {
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
    async fn deadline_aborts_and_joins_only_registered_tasks() {
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
    async fn registration_racing_shutdown_is_rejected_or_joined() {
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
