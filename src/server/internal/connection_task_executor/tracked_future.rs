// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use std::future::Future;
use std::panic::AssertUnwindSafe;
use std::panic::catch_unwind;
use std::panic::resume_unwind;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;

use tracing::Dispatch;

use super::task_completion_guard::TaskCompletionGuard;

/// Wraps an application future so task completion is recorded after its
/// resources and output have been dropped.
///
/// # Type Parameters
///
/// * `F` - Application future whose lifetime is tracked by the executor.
pub(super) struct TrackedFuture<F> {
    /// Pinned application work, released before the completion guard.
    pub(super) future: Option<Pin<Box<F>>>,
    /// Updates active-task and completion-time state when dropped.
    pub(super) completion: Option<TaskCompletionGuard>,
    /// Prevents a panic observed during polling from being logged again when
    /// unwinding drops this tracked future.
    pub(super) panic_reported: bool,
    /// Subscriber active when the task was registered with the executor.
    pub(super) subscriber: Dispatch,
}

impl<F> Future for TrackedFuture<F>
where
    F: Future,
{
    /// Output is unit because the wrapped application output is dropped here.
    type Output = ();
    /// Polls the application future and records completion after dropping its
    /// output and future resources.
    ///
    /// # Parameters
    ///
    /// * `context` - Poll context used by the wrapped future.
    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        let result = catch_unwind(AssertUnwindSafe(|| {
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
                    Poll::Ready(())
                }
                Poll::Pending => Poll::Pending,
            }
        }));
        match result {
            Ok(Poll::Ready(())) => {
                this.completion.take();
                Poll::Ready(())
            }
            Ok(Poll::Pending) => Poll::Pending,
            Err(payload) => {
                this.report_panic();
                resume_unwind(payload)
            }
        }
    }
}

impl<F> Drop for TrackedFuture<F> {
    /// Drops the application future before recording task completion.
    fn drop(&mut self) {
        let result = catch_unwind(AssertUnwindSafe(|| drop(self.future.take())));
        self.completion.take();
        if let Err(payload) = result {
            self.report_panic();
            resume_unwind(payload);
        }
    }
}

impl<F> TrackedFuture<F> {
    /// Captures the current tracing subscriber for task-local panic events.
    pub(super) fn current_subscriber() -> Dispatch {
        tracing::dispatcher::get_default(Clone::clone)
    }

    /// Reports the stable Hyper panic event at most once for this task.
    fn report_panic(&mut self) {
        if !self.panic_reported {
            self.panic_reported = true;
            tracing::dispatcher::with_default(&self.subscriber, || {
                tracing::warn!(
                    target: "qubit_web::transport",
                    event = "connection_task_panic",
                    task_kind = "hyper",
                    "connection task panicked"
                );
            });
        }
    }
}

#[cfg(test)]
mod tests {
    use std::future::Future;
    use std::pin::Pin;
    use std::sync::Arc;
    use std::sync::Mutex;
    use std::task::Context;
    use std::task::Poll;

    use tokio::sync::oneshot;
    use tracing::instrument::WithSubscriber;

    use super::super::super::connection_diagnostics::tests::SharedLog;
    use super::super::super::connection_diagnostics::tests::diagnostic_subscriber;
    use super::super::task_completion_guard::TaskCompletionGuard;
    use super::super::task_executor_state::TaskExecutorState;
    use super::TrackedFuture;

    fn tracked<F: Future>(future: F) -> (TrackedFuture<F>, Arc<Mutex<TaskExecutorState>>) {
        let state = Arc::new(Mutex::new(TaskExecutorState {
            accepting: false,
            tasks: None,
            active: 1,
            last_completed: None,
        }));
        let tracked = TrackedFuture {
            future: Some(Box::pin(future)),
            completion: Some(TaskCompletionGuard {
                state: Arc::clone(&state),
            }),
            panic_reported: false,
            subscriber: TrackedFuture::<F>::current_subscriber(),
        };
        (tracked, state)
    }

    fn assert_one_safe_panic(log: &SharedLog, private_payload: &str) {
        let output = log.contents();
        assert_eq!(output.matches("connection_task_panic").count(), 1, "{output}");
        assert!(output.contains("task_kind=\"hyper\""), "{output}");
        assert!(!output.contains(private_payload), "{output}");
    }

    #[tokio::test]
    async fn test_normal_completion_and_cancellation_do_not_report_panics() {
        let log = SharedLog::default();
        let dispatch = diagnostic_subscriber(&log);
        async {
            let (tracked_future, completed_state) = tracked(async {});
            tokio::spawn(tracked_future).await.expect("normal task should complete");
            assert_eq!(completed_state.lock().unwrap().active, 0);

            let (entered_tx, entered_rx) = oneshot::channel();
            let (tracked_future, cancelled_state) = tracked(async move {
                let _ = entered_tx.send(());
                std::future::pending::<()>().await;
            });
            let task = tokio::spawn(tracked_future);
            entered_rx.await.expect("pending task should be polled");
            task.abort();
            assert!(task.await.expect_err("task should be cancelled").is_cancelled());
            assert_eq!(cancelled_state.lock().unwrap().active, 0);
            assert!(!log.contents().contains("connection_task_panic"), "{}", log.contents());
        }
        .with_subscriber(dispatch)
        .await;
    }

    #[tokio::test]
    async fn test_poll_panic_remains_a_panic_join_error_and_releases_completion() {
        let log = SharedLog::default();
        let dispatch = diagnostic_subscriber(&log);
        async {
            let (tracked, state) = tracked(async { panic!("private poll payload") });
            let result = tokio::spawn(tracked).await;
            assert!(result.expect_err("poll panic must propagate").is_panic());
            assert_eq!(state.lock().unwrap().active, 0);
            assert_one_safe_panic(&log, "private poll payload");
        }
        .with_subscriber(dispatch)
        .await;
    }

    #[tokio::test]
    async fn test_success_output_drop_panic_is_reported_and_rethrown() {
        struct PanicOnDrop;

        impl Drop for PanicOnDrop {
            fn drop(&mut self) {
                panic!("private output drop payload");
            }
        }

        let log = SharedLog::default();
        let dispatch = diagnostic_subscriber(&log);
        async {
            let (tracked, state) = tracked(async { PanicOnDrop });
            let result = tokio::spawn(tracked).await;
            assert!(result.expect_err("output drop panic must propagate").is_panic());
            assert_eq!(state.lock().unwrap().active, 0);
            assert_one_safe_panic(&log, "private output drop payload");
        }
        .with_subscriber(dispatch)
        .await;
    }

    #[tokio::test]
    async fn test_future_drop_panic_is_reported_once_and_rethrown() {
        struct PanicOnDrop(Option<oneshot::Sender<()>>);

        impl Future for PanicOnDrop {
            type Output = ();

            fn poll(mut self: Pin<&mut Self>, _context: &mut Context<'_>) -> Poll<Self::Output> {
                if let Some(sender) = self.0.take() {
                    let _ = sender.send(());
                }
                Poll::Pending
            }
        }

        impl Drop for PanicOnDrop {
            fn drop(&mut self) {
                panic!("private future drop payload");
            }
        }

        let log = SharedLog::default();
        let dispatch = diagnostic_subscriber(&log);
        async {
            let (entered_tx, entered_rx) = oneshot::channel();
            let (tracked, state) = tracked(PanicOnDrop(Some(entered_tx)));
            let task = tokio::spawn(tracked);
            entered_rx.await.expect("task was polled before cancellation");
            task.abort();
            assert!(task.await.expect_err("abort while dropping must panic").is_panic());
            assert_eq!(state.lock().unwrap().active, 0);
            assert_one_safe_panic(&log, "private future drop payload");
        }
        .with_subscriber(dispatch)
        .await;
    }
}
