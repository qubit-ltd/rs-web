// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Closes admission and reaps managed and transport tasks after serving.
use std::io;
use std::sync::Arc;

use hyper_util::server::graceful::GracefulShutdown;
use tokio::net::TcpListener;
use tokio::task::JoinSet;

use super::connection_completion::ConnectionCompletion;
use super::connection_task_executor::ConnectionTaskExecutor;
use super::serve_error;
use super::server_context::ServerContext;
use super::shutdown_report::ShutdownReport;
use crate::WebServerError;
/// Completes shutdown within the established deadline and reaps all connection
/// tasks.
///
/// It closes the listener, starts the server shutdown deadline, closes managed
/// task registration, and waits for graceful transport completion, sessions,
/// and registered tasks until that deadline. It then snapshots completion
/// state, aborts unfinished connection tasks when needed, and always joins
/// them.
/// # Parameters
///
/// * `listener` - Bound TCP listener served by this server.
/// * `context` - Task context used to register a waker or carry request state.
/// * `graceful` - Hyper graceful-shutdown watcher for accepted connections.
/// * `completion` - Shared completion state updated when the guard is dropped.
/// * `connection_tasks` - Executor tracking Hyper tasks for accepted
///   connections.
/// * `connections` - Set of connection tasks that must be joined or aborted.
/// * `accept_error` - Terminal I/O error returned by the accept loop, if any.
pub(super) async fn finish_serve(
    listener: Arc<TcpListener>,
    context: ServerContext,
    graceful: GracefulShutdown,
    completion: ConnectionCompletion,
    connection_tasks: ConnectionTaskExecutor,
    mut connections: JoinSet<()>,
    accept_error: Option<io::Error>,
) -> Result<ShutdownReport, WebServerError> {
    drop(listener);
    let started = tokio::time::Instant::now();
    let shutdown_deadline = match context.begin_shutdown() {
        Ok(deadline) => deadline,
        Err(error) => {
            let shutdown_deadline = context.shutdown_deadline().unwrap_or_else(tokio::time::Instant::now);
            let tracked_tasks = connection_tasks.close();
            connections.abort_all();
            tokio::join!(tracked_tasks.wait_until(shutdown_deadline), async {
                while connections.join_next().await.is_some() {}
            },);
            return Err(error);
        }
    };
    let tracked_tasks = connection_tasks.close();
    let (_wait_result, managed_task_report) = tokio::join!(
        tokio::time::timeout_at(shutdown_deadline, async {
            tokio::join!(graceful.shutdown(), context.wait_for_sessions())
        }),
        tracked_tasks.wait_until(shutdown_deadline),
    );
    let unfinished_managed_sessions = context.active_sessions_at(shutdown_deadline);
    let transport_completed_by_deadline = completion.completed_by(shutdown_deadline);
    let graceful = unfinished_managed_sessions == 0
        && transport_completed_by_deadline
        && managed_task_report.completed_by_deadline;
    if !graceful {
        connections.abort_all();
    }
    while connections.join_next().await.is_some() {}
    let report = ShutdownReport {
        graceful,
        forced_connections: None,
        unfinished_managed_sessions,
        elapsed: started.elapsed(),
    };
    if let Some(source) = accept_error {
        Err(serve_error(source))
    } else {
        Ok(report)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::future::pending;
    use std::io;
    use std::sync::Arc;
    use std::sync::atomic::AtomicBool;
    use std::sync::atomic::Ordering;
    use std::time::Duration;

    use hyper_util::server::graceful::GracefulShutdown;
    use tokio::net::TcpListener;
    use tokio::sync::Semaphore;
    use tokio::task::JoinSet;
    use tokio::time::timeout;

    use super::super::accept_loop::run_accept_loop;
    use super::super::accept_retry::AcceptRetry;
    use super::super::connection_completion::ConnectionCompletion;
    use super::super::connection_task_executor::ConnectionTaskExecutor;
    use super::super::server_context::ServerContext;
    use super::finish_serve;

    #[tokio::test]
    async fn test_finish_serve_reaps_hyper_tasks_when_deadline_overflows() {
        struct SignalOnDrop(Arc<AtomicBool>);

        impl Drop for SignalOnDrop {
            fn drop(&mut self) {
                self.0.store(true, Ordering::SeqCst);
            }
        }

        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind test listener");
        let context = ServerContext::new(Duration::MAX);
        let completion = ConnectionCompletion::new();
        let connection_tasks = ConnectionTaskExecutor::new();
        let dropped = Arc::new(AtomicBool::new(false));
        let task_signal = SignalOnDrop(dropped.clone());
        connection_tasks
            .spawn(async move {
                let _signal = task_signal;
                pending::<()>().await;
            })
            .expect("executor should accept tasks before shutdown");

        let result = timeout(
            Duration::from_secs(1),
            finish_serve(
                Arc::new(listener),
                context,
                GracefulShutdown::new(),
                completion,
                connection_tasks,
                JoinSet::new(),
                None,
            ),
        )
        .await
        .expect("overflow cleanup should finish after aborting tracked Hyper tasks");

        assert!(matches!(result, Err(crate::WebServerError::InvalidConfig)));
        assert!(dropped.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn test_finish_serve_waits_for_hyper_tasks_and_marks_forced_shutdown() {
        struct SignalOnDrop(Arc<AtomicBool>);

        impl Drop for SignalOnDrop {
            fn drop(&mut self) {
                self.0.store(true, Ordering::SeqCst);
            }
        }

        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind test listener");
        let context = ServerContext::new(Duration::from_millis(20));
        let completion = ConnectionCompletion::new();
        let connection_tasks = ConnectionTaskExecutor::new();
        let dropped = Arc::new(AtomicBool::new(false));
        let task_signal = SignalOnDrop(dropped.clone());
        connection_tasks
            .spawn(async move {
                let _signal = task_signal;
                pending::<()>().await;
            })
            .expect("executor should accept tasks before shutdown");

        let report = timeout(
            Duration::from_secs(1),
            finish_serve(
                Arc::new(listener),
                context,
                GracefulShutdown::new(),
                completion,
                connection_tasks,
                JoinSet::new(),
                None,
            ),
        )
        .await
        .expect("shutdown cleanup should finish after aborting tracked Hyper tasks")
        .expect("no accept error should be returned");

        assert!(!report.graceful);
        assert_eq!(report.unfinished_managed_sessions, 0);
        assert!(report.elapsed >= Duration::from_millis(20));
        assert!(dropped.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn test_finish_serve_reports_late_transport_completion_as_not_graceful() {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind test listener");
        let context = ServerContext::new(Duration::from_millis(20));
        let deadline = context.begin_shutdown().expect("shutdown deadline is representable");
        let completion = ConnectionCompletion::new();
        let late_completion = deadline + Duration::from_millis(1);
        completion.record_completion(late_completion);
        tokio::time::sleep_until(late_completion).await;

        let report = finish_serve(
            Arc::new(listener),
            context,
            GracefulShutdown::new(),
            completion,
            ConnectionTaskExecutor::new(),
            JoinSet::new(),
            None,
        )
        .await
        .expect("no accept error should be returned");

        assert!(!report.graceful);
        assert_eq!(report.unfinished_managed_sessions, 0);
    }

    #[tokio::test]
    async fn test_finish_serve_keeps_on_time_transport_graceful_when_observed_late() {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind test listener");
        let context = ServerContext::new(Duration::from_millis(20));
        let deadline = context.begin_shutdown().expect("shutdown deadline is representable");
        let completion = ConnectionCompletion::new();
        completion.record_completion(deadline - Duration::from_millis(1));
        tokio::time::sleep_until(deadline + Duration::from_millis(1)).await;

        let report = finish_serve(
            Arc::new(listener),
            context,
            GracefulShutdown::new(),
            completion,
            ConnectionTaskExecutor::new(),
            JoinSet::new(),
            None,
        )
        .await
        .expect("no accept error should be returned");

        assert!(report.graceful);
        assert_eq!(report.unfinished_managed_sessions, 0);
    }

    #[tokio::test]
    async fn test_accept_terminal_error_closes_listener_and_reaps_connection_task() {
        struct DropSignal(Arc<AtomicBool>);

        impl Drop for DropSignal {
            fn drop(&mut self) {
                self.0.store(true, Ordering::SeqCst);
            }
        }

        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind test listener");
        let address = listener.local_addr().expect("read test listener address");
        let dropped = Arc::new(AtomicBool::new(false));
        let session_cancelled = Arc::new(AtomicBool::new(false));
        let context = ServerContext::new(Duration::from_millis(50));
        let cancellation = context.cancellation_token();
        let session_guard = context
            .try_register_session()
            .expect("session registers before shutdown");
        let mut connections = JoinSet::new();
        let watcher = GracefulShutdown::new();
        let task_dropped = dropped.clone();
        connections.spawn(async move {
            let _drop_signal = DropSignal(task_dropped);
            std::future::pending::<()>().await;
        });
        let was_cancelled = session_cancelled.clone();
        connections.spawn(async move {
            let _session_guard = session_guard;
            cancellation.cancelled().await;
            was_cancelled.store(true, Ordering::SeqCst);
            std::future::pending::<()>().await;
        });

        let mut failures: VecDeque<io::Result<()>> = VecDeque::new();
        for _ in 0..8 {
            failures.push_back(Err(io::Error::from(io::ErrorKind::ConnectionAborted)));
        }
        failures.push_back(Err(io::Error::new(
            io::ErrorKind::ConnectionAborted,
            "ninth accept sentinel",
        )));
        let failures = Arc::new(std::sync::Mutex::new(failures));
        let permits = Arc::new(Semaphore::new(1));
        let mut retry = AcceptRetry::default();
        let (accept_error, connections) = run_accept_loop(
            connections,
            std::future::pending::<()>(),
            &mut retry,
            {
                let failures = failures.clone();
                let permits = permits.clone();
                move |_retry_at| {
                    let failures = failures.clone();
                    let permits = permits.clone();
                    async move {
                        let permit = permits.acquire_owned().await.ok()?;
                        let result = failures
                            .lock()
                            .expect("scripted failures lock")
                            .pop_front()
                            .expect("script provides nine accept failures");
                        Some((permit, result))
                    }
                }
            },
            |_accepted, _permit, _connections| {},
        )
        .await;
        let accept_error = accept_error.expect("ninth transient failure terminates the accept loop");
        assert_eq!(accept_error.to_string(), "ninth accept sentinel");

        let result = finish_serve(
            Arc::new(listener),
            context,
            watcher,
            ConnectionCompletion::new(),
            ConnectionTaskExecutor::new(),
            connections,
            Some(accept_error),
        )
        .await;

        let error = result.expect_err("terminal accept error is returned after cleanup");
        assert!(matches!(&error, crate::WebServerError::ServeFailed { .. }));
        let source = std::error::Error::source(&error)
            .and_then(|source| source.downcast_ref::<io::Error>())
            .expect("the original accept error remains the source");
        assert_eq!(source.kind(), io::ErrorKind::ConnectionAborted);
        assert_eq!(source.to_string(), "ninth accept sentinel");
        assert!(
            dropped.load(Ordering::SeqCst),
            "connection task must exit and be reaped"
        );
        assert!(
            session_cancelled.load(Ordering::SeqCst),
            "terminal accept error initiates managed session cancellation"
        );
        assert!(
            TcpListener::bind(address).await.is_ok(),
            "terminal accept error closes listener"
        );
    }
}
