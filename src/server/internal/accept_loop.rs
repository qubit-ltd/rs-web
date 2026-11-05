// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Coordinates accept results, shutdown priority, and completed connection
//! tasks.
use std::future::Future;
use std::io;

use tokio::sync::OwnedSemaphorePermit;
use tokio::task::JoinSet;

use super::accept_result::AcceptResult;
use super::accept_retry::AcceptRetry;
use super::connection_diagnostics::record_task_result;
use super::server_event::ServerEvent;
/// Runs accept, shutdown, and connection-reaping coordination until serving
/// stops.
///
/// Shutdown remains biased over ready accepts, and completed tasks are polled
/// between accepts so a continuously ready listener cannot starve reaping.
/// # Type Parameters
///
/// * `T` - Accepted transport or item type carried by this operation.
/// * `MakeAccept` - Callback type that creates the listener accept future.
/// * `AcceptFuture` - Future type returned by the accept callback.
/// * `OnAccepted` - Callback type that handles an accepted transport.
/// * `Shutdown` - Future type that resolves when shutdown is requested.
/// # Parameters
///
/// * `connections` - Set of connection tasks that must be joined or aborted.
/// * `shutdown` - Cancellation token that can stop the operation.
/// * `retry` - Mutable retry state updated for this accept result.
/// * `make_accept` - Callback creating the next accept future for a retry
///   deadline.
/// * `on_accepted` - Callback that handles each accepted transport and permit.
pub(super) async fn run_accept_loop<T, MakeAccept, AcceptFuture, OnAccepted, Shutdown>(
    mut connections: JoinSet<()>,
    mut shutdown: Shutdown,
    retry: &mut AcceptRetry,
    mut make_accept: MakeAccept,
    mut on_accepted: OnAccepted,
) -> (Option<io::Error>, JoinSet<()>)
where
    MakeAccept: FnMut(Option<tokio::time::Instant>) -> AcceptFuture,
    AcceptFuture: Future<Output = Option<(OwnedSemaphorePermit, io::Result<T>)>>,
    OnAccepted: FnMut(T, OwnedSemaphorePermit, &mut JoinSet<()>),
    Shutdown: Future<Output = ()> + Unpin,
{
    let accept_error = loop {
        let event = select_server_event(&mut connections, &mut shutdown, make_accept(retry.retry_at())).await;
        match event {
            ServerEvent::Shutdown => break None,
            ServerEvent::Joined => {}
            ServerEvent::Accept(None) => break None,
            ServerEvent::Accept(Some((permit, result))) => match handle_accept_result(retry, result) {
                AcceptResult::Accepted(accepted) => on_accepted(accepted, permit, &mut connections),
                AcceptResult::Retry => drop(permit),
                AcceptResult::Terminal(source) => {
                    drop(permit);
                    break Some(source);
                }
            },
        }
    };
    (accept_error, connections)
}

/// Records an accept result and determines whether to continue, retry, or stop.
/// # Type Parameters
///
/// * `T` - Accepted transport or item type carried by this operation.
/// # Parameters
///
/// * `retry` - Mutable retry state updated for this accept result.
/// * `result` - Result returned by the underlying operation.
pub(super) fn handle_accept_result<T>(retry: &mut AcceptRetry, result: io::Result<T>) -> AcceptResult<T> {
    match result {
        Ok(accepted) => {
            retry.record_success();
            AcceptResult::Accepted(accepted)
        }
        Err(source) => match retry.record_error(source) {
            Some(_) => AcceptResult::Retry,
            None => AcceptResult::Terminal(retry.take_terminal_error().expect("terminal accept error is retained")),
        },
    }
}

/// Selects shutdown, a completed connection task, or the current accept result.
/// # Type Parameters
///
/// * `A` - Future type supplying the accept event.
/// * `S` - Shutdown signal future with `Output = ()`, resolved when shutdown is
///   requested.
/// # Parameters
///
/// * `connections` - Set of connection tasks that must be joined or aborted.
/// * `shutdown` - Future that resolves when the server should stop accepting.
/// * `accept` - Future waiting for one listener accept result.
pub(super) async fn select_server_event<A, S>(
    connections: &mut JoinSet<()>,
    shutdown: S,
    accept: A,
) -> ServerEvent<A::Output>
where
    A: Future,
    S: Future<Output = ()>,
{
    tokio::select! {
        biased;
        _ = shutdown => ServerEvent::Shutdown,
        joined = connections.join_next(), if !connections.is_empty() => {
            if let Some(result) = joined {
                record_task_result(result, "transport");
            }
            ServerEvent::Joined
        }
        accepted = accept => ServerEvent::Accept(accepted),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::future::pending;
    use std::future::ready;
    use std::io;
    use std::pin::pin;
    use std::sync::Arc;
    use std::sync::atomic::Ordering;
    use std::task::Context;
    use std::task::Poll;
    use std::task::Waker;
    use std::time::Duration;

    use tokio::sync::Semaphore;
    use tokio::sync::mpsc;
    use tokio::sync::oneshot;
    use tokio::task::JoinSet;
    use tracing::instrument::WithSubscriber;

    use super::super::accept_retry::AcceptRetry;
    use super::super::connection_diagnostics::tests::SharedLog;
    use super::super::connection_diagnostics::tests::diagnostic_subscriber;
    use super::super::server_event::ServerEvent;
    use super::run_accept_loop;
    use super::select_server_event;

    #[tokio::test(flavor = "current_thread")]
    async fn test_accept_readiness_does_not_starve_completed_join_set_entries() {
        let mut connections = JoinSet::new();
        let (completed_tx, completed_rx) = oneshot::channel();
        connections.spawn(async move {
            completed_tx.send(()).expect("completion receiver remains open");
        });
        completed_rx.await.expect("connection task completed");
        tokio::task::yield_now().await;

        let mut reaped = 0;
        for accepted in 0..8 {
            match select_server_event(&mut connections, pending(), ready(accepted)).await {
                ServerEvent::Accept(value) => assert_eq!(value, accepted),
                ServerEvent::Joined => reaped += 1,
                ServerEvent::Shutdown => panic!("shutdown is pending"),
            }
        }

        let completed_entry_retained = connections.try_join_next().is_some();
        assert!(
            reaped > 0,
            "ready accept won every biased selection; reaped={reaped}, completed entry retained={completed_entry_retained}"
        );
    }

    #[tokio::test]
    async fn test_accept_is_selected_when_join_set_has_no_completed_entries() {
        let mut connections = JoinSet::new();

        let event = select_server_event(&mut connections, pending(), ready("accepted")).await;

        assert!(matches!(event, ServerEvent::Accept("accepted")));
        assert!(connections.is_empty());
    }

    #[tokio::test]
    async fn test_panicking_connection_is_reaped_and_accept_continues() {
        let log = SharedLog::default();
        let dispatch = diagnostic_subscriber(&log);
        let mut connections = JoinSet::new();
        let (panicked_tx, panicked_rx) = oneshot::channel();
        connections.spawn(async move {
            let _ = panicked_tx.send(());
            panic!("accept-loop-panic-sentinel");
        });
        panicked_rx.await.expect("connection task started");

        let event = select_server_event(&mut connections, pending(), ready(23))
            .with_subscriber(dispatch)
            .await;
        assert!(matches!(event, ServerEvent::Joined));

        let next_event = select_server_event(&mut connections, pending(), ready(24)).await;
        assert!(matches!(next_event, ServerEvent::Accept(24)));
        let output = log.contents();
        assert_eq!(output.matches("connection_task_panic").count(), 1, "{output}");
        assert!(output.contains("task_kind=\"transport\""), "{output}");
        assert!(!output.contains("accept-loop-panic-sentinel"), "{output}");
    }

    #[test]
    fn test_empty_join_set_does_not_make_selector_ready() {
        let mut connections = JoinSet::new();
        let mut event = pin!(select_server_event(&mut connections, pending::<()>(), pending::<()>(),));
        let mut context = Context::from_waker(Waker::noop());

        assert!(matches!(event.as_mut().poll(&mut context), Poll::Pending));
    }

    #[tokio::test]
    async fn test_shutdown_is_selected_before_ready_accept() {
        let mut connections = JoinSet::new();

        let event = select_server_event(&mut connections, ready(()), ready("accepted")).await;

        assert!(matches!(event, ServerEvent::Shutdown));
    }

    #[tokio::test]
    async fn test_accept_loop_retries_controlled_error_then_accepts_connection() {
        let results = Arc::new(std::sync::Mutex::new(VecDeque::from([
            Err(io::Error::from(io::ErrorKind::ConnectionAborted)),
            Ok(23),
        ])));
        let permits = Arc::new(Semaphore::new(1));
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let (accepted_tx, accepted_rx) = oneshot::channel();
        let mut shutdown_tx = Some(shutdown_tx);
        let mut accepted_tx = Some(accepted_tx);
        let mut retry = AcceptRetry::default();
        let shutdown = async move {
            let _ = shutdown_rx.await;
        };
        tokio::pin!(shutdown);

        let (error, connections) = run_accept_loop(
            JoinSet::new(),
            shutdown,
            &mut retry,
            {
                let results = results.clone();
                let permits = permits.clone();
                move |retry_at| {
                    let results = results.clone();
                    let permits = permits.clone();
                    async move {
                        if let Some(deadline) = retry_at {
                            tokio::time::sleep_until(deadline).await;
                        }
                        let permit = permits.acquire_owned().await.ok()?;
                        let result = results
                            .lock()
                            .expect("scripted results lock")
                            .pop_front()
                            .expect("script contains error and then success");
                        Some((permit, result))
                    }
                }
            },
            move |accepted, _permit, _connections| {
                accepted_tx
                    .take()
                    .expect("only one scripted accept succeeds")
                    .send(accepted)
                    .expect("acceptance receiver remains open");
                shutdown_tx
                    .take()
                    .expect("shutdown is requested once")
                    .send(())
                    .expect("shutdown receiver remains open");
            },
        )
        .await;

        assert!(error.is_none(), "a transient error must not terminate accept loop");
        assert_eq!(accepted_rx.await.expect("successful accept is delivered"), 23);
        assert_eq!(retry.consecutive_failures(), 0, "success resets retry count");
        assert!(connections.is_empty());
    }

    #[tokio::test]
    async fn test_accept_loop_fatal_error_stops_after_one_accept_attempt() {
        let permits = Arc::new(Semaphore::new(1));
        let accept_calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let mut retry = AcceptRetry::default();
        let (error, connections) = run_accept_loop(
            JoinSet::new(),
            std::future::pending::<()>(),
            &mut retry,
            {
                let permits = permits.clone();
                let accept_calls = accept_calls.clone();
                move |_retry_at| {
                    let permits = permits.clone();
                    let accept_calls = accept_calls.clone();
                    async move {
                        accept_calls.fetch_add(1, Ordering::SeqCst);
                        let permit = permits.acquire_owned().await.ok()?;
                        Some((
                            permit,
                            Err::<(), io::Error>(io::Error::new(io::ErrorKind::PermissionDenied, "fatal sentinel")),
                        ))
                    }
                }
            },
            |_accepted, _permit, _connections| {},
        )
        .await;

        assert_eq!(accept_calls.load(Ordering::SeqCst), 1, "fatal error is not retried");
        assert_eq!(
            error.expect("fatal accept error terminates loop").to_string(),
            "fatal sentinel"
        );
        assert!(connections.is_empty());
    }

    #[tokio::test]
    async fn test_accept_loop_releases_permit_before_retry_backoff() {
        let permits = Arc::new(Semaphore::new(1));
        let permit_observer = permits.clone();
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let (retried_tx, retried_rx) = oneshot::channel();
        let mut retried_tx = Some(retried_tx);
        let mut accept_count = 0;
        let mut accept_loop = tokio::spawn(async move {
            let shutdown = async move {
                let _ = shutdown_rx.await;
            };
            tokio::pin!(shutdown);
            let mut retry = AcceptRetry::default();
            run_accept_loop(
                JoinSet::new(),
                shutdown,
                &mut retry,
                move |retry_at| {
                    let permits = permits.clone();
                    let current_attempt = accept_count;
                    accept_count += 1;
                    let retried_tx = if current_attempt == 0 { None } else { retried_tx.take() };
                    async move {
                        if let Some(deadline) = retry_at {
                            tokio::time::sleep_until(deadline).await;
                        }
                        let permit = permits.acquire_owned().await.ok()?;
                        if current_attempt == 0 {
                            return Some((permit, Err(io::Error::from(io::ErrorKind::ConnectionAborted))));
                        }
                        retried_tx
                            .expect("second accept receives the retry notification")
                            .send(())
                            .expect("test observes reacquired permit");
                        let _permit = permit;
                        std::future::pending::<Option<(tokio::sync::OwnedSemaphorePermit, io::Result<()>)>>().await
                    }
                },
                |_accepted, _permit, _connections| {},
            )
            .await
        });

        let retry_result = tokio::time::timeout(Duration::from_secs(1), retried_rx).await;
        if let Err(error) = retry_result {
            shutdown_tx.send(()).expect("shutdown receiver remains open");
            accept_loop.abort();
            let _ = accept_loop.await;
            panic!("retry must reacquire the only transport permit: {error}");
        }
        retry_result
            .expect("retry notification arrived")
            .expect("accept loop reacquired the permit");
        shutdown_tx.send(()).expect("shutdown receiver remains open");
        let loop_result = tokio::time::timeout(Duration::from_secs(1), &mut accept_loop).await;
        if let Err(error) = loop_result {
            accept_loop.abort();
            let _ = accept_loop.await;
            panic!("shutdown cancels the pending retry accept: {error}");
        }
        let (_error, connections) = loop_result
            .expect("loop result was checked")
            .expect("accept loop task completes");
        assert!(connections.is_empty());
        assert_eq!(
            permit_observer.available_permits(),
            1,
            "cancelled accept releases its permit"
        );
    }

    #[tokio::test]
    async fn test_accept_shutdown_cancels_rebuilt_backoff_future() {
        let permits = Arc::new(Semaphore::new(1));
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let (retry_started_tx, retry_started_rx) = oneshot::channel();
        let mut retry_started_tx = Some(retry_started_tx);
        let mut accept_count = 0;
        let mut accept_loop = tokio::spawn(async move {
            let shutdown = async move {
                let _ = shutdown_rx.await;
            };
            tokio::pin!(shutdown);
            let mut retry = AcceptRetry::default();
            run_accept_loop(
                JoinSet::new(),
                shutdown,
                &mut retry,
                move |retry_at| {
                    let permits = permits.clone();
                    let this_accept = accept_count;
                    accept_count += 1;
                    let retry_started_tx = if this_accept == 0 {
                        None
                    } else {
                        retry_started_tx.take()
                    };
                    async move {
                        if this_accept == 0 {
                            let permit = permits.acquire_owned().await.ok()?;
                            return Some((permit, Err(io::Error::from(io::ErrorKind::ConnectionAborted))));
                        }
                        let deadline = retry_at.expect("retry future receives its saved deadline");
                        retry_started_tx
                            .expect("retry future is built once")
                            .send(())
                            .expect("test observes retry future");
                        tokio::time::sleep_until(deadline).await;
                        let permit = permits.acquire_owned().await.ok()?;
                        let _permit = permit;
                        std::future::pending::<Option<(tokio::sync::OwnedSemaphorePermit, io::Result<()>)>>().await
                    }
                },
                |_accepted, _permit, _connections| {},
            )
            .await
        });

        retry_started_rx
            .await
            .expect("loop rebuilt the accept future during backoff");
        shutdown_tx.send(()).expect("shutdown receiver remains open");
        let loop_result = tokio::time::timeout(Duration::from_secs(1), &mut accept_loop).await;
        if let Err(error) = loop_result {
            accept_loop.abort();
            let _ = accept_loop.await;
            panic!("shutdown must cancel the pending retry future: {error}");
        }
        let (_error, connections) = loop_result
            .expect("loop result was checked")
            .expect("accept loop task completes");
        assert!(connections.is_empty());
    }

    #[tokio::test]
    async fn test_accept_join_rebuilds_future_without_resetting_backoff_deadline() {
        let permits = Arc::new(Semaphore::new(1));
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let (join_tx, join_rx) = oneshot::channel();
        let (deadlines_tx, mut deadlines_rx) = mpsc::unbounded_channel();
        let mut accept_count = 0;
        let mut connections = JoinSet::new();
        connections.spawn(async move {
            let _ = join_rx.await;
        });

        let mut accept_loop = tokio::spawn(async move {
            let shutdown = async move {
                let _ = shutdown_rx.await;
            };
            tokio::pin!(shutdown);
            let mut retry = AcceptRetry::default();
            run_accept_loop(
                connections,
                shutdown,
                &mut retry,
                move |retry_at| {
                    let permits = permits.clone();
                    let deadlines_tx = deadlines_tx.clone();
                    let this_accept = accept_count;
                    accept_count += 1;
                    async move {
                        if this_accept == 0 {
                            let permit = permits.acquire_owned().await.ok()?;
                            return Some((permit, Err(io::Error::from(io::ErrorKind::ConnectionAborted))));
                        }
                        let deadline = retry_at.expect("retry future receives its saved deadline");
                        deadlines_tx.send(deadline).expect("deadline observer remains open");
                        tokio::time::sleep_until(deadline).await;
                        let permit = permits.acquire_owned().await.ok()?;
                        let _permit = permit;
                        std::future::pending::<Option<(tokio::sync::OwnedSemaphorePermit, io::Result<()>)>>().await
                    }
                },
                |_accepted, _permit, _connections| {},
            )
            .await
        });

        let first_deadline = deadlines_rx.recv().await.expect("first retry future is built");
        join_tx.send(()).expect("test connection task remains pending");
        let second_deadline = tokio::time::timeout(Duration::from_secs(1), deadlines_rx.recv())
            .await
            .expect("join event rebuilds accept future")
            .expect("rebuilt future reports its deadline");
        assert_eq!(first_deadline, second_deadline, "join must not restart backoff");
        shutdown_tx.send(()).expect("shutdown receiver remains open");
        let loop_result = tokio::time::timeout(Duration::from_secs(1), &mut accept_loop).await;
        if let Err(error) = loop_result {
            accept_loop.abort();
            let _ = accept_loop.await;
            panic!("shutdown must interrupt rebuilt retry wait: {error}");
        }
        loop_result
            .expect("loop result was checked")
            .expect("accept loop task completes");
    }
}
