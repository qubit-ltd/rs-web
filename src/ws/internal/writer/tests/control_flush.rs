// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::pin::Pin;
use std::sync::Arc;
use std::sync::Barrier;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::task::Context;
use std::task::Poll;
use std::time::Duration;

use axum::extract::ws::Message;
use futures_util::Sink;
use futures_util::task::noop_waker_ref;
use tokio::sync::Notify;
use tokio::sync::Semaphore;
use tokio::sync::oneshot;
use tokio::time::timeout;
use tokio_util::sync::CancellationToken;
use tracing::Level;
use tracing::instrument::WithSubscriber;

use super::super::FlushOutcome;
use super::super::flush_until;
use super::super::writer_loop;
use super::support::PeerCloseFailingSink;
use super::support::PeerCloseFlushBranch;
use super::support::PendingFlush;
use super::support::PendingSend;
use super::support::ReadyFlush;
use super::support::SharedLog;
use super::support::assert_notify_flush_peer_close;
use super::support::assert_peer_close_flush_failure;
use super::support::assert_pending_send_times_out_after_one_idle_window;
use crate::ServerOptions;
use crate::WebServer;
use crate::ws::WsSendQueue;
use crate::ws::WsTermination;
use crate::ws::internal::SessionLifecycle;
use crate::ws::internal::SessionResources;
use crate::ws::internal::TerminationState;
use crate::ws::ws_send_error::WsSendError;

#[test]
fn test_concurrent_termination_records_freeze_one_first_winner() {
    let state = Arc::new(TerminationState::default());
    let barrier = Arc::new(Barrier::new(3));
    let candidates = [WsTermination::ReadError, WsTermination::WriteError];
    let workers: Vec<_> = candidates
        .into_iter()
        .map(|candidate| {
            let state = state.clone();
            let barrier = barrier.clone();
            std::thread::spawn(move || {
                barrier.wait();
                state.record(candidate);
            })
        })
        .collect();
    barrier.wait();
    for worker in workers {
        worker.join().unwrap();
    }

    let winner = state.candidate().unwrap();
    assert!(matches!(winner, WsTermination::ReadError | WsTermination::WriteError));
    state.finalize();
    state.record(WsTermination::ServerShutdown);
    assert_eq!(state.snapshot(), Some(winner));
}
#[tokio::test]
async fn test_writer_control_flush_error_reports_flush_once() {
    let log = SharedLog::default();
    let dispatch = tracing::Dispatch::new(
        tracing_subscriber::fmt()
            .with_max_level(Level::DEBUG)
            .with_ansi(false)
            .with_writer(log.clone())
            .finish(),
    );
    let server = WebServer::bind_http(ServerOptions::new("127.0.0.1:0".parse().unwrap()))
        .await
        .expect("test server should bind");
    let session = server
        .context()
        .try_register_session()
        .expect("session should register");
    let permits = Arc::new(Semaphore::new(1));
    let permit = permits
        .clone()
        .acquire_owned()
        .await
        .expect("permit should be available");
    let queue = WsSendQueue::new(1, 128);
    let notify = queue.notify.clone();
    let writer_notify = notify.clone();
    let lifecycle = SessionLifecycle {
        shutdown: CancellationToken::new(),
        server_shutdown: CancellationToken::new(),
        termination: Arc::new(crate::ws::internal::TerminationState::default()),
        close_code: Arc::new(std::sync::atomic::AtomicU16::new(1000)),
        close_deadline: Arc::new(std::sync::Mutex::new(None)),
        server_deadline: None,
        shutdown_timeout: Duration::from_millis(100),
    };
    let resources = Arc::new(SessionResources::new(permit, session));
    let (ack_tx, ack_rx) = oneshot::channel();
    let writer = tokio::spawn(
        async move {
            writer_loop(
                ReadyFlush(Some(Err(std::io::Error::other("secret-sentinel flush failure")))),
                queue,
                writer_notify,
                Duration::from_secs(60),
                lifecycle,
                ack_rx,
                resources,
            )
            .await;
        }
        .with_subscriber(dispatch),
    );
    notify.notify_one();
    timeout(Duration::from_secs(1), writer)
        .await
        .expect("writer should exit after control flush error")
        .expect("writer should not panic");
    let output = log.contents();
    let failures: Vec<_> = output
        .lines()
        .filter(|line| line.contains("websocket_io_failure"))
        .collect();
    assert_eq!(failures.len(), 1, "expected exactly one flush failure event: {output}");
    assert!(
        failures[0].contains("operation=\"flush\""),
        "unexpected operation: {output}"
    );
    assert!(
        failures[0].contains("direction=\"write\""),
        "unexpected direction: {output}"
    );
    assert!(
        !output.contains("secret-sentinel"),
        "underlying error text must be redacted: {output}"
    );
    drop(ack_tx);
    drop(server);
}
#[tokio::test]
async fn test_writer_peer_close_ack_while_idle_flush_failure_reports_once() {
    assert_peer_close_flush_failure(PeerCloseFlushBranch::AckWhileIdle).await;
}
#[tokio::test]
async fn test_writer_peer_close_notify_flush_failure_reports_once() {
    assert_peer_close_flush_failure(PeerCloseFlushBranch::Notify).await;
}
#[tokio::test]
async fn test_writer_peer_close_ack_during_send_flush_failure_reports_once() {
    assert_peer_close_flush_failure(PeerCloseFlushBranch::AckWhileSending).await;
}
#[test]
fn test_peer_close_failing_sink_accepts_frames_and_closes() {
    let mut sink = PeerCloseFailingSink::Ready;
    let waker = std::task::Waker::noop();
    let mut context = Context::from_waker(waker);

    assert!(matches!(
        Sink::poll_ready(Pin::new(&mut sink), &mut context),
        Poll::Ready(Ok(()))
    ));
    assert!(Sink::start_send(Pin::new(&mut sink), Message::Text("test".into())).is_ok());
    assert!(matches!(
        Sink::poll_close(Pin::new(&mut sink), &mut context),
        Poll::Ready(Ok(()))
    ));
}
#[tokio::test(start_paused = true)]
async fn test_pending_flush_times_out_after_one_idle_window_without_external_close() {
    let pending = Arc::new(Notify::new());
    let polls = Arc::new(AtomicUsize::new(0));
    assert_pending_send_times_out_after_one_idle_window(
        PendingSend {
            pending_flush: pending.clone(),
            flush_polls: polls.clone(),
        },
        pending,
        polls,
    )
    .await;
}
#[tokio::test(start_paused = true)]
async fn test_control_flush_times_out_after_one_idle_window_despite_new_notifications() {
    const IDLE_WINDOW: Duration = Duration::from_millis(500);

    let server = WebServer::bind_http(ServerOptions::new("127.0.0.1:0".parse().unwrap()))
        .await
        .expect("test server should bind");
    let context = server.context();
    let session = context.try_register_session().expect("session should register");
    let permits = Arc::new(Semaphore::new(1));
    let permit = permits
        .clone()
        .acquire_owned()
        .await
        .expect("permit should be available");
    let queue = WsSendQueue::new(1, 128);
    let notify = queue.notify.clone();
    let pending_flush = Arc::new(Notify::new());
    let shutdown = CancellationToken::new();
    let lifecycle = SessionLifecycle {
        shutdown: shutdown.clone(),
        server_shutdown: shutdown.clone(),
        termination: Arc::new(crate::ws::internal::TerminationState::default()),
        close_code: Arc::new(std::sync::atomic::AtomicU16::new(1001)),
        close_deadline: Arc::new(std::sync::Mutex::new(None)),
        server_deadline: None,
        shutdown_timeout: Duration::from_millis(100),
    };
    let resources = Arc::new(SessionResources::new(permit, session));
    let (ack_tx, ack_rx) = oneshot::channel();
    let writer = tokio::spawn({
        let queue = queue.clone();
        let sink = PendingFlush {
            polled: pending_flush.clone(),
        };
        async move {
            writer_loop(sink, queue.clone(), notify, IDLE_WINDOW, lifecycle, ack_rx, resources).await;
        }
    });
    queue.notify.notify_one();
    pending_flush.notified().await;
    assert!(!writer.is_finished(), "control flush should begin pending");

    for _ in 0..4 {
        tokio::time::advance(IDLE_WINDOW / 5).await;
        queue.notify.notify_one();
        tokio::task::yield_now().await;
        assert!(
            !writer.is_finished(),
            "unrelated notifications do not shorten the flush deadline"
        );
        assert!(
            !shutdown.is_cancelled(),
            "control flush remains active before its deadline"
        );
    }
    tokio::time::advance(IDLE_WINDOW / 5).await;
    tokio::task::yield_now().await;
    drop(ack_tx);
    timeout(Duration::from_secs(1), writer)
        .await
        .expect("control flush should time out without external closure")
        .expect("writer should not panic");

    assert!(shutdown.is_cancelled(), "flush timeout cancels the session token");
    assert!(queue.is_empty(), "flush timeout closes the outbound queue");
    assert_eq!(
        context.active_sessions(),
        0,
        "flush timeout releases session registration"
    );
    assert_eq!(
        permits.available_permits(),
        1,
        "flush timeout releases connection capacity"
    );
    drop(server);
}
#[tokio::test]
async fn test_pending_flush_is_cancelled() {
    let polled = std::sync::Arc::new(Notify::new());
    let sink = PendingFlush { polled: polled.clone() };
    let token = CancellationToken::new();
    let task_token = token.clone();
    let task = tokio::spawn(async move {
        let mut sink = sink;
        flush_until(&mut sink, &task_token, None).await
    });

    polled.notified().await;
    assert!(!task.is_finished(), "flush should remain pending before cancellation");
    token.cancel();
    let result = timeout(Duration::from_secs(1), task)
        .await
        .expect("flush task should stop promptly")
        .expect("flush task should not panic");
    assert_eq!(result, FlushOutcome::Cancelled);
}
#[tokio::test]
async fn test_pending_flush_obeys_deadline() {
    let mut sink = PendingFlush {
        polled: std::sync::Arc::new(Notify::new()),
    };
    let token = CancellationToken::new();
    let result = flush_until(&mut sink, &token, Some(tokio::time::Instant::now())).await;
    assert_eq!(result, FlushOutcome::TimedOut);
}
#[tokio::test]
async fn test_ready_flush_succeeds() {
    let mut sink = ReadyFlush(Some(Ok(())));
    let token = CancellationToken::new();
    let result = flush_until(&mut sink, &token, None).await;
    assert_eq!(result, FlushOutcome::Flushed);
}
#[tokio::test]
async fn test_failed_flush_is_reported() {
    let mut sink = ReadyFlush(Some(Err(std::io::Error::other("injected"))));
    let token = CancellationToken::new();
    let result = flush_until(&mut sink, &token, None).await;
    assert_eq!(result, FlushOutcome::Failed);
}
#[tokio::test]
async fn test_writer_pending_send_peer_close_ack_does_not_cancel_session() {
    let server = WebServer::bind_http(ServerOptions::new("127.0.0.1:0".parse().unwrap()))
        .await
        .expect("test server should bind");
    let context = server.context();
    let session = context.try_register_session().expect("session should register");
    let permits = Arc::new(Semaphore::new(1));
    let permit = permits
        .clone()
        .acquire_owned()
        .await
        .expect("permit should be available");
    let queue = WsSendQueue::new(2, 1024);
    queue
        .try_send(Message::binary(vec![1; 128]))
        .expect("message should be queued");
    let pending_flush = Arc::new(Notify::new());
    let flush_polls = Arc::new(AtomicUsize::new(0));
    let sink = PendingSend {
        pending_flush: pending_flush.clone(),
        flush_polls: flush_polls.clone(),
    };
    let shutdown = CancellationToken::new();
    let resources = Arc::new(SessionResources::new(permit, session));
    let lifecycle = SessionLifecycle {
        shutdown: shutdown.clone(),
        server_shutdown: shutdown.clone(),
        termination: Arc::new(crate::ws::internal::TerminationState::default()),
        close_code: Arc::new(std::sync::atomic::AtomicU16::new(1000)),
        close_deadline: Arc::new(std::sync::Mutex::new(None)),
        server_deadline: None,
        shutdown_timeout: Duration::from_millis(100),
    };
    let notify = queue.notify.clone();
    let (ack_tx, ack_rx) = oneshot::channel();
    let writer = tokio::spawn(async move {
        writer_loop(
            sink,
            queue.clone(),
            notify,
            Duration::from_secs(60),
            lifecycle,
            ack_rx,
            resources,
        )
        .await;
        queue
    });

    timeout(Duration::from_secs(1), pending_flush.notified())
        .await
        .expect("writer must reach pending send flush before peer acknowledgement");
    assert!(flush_polls.load(Ordering::SeqCst) > 0);
    ack_tx.send(()).expect("peer acknowledgement receiver remains alive");
    let queue = timeout(Duration::from_secs(2), writer)
        .await
        .expect("writer should stop within the close deadline")
        .expect("writer task should not panic");

    assert!(
        !shutdown.is_cancelled(),
        "peer acknowledgement must not cancel the session"
    );
    assert_eq!(queue.try_send(Message::text("closed")), Err(WsSendError::Closed));
    let state = queue.state.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
    assert!(state.messages.is_empty(), "pending messages must be discarded");
    assert_eq!(state.queued_bytes, 0, "queued byte budget must be returned");
    assert_eq!(state.in_flight_messages, 0, "in-flight message budget must be returned");
    drop(state);
    assert_eq!(context.active_sessions(), 0, "session guard should be released");
    assert_eq!(permits.available_permits(), 1, "connection permit should be released");
    drop(server);
}
#[tokio::test]
async fn test_writer_notify_flush_peer_close_releases_resources() {
    assert_notify_flush_peer_close(None, false, false).await;
}
#[tokio::test]
async fn test_writer_notify_flush_close_ack_sender_drop_releases_resources() {
    assert_notify_flush_peer_close(None, true, false).await;
}
#[tokio::test]
async fn test_writer_notify_flush_close_ack_uses_expired_server_deadline() {
    assert_notify_flush_peer_close(Some(tokio::time::Instant::now()), false, false).await;
}
#[tokio::test]
async fn test_writer_notify_flush_close_ack_and_cancellation_simultaneously_ready() {
    assert_notify_flush_peer_close(None, false, true).await;
}
#[test]
fn test_sinks_accept_and_close_messages() {
    let mut context = Context::from_waker(noop_waker_ref());
    let mut pending = PendingFlush {
        polled: std::sync::Arc::new(Notify::new()),
    };
    let mut ready = ReadyFlush(Some(Ok(())));

    assert!(Pin::new(&mut pending).poll_ready(&mut context).is_ready());
    Pin::new(&mut pending).start_send(Message::Text("test".into())).unwrap();
    assert!(Pin::new(&mut pending).poll_close(&mut context).is_ready());

    assert!(Pin::new(&mut ready).poll_ready(&mut context).is_ready());
    Pin::new(&mut ready).start_send(Message::Text("test".into())).unwrap();
    assert!(Pin::new(&mut ready).poll_close(&mut context).is_ready());
}
