// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::time::Duration;

use axum::extract::ws::Message;
use tokio::sync::Notify;
use tokio::sync::Semaphore;
use tokio::sync::oneshot;
use tokio::time::timeout;
use tokio_util::sync::CancellationToken;
use tracing::Level;
use tracing::instrument::WithSubscriber;

use super::super::writer_loop;
use super::support::FailurePoint;
use super::support::PendingReady;
use super::support::PendingSend;
use super::support::RecordingSink;
use super::support::SharedLog;
use super::support::SuccessfulSink;
use super::support::assert_close_failure_reports;
use super::support::assert_pending_send_times_out_after_one_idle_window;
use super::support::assert_writer_failure;
use crate::ServerOptions;
use crate::WebServer;
use crate::ws::WsSendQueue;
use crate::ws::internal::SessionLifecycle;
use crate::ws::internal::SessionResources;
use crate::ws::ws_send_error::WsSendError;

#[tokio::test]
async fn test_writer_close_preserves_frame_drops_pending_data_and_times_out_without_ack() {
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
    let queue = WsSendQueue::new(2, 128);
    queue
        .try_send(Message::Close(Some(axum::extract::ws::CloseFrame {
            code: 4001,
            reason: "custom reason".into(),
        })))
        .expect("custom Close should be queued");
    queue
        .try_send(Message::text("discard after Close"))
        .expect("following data should fit before the writer starts Close");
    let recorded = Arc::new(std::sync::Mutex::new(Vec::new()));
    let shutdown = CancellationToken::new();
    let lifecycle = SessionLifecycle {
        shutdown: shutdown.clone(),
        server_shutdown: shutdown.clone(),
        termination: Arc::new(crate::ws::internal::TerminationState::default()),
        close_code: Arc::new(std::sync::atomic::AtomicU16::new(1001)),
        close_deadline: Arc::new(std::sync::Mutex::new(None)),
        server_deadline: None,
        shutdown_timeout: Duration::from_millis(25),
    };
    let resources = Arc::new(SessionResources::new(permit, session));
    let (ack_tx, ack_rx) = oneshot::channel();
    let writer = tokio::spawn({
        let queue = queue.clone();
        let recorded = recorded.clone();
        async move {
            writer_loop(
                RecordingSink(recorded),
                queue.clone(),
                queue.notify.clone(),
                Duration::from_secs(60),
                lifecycle,
                ack_rx,
                resources,
            )
            .await;
        }
    });
    timeout(Duration::from_secs(1), writer)
        .await
        .expect("writer should finish at the shared close deadline")
        .expect("writer should not panic");
    drop(ack_tx);

    assert!(shutdown.is_cancelled(), "queued Close should start session shutdown");
    assert_eq!(queue.try_send(Message::text("late")), Err(WsSendError::Closed));
    assert!(queue.is_empty(), "queued and in-flight capacity should be returned");
    let sent = recorded.lock().unwrap();
    assert_eq!(sent.len(), 1, "writer should send only the queued Close frame");
    let Message::Close(Some(frame)) = &sent[0] else {
        panic!("writer should preserve the queued Close frame");
    };
    assert_eq!(frame.code, 4001);
    assert_eq!(frame.reason, "custom reason");
    drop(sent);
    assert_eq!(context.active_sessions(), 0, "session registration should be released");
    assert_eq!(permits.available_permits(), 1, "policy permit should be released");
    drop(server);
}
#[tokio::test]
async fn test_writer_failure_terminates_active_session() {
    for point in [FailurePoint::Ready, FailurePoint::Send, FailurePoint::Flush] {
        assert_writer_failure(point).await;
    }
}
#[tokio::test]
async fn test_writer_clean_shutdown_does_not_report_failure() {
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
    let shutdown = CancellationToken::new();
    shutdown.cancel();
    let lifecycle = SessionLifecycle {
        server_shutdown: shutdown.clone(),
        termination: Arc::new(crate::ws::internal::TerminationState::default()),
        shutdown,
        close_code: Arc::new(std::sync::atomic::AtomicU16::new(1001)),
        close_deadline: Arc::new(std::sync::Mutex::new(None)),
        server_deadline: None,
        shutdown_timeout: Duration::from_millis(100),
    };
    let resources = Arc::new(SessionResources::new(permit, session));
    let (ack_tx, ack_rx) = oneshot::channel();
    ack_tx.send(()).expect("writer should retain acknowledgement receiver");
    tokio::spawn(
        async move {
            writer_loop(
                SuccessfulSink,
                queue,
                notify,
                Duration::from_secs(60),
                lifecycle,
                ack_rx,
                resources,
            )
            .await;
        }
        .with_subscriber(dispatch),
    )
    .await
    .expect("writer should not panic");
    let output = log.contents();
    assert!(
        !output.contains("websocket_io_failure"),
        "clean shutdown must not log a failure: {output}"
    );
    drop(server);
}
#[tokio::test]
async fn test_writer_close_send_error_reports_send_once() {
    assert_close_failure_reports("send", true, false).await;
}
#[tokio::test]
async fn test_writer_post_ack_close_flush_error_reports_flush_once() {
    assert_close_failure_reports("flush", false, true).await;
}
#[tokio::test]
async fn test_writer_close_ack_timeout_does_not_report_failure() {
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
    let shutdown = CancellationToken::new();
    shutdown.cancel();
    let lifecycle = SessionLifecycle {
        server_shutdown: shutdown.clone(),
        termination: Arc::new(crate::ws::internal::TerminationState::default()),
        shutdown,
        close_code: Arc::new(std::sync::atomic::AtomicU16::new(1001)),
        close_deadline: Arc::new(std::sync::Mutex::new(None)),
        server_deadline: None,
        shutdown_timeout: Duration::from_millis(20),
    };
    let resources = Arc::new(SessionResources::new(permit, session));
    let (_ack_tx, ack_rx) = oneshot::channel();
    let writer = tokio::spawn(
        async move {
            writer_loop(
                SuccessfulSink,
                queue,
                notify,
                Duration::from_secs(60),
                lifecycle,
                ack_rx,
                resources,
            )
            .await;
        }
        .with_subscriber(dispatch),
    );
    timeout(Duration::from_secs(1), writer)
        .await
        .expect("writer should stop after acknowledgement timeout")
        .expect("writer should not panic");
    let output = log.contents();
    assert!(
        !output.contains("websocket_io_failure"),
        "waiting for close acknowledgement must not log a failure: {output}"
    );
    drop(server);
}
#[tokio::test(start_paused = true)]
async fn test_pending_ready_times_out_after_one_idle_window_without_external_close() {
    let pending = Arc::new(Notify::new());
    let polls = Arc::new(AtomicUsize::new(0));
    assert_pending_send_times_out_after_one_idle_window(
        PendingReady {
            pending_ready: pending.clone(),
            ready_polls: polls.clone(),
        },
        pending,
        polls,
    )
    .await;
}
#[tokio::test]
async fn test_writer_close_send_obeys_shared_deadline_when_sink_blocks() {
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
    queue.try_send(Message::Close(None)).expect("Close should be queued");
    let pending_flush = Arc::new(Notify::new());
    let flush_polls = Arc::new(AtomicUsize::new(0));
    let lifecycle = SessionLifecycle {
        shutdown: CancellationToken::new(),
        server_shutdown: CancellationToken::new(),
        termination: Arc::new(crate::ws::internal::TerminationState::default()),
        close_code: Arc::new(std::sync::atomic::AtomicU16::new(1001)),
        close_deadline: Arc::new(std::sync::Mutex::new(None)),
        server_deadline: None,
        shutdown_timeout: Duration::from_millis(25),
    };
    let resources = Arc::new(SessionResources::new(permit, session));
    let (ack_tx, ack_rx) = oneshot::channel();
    let writer = tokio::spawn({
        let queue = queue.clone();
        let sink = PendingSend {
            pending_flush: pending_flush.clone(),
            flush_polls,
        };
        async move {
            writer_loop(
                sink,
                queue.clone(),
                queue.notify.clone(),
                Duration::from_secs(60),
                lifecycle,
                ack_rx,
                resources,
            )
            .await;
        }
    });

    timeout(Duration::from_secs(1), pending_flush.notified())
        .await
        .expect("Close send should reach the intentionally pending flush");
    assert!(
        !writer.is_finished(),
        "writer must remain bounded by its close deadline"
    );
    timeout(Duration::from_secs(1), writer)
        .await
        .expect("blocked Close send should stop at the shared deadline")
        .expect("writer should not panic");
    drop(ack_tx);

    assert!(
        queue.is_empty(),
        "in-flight Close budget should be released exactly once"
    );
    assert_eq!(queue.try_send(Message::text("late")), Err(WsSendError::Closed));
    assert_eq!(context.active_sessions(), 0);
    assert_eq!(permits.available_permits(), 1);
    drop(server);
}
#[tokio::test]
async fn test_writer_pending_send_shutdown_releases_resources() {
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
    let close_deadline = Arc::new(std::sync::Mutex::new(None));
    let resources = Arc::new(SessionResources::new(permit, session));
    let lifecycle = SessionLifecycle {
        shutdown: shutdown.clone(),
        server_shutdown: shutdown.clone(),
        termination: Arc::new(crate::ws::internal::TerminationState::default()),
        close_code: Arc::new(std::sync::atomic::AtomicU16::new(1001)),
        close_deadline,
        server_deadline: None,
        shutdown_timeout: Duration::from_millis(100),
    };
    let notify = queue.notify.clone();
    let writer = tokio::spawn(async move {
        let (_ack_tx, ack_rx) = oneshot::channel();
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
        .expect("actual writer send must reach pending poll_flush");
    assert!(flush_polls.load(Ordering::SeqCst) > 0);
    shutdown.cancel();
    let queue = timeout(Duration::from_secs(2), writer)
        .await
        .expect("writer should stop within the close deadline")
        .expect("writer task should not panic");

    assert!(queue.is_empty(), "in-flight queue budget should be returned");
    assert_eq!(queue.try_send(Message::text("closed")), Err(WsSendError::Closed));
    assert_eq!(context.active_sessions(), 0, "session guard should be released");
    assert_eq!(permits.available_permits(), 1, "connection permit should be released");
    drop(server);
}
