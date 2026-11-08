// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::Ordering;
use std::time::Duration;

use axum::Router;
use axum::extract::State;
use axum::extract::WebSocketUpgrade;
use axum::extract::ws::Message;
use axum::http::HeaderMap;
use axum::http::StatusCode;
use axum::response::Response;
use axum::routing::get;
use tokio::sync::Notify;
use tokio::sync::oneshot;
use tokio_util::sync::CancellationToken;

use super::super::operation_deadline;
use super::support::TEST_PENDING_OBSERVER;
use super::support::TestPendingObserver;
use super::support::TestPendingObserverGuard;
use super::support::TestWriterEvent;
use super::support::test_handshake;
use super::support::test_send_masked_text;
use crate::ServerOptions;
use crate::WebServer;
use crate::ws::WsTermination;
use crate::ws::WsUpgradePolicy;

#[tokio::test]
async fn test_real_network_write_stall_times_out_and_releases_capacity() {
    const IDLE_TIMEOUT: Duration = Duration::from_millis(500);
    const MESSAGE_BYTES: usize = 32 * 1024 * 1024;
    const STALL_MARKER: &[u8] = b"rs-web-write-stall-20261107";

    type HandlerState = (
        WsUpgradePolicy,
        crate::server::ServerContext,
        Arc<Notify>,
        Arc<std::sync::atomic::AtomicUsize>,
        Arc<std::sync::atomic::AtomicUsize>,
        Arc<Mutex<Option<oneshot::Sender<Option<WsTermination>>>>>,
    );

    async fn upgrade(
        State((policy, context, admitted, inbound, connection_count, termination_tx)): State<HandlerState>,
        headers: HeaderMap,
        ws: WebSocketUpgrade,
    ) -> Response {
        policy.on_upgrade(ws, &headers, context, move |mut session| async move {
            if connection_count.fetch_add(1, Ordering::SeqCst) == 0 {
                let mut payload = vec![0x5a; MESSAGE_BYTES];
                payload[..STALL_MARKER.len()].copy_from_slice(STALL_MARKER);
                session
                    .try_send(Message::Binary(payload.into()))
                    .expect("large server message should enter the bounded queue");
                admitted.notify_one();
            } else {
                session
                    .try_send(Message::text("second-session"))
                    .expect("second session message should enter the bounded queue");
            }
            while let Some(Ok(message)) = session.recv().await {
                if matches!(message, Message::Text(_)) {
                    inbound.fetch_add(1, Ordering::SeqCst);
                }
            }
            let _ = termination_tx
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .take()
                .unwrap()
                .send(session.termination());
        })
    }

    let (event_tx, mut event_rx) = tokio::sync::mpsc::unbounded_channel();
    let observer = TestPendingObserver {
        marker: Arc::from(STALL_MARKER),
        events: event_tx,
    };
    let observer_slot = TEST_PENDING_OBSERVER.get_or_init(|| Mutex::new(None));
    *observer_slot.lock().unwrap_or_else(std::sync::PoisonError::into_inner) = Some(observer);
    let _observer_guard = TestPendingObserverGuard(observer_slot);

    let policy = WsUpgradePolicy::new()
        .max_connections(1)
        .max_frame_bytes(MESSAGE_BYTES)
        .max_message_bytes(MESSAGE_BYTES)
        .queue_limits(1, MESSAGE_BYTES)
        .idle_timeout(Duration::from_secs(60))
        .write_timeout(IDLE_TIMEOUT);
    let server = WebServer::bind_http(
        ServerOptions::new("127.0.0.1:0".parse().unwrap()).with_shutdown_timeout(Duration::from_millis(200)),
    )
    .await
    .expect("test WebServer should bind");
    let address = server.local_addr();
    let context = server.context();
    let shutdown = CancellationToken::new();
    let admitted = Arc::new(Notify::new());
    let inbound = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let connection_count = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let (termination_tx, termination_rx) = oneshot::channel();
    let app = Router::new().route(
        "/ws",
        get(upgrade).with_state((
            policy.clone(),
            context.clone(),
            admitted.clone(),
            inbound.clone(),
            connection_count,
            Arc::new(Mutex::new(Some(termination_tx))),
        )),
    );
    let serve_shutdown = shutdown.clone();
    let serving = tokio::spawn(async move { server.serve(app, serve_shutdown.cancelled_owned()).await });

    let (client, status) = test_handshake(address).await;
    assert_eq!(status, StatusCode::SWITCHING_PROTOCOLS);
    admitted.notified().await;
    let (client_read, client_write) = client.into_split();
    let inbound_writer = tokio::spawn(async move {
        let mut client_write = client_write;
        let mut interval = tokio::time::interval(Duration::from_millis(50));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            interval.tick().await;
            test_send_masked_text(&mut client_write, b"inbound-progress").await;
        }
    });

    let started = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            match event_rx.recv().await {
                Some(TestWriterEvent::SendStarted(at)) => break at,
                Some(_) => {}
                None => panic!("writer observer channel closed before target send"),
            }
        }
    })
    .await
    .expect("writer should start the marked network send");
    let first_pending = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            match event_rx.recv().await {
                Some(TestWriterEvent::FlushPending(at)) => break at,
                Some(TestWriterEvent::FlushReady(_)) => {
                    panic!("large marked send completed before the stall window")
                }
                Some(TestWriterEvent::SendStarted(_)) => {}
                None => panic!("writer observer channel closed before Pending"),
            }
        }
    })
    .await
    .expect("actual WebSocket sink should reach poll_flush Pending");
    assert!(first_pending >= started);

    let mut inbound_timeline = Vec::new();
    let previous_inbound = inbound.load(Ordering::SeqCst);
    tokio::time::timeout(Duration::from_secs(15), async {
            let deadline = started + IDLE_TIMEOUT;
            let midpoint = first_pending + (deadline - first_pending) / 2;
            let mut sampled_midpoint = false;
            loop {
                tokio::select! {
                    _ = tokio::time::sleep_until(deadline) => break,
                    _ = tokio::time::sleep_until(midpoint), if !sampled_midpoint => {
                        let count = inbound.load(Ordering::SeqCst);
                        assert!(count > previous_inbound, "application inbound count must advance while the write is pending");
                        assert_eq!(policy.active_connections(), 1, "pending writer retains its policy permit before timeout");
                        assert_eq!(context.active_sessions(), 1, "session remains registered before timeout");
                        sampled_midpoint = true;
                    }
                    event = event_rx.recv() => match event {
                        Some(TestWriterEvent::FlushPending(_)) => {},
                        Some(TestWriterEvent::FlushReady(at)) => {
                            panic!("large send completed before its write deadline at {at:?}")
                        }
                        Some(TestWriterEvent::SendStarted(_)) => {},
                        None => panic!("writer observer channel closed during observation"),
                    }
                }
            }
            let count = inbound.load(Ordering::SeqCst);
            assert!(count > previous_inbound, "application inbound count must advance during the write deadline");
            inbound_timeline.push((1, tokio::time::Instant::now(), count));
            assert!(
                !shutdown.is_cancelled(),
                "the server was not shut down during observation"
            );
        })
        .await
        .expect("write timeout observation must remain within the 15 second bound");

    tokio::time::timeout(Duration::from_secs(3), async {
        while policy.active_connections() != 0 || context.active_sessions() != 0 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("write timeout should release both I/O tasks and session capacity");
    assert_eq!(
        termination_rx.await.expect("handler should observe completion"),
        Some(WsTermination::WriteTimeout),
        "the independent write deadline should publish the write timeout cause",
    );

    let (second_client, second_status) = test_handshake(address).await;
    assert_eq!(second_status, StatusCode::SWITCHING_PROTOCOLS);
    drop(second_client);

    inbound_writer.abort();
    let _ = inbound_writer.await;
    drop(client_read);
    shutdown.cancel();
    let report = tokio::time::timeout(Duration::from_secs(3), serving)
        .await
        .expect("server cleanup should be bounded")
        .expect("server task should not panic")
        .expect("server should report successful shutdown");
    assert!(report.graceful, "server should finish cleanup within shutdown_timeout");
    assert_eq!(
        policy.active_connections(),
        0,
        "shutdown should release policy capacity"
    );
    assert_eq!(
        context.active_sessions(),
        0,
        "shutdown should release session registration"
    );
    println!("writer_send_started={started:?}");
    println!("writer_flush_pending={first_pending:?}");
    println!("inbound_windows={inbound_timeline:?}");
    println!("second_upgrade_status={second_status}");
    println!("cleanup_active_connections={}", policy.active_connections());
    println!("cleanup_active_sessions={}", context.active_sessions());
}
#[test]
fn test_operation_deadline_is_immediate_when_duration_cannot_be_added() {
    let deadline = operation_deadline(Duration::MAX);

    assert!(deadline <= tokio::time::Instant::now());
}
