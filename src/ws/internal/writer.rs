// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Outbound WebSocket writer loop.

use std::sync::Arc;
use std::sync::atomic::Ordering;

use axum::extract::ws::Message;
use futures_util::Sink;
use futures_util::SinkExt;
use tokio::select;
use tokio::sync::Notify;
use tokio::sync::OwnedSemaphorePermit;
use tokio::sync::oneshot;
use tokio::time::Instant;
use tokio::time::timeout_at;
use tokio_util::sync::CancellationToken;

use super::SessionLifecycle;
use super::close::get_close_deadline;
use super::close::send_close_and_wait;
use crate::server::SessionGuard;
use crate::ws::internal::CloseQueueOnDrop;
use crate::ws::ws_send_queue::WsSendQueue;
use crate::ws::ws_send_queue::message_size;

/// Drains the session queue through a sink until shutdown or a terminal error.
///
/// The sink is generic so tests can exercise actual writer-loop behavior with
/// deterministic readiness and flush results; production uses Axum's split
/// WebSocket sink.
pub(in crate::ws) async fn writer_loop<S>(
    mut sink: S,
    queue: WsSendQueue,
    notify: Arc<Notify>,
    lifecycle: SessionLifecycle,
    mut close_ack: oneshot::Receiver<()>,
    _permit: OwnedSemaphorePermit,
    _session: SessionGuard,
) where
    S: Sink<Message> + Unpin,
{
    let SessionLifecycle {
        shutdown,
        close_code,
        close_deadline,
        server_deadline,
        shutdown_timeout,
    } = lifecycle;
    let _close_queue_on_drop = CloseQueueOnDrop::new(queue.clone());
    loop {
        if shutdown.is_cancelled() {
            let deadline = get_close_deadline(&close_deadline, server_deadline.as_ref(), shutdown_timeout);
            send_close_and_wait(&mut sink, &mut close_ack, close_code.load(Ordering::Acquire), deadline).await;
            return;
        }
        let message = loop {
            let message = queue.take_next();
            if message.is_some() {
                break message;
            }
            select! {
                biased;
                _ = shutdown.cancelled() => {
                    let deadline = get_close_deadline(&close_deadline, server_deadline.as_ref(), shutdown_timeout);
                    send_close_and_wait(&mut sink, &mut close_ack, close_code.load(Ordering::Acquire), deadline).await;
                    return;
                }
                _ = &mut close_ack => {
                    let deadline = get_close_deadline(&close_deadline, server_deadline.as_ref(), shutdown_timeout);
                    let _ = flush_until(&mut sink, &shutdown, Some(deadline)).await;
                    return;
                }
                _ = notify.notified() => {
                    match flush_until(&mut sink, &shutdown, None).await {
                        FlushOutcome::Flushed => {}
                        FlushOutcome::Cancelled => {
                            let deadline = get_close_deadline(&close_deadline, server_deadline.as_ref(), shutdown_timeout);
                            send_close_and_wait(&mut sink, &mut close_ack, close_code.load(Ordering::Acquire), deadline).await;
                            return;
                        }
                        FlushOutcome::Failed | FlushOutcome::TimedOut => {
                            shutdown.cancel();
                            return;
                        }
                    }
                }
            }
        };
        let Some(message) = message else { continue };
        let message_bytes = message_size(&message);
        let sent = select! {
            biased;
            _ = shutdown.cancelled() => None,
            _ = &mut close_ack => Some(Err(())),
            result = sink.send(message) => Some(result.map_err(|_| ())),
        };
        queue.finish_message(message_bytes);
        match sent {
            Some(Ok(())) => {}
            Some(Err(())) => {
                let deadline = get_close_deadline(&close_deadline, server_deadline.as_ref(), shutdown_timeout);
                let _ = flush_until(&mut sink, &shutdown, Some(deadline)).await;
                return;
            }
            None => {
                let deadline = get_close_deadline(&close_deadline, server_deadline.as_ref(), shutdown_timeout);
                send_close_and_wait(&mut sink, &mut close_ack, close_code.load(Ordering::Acquire), deadline).await;
                return;
            }
        }
    }
}

#[derive(Debug, Eq, PartialEq)]
enum FlushOutcome {
    Flushed,
    Cancelled,
    Failed,
    TimedOut,
}

async fn flush_until<S>(sink: &mut S, shutdown: &CancellationToken, deadline: Option<Instant>) -> FlushOutcome
where
    S: Sink<Message> + Unpin,
{
    let flushing = sink.flush();
    tokio::pin!(flushing);
    match deadline {
        Some(deadline) => tokio::select! {
            biased;
            _ = shutdown.cancelled() => FlushOutcome::Cancelled,
            result = timeout_at(deadline, &mut flushing) => match result {
                Ok(Ok(())) => FlushOutcome::Flushed,
                Ok(Err(_)) => FlushOutcome::Failed,
                Err(_) => FlushOutcome::TimedOut,
            },
        },
        None => tokio::select! {
            biased;
            _ = shutdown.cancelled() => FlushOutcome::Cancelled,
            result = &mut flushing => if result.is_ok() {
                FlushOutcome::Flushed
            } else {
                FlushOutcome::Failed
            },
        },
    }
}

#[cfg(test)]
mod tests {
    use std::pin::Pin;
    use std::sync::Arc;
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

    use super::FlushOutcome;
    use super::flush_until;
    use super::writer_loop;
    use crate::ServerOptions;
    use crate::WebServer;
    use crate::ws::WsSendQueue;
    use crate::ws::internal::SessionLifecycle;
    use crate::ws::ws_send_error::WsSendError;

    struct PendingFlush {
        polled: std::sync::Arc<Notify>,
    }

    impl Sink<Message> for PendingFlush {
        type Error = std::io::Error;

        fn poll_ready(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }

        fn start_send(self: Pin<&mut Self>, _item: Message) -> Result<(), Self::Error> {
            Ok(())
        }

        fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            self.get_mut().polled.notify_one();
            Poll::Pending
        }

        fn poll_close(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }
    }

    struct ReadyFlush(Option<Result<(), std::io::Error>>);

    impl Sink<Message> for ReadyFlush {
        type Error = std::io::Error;

        fn poll_ready(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }

        fn start_send(self: Pin<&mut Self>, _item: Message) -> Result<(), Self::Error> {
            Ok(())
        }

        fn poll_flush(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(self.0.take().expect("poll_flush should be called once"))
        }

        fn poll_close(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }
    }

    struct PendingSend {
        pending_flush: Arc<Notify>,
        flush_polls: Arc<AtomicUsize>,
    }

    impl Sink<Message> for PendingSend {
        type Error = std::io::Error;

        fn poll_ready(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }

        fn start_send(self: Pin<&mut Self>, _item: Message) -> Result<(), Self::Error> {
            Ok(())
        }

        fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            if self.flush_polls.fetch_add(1, Ordering::SeqCst) == 0 {
                self.pending_flush.notify_one();
            }
            Poll::Pending
        }

        fn poll_close(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }
    }

    #[tokio::test]
    async fn pending_flush_is_cancelled() {
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
    async fn pending_flush_obeys_deadline() {
        let mut sink = PendingFlush {
            polled: std::sync::Arc::new(Notify::new()),
        };
        let token = CancellationToken::new();
        let result = flush_until(&mut sink, &token, Some(tokio::time::Instant::now())).await;
        assert_eq!(result, FlushOutcome::TimedOut);
    }

    #[tokio::test]
    async fn ready_flush_succeeds() {
        let mut sink = ReadyFlush(Some(Ok(())));
        let token = CancellationToken::new();
        let result = flush_until(&mut sink, &token, None).await;
        assert_eq!(result, FlushOutcome::Flushed);
    }

    #[tokio::test]
    async fn failed_flush_is_reported() {
        let mut sink = ReadyFlush(Some(Err(std::io::Error::other("injected"))));
        let token = CancellationToken::new();
        let result = flush_until(&mut sink, &token, None).await;
        assert_eq!(result, FlushOutcome::Failed);
    }

    #[tokio::test]
    async fn writer_pending_send_shutdown_releases_resources() {
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
        let lifecycle = SessionLifecycle {
            shutdown: shutdown.clone(),
            close_code: Arc::new(std::sync::atomic::AtomicU16::new(1001)),
            close_deadline,
            server_deadline: None,
            shutdown_timeout: Duration::from_millis(100),
        };
        let notify = queue.notify.clone();
        let writer = tokio::spawn(async move {
            let (_ack_tx, ack_rx) = oneshot::channel();
            writer_loop(sink, queue.clone(), notify, lifecycle, ack_rx, permit, session).await;
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
}
