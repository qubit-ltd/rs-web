// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use tokio::sync::OwnedSemaphorePermit;
use tokio_util::sync::CancellationToken;

use crate::server::SessionGuard;

/// Connection capacity and server session registration shared by both I/O
/// tasks.
///
/// The permit and guard are released only after the reader and writer have both
/// dropped their `Arc` references to this value.
pub(in crate::ws) struct SessionResources {
    resources: Option<(OwnedSemaphorePermit, SessionGuard)>,
    closed: CancellationToken,
}

impl SessionResources {
    /// Holds the resources reserved for one admitted WebSocket connection.
    pub(in crate::ws) fn new(permit: OwnedSemaphorePermit, session: SessionGuard) -> Self {
        Self {
            resources: Some((permit, session)),
            closed: CancellationToken::new(),
        }
    }

    pub(in crate::ws) fn closed_token(&self) -> CancellationToken {
        self.closed.clone()
    }
}

impl Drop for SessionResources {
    fn drop(&mut self) {
        drop(self.resources.take());
        self.closed.cancel();
    }
}

#[cfg(test)]
mod tests {
    use std::future::Future;
    use std::sync::Arc;
    use std::task::Poll;
    use std::time::Duration;

    use tokio::sync::Semaphore;
    use tokio::sync::oneshot;
    use tokio::time::timeout;

    use super::SessionResources;
    use crate::ServerOptions;
    use crate::WebServer;

    #[tokio::test]
    async fn notifies_only_after_last_io_reference_releases_resources() {
        let server = WebServer::bind_http(ServerOptions::new("127.0.0.1:0".parse().unwrap()))
            .await
            .expect("test server should bind");
        let context = server.context();
        let session = context.try_register_session().expect("session should register");
        let semaphore = Arc::new(Semaphore::new(1));
        let permit = semaphore
            .clone()
            .try_acquire_owned()
            .expect("permit should be available");
        let resources = Arc::new(SessionResources::new(permit, session));
        let closed = resources.closed_token();
        let io_reference = resources.clone();
        drop(resources);

        assert_eq!(semaphore.available_permits(), 0);
        assert_eq!(context.active_sessions(), 1);
        assert!(timeout(Duration::from_millis(10), closed.cancelled()).await.is_err());

        drop(io_reference);
        timeout(Duration::from_secs(1), closed.cancelled())
            .await
            .expect("last I/O reference should publish completion");
        assert_eq!(semaphore.available_permits(), 1);
        assert_eq!(context.active_sessions(), 0);
    }

    #[tokio::test]
    async fn supports_multiple_waiters_late_registration_and_waiter_cancellation() {
        let server = WebServer::bind_http(ServerOptions::new("127.0.0.1:0".parse().unwrap()))
            .await
            .expect("test server should bind");
        let session = server
            .context()
            .try_register_session()
            .expect("session should register");
        let resources = Arc::new(SessionResources::new(
            Arc::new(Semaphore::new(1)).try_acquire_owned().unwrap(),
            session,
        ));
        let token = resources.closed_token();
        let cancelled_waiter = token.clone();
        let surviving_waiter = token.clone();
        let (pending_tx, pending_rx) = oneshot::channel();
        let mut pending_tx = Some(pending_tx);
        let waiter = tokio::spawn(async move {
            let mut cancellation = Box::pin(cancelled_waiter.cancelled());
            std::future::poll_fn(|context| {
                assert!(cancellation.as_mut().poll(context).is_pending());
                let _ = pending_tx.take().expect("pending signal is sent once").send(());
                Poll::Ready(())
            })
            .await;
            cancellation.await;
        });
        pending_rx.await.expect("waiter should be pending before it is aborted");
        waiter.abort();
        assert!(
            waiter
                .await
                .expect_err("aborted waiter should fail to join")
                .is_cancelled()
        );
        let second = tokio::spawn(async move { surviving_waiter.cancelled().await });
        drop(resources);
        timeout(Duration::from_secs(1), second)
            .await
            .expect("remaining waiter should wake")
            .expect("waiter task should complete");
        timeout(Duration::from_secs(1), token.cancelled())
            .await
            .expect("late waiter should complete immediately");
    }
}
