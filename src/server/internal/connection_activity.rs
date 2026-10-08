// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Tracks active HTTP requests and the response bodies they produce.

use std::pin::Pin;
use std::task::Context;
use std::task::Poll;
use std::time::Duration;

use axum::body::Body;
use axum::body::Bytes;
use hyper::body::Body as HttpBody;
use hyper::body::SizeHint;
use tokio::sync::watch;

/// Shared per-connection request activity counter.
#[derive(Clone, Debug)]
pub(super) struct ConnectionActivity {
    active: watch::Sender<usize>,
}

impl ConnectionActivity {
    /// Creates an inactive connection tracker.
    pub(super) fn new() -> Self {
        let (active, _) = watch::channel(0);
        Self { active }
    }

    /// Acquires a lease that keeps the connection active until dropped.
    fn acquire(&self) -> ActivityLease {
        self.active.send_modify(|active| *active += 1);
        ActivityLease {
            active: self.active.clone(),
        }
    }

    /// Waits for one full idle interval, restarting whenever activity changes.
    pub(super) async fn wait_until_idle_for(&self, timeout: Duration) {
        let mut active = self.active.subscribe();
        loop {
            if *active.borrow_and_update() == 0 {
                tokio::select! {
                    biased;
                    changed = active.changed() => {
                        if changed.is_err() {
                            return;
                        }
                    }
                    _ = tokio::time::sleep(timeout) => return,
                }
            } else if active.changed().await.is_err() {
                return;
            }
        }
    }
}

/// One active request or response-body lifetime.
pub(super) struct ActivityLease {
    active: watch::Sender<usize>,
}

impl Drop for ActivityLease {
    /// Decrements the connection activity count exactly once.
    fn drop(&mut self) {
        self.active.send_modify(|active| *active -= 1);
    }
}

/// Response body retaining a request lease through EOF, error, or drop.
struct ActivityBody {
    inner: Body,
    lease: Option<ActivityLease>,
}

impl HttpBody for ActivityBody {
    type Data = Bytes;
    type Error = <Body as HttpBody>::Error;

    /// Polls the response body and releases its lease at completion or error.
    fn poll_frame(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
    ) -> Poll<Option<Result<hyper::body::Frame<Self::Data>, Self::Error>>> {
        let result = Pin::new(&mut self.inner).poll_frame(context);
        if matches!(&result, Poll::Ready(None) | Poll::Ready(Some(Err(_)))) {
            self.lease.take();
        }
        result
    }

    /// Reports whether the wrapped body has completed.
    fn is_end_stream(&self) -> bool {
        self.inner.is_end_stream()
    }

    /// Returns the wrapped body's size hint.
    fn size_hint(&self) -> SizeHint {
        self.inner.size_hint()
    }
}

/// Wraps a response body so its request lease lasts through body completion.
pub(super) fn retain_response_activity(body: Body, lease: ActivityLease) -> Body {
    if body.is_end_stream() {
        drop(lease);
        return body;
    }
    Body::new(ActivityBody {
        inner: body,
        lease: Some(lease),
    })
}

/// Acquires a request lease from the connection tracker.
pub(super) fn request_lease(activity: &ConnectionActivity) -> ActivityLease {
    activity.acquire()
}
