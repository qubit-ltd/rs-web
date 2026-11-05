// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Tracks active HTTP requests and the response bodies they produce.

use std::time::Duration;

use axum::body::Body;
use hyper::body::Body as HttpBody;
use tokio::sync::watch;

#[path = "activity_body.rs"]
mod activity_body;
#[path = "activity_lease.rs"]
mod activity_lease;
use activity_body::ActivityBody;
use activity_lease::ActivityLease;

/// Shared per-connection request activity counter.
#[derive(Clone, Debug)]
pub(super) struct ConnectionActivity {
    /// Internal item implementation detail.
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
        ActivityLease::new(self.active.clone())
    }

    /// Waits for one full idle interval, restarting whenever activity changes.
    /// # Parameters
    ///
    /// * `timeout` - Maximum duration allowed for the operation.
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
                    _ = async {
                        if let Some(deadline) = tokio::time::Instant::now().checked_add(timeout) {
                            tokio::time::sleep_until(deadline).await;
                        }
                    } => return,
                }
            } else if active.changed().await.is_err() {
                return;
            }
        }
    }
}

/// Wraps a response body so its request lease lasts through body completion.
/// # Parameters
///
/// * `body` - HTTP response body whose activity is retained.
/// * `lease` - Activity lease retained until the body is dropped.
pub(super) fn retain_response_activity(body: Body, lease: ActivityLease) -> Body {
    if body.is_end_stream() {
        drop(lease);
        return body;
    }
    Body::new(ActivityBody::new(body, lease))
}

/// Acquires a request lease from the connection tracker.
/// # Parameters
///
/// * `activity` - Connection activity tracker from which the request lease is
///   acquired.
pub(super) fn request_lease(activity: &ConnectionActivity) -> ActivityLease {
    activity.acquire()
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use tokio::time::timeout;

    use super::ConnectionActivity;

    #[tokio::test]
    async fn test_unrepresentable_idle_deadline_ends_the_wait_immediately() {
        timeout(
            Duration::from_millis(100),
            ConnectionActivity::new().wait_until_idle_for(Duration::MAX),
        )
        .await
        .expect("an unrepresentable idle deadline must not create an unbounded wait");
    }
}
