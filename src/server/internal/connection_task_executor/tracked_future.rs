// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use std::future::Future;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;

use super::task_completion_guard::TaskCompletionGuard;

/// Wraps an application future so task completion is recorded after its
/// resources and output have been dropped.
pub(super) struct TrackedFuture<F> {
    /// Pinned application work, released before the completion guard.
    pub(super) future: Option<Pin<Box<F>>>,
    /// Updates active-task and completion-time state when dropped.
    pub(super) completion: Option<TaskCompletionGuard>,
}

impl<F> Future for TrackedFuture<F>
where
    F: Future,
{
    type Output = ();

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
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
                this.completion.take();
                Poll::Ready(())
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

impl<F> Drop for TrackedFuture<F> {
    fn drop(&mut self) {
        self.future.take();
        self.completion.take();
    }
}
