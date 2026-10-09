// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;

use axum::body::Body;
use axum::body::Bytes;
use hyper::body::Body as HttpBody;
use hyper::body::SizeHint;

use super::activity_lease::ActivityLease;

/// Response body retaining a request lease through EOF, error, or drop.
pub(super) struct ActivityBody {
    inner: Body,
    lease: Option<ActivityLease>,
}

impl ActivityBody {
    /// Wraps a body and keeps its request activity lease until completion.
    pub(super) fn new(inner: Body, lease: ActivityLease) -> Self {
        Self {
            inner,
            lease: Some(lease),
        }
    }
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
