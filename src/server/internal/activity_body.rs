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

/// Response body retaining a request lease until completion, error, or drop.
pub(super) struct ActivityBody {
    /// The response body whose frames are forwarded unchanged.
    inner: Body,
    /// The request activity lease, released after EOF, an error, or a
    /// successful final frame is observed, or when this wrapper is dropped
    /// while held.
    lease: Option<ActivityLease>,
}

impl ActivityBody {
    /// Wraps a body and keeps its request activity lease until completion.
    /// # Parameters
    ///
    /// * `inner` - Wrapped HTTP body polled by this activity-aware body.
    /// * `lease` - Activity lease retained until the body is dropped.
    pub(super) fn new(inner: Body, lease: ActivityLease) -> Self {
        Self {
            inner,
            lease: Some(lease),
        }
    }
}

impl HttpBody for ActivityBody {
    /// HTTP response data frame payload type.
    type Data = Bytes;
    /// Axum error returned by the wrapped response body.
    type Error = <Body as HttpBody>::Error;

    /// Polls the response body and releases its lease at completion or error.
    /// # Parameters
    ///
    /// * `context` - Poll context passed to the wrapped response body.
    fn poll_frame(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
    ) -> Poll<Option<Result<hyper::body::Frame<Self::Data>, Self::Error>>> {
        let result = Pin::new(&mut self.inner).poll_frame(context);
        if matches!(&result, Poll::Ready(None) | Poll::Ready(Some(Err(_))))
            || (matches!(&result, Poll::Ready(Some(Ok(_)))) && self.inner.is_end_stream())
        {
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

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::convert::Infallible;
    use std::pin::Pin;
    use std::task::Context;
    use std::task::Poll;

    use axum::body::Body;
    use axum::body::Bytes;
    use hyper::body::Body as HttpBody;
    use tokio::sync::watch;

    use super::super::activity_lease::ActivityLease;
    use super::ActivityBody;

    struct FinalFrameBody(Option<Bytes>);

    impl HttpBody for FinalFrameBody {
        type Data = Bytes;
        type Error = Infallible;

        fn poll_frame(
            mut self: Pin<&mut Self>,
            _: &mut Context<'_>,
        ) -> Poll<Option<Result<hyper::body::Frame<Self::Data>, Self::Error>>> {
            Poll::Ready(self.0.take().map(|data| Ok(hyper::body::Frame::data(data))))
        }

        fn is_end_stream(&self) -> bool {
            self.0.is_none()
        }
    }

    enum Event {
        Pending,
        Data(&'static [u8]),
        Error,
    }

    struct ScriptedBody(VecDeque<Event>);

    impl HttpBody for ScriptedBody {
        type Data = Bytes;
        type Error = &'static str;

        fn poll_frame(
            mut self: Pin<&mut Self>,
            _: &mut Context<'_>,
        ) -> Poll<Option<Result<hyper::body::Frame<Self::Data>, Self::Error>>> {
            match self.0.pop_front() {
                Some(Event::Pending) => Poll::Pending,
                Some(Event::Data(data)) => Poll::Ready(Some(Ok(hyper::body::Frame::data(Bytes::from_static(data))))),
                Some(Event::Error) => Poll::Ready(Some(Err("scripted error"))),
                None => Poll::Ready(None),
            }
        }

        fn is_end_stream(&self) -> bool {
            self.0.is_empty()
        }
    }

    #[tokio::test]
    async fn test_activity_body_releases_lease_on_final_frame_and_only_once() {
        let (active, mut observed) = watch::channel(1_usize);
        let mut body = Box::pin(ActivityBody::new(
            Body::new(FinalFrameBody(Some(Bytes::from_static(b"final")))),
            ActivityLease::new(active),
        ));
        let waker = futures_util::task::noop_waker();
        let mut context = Context::from_waker(&waker);

        let frame = match body.as_mut().poll_frame(&mut context) {
            Poll::Ready(Some(Ok(frame))) => frame,
            other => panic!("expected final data frame, got {other:?}"),
        };
        assert_eq!(frame.into_data().expect("final frame is data"), b"final"[..]);
        assert_eq!(*observed.borrow_and_update(), 0);

        assert!(matches!(body.as_mut().poll_frame(&mut context), Poll::Ready(None)));
        drop(body);
        assert_eq!(*observed.borrow_and_update(), 0);
    }

    #[tokio::test]
    async fn test_activity_body_retains_lease_until_completion_error_or_drop() {
        let (active, mut observed) = watch::channel(1_usize);
        let mut body = Box::pin(ActivityBody::new(
            Body::new(ScriptedBody(VecDeque::from([
                Event::Pending,
                Event::Data(b"first"),
                Event::Data(b"last"),
            ]))),
            ActivityLease::new(active),
        ));
        let waker = futures_util::task::noop_waker();
        let mut context = Context::from_waker(&waker);

        assert!(matches!(body.as_mut().poll_frame(&mut context), Poll::Pending));
        assert_eq!(*observed.borrow_and_update(), 1);
        let first = body.as_mut().poll_frame(&mut context);
        assert!(matches!(first, Poll::Ready(Some(Ok(_)))));
        assert_eq!(*observed.borrow_and_update(), 1);
        let last = body.as_mut().poll_frame(&mut context);
        let Poll::Ready(Some(Ok(frame))) = last else {
            panic!("expected final frame")
        };
        assert_eq!(frame.into_data().unwrap(), b"last"[..]);
        assert_eq!(*observed.borrow_and_update(), 0);
        assert!(matches!(body.as_mut().poll_frame(&mut context), Poll::Ready(None)));
        drop(body);
        assert_eq!(*observed.borrow_and_update(), 0);

        let (active, mut observed) = watch::channel(1_usize);
        let mut errored = Box::pin(ActivityBody::new(
            Body::new(ScriptedBody(VecDeque::from([Event::Error]))),
            ActivityLease::new(active),
        ));
        assert!(matches!(
            errored.as_mut().poll_frame(&mut context),
            Poll::Ready(Some(Err(_)))
        ));
        assert_eq!(*observed.borrow_and_update(), 0);
        drop(errored);
        assert_eq!(*observed.borrow_and_update(), 0);

        let (active, mut observed) = watch::channel(1_usize);
        let mut eof = Box::pin(ActivityBody::new(
            Body::new(ScriptedBody(VecDeque::new())),
            ActivityLease::new(active),
        ));
        assert!(matches!(eof.as_mut().poll_frame(&mut context), Poll::Ready(None)));
        assert_eq!(*observed.borrow_and_update(), 0);
        drop(eof);
        assert_eq!(*observed.borrow_and_update(), 0);

        let (active, mut observed) = watch::channel(1_usize);
        let dropped = ActivityBody::new(
            Body::new(ScriptedBody(VecDeque::from([Event::Pending]))),
            ActivityLease::new(active),
        );
        drop(dropped);
        assert_eq!(*observed.borrow_and_update(), 0);
    }
}
