// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Bounded outbound WebSocket message queue.

use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::PoisonError;

use axum::extract::ws::Message;
use tokio::sync::Notify;

use super::internal::QueueState;
use super::ws_send_error::WsSendError;
use super::ws_send_failure::WsSendFailure;

/// A nonblocking bounded queue for outbound WebSocket messages.
///
/// Closing the queue rejects future sends and discards pending messages while
/// preserving accounting for messages already being sent.
///
/// Debug output contains only queue counts, capacity limits, and closed state;
/// message payloads are omitted.
///
/// # Examples
///
/// ```
/// use axum::extract::ws::Message;
/// use qubit_web::ws::WsSendQueue;
///
/// let queue = WsSendQueue::new(4, 1024);
/// queue.try_send(Message::text("ready")).expect("capacity available");
/// assert_eq!(queue.len(), 1);
/// ```
#[derive(Clone)]
#[must_use]
pub struct WsSendQueue {
    /// Mutex-protected pending and in-flight queue accounting.
    pub(super) state: Arc<Mutex<QueueState>>,
    /// Wake-up signal consumed by the dedicated writer task.
    pub(super) notify: Arc<Notify>,
    /// Maximum pending and in-flight message count.
    max_messages: usize,
    /// Maximum bytes retained by pending and in-flight messages.
    max_bytes: usize,
}

impl std::fmt::Debug for WsSendQueue {
    /// Formats the value using its stable diagnostic representation.
    /// # Parameters
    ///
    /// * `f` - Destination receiving the queue summary.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        f.debug_struct("WsSendQueue")
            .field("len", &(state.messages.len() + state.in_flight_messages))
            .field("queued_bytes", &state.queued_bytes)
            .field("max_messages", &self.max_messages)
            .field("max_bytes", &self.max_bytes)
            .field("closed", &state.closed)
            .finish()
    }
}

impl WsSendQueue {
    /// Creates a queue bounded by message count and total payload bytes.
    ///
    /// # Parameters
    ///
    /// * `max_messages` - Maximum queued plus in-flight message count. Zero
    ///   rejects every message with [`WsSendError::Backpressure`].
    /// * `max_bytes` - Maximum total payload bytes. Zero permits only
    ///   zero-payload messages.
    ///
    /// [`Self::try_send`] checks errors in this order: [`WsSendError::Closed`],
    /// [`WsSendError::InvalidCloseCode`], [`WsSendError::MessageTooLarge`],
    /// then [`WsSendError::Backpressure`].
    /// # Returns
    ///
    /// An empty queue with the supplied capacity bounds.
    pub fn new(max_messages: usize, max_bytes: usize) -> Self {
        Self {
            state: Arc::new(Mutex::new(QueueState {
                messages: VecDeque::new(),
                queued_bytes: 0,
                in_flight_messages: 0,
                closed: false,
            })),
            notify: Arc::new(Notify::new()),
            max_messages,
            max_bytes,
        }
    }

    /// Returns the number of queued messages.
    ///
    /// # Returns
    ///
    /// The queued plus currently in-flight message count.
    #[must_use]
    #[inline]
    pub fn len(&self) -> usize {
        let state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        state.messages.len() + state.in_flight_messages
    }

    /// Returns whether no message is queued.
    ///
    /// # Returns
    ///
    /// `true` when no queued or in-flight messages remain.
    #[must_use]
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Enqueues a message without waiting.
    ///
    /// # Parameters
    ///
    /// * `message` - Text, binary, ping, pong, or close message to enqueue.
    /// # Returns
    ///
    /// `Ok(())` when accepted into the queue. On rejection, the message is
    /// discarded; use [`Self::try_send_recoverable`] to retain it.
    ///
    /// # Errors
    ///
    /// Returns [`WsSendError::Closed`] if the queue is closed,
    /// [`WsSendError::InvalidCloseCode`] if a Close frame uses a code a server
    /// cannot send,
    /// [`WsSendError::MessageTooLarge`] if a Ping/Pong payload exceeds 125
    /// bytes, a Close reason exceeds 123 UTF-8 bytes, or the message exceeds
    /// the configured total byte budget. Returns [`WsSendError::Backpressure`]
    /// if the queue lacks message count or remaining byte capacity. These
    /// checks occur in that order after the closed check. Protocol limits
    /// count bytes; payloads are rejected without truncation.
    /// Success means only that the message entered the queue, not that it was
    /// delivered; a message still pending at closure may be discarded.
    pub fn try_send(&self, message: Message) -> Result<(), WsSendError> {
        self.try_send_recoverable(message).map_err(|failure| failure.kind())
    }

    /// Attempts to enqueue a message and returns ownership if it is rejected.
    ///
    /// Admission uses the same order as [`Self::try_send`]: closed queue,
    /// invalid server Close code, message limits, then remaining queue
    /// capacity. A successful result means only that the message entered the
    /// queue; it may still be discarded if the session closes before writing.
    ///
    /// # Parameters
    ///
    /// * `message` - Outbound message to enqueue.
    /// # Returns
    ///
    /// `Ok(())` if accepted, or a [`WsSendFailure`] owning the original
    /// message if rejected.
    /// # Errors
    ///
    /// Returns a failure with kind [`WsSendError::Closed`],
    /// [`WsSendError::InvalidCloseCode`], [`WsSendError::MessageTooLarge`], or
    /// [`WsSendError::Backpressure`]. The rejected message is retained without
    /// cloning, truncation, or re-encoding.
    pub fn try_send_recoverable(&self, message: Message) -> Result<(), WsSendFailure> {
        let bytes = message_size(&message);
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        if state.closed {
            return Err(WsSendFailure::new(WsSendError::Closed, message));
        }
        if let Message::Close(Some(frame)) = &message {
            let code = tungstenite::protocol::frame::coding::CloseCode::from(frame.code);
            if !code.is_allowed() || frame.code == 1010 {
                return Err(WsSendFailure::new(WsSendError::InvalidCloseCode, message));
            }
        }
        if exceeds_control_frame_limit(&message) || bytes > self.max_bytes {
            return Err(WsSendFailure::new(WsSendError::MessageTooLarge, message));
        }
        if state.messages.len() + state.in_flight_messages >= self.max_messages
            || bytes > self.max_bytes.saturating_sub(state.queued_bytes)
        {
            return Err(WsSendFailure::new(WsSendError::Backpressure, message));
        }
        state.queued_bytes += bytes;
        state.messages.push_back(message);
        drop(state);
        self.notify.notify_one();
        Ok(())
    }

    /// Closes the queue, rejecting future messages and discarding pending ones.
    ///
    /// Messages already removed by the writer remain charged until
    /// [`Self::finish_message`] completes. Repeated calls have no additional
    /// effect.
    pub(in crate::ws) fn close(&self) {
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        if state.closed {
            return;
        }
        state.closed = true;
        let queued_bytes = state.messages.iter().map(message_size).sum::<usize>();
        state.messages.clear();
        state.queued_bytes = state.queued_bytes.saturating_sub(queued_bytes);
    }

    /// Releases byte and item capacity after a writer finishes sending a
    /// message.
    ///
    /// # Parameters
    ///
    /// * `bytes` - Size previously charged when the message entered flight.
    pub(super) fn finish_message(&self, bytes: usize) {
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        state.in_flight_messages = state.in_flight_messages.saturating_sub(1);
        state.queued_bytes = state.queued_bytes.saturating_sub(bytes);
    }

    /// Moves the next queued message into the in-flight count for the writer.
    ///
    /// # Returns
    ///
    /// The oldest queued message, or `None` when the queue is empty.
    pub(super) fn take_next(&self) -> Option<Message> {
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        let message = state.messages.pop_front();
        if message.is_some() {
            state.in_flight_messages += 1;
        }
        message
    }
}

/// Returns whether a control frame exceeds the WebSocket protocol payload
/// limit.
///
/// Ping and Pong payloads are limited to 125 bytes. A Close reason shares the
/// control frame's 125-byte payload with its two-byte status code, leaving 123
/// UTF-8 bytes for the reason.
///
/// # Parameters
///
/// * `message` - Outbound frame to inspect.
/// # Returns
///
/// `true` when a Ping/Pong payload or Close reason exceeds its protocol limit.
#[must_use]
#[inline]
fn exceeds_control_frame_limit(message: &Message) -> bool {
    match message {
        Message::Ping(payload) | Message::Pong(payload) => payload.len() > 125,
        Message::Close(Some(frame)) => frame.reason.len() > 123,
        _ => false,
    }
}

/// Measures the payload bytes charged against the outbound queue budget.
///
/// # Parameters
///
/// * `message` - WebSocket frame whose queue cost is being calculated.
/// # Returns
///
/// Payload size in bytes, including the close code when present.
#[must_use]
#[inline]
pub(super) fn message_size(message: &Message) -> usize {
    match message {
        Message::Text(text) => text.as_str().len(),
        Message::Binary(bytes) | Message::Ping(bytes) | Message::Pong(bytes) => bytes.len(),
        Message::Close(Some(frame)) => frame.reason.len() + 2,
        Message::Close(None) => 0,
    }
}

#[cfg(test)]
mod tests {
    use axum::extract::ws::Message;

    use super::WsSendQueue;
    use super::message_size;
    use crate::ws::ws_send_error::WsSendError;

    #[test]
    fn test_oversized_message_is_not_temporary_backpressure() {
        let queue = WsSendQueue::new(4, 3);
        assert_eq!(queue.try_send(Message::text("four")), Err(WsSendError::MessageTooLarge));
        assert!(queue.is_empty());
        queue.try_send(Message::text("123")).unwrap();
        assert_eq!(queue.try_send(Message::text("x")), Err(WsSendError::Backpressure));
        let message = queue.take_next().unwrap();
        queue.finish_message(message_size(&message));
        queue.try_send(Message::text("123")).unwrap();
    }

    #[test]
    fn test_oversized_message_precedes_capacity_checks_without_changing_accounting() {
        let full = WsSendQueue::new(1, 3);
        full.try_send(Message::text("123")).unwrap();
        assert_eq!(full.try_send(Message::text("four")), Err(WsSendError::MessageTooLarge));
        assert_eq!(full.len(), 1);
        let in_flight = full.take_next().unwrap();
        full.finish_message(message_size(&in_flight));

        let closed = WsSendQueue::new(0, 3);
        closed.close();
        assert_eq!(closed.try_send(Message::text("four")), Err(WsSendError::Closed));

        let count_limited = WsSendQueue::new(0, 4);
        assert_eq!(
            count_limited.try_send(Message::text("1234")),
            Err(WsSendError::Backpressure)
        );
        assert_eq!(
            count_limited.try_send(Message::text("12345")),
            Err(WsSendError::MessageTooLarge)
        );
    }

    #[test]
    fn test_control_frame_protocol_boundaries_and_rejections() {
        use axum::extract::ws::CloseFrame;

        let queue = WsSendQueue::new(16, 4096);
        for (message, accepted) in [
            (Message::Ping(vec![0; 125].into()), true),
            (Message::Ping(vec![0; 126].into()), false),
            (Message::Pong(vec![0; 125].into()), true),
            (Message::Pong(vec![0; 126].into()), false),
            (
                Message::Close(Some(CloseFrame {
                    code: 1000,
                    reason: "a".repeat(123).into(),
                })),
                true,
            ),
            (
                Message::Close(Some(CloseFrame {
                    code: 1000,
                    reason: "a".repeat(124).into(),
                })),
                false,
            ),
            (
                Message::Close(Some(CloseFrame {
                    code: 1000,
                    reason: "你".repeat(41).into(),
                })),
                true,
            ),
            (
                Message::Close(Some(CloseFrame {
                    code: 1000,
                    reason: "你".repeat(42).into(),
                })),
                false,
            ),
            (Message::Close(None), true),
        ] {
            let before_len = queue.len();
            let before_bytes = queue.state.lock().unwrap().queued_bytes;
            let result = queue.try_send(message);
            if accepted {
                assert_eq!(result, Ok(()));
            } else {
                assert_eq!(result, Err(WsSendError::MessageTooLarge));
                assert_eq!(queue.len(), before_len);
                assert_eq!(queue.state.lock().unwrap().queued_bytes, before_bytes);
            }
        }
    }

    #[test]
    fn test_control_frame_protocol_error_precedence_and_rejection_has_no_side_effects() {
        use axum::extract::ws::CloseFrame;

        let full = WsSendQueue::new(1, 1024);
        full.try_send(Message::text("held")).unwrap();
        let before_bytes = full.state.lock().unwrap().queued_bytes;
        assert_eq!(
            full.try_send(Message::Ping(vec![0; 126].into())),
            Err(WsSendError::MessageTooLarge)
        );
        assert_eq!(full.len(), 1);
        assert_eq!(full.state.lock().unwrap().queued_bytes, before_bytes);

        let closed = WsSendQueue::new(1, 1024);
        closed.close();
        assert_eq!(
            closed.try_send(Message::Pong(vec![0; 126].into())),
            Err(WsSendError::Closed)
        );

        let queue = WsSendQueue::new(2, 1024);
        assert_eq!(
            queue.try_send(Message::Close(Some(CloseFrame {
                code: 1000,
                reason: "x".repeat(124).into()
            }))),
            Err(WsSendError::MessageTooLarge)
        );
        assert!(queue.is_empty());
        queue.try_send(Message::text("legal")).unwrap();
        assert_eq!(queue.len(), 1);

        let budget_limited = WsSendQueue::new(1, 124);
        assert_eq!(
            budget_limited.try_send(Message::Ping(vec![0; 125].into())),
            Err(WsSendError::MessageTooLarge)
        );

        let remaining_capacity = WsSendQueue::new(2, 125);
        remaining_capacity.try_send(Message::Ping(vec![0; 125].into())).unwrap();
        assert_eq!(
            remaining_capacity.try_send(Message::Ping(vec![0; 125].into())),
            Err(WsSendError::Backpressure)
        );
    }

    #[test]
    fn test_invalid_close_code_is_rejected_without_queue_side_effects() {
        use axum::extract::ws::CloseFrame;

        let queue = WsSendQueue::new(4, 1024);
        for code in [0, 999, 1004, 1005, 1006, 1010, 1015, 5000, 65535] {
            let result = queue.try_send(Message::Close(Some(CloseFrame {
                code,
                reason: "private-close-reason".into(),
            })));
            assert_eq!(result, Err(WsSendError::InvalidCloseCode), "code {code}");
            assert!(queue.is_empty(), "code {code}");
            assert_eq!(queue.state.lock().unwrap().queued_bytes, 0, "code {code}");
        }

        for code in [1000, 1001, 1002, 1011, 3000, 4000, 4999] {
            let legal = WsSendQueue::new(1, 1024);
            legal
                .try_send(Message::Close(Some(CloseFrame {
                    code,
                    reason: "legal".into(),
                })))
                .unwrap();
            assert_eq!(legal.len(), 1, "code {code}");
        }
        let no_code = WsSendQueue::new(1, 1024);
        no_code.try_send(Message::Close(None)).unwrap();
        assert_eq!(no_code.len(), 1);

        let full = WsSendQueue::new(1, 1024);
        full.try_send(Message::text("held")).unwrap();
        let before_bytes = full.state.lock().unwrap().queued_bytes;
        assert_eq!(
            full.try_send(Message::Close(Some(CloseFrame {
                code: 1005,
                reason: "x".repeat(124).into(),
            }))),
            Err(WsSendError::InvalidCloseCode)
        );
        assert_eq!(full.len(), 1);
        assert_eq!(full.state.lock().unwrap().queued_bytes, before_bytes);
        let closed = WsSendQueue::new(1, 1024);
        closed.close();
        assert_eq!(
            closed.try_send(Message::Close(Some(CloseFrame {
                code: 1005,
                reason: "short".into(),
            }))),
            Err(WsSendError::Closed)
        );

        let reusable = WsSendQueue::new(1, 1024);
        assert_eq!(
            reusable.try_send(Message::Close(Some(CloseFrame {
                code: 1005,
                reason: "short".into(),
            }))),
            Err(WsSendError::InvalidCloseCode)
        );
        reusable.try_send(Message::text("still-open")).unwrap();
        assert_eq!(reusable.len(), 1);
    }

    #[test]
    fn test_recoverable_send_returns_rejected_message_and_preserves_binary_storage() {
        use axum::body::Bytes;
        use axum::extract::ws::CloseFrame;

        let binary = Bytes::from_static(b"recover-this-binary");
        let binary_ptr = binary.as_ptr();
        let message = Message::Binary(binary);
        let failure = WsSendQueue::new(0, 1024).try_send_recoverable(message).unwrap_err();
        assert_eq!(failure.kind(), WsSendError::Backpressure);
        match failure.into_message() {
            Message::Binary(bytes) => assert_eq!(bytes.as_ptr(), binary_ptr),
            other => panic!("expected original binary message, got {other:?}"),
        }

        for (message, expected_variant) in [
            (Message::Ping(Bytes::from_static(b"ping")), "ping"),
            (Message::Pong(Bytes::from_static(b"pong")), "pong"),
            (
                Message::Close(Some(CloseFrame {
                    code: 1000,
                    reason: "close-reason".into(),
                })),
                "close-some",
            ),
            (Message::Close(None), "close-none"),
        ] {
            let failure = WsSendQueue::new(0, 1024).try_send_recoverable(message).unwrap_err();
            assert_eq!(failure.kind(), WsSendError::Backpressure);
            match (expected_variant, failure.into_message()) {
                ("ping", Message::Ping(bytes)) => assert_eq!(&bytes[..], b"ping"),
                ("pong", Message::Pong(bytes)) => assert_eq!(&bytes[..], b"pong"),
                ("close-some", Message::Close(Some(frame))) => {
                    assert_eq!(frame.code, 1000);
                    assert_eq!(frame.reason.as_str(), "close-reason");
                }
                ("close-none", Message::Close(None)) => {}
                (expected, message) => panic!("expected {expected}, got {message:?}"),
            }
        }

        let cases = [
            (
                WsSendQueue::new(1, 0),
                Message::text("too-large"),
                WsSendError::MessageTooLarge,
            ),
            (
                WsSendQueue::new(1, 1024),
                Message::Close(Some(CloseFrame {
                    code: 1005,
                    reason: "invalid-code".into(),
                })),
                WsSendError::InvalidCloseCode,
            ),
        ];
        for (queue, message, expected) in cases {
            let failure = queue.try_send_recoverable(message).unwrap_err();
            let (kind, message) = failure.into_parts();
            assert_eq!(kind, expected);
            match (expected, message) {
                (WsSendError::MessageTooLarge, Message::Text(text)) => {
                    assert_eq!(text.as_str(), "too-large");
                }
                (WsSendError::InvalidCloseCode, Message::Close(Some(frame))) => {
                    assert_eq!(frame.code, 1005);
                    assert_eq!(frame.reason.as_str(), "invalid-code");
                }
                (kind, message) => panic!("unexpected recovered pair {kind:?}, {message:?}"),
            }
        }

        let closed = WsSendQueue::new(1, 1024);
        closed.close();
        let message = Message::text("closed-original");
        let failure = closed.try_send_recoverable(message).unwrap_err();
        assert_eq!(failure.kind(), WsSendError::Closed);
        assert!(matches!(failure.message(), Message::Text(text) if text.as_str() == "closed-original"));
    }

    #[test]
    fn test_legacy_and_recoverable_send_report_the_same_error_kinds() {
        use axum::extract::ws::CloseFrame;

        let make_cases = || {
            [
                (WsSendQueue::new(0, 1024), Message::text("backpressure")),
                (WsSendQueue::new(1, 0), Message::text("too-large")),
                (
                    WsSendQueue::new(1, 1024),
                    Message::Close(Some(CloseFrame {
                        code: 1005,
                        reason: "invalid".into(),
                    })),
                ),
                (
                    {
                        let queue = WsSendQueue::new(1, 1024);
                        queue.close();
                        queue
                    },
                    Message::text("closed"),
                ),
            ]
        };
        for (queue, message) in make_cases() {
            let expected = queue.try_send(message.clone()).unwrap_err();
            assert_eq!(queue.try_send_recoverable(message).unwrap_err().kind(), expected);
        }
    }

    #[test]
    fn test_message_size_boundaries_use_utf8_payload_bytes_and_support_zero_budget() {
        let five_bytes = WsSendQueue::new(1, 5);
        assert_eq!(
            five_bytes.try_send(Message::text("你好")),
            Err(WsSendError::MessageTooLarge)
        );
        assert!(five_bytes.is_empty());

        let six_bytes = WsSendQueue::new(1, 6);
        six_bytes.try_send(Message::text("你好")).unwrap();
        assert_eq!(six_bytes.len(), 1);

        let zero_bytes = WsSendQueue::new(1, 0);
        zero_bytes.try_send(Message::text("")).unwrap();
        assert_eq!(
            zero_bytes.try_send(Message::text("x")),
            Err(WsSendError::MessageTooLarge)
        );

        let zero_messages = WsSendQueue::new(0, 1);
        assert_eq!(
            zero_messages.try_send(Message::text("x")),
            Err(WsSendError::Backpressure)
        );
    }

    #[test]
    fn test_in_flight_messages_keep_queue_capacity_until_send_finishes() {
        let queue = WsSendQueue::new(1, 3);
        queue.try_send(Message::text("one")).unwrap();
        let in_flight = queue.take_next().unwrap();

        assert_eq!(queue.len(), 1);
        assert_eq!(queue.try_send(Message::text("two")), Err(WsSendError::Backpressure));

        queue.finish_message(message_size(&in_flight));
        assert!(queue.is_empty());
        queue.try_send(Message::text("two")).unwrap();
    }

    #[test]
    fn test_close_rejects_new_messages_and_clears_only_pending() {
        let queue = WsSendQueue::new(2, 6);
        queue.try_send(Message::text("one")).unwrap();
        queue.try_send(Message::text("two")).unwrap();
        let in_flight = queue.take_next().unwrap();

        queue.close();
        queue.close();

        assert_eq!(queue.try_send(Message::text("x")), Err(WsSendError::Closed));
        assert_eq!(queue.len(), 1);

        queue.finish_message(message_size(&in_flight));
        assert!(queue.is_empty());
    }

    #[test]
    fn test_closing_a_queue_closes_its_clones() {
        let queue = WsSendQueue::new(2, 6);
        let clone = queue.clone();

        queue.close();

        assert_eq!(clone.try_send(Message::text("x")), Err(WsSendError::Closed));
    }

    #[test]
    fn test_debug_redacts_all_message_payloads() {
        use axum::extract::ws::CloseFrame;

        const SECRET: &str = "queue-debug-secret-sentinel";
        let messages = [
            Message::text(SECRET),
            Message::Binary(SECRET.as_bytes().to_vec().into()),
            Message::Ping(SECRET.as_bytes().to_vec().into()),
            Message::Pong(SECRET.as_bytes().to_vec().into()),
            Message::Close(Some(CloseFrame {
                code: 1000,
                reason: SECRET.into(),
            })),
        ];
        let numeric = SECRET
            .as_bytes()
            .iter()
            .map(u8::to_string)
            .collect::<Vec<_>>()
            .join(", ");
        for message in messages {
            let queue = WsSendQueue::new(4, 1024);
            queue.try_send(message).unwrap();
            for output in [format!("{queue:?}"), format!("{queue:#?}")] {
                assert!(!output.contains(SECRET));
                assert!(!output.contains(&numeric));
                for forbidden in ["messages: [", "state:", "notify:"] {
                    assert!(!output.contains(forbidden));
                }
                for field in ["len:", "queued_bytes:", "max_messages:", "max_bytes:", "closed:"] {
                    assert!(output.contains(field));
                }
            }
        }
    }

    #[test]
    fn test_debug_reports_shared_accounting_without_messages() {
        let queue = WsSendQueue::new(2, 6);
        let snapshot = |len: usize, bytes: usize, closed: bool| {
            for text in [format!("{queue:?}"), format!("{queue:#?}")] {
                assert!(text.contains(&format!("len: {len}")));
                assert!(text.contains(&format!("queued_bytes: {bytes}")));
                assert!(text.contains("max_messages: 2"));
                assert!(text.contains("max_bytes: 6"));
                assert!(text.contains(&format!("closed: {closed}")));
            }
        };
        snapshot(0, 0, false);
        queue.try_send(Message::text("one")).unwrap();
        queue.clone().try_send(Message::text("two")).unwrap();
        snapshot(2, 6, false);
        let in_flight = queue.take_next().unwrap();
        snapshot(2, 6, false);
        queue.clone().close();
        snapshot(1, 3, true);
        queue.finish_message(message_size(&in_flight));
        snapshot(0, 0, true);
    }

    #[test]
    fn test_debug_recovers_poisoned_state_without_payloads() {
        let queue = WsSendQueue::new(1, 1024);
        queue.try_send(Message::text("poison-secret-sentinel")).unwrap();
        let state = queue.state.clone();
        let result = std::thread::spawn(move || {
            let _guard = state.lock().unwrap();
            panic!("intentional test poison");
        })
        .join();
        assert!(result.is_err());
        for output in [format!("{queue:?}"), format!("{queue:#?}")] {
            assert!(!output.contains("poison-secret-sentinel"));
            assert!(output.contains("len: 1"));
        }
    }
}
