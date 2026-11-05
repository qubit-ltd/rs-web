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
    /// Creates a queue whose item and byte limits must both be satisfied.
    ///
    /// # Parameters
    ///
    /// * `max_messages` - Maximum message capacity, including in-flight sends;
    /// * `max_bytes` - Maximum total payload byte capacity. Zero permits
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
    /// `Ok(())` when accepted into the queue.
    ///
    /// # Errors
    ///
    /// Returns [`WsSendError::Closed`] if the queue is closed,
    /// [`WsSendError::MessageTooLarge`] if the message exceeds the total byte
    /// budget, or [`WsSendError::Backpressure`] if the queue lacks message
    /// count or remaining byte capacity. These checks occur in that order.
    /// Success means only that the message entered the queue, not that it was
    /// delivered; a message still pending at closure may be discarded.
    pub fn try_send(&self, message: Message) -> Result<(), WsSendError> {
        let bytes = message_size(&message);
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        if state.closed {
            return Err(WsSendError::Closed);
        }
        if bytes > self.max_bytes {
            return Err(WsSendError::MessageTooLarge);
        }
        if state.messages.len() + state.in_flight_messages >= self.max_messages
            || bytes > self.max_bytes.saturating_sub(state.queued_bytes)
        {
            return Err(WsSendError::Backpressure);
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
