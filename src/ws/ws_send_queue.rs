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

use super::ws_send_error::WsSendError;

/// Tracks queued and in-flight message counts and byte charges under one lock.
#[derive(Debug)]
pub(super) struct QueueState {
    /// Messages accepted but not yet removed by the writer task.
    pub(super) messages: VecDeque<Message>,
    /// Bytes retained in queued messages and current in-flight sends.
    pub(super) queued_bytes: usize,
    /// Messages removed from the queue but not yet completed by the sink.
    in_flight_messages: usize,
}

/// A nonblocking bounded queue for outbound WebSocket messages.
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
#[derive(Clone, Debug)]
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

impl WsSendQueue {
    /// Creates a queue whose item and byte limits must both be satisfied.
    ///
    /// # Parameters
    ///
    /// * `max_messages` - Maximum message capacity, including in-flight sends;
    ///   zero prevents every send.
    /// * `max_bytes` - Maximum total byte capacity; zero prevents every send.
    ///
    /// # Returns
    ///
    /// An empty queue with the supplied capacity bounds.
    pub fn new(max_messages: usize, max_bytes: usize) -> Self {
        Self {
            state: Arc::new(Mutex::new(QueueState {
                messages: VecDeque::new(),
                queued_bytes: 0,
                in_flight_messages: 0,
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

    /// Enqueues a message without waiting; full queues return backpressure.
    ///
    /// # Parameters
    ///
    /// * `message` - Text, binary, ping, pong, or close message to enqueue.
    ///
    /// # Returns
    ///
    /// `Ok(())` when accepted into the queue.
    ///
    /// # Errors
    ///
    /// Returns [`WsSendError::Backpressure`] when either capacity bound would
    /// be exceeded.
    pub fn try_send(&self, message: Message) -> Result<(), WsSendError> {
        let bytes = message_size(&message);
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
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
///
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
}
