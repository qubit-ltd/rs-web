// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Bounded WebSocket upgrades and per-session outbound backpressure.
//!
//! Applications retain responsibility for authentication and their message
//! protocol. Use [`WsUpgradePolicy::on_upgrade`] after authentication to apply
//! origin, connection, frame, and message limits. Selecting Axum's native
//! `WebSocketUpgrade::on_upgrade` directly bypasses this module's queue and
//! lifecycle policy.
mod internal;
mod ws_policy_error;
mod ws_send_error;
mod ws_send_queue;
mod ws_session;
mod ws_upgrade_policy;

pub use ws_policy_error::WsPolicyError;
pub use ws_send_error::WsSendError;
pub use ws_send_queue::WsSendQueue;
pub use ws_session::WsSession;
pub use ws_upgrade_policy::WsUpgradePolicy;
