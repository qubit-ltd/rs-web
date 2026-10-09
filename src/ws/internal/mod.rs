// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Private WebSocket reader, writer, and close helpers.

mod close_queue_on_drop;
mod delivery_result;
mod policy_inner;
mod queue_state;
mod upgrade_lifecycle;

pub(in crate::ws) use close_queue_on_drop::CloseQueueOnDrop;
pub(in crate::ws) use delivery_result::DeliveryResult;
pub(in crate::ws) use policy_inner::PolicyInner;
pub(in crate::ws) use queue_state::QueueState;
pub(in crate::ws) use upgrade_lifecycle::UpgradeLifecycle;

mod close;
mod reader;
mod session_lifecycle;
mod writer;

pub(in crate::ws) use close::server_shutting_down_response;
pub(in crate::ws) use reader::reader_loop;
pub(in crate::ws) use session_lifecycle::SessionLifecycle;
pub(in crate::ws) use writer::writer_loop;
