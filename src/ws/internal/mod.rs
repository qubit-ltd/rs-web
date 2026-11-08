// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Private WebSocket reader, writer, and close helpers.

mod close_queue_on_drop;
mod control_flush_outcome;
mod delivery_result;
mod flush_outcome;
mod inbound_message;
mod inbound_state;
mod observe_io_task;
mod policy_inner;
mod queue_state;
mod send_outcome;
mod session_resources;
mod termination_data;
mod termination_state;
mod upgrade_lifecycle;

pub(in crate::ws) use close_queue_on_drop::CloseQueueOnDrop;
pub(in crate::ws) use delivery_result::DeliveryResult;
pub(in crate::ws) use inbound_message::InboundMessage;
pub(in crate::ws) use inbound_state::InboundState;
pub(in crate::ws) use observe_io_task::observe_io_task;
pub(in crate::ws) use policy_inner::PolicyInner;
pub(in crate::ws) use queue_state::QueueState;
pub(in crate::ws) use session_resources::SessionResources;
pub(in crate::ws) use termination_state::TerminationState;
pub(in crate::ws) use upgrade_lifecycle::UpgradeLifecycle;

mod close;
mod reader;
mod reader_context;
mod reader_wait_result;
mod session_lifecycle;
mod writer;

pub(in crate::ws) use close::server_shutting_down_response;
pub(in crate::ws) use reader::reader_loop;
pub(in crate::ws) use reader_context::ReaderContext;
pub(in crate::ws) use session_lifecycle::SessionLifecycle;
pub(in crate::ws) use writer::writer_loop;
