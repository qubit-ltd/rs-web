// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Private WebSocket reader, writer, and close helpers.

mod close;
mod reader;
mod session_lifecycle;
mod writer;

pub(in crate::ws) use close::server_shutting_down_response;
pub(in crate::ws) use reader::reader_loop;
pub(in crate::ws) use session_lifecycle::SessionLifecycle;
pub(in crate::ws) use writer::writer_loop;
