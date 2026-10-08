// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
#[test]
fn invalid_controller_declarations_fail_at_compile_time() {
    let cases = trybuild::TestCases::new();
    cases.compile_fail("tests/ui/controller_*.rs");
}
