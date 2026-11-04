// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use std::path::Path;
use std::process::Command;

const FIXTURES: &[&str] = &[
    "canonical",
    "renamed_runtime",
    "renamed_axum",
    "renamed_both",
    "keyword_runtime",
];

#[test]
fn macro_consumers_compile_with_dependency_names() {
    let repository = Path::new(env!("CARGO_MANIFEST_DIR"));
    // Keep nested Cargo builds under the parent test's isolated target directory.
    let target_dir = Path::new(env!("CARGO_TARGET_TMPDIR")).join("macro-consumers");
    let cargo = std::env::var_os("CARGO").unwrap_or_else(|| "cargo".into());

    for fixture in FIXTURES {
        let manifest = repository
            .join("tests/fixtures/macro_consumers")
            .join(fixture)
            .join("Cargo.toml");
        let output = Command::new(&cargo)
            .arg("check")
            .arg("--locked")
            .arg("--manifest-path")
            .arg(&manifest)
            .arg("--target-dir")
            .arg(&target_dir)
            .env("CARGO_TARGET_DIR", &target_dir)
            .output()
            .unwrap_or_else(|error| panic!("could not check fixture {fixture}: {error}"));

        assert!(
            output.status.success(),
            "fixture {fixture} failed to compile:\n{}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
}
