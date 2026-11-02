// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use std::collections::HashSet;
use std::collections::VecDeque;
use std::fs;
use std::path::PathBuf;

fn markdown_links(source: &str) -> Vec<(usize, String)> {
    let mut links = Vec::new();
    let mut in_fence = false;

    for (line_index, line) in source.lines().enumerate() {
        let trimmed = line.trim_start();
        if trimmed.starts_with("```") || trimmed.starts_with("~~~") {
            in_fence = !in_fence;
            continue;
        }
        if in_fence {
            continue;
        }

        let mut rest = line;
        let mut offset = 0;
        while let Some(close) = rest.find("](") {
            let destination_start = close + 2;
            let destination = &rest[destination_start..];
            let Some(end) = destination.find(')') else {
                break;
            };
            let target = destination[..end]
                .split_whitespace()
                .next()
                .unwrap_or("")
                .trim_matches(['<', '>']);
            if !target.is_empty() {
                links.push((line_index + 1, target.to_owned()));
            }
            let consumed = destination_start + end + 1;
            offset += consumed;
            rest = &line[offset..];
        }
    }
    links
}

#[test]
fn test_documentation_local_markdown_links_exist() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let mut pending = VecDeque::from([
        root.join("README.md"),
        root.join("README.zh_CN.md"),
        root.join("doc/README.md"),
    ]);
    let mut visited = HashSet::new();
    let mut failures = Vec::new();

    while let Some(source_path) = pending.pop_front() {
        let Ok(canonical_source) = source_path.canonicalize() else {
            failures.push(format!(
                "{}: source Markdown file does not exist",
                source_path.display()
            ));
            continue;
        };
        if !canonical_source.starts_with(&root) || !visited.insert(canonical_source.clone()) {
            continue;
        }
        let source = fs::read_to_string(&canonical_source)
            .unwrap_or_else(|error| panic!("cannot read {}: {error}", canonical_source.display()));

        for (line, target) in markdown_links(&source) {
            if target.starts_with('#') || target.contains("://") || target.starts_with("mailto:") {
                continue;
            }
            let target_path = target.split('#').next().unwrap_or("");
            if target_path.is_empty() {
                continue;
            }
            let resolved = canonical_source
                .parent()
                .expect("Markdown source has a parent")
                .join(target_path);
            let Ok(canonical_target) = resolved.canonicalize() else {
                failures.push(format!(
                    "{}:{line}: local link target does not exist: {target}",
                    canonical_source
                        .strip_prefix(&root)
                        .unwrap_or(&canonical_source)
                        .display()
                ));
                continue;
            };
            if !canonical_target.starts_with(&root) {
                continue;
            }
            if canonical_target.extension().is_some_and(|extension| extension == "md") {
                pending.push_back(canonical_target);
            }
        }
    }

    assert!(
        failures.is_empty(),
        "broken documentation links:\n{}",
        failures.join("\n")
    );
}
