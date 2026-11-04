// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use std::collections::HashMap;
use std::collections::HashSet;
use std::collections::VecDeque;
use std::fs;
use std::path::Path;
use std::path::PathBuf;

#[test]
fn heading_anchors_match_markdown_heading_rules() {
    let source = "# Transport budgets\n\n# 配置与启动\n\n## Retry\n\n## Retry\n\n## Retry-1\n\n# Use `WsSession`\n\n```md\n# Fake\n```\n";
    let anchors = heading_anchors(source);

    for expected in [
        "transport-budgets",
        "配置与启动",
        "retry",
        "retry-1",
        "retry-1-1",
        "use-wssession",
    ] {
        assert!(anchors.contains(expected), "missing anchor {expected:?}");
    }
    assert!(!anchors.contains("fake"));
    assert!(!heading_anchors("    # Indented code heading\n").contains("indented-code-heading"));
}

#[test]
fn validates_fragments_after_percent_decoding() {
    let source = "# 配置与启动\n";
    assert!(validate_fragment(source, "配置与启动").is_ok());
    assert!(validate_fragment(source, "%E9%85%8D%E7%BD%AE%E4%B8%8E%E5%90%AF%E5%8A%A8").is_ok());
    assert!(validate_fragment(source, "%GG").is_err());
    assert!(validate_fragment(source, "missing").is_err());
}

#[test]
fn validates_local_document_links_and_reports_source_line() {
    let fixture = LinkFixture::new();
    fixture.write("index.md", "first\n[bad](target.md#missing)\n");
    fixture.write("target.md", "# Present\n");

    let failures = check_markdown_links(&fixture.root, &[fixture.root.join("index.md")]);
    assert_eq!(failures.len(), 1, "{failures:?}");
    assert!(failures[0].contains("index.md:2:"), "{}", failures[0]);
    assert!(failures[0].contains("#missing"), "{}", failures[0]);
}

#[test]
fn empty_or_absent_fragments_and_external_urls_are_distinguished() {
    let fixture = LinkFixture::new();
    fixture.write(
        "index.md",
        "[empty](target.md#)\n[plain](target.md)\n[external](https://example.com/#ignored)\n",
    );
    fixture.write("target.md", "# Present\n");

    let failures = check_markdown_links(&fixture.root, &[fixture.root.join("index.md")]);
    assert!(failures.is_empty(), "{failures:?}");
}

#[test]
fn link_to_heading_inside_fenced_or_indented_code_is_rejected() {
    let fixture = LinkFixture::new();
    fixture.write(
        "index.md",
        "[fenced](target.md#fake)\n[still fenced](target.md#still-fake)\n[indented](target.md#indented-code)\n",
    );
    fixture.write(
        "target.md",
        "```md\n# Fake\n    ```\n# Still fake\n```\n    # Indented code\n",
    );

    let failures = check_markdown_links(&fixture.root, &[fixture.root.join("index.md")]);
    assert_eq!(failures.len(), 3, "{failures:?}");
    assert!(failures[0].contains("#fake"), "{}", failures[0]);
    assert!(failures[1].contains("#still-fake"), "{}", failures[1]);
    assert!(failures[2].contains("#indented-code"), "{}", failures[2]);
}

#[test]
fn duplicate_heading_ids_skip_ids_already_claimed_by_later_headings() {
    let anchors = heading_anchors("## Retry\n## Retry\n## Retry-1\n");
    assert!(anchors.contains("retry"));
    assert!(anchors.contains("retry-1"));
    assert!(anchors.contains("retry-1-1"));
}

#[test]
fn heading_slug_uses_visible_text_from_inline_links_and_setext_headings() {
    let anchors = heading_anchors("[Transport](guide.md) `WsSession`\n==============================\n");
    assert!(anchors.contains("transport-wssession"));
}

fn markdown_links(source: &str) -> Vec<(usize, String)> {
    let mut links = Vec::new();
    let mut in_fence = false;

    for (line_index, line) in source.lines().enumerate() {
        let Some(trimmed) = markdown_line_content(line) else {
            continue;
        };
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

fn heading_anchors(source: &str) -> HashSet<String> {
    let mut anchors = HashSet::new();
    let mut in_fence = false;
    let mut fence_char = '\0';
    let mut fence_len = 0;
    let lines = source.lines().collect::<Vec<_>>();
    let mut index = 0;

    while index < lines.len() {
        let line = lines[index];
        let Some(trimmed) = markdown_line_content(line) else {
            index += 1;
            continue;
        };
        let fence = trimmed
            .chars()
            .take_while(|ch| *ch == '`' || *ch == '~')
            .collect::<String>();
        if fence.len() >= 3 {
            let ch = fence.chars().next().unwrap();
            if !in_fence {
                in_fence = true;
                fence_char = ch;
                fence_len = fence.len();
            } else if ch == fence_char && fence.len() >= fence_len {
                in_fence = false;
            }
            index += 1;
            continue;
        }
        if in_fence {
            index += 1;
            continue;
        }
        let hashes = trimmed.chars().take_while(|ch| *ch == '#').count();
        let title = if hashes > 0 && hashes <= 6 && trimmed[hashes..].chars().next().is_some_and(char::is_whitespace) {
            trimmed[hashes..].trim().trim_end_matches('#').trim().to_owned()
        } else if index + 1 < lines.len()
            && (lines[index + 1].trim().chars().all(|ch| ch == '=')
                || lines[index + 1].trim().chars().all(|ch| ch == '-'))
            && !lines[index + 1].trim().is_empty()
        {
            index += 1;
            line.trim().to_owned()
        } else {
            index += 1;
            continue;
        };
        let title = visible_heading_text(&title);
        let slug = title
            .chars()
            .flat_map(char::to_lowercase)
            .filter(|ch| ch.is_alphanumeric() || *ch == '-' || *ch == '_' || ch.is_whitespace())
            .collect::<String>()
            .split_whitespace()
            .collect::<Vec<_>>()
            .join("-");
        if slug.is_empty() {
            index += 1;
            continue;
        }
        let mut candidate = slug.clone();
        let mut suffix = 1;
        while anchors.contains(&candidate) {
            candidate = format!("{slug}-{suffix}");
            suffix += 1;
        }
        anchors.insert(candidate);
        index += 1;
    }
    anchors
}

fn markdown_line_content(line: &str) -> Option<&str> {
    let spaces = line.bytes().take_while(|byte| *byte == b' ').count();
    if spaces >= 4 || line.starts_with('\t') {
        return None;
    }
    Some(&line[spaces..])
}

fn visible_heading_text(title: &str) -> String {
    let mut visible = String::new();
    let mut chars = title.chars().peekable();
    while let Some(ch) = chars.next() {
        match ch {
            '`' | '*' | '_' | '~' => {}
            '[' => {
                let mut label = String::new();
                while let Some(next) = chars.next() {
                    if next == ']' {
                        if chars.peek() == Some(&'(') {
                            for end in chars.by_ref() {
                                if end == ')' {
                                    break;
                                }
                            }
                        }
                        visible.push_str(&label);
                        break;
                    }
                    label.push(next);
                }
            }
            '<' => {
                for end in chars.by_ref() {
                    if end == '>' {
                        break;
                    }
                }
            }
            _ => visible.push(ch),
        }
    }
    visible
}

fn validate_fragment(source: &str, fragment: &str) -> Result<(), String> {
    let decoded = percent_decode(fragment)?;
    if heading_anchors(source).contains(&decoded) {
        Ok(())
    } else {
        Err(format!("missing heading anchor: {decoded}"))
    }
}

fn percent_decode(fragment: &str) -> Result<String, String> {
    let bytes = fragment.as_bytes();
    let mut decoded = Vec::with_capacity(bytes.len());
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index] == b'%' {
            let pair = bytes
                .get(index + 1..index + 3)
                .ok_or_else(|| "incomplete percent escape".to_owned())?;
            let hex = std::str::from_utf8(pair).map_err(|_| "invalid percent escape".to_owned())?;
            decoded.push(u8::from_str_radix(hex, 16).map_err(|_| "invalid percent escape".to_owned())?);
            index += 3;
        } else {
            decoded.push(bytes[index]);
            index += 1;
        }
    }
    String::from_utf8(decoded).map_err(|_| "fragment is not valid UTF-8".to_owned())
}

#[test]
fn test_documentation_local_markdown_links_exist() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let entries = [
        root.join("README.md"),
        root.join("README.zh_CN.md"),
        root.join("doc/README.md"),
    ];
    let failures = check_markdown_links(&root, &entries);

    assert!(
        failures.is_empty(),
        "broken documentation links:\n{}",
        failures.join("\n")
    );
}

fn check_markdown_links(root: &Path, entries: &[PathBuf]) -> Vec<String> {
    let Ok(root) = root.canonicalize() else {
        return vec![format!("{}: root directory does not exist", root.display())];
    };
    let mut pending = VecDeque::from(entries.to_vec());
    let mut visited = HashSet::new();
    let mut anchor_cache = HashMap::new();
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
            if target.contains("://") || target.starts_with("mailto:") {
                continue;
            }
            let (target_path, fragment) = target.split_once('#').unwrap_or((target.as_str(), ""));
            let resolved = if target_path.is_empty() {
                canonical_source.clone()
            } else {
                canonical_source
                    .parent()
                    .expect("Markdown source has a parent")
                    .join(target_path)
            };
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
                if !fragment.is_empty() {
                    let anchors = anchor_cache.entry(canonical_target.clone()).or_insert_with(|| {
                        fs::read_to_string(&canonical_target)
                            .map(|source| heading_anchors(&source))
                            .unwrap_or_default()
                    });
                    match percent_decode(fragment) {
                        Ok(decoded) if anchors.contains(&decoded) => {}
                        Ok(decoded) => failures.push(format!(
                            "{}:{line}: Markdown link target has no heading anchor #{decoded}: {target}",
                            canonical_source
                                .strip_prefix(&root)
                                .unwrap_or(&canonical_source)
                                .display()
                        )),
                        Err(reason) => failures.push(format!(
                            "{}:{line}: invalid Markdown link fragment ({reason}): {target}",
                            canonical_source
                                .strip_prefix(&root)
                                .unwrap_or(&canonical_source)
                                .display()
                        )),
                    }
                }
                pending.push_back(canonical_target);
            }
        }
    }

    failures
}

struct LinkFixture {
    root: PathBuf,
}

impl LinkFixture {
    fn new() -> Self {
        static NEXT_ID: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
        let id = NEXT_ID.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let root = std::env::temp_dir().join(format!("qubit-web-link-test-{}-{id}", std::process::id()));
        fs::create_dir_all(&root).expect("create Markdown link fixture directory");
        Self { root }
    }

    fn write(&self, name: &str, contents: &str) {
        fs::write(self.root.join(name), contents).expect("write Markdown link fixture");
    }
}

impl Drop for LinkFixture {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.root);
    }
}
