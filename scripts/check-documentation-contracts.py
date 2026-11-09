#!/usr/bin/env python3
"""Compile selected source snippets from the public Markdown documentation."""
import argparse
import json
import re
import shutil
import subprocess
import sys
import tempfile
import tomllib
from pathlib import Path

CONTRACTS = Path(__file__).with_name("documentation-contracts.json")
EXPECTED_IDS = {"readme-en-quick-start", "readme-zh-quick-start", "guide-en-prompt-stream", "guide-zh-prompt-stream", "guide-en-extractor-budgets", "guide-zh-extractor-budgets"}

class ContractError(Exception):
    pass

def heading_blocks(text):
    lines = text.splitlines()
    headings = []
    for index, line in enumerate(lines):
        match = re.match(r"^(#{1,6})\s+(.+?)\s*#*\s*$", line)
        if match:
            headings.append((index, len(match.group(1)), match.group(2)))
    return lines, headings

def extract_fence(root, spec):
    path = root / spec["source"]
    if spec.get("whole_file"):
        if not path.is_file():
            raise ContractError(f"missing source file {path}")
        return path.read_text(encoding="utf-8"), 1
    if not path.is_file():
        raise ContractError(f"missing source file {path}")
    lines, headings = heading_blocks(path.read_text(encoding="utf-8"))
    matches = [(i, level, title) for i, level, title in headings if title == spec["heading"]]
    if len(matches) != 1:
        raise ContractError(f"{path}: expected one heading {spec['heading']!r}, found {len(matches)}")
    start, level, _ = matches[0]
    end = next((i for i, lv, _ in headings if i > start and lv <= level), len(lines))
    selected = []
    i = start + 1
    while i < end:
        opening = re.match(r"^\s*(`{3,}|~{3,})(.*)$", lines[i])
        if not opening:
            i += 1
            continue
        marker, info = opening.groups()
        fence_char = marker[0]
        close = i + 1
        while close < end and not re.match(r"^\s*" + re.escape(fence_char) + r"{" + str(len(marker)) + r",}\s*$", lines[close]):
            close += 1
        if close == end:
            raise ContractError(f"{path}:{i+1}: unclosed fenced block")
        tag = info.strip().split(",", 1)[0].strip()
        if tag == spec["language"]:
            ignored = info.strip().endswith(",ignore")
            if ignored and not spec.get("allow_ignore", False):
                pass
            else:
                selected.append(("\n".join(lines[i+1:close]) + "\n", i+2))
        i = close + 1
    ordinal = spec.get("ordinal", 1)
    if len(selected) < ordinal:
        raise ContractError(f"{path}: missing {spec['language']} fence #{ordinal} under {spec['heading']!r}")
    return selected[ordinal-1]

def patch_qubit_web(manifest_text, parsed, root):
    deps = parsed.get("dependencies")
    if not isinstance(deps, dict) or "qubit-web" not in deps:
        raise ContractError("TOML must declare [dependencies].qubit-web")
    matches = list(re.finditer(r'(?m)^(\s*qubit-web\s*=\s*)([^\n]+)$', manifest_text))
    if len(matches) != 1:
        raise ContractError(f"expected one simple qubit-web dependency declaration, found {len(matches)}")
    original = deps["qubit-web"]
    if isinstance(original, str):
        replacement = '{ version = ' + json.dumps(original) + ', path = ' + json.dumps(str(root.resolve())) + ' }'
    elif isinstance(original, dict):
        if any(key in original for key in ("git", "branch", "tag", "rev")):
            raise ContractError("qubit-web dependency must not use a git source")
        if "path" in original:
            raise ContractError("source TOML must not already set qubit-web path")
        # Preserve the source declaration's fields and ordering while adding only path.
        body = matches[0].group(2).strip()
        if not (body.startswith("{") and body.endswith("}")):
            raise ContractError("qubit-web table dependency must use an inline table")
        replacement = body[:-1].rstrip() + (", " if body[1:-1].strip() else "") + "path = " + json.dumps(str(root.resolve())) + " }"
    else:
        raise ContractError("unsupported qubit-web dependency form")
    return manifest_text[:matches[0].start(2)] + replacement + manifest_text[matches[0].end(2):]

def source_file(code, wrapper):
    if wrapper == "binary":
        return code
    if wrapper == "budget-router":
        return "fn main() -> Result<(), Box<dyn std::error::Error>> {\n" + code + "\nlet _: axum::Router<()> = app;\nOk(())\n}\n"
    raise ContractError(f"unsupported wrapper {wrapper!r}")

def validate_scenarios(data):
    scenarios = data.get("scenarios")
    if not isinstance(scenarios, list) or {s.get("id") for s in scenarios} != EXPECTED_IDS or len(scenarios) != 6:
        raise ContractError("contract manifest must contain exactly the six required scenario IDs")
    for scenario in scenarios:
        if scenario.get("wrapper") not in {"binary", "budget-router"}:
            raise ContractError(f"{scenario.get('id')}: unsupported wrapper {scenario.get('wrapper')!r}")

def run(root, artifacts_dir=None, target_dir=None):
    data = json.loads(CONTRACTS.read_text(encoding="utf-8"))
    validate_scenarios(data)
    root = root.resolve()
    artifacts = artifacts_dir.resolve() if artifacts_dir else None
    if artifacts:
        artifacts.mkdir(parents=True, exist_ok=True)
    for scenario in data["scenarios"]:
        sid = scenario["id"]
        try:
            manifest_text, manifest_line = extract_fence(root, scenario["manifest"])
            parsed = tomllib.loads(manifest_text)
            manifest_text = patch_qubit_web(manifest_text, parsed, root)
            code, code_line = extract_fence(root, scenario["code"])
            code = source_file(code, scenario["wrapper"])
            with tempfile.TemporaryDirectory(prefix=f"docs-{sid}-") as temp:
                project = Path(temp)
                (project / "src").mkdir()
                generated_manifest = '[package]\nname = "docs-contract-' + sid.replace("-", "_") + '"\nversion = "0.0.0"\nedition = "2024"\n\n[workspace]\n\n' + manifest_text
                (project / "Cargo.toml").write_text(generated_manifest, encoding="utf-8")
                generated_source = f"// Source: {scenario['code']['source']}:{code_line}\n" + code
                (project / "src/main.rs").write_text(generated_source, encoding="utf-8")
                if artifacts:
                    saved = artifacts / sid
                    saved.mkdir(exist_ok=True)
                    (saved / "Cargo.toml").write_text(generated_manifest, encoding="utf-8")
                    (saved / "main.rs").write_text(generated_source, encoding="utf-8")
                cmd = ["cargo", "check", "--manifest-path", str(project / "Cargo.toml")]
                if target_dir:
                    cmd.extend(["--target-dir", str(target_dir)])
                result = subprocess.run(cmd, cwd=root, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=False)
                if result.returncode:
                    if artifacts:
                        (artifacts / sid / "stderr.txt").write_text(result.stderr, encoding="utf-8")
                    raise ContractError(f"scenario {sid} failed at {scenario['code']['source']}:{code_line}\ncommand: {' '.join(cmd)}\n{result.stderr}")
                metadata = subprocess.run(["cargo", "metadata", "--format-version", "1", "--manifest-path", str(project / "Cargo.toml")], cwd=root, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=False)
                if metadata.returncode:
                    raise ContractError(f"scenario {sid}: cargo metadata failed: {metadata.stderr}")
                packages = json.loads(metadata.stdout).get("packages", [])
                qweb = [p for p in packages if p.get("name") == "qubit-web"]
                expected = (root / "Cargo.toml").resolve()
                if len(qweb) != 1 or Path(qweb[0]["manifest_path"]).resolve() != expected or qweb[0].get("source") is not None:
                    raise ContractError(f"scenario {sid}: qubit-web metadata did not resolve to {expected}")
            print(f"PASS {sid}")
        except (ContractError, tomllib.TOMLDecodeError, OSError, json.JSONDecodeError) as error:
            print(f"FAIL {sid}: {error}", file=sys.stderr)
            return 1
    return 0

def main(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", type=Path, default=Path(__file__).resolve().parents[1])
    parser.add_argument("--artifacts-dir", type=Path)
    parser.add_argument("--target-dir", type=Path)
    args = parser.parse_args(argv)
    return run(args.root, args.artifacts_dir, args.target_dir)

if __name__ == "__main__":
    raise SystemExit(main())
