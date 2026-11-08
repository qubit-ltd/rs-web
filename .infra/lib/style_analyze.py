#!/usr/bin/env python3
"""Run the explicitly installed Analyzer without changing project source."""
from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import re
import shutil
import subprocess
import sys
import tempfile

EXCLUDED = {"generated", "vendor", "vendored", "target", ".git", ".codex", ".worktree"}


def regular(root: Path, relative: str) -> Path:
    """Reject traversal and symbolic links before consuming project inputs."""
    pure = PurePosixPath(relative)
    if (not relative or pure.is_absolute() or "\\" in relative
            or any(part in ("", ".", "..") for part in relative.split("/"))):
        raise ValueError(f"invalid project-relative source path: {relative!r}")
    path = root / relative
    for component in (path, *path.parents):
        if component == root:
            break
        if component.is_symlink():
            raise ValueError(f"symbolic-link input is unsupported: {relative}")
    if not path.is_file():
        raise ValueError(f"project input is not an existing regular file: {relative}")
    return path


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def analyzer(installation: Path) -> dict:
    """Verify native identity; never choose arbitrary shared cache entries."""
    override = os.environ.get("RUST_STYLE_ANALYZER")
    if override is not None:
        if not override:
            raise ValueError("RUST_STYLE_ANALYZER must name an executable")
        value = Path(override).expanduser()
        if not value.is_absolute():
            found = shutil.which(override)
            value = Path(found) if found else installation / value
        executable = value.resolve(strict=True)
        revision = None
        binary_digest = sha256(executable)
    else:
        suffix = ".exe" if os.name == "nt" else ""
        executable = regular(installation, f".infra/style/bin/rs-infra-style{suffix}")
        metadata_path = regular(installation, ".infra/style/tool.revision.json")
        raw = metadata_path.read_bytes()
        metadata = json.loads(raw)
        if (not isinstance(metadata, dict) or type(metadata.get("schema_version")) is not int
                or metadata["schema_version"] != 1 or metadata.get("tool") != "rs-infra-style"
                or not isinstance(metadata.get("revision"), str)
                or not re.fullmatch(r"[0-9a-f]{40}", metadata["revision"])
                or not isinstance(metadata.get("sha256"), str)
                or not re.fullmatch(r"[0-9a-f]{64}", metadata["sha256"])):
            raise ValueError("invalid installed Analyzer identity metadata")
        binary_digest = sha256(executable)
        if binary_digest != metadata["sha256"] or metadata_path.read_bytes() != raw:
            raise ValueError("installed Analyzer bytes and identity metadata do not match")
        revision = metadata["revision"]
    if not executable.is_file() or not os.access(executable, os.X_OK):
        raise ValueError("installed Analyzer is not executable")
    return {"schema_version": 1, "executable": str(executable),
            "sha256": binary_digest, "revision": revision}


def sources(project: Path) -> list[str]:
    """Include every Git-owned Rust file rather than Cargo's default targets."""
    result = subprocess.run(["git", "-C", str(project), "ls-files", "--cached", "--others",
                             "--exclude-standard", "-z"], capture_output=True)
    if result.returncode:
        raise ValueError("cannot enumerate project sources: " + result.stderr.decode(errors="replace"))
    selected = set()
    for raw in result.stdout.split(b"\0"):
        if not raw:
            continue
        name = os.fsdecode(raw)
        if not name.endswith(".rs") or EXCLUDED.intersection(PurePosixPath(name).parts):
            continue
        path = project / name
        # Staged deletions are no longer part of the current source tree.
        if not path.exists() and not path.is_symlink():
            continue
        regular(project, name)
        selected.add(name)
    return sorted(selected)


def external_temp_root(project: Path) -> Path:
    """Honor TMPDIR only when its resolved directory is outside the project."""
    candidates = [Path(tempfile.gettempdir())]
    if os.name != "nt":
        candidates.extend([Path("/tmp"), Path("/var/tmp")])
    if os.environ.get("SystemRoot"):
        candidates.append(Path(os.environ["SystemRoot"]) / "Temp")
    for candidate in candidates:
        candidate = candidate.resolve()
        if (candidate.is_dir() and os.access(candidate, os.W_OK)
                and candidate != project and project not in candidate.parents):
            return candidate
    raise ValueError("no writable temporary directory outside the project")


def main() -> int:
    installation = Path(sys.argv[1]).resolve(strict=True)
    parser = argparse.ArgumentParser(description="Analyze this project's Rust source without formatting or building it")
    parser.add_argument("--project", type=Path, default=installation,
                        help="project to analyze; defaults to the launcher project")
    parser.add_argument("--files-from", type=Path, help="optional JSON array of project-relative Rust paths")
    parser.add_argument("--standards", "--rules-directory", type=Path, dest="standards",
                        help="immutable normative snapshot checked by the native Analyzer")
    parser.add_argument("--format", choices=("text", "json"), default="text")
    parser.add_argument("--repository-readme", action="store_true")
    parser.add_argument("--describe-analyzer", action="store_true", help=argparse.SUPPRESS)
    args = parser.parse_args(sys.argv[2:])
    try:
        identity = analyzer(installation)
        if args.describe_analyzer:
            print(json.dumps(identity, ensure_ascii=False))
            return 0
        if args.project.is_symlink():
            raise ValueError("symbolic-link project roots are unsupported")
        project = args.project.resolve(strict=True)
        if not project.is_dir():
            raise ValueError("analysis project must be a directory")
        paths = sources(project) if args.files_from is None else json.loads(args.files_from.read_text())
        if not isinstance(paths, list) or any(not isinstance(path, str) or not path.endswith(".rs") for path in paths):
            raise ValueError("--files-from must contain a JSON array of Rust source paths")
        paths = sorted(set(paths))
        for path in paths:
            regular(project, path)
        # The native Analyzer receives only a snapshot of the validated list.
        with tempfile.TemporaryDirectory(prefix="rs-style-analyze-", dir=external_temp_root(project)) as temporary:
            manifest = Path(temporary) / "rust-source-list.json"
            manifest.write_text(json.dumps(paths, ensure_ascii=False), encoding="utf-8")
            command = [identity["executable"], "analyze", "--project", str(project),
                       "--files-from", str(manifest), "--format", args.format]
            if args.standards is not None:
                command += ["--standards", str(args.standards.resolve())]
            if args.repository_readme:
                command.append("--repository-readme")
            result = subprocess.run(command, cwd=project)
            if analyzer(installation) != identity:
                raise ValueError("Analyzer installation changed during analysis; result is not accepted")
            if result.returncode not in (0, 1, 2):
                raise ValueError(f"Analyzer returned unexpected exit code {result.returncode}")
            return result.returncode
    except (OSError, ValueError, TypeError) as error:
        print(f"error: {error}; run {installation / 'update-infra.sh'} to prepare a valid Analyzer", file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main())
