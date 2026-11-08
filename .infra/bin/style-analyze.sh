#!/usr/bin/env bash
set -euo pipefail

project_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd -P)
python_cmd=${RS_INFRA_PYTHON:-python3}
command -v "$python_cmd" >/dev/null 2>&1 || {
    echo "error: Python 3.10 or later is required to analyze Rust source" >&2
    exit 2
}
exec "$python_cmd" "$project_root/.infra/lib/style_analyze.py" "$project_root" "$@"
