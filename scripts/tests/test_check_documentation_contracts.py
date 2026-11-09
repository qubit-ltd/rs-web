import importlib.util
import json
import contextlib
import io
import tempfile
import unittest
from pathlib import Path
from unittest import mock

SCRIPT = Path(__file__).resolve().parents[1] / "check-documentation-contracts.py"
SPEC = importlib.util.spec_from_file_location("check_documentation_contracts", SCRIPT)
checker = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(checker)

class DocumentationContractsTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        (self.root / "doc").mkdir()
        (self.root / "README.md").write_text("# Title\n## A\ntext\n```rust,ignore\nlet a = 1;\n```\n```rust\nlet b = 2;\n```\n## B\n```toml\n[dependencies]\nqubit-web = \"0.3\"\naxum = \"0.8\"\n```\n", encoding="utf-8")

    def tearDown(self):
        self.temp.cleanup()

    def test_manifest_has_exact_six_scenarios(self):
        data = json.loads(checker.CONTRACTS.read_text())
        checker.validate_scenarios(data)
        data["scenarios"].pop()
        with self.assertRaises(checker.ContractError):
            checker.validate_scenarios(data)

    def test_extracts_exact_heading_and_skips_ignored_unless_allowed(self):
        spec = {"source":"README.md", "heading":"A", "language":"rust", "ordinal":1}
        self.assertEqual(checker.extract_fence(self.root, spec), ("let b = 2;\n", 8))
        spec["allow_ignore"] = True
        self.assertEqual(checker.extract_fence(self.root, spec), ("let a = 1;\n", 5))

    def test_duplicate_heading_is_ambiguous(self):
        path = self.root / "README.md"
        path.write_text(path.read_text() + "\n## A\n```rust\nx\n```\n")
        with self.assertRaisesRegex(checker.ContractError, "expected one heading"):
            checker.extract_fence(self.root, {"source":"README.md", "heading":"A", "language":"rust", "ordinal":1})

    def test_missing_fence_fails(self):
        with self.assertRaisesRegex(checker.ContractError, "missing rust fence"):
            checker.extract_fence(self.root, {"source":"README.md", "heading":"B", "language":"rust", "ordinal":1})

    def test_unclosed_fence_fails(self):
        (self.root / "README.md").write_text("## A\n```rust\nx\n")
        with self.assertRaisesRegex(checker.ContractError, "unclosed"):
            checker.extract_fence(self.root, {"source":"README.md", "heading":"A", "language":"rust", "ordinal":1})

    def test_toml_parser_rejects_invalid_manifest(self):
        with self.assertRaises(Exception):
            checker.tomllib.loads("[dependencies\n")

    def test_dependency_patch_preserves_all_other_fields(self):
        raw = '[dependencies]\nqubit-web = { version = "0.3", features = ["json", "ws"] }\naxum = "0.8"\n'
        parsed = checker.tomllib.loads(raw)
        patched = checker.patch_qubit_web(raw, parsed, self.root)
        deps = checker.tomllib.loads(patched)["dependencies"]
        self.assertEqual(deps["qubit-web"], {"version":"0.3", "features":["json", "ws"], "path":str(self.root.resolve())})
        self.assertEqual(deps["axum"], "0.8")

    def test_missing_qubit_web_is_not_silently_added(self):
        raw = '[dependencies]\naxum = "0.8"\n'
        with self.assertRaisesRegex(checker.ContractError, "must declare"):
            checker.patch_qubit_web(raw, checker.tomllib.loads(raw), self.root)

    def test_wrapper_is_limited_to_budget_router(self):
        result = checker.source_file("let app = Router::new();", "budget-router")
        self.assertIn("fn main() -> Result<(), Box<dyn std::error::Error>>", result)
        self.assertTrue(result.endswith("let _: axum::Router<()> = app;\nOk(())\n}\n"))
        with self.assertRaisesRegex(checker.ContractError, "unsupported wrapper"):
            checker.source_file("x", "magical")

    def test_subprocess_failure_is_reported(self):
        manifest = '[dependencies]\nqubit-web = "0.3"\n'
        extracted = [(manifest, 1), ("fn main() {}\n", 1)]
        completed = mock.Mock(returncode=17, stdout="", stderr="rustc: deliberate failure")
        stderr = io.StringIO()
        with mock.patch.object(checker, "extract_fence", side_effect=extracted), \
             mock.patch.object(checker.subprocess, "run", return_value=completed) as run, \
             contextlib.redirect_stderr(stderr):
            self.assertEqual(checker.run(self.root), 1)
        self.assertIn("readme-en-quick-start", stderr.getvalue())
        self.assertIn("rustc: deliberate failure", stderr.getvalue())
        self.assertIn("cargo check", " ".join(run.call_args.args[0]))

if __name__ == "__main__":
    unittest.main()
