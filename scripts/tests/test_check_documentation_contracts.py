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

    def test_manifest_has_exact_scenario_mappings(self):
        data = json.loads(checker.CONTRACTS.read_text())
        self.assertEqual(
            [
                (s["id"], s["manifest"], s["code"], s["wrapper"])
                for s in data["scenarios"]
            ],
            [
                ("readme-en-quick-start", {"source": "README.md", "heading": "Install", "language": "toml", "ordinal": 1}, {"source": "README.md", "heading": "Quick start", "language": "rust", "ordinal": 1}, "binary"),
                ("readme-zh-quick-start", {"source": "README.zh_CN.md", "heading": "安装", "language": "toml", "ordinal": 1}, {"source": "README.zh_CN.md", "heading": "快速开始", "language": "rust", "ordinal": 1}, "binary"),
                ("guide-en-prompt-stream", {"source": "doc/user_guide.md", "heading": "Prepare settings and start the listener", "language": "toml", "ordinal": 1}, {"source": "examples/prompt_stream.rs", "whole_file": True}, "binary"),
                ("guide-zh-prompt-stream", {"source": "doc/user_guide.zh_CN.md", "heading": "配置并启动监听器", "language": "toml", "ordinal": 1}, {"source": "examples/prompt_stream.rs", "whole_file": True}, "binary"),
                ("guide-en-extractor-budgets", {"source": "doc/user_guide.md", "heading": "Prepare settings and start the listener", "language": "toml", "ordinal": 1}, {"source": "doc/user_guide.md", "heading": "Combine Axum extractor limits with request budgets", "language": "rust", "ordinal": 1, "allow_ignore": True}, "budget-router"),
                ("guide-zh-extractor-budgets", {"source": "doc/user_guide.zh_CN.md", "heading": "配置并启动监听器", "language": "toml", "ordinal": 1}, {"source": "doc/user_guide.zh_CN.md", "heading": "原生提取器与请求体预算要分别配置", "language": "rust", "ordinal": 1, "allow_ignore": True}, "budget-router"),
            ],
        )

    def test_extracts_exact_heading_and_skips_ignored_unless_allowed(self):
        spec = {"source":"README.md", "heading":"A", "language":"rust", "ordinal":1}
        self.assertEqual(checker.extract_fence(self.root, spec), ("let b = 2;\n", 8))
        spec["allow_ignore"] = True
        self.assertEqual(checker.extract_fence(self.root, spec), ("let a = 1;\n", 5))

    def test_ignore_token_is_recognized_in_any_order(self):
        (self.root / "README.md").write_text("## A\n```rust,edition2024,ignore\nlet a = 1;\n```\n```rust,ignore,edition2024\nlet b = 2;\n```\n", encoding="utf-8")
        spec = {"source": "README.md", "heading": "A", "language": "rust", "ordinal": 1}
        with self.assertRaisesRegex(checker.ContractError, "missing rust fence"):
            checker.extract_fence(self.root, spec)
        spec["allow_ignore"] = True
        self.assertEqual(checker.extract_fence(self.root, spec), ("let a = 1;\n", 3))

    def test_duplicate_heading_is_ambiguous(self):
        path = self.root / "README.md"
        path.write_text(path.read_text() + "\n## A\n```rust\nx\n```\n")
        with self.assertRaisesRegex(checker.ContractError, "expected one heading"):
            checker.extract_fence(self.root, {"source":"README.md", "heading":"A", "language":"rust", "ordinal":1})

    def test_heading_looking_text_inside_fence_is_not_a_heading(self):
        (self.root / "README.md").write_text("## A\n~~~markdown\n## A\n~~~\n```rust\nlet a = 1;\n```\n", encoding="utf-8")
        self.assertEqual(checker.extract_fence(self.root, {"source": "README.md", "heading": "A", "language": "rust", "ordinal": 1}), ("let a = 1;\n", 6))

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

    def test_metadata_failure_reports_and_saves_diagnostics(self):
        manifest = '[dependencies]\nqubit-web = "0.3"\n'
        extracted = [(manifest, 12), ("fn main() {}\n", 34)]
        check_ok = mock.Mock(returncode=0, stdout="", stderr="")
        metadata_failed = mock.Mock(returncode=1, stdout="", stderr="metadata failed")
        artifacts = self.root / "artifacts"
        stderr = io.StringIO()
        with mock.patch.object(checker, "extract_fence", side_effect=extracted), \
             mock.patch.object(checker.subprocess, "run", side_effect=[check_ok, metadata_failed]) as run, \
             contextlib.redirect_stderr(stderr):
            self.assertEqual(checker.run(self.root, artifacts_dir=artifacts), 1)
        diagnostic = stderr.getvalue()
        self.assertIn("readme-en-quick-start", diagnostic)
        self.assertIn("README.md:34", diagnostic)
        self.assertIn("cargo metadata", diagnostic)
        self.assertIn("metadata failed", diagnostic)
        saved = artifacts / "readme-en-quick-start"
        self.assertIn("cargo metadata", (saved / "command.txt").read_text())
        self.assertEqual((saved / "stderr.txt").read_text(), "metadata failed")
        self.assertTrue((saved / "Cargo.toml").is_file())
        self.assertTrue((saved / "main.rs").is_file())
        self.assertEqual(run.call_count, 2)

if __name__ == "__main__":
    unittest.main()
