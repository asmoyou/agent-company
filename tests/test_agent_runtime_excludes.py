import contextlib
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
AGENTS_DIR = ROOT / "agents"
if str(AGENTS_DIR) not in sys.path:
    sys.path.insert(0, str(AGENTS_DIR))

import base as base_module  # noqa: E402


class AgentRuntimeExcludesTest(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.worktree = Path(self._tmp.name) / "repo"
        (self.worktree / ".git" / "info").mkdir(parents=True, exist_ok=True)
        self.agent = base_module.BaseAgent()

    def tearDown(self):
        with contextlib.suppress(Exception):
            import asyncio

            asyncio.run(self.agent.http.aclose())
            asyncio.run(self.agent.http_output.aclose())
        self._tmp.cleanup()

    def test_ensure_runtime_git_excludes_adds_decision_dir_once(self):
        exclude_path = self.worktree / ".git" / "info" / "exclude"

        self.agent._ensure_runtime_git_excludes(self.worktree)
        self.agent._ensure_runtime_git_excludes(self.worktree)

        content = exclude_path.read_text(encoding="utf-8")
        self.assertIn(".opc/decisions/", content)
        self.assertEqual(content.count(".opc/decisions/"), 1)
        self.assertIn("opc-codex-last-*", content)
        self.assertIn("opc-codex-schema-*", content)

    def test_ensure_runtime_git_excludes_supports_worktree_gitdir_file(self):
        repo = Path(self._tmp.name) / "linked-repo"
        actual_git_dir = Path(self._tmp.name) / "actual-gitdir"
        repo.mkdir(parents=True, exist_ok=True)
        (actual_git_dir / "info").mkdir(parents=True, exist_ok=True)
        (repo / ".git").write_text(f"gitdir: {actual_git_dir}\n", encoding="utf-8")

        self.agent._ensure_runtime_git_excludes(repo)

        content = (actual_git_dir / "info" / "exclude").read_text(encoding="utf-8")
        self.assertIn(".opc/decisions/", content)


if __name__ == "__main__":
    unittest.main()
