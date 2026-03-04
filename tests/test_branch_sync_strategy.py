import contextlib
import sys
import unittest
from pathlib import Path
from unittest import mock

ROOT = Path(__file__).resolve().parents[1]
AGENTS_DIR = ROOT / "agents"
if str(AGENTS_DIR) not in sys.path:
    sys.path.insert(0, str(AGENTS_DIR))

import base as base_module  # noqa: E402


class BranchSyncStrategyTest(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.agent = base_module.BaseAgent()
        self.root = Path("/tmp/repo")
        self.worktree = self.root / ".worktrees" / "developer" / "task-1"
        self.branch = "agent/developer/task-1"

    async def asyncTearDown(self):
        with contextlib.suppress(Exception):
            await self.agent.http.aclose()
        with contextlib.suppress(Exception):
            await self.agent.http_output.aclose()

    async def test_sync_disabled_strategy(self):
        with mock.patch.object(base_module, "BRANCH_SYNC_STRATEGY", "none"):
            self.agent.git = mock.AsyncMock(return_value="")
            result = await self.agent._sync_branch_with_main(
                self.root, self.worktree, self.branch
            )
        self.assertEqual(result, "sync_disabled")
        self.agent.git.assert_not_called()

    async def test_sync_uses_merge_strategy_by_default(self):
        calls = []

        async def _fake_git(*args, cwd: Path, task_id=None):
            calls.append(args)
            if args[:3] == ("branch", "--list", "main"):
                return "  main"
            if args[:2] == ("status", "--porcelain"):
                return ""
            if args[:2] == ("rev-parse", "HEAD"):
                return "a" * 40 if len([c for c in calls if c[:2] == ("rev-parse", "HEAD")]) == 1 else "b" * 40
            if args[:3] == ("merge", "--no-edit", "main"):
                return ""
            return ""

        with mock.patch.object(base_module, "BRANCH_SYNC_STRATEGY", "merge"):
            self.agent.git = mock.AsyncMock(side_effect=_fake_git)
            result = await self.agent._sync_branch_with_main(
                self.root, self.worktree, self.branch
            )
        self.assertEqual(result, "merged")
        self.assertIn(("merge", "--no-edit", "main"), calls)

    async def test_sync_uses_rebase_strategy(self):
        calls = []
        rev_parse_calls = {"n": 0}

        async def _fake_git(*args, cwd: Path, task_id=None):
            calls.append(args)
            if args[:3] == ("branch", "--list", "main"):
                return "  main"
            if args[:2] == ("status", "--porcelain"):
                return ""
            if args[:2] == ("rev-parse", "HEAD"):
                rev_parse_calls["n"] += 1
                return "a" * 40 if rev_parse_calls["n"] == 1 else "c" * 40
            if args[:2] == ("rebase", "main"):
                return ""
            return ""

        with mock.patch.object(base_module, "BRANCH_SYNC_STRATEGY", "rebase"):
            self.agent.git = mock.AsyncMock(side_effect=_fake_git)
            result = await self.agent._sync_branch_with_main(
                self.root, self.worktree, self.branch
            )
        self.assertEqual(result, "rebased")
        self.assertIn(("rebase", "main"), calls)


if __name__ == "__main__":
    unittest.main()
