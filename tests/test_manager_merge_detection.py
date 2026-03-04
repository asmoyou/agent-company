import contextlib
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

ROOT = Path(__file__).resolve().parents[1]
AGENTS_DIR = ROOT / "agents"
if str(AGENTS_DIR) not in sys.path:
    sys.path.insert(0, str(AGENTS_DIR))

import manager as manager_module  # noqa: E402


class ManagerMergeDetectionTest(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.repo = Path(self._tmp.name) / "repo"
        self.repo.mkdir(parents=True, exist_ok=True)
        self.agent = manager_module.ManagerAgent()
        self.agent.add_log = mock.AsyncMock()
        self.agent.add_alert = mock.AsyncMock()
        self.agent.transition_task = mock.AsyncMock()
        self.agent.stop_if_task_cancelled = mock.AsyncMock(return_value=False)
        self.agent.build_handoff_context = mock.AsyncMock(return_value="")
        self.agent._ensure_on_main = mock.AsyncMock()
        self.agent._cleanup_merge_state = mock.AsyncMock()

    async def asyncTearDown(self):
        with contextlib.suppress(Exception):
            await self.agent.http.aclose()
        with contextlib.suppress(Exception):
            await self.agent.http_output.aclose()
        self._tmp.cleanup()

    def _task(self, commit_hash: str) -> dict:
        return {
            "id": "task-1",
            "title": "写一份锅包肉的菜谱",
            "description": "",
            "status": "approved",
            "assignee": "manager",
            "project_id": "project-1",
            "commit_hash": commit_hash,
            "assigned_agent": "developer",
            "dev_agent": "developer",
        }

    def test_output_has_conflict_signal_ignores_negated_conflict(self):
        self.assertFalse(self.agent._output_has_conflict_signal("冲突情况：无冲突"))
        self.assertFalse(self.agent._output_has_conflict_signal("若冲突，停止并保留冲突现场。"))

    def test_output_has_conflict_signal_detects_real_conflict(self):
        self.assertTrue(
            self.agent._output_has_conflict_signal(
                "CONFLICT (content): Merge conflict in index.html"
            )
        )
        self.assertTrue(self.agent._output_has_conflict_signal("error: could not apply abc123"))

    async def test_is_patch_equivalent_on_ref_parses_git_cherry_marker(self):
        commit = "4c6a0941655523f7dd2aded90e055525d813c1d1"
        self.agent.git = mock.AsyncMock(return_value=f"- {commit}\n")
        self.assertTrue(await self.agent._is_patch_equivalent_on_ref(self.repo, commit, "main"))

        self.agent.git = mock.AsyncMock(return_value=f"+ {commit}\n")
        self.assertFalse(await self.agent._is_patch_equivalent_on_ref(self.repo, commit, "main"))

    async def test_process_task_accepts_cherry_pick_equivalent_merge(self):
        target_commit = "4c6a0941655523f7dd2aded90e055525d813c1d1"
        task = self._task(target_commit)
        self.agent.ensure_agent_workspace = mock.AsyncMock(
            return_value=(self.repo, self.repo / ".worktrees" / "developer", "agent/developer")
        )
        self.agent.run_cli = mock.AsyncMock(
            return_value=(
                0,
                "合并结果：main 新提交为 14a2a1a\n冲突情况：无冲突\n",
            )
        )
        self.agent._is_ancestor = mock.AsyncMock(side_effect=[True, False, False])
        self.agent._is_patch_equivalent_on_ref = mock.AsyncMock(side_effect=[False, True])

        heads = iter(["ec0a922", "14a2a1a"])

        async def _fake_git(*args, cwd: Path, task_id=None):
            if args[:2] == ("cat-file", "-e"):
                return ""
            if args[:2] == ("rev-parse", "--short"):
                return next(heads)
            return ""

        self.agent.git = mock.AsyncMock(side_effect=_fake_git)

        await self.agent.process_task(task)

        self.agent.add_alert.assert_not_awaited()
        self.agent.transition_task.assert_awaited_once()
        call = self.agent.transition_task.await_args
        self.assertEqual(call.args[0], task["id"])
        self.assertEqual(call.kwargs["fields"]["status"], "pending_acceptance")
        self.assertEqual(call.kwargs["handoff"]["stage"], "merge_to_acceptance")
        self.assertEqual(call.kwargs["handoff"]["title"], "合并完成，交接验收")

    async def test_process_task_returns_to_dev_when_commit_parent_not_on_main(self):
        target_commit = "4c6a0941655523f7dd2aded90e055525d813c1d1"
        parent_commit = "7f9c2ba4e88f827d616045507605853ed73b809c"
        task = self._task(target_commit)
        self.agent.ensure_agent_workspace = mock.AsyncMock(
            return_value=(self.repo, self.repo / ".worktrees" / "developer" / "task-1", "agent/developer/task-1")
        )
        self.agent.run_cli = mock.AsyncMock(return_value=(0, "should not run"))
        self.agent._is_ancestor = mock.AsyncMock(side_effect=[True, False])
        self.agent._is_patch_equivalent_on_ref = mock.AsyncMock(return_value=False)

        async def _fake_git(*args, cwd: Path, task_id=None):
            if args[:2] == ("cat-file", "-e"):
                return ""
            if args == ("rev-parse", f"{target_commit}^"):
                return parent_commit
            return "ec0a922"

        self.agent.git = mock.AsyncMock(side_effect=_fake_git)

        await self.agent.process_task(task)

        self.agent.run_cli.assert_not_awaited()
        self.agent.transition_task.assert_awaited_once()
        call = self.agent.transition_task.await_args
        self.assertEqual(call.args[0], task["id"])
        self.assertEqual(call.kwargs["fields"]["status"], "needs_changes")
        self.assertEqual(call.kwargs["handoff"]["stage"], "merge_to_dev")
        self.assertIn("提交基线不一致", call.kwargs["handoff"]["summary"])


if __name__ == "__main__":
    unittest.main()
