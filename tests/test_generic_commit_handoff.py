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

import generic as generic_module  # noqa: E402


class GenericCommitHandoffTest(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.root = Path(self._tmp.name) / "repo"
        self.worktree = self.root / ".worktrees" / "writer"
        self.worktree.mkdir(parents=True, exist_ok=True)

        self.agent = generic_module.GenericAgent(
            {
                "key": "writer",
                "name": "Writer",
                "poll_statuses": "[\"todo\"]",
                "next_status": "in_review",
                "working_status": "in_progress",
                "prompt": "do work: {task_title}",
                "cli": "claude",
            }
        )
        self.agent.ensure_agent_workspace = mock.AsyncMock(
            return_value=(self.root, self.worktree, "agent/writer")
        )
        self.agent.sync_from_latest_handoff = mock.AsyncMock(return_value={"status": "no_handoff"})
        self.agent.build_handoff_context = mock.AsyncMock(return_value="")
        self.agent.run_cli = mock.AsyncMock(return_value=(0, "done"))
        self.agent.stop_if_task_cancelled = mock.AsyncMock(return_value=False)
        self.agent.add_log = mock.AsyncMock()
        self.agent.add_alert = mock.AsyncMock()
        self.agent.transition_task = mock.AsyncMock(
            return_value={"task": {"id": "task-1", "status": "in_review"}}
        )

    async def asyncTearDown(self):
        with contextlib.suppress(Exception):
            await self.agent.http.aclose()
        with contextlib.suppress(Exception):
            await self.agent.http_output.aclose()
        self._tmp.cleanup()

    async def test_cli_commit_with_uncommitted_diff_still_handoffs(self):
        head_before = "a" * 40
        head_after = "b" * 40

        async def _fake_git(*args, cwd: Path, task_id=None):
            if args == ("rev-parse", "HEAD"):
                return head_before if _fake_git.rev_parse_calls == 0 else head_after
            if args == ("add", "-A"):
                return ""
            if args == ("diff", "--cached", "--stat"):
                return " main.js | 3 ++-"
            if args == ("rev-parse", "--short", "HEAD"):
                return head_after[:7]
            raise AssertionError(f"Unexpected git args: {args}")

        _fake_git.rev_parse_calls = 0

        async def _git_side_effect(*args, cwd: Path, task_id=None):
            if args == ("rev-parse", "HEAD"):
                val = await _fake_git(*args, cwd=cwd, task_id=task_id)
                _fake_git.rev_parse_calls += 1
                return val
            return await _fake_git(*args, cwd=cwd, task_id=task_id)

        self.agent.git = mock.AsyncMock(side_effect=_git_side_effect)

        task = {
            "id": "task-1",
            "title": "custom task",
            "description": "",
            "status": "in_progress",
            "_claimed_from_status": "todo",
        }

        await self.agent.process_task(task)

        self.agent.transition_task.assert_awaited_once()
        call = self.agent.transition_task.await_args
        self.assertEqual(call.kwargs["fields"]["status"], "in_review")
        self.assertEqual(call.kwargs["fields"]["commit_hash"], head_after[:7])
        self.assertEqual(call.kwargs["handoff"]["stage"], "writer_handoff")

        payload = call.kwargs["handoff"]["payload"]
        self.assertTrue(payload["committed_by_cli"])
        self.assertTrue(payload["has_uncommitted_changes"])
        self.assertIn("main.js", payload["uncommitted_diff_stat"])

    async def test_cli_commit_with_review_disabled_skips_to_approved(self):
        head_before = "a" * 40
        head_after = "d" * 40

        async def _fake_git(*args, cwd: Path, task_id=None):
            if args == ("rev-parse", "HEAD"):
                return head_before if _fake_git.rev_parse_calls == 0 else head_after
            if args == ("add", "-A"):
                return ""
            if args == ("diff", "--cached", "--stat"):
                return ""
            if args == ("rev-parse", "--short", "HEAD"):
                return head_after[:7]
            raise AssertionError(f"Unexpected git args: {args}")

        _fake_git.rev_parse_calls = 0

        async def _git_side_effect(*args, cwd: Path, task_id=None):
            if args == ("rev-parse", "HEAD"):
                val = await _fake_git(*args, cwd=cwd, task_id=task_id)
                _fake_git.rev_parse_calls += 1
                return val
            return await _fake_git(*args, cwd=cwd, task_id=task_id)

        self.agent.git = mock.AsyncMock(side_effect=_git_side_effect)

        task = {
            "id": "task-2",
            "title": "custom task no review",
            "description": "",
            "status": "in_progress",
            "_claimed_from_status": "todo",
            "review_enabled": 0,
        }

        await self.agent.process_task(task)

        self.agent.transition_task.assert_awaited_once()
        call = self.agent.transition_task.await_args
        self.assertEqual(call.kwargs["fields"]["status"], "approved")
        self.assertEqual(call.kwargs["fields"]["assigned_agent"], "manager")
        self.assertFalse(call.kwargs["handoff"]["payload"]["review_enabled"])

    async def test_prompt_includes_execution_contract_for_generic_worker(self):
        captured = {}

        async def _capture_run_cli(prompt, cwd, **kwargs):
            captured["prompt"] = prompt
            return 0, "done"

        async def _fake_git(*args, cwd: Path, task_id=None):
            if args == ("rev-parse", "HEAD"):
                return "a" * 40
            if args == ("add", "-A"):
                return ""
            if args == ("diff", "--cached", "--stat"):
                return ""
            raise AssertionError(f"Unexpected git args: {args}")

        self.agent.run_cli = mock.AsyncMock(side_effect=_capture_run_cli)
        self.agent.git = mock.AsyncMock(side_effect=_fake_git)

        task = {
            "id": "task-3",
            "title": "write FAQ",
            "description": (
                "## 任务目标\n- 输出客服 FAQ 初稿\n\n"
                "## 交付物\n- docs/faq.md\n\n"
                "## 验收标准\n- [ ] FAQ 至少覆盖 10 个常见问题\n- [ ] 结构清晰便于审查"
            ),
            "status": "in_progress",
            "_claimed_from_status": "todo",
        }

        await self.agent.process_task(task)

        prompt = captured["prompt"]
        self.assertIn("## 执行基线（必须遵守）", prompt)
        self.assertIn("docs/faq.md", prompt)
        self.assertIn("FAQ 至少覆盖 10 个常见问题", prompt)


if __name__ == "__main__":
    unittest.main()
