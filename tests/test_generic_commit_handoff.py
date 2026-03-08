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
        self.agent.build_patchset_snapshot = mock.AsyncMock(
            return_value={
                "id": "ps-writer-1",
                "source_branch": "agent/writer",
                "base_sha": "a" * 40,
                "head_sha": "b" * 40,
                "commit_count": 1,
                "commit_list": [{"hash": "b" * 40, "short": "bbbbbbb", "subject": "feat: task"}],
                "diff_stat": " main.js | 3 ++-",
                "status": "",
                "worktree_clean": False,
                "merge_strategy": "squash",
                "summary": "",
                "artifact_path": str(self.worktree),
                "created_by_agent": "writer",
            }
        )
        self.agent.run_cli = mock.AsyncMock(return_value=(0, "done"))
        self.agent.stop_if_task_cancelled = mock.AsyncMock(return_value=False)
        self.agent.get_handoffs = mock.AsyncMock(return_value=[])
        self.agent.add_log = mock.AsyncMock()
        self.agent.add_alert = mock.AsyncMock()
        self.agent.update_patchset = mock.AsyncMock(return_value={"id": "ps-writer-1", "status": "draft"})
        self.agent.transition_task = mock.AsyncMock(
            return_value={"task": {"id": "task-1", "status": "in_review"}}
        )

    async def asyncTearDown(self):
        with contextlib.suppress(Exception):
            await self.agent.http.aclose()
        with contextlib.suppress(Exception):
            await self.agent.http_output.aclose()
        self._tmp.cleanup()

    async def test_cli_commit_with_uncommitted_diff_returns_to_previous_status(self):
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

        self.assertEqual(self.agent.transition_task.await_count, 2)
        checkpoint_call = self.agent.transition_task.await_args_list[0]
        self.assertEqual(checkpoint_call.kwargs["fields"]["status"], "in_progress")
        self.assertEqual(checkpoint_call.kwargs["fields"]["current_patchset_status"], "draft")
        call = self.agent.transition_task.await_args_list[1]
        self.assertEqual(call.kwargs["fields"]["status"], "todo")
        self.assertEqual(call.kwargs["fields"]["commit_hash"], head_after[:7])
        self.assertEqual(call.kwargs["fields"]["current_patchset_id"], "ps-writer-1")
        self.assertEqual(call.kwargs["fields"]["current_patchset_status"], "draft")
        self.assertEqual(call.kwargs["handoff"]["stage"], "writer_dirty_patchset")
        self.assertEqual(call.kwargs["handoff"]["status_from"], "in_progress")

        payload = call.kwargs["handoff"]["payload"]
        self.assertTrue(payload["committed_by_cli"])
        self.assertTrue(payload["has_uncommitted_changes"])
        self.assertTrue(payload["requires_clean_worktree"])
        self.assertIn("main.js", payload["uncommitted_diff_stat"])
        self.assertEqual(payload["patchset"]["head_sha"], head_after)
        self.assertEqual(payload["patchset"]["id"], "ps-writer-1")
        self.assertEqual(payload["patchset"]["status"], "draft")

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

        self.assertEqual(self.agent.transition_task.await_count, 2)
        checkpoint_call = self.agent.transition_task.await_args_list[0]
        self.assertEqual(checkpoint_call.kwargs["fields"]["status"], "in_progress")
        self.assertEqual(checkpoint_call.kwargs["fields"]["current_patchset_status"], "draft")
        call = self.agent.transition_task.await_args_list[1]
        self.assertEqual(call.kwargs["fields"]["status"], "approved")
        self.assertEqual(call.kwargs["fields"]["assigned_agent"], "manager")
        self.assertFalse(call.kwargs["handoff"]["payload"]["review_enabled"])
        self.assertEqual(call.kwargs["handoff"]["status_from"], "in_progress")

    async def test_post_commit_transition_failure_keeps_checkpointed_delivery_artifacts(self):
        head_before = "a" * 40
        head_after = "e" * 40

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
        self.agent.transition_task = mock.AsyncMock(
            side_effect=[
                {"task": {"id": "task-7", "status": "in_progress"}},
                RuntimeError("Transition failed (422) for task task-7: handoff.status_from 与任务当前状态不一致"),
            ]
        )

        task = {
            "id": "task-7",
            "title": "post commit checkpoint",
            "description": "",
            "status": "in_progress",
            "_claimed_from_status": "todo",
        }

        await self.agent.process_task(task)

        self.agent.update_patchset.assert_awaited_once()
        patchset_call = self.agent.update_patchset.await_args
        self.assertTrue(patchset_call.kwargs["update_task_refs"])
        self.assertEqual(patchset_call.kwargs["patchset"]["status"], "draft")

        self.assertEqual(self.agent.transition_task.await_count, 2)
        checkpoint_call = self.agent.transition_task.await_args_list[0]
        final_call = self.agent.transition_task.await_args_list[1]
        self.assertEqual(checkpoint_call.kwargs["fields"]["status"], "in_progress")
        self.assertEqual(checkpoint_call.kwargs["fields"]["commit_hash"], head_after[:7])
        self.assertEqual(checkpoint_call.kwargs["fields"]["current_patchset_status"], "draft")
        self.assertEqual(final_call.kwargs["fields"]["status"], "in_review")
        self.agent.add_alert.assert_awaited()
        self.assertIn("无法把任务推进到 in_review", self.agent.add_log.await_args_list[-1].args[1])

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

    async def test_no_progress_blocks_after_three_consecutive_delivery_failures(self):
        async def _fake_git(*args, cwd: Path, task_id=None):
            if args == ("rev-parse", "HEAD"):
                return "a" * 40
            if args == ("add", "-A"):
                return ""
            if args == ("diff", "--cached", "--stat"):
                return ""
            raise AssertionError(f"Unexpected git args: {args}")

        self.agent.git = mock.AsyncMock(side_effect=_fake_git)
        self.agent.get_handoffs = mock.AsyncMock(
            return_value=[
                {"stage": "writer_commit_required"},
                {"stage": "writer_no_progress"},
            ]
        )

        task = {
            "id": "task-4",
            "title": "no delivery",
            "description": "",
            "status": "in_progress",
            "_claimed_from_status": "todo",
        }

        await self.agent.process_task(task)

        self.agent.transition_task.assert_awaited_once()
        call = self.agent.transition_task.await_args
        self.assertEqual(call.kwargs["fields"]["status"], "blocked")
        self.assertEqual(call.kwargs["fields"]["assigned_agent"], "writer")
        self.assertEqual(call.kwargs["handoff"]["stage"], "writer_delivery_blocked")
        payload = call.kwargs["handoff"]["payload"]
        self.assertTrue(payload["delivery_blocked"])
        self.assertEqual(payload["delivery_retry_count"], 3)
        self.assertEqual(payload["resume_status"], "todo")
        self.assertEqual(payload["latest_failure_stage"], "writer_no_progress")

    async def test_commit_required_blocks_after_three_consecutive_delivery_failures(self):
        async def _fake_git(*args, cwd: Path, task_id=None):
            if args == ("rev-parse", "HEAD"):
                return "a" * 40
            if args == ("add", "-A"):
                return ""
            if args == ("diff", "--cached", "--stat"):
                return " notes.md | 10 +++++-----"
            raise AssertionError(f"Unexpected git args: {args}")

        self.agent.git = mock.AsyncMock(side_effect=_fake_git)
        self.agent.get_handoffs = mock.AsyncMock(
            return_value=[
                {"stage": "writer_no_progress"},
                {"stage": "writer_commit_required"},
            ]
        )

        task = {
            "id": "task-5",
            "title": "forgot to commit",
            "description": "",
            "status": "in_progress",
            "_claimed_from_status": "todo",
        }

        await self.agent.process_task(task)

        self.agent.transition_task.assert_awaited_once()
        call = self.agent.transition_task.await_args
        self.assertEqual(call.kwargs["fields"]["status"], "blocked")
        self.assertEqual(call.kwargs["handoff"]["stage"], "writer_delivery_blocked")
        payload = call.kwargs["handoff"]["payload"]
        self.assertTrue(payload["requires_cli_commit"])
        self.assertEqual(payload["delivery_retry_count"], 3)
        self.assertEqual(payload["latest_failure_stage"], "writer_commit_required")

    async def test_resolve_task_patchset_ignores_conflicting_handoff_patchset(self):
        self.agent.get_handoffs = mock.AsyncMock(
            return_value=[
                {
                    "to_agent": "writer",
                    "payload": {
                        "patchset": {
                            "id": "ps-stale-1",
                            "head_sha": "a" * 40,
                            "status": "submitted",
                        }
                    },
                }
            ]
        )

        resolved = await self.agent.resolve_task_patchset(
            {
                "id": "task-6",
                "current_patchset_id": "ps-current-1",
                "current_patchset_status": "approved",
                "commit_hash": "b" * 40,
            }
        )

        self.assertIsNotNone(resolved)
        self.assertEqual(resolved["id"], "ps-current-1")
        self.assertEqual(resolved["head_sha"], "b" * 40)
        self.assertEqual(resolved["status"], "approved")


if __name__ == "__main__":
    unittest.main()
