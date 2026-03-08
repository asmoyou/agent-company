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
        self.agent.resolve_task_patchset = mock.AsyncMock(return_value=None)
        self.agent.update_patchset = mock.AsyncMock(return_value={})
        self.agent.enrich_patchset_snapshot = mock.AsyncMock(side_effect=lambda repo_root, patchset, source_branch="": patchset)
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
            "status": "merging",
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
            if args[:2] == ("config", "user.email"):
                return ""
            if args[:2] == ("config", "user.name"):
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
        self.assertEqual(call.kwargs["handoff"]["status_from"], "merging")
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
        self.assertEqual(call.kwargs["handoff"]["status_from"], "merging")
        self.assertIn("提交基线不一致", call.kwargs["handoff"]["summary"])

    async def test_process_task_conflict_auto_merge_success(self):
        target_commit = "4c6a0941655523f7dd2aded90e055525d813c1d1"
        task = self._task(target_commit)
        self.agent.ensure_agent_workspace = mock.AsyncMock(
            return_value=(self.repo, self.repo / ".worktrees" / "developer" / "task-1", "agent/developer/task-1")
        )
        self.agent.run_cli = mock.AsyncMock(
            return_value=(0, "CONFLICT (content): Merge conflict in index.html")
        )
        self.agent._attempt_auto_merge_strategies = mock.AsyncMock(
            return_value={
                "resolved": True,
                "strategy": "theirs",
                "head_after": "14a2a1a",
                "attempts": [{"strategy": "theirs", "status": "merged", "conflicts": []}],
            }
        )
        self.agent._is_ancestor = mock.AsyncMock(side_effect=[True, False, False, True])
        self.agent._is_patch_equivalent_on_ref = mock.AsyncMock(side_effect=[False, False])

        heads = iter(["ec0a922", "ec0a922"])

        async def _fake_git(*args, cwd: Path, task_id=None):
            if args[:2] == ("cat-file", "-e"):
                return ""
            if args[:2] == ("rev-parse", "--short"):
                return next(heads)
            if args[0] == "rev-parse" and args[1].endswith("^"):
                raise RuntimeError("no parent in test")
            return ""

        self.agent.git = mock.AsyncMock(side_effect=_fake_git)

        await self.agent.process_task(task)

        self.agent.transition_task.assert_awaited_once()
        call = self.agent.transition_task.await_args
        self.assertEqual(call.kwargs["fields"]["status"], "pending_acceptance")
        self.assertIn("冲突自动处理成功", call.kwargs["handoff"]["summary"])
        self.assertEqual(call.kwargs["handoff"]["payload"]["auto_merge_strategy"], "theirs")

    async def test_process_task_conflict_auto_merge_fail_returns_actionable_feedback(self):
        target_commit = "4c6a0941655523f7dd2aded90e055525d813c1d1"
        task = self._task(target_commit)
        self.agent.ensure_agent_workspace = mock.AsyncMock(
            return_value=(self.repo, self.repo / ".worktrees" / "developer" / "task-1", "agent/developer/task-1")
        )
        self.agent.run_cli = mock.AsyncMock(
            return_value=(0, "CONFLICT (modify/delete): cooking-game.html deleted in HEAD")
        )
        self.agent._attempt_auto_merge_strategies = mock.AsyncMock(
            return_value={
                "resolved": False,
                "strategy": "",
                "head_after": "",
                "attempts": [
                    {
                        "strategy": "theirs",
                        "status": "failed",
                        "error": "could not apply",
                        "conflicts": [{"code": "DU", "path": "cooking-game.html"}],
                    }
                ],
            }
        )
        self.agent._is_ancestor = mock.AsyncMock(side_effect=[True, False, False])
        self.agent._is_patch_equivalent_on_ref = mock.AsyncMock(side_effect=[False, False])

        heads = iter(["ec0a922", "ec0a922"])

        async def _fake_git(*args, cwd: Path, task_id=None):
            if args[:2] == ("cat-file", "-e"):
                return ""
            if args[:2] == ("rev-parse", "--short"):
                return next(heads)
            if args[0] == "rev-parse" and args[1].endswith("^"):
                raise RuntimeError("no parent in test")
            return ""

        self.agent.git = mock.AsyncMock(side_effect=_fake_git)

        await self.agent.process_task(task)

        self.agent.transition_task.assert_awaited_once()
        call = self.agent.transition_task.await_args
        self.assertEqual(call.kwargs["fields"]["status"], "needs_changes")
        self.assertEqual(call.kwargs["handoff"]["stage"], "merge_to_dev")
        self.assertIn("修改建议", call.kwargs["fields"]["review_feedback"])
        self.assertIn("DU cooking-game.html", call.kwargs["fields"]["review_feedback"])

    async def test_process_task_merges_patchset_via_squash_without_cli(self):
        head_sha = "4c6a0941655523f7dd2aded90e055525d813c1d1"
        task = self._task(head_sha)
        self.agent.resolve_task_patchset = mock.AsyncMock(
            return_value={
                "id": "ps-merge-1",
                "base_sha": "7f9c2ba4e88f827d616045507605853ed73b809c",
                "head_sha": head_sha,
                "source_branch": "agent/developer/task-1",
                "commit_count": 3,
                "commit_list": [],
                "diff_stat": " index.html | 2 +-",
                "status": "approved",
                "worktree_clean": True,
            }
        )
        self.agent.ensure_agent_workspace = mock.AsyncMock(
            return_value=(self.repo, self.repo / ".worktrees" / "developer" / "task-1", "agent/developer/task-1")
        )
        self.agent.run_cli = mock.AsyncMock(return_value=(0, "should not run"))

        heads = iter(["ec0a922", "14a2a1a"])

        async def _fake_git(*args, cwd: Path, task_id=None):
            if args[:2] == ("cat-file", "-e"):
                return ""
            if args == ("rev-parse", "HEAD"):
                return "ec0a9227f8c8f62ef73fb5a86d6b5f1f0d5f0f0f"
            if args == ("status", "--porcelain"):
                return ""
            if args[:2] == ("rev-parse", "--short"):
                return next(heads)
            if args == ("merge", "--squash", "--no-commit", head_sha):
                return ""
            if args == ("diff", "--cached", "--stat"):
                return " index.html | 2 +-"
            if args[:2] == ("commit", "-m"):
                return ""
            raise AssertionError(f"Unexpected git args: {args}")

        self.agent.git = mock.AsyncMock(side_effect=_fake_git)

        await self.agent.process_task(task)

        self.agent.run_cli.assert_not_awaited()
        self.agent.transition_task.assert_awaited_once()
        call = self.agent.transition_task.await_args
        self.assertEqual(call.kwargs["fields"]["status"], "pending_acceptance")
        self.assertEqual(call.kwargs["fields"]["current_patchset_status"], "merged")
        self.assertEqual(call.kwargs["fields"]["merged_patchset_id"], "ps-merge-1")
        self.assertEqual(call.kwargs["handoff"]["payload"]["patchset"]["merge_strategy"], "squash")
        self.assertEqual(call.kwargs["handoff"]["payload"]["patchset"]["queue_status"], "merged")
        self.assertEqual(call.kwargs["handoff"]["payload"]["patchset"]["queue_main_sha"], "ec0a9227f8c8f62ef73fb5a86d6b5f1f0d5f0f0f")
        self.agent.update_patchset.assert_awaited_once()

    async def test_process_task_returns_refresh_hint_when_patchset_stales_after_main_advanced(self):
        head_sha = "4c6a0941655523f7dd2aded90e055525d813c1d1"
        task = self._task(head_sha)
        patchset = {
            "id": "ps-stale-1",
            "base_sha": "7f9c2ba4e88f827d616045507605853ed73b809c",
            "head_sha": head_sha,
            "source_branch": "agent/developer/task-1",
            "commit_count": 2,
            "status": "approved",
            "reviewed_main_sha": "c" * 40,
            "changed_files": [{"status": "M", "path": "index.html"}],
        }
        self.agent.resolve_task_patchset = mock.AsyncMock(return_value=patchset)
        self.agent.enrich_patchset_snapshot = mock.AsyncMock(return_value=patchset)
        self.agent.ensure_agent_workspace = mock.AsyncMock(
            return_value=(self.repo, self.repo / ".worktrees" / "developer" / "task-1", "agent/developer/task-1")
        )
        self.agent._merge_patchset_squash = mock.AsyncMock(
            return_value={
                "status": "conflict",
                "head_before": "ec0a922",
                "head_after": "ec0a922",
                "conflicts": [{"code": "UU", "path": "index.html"}],
            }
        )

        async def _fake_git(*args, cwd: Path, task_id=None):
            if args[:2] == ("cat-file", "-e"):
                return ""
            if args == ("rev-parse", "HEAD"):
                return "d" * 40
            if args == ("status", "--porcelain"):
                return ""
            raise AssertionError(f"Unexpected git args: {args}")

        self.agent.git = mock.AsyncMock(side_effect=_fake_git)

        await self.agent.process_task(task)

        self.agent.transition_task.assert_awaited_once()
        call = self.agent.transition_task.await_args
        self.assertEqual(call.kwargs["fields"]["status"], "needs_changes")
        self.assertEqual(call.kwargs["handoff"]["payload"]["patchset"]["queue_reason"], "main_advanced_before_merge")
        self.assertEqual(call.kwargs["handoff"]["payload"]["refresh_hint"]["reason"], "main_advanced_before_merge")
        self.assertEqual(call.kwargs["handoff"]["payload"]["refresh_hint"]["changed_files"][0]["path"], "index.html")

    async def test_process_task_returns_to_dev_before_merge_when_reviewed_main_is_stale(self):
        head_sha = "4c6a0941655523f7dd2aded90e055525d813c1d1"
        task = self._task(head_sha)
        patchset = {
            "id": "ps-stale-precheck-1",
            "base_sha": "7f9c2ba4e88f827d616045507605853ed73b809c",
            "head_sha": head_sha,
            "source_branch": "agent/developer/task-1",
            "commit_count": 2,
            "status": "approved",
            "reviewed_main_sha": "c" * 40,
            "changed_files": [{"status": "M", "path": "index.html"}],
        }
        self.agent.resolve_task_patchset = mock.AsyncMock(return_value=patchset)
        self.agent.enrich_patchset_snapshot = mock.AsyncMock(return_value=patchset)
        self.agent.ensure_agent_workspace = mock.AsyncMock(
            return_value=(self.repo, self.repo / ".worktrees" / "developer" / "task-1", "agent/developer/task-1")
        )
        self.agent._merge_patchset_squash = mock.AsyncMock(return_value={"status": "merged"})

        async def _fake_git(*args, cwd: Path, task_id=None):
            if args == ("rev-parse", "HEAD"):
                return "d" * 40
            if args == ("status", "--porcelain"):
                return ""
            raise AssertionError(f"Unexpected git args: {args}")

        self.agent.git = mock.AsyncMock(side_effect=_fake_git)

        await self.agent.process_task(task)

        self.agent._merge_patchset_squash.assert_not_awaited()
        call = self.agent.transition_task.await_args
        self.assertEqual(call.kwargs["fields"]["status"], "needs_changes")
        self.assertEqual(call.kwargs["handoff"]["payload"]["patchset"]["queue_reason"], "main_advanced_before_merge")

    async def test_process_task_blocks_when_main_worktree_is_dirty(self):
        head_sha = "4c6a0941655523f7dd2aded90e055525d813c1d1"
        task = self._task(head_sha)
        patchset = {
            "id": "ps-dirty-root-1",
            "base_sha": "7f9c2ba4e88f827d616045507605853ed73b809c",
            "head_sha": head_sha,
            "source_branch": "agent/developer/task-1",
            "commit_count": 2,
            "status": "approved",
            "changed_files": [{"status": "M", "path": "index.html"}],
        }
        self.agent.resolve_task_patchset = mock.AsyncMock(return_value=patchset)
        self.agent.enrich_patchset_snapshot = mock.AsyncMock(return_value=patchset)
        self.agent.ensure_agent_workspace = mock.AsyncMock(
            return_value=(self.repo, self.repo / ".worktrees" / "developer" / "task-1", "agent/developer/task-1")
        )
        self.agent._merge_patchset_squash = mock.AsyncMock(return_value={"status": "merged"})

        async def _fake_git(*args, cwd: Path, task_id=None):
            if args == ("rev-parse", "HEAD"):
                return "d" * 40
            if args == ("status", "--porcelain"):
                return " M README.md"
            raise AssertionError(f"Unexpected git args: {args}")

        self.agent.git = mock.AsyncMock(side_effect=_fake_git)

        await self.agent.process_task(task)

        self.agent._merge_patchset_squash.assert_not_awaited()
        call = self.agent.transition_task.await_args
        self.assertEqual(call.kwargs["fields"]["status"], "blocked")
        self.assertEqual(call.kwargs["handoff"]["stage"], "merge_blocked")
        self.agent.add_alert.assert_awaited()

    async def test_process_task_falls_back_to_commit_merge_when_delivery_model_is_commit(self):
        target_commit = "4c6a0941655523f7dd2aded90e055525d813c1d1"
        task = self._task(target_commit)
        self.agent.resolve_task_patchset = mock.AsyncMock(
            return_value={
                "id": "ps-legacy-1",
                "base_sha": "7f9c2ba4e88f827d616045507605853ed73b809c",
                "head_sha": target_commit,
                "status": "approved",
            }
        )
        self.agent.ensure_agent_workspace = mock.AsyncMock(
            return_value=(self.repo, self.repo / ".worktrees" / "developer" / "task-1", "agent/developer/task-1")
        )
        self.agent.run_cli = mock.AsyncMock(
            return_value=(0, "合并结果：main 新提交为 14a2a1a\n冲突情况：无冲突\n")
        )
        self.agent._is_ancestor = mock.AsyncMock(side_effect=[True, False, False])
        self.agent._is_patch_equivalent_on_ref = mock.AsyncMock(side_effect=[False, True])

        heads = iter(["ec0a922", "14a2a1a"])

        async def _fake_git(*args, cwd: Path, task_id=None):
            if args[:2] == ("cat-file", "-e"):
                return ""
            if args[:2] == ("config", "user.email"):
                return ""
            if args[:2] == ("config", "user.name"):
                return ""
            if args[:2] == ("rev-parse", "--short"):
                return next(heads)
            return ""

        self.agent.git = mock.AsyncMock(side_effect=_fake_git)

        with mock.patch.object(manager_module, "TASK_DELIVERY_MODEL", "commit"):
            await self.agent.process_task(task)

        self.agent.run_cli.assert_awaited_once()
        self.agent.update_patchset.assert_not_awaited()
        call = self.agent.transition_task.await_args
        self.assertEqual(call.kwargs["fields"]["status"], "pending_acceptance")
        self.assertIsNone(call.kwargs["fields"].get("merged_patchset_id"))


if __name__ == "__main__":
    unittest.main()
