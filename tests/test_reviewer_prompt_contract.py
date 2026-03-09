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

import reviewer as reviewer_module  # noqa: E402


class ReviewerPromptContractTest(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.root = Path(self._tmp.name) / "repo"
        self.worktree = self.root / ".worktrees" / "developer"
        self.worktree.mkdir(parents=True, exist_ok=True)

        self.agent = reviewer_module.ReviewerAgent()
        self.agent.ensure_agent_workspace = mock.AsyncMock(
            return_value=(self.root, self.worktree, "agent/developer")
        )
        self.agent.stop_if_task_cancelled = mock.AsyncMock(return_value=False)
        self.agent.add_log = mock.AsyncMock()
        self.agent.add_alert = mock.AsyncMock()
        self.agent.build_handoff_context = mock.AsyncMock(return_value="")
        self.agent.resolve_task_patchset = mock.AsyncMock(return_value=None)
        self.agent.get_diff_for_commit = mock.AsyncMock(return_value="diff --git a/a b/a\n+hello\n")
        self.agent.transition_task = mock.AsyncMock(
            return_value={"task": {"id": "task-1", "status": "approved"}}
        )

    async def asyncTearDown(self):
        with contextlib.suppress(Exception):
            await self.agent.http.aclose()
        with contextlib.suppress(Exception):
            await self.agent.http_output.aclose()
        self._tmp.cleanup()

    async def test_prompt_includes_independent_review_contract(self):
        captured = {}

        async def _capture_run_cli(prompt, cwd, **kwargs):
            captured["prompt"] = prompt
            captured["kwargs"] = kwargs
            return 0, '{"decision":"approve","comment":"ok"}'

        self.agent.run_cli = mock.AsyncMock(side_effect=_capture_run_cli)
        self.agent._load_decision_file = mock.Mock(return_value={"decision": "approve", "comment": "ok"})

        task = {
            "id": "task-1",
            "title": "补全接口测试覆盖",
            "description": (
                "## 任务目标\n- 补全 claim 接口测试\n\n"
                "## 范围\n- /tasks/claim 鉴权与异常分支\n\n"
                "## 假设\n- 未明确说明的异常文案沿用现有接口风格\n\n"
                "## 交付物\n- tests/test_task_actions_api.py 中的新增测试\n\n"
                "## 验收标准\n- [ ] 新增测试全部通过\n- [ ] 不影响现有测试\n\n"
                "## 关键约束\n- 不修改生产逻辑"
            ),
            "status": "in_review",
            "commit_hash": "a" * 40,
            "assigned_agent": "developer",
            "dev_agent": "developer",
        }

        await self.agent.process_task(task)

        prompt = captured["prompt"]
        self.assertIn("## 独立验收基线（必须据此审查）", prompt)
        self.assertIn("必须逐项核验的验收标准", prompt)
        self.assertIn("新增测试全部通过", prompt)
        self.assertIn("不修改生产逻辑", prompt)
        self.assertIn("只要任一验收项缺少证据", prompt)
        self.assertIn("允许沿用的默认假设", prompt)
        self.assertIn("不要因为“存在 assumptions”本身打回", prompt)
        self.assertEqual(captured["kwargs"]["reasoning_effort"], "high")

    async def test_prompt_allows_cli_equivalent_evidence_for_document_conversion_tasks(self):
        captured = {}

        async def _capture_run_cli(prompt, cwd, **kwargs):
            captured["prompt"] = prompt
            return 0, '{"decision":"approve","comment":"ok"}'

        self.agent.run_cli = mock.AsyncMock(side_effect=_capture_run_cli)
        self.agent._load_decision_file = mock.Mock(return_value={"decision": "approve", "comment": "ok"})

        task = {
            "id": "task-doc-review",
            "title": "合同模板转 Word",
            "description": "",
            "status": "reviewing",
            "commit_hash": "a" * 40,
            "assigned_agent": "admin_specialist",
            "dev_agent": "admin_specialist",
            "current_contract": {
                "version": 2,
                "goal": "将合同模板转为可编辑 Word 文件",
                "deliverables": ["外包劳务派遣合同模板.docx", "合同模板转Word处理说明.md"],
                "acceptance": ["生成的 Word 文件可继续编辑", "处理说明记录验证过程"],
                "evidence_required": ["验证生成的 Word 文件能够被本地继续读取或转换"],
            },
        }

        await self.agent.process_task(task)

        prompt = captured["prompt"]
        self.assertIn("CLI/headless 环境下的等价本地证据", prompt)
        self.assertIn("不要仅因缺少 GUI 打开过程而打回", prompt)

    async def test_prompt_prefers_scriptable_evidence_for_interactive_tasks(self):
        captured = {}

        async def _capture_run_cli(prompt, cwd, **kwargs):
            captured["prompt"] = prompt
            return 0, '{"decision":"approve","comment":"ok"}'

        self.agent.run_cli = mock.AsyncMock(side_effect=_capture_run_cli)
        self.agent._load_decision_file = mock.Mock(return_value={"decision": "approve", "comment": "ok"})

        task = {
            "id": "task-ui-review",
            "title": "按钮交互页面",
            "description": "",
            "status": "reviewing",
            "commit_hash": "a" * 40,
            "assigned_agent": "developer",
            "dev_agent": "developer",
            "current_contract": {
                "version": 2,
                "goal": "实现一个网页按钮交互流程",
                "scope": ["页面包含按钮、成功反馈和失败提示"],
                "deliverables": ["index.html", "script.js"],
                "acceptance": ["点击按钮后页面反馈正确"],
                "evidence_required": ["覆盖开始、关键交互和失败恢复路径的本地验证"],
            },
        }

        await self.agent.process_task(task)

        prompt = captured["prompt"]
        self.assertIn("不要默认以缺少这些人工操作为由打回", prompt)
        self.assertIn("可接受可脚本化的冒烟脚本、自动化测试、截图或断言结果", prompt)
        self.assertIn("不要默认要求人工逐步点击界面", prompt)

    async def test_lightweight_static_review_uses_medium_reasoning_effort(self):
        captured = {}

        async def _capture_run_cli(prompt, cwd, **kwargs):
            captured["kwargs"] = kwargs
            return 0, '{"decision":"approve","comment":"ok"}'

        self.agent.run_cli = mock.AsyncMock(side_effect=_capture_run_cli)
        self.agent._load_decision_file = mock.Mock(return_value={"decision": "approve", "comment": "ok"})

        task = {
            "id": "task-static-review",
            "title": "福州旅游攻略",
            "description": "",
            "status": "reviewing",
            "commit_hash": "a" * 40,
            "assigned_agent": "developer",
            "dev_agent": "developer",
            "current_contract": {
                "version": 2,
                "goal": "输出静态旅游攻略网页",
                "scope": ["使用静态网页展示内容"],
                "deliverables": ["index.html", "styles.css", "script.js"],
                "acceptance": ["页面可在桌面端与移动端浏览"],
                "evidence_required": ["node --check script.js"],
                "allowed_surface": {
                    "roots": ["index.html", "styles.css", "script.js"],
                    "files": ["index.html", "styles.css", "script.js"],
                    "docs": [],
                    "cli_paths": ["index.html", "styles.css", "script.js"],
                },
            },
            "allowed_surface": {
                "roots": ["index.html", "styles.css", "script.js"],
                "files": ["index.html", "styles.css", "script.js"],
                "docs": [],
                "cli_paths": ["index.html", "styles.css", "script.js"],
            },
        }

        await self.agent.process_task(task)

        self.assertEqual(captured["kwargs"]["reasoning_effort"], "medium")

    async def test_process_task_rejects_commit_that_is_not_independently_mergeable(self):
        target_commit = "4c6a0941655523f7dd2aded90e055525d813c1d1"
        parent_commit = "7f9c2ba4e88f827d616045507605853ed73b809c"

        async def _fake_git(*args, cwd: Path, task_id=None):
            if args == ("rev-parse", f"{target_commit}^"):
                return parent_commit
            raise AssertionError(f"Unexpected git args: {args}")

        self.agent.git = mock.AsyncMock(side_effect=_fake_git)
        self.agent._is_ancestor = mock.AsyncMock(return_value=False)
        self.agent._is_patch_equivalent_on_ref = mock.AsyncMock(return_value=False)
        self.agent.run_cli = mock.AsyncMock(return_value=(0, '{"decision":"approve","comment":"ok"}'))

        task = {
            "id": "task-2",
            "title": "补全接口测试覆盖",
            "description": "",
            "status": "in_review",
            "commit_hash": target_commit,
            "assigned_agent": "developer",
            "dev_agent": "developer",
        }

        await self.agent.process_task(task)

        self.agent.run_cli.assert_not_awaited()
        self.agent.transition_task.assert_awaited_once()
        call = self.agent.transition_task.await_args
        self.assertEqual(call.kwargs["fields"]["status"], "needs_changes")
        self.assertEqual(call.kwargs["handoff"]["stage"], "review_to_dev")
        self.assertIn("提交基线不一致", call.kwargs["handoff"]["summary"])

    async def test_process_task_rejects_patchset_with_dirty_worktree_before_llm_review(self):
        patchset = {
            "id": "ps-review-1",
            "base_sha": "a" * 40,
            "head_sha": "b" * 40,
            "source_branch": "agent/developer/task-1",
            "commit_count": 2,
            "commit_list": [],
            "diff_stat": " index.html | 2 +-",
            "status": "submitted",
            "worktree_clean": False,
        }
        self.agent.resolve_task_patchset = mock.AsyncMock(return_value=patchset)

        async def _fake_git(*args, cwd: Path, task_id=None):
            if args == ("status", "--porcelain"):
                return " M index.html"
            raise AssertionError(f"Unexpected git args: {args}")

        self.agent.git = mock.AsyncMock(side_effect=_fake_git)
        self.agent.run_cli = mock.AsyncMock(return_value=(0, '{"decision":"approve","comment":"ok"}'))

        task = {
            "id": "task-3",
            "title": "补全接口测试覆盖",
            "description": "",
            "status": "in_review",
            "commit_hash": patchset["head_sha"],
            "assigned_agent": "developer",
            "dev_agent": "developer",
            "current_patchset_id": patchset["id"],
            "current_patchset_status": "submitted",
        }

        await self.agent.process_task(task)

        self.agent.run_cli.assert_not_awaited()
        self.agent.transition_task.assert_awaited_once()
        call = self.agent.transition_task.await_args
        self.assertEqual(call.kwargs["fields"]["status"], "needs_changes")
        self.assertEqual(call.kwargs["fields"]["current_patchset_status"], "rejected")
        self.assertEqual(call.kwargs["handoff"]["payload"]["patchset"]["status"], "rejected")
        self.assertEqual(call.kwargs["handoff"]["payload"]["reason"], "dirty_worktree")

    async def test_process_task_queues_patchset_metadata_on_approve(self):
        patchset = {
            "id": "ps-review-approve-1",
            "base_sha": "a" * 40,
            "head_sha": "b" * 40,
            "source_branch": "agent/developer/task-2",
            "commit_count": 2,
            "commit_list": [],
            "diff_stat": " index.html | 2 +-",
            "status": "submitted",
            "worktree_clean": True,
        }
        self.agent.resolve_task_patchset = mock.AsyncMock(return_value=patchset)
        self.agent.enrich_patchset_snapshot = mock.AsyncMock(
            return_value={
                **patchset,
                "changed_files": [{"status": "M", "path": "index.html"}],
                "artifact_manifest": {
                    "path": ".opc/delivery.json",
                    "keys": ["deliverables"],
                },
            }
        )
        self.agent.get_diff_for_patchset = mock.AsyncMock(return_value="diff --git a/a b/a\n+hello\n")
        self.agent.run_cli = mock.AsyncMock(return_value=(0, '{"decision":"approve","comment":"ok"}'))
        self.agent._load_decision_file = mock.Mock(return_value={"decision": "approve", "comment": "ok"})

        async def _fake_git(*args, cwd: Path, task_id=None):
            if args == ("status", "--porcelain"):
                return ""
            if args == ("rev-parse", "main"):
                return "c" * 40
            raise AssertionError(f"Unexpected git args: {args}")

        self.agent.git = mock.AsyncMock(side_effect=_fake_git)

        task = {
            "id": "task-4",
            "title": "补全接口测试覆盖",
            "description": "",
            "status": "reviewing",
            "commit_hash": patchset["head_sha"],
            "assigned_agent": "developer",
            "dev_agent": "developer",
            "current_patchset_id": patchset["id"],
            "current_patchset_status": "submitted",
        }

        await self.agent.process_task(task)

        call = self.agent.transition_task.await_args
        self.assertEqual(call.kwargs["fields"]["status"], "approved")
        self.assertEqual(call.kwargs["handoff"]["status_from"], "reviewing")
        self.assertEqual(call.kwargs["handoff"]["payload"]["patchset"]["queue_status"], "queued")
        self.assertEqual(call.kwargs["handoff"]["payload"]["patchset"]["reviewed_main_sha"], "c" * 40)
        self.assertEqual(call.kwargs["handoff"]["payload"]["patchset"]["changed_files"][0]["path"], "index.html")
        self.assertEqual(call.kwargs["handoff"]["payload"]["patchset"]["artifact_manifest"]["path"], ".opc/delivery.json")

    async def test_process_task_blocks_approve_when_machine_evidence_still_has_blockers(self):
        patchset = {
            "id": "ps-review-blocked-1",
            "base_sha": "a" * 40,
            "head_sha": "b" * 40,
            "source_branch": "agent/developer/task-blocked",
            "commit_count": 1,
            "commit_list": [],
            "diff_stat": " smoke-test.js | 2 +-",
            "status": "submitted",
            "worktree_clean": True,
        }
        self.agent.resolve_task_patchset = mock.AsyncMock(return_value=patchset)
        self.agent.enrich_patchset_snapshot = mock.AsyncMock(return_value=patchset)
        self.agent.get_diff_for_patchset = mock.AsyncMock(return_value="diff --git a/a b/a\n+hello\n")
        self.agent.run_cli = mock.AsyncMock(return_value=(0, '{"decision":"approve","comment":"ok"}'))
        self.agent._load_decision_file = mock.Mock(return_value={"decision": "approve", "comment": "ok"})

        async def _fake_git(*args, cwd: Path, task_id=None):
            if args == ("status", "--porcelain"):
                return ""
            if args == ("rev-parse", "main"):
                return "c" * 40
            raise AssertionError(f"Unexpected git args: {args}")

        self.agent.git = mock.AsyncMock(side_effect=_fake_git)

        task = {
            "id": "task-guard-1",
            "title": "补全交互验证",
            "description": "",
            "status": "reviewing",
            "commit_hash": patchset["head_sha"],
            "assigned_agent": "developer",
            "dev_agent": "developer",
            "current_patchset_id": patchset["id"],
            "current_patchset_status": "submitted",
            "latest_evidence": {
                "summary": "预检仍缺证据",
                "bundle": {
                    "hard_blockers": [
                        {
                            "issue_id": "evidence-required-1",
                            "summary": "预检未发现与证据要求对应的验证资产：node smoke-test.js",
                            "category": "evidence",
                        }
                    ],
                    "missing_evidence_required": [{"item": "node smoke-test.js", "status": "missing"}],
                    "missing_acceptance_checks": [],
                    "assumption_conflicts": [],
                    "surface_violations": [],
                },
            },
        }

        await self.agent.process_task(task)

        call = self.agent.transition_task.await_args
        self.assertEqual(call.kwargs["fields"]["status"], "needs_changes")
        self.assertEqual(call.kwargs["handoff"]["stage"], "review_to_dev")
        self.assertEqual(call.kwargs["handoff"]["status_from"], "reviewing")
        self.assertIn("机器校验阻止通过", call.kwargs["fields"]["review_feedback"])

    async def test_process_task_falls_back_to_commit_review_when_delivery_model_is_commit(self):
        patchset = {
            "id": "ps-review-commit-1",
            "base_sha": "a" * 40,
            "head_sha": "b" * 40,
            "source_branch": "agent/developer/task-commit",
            "commit_count": 2,
            "status": "submitted",
            "worktree_clean": True,
        }
        self.agent.resolve_task_patchset = mock.AsyncMock(return_value=patchset)
        self.agent.get_diff_for_commit = mock.AsyncMock(return_value="diff --git a/a b/a\n+legacy\n")
        self.agent.get_diff_for_patchset = mock.AsyncMock(return_value="should not use")
        self.agent.run_cli = mock.AsyncMock(return_value=(0, '{"decision":"approve","comment":"ok"}'))
        self.agent._load_decision_file = mock.Mock(return_value={"decision": "approve", "comment": "ok"})
        self.agent._is_ancestor = mock.AsyncMock(return_value=True)
        self.agent._is_patch_equivalent_on_ref = mock.AsyncMock(return_value=False)

        async def _fake_git(*args, cwd: Path, task_id=None):
            if args == ("rev-parse", f"{patchset['head_sha']}^"):
                return "c" * 40
            raise AssertionError(f"Unexpected git args: {args}")

        self.agent.git = mock.AsyncMock(side_effect=_fake_git)

        task = {
            "id": "task-5",
            "title": "补全接口测试覆盖",
            "description": "",
            "status": "in_review",
            "commit_hash": patchset["head_sha"],
            "assigned_agent": "developer",
            "dev_agent": "developer",
            "current_patchset_id": patchset["id"],
            "current_patchset_status": "submitted",
        }

        with mock.patch.object(reviewer_module, "TASK_DELIVERY_MODEL", "commit"):
            await self.agent.process_task(task)

        self.agent.get_diff_for_commit.assert_awaited_once()
        self.agent.get_diff_for_patchset.assert_not_awaited()
        call = self.agent.transition_task.await_args
        self.assertEqual(call.kwargs["fields"]["status"], "approved")
        self.assertIsNone(call.kwargs["fields"].get("current_patchset_status"))
        self.assertEqual(call.kwargs["handoff"]["payload"]["patchset"], {})


if __name__ == "__main__":
    unittest.main()
