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
            return 0, '{"decision":"approve","comment":"ok"}'

        self.agent.run_cli = mock.AsyncMock(side_effect=_capture_run_cli)
        self.agent._load_decision_file = mock.Mock(return_value={"decision": "approve", "comment": "ok"})

        task = {
            "id": "task-1",
            "title": "补全接口测试覆盖",
            "description": (
                "## 任务目标\n- 补全 claim 接口测试\n\n"
                "## 范围\n- /tasks/claim 鉴权与异常分支\n\n"
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


if __name__ == "__main__":
    unittest.main()
