import contextlib
import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

ROOT = Path(__file__).resolve().parents[1]
AGENTS_DIR = ROOT / "agents"
if str(AGENTS_DIR) not in sys.path:
    sys.path.insert(0, str(AGENTS_DIR))

import leader as leader_module  # noqa: E402


class LeaderRequirementsRefinementTest(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.run_dir = Path(self._tmp.name)
        self.agent = leader_module.LeaderAgent()
        self.agent.add_log = mock.AsyncMock()
        self.agent.add_alert = mock.AsyncMock()
        self.agent.build_handoff_context = mock.AsyncMock(return_value="")
        self.agent._get_agent_list = mock.AsyncMock(return_value="- developer: 开发者")
        self.agent.stop_if_task_cancelled = mock.AsyncMock(return_value=False)

    async def asyncTearDown(self):
        with contextlib.suppress(Exception):
            await self.agent.http.aclose()
        with contextlib.suppress(Exception):
            await self.agent.http_output.aclose()
        self._tmp.cleanup()

    def _task(self, *, status: str = "triaging", claimed_from: str = "triage") -> dict:
        return {
            "id": "task-1",
            "title": "补全接口测试覆盖",
            "description": "把接口测试补一下",
            "status": status,
            "_claimed_from_status": claimed_from,
            "assignee": "leader",
            "project_path": str(self.run_dir),
            "project_id": "project-1",
        }

    def _resp(self, status_code: int, payload):
        r = mock.Mock()
        r.status_code = status_code
        r.json.return_value = payload
        return r

    async def test_auto_triage_runs_single_cli_and_updates_description(self):
        task = self._task(status="triaging", claimed_from="triage")
        refined = (
            "## 任务目标\n- 完成关键接口的鉴权与状态流转覆盖\n\n"
            "## 范围\n- /tasks/claim\n- /tasks/{id}/transition\n\n"
            "## 非范围\n- 不修改生产逻辑\n\n"
            "## 关键约束\n- 仅补充测试，不引入兜底机制\n\n"
            "## 验收标准\n- [ ] 新增测试全部通过\n- [ ] 不影响现有测试"
        )
        self.agent.http.get = mock.AsyncMock(return_value=self._resp(200, []))

        async def _fake_run_cli(prompt, cwd, **kwargs):
            out = Path(cwd) / ".opc" / "decisions" / f"{task['id']}.leader-triage.json"
            out.parent.mkdir(parents=True, exist_ok=True)
            out.write_text(
                json.dumps(
                    {
                        "refined_description": refined,
                        "action": "simple",
                        "reason": "单 agent 可直接完成",
                    },
                    ensure_ascii=False,
                ),
                encoding="utf-8",
            )
            return 0, "ok"

        self.agent.run_cli = mock.AsyncMock(side_effect=_fake_run_cli)
        self.agent.transition_task = mock.AsyncMock(
            return_value={"task": {**task, "description": refined, "status": "todo"}}
        )
        self.agent._create_subtasks = mock.AsyncMock(return_value=0)

        await self.agent._auto_triage(task)

        self.assertEqual(self.agent.run_cli.await_count, 1)
        self.agent._create_subtasks.assert_not_awaited()
        call = self.agent.transition_task.await_args
        self.assertEqual(call.args[0], task["id"])
        self.assertEqual(call.kwargs["fields"]["description"], refined)
        self.assertEqual(call.kwargs["fields"]["status"], "todo")

    async def test_force_decompose_runs_single_cli_and_updates_description(self):
        task = self._task(status="decompose", claimed_from="decompose")
        refined = (
            "## 任务目标\n- 拆分任务并形成可独立交付的子任务清单\n\n"
            "## 范围\n- 接口测试补全\n\n"
            "## 非范围\n- 生产代码重构\n\n"
            "## 关键约束\n- 子任务必须可验收\n\n"
            "## 验收标准\n- [ ] 至少 2 个高质量子任务"
        )
        self.agent.http.get = mock.AsyncMock(return_value=self._resp(200, []))

        subtasks = [
            {
                "title": "补全 claim/lease 鉴权测试",
                "objective": "补全 claim 与 lease 接口的鉴权分支与异常分支，确保失败路径可回归。",
                "parent_refs": ["R1"],
                "implementation_scope": ["tests/test_task_actions_api.py"],
                "todo_steps": ["补全缺失用例", "执行并修复失败测试"],
                "deliverables": ["新增/更新 API 测试用例"],
                "acceptance_criteria": ["所有新用例通过", "不影响现有测试"],
                "agent": "developer",
            },
            {
                "title": "补全 agent status/output 鉴权测试",
                "objective": "覆盖 agent status/output 的缺失鉴权场景并验证 token 校验。",
                "parent_refs": ["R1"],
                "implementation_scope": ["tests/test_task_actions_api.py"],
                "todo_steps": ["新增缺失鉴权测试", "全量回归测试"],
                "deliverables": ["status/output 鉴权测试用例"],
                "acceptance_criteria": ["缺失/错误 token 返回 401", "测试稳定通过"],
                "agent": "developer",
            },
        ]

        async def _fake_run_cli(prompt, cwd, **kwargs):
            out = Path(cwd) / ".opc" / "decisions" / f"{task['id']}.leader-force-decompose.json"
            out.parent.mkdir(parents=True, exist_ok=True)
            out.write_text(
                json.dumps(
                    {"refined_description": refined, "subtasks": subtasks},
                    ensure_ascii=False,
                ),
                encoding="utf-8",
            )
            return 0, "ok"

        self.agent.run_cli = mock.AsyncMock(side_effect=_fake_run_cli)
        self.agent._create_subtasks = mock.AsyncMock(return_value=2)
        self.agent.transition_task = mock.AsyncMock(
            return_value={"task": {**task, "description": refined, "status": "decomposed"}}
        )

        await self.agent._force_decompose(task)

        self.assertEqual(self.agent.run_cli.await_count, 1)
        self.agent._create_subtasks.assert_awaited_once()
        call = self.agent.transition_task.await_args
        self.assertEqual(call.args[0], task["id"])
        self.assertEqual(call.kwargs["fields"]["description"], refined)
        self.assertEqual(call.kwargs["fields"]["status"], "decomposed")


if __name__ == "__main__":
    unittest.main()
