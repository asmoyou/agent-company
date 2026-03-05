import contextlib
import json
import sys
import unittest
from pathlib import Path
from unittest import mock

ROOT = Path(__file__).resolve().parents[1]
AGENTS_DIR = ROOT / "agents"
if str(AGENTS_DIR) not in sys.path:
    sys.path.insert(0, str(AGENTS_DIR))

import base as base_module  # noqa: E402


class FeedbackContextTest(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.agent = base_module.BaseAgent()

    async def asyncTearDown(self):
        with contextlib.suppress(Exception):
            await self.agent.http.aclose()
        with contextlib.suppress(Exception):
            await self.agent.http_output.aclose()

    def test_unresolved_feedback_section_only_contains_open_items(self):
        task = {
            "status": "needs_changes",
            "review_feedback_history": json.dumps(
                [
                    {
                        "id": "FB0001",
                        "created_at": "2026-03-05T10:00:00",
                        "source": "reviewer",
                        "status_at": "needs_changes",
                        "stage": "review_to_dev",
                        "actor": "reviewer",
                        "feedback": "旧问题：补充单测",
                        "resolved": True,
                        "resolved_at": "2026-03-05T11:00:00",
                        "resolved_reason": "superseded",
                    },
                    {
                        "id": "FB0002",
                        "created_at": "2026-03-05T11:30:00",
                        "source": "user",
                        "status_at": "needs_changes",
                        "stage": "user_to_dev",
                        "actor": "user",
                        "feedback": "新问题：修复接口分页边界",
                        "resolved": False,
                        "resolved_at": "",
                        "resolved_reason": "",
                    },
                ],
                ensure_ascii=False,
            ),
        }

        lines = self.agent._build_unresolved_feedback_lines(task)
        text = "\n".join(lines)
        self.assertIn("本次打回原因", text)
        self.assertIn("只处理下面标记“未解决”的意见", text)
        self.assertIn("FB0002", text)
        self.assertIn("新问题：修复接口分页边界", text)
        self.assertNotIn("旧问题：补充单测", text)

    async def test_build_handoff_context_includes_feedback_timeline_and_handoff_time(self):
        task = {
            "id": "task-1",
            "status": "needs_changes",
            "review_feedback_history": json.dumps(
                [
                    {
                        "id": "FB0001",
                        "created_at": "2026-03-05T10:00:00",
                        "source": "reviewer",
                        "status_at": "needs_changes",
                        "stage": "review_to_dev",
                        "actor": "reviewer",
                        "feedback": "旧问题：补充单测",
                        "resolved": True,
                        "resolved_at": "2026-03-05T11:00:00",
                        "resolved_reason": "superseded",
                    },
                    {
                        "id": "FB0002",
                        "created_at": "2026-03-05T11:30:00",
                        "source": "user",
                        "status_at": "needs_changes",
                        "stage": "user_to_dev",
                        "actor": "user",
                        "feedback": "新问题：修复接口分页边界",
                        "resolved": False,
                        "resolved_at": "",
                        "resolved_reason": "",
                    },
                ],
                ensure_ascii=False,
            ),
        }
        self.agent.get_task = mock.AsyncMock(return_value=task)
        self.agent.get_handoffs = mock.AsyncMock(
            return_value=[
                {
                    "created_at": "2026-03-05T12:00:00",
                    "stage": "review_to_dev",
                    "from_agent": "reviewer",
                    "to_agent": "developer",
                    "summary": "需修复分页边界",
                    "commit_hash": "abc1234",
                    "conclusion": "退回开发",
                }
            ]
        )

        text = await self.agent.build_handoff_context("task-1")
        self.assertIn("## 本次打回原因（仅处理未解决意见）", text)
        self.assertIn("## 反馈时间线（最近记录）", text)
        self.assertIn("已解决:superseded", text)
        self.assertIn("## 历史交接记录（仅供时间线参考，非当前待办）", text)
        self.assertIn("2026-03-05 12:00:00", text)


if __name__ == "__main__":
    unittest.main()
