import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SERVER_DIR = ROOT / "server"
if str(SERVER_DIR) not in sys.path:
    sys.path.insert(0, str(SERVER_DIR))

import task_intelligence  # noqa: E402


class ReasoningEffortSelectorTest(unittest.TestCase):
    def test_leader_simple_triage_prefers_medium(self):
        task = {
            "title": "福州旅游攻略",
            "description": "写一篇福州的旅游攻略，5天4夜游的，形成美观的网页介绍。",
            "status": "triaging",
        }

        effort = task_intelligence.select_reasoning_effort(
            task,
            agent="leader",
            operation="triage",
        )

        self.assertEqual(effort, "medium")

    def test_leader_force_decompose_prefers_high(self):
        task = {
            "title": "补全接口测试覆盖",
            "description": "把接口测试补一下",
            "status": "decompose",
        }

        effort = task_intelligence.select_reasoning_effort(
            task,
            agent="leader",
            operation="decompose",
        )

        self.assertEqual(effort, "high")

    def test_developer_retry_with_state_machine_risk_escalates_to_xhigh(self):
        task = {
            "title": "修复 claim 状态流转",
            "description": (
                "## 任务目标\n- 修复 claim 接口的鉴权与状态机回归\n\n"
                "## 范围\n- /tasks/claim\n- lease token 校验\n\n"
                "## 交付物\n- server/app.py\n- tests/test_task_actions_api.py\n"
            ),
            "status": "needs_changes",
            "_claimed_from_status": "needs_changes",
            "retry_strategy": "critic_pass",
            "same_fingerprint_streak": 2,
            "open_issue_count": 3,
        }

        effort = task_intelligence.select_reasoning_effort(
            task,
            agent="developer",
            operation="implement",
        )

        self.assertEqual(effort, "xhigh")

    def test_reviewer_lightweight_static_contract_prefers_medium(self):
        task = {
            "title": "福州旅游攻略",
            "description": "",
            "status": "reviewing",
            "current_contract": {
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

        effort = task_intelligence.select_reasoning_effort(
            task,
            agent="reviewer",
            operation="review",
        )

        self.assertEqual(effort, "medium")

    def test_manager_merge_defaults_to_high(self):
        task = {
            "title": "合并菜谱页面",
            "description": "",
            "status": "merging",
            "commit_hash": "a" * 40,
        }

        effort = task_intelligence.select_reasoning_effort(
            task,
            agent="manager",
            operation="merge",
        )

        self.assertEqual(effort, "high")


if __name__ == "__main__":
    unittest.main()
