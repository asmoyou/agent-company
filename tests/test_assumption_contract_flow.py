import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
AGENTS_DIR = ROOT / "agents"
SERVER_DIR = ROOT / "server"
for path in (AGENTS_DIR, SERVER_DIR):
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))

import leader as leader_module  # noqa: E402
import reviewer as reviewer_module  # noqa: E402
from task_intelligence import extract_task_contract_from_description  # noqa: E402


class AssumptionContractFlowTest(unittest.TestCase):
    def test_leader_refined_description_appends_default_assumptions(self):
        agent = leader_module.LeaderAgent()

        refined = agent._normalize_refined_description(
            (
                "## 任务目标\n- 补全下载命令\n\n"
                "## 范围\n- 兼容现有入口\n\n"
                "## 非范围\n- 不新增后台服务\n\n"
                "## 关键约束\n- 不修改现有鉴权链路\n\n"
                "## 交付物\n- cli/download.py\n\n"
                "## 验收标准\n- [ ] 命令可执行\n- [ ] 不影响现有入口"
            ),
            "",
        )

        self.assertIn("## 假设", refined)
        self.assertIn("## 证据要求", refined)
        self.assertIn("最小可逆方案处理", refined)
        contract = extract_task_contract_from_description(refined)
        self.assertTrue(contract.get("assumptions"))
        self.assertTrue(contract.get("evidence_required"))

    def test_review_contract_treats_assumptions_as_allowed_baseline(self):
        agent = reviewer_module.ReviewerAgent()

        block = agent.build_review_contract_block(
            {
                "current_contract": {
                    "goal": "补全下载能力",
                    "scope": ["保持现有 CLI 主入口"],
                    "non_scope": ["不新增兼容入口"],
                    "constraints": ["不修改鉴权逻辑"],
                    "deliverables": ["cli/download.py", "tests/test_download.py"],
                    "acceptance": ["下载命令可执行", "测试通过"],
                    "assumptions": ["未明确说明的输出命名沿用现有 CLI 规范"],
                    "evidence_required": ["pytest tests/test_download.py"],
                }
            }
        )

        self.assertIn("允许沿用的默认假设", block)
        self.assertIn("不要因为“存在 assumptions”本身打回", block)
        self.assertIn("pytest tests/test_download.py", block)

    def test_leader_simple_decision_strips_parent_requirement_ids_from_description(self):
        agent = leader_module.LeaderAgent()

        decision, issues = agent._normalize_triage_decision(
            {
                "action": "simple",
                "reason": "单个开发者可完成",
                "assignee": "developer",
                "refined_description": (
                    "## 任务目标\n- 基于 R1、R2，交付一个可玩的网页小游戏。\n\n"
                    "## 范围\n- 页面整体视觉需围绕 R2 的甜美可爱要求设计。\n\n"
                    "## 关键约束\n- 必须满足 R1：交付物是网页游戏。\n\n"
                    "## 假设\n- 默认采用单页前端实现。\n\n"
                    "## 交付物\n- index.html\n\n"
                    "## 验收标准\n- [ ] 可以打开游玩\n- [ ] 视觉风格统一"
                ),
            },
            fallback_description="",
        )

        self.assertEqual(issues, [])
        self.assertEqual(decision["action"], "simple")
        self.assertNotIn("R1", decision["refined_description"])
        self.assertNotIn("R2", decision["refined_description"])
        self.assertIn("原始需求", decision["refined_description"])


if __name__ == "__main__":
    unittest.main()
