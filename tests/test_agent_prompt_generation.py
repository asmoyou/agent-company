import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SERVER_DIR = ROOT / "server"
if str(SERVER_DIR) not in sys.path:
    sys.path.insert(0, str(SERVER_DIR))

import app as app_module  # noqa: E402


class AgentPromptGenerationTest(unittest.TestCase):
    def test_meta_prompt_includes_delivery_contract_rules(self):
        text = app_module._build_agent_prompt_generation_meta_prompt("客服 Agent")
        self.assertIn("{task_title}", text)
        self.assertIn("{task_description}", text)
        self.assertIn("{rework_section}", text)
        self.assertIn("必须把结果写入当前工作区文件", text)
        self.assertIn("git add -A", text)
        self.assertIn("git commit -m", text)
        self.assertIn("commit hash", text)
        self.assertIn("不得声称任务已完成或已交付", text)
        self.assertIn("交付物、验收标准、范围或约束", text)


if __name__ == "__main__":
    unittest.main()
