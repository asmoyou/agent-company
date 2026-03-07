import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SERVER_DIR = ROOT / "server"
if str(SERVER_DIR) not in sys.path:
    sys.path.insert(0, str(SERVER_DIR))

import db  # noqa: E402


LEGACY_DEVELOPER_PROMPT = (
    "你是一名专业软件工程师，负责实现以下任务。\n\n"
    "## 任务信息\n\n"
    "**标题**：{task_title}\n\n"
    "**需求描述**：\n"
    "{task_description}\n\n"
    "{rework_section}\n\n"
    "## 工作要求\n\n"
    "1. **所有成果必须写入文件**，不要只在终端打印输出\n"
    "   - 代码任务 → 创建对应语言的源文件（.py / .ts / .go 等）\n"
    "   - 文档/方案任务 → 创建 `.md` 文件，把完整内容写入\n"
    "   - 目标是形成可审查的交付物；若本轮无需新增文件，需在交接中写明依据\n\n"
    "2. **质量标准**\n"
    "   - 代码需有适当注释，边界情况需处理\n"
    "   - 文档需完整、结构清晰\n\n"
    "3. **分支与交接约束**\n"
    "   - 在当前工作分支完成实现并提交，不要自行合并 main\n"
    "   - 提交后由 reviewer/manager 继续流程，不要跳过审查与合并环节\n"
    "   - 不要伪造“已合并/已发布”结论\n\n"
    "4. 直接开始实现，不需要解释计划"
)

LEGACY_REVIEWER_PROMPT = (
    "你是资深代码/文档审查工程师，负责审查以下变更。\n\n"
    "## 任务信息\n\n"
    "**标题**：{task_title}\n\n"
    "**需求描述**：\n"
    "{task_description}\n\n"
    "## 变更内容\n\n"
    "```\n"
    "{diff}\n"
    "```\n\n"
    "## 审查要点\n\n"
    "- 是否完整实现了需求描述中的所有要求\n"
    "- 代码/内容是否正确，有无明显错误或遗漏\n"
    "- 代码质量、可读性、边界情况处理\n"
    "- 文件结构是否合理\n\n"
    "## 输出格式\n\n"
    "审查完毕后，在回复最后一行只输出一个 JSON 对象（不要代码块、不要额外文字）：\n"
    '- decision 只能是 "approve" 或 "request_changes"\n'
    '- decision="approve" 时必须提供 comment 字段\n'
    '- decision="request_changes" 时必须提供 feedback 字段'
)


class BuiltinPromptDefaultsTest(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self._old_db_path = db.DB_PATH
        db.DB_PATH = Path(self._tmp.name) / "tasks-prompts.db"
        db.init_db()

    def tearDown(self):
        db.DB_PATH = self._old_db_path
        self._tmp.cleanup()

    def test_seeded_builtin_prompts_include_contract_language(self):
        developer = db.get_agent_type("developer")
        reviewer = db.get_agent_type("reviewer")
        leader = db.get_agent_type("leader")

        self.assertIn("完成定义（必须自检）", developer["prompt"])
        self.assertIn("任务描述中的“交付物”“验收标准”“关键约束”同样是本轮实现的完成定义", developer["prompt"])
        self.assertIn("assumptions 属于 leader 已吸收的不确定性", developer["prompt"])
        self.assertIn("任务描述中的“交付物”“验收标准”“关键约束”同样是你的独立核查清单", reviewer["prompt"])
        self.assertIn("只有所有验收项都有代码、测试、文档或行为证据时，才能 approve", reviewer["prompt"])
        self.assertIn("不要因为存在 assumptions 本身而打回", reviewer["prompt"])
        self.assertIn("用最小可逆 assumptions 吸收普通细节缺口", leader["prompt"])
        self.assertIn("编号只用于 subtasks.parent_refs", leader["prompt"])
        self.assertIn("simple 任务的 refined_description", leader["prompt"])

    def test_init_db_migrates_legacy_builtin_prompts(self):
        db.update_agent_type("developer", prompt=LEGACY_DEVELOPER_PROMPT)
        db.update_agent_type("reviewer", prompt=LEGACY_REVIEWER_PROMPT)

        db.init_db()

        developer = db.get_agent_type("developer")
        reviewer = db.get_agent_type("reviewer")

        self.assertEqual(developer["prompt"], db.BUILTIN_PROMPTS["developer"])
        self.assertEqual(reviewer["prompt"], db.BUILTIN_PROMPTS["reviewer"])


if __name__ == "__main__":
    unittest.main()
