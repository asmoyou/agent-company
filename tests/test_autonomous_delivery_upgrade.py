import contextlib
import sys
import tempfile
import unittest
from datetime import datetime, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SERVER_DIR = ROOT / "server"
AGENTS_DIR = ROOT / "agents"
if str(SERVER_DIR) not in sys.path:
    sys.path.insert(0, str(SERVER_DIR))
if str(AGENTS_DIR) not in sys.path:
    sys.path.insert(0, str(AGENTS_DIR))

import db  # noqa: E402
import generic as generic_module  # noqa: E402
import task_intelligence  # noqa: E402


class AutonomousDeliveryDbTest(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self._old_db_path = db.DB_PATH
        db.DB_PATH = Path(self._tmp.name) / "tasks-upgrade.db"
        db.init_db()
        self.project = db.create_project("upgrade", self._tmp.name)

    def tearDown(self):
        db.DB_PATH = self._old_db_path
        self._tmp.cleanup()

    def _create_task(self, *, status: str = "todo", description: str = "") -> dict:
        return db.create_task(
            title="upgrade task",
            description=description,
            project_id=self.project["id"],
            status=status,
            assigned_agent="developer" if status == "todo" else None,
            dev_agent="developer",
        )

    def test_create_task_compiles_and_enriches_contract(self):
        task = self._create_task(
            description=(
                "## 任务目标\n- 补全 claim 接口测试\n\n"
                "## 交付物\n- tests/test_task_actions_api.py\n\n"
                "## 验收标准\n- [ ] 新增测试全部通过\n- [ ] 不影响现有测试\n"
            )
        )

        refreshed = db.get_task(task["id"])
        self.assertTrue(refreshed["current_contract_id"])
        self.assertEqual(refreshed["open_issue_count"], 0)
        self.assertEqual(refreshed["current_contract"]["goal"], "补全 claim 接口测试")
        self.assertIn("tests/test_task_actions_api.py", refreshed["current_contract"]["deliverables"])
        self.assertIn("新增测试全部通过", refreshed["current_contract"]["acceptance"])

    def test_transition_persists_attempt_issue_and_evidence(self):
        task = self._create_task(status="in_review")
        result = db.transition_task(
            task["id"],
            fields={
                "status": "needs_changes",
                "assigned_agent": "developer",
                "dev_agent": "developer",
                "review_feedback": "请补充真实下载路径测试",
                "feedback_source": "reviewer",
                "feedback_stage": "review_to_dev",
                "feedback_actor": "reviewer",
            },
            handoff={
                "stage": "review_to_dev",
                "from_agent": "reviewer",
                "to_agent": "developer",
                "status_from": "in_review",
                "status_to": "needs_changes",
                "title": "审查退回开发",
                "summary": "请补充真实下载路径测试",
                "payload": {
                    "issues": [
                        {
                            "issue_id": "coverage-1",
                            "acceptance_item": "真实下载路径测试",
                            "severity": "high",
                            "category": "coverage",
                            "summary": "未覆盖真实下载路径",
                            "status": "new",
                        }
                    ],
                    "evidence_bundle": {
                        "acceptance_checks": [{"item": "真实下载路径测试", "status": "missing"}],
                    },
                    "evidence_summary": "证据包显示真实下载路径测试缺失",
                    "attempt": {
                        "stage": "review_to_dev",
                        "outcome": "request_changes",
                        "execution_phase": "critic",
                        "retry_strategy": "test_first",
                        "failure_fingerprint": "fp-001",
                        "same_fingerprint_streak": 1,
                        "summary": "补齐测试后重提",
                    },
                },
            },
        )

        self.assertIsNotNone(result["attempt"])
        self.assertIsNotNone(result["evidence"])
        refreshed = db.get_task(task["id"])
        self.assertEqual(refreshed["open_issue_count"], 1)
        self.assertEqual(refreshed["retry_strategy"], "test_first")
        self.assertEqual(refreshed["latest_attempt"]["failure_fingerprint"], "fp-001")
        self.assertEqual(refreshed["latest_evidence"]["summary"], "证据包显示真实下载路径测试缺失")
        issues = db.list_task_issues(task["id"], include_resolved=False)
        self.assertEqual(len(issues), 1)
        self.assertEqual(issues[0]["summary"], "未覆盖真实下载路径")

    def test_patchset_round_trip_deserializes_structured_fields(self):
        task = self._create_task(status="todo")
        saved = db.save_task_patchset(
            task["id"],
            {
                "id": "ps-roundtrip-1",
                "base_sha": "a" * 40,
                "head_sha": "b" * 40,
                "commit_count": 1,
                "commit_list": [{"hash": "b" * 40, "short": "bbbbbbb", "subject": "feat: roundtrip"}],
                "changed_files": [{"status": "M", "path": "smoke-test.js"}],
                "artifact_manifest": {"path": ".opc/delivery.json", "keys": ["deliverables"]},
                "status": "draft",
                "worktree_clean": False,
            },
        )

        self.assertIsNotNone(saved)
        fetched = db.get_task_patchset("ps-roundtrip-1")
        self.assertEqual(fetched["changed_files"][0]["path"], "smoke-test.js")
        self.assertEqual(fetched["artifact_manifest"]["path"], ".opc/delivery.json")
        listed = db.list_task_patchsets(task["id"])
        self.assertEqual(listed[0]["commit_list"][0]["subject"], "feat: roundtrip")

    def test_claim_task_skips_cooldown(self):
        task = self._create_task(status="todo")
        db.update_task(
            task["id"],
            cooldown_until=(datetime.utcnow() + timedelta(minutes=5)).isoformat(),
        )
        claimed = db.claim_task(
            status="todo",
            working_status="in_progress",
            agent="developer",
            agent_key="developer",
            respect_assignment=True,
            project_id=self.project["id"],
        )
        self.assertIsNone(claimed)

        db.update_task(
            task["id"],
            cooldown_until=(datetime.utcnow() - timedelta(minutes=1)).isoformat(),
        )
        claimed = db.claim_task(
            status="todo",
            working_status="in_progress",
            agent="developer",
            agent_key="developer",
            respect_assignment=True,
            project_id=self.project["id"],
        )
        self.assertIsNotNone(claimed)
        self.assertEqual(claimed["id"], task["id"])


class DeveloperPreReviewVerifierTest(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.agent = generic_module.GenericAgent(
            {
                "key": "developer",
                "name": "开发者",
                "prompt": "do work: {task_title}",
                "poll_statuses": "[\"todo\",\"needs_changes\"]",
                "next_status": "in_review",
                "working_status": "in_progress",
                "cli": "codex",
                "runtime_profile": "developer",
            }
        )

    async def asyncTearDown(self):
        with contextlib.suppress(Exception):
            await self.agent.http.aclose()
        with contextlib.suppress(Exception):
            await self.agent.http_output.aclose()

    def test_pre_review_verifier_detects_missing_test_evidence(self):
        task = {
            "id": "task-1",
            "title": "补全测试",
            "description": (
                "## 任务目标\n- 补齐下载路径测试\n\n"
                "## 证据要求\n- pytest tests/test_download.py\n\n"
                "## 交付物\n- tests/test_download.py\n\n"
                "## 验收标准\n- [ ] 新增真实下载路径测试\n"
            ),
            "status": "needs_changes",
            "_claimed_from_status": "needs_changes",
            "current_contract": {
                "version": 1,
                "goal": "补齐下载路径测试",
                "deliverables": ["tests/test_download.py"],
                "acceptance": ["新增真实下载路径测试"],
                "evidence_required": ["pytest tests/test_download.py"],
                "allowed_surface": {"roots": ["tests"], "files": ["tests/test_download.py"]},
            },
            "allowed_surface": {"roots": ["tests"], "files": ["tests/test_download.py"]},
        }
        patchset = {
            "changed_files": [{"status": "M", "path": "src/downloader.py"}],
        }

        result = self.agent._build_pre_review_evidence_bundle(task, patchset)

        self.assertTrue(result["has_blockers"])
        self.assertTrue(result["bundle"]["missing_acceptance_checks"])
        categories = {item["category"] for item in result["issues"]}
        self.assertIn("coverage", categories)
        self.assertTrue(result["bundle"]["missing_evidence_required"])
        self.assertIn("evidence", categories)

    def test_pre_review_verifier_detects_assumption_conflict(self):
        task = {
            "id": "task-2",
            "title": "单页前端实现",
            "description": (
                "## 任务目标\n- 交付单页前端页面\n\n"
                "## 假设\n- 默认采用单页前端实现，不引入后端服务。\n\n"
                "## 交付物\n- index.html\n\n"
                "## 验收标准\n- [ ] 页面可打开\n"
            ),
            "status": "needs_changes",
            "_claimed_from_status": "needs_changes",
            "current_contract": {
                "version": 1,
                "goal": "交付单页前端页面",
                "deliverables": ["index.html"],
                "acceptance": ["页面可打开"],
                "assumptions": ["默认采用单页前端实现，不引入后端服务。"],
                "allowed_surface": {"roots": ["index.html"], "files": ["index.html"]},
            },
            "allowed_surface": {"roots": ["index.html"], "files": ["index.html"]},
        }
        patchset = {
            "changed_files": [{"status": "A", "path": "backend/server.py"}],
        }

        result = self.agent._build_pre_review_evidence_bundle(task, patchset)

        self.assertTrue(result["has_blockers"])
        self.assertTrue(result["bundle"]["assumption_conflicts"])
        summaries = {item["summary"] for item in result["issues"]}
        self.assertTrue(any("假设" in summary for summary in summaries))

    def test_pre_review_verifier_accepts_behavioral_smoke_test_evidence(self):
        task = {
            "id": "task-3",
            "title": "丰富贪吃蛇食物种类",
            "description": (
                "## 任务目标\n- 丰富贪吃蛇食物种类\n\n"
                "## 证据要求\n"
                "- 本地执行 `node smoke-test.js`，结果需覆盖特殊食物与重开流程。\n"
                "- 若特殊食物生成包含随机性，测试中需使用可控输入、桩数据或固定序列，避免只能依赖随机命中证明功能有效。\n\n"
                "## 交付物\n"
                "- `script.js`：多种食物与特殊效果逻辑。\n"
                "- `index.html`：页面状态提示。\n"
                "- `smoke-test.js`：本地可执行冒烟脚本。\n\n"
                "## 验收标准\n"
                "- [ ] 吃到至少一种特殊食物时，会触发明确的额外效果，且效果结果能被玩家观察到或被测试断言验证。\n"
                "- [ ] 执行 `node smoke-test.js` 通过，且手动完成开始、关键交互和失败恢复三段冒烟验证。\n"
            ),
            "status": "needs_changes",
            "_claimed_from_status": "needs_changes",
            "current_contract": {
                "version": 2,
                "goal": "丰富贪吃蛇食物种类",
                "deliverables": ["script.js", "index.html", "smoke-test.js"],
                "acceptance": [
                    "吃到至少一种特殊食物时，会触发明确的额外效果，且效果结果能被玩家观察到或被测试断言验证。",
                    "执行 `node smoke-test.js` 通过，且手动完成开始、关键交互和失败恢复三段冒烟验证。",
                ],
                "evidence_required": [
                    "本地执行 `node smoke-test.js`，结果需覆盖特殊食物与重开流程。",
                    "若特殊食物生成包含随机性，测试中需使用可控输入、桩数据或固定序列，避免只能依赖随机命中证明功能有效。",
                ],
                "allowed_surface": {
                    "roots": ["index.html", "script.js", "smoke-test.js"],
                    "files": ["index.html", "script.js", "smoke-test.js"],
                },
            },
            "allowed_surface": {
                "roots": ["index.html", "script.js", "smoke-test.js"],
                "files": ["index.html", "script.js", "smoke-test.js"],
            },
        }
        patchset = {
            "changed_files": [
                {"status": "M", "path": "index.html"},
                {"status": "M", "path": "script.js"},
                {"status": "M", "path": "smoke-test.js"},
            ],
        }

        result = self.agent._build_pre_review_evidence_bundle(task, patchset)

        self.assertFalse(result["has_blockers"])
        self.assertFalse(result["bundle"]["missing_acceptance_checks"])
        self.assertFalse(result["bundle"]["missing_evidence_required"])

    def test_infer_allowed_surface_ignores_generic_tech_labels(self):
        contract = {
            "deliverables": [
                "`index.html`：页面主文件。",
                "`styles.css` 或等效样式资源：负责 HTML/CSS 视觉表现。",
                "`script.js` 或等效脚本：若无脚本，应保证页面仅靠 HTML/CSS 也可完成浏览。",
            ]
        }

        allowed_surface = task_intelligence.infer_allowed_surface(contract)

        self.assertEqual(allowed_surface["files"], ["index.html", "styles.css", "script.js"])
        self.assertEqual(allowed_surface["roots"], ["index.html", "styles.css", "script.js"])

    def test_pre_review_verifier_does_not_block_on_generic_html_css_label(self):
        task = {
            "id": "task-4",
            "title": "福州旅游攻略",
            "description": "",
            "status": "needs_changes",
            "_claimed_from_status": "needs_changes",
            "current_contract": {
                "version": 2,
                "goal": "输出福州旅游攻略单页",
                "deliverables": ["index.html", "styles.css", "script.js"],
                "acceptance": ["页面可浏览"],
                "evidence_required": [
                    "本地执行 `python3 -m http.server 8000` 后访问网页首页，能够正常加载完整页面。"
                ],
                "allowed_surface": {
                    "roots": ["index.html", "styles.css", "script.js", "HTML"],
                    "files": ["index.html", "styles.css", "script.js", "HTML/CSS"],
                    "docs": [],
                    "cli_paths": ["index.html", "styles.css", "script.js"],
                },
            },
            "allowed_surface": {
                "roots": ["index.html", "styles.css", "script.js", "HTML"],
                "files": ["index.html", "styles.css", "script.js", "HTML/CSS"],
                "docs": [],
                "cli_paths": ["index.html", "styles.css", "script.js"],
            },
        }
        patchset = {
            "changed_files": [
                {"status": "M", "path": "index.html"},
                {"status": "M", "path": "styles.css"},
                {"status": "M", "path": "script.js"},
            ],
        }

        result = self.agent._build_pre_review_evidence_bundle(task, patchset)

        blocker_summaries = {item["summary"] for item in result["bundle"]["hard_blockers"]}
        self.assertFalse(any("HTML/CSS" in summary for summary in blocker_summaries))
        self.assertFalse(result["has_blockers"])


if __name__ == "__main__":
    unittest.main()
