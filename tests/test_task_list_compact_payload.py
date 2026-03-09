import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SERVER_DIR = ROOT / "server"
if str(SERVER_DIR) not in sys.path:
    sys.path.insert(0, str(SERVER_DIR))

import db  # noqa: E402


class TaskListCompactPayloadTest(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self._old_db_path = db.DB_PATH
        db.DB_PATH = Path(self._tmp.name) / "tasks-compact-test.db"
        db.init_db()
        self.project = db.create_project("compact-test", self._tmp.name)

    def tearDown(self):
        db.DB_PATH = self._old_db_path
        self._tmp.cleanup()

    def test_list_tasks_compact_omits_heavy_fields(self):
        created = db.create_task(
            title="large payload task",
            description="x" * 2000,
            project_id=self.project["id"],
            status="needs_changes",
        )
        db.update_task(
            created["id"],
            status="needs_changes",
            review_feedback="f" * 600,
            feedback_source="reviewer",
            feedback_stage="review_to_dev",
            feedback_actor="reviewer",
        )

        rows = db.list_tasks(project_id=self.project["id"], compact=True)
        self.assertEqual(len(rows), 1)
        row = rows[0]

        self.assertEqual(row["id"], created["id"])
        self.assertEqual(row["title"], "large payload task")
        self.assertIn("review_feedback", row)
        self.assertLessEqual(len(str(row.get("review_feedback") or "")), 240)

        self.assertNotIn("description", row)
        self.assertNotIn("review_feedback_history", row)
        self.assertNotIn("claim_run_id", row)
        self.assertNotIn("lease_token", row)

    def test_list_tasks_compact_keeps_cancel_reason(self):
        created = db.create_task(
            title="cancelled task",
            description="cancel me",
            project_id=self.project["id"],
            status="todo",
        )
        db.cancel_task(created["id"], include_subtasks=False, reason="需求调整")

        rows = db.list_tasks(project_id=self.project["id"], compact=True)
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["cancel_reason"], "需求调整")

    def test_get_task_backfills_cancel_reason_from_legacy_cancel_log(self):
        created = db.create_task(
            title="legacy cancelled task",
            description="legacy cancel",
            project_id=self.project["id"],
            status="todo",
        )
        conn = db.get_conn()
        try:
            conn.execute(
                "UPDATE tasks SET status='cancelled', archived=1, cancel_reason='' WHERE id=?",
                (created["id"],),
            )
            conn.commit()
        finally:
            conn.close()
        db.add_log(created["id"], "system", "任务已取消并归档，不再执行。\n原因：违规任务")

        task = db.get_task(created["id"])
        self.assertIsNotNone(task)
        self.assertEqual(task["cancel_reason"], "违规任务")

        rows = db.list_tasks(project_id=self.project["id"], compact=True)
        self.assertEqual(rows[0]["cancel_reason"], "违规任务")

    def test_get_task_preserves_original_description_after_refinement(self):
        created = db.create_task(
            title="refined task",
            description="用户手工填写的原始需求",
            project_id=self.project["id"],
            status="triage",
        )

        db.update_task(created["id"], description="AI 补全后的执行描述", status="todo")

        task = db.get_task(created["id"])
        self.assertIsNotNone(task)
        self.assertEqual(task["original_description"], "用户手工填写的原始需求")
        self.assertEqual(task["description"], "AI 补全后的执行描述")

    def test_task_lifecycle_is_available_for_compact_and_detail_payloads(self):
        created = db.create_task(
            title="timeline task",
            description="timeline",
            project_id=self.project["id"],
            status="triage",
        )
        t0 = "2026-03-01T09:00:00"
        t1 = "2026-03-01T09:05:00"
        t2 = "2026-03-01T09:20:00"
        t3 = "2026-03-01T09:45:00"
        t4 = "2026-03-01T10:00:00"

        h1 = db.add_handoff(
            created["id"],
            stage="leader_to_todo",
            from_agent="leader",
            to_agent="developer",
            status_from="triage",
            status_to="todo",
            summary="进入待开发",
        )
        h2 = db.add_handoff(
            created["id"],
            stage="dev_started",
            from_agent="developer",
            to_agent="developer",
            status_from="todo",
            status_to="in_progress",
            summary="开始开发",
        )
        h3 = db.add_handoff(
            created["id"],
            stage="dev_to_review",
            from_agent="developer",
            to_agent="reviewer",
            status_from="in_progress",
            status_to="in_review",
            summary="进入审查",
        )
        h4 = db.add_handoff(
            created["id"],
            stage="merge_to_acceptance",
            from_agent="manager",
            to_agent="user",
            status_from="approved",
            status_to="pending_acceptance",
            summary="等待验收",
        )

        conn = db.get_conn()
        try:
            conn.execute(
                "UPDATE tasks SET status=?, created_at=?, updated_at=? WHERE id=?",
                ("pending_acceptance", t0, t4, created["id"]),
            )
            conn.execute("UPDATE task_handoffs SET created_at=? WHERE id=?", (t1, h1["id"]))
            conn.execute("UPDATE task_handoffs SET created_at=? WHERE id=?", (t2, h2["id"]))
            conn.execute("UPDATE task_handoffs SET created_at=? WHERE id=?", (t3, h3["id"]))
            conn.execute("UPDATE task_handoffs SET created_at=? WHERE id=?", (t4, h4["id"]))
            conn.commit()
        finally:
            conn.close()

        task = db.get_task(created["id"])
        self.assertIsNotNone(task)
        self.assertEqual(task["lifecycle"]["created_at"], t0)
        self.assertEqual(task["lifecycle"]["started_at"], t2)
        self.assertEqual(task["lifecycle"]["review_started_at"], t3)
        self.assertEqual(task["lifecycle"]["acceptance_started_at"], t4)
        self.assertEqual(task["lifecycle"]["current_status"], "pending_acceptance")
        self.assertEqual(task["lifecycle"]["current_status_at"], t4)
        self.assertEqual(
            [item["status"] for item in task["status_timeline"]],
            ["triage", "todo", "in_progress", "in_review", "pending_acceptance"],
        )

        rows = db.list_tasks(project_id=self.project["id"], compact=True)
        self.assertEqual(rows[0]["lifecycle"]["started_at"], t2)
        self.assertNotIn("status_timeline", rows[0])


if __name__ == "__main__":
    unittest.main()
