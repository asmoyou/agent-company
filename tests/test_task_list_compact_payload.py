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


if __name__ == "__main__":
    unittest.main()
