import sys
import tempfile
import unittest
from pathlib import Path

from fastapi.testclient import TestClient

ROOT = Path(__file__).resolve().parents[1]
SERVER_DIR = ROOT / "server"
if str(SERVER_DIR) not in sys.path:
    sys.path.insert(0, str(SERVER_DIR))

import app as app_module  # noqa: E402
import db  # noqa: E402


class TaskActionsApiTest(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self._old_db_path = db.DB_PATH
        db.DB_PATH = Path(self._tmp.name) / "tasks-api-test.db"
        db.init_db()
        self.client = TestClient(app_module.app)
        self.project = db.create_project("api-test", self._tmp.name)

    def tearDown(self):
        self.client.close()
        db.DB_PATH = self._old_db_path
        self._tmp.cleanup()

    def _create_task(self, status: str, **extra) -> dict:
        return db.create_task(
            title="api-action-task",
            description="status action api test",
            project_id=self.project["id"],
            assigned_agent=extra.get("assigned_agent"),
            dev_agent=extra.get("dev_agent"),
            status=status,
        )

    def test_patch_rejects_direct_status_change(self):
        task = self._create_task(status="todo")
        res = self.client.patch(f"/tasks/{task['id']}", json={"status": "in_review"})
        self.assertEqual(res.status_code, 403)

    def test_transition_rejects_invalid_status_flow(self):
        task = self._create_task(status="todo")
        res = self.client.post(
            f"/tasks/{task['id']}/transition",
            json={"fields": {"status": "completed"}},
        )
        self.assertEqual(res.status_code, 409)

    def test_claim_uses_server_working_status(self):
        task = self._create_task(status="todo", assigned_agent="developer", dev_agent="developer")
        res = self.client.post(
            "/tasks/claim",
            json={
                "status": "todo",
                "working_status": "hijacked_status",
                "agent": "developer",
                "agent_key": "developer",
                "respect_assignment": True,
                "project_id": self.project["id"],
            },
        )
        self.assertEqual(res.status_code, 200)
        claimed = res.json()["task"]
        self.assertIsNotNone(claimed)
        self.assertEqual(claimed["id"], task["id"])
        self.assertEqual(claimed["status"], "in_progress")

    def test_claim_rejects_status_outside_agent_poll_range(self):
        self._create_task(status="approved")
        res = self.client.post(
            "/tasks/claim",
            json={
                "status": "approved",
                "working_status": "merging",
                "agent": "developer",
                "agent_key": "developer",
                "respect_assignment": True,
                "project_id": self.project["id"],
            },
        )
        self.assertEqual(res.status_code, 409)

    def test_accept_action_completes_task(self):
        task = self._create_task(status="pending_acceptance")
        res = self.client.post(f"/tasks/{task['id']}/actions", json={"action": "accept"})
        self.assertEqual(res.status_code, 200)
        data = res.json()
        self.assertEqual(data["task"]["status"], "completed")

    def test_reject_action_routes_to_dev(self):
        task = self._create_task(status="pending_acceptance")
        res = self.client.post(
            f"/tasks/{task['id']}/actions",
            json={"action": "reject", "feedback": "验收不通过，请修复边界情况"},
        )
        self.assertEqual(res.status_code, 200)
        data = res.json()
        self.assertEqual(data["task"]["status"], "needs_changes")
        self.assertEqual(data["task"]["assigned_agent"], "developer")
        self.assertEqual(data["task"]["dev_agent"], "developer")
        handoffs = db.get_handoffs(task["id"])
        self.assertTrue(any(h["stage"] == "user_to_dev" for h in handoffs))

    def test_retry_blocked_action_is_backend_decided(self):
        task = self._create_task(
            status="blocked",
            assigned_agent="reviewer",
            dev_agent="developer",
        )
        db.update_task(task["id"], review_feedback="[系统错误] 审查器连续失败")
        res = self.client.post(
            f"/tasks/{task['id']}/actions",
            json={"action": "retry_blocked"},
        )
        self.assertEqual(res.status_code, 200)
        data = res.json()
        self.assertEqual(data["task"]["status"], "in_review")
        self.assertEqual(data["task"]["assigned_agent"], "reviewer")

    def test_decompose_action_moves_todo_to_decompose(self):
        task = self._create_task(status="todo")
        res = self.client.post(
            f"/tasks/{task['id']}/actions",
            json={"action": "decompose"},
        )
        self.assertEqual(res.status_code, 200)
        self.assertEqual(res.json()["task"]["status"], "decompose")

    def test_archive_action_only_for_completed(self):
        task = self._create_task(status="completed")
        res = self.client.post(
            f"/tasks/{task['id']}/actions",
            json={"action": "archive"},
        )
        self.assertEqual(res.status_code, 200)
        self.assertEqual(int(res.json()["task"]["archived"]), 1)

        not_done = self._create_task(status="todo")
        bad = self.client.post(
            f"/tasks/{not_done['id']}/actions",
            json={"action": "archive"},
        )
        self.assertEqual(bad.status_code, 409)

    def test_task_files_branch_falls_back_to_assignee(self):
        task = self._create_task(status="in_progress")
        db.update_task(
            task["id"],
            assignee="asmo-dev",
            assigned_agent=None,
            dev_agent=None,
        )
        res = self.client.get(f"/tasks/{task['id']}/files")
        self.assertEqual(res.status_code, 200)
        data = res.json()
        self.assertEqual(data["branch"], "agent/asmo-dev")


if __name__ == "__main__":
    unittest.main()
