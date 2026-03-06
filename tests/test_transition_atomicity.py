import sys
import tempfile
import unittest
import json
from pathlib import Path
from unittest import mock

ROOT = Path(__file__).resolve().parents[1]
SERVER_DIR = ROOT / "server"
if str(SERVER_DIR) not in sys.path:
    sys.path.insert(0, str(SERVER_DIR))

import db  # noqa: E402


class TransitionTaskAtomicityTest(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self._old_db_path = db.DB_PATH
        db.DB_PATH = Path(self._tmp.name) / "tasks-test.db"
        db.init_db()
        self.project = db.create_project("test", self._tmp.name)

    def tearDown(self):
        db.DB_PATH = self._old_db_path
        self._tmp.cleanup()

    def _create_task(self, status: str = "todo") -> dict:
        return db.create_task(
            title="atomic transition",
            description="verify transition transaction behavior",
            project_id=self.project["id"],
            status=status,
        )

    def _handoff(self) -> dict:
        return {
            "stage": "dev_to_review",
            "from_agent": "developer",
            "to_agent": "reviewer",
            "status_from": "todo",
            "status_to": "in_review",
            "title": "handoff",
            "summary": "ready for review",
            "commit_hash": "abc1234",
            "conclusion": "done",
            "payload": {"commit_hash": "abc1234"},
        }

    def test_transition_updates_task_and_creates_side_effects(self):
        task = self._create_task(status="todo")
        result = db.transition_task(
            task["id"],
            fields={"status": "in_review", "assignee": None, "commit_hash": "abc1234"},
            handoff=self._handoff(),
            log={"agent": "developer", "message": "submitted"},
        )
        self.assertIsNotNone(result)
        self.assertEqual(result["task"]["status"], "in_review")
        self.assertIsNotNone(result["handoff"])
        self.assertIsNotNone(result["log"])
        self.assertEqual(len(db.get_handoffs(task["id"])), 1)
        self.assertEqual(len(db.get_logs(task["id"])), 1)

    def test_transition_rolls_back_when_handoff_insert_fails(self):
        task = self._create_task(status="todo")
        with mock.patch("db._add_handoff_in_conn", side_effect=RuntimeError("boom")):
            with self.assertRaises(RuntimeError):
                db.transition_task(
                    task["id"],
                    fields={"status": "in_review"},
                    handoff=self._handoff(),
                )
        refreshed = db.get_task(task["id"])
        self.assertIsNotNone(refreshed)
        self.assertEqual(refreshed["status"], "todo")
        self.assertEqual(len(db.get_handoffs(task["id"])), 0)

    def test_cancelled_task_remains_immutable_and_skips_side_effects(self):
        task = self._create_task(status="todo")
        db.cancel_task(task["id"], include_subtasks=False)
        result = db.transition_task(
            task["id"],
            fields={"status": "in_review"},
            handoff=self._handoff(),
            log={"agent": "developer", "message": "should be skipped"},
        )
        self.assertIsNotNone(result)
        self.assertEqual(result["task"]["status"], "cancelled")
        self.assertEqual(result["task"]["archived"], 1)
        self.assertIsNone(result["handoff"])
        self.assertIsNone(result["log"])
        self.assertEqual(len(db.get_handoffs(task["id"])), 0)
        self.assertEqual(len(db.get_logs(task["id"])), 0)

    def test_transition_returns_none_for_missing_task(self):
        result = db.transition_task(
            "missing-task-id",
            fields={"status": "in_review"},
            handoff=self._handoff(),
            log={"agent": "developer", "message": "submitted"},
        )
        self.assertIsNone(result)

    def test_feedback_history_keeps_cross_stage_issues_open(self):
        task = self._create_task(status="todo")

        db.transition_task(
            task["id"],
            fields={
                "status": "needs_changes",
                "review_feedback": "manager: rebuild on latest main",
                "feedback_source": "manager",
                "feedback_stage": "merge_to_dev",
                "feedback_actor": "manager",
            },
        )
        db.transition_task(
            task["id"],
            fields={
                "status": "needs_changes",
                "review_feedback": "reviewer: remove out-of-scope files",
                "feedback_source": "reviewer",
                "feedback_stage": "review_to_dev",
                "feedback_actor": "reviewer",
            },
        )

        refreshed = db.get_task(task["id"])
        history = json.loads(refreshed["review_feedback_history"])
        self.assertEqual(len(history), 2)
        self.assertFalse(history[0]["resolved"])
        self.assertFalse(history[1]["resolved"])

    def test_feedback_history_only_supersedes_same_stage_feedback(self):
        task = self._create_task(status="todo")

        db.transition_task(
            task["id"],
            fields={
                "status": "needs_changes",
                "review_feedback": "manager: rebuild on latest main",
                "feedback_source": "manager",
                "feedback_stage": "merge_to_dev",
                "feedback_actor": "manager",
            },
        )
        db.transition_task(
            task["id"],
            fields={
                "status": "needs_changes",
                "review_feedback": "reviewer: first review issue",
                "feedback_source": "reviewer",
                "feedback_stage": "review_to_dev",
                "feedback_actor": "reviewer",
            },
        )
        db.transition_task(
            task["id"],
            fields={
                "status": "needs_changes",
                "review_feedback": "reviewer: second review issue",
                "feedback_source": "reviewer",
                "feedback_stage": "review_to_dev",
                "feedback_actor": "reviewer",
            },
        )

        refreshed = db.get_task(task["id"])
        history = json.loads(refreshed["review_feedback_history"])
        self.assertEqual(len(history), 3)
        self.assertFalse(history[0]["resolved"])
        self.assertTrue(history[1]["resolved"])
        self.assertEqual(history[1]["resolved_reason"], "superseded")
        self.assertFalse(history[2]["resolved"])


if __name__ == "__main__":
    unittest.main()
