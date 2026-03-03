import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SERVER_DIR = ROOT / "server"
if str(SERVER_DIR) not in sys.path:
    sys.path.insert(0, str(SERVER_DIR))

import db  # noqa: E402


class SubtaskOrderingTest(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self._old_db_path = db.DB_PATH
        db.DB_PATH = Path(self._tmp.name) / "tasks-order-test.db"
        db.init_db()
        self.project = db.create_project("order-test", self._tmp.name)

    def tearDown(self):
        db.DB_PATH = self._old_db_path
        self._tmp.cleanup()

    def _create_parent(self) -> dict:
        return db.create_task(
            title="parent",
            description="parent task",
            project_id=self.project["id"],
            status="decomposed",
        )

    def test_claim_blocks_later_subtask_until_previous_completed(self):
        parent = self._create_parent()
        st1 = db.create_task(
            title="step-1",
            description="first step",
            project_id=self.project["id"],
            parent_task_id=parent["id"],
            assigned_agent="developer",
            status="todo",
            subtask_order=1,
        )
        st2 = db.create_task(
            title="step-2",
            description="second step",
            project_id=self.project["id"],
            parent_task_id=parent["id"],
            assigned_agent="developer",
            status="todo",
            subtask_order=2,
        )

        claimed_1 = db.claim_task(
            status="todo",
            working_status="in_progress",
            agent="developer",
            agent_key="developer",
            respect_assignment=True,
            project_id=self.project["id"],
        )
        self.assertIsNotNone(claimed_1)
        self.assertEqual(claimed_1["id"], st1["id"])

        claimed_2 = db.claim_task(
            status="todo",
            working_status="in_progress",
            agent="developer",
            agent_key="developer",
            respect_assignment=True,
            project_id=self.project["id"],
        )
        self.assertIsNone(claimed_2)

        db.update_task(st1["id"], status="completed", assignee=None)

        claimed_3 = db.claim_task(
            status="todo",
            working_status="in_progress",
            agent="developer",
            agent_key="developer",
            respect_assignment=True,
            project_id=self.project["id"],
        )
        self.assertIsNotNone(claimed_3)
        self.assertEqual(claimed_3["id"], st2["id"])

    def test_list_subtasks_returns_declared_order(self):
        parent = self._create_parent()
        db.create_task(
            title="step-2",
            description="second",
            project_id=self.project["id"],
            parent_task_id=parent["id"],
            status="todo",
            subtask_order=2,
        )
        db.create_task(
            title="step-1",
            description="first",
            project_id=self.project["id"],
            parent_task_id=parent["id"],
            status="todo",
            subtask_order=1,
        )
        listed = db.list_subtasks(parent["id"])
        orders = [int(x.get("subtask_order") or 0) for x in listed]
        self.assertEqual(orders, [1, 2])

    def test_init_db_backfills_legacy_subtask_order(self):
        parent = self._create_parent()
        st1 = db.create_task(
            title="legacy-1",
            description="legacy",
            project_id=self.project["id"],
            parent_task_id=parent["id"],
            status="todo",
        )
        st2 = db.create_task(
            title="legacy-2",
            description="legacy",
            project_id=self.project["id"],
            parent_task_id=parent["id"],
            status="todo",
        )
        before = db.list_subtasks(parent["id"])
        self.assertTrue(all(int(x.get("subtask_order") or 0) == 0 for x in before))

        db.init_db()
        after = db.list_subtasks(parent["id"])
        orders = [int(x.get("subtask_order") or 0) for x in after]
        self.assertEqual(orders, [1, 2])
        ids = [x["id"] for x in after]
        self.assertEqual(ids, [st1["id"], st2["id"]])


if __name__ == "__main__":
    unittest.main()
