import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SERVER_DIR = ROOT / "server"
if str(SERVER_DIR) not in sys.path:
    sys.path.insert(0, str(SERVER_DIR))

import db  # noqa: E402


class TaskPriorityDependencyTest(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self._old_db_path = db.DB_PATH
        db.DB_PATH = Path(self._tmp.name) / "tasks-priority-dependency-test.db"
        db.init_db()
        self.project = db.create_project("prio-dep", self._tmp.name)

    def tearDown(self):
        db.DB_PATH = self._old_db_path
        self._tmp.cleanup()

    def _create_task(self, title: str, **kwargs) -> dict:
        return db.create_task(
            title=title,
            description=f"{title} desc",
            project_id=self.project["id"],
            **kwargs,
        )

    def test_create_task_defaults_priority_to_p2(self):
        task = self._create_task("default-priority")
        self.assertEqual(int(task.get("priority") or -1), 2)

    def test_claim_prefers_higher_priority(self):
        low = self._create_task(
            "low-priority",
            status="todo",
            assigned_agent="developer",
            dev_agent="developer",
            priority=3,
        )
        high = self._create_task(
            "high-priority",
            status="todo",
            assigned_agent="developer",
            dev_agent="developer",
            priority=0,
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
        self.assertEqual(claimed["id"], high["id"])
        self.assertNotEqual(claimed["id"], low["id"])

    def test_claim_blocks_when_dependency_not_satisfied(self):
        dep = self._create_task("dep-task", status="in_review")
        blocked = self._create_task(
            "blocked-task",
            status="todo",
            assigned_agent="developer",
            dev_agent="developer",
            dependencies=[
                {
                    "depends_on_task_id": dep["id"],
                    "required_state": "completed",
                }
            ],
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

        listed = db.list_tasks(project_id=self.project["id"])
        row = next(t for t in listed if t["id"] == blocked["id"])
        self.assertEqual(int(row.get("blocking_dependency_count") or 0), 1)
        self.assertFalse(bool(row.get("ready")))

        db.update_task(dep["id"], status="completed")
        claimed2 = db.claim_task(
            status="todo",
            working_status="in_progress",
            agent="developer",
            agent_key="developer",
            respect_assignment=True,
            project_id=self.project["id"],
        )
        self.assertIsNotNone(claimed2)
        self.assertEqual(claimed2["id"], blocked["id"])

    def test_dependency_required_state_approved(self):
        dep = self._create_task("dep-approved", status="approved")
        task = self._create_task(
            "wait-approved",
            status="todo",
            assigned_agent="developer",
            dev_agent="developer",
            dependencies=[
                {
                    "depends_on_task_id": dep["id"],
                    "required_state": "approved",
                }
            ],
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

    def test_dependency_defaults_to_approved_and_accepts_pending_acceptance(self):
        dep = self._create_task("dep-pending-acceptance", status="pending_acceptance")
        task = self._create_task(
            "wait-default-approved",
            status="todo",
            assigned_agent="developer",
            dev_agent="developer",
            dependencies=[
                {
                    "depends_on_task_id": dep["id"],
                }
            ],
        )

        deps = db.list_task_dependencies(task["id"])
        self.assertEqual(len(deps), 1)
        self.assertEqual(deps[0]["required_state"], "approved")

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

    def test_dependency_cycle_rejected(self):
        a = self._create_task("task-a")
        b = self._create_task("task-b")
        ok = db.replace_task_dependencies(
            a["id"],
            [{"depends_on_task_id": b["id"], "required_state": "completed"}],
            created_by="tester",
        )
        self.assertIsNotNone(ok)

        with self.assertRaises(db.DependencyCycleError):
            db.replace_task_dependencies(
                b["id"],
                [{"depends_on_task_id": a["id"], "required_state": "completed"}],
                created_by="tester",
            )


if __name__ == "__main__":
    unittest.main()
