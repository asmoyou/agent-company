import sys
import tempfile
import unittest
from datetime import datetime, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SERVER_DIR = ROOT / "server"
if str(SERVER_DIR) not in sys.path:
    sys.path.insert(0, str(SERVER_DIR))

import db  # noqa: E402


class AgentRuntimeMonitoringDbTest(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self._old_db_path = db.DB_PATH
        db.DB_PATH = Path(self._tmp.name) / "runtime-monitoring-test.db"
        db.init_db()
        self.project = db.create_project("runtime-monitoring", self._tmp.name)

    def tearDown(self):
        db.DB_PATH = self._old_db_path
        self._tmp.cleanup()

    def _create_claimed_task(self) -> dict:
        task = db.create_task(
            title="lease-task",
            description="lease recovery test",
            project_id=self.project["id"],
            assigned_agent="developer",
            dev_agent="developer",
            status="todo",
        )
        claimed = db.claim_task(
            status="todo",
            working_status="in_progress",
            agent="developer",
            agent_key="developer",
            respect_assignment=True,
            project_id=self.project["id"],
            lease_ttl_secs=180,
        )
        self.assertIsNotNone(claimed)
        return claimed

    def test_recover_expired_leases_respects_exclude_task_ids(self):
        claimed = self._create_claimed_task()
        expired_at = (datetime.utcnow() - timedelta(seconds=5)).isoformat()
        db.update_task(claimed["id"], lease_expires_at=expired_at)

        skipped = db.recover_expired_task_leases(
            grace_secs=0,
            exclude_task_ids={claimed["id"]},
        )
        self.assertEqual(skipped, [])
        still_claimed = db.get_task(claimed["id"])
        self.assertEqual(still_claimed["status"], "in_progress")
        self.assertEqual(still_claimed["assignee"], "developer")

        recovered = db.recover_expired_task_leases(grace_secs=0)
        self.assertEqual(len(recovered), 1)
        refreshed = db.get_task(claimed["id"])
        self.assertEqual(refreshed["status"], "todo")
        self.assertIsNone(refreshed["assignee"])

    def test_agent_output_is_persisted_and_trimmed(self):
        db.add_agent_output("developer", "line-1", task_id="t1", run_id="r1", keep_last=3)
        db.add_agent_output("developer", "line-2", task_id="t1", run_id="r1", keep_last=3)
        db.add_agent_output(
            "developer",
            "line-3",
            task_id="t1",
            run_id="r1",
            kind="stderr",
            keep_last=3,
        )
        db.add_agent_output(
            "developer",
            "line-4",
            task_id="t1",
            run_id="r1",
            kind="event",
            event="finished",
            exit_code=1,
            keep_last=3,
        )

        lines = db.get_agent_output_lines("developer", limit=10)
        self.assertEqual(lines, ["line-2", "line-3", "line-4"])
        entries = db.get_agent_output_entries("developer", limit=10)
        self.assertEqual(len(entries), 3)
        self.assertEqual(entries[-1]["kind"], "event")
        self.assertEqual(entries[-1]["event"], "finished")
        self.assertEqual(entries[-1]["exit_code"], 1)
        agents = set(db.list_agent_output_agents())
        self.assertIn("developer", agents)


if __name__ == "__main__":
    unittest.main()
