import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SERVER_DIR = ROOT / "server"
if str(SERVER_DIR) not in sys.path:
    sys.path.insert(0, str(SERVER_DIR))

import app as app_module  # noqa: E402


def _run(args: list[str], cwd: Path):
    proc = subprocess.run(args, cwd=str(cwd), capture_output=True, text=True)
    if proc.returncode != 0:
        raise AssertionError(f"cmd failed: {' '.join(args)}\n{proc.stderr}")
    return proc.stdout.strip()


class WorkspaceCleanupTest(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.repo = Path(self._tmp.name) / "repo"
        self.repo.mkdir(parents=True, exist_ok=True)
        _run(["git", "init"], cwd=self.repo)
        _run(["git", "config", "user.email", "agent@opc-demo.local"], cwd=self.repo)
        _run(["git", "config", "user.name", "OPC Agent"], cwd=self.repo)
        _run(["git", "checkout", "-b", "main"], cwd=self.repo)
        _run(["git", "commit", "--allow-empty", "-m", "chore: init"], cwd=self.repo)

    def tearDown(self):
        self._tmp.cleanup()

    def test_cleanup_completed_task_removes_worktree_and_branch(self):
        task = {
            "id": "task-1",
            "status": "completed",
            "project_path": str(self.repo),
            "assigned_agent": "developer",
            "dev_agent": "developer",
        }
        branch = app_module.task_dev_branch(task)
        worktree = self.repo / ".worktrees" / "developer" / "task-1"

        _run(["git", "branch", branch, "main"], cwd=self.repo)
        _run(["git", "worktree", "add", str(worktree), branch], cwd=self.repo)

        result = app_module._cleanup_task_workspace_sync(task, "unit_test")
        self.assertTrue(result["ok"])
        self.assertFalse(worktree.exists())

        listed = _run(["git", "branch", "--list", branch], cwd=self.repo)
        self.assertEqual(listed, "")

    def test_cleanup_cancelled_task_force_deletes_unmerged_branch(self):
        task = {
            "id": "task-2",
            "status": "cancelled",
            "project_path": str(self.repo),
            "assigned_agent": "developer",
            "dev_agent": "developer",
        }
        branch = app_module.task_dev_branch(task)
        worktree = self.repo / ".worktrees" / "developer" / "task-2"

        _run(["git", "branch", branch, "main"], cwd=self.repo)
        _run(["git", "worktree", "add", str(worktree), branch], cwd=self.repo)
        (worktree / "x.txt").write_text("x\n", encoding="utf-8")
        _run(["git", "add", "x.txt"], cwd=worktree)
        _run(["git", "commit", "-m", "feat: temp"], cwd=worktree)
        _run(["git", "checkout", "main"], cwd=self.repo)

        result = app_module._cleanup_task_workspace_sync(task, "unit_test_cancelled")
        self.assertTrue(result["ok"])
        self.assertFalse(worktree.exists())

        listed = _run(["git", "branch", "--list", branch], cwd=self.repo)
        self.assertEqual(listed, "")


if __name__ == "__main__":
    unittest.main()
