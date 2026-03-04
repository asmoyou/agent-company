import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
AGENTS_DIR = ROOT / "agents"
if str(AGENTS_DIR) not in sys.path:
    sys.path.insert(0, str(AGENTS_DIR))

import base as base_module  # noqa: E402


class TaskScopedWorkspaceTest(unittest.TestCase):
    def test_agent_branch_is_task_scoped(self):
        task = {"id": "8cd3dd31-13f1-424c-80fb-95204f6ab5bc"}
        branch = base_module.get_agent_branch("developer", task=task)
        self.assertEqual(branch, "agent/developer/8cd3dd31-13f1-424c-80fb-95204f6ab5bc")

    def test_project_dirs_include_task_scope(self):
        task = {
            "id": "task-1",
            "project_path": "/tmp/project-a",
            "assigned_agent": "developer",
            "dev_agent": "developer",
        }
        root, worktree = base_module.get_project_dirs(task, agent_key="developer")
        self.assertEqual(root, Path("/tmp/project-a"))
        self.assertEqual(worktree, Path("/tmp/project-a/.worktrees/developer/task-1"))


if __name__ == "__main__":
    unittest.main()
