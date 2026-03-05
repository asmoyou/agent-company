import subprocess
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


def _run(args: list[str], cwd: Path) -> str:
    proc = subprocess.run(args, cwd=str(cwd), capture_output=True, text=True)
    if proc.returncode != 0:
        raise AssertionError(f"cmd failed: {' '.join(args)}\n{proc.stderr}")
    return proc.stdout.strip()


class ProjectFileAutoCommitTest(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.repo = Path(self._tmp.name) / "repo"
        self.repo.mkdir(parents=True, exist_ok=True)

        self._old_db_path = db.DB_PATH
        db.DB_PATH = Path(self._tmp.name) / "project-file-autocommit.db"
        db.init_db()

        self.client = TestClient(app_module.app)
        setup = self.client.post("/auth/setup-admin", json={"password": "admin123"})
        self.assertEqual(setup.status_code, 200)
        self.headers = {"Authorization": f"Bearer {setup.json()['token']}"}

        self.project = db.create_project("repo", str(self.repo))
        setup_repo = self.client.post(f"/projects/{self.project['id']}/setup", headers=self.headers)
        self.assertEqual(setup_repo.status_code, 200)

    def tearDown(self):
        self.client.close()
        db.DB_PATH = self._old_db_path
        self._tmp.cleanup()

    def test_upload_auto_commits_to_main_and_is_visible_in_new_branch(self):
        res = self.client.post(
            f"/projects/{self.project['id']}/files/upload",
            params={"path": ""},
            files=[("files", ("context.txt", b"hello\n", "text/plain"))],
            headers=self.headers,
        )
        self.assertEqual(res.status_code, 200)
        payload = res.json()
        self.assertEqual(payload.get("git", {}).get("status"), "committed")

        files_on_main = _run(["git", "ls-tree", "-r", "--name-only", "main"], cwd=self.repo).splitlines()
        self.assertIn("context.txt", files_on_main)

        branch = "agent/developer/task-upload"
        _run(["git", "branch", branch, "main"], cwd=self.repo)
        blob = _run(["git", "show", f"{branch}:context.txt"], cwd=self.repo)
        self.assertEqual(blob, "hello")

    def test_delete_auto_commits_to_main(self):
        self.client.post(
            f"/projects/{self.project['id']}/files/upload",
            params={"path": ""},
            files=[("files", ("remove-me.txt", b"bye\n", "text/plain"))],
            headers=self.headers,
        )
        before_count = int(_run(["git", "rev-list", "--count", "main"], cwd=self.repo))

        res = self.client.delete(
            f"/projects/{self.project['id']}/files",
            params={"path": "remove-me.txt"},
            headers=self.headers,
        )
        self.assertEqual(res.status_code, 200)
        payload = res.json()
        self.assertEqual(payload.get("git", {}).get("status"), "committed")

        after_count = int(_run(["git", "rev-list", "--count", "main"], cwd=self.repo))
        self.assertEqual(after_count, before_count + 1)

        files_on_main = _run(["git", "ls-tree", "-r", "--name-only", "main"], cwd=self.repo).splitlines()
        self.assertNotIn("remove-me.txt", files_on_main)


if __name__ == "__main__":
    unittest.main()
