import sys
import shutil
import tempfile
import unittest
from datetime import datetime, timedelta
from pathlib import Path
from unittest import mock

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
        app_module._TASK_WORKSPACE_CLEANUP_INFLIGHT.clear()
        app_module._TASK_WORKSPACE_CLEANUP_STATE.clear()
        app_module._TASK_WORKSPACE_CLEANUP_EVENTS.clear()
        app_module._TASK_WORKSPACE_CLEANUP_METRICS.update(
            {
                "scheduled": 0,
                "executed": 0,
                "finalized": 0,
                "failed": 0,
                "last_run_at": "",
                "last_finalized_at": "",
                "last_failed_at": "",
            }
        )
        app_module.AGENT_OUTPUT.clear()
        app_module.AGENT_STATUS.clear()
        for key in ("developer", "reviewer", "manager"):
            app_module._ensure_agent_state(key)
        self.client = TestClient(app_module.app)
        setup = self.client.post("/auth/setup-admin", json={"password": "admin123"})
        self.assertEqual(setup.status_code, 200)
        self._headers = {"Authorization": f"Bearer {setup.json()['token']}"}
        self._agent_headers = {"X-Agent-Token": app_module.AGENT_API_TOKEN}
        self._bad_agent_headers = {"X-Agent-Token": "bad-token"}
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

    def _create_custom_agent(self, key: str = "asmo-dev", runtime_profile: str = "generic") -> None:
        res = self.client.post(
            "/agent-types",
            json={
                "key": key,
                "name": "ASMO Developer",
                "description": "custom developer",
                "prompt": "do task",
                "poll_statuses": ["todo", "needs_changes"],
                "next_status": "in_review",
                "working_status": "coding",
                "runtime_profile": runtime_profile,
                "cli": "claude",
            },
            headers=self._headers,
        )
        self.assertEqual(res.status_code, 201)

    def test_admin_can_edit_and_delete_user(self):
        created = self.client.post(
            "/users",
            json={"username": "dev01", "password": "devpass1"},
            headers=self._headers,
        )
        self.assertEqual(created.status_code, 201)
        user = created.json()

        updated = self.client.patch(
            f"/users/{user['id']}",
            json={"username": "dev02", "role": "admin", "password": "newpass1"},
            headers=self._headers,
        )
        self.assertEqual(updated.status_code, 200)
        payload = updated.json()
        self.assertEqual(payload["username"], "dev02")
        self.assertEqual(payload["role"], "admin")
        self.assertTrue(payload["password_set"])

        relogin = self.client.post(
            "/auth/login",
            json={"username": "dev02", "password": "newpass1"},
        )
        self.assertEqual(relogin.status_code, 200)

        deleted = self.client.delete(f"/users/{user['id']}", headers=self._headers)
        self.assertEqual(deleted.status_code, 200)
        self.assertTrue(deleted.json().get("ok"))

        users = self.client.get("/users", headers=self._headers)
        self.assertEqual(users.status_code, 200)
        self.assertNotIn("dev02", {u["username"] for u in users.json()})

    def test_user_management_rejects_delete_self(self):
        me = self.client.get("/auth/me", headers=self._headers)
        self.assertEqual(me.status_code, 200)
        uid = me.json()["id"]

        res = self.client.delete(f"/users/{uid}", headers=self._headers)
        self.assertEqual(res.status_code, 422)

    def test_user_management_rejects_demote_current_admin(self):
        me = self.client.get("/auth/me", headers=self._headers)
        self.assertEqual(me.status_code, 200)
        uid = me.json()["id"]

        res = self.client.patch(
            f"/users/{uid}",
            json={"role": "user"},
            headers=self._headers,
        )
        self.assertEqual(res.status_code, 422)

    def test_onboarding_complete_marks_user_once(self):
        me_before = self.client.get("/auth/me", headers=self._headers)
        self.assertEqual(me_before.status_code, 200)
        self.assertFalse(me_before.json().get("onboarding_completed_at"))

        first = self.client.post("/auth/onboarding-complete", headers=self._headers)
        self.assertEqual(first.status_code, 200)
        first_payload = first.json()
        self.assertTrue(first_payload.get("ok"))
        stamped = str(first_payload["user"].get("onboarding_completed_at") or "").strip()
        self.assertTrue(stamped)

        me_after = self.client.get("/auth/me", headers=self._headers)
        self.assertEqual(me_after.status_code, 200)
        self.assertEqual(me_after.json().get("onboarding_completed_at"), stamped)

        second = self.client.post("/auth/onboarding-complete", headers=self._headers)
        self.assertEqual(second.status_code, 200)
        self.assertEqual(second.json()["user"].get("onboarding_completed_at"), stamped)

    def test_non_admin_cannot_manage_users(self):
        created = self.client.post(
            "/users",
            json={"username": "dev03", "password": "devpass3"},
            headers=self._headers,
        )
        self.assertEqual(created.status_code, 201)
        target = created.json()

        login = self.client.post(
            "/auth/login",
            json={"username": "dev03", "password": "devpass3"},
        )
        self.assertEqual(login.status_code, 200)
        user_headers = {"Authorization": f"Bearer {login.json()['token']}"}

        list_res = self.client.get("/users", headers=user_headers)
        self.assertEqual(list_res.status_code, 403)

        patch_res = self.client.patch(
            f"/users/{target['id']}",
            json={"username": "dev03x"},
            headers=user_headers,
        )
        self.assertEqual(patch_res.status_code, 403)

        del_res = self.client.delete(f"/users/{target['id']}", headers=user_headers)
        self.assertEqual(del_res.status_code, 403)

    def test_user_can_change_own_password_and_rotate_session(self):
        created = self.client.post(
            "/users",
            json={"username": "dev04", "password": "devpass4"},
            headers=self._headers,
        )
        self.assertEqual(created.status_code, 201)

        login = self.client.post(
            "/auth/login",
            json={"username": "dev04", "password": "devpass4"},
        )
        self.assertEqual(login.status_code, 200)
        old_token = login.json()["token"]
        user_headers = {"Authorization": f"Bearer {old_token}"}

        changed = self.client.post(
            "/auth/change-password",
            json={"current_password": "devpass4", "new_password": "newpass4"},
            headers=user_headers,
        )
        self.assertEqual(changed.status_code, 200)
        payload = changed.json()
        self.assertTrue(payload.get("ok"))
        self.assertIn("token", payload)
        self.assertNotEqual(payload["token"], old_token)

        old_me = self.client.get("/auth/me", headers=user_headers)
        self.assertEqual(old_me.status_code, 401)

        new_headers = {"Authorization": f"Bearer {payload['token']}"}
        new_me = self.client.get("/auth/me", headers=new_headers)
        self.assertEqual(new_me.status_code, 200)
        self.assertEqual(new_me.json()["username"], "dev04")

        old_login = self.client.post(
            "/auth/login",
            json={"username": "dev04", "password": "devpass4"},
        )
        self.assertEqual(old_login.status_code, 401)

        new_login = self.client.post(
            "/auth/login",
            json={"username": "dev04", "password": "newpass4"},
        )
        self.assertEqual(new_login.status_code, 200)

    def test_change_password_rejects_wrong_current_password(self):
        created = self.client.post(
            "/users",
            json={"username": "dev05", "password": "devpass5"},
            headers=self._headers,
        )
        self.assertEqual(created.status_code, 201)

        login = self.client.post(
            "/auth/login",
            json={"username": "dev05", "password": "devpass5"},
        )
        self.assertEqual(login.status_code, 200)
        user_headers = {"Authorization": f"Bearer {login.json()['token']}"}

        changed = self.client.post(
            "/auth/change-password",
            json={"current_password": "wrong-pass", "new_password": "newpass5"},
            headers=user_headers,
        )
        self.assertEqual(changed.status_code, 422)

        me = self.client.get("/auth/me", headers=user_headers)
        self.assertEqual(me.status_code, 200)

        relogin = self.client.post(
            "/auth/login",
            json={"username": "dev05", "password": "devpass5"},
        )
        self.assertEqual(relogin.status_code, 200)

    def test_admin_can_set_and_clear_user_quota(self):
        created = self.client.post(
            "/users",
            json={
                "username": "quota01",
                "password": "quotapass1",
                "max_projects": 1,
                "max_tasks": 2,
            },
            headers=self._headers,
        )
        self.assertEqual(created.status_code, 201)
        user = created.json()
        self.assertEqual(user["max_projects"], 1)
        self.assertEqual(user["max_tasks"], 2)

        updated = self.client.patch(
            f"/users/{user['id']}",
            json={"max_projects": 3, "max_tasks": None},
            headers=self._headers,
        )
        self.assertEqual(updated.status_code, 200)
        payload = updated.json()
        self.assertEqual(payload["max_projects"], 3)
        self.assertIsNone(payload["max_tasks"])

    def test_user_project_quota_blocks_create_project(self):
        created = self.client.post(
            "/users",
            json={
                "username": "quota_project_user",
                "password": "devpass1",
                "max_projects": 1,
            },
            headers=self._headers,
        )
        self.assertEqual(created.status_code, 201)

        login = self.client.post(
            "/auth/login",
            json={"username": "quota_project_user", "password": "devpass1"},
        )
        self.assertEqual(login.status_code, 200)
        user_headers = {"Authorization": f"Bearer {login.json()['token']}"}

        project_a = Path(self._tmp.name) / "quota-project-a"
        project_b = Path(self._tmp.name) / "quota-project-b"

        first = self.client.post(
            "/projects",
            json={"name": "quota-a", "path": str(project_a)},
            headers=user_headers,
        )
        self.assertEqual(first.status_code, 201)

        second = self.client.post(
            "/projects",
            json={"name": "quota-b", "path": str(project_b)},
            headers=user_headers,
        )
        self.assertEqual(second.status_code, 403)
        self.assertIn("项目创建上限", second.json().get("detail", ""))

    def test_user_task_quota_blocks_create_task(self):
        created = self.client.post(
            "/users",
            json={
                "username": "quota_task_user",
                "password": "devpass2",
                "max_tasks": 1,
            },
            headers=self._headers,
        )
        self.assertEqual(created.status_code, 201)

        login = self.client.post(
            "/auth/login",
            json={"username": "quota_task_user", "password": "devpass2"},
        )
        self.assertEqual(login.status_code, 200)
        user_headers = {"Authorization": f"Bearer {login.json()['token']}"}

        project_path = Path(self._tmp.name) / "quota-task-project"
        created_project = self.client.post(
            "/projects",
            json={"name": "quota-task", "path": str(project_path)},
            headers=user_headers,
        )
        self.assertEqual(created_project.status_code, 201)
        project_id = created_project.json()["id"]

        first = self.client.post(
            "/tasks",
            json={
                "title": "quota-task-1",
                "description": "first",
                "project_id": project_id,
            },
            headers=user_headers,
        )
        self.assertEqual(first.status_code, 201)

        second = self.client.post(
            "/tasks",
            json={
                "title": "quota-task-2",
                "description": "second",
                "project_id": project_id,
            },
            headers=user_headers,
        )
        self.assertEqual(second.status_code, 403)
        self.assertIn("任务创建上限", second.json().get("detail", ""))

    def test_login_lockout_backoff_after_five_failures(self):
        username = "admin"
        for _ in range(4):
            res = self.client.post(
                "/auth/login",
                json={"username": username, "password": "wrong-pass"},
            )
            self.assertEqual(res.status_code, 401)

        fifth = self.client.post(
            "/auth/login",
            json={"username": username, "password": "wrong-pass"},
        )
        self.assertEqual(fifth.status_code, 429)
        self.assertIn("登录失败次数过多", fifth.json().get("detail", ""))

        conn = db.get_conn()
        row_5 = conn.execute(
            "SELECT failed_login_attempts, lock_until, last_failed_login_at FROM users WHERE username=?",
            (username,),
        ).fetchone()
        conn.close()
        self.assertIsNotNone(row_5)
        self.assertEqual(int(row_5["failed_login_attempts"] or 0), 5)
        self.assertTrue(str(row_5["lock_until"] or "").strip())

        locked = self.client.post(
            "/auth/login",
            json={"username": username, "password": "admin123"},
        )
        self.assertEqual(locked.status_code, 429)

        conn = db.get_conn()
        conn.execute(
            "UPDATE users SET lock_until=? WHERE username=?",
            ((datetime.utcnow() - timedelta(seconds=1)).isoformat(), username),
        )
        conn.commit()
        conn.close()

        sixth = self.client.post(
            "/auth/login",
            json={"username": username, "password": "wrong-pass"},
        )
        self.assertEqual(sixth.status_code, 429)

        conn = db.get_conn()
        row_6 = conn.execute(
            "SELECT failed_login_attempts, lock_until, last_failed_login_at FROM users WHERE username=?",
            (username,),
        ).fetchone()
        conn.close()
        self.assertIsNotNone(row_6)
        self.assertEqual(int(row_6["failed_login_attempts"] or 0), 6)

        lock_5 = datetime.fromisoformat(row_5["lock_until"])
        fail_5 = datetime.fromisoformat(row_5["last_failed_login_at"])
        lock_6 = datetime.fromisoformat(row_6["lock_until"])
        fail_6 = datetime.fromisoformat(row_6["last_failed_login_at"])
        self.assertGreater((lock_6 - fail_6).total_seconds(), (lock_5 - fail_5).total_seconds())

    def test_successful_login_resets_failed_attempt_counters(self):
        username = "admin"
        for _ in range(2):
            res = self.client.post(
                "/auth/login",
                json={"username": username, "password": "wrong-pass"},
            )
            self.assertEqual(res.status_code, 401)

        conn = db.get_conn()
        before = conn.execute(
            "SELECT failed_login_attempts FROM users WHERE username=?",
            (username,),
        ).fetchone()
        conn.close()
        self.assertEqual(int(before["failed_login_attempts"] or 0), 2)

        ok = self.client.post(
            "/auth/login",
            json={"username": username, "password": "admin123"},
        )
        self.assertEqual(ok.status_code, 200)

        conn = db.get_conn()
        after = conn.execute(
            "SELECT failed_login_attempts, lock_until, last_failed_login_at FROM users WHERE username=?",
            (username,),
        ).fetchone()
        conn.close()
        self.assertEqual(int(after["failed_login_attempts"] or 0), 0)
        self.assertFalse(str(after["lock_until"] or "").strip())
        self.assertFalse(str(after["last_failed_login_at"] or "").strip())

    def test_patch_rejects_direct_status_change(self):
        task = self._create_task(status="todo")
        res = self.client.patch(
            f"/tasks/{task['id']}",
            json={"status": "in_review"},
            headers=self._headers,
        )
        self.assertEqual(res.status_code, 403)

    def test_transition_rejects_invalid_status_flow(self):
        task = self._create_task(status="todo")
        res = self.client.post(
            f"/tasks/{task['id']}/transition",
            json={"fields": {"status": "completed"}},
            headers=self._headers,
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
            headers=self._headers,
        )
        self.assertEqual(res.status_code, 200)
        claimed = res.json()["task"]
        self.assertIsNotNone(claimed)
        self.assertEqual(claimed["id"], task["id"])
        self.assertEqual(claimed["status"], "in_progress")

    def test_claim_rejects_missing_auth(self):
        self._create_task(status="todo", assigned_agent="developer", dev_agent="developer")
        res = self.client.post(
            "/tasks/claim",
            json={
                "status": "todo",
                "working_status": "in_progress",
                "agent": "developer",
                "agent_key": "developer",
                "respect_assignment": True,
                "project_id": self.project["id"],
            },
        )
        self.assertEqual(res.status_code, 401)

    def test_claim_rejects_missing_project_scope_when_strict(self):
        self._create_task(status="todo", assigned_agent="developer", dev_agent="developer")
        res = self.client.post(
            "/tasks/claim",
            json={
                "status": "todo",
                "working_status": "in_progress",
                "agent": "developer",
                "agent_key": "developer",
                "respect_assignment": True,
            },
            headers=self._headers,
        )
        self.assertEqual(res.status_code, 422)

    def test_claim_accepts_agent_token(self):
        task = self._create_task(status="todo", assigned_agent="developer", dev_agent="developer")
        res = self.client.post(
            "/tasks/claim",
            json={
                "status": "todo",
                "working_status": "in_progress",
                "agent": "developer",
                "agent_key": "developer",
                "respect_assignment": True,
                "project_id": self.project["id"],
            },
            headers=self._agent_headers,
        )
        self.assertEqual(res.status_code, 200)
        claimed = res.json()["task"]
        self.assertIsNotNone(claimed)
        self.assertEqual(claimed["id"], task["id"])

    def test_create_task_accepts_agent_token(self):
        res = self.client.post(
            "/tasks",
            json={
                "title": "agent-created-task",
                "description": "created by agent token",
                "project_id": self.project["id"],
                "status": "todo",
            },
            headers=self._agent_headers,
        )
        self.assertEqual(res.status_code, 201)
        created = res.json()
        self.assertEqual(created["title"], "agent-created-task")
        self.assertEqual(created["project_id"], self.project["id"])
        self.assertEqual(created["status"], "todo")

    def test_user_created_task_without_assignee_enters_triage(self):
        res = self.client.post(
            "/tasks",
            json={
                "title": "user-created-task",
                "description": "no assignee",
                "project_id": self.project["id"],
            },
            headers=self._headers,
        )
        self.assertEqual(res.status_code, 201)
        created = res.json()
        self.assertEqual(created["status"], "triage")
        self.assertIsNone(created.get("assigned_agent"))

    def test_user_created_task_with_todo_assignee_skips_triage(self):
        res = self.client.post(
            "/tasks",
            json={
                "title": "fast lane",
                "description": "assigned to developer",
                "project_id": self.project["id"],
                "assigned_agent": "developer",
            },
            headers=self._headers,
        )
        self.assertEqual(res.status_code, 201)
        created = res.json()
        self.assertEqual(created["status"], "todo")
        self.assertEqual(created["assigned_agent"], "developer")
        self.assertEqual(created["dev_agent"], "developer")

    def test_user_created_task_can_disable_review(self):
        res = self.client.post(
            "/tasks",
            json={
                "title": "no review task",
                "description": "skip reviewer",
                "project_id": self.project["id"],
                "assigned_agent": "developer",
                "review_enabled": False,
            },
            headers=self._headers,
        )
        self.assertEqual(res.status_code, 201)
        created = res.json()
        self.assertEqual(created["status"], "todo")
        self.assertIn(created.get("review_enabled"), [0, False])

    def test_claim_rejects_bad_agent_token(self):
        self._create_task(status="todo", assigned_agent="developer", dev_agent="developer")
        res = self.client.post(
            "/tasks/claim",
            json={
                "status": "todo",
                "working_status": "in_progress",
                "agent": "developer",
                "agent_key": "developer",
                "respect_assignment": True,
                "project_id": self.project["id"],
            },
            headers=self._bad_agent_headers,
        )
        self.assertEqual(res.status_code, 401)

    def test_claim_returns_lease_and_renew_extends_it(self):
        task = self._create_task(status="todo", assigned_agent="developer", dev_agent="developer")
        res = self.client.post(
            "/tasks/claim",
            json={
                "status": "todo",
                "working_status": "in_progress",
                "agent": "developer",
                "agent_key": "developer",
                "respect_assignment": True,
                "project_id": self.project["id"],
                "lease_ttl_secs": 180,
            },
            headers=self._headers,
        )
        self.assertEqual(res.status_code, 200)
        claimed = res.json()["task"]
        self.assertTrue(str(claimed.get("claim_run_id") or "").strip())
        self.assertTrue(str(claimed.get("lease_token") or "").strip())
        self.assertTrue(str(claimed.get("lease_expires_at") or "").strip())

        renew = self.client.post(
            f"/tasks/{task['id']}/lease/renew",
            json={
                "run_id": claimed["claim_run_id"],
                "lease_token": claimed["lease_token"],
                "lease_ttl_secs": 300,
            },
            headers=self._headers,
        )
        self.assertEqual(renew.status_code, 200)
        renewed_at = str(renew.json().get("lease_expires_at") or "").strip()
        self.assertTrue(renewed_at)
        self.assertGreaterEqual(renewed_at, str(claimed["lease_expires_at"]))

    def test_transition_rejects_stale_lease_fence(self):
        task = self._create_task(status="todo", assigned_agent="developer", dev_agent="developer")
        claimed = self.client.post(
            "/tasks/claim",
            json={
                "status": "todo",
                "working_status": "in_progress",
                "agent": "developer",
                "agent_key": "developer",
                "respect_assignment": True,
                "project_id": self.project["id"],
            },
            headers=self._headers,
        ).json()["task"]
        bad = self.client.post(
            f"/tasks/{task['id']}/transition",
            json={
                "fields": {"status": "todo", "assignee": None},
                "expected_run_id": "stale-run-id",
                "expected_lease_token": "stale-lease-token",
            },
            headers=self._headers,
        )
        self.assertEqual(bad.status_code, 409)

        good = self.client.post(
            f"/tasks/{task['id']}/transition",
            json={
                "fields": {"status": "todo", "assignee": None},
                "expected_run_id": claimed["claim_run_id"],
                "expected_lease_token": claimed["lease_token"],
            },
            headers=self._headers,
        )
        self.assertEqual(good.status_code, 200)
        self.assertEqual(good.json()["task"]["status"], "todo")

    def test_transition_accepts_related_history_commits_as_commit_evidence(self):
        task = self._create_task(status="in_progress", assigned_agent="developer", dev_agent="developer")
        res = self.client.post(
            f"/tasks/{task['id']}/transition",
            json={
                "fields": {"status": "in_review", "assignee": None},
                "handoff": {
                    "stage": "dev_to_review",
                    "from_agent": "developer",
                    "to_agent": "reviewer",
                    "status_from": "in_progress",
                    "status_to": "in_review",
                    "title": "开发交接审查（历史证据）",
                    "summary": "无新增提交，附带历史提交证据",
                    "conclusion": "使用历史提交证据继续审查",
                    "payload": {
                        "has_commit": True,
                        "related_history_commits": [
                            {"hash": "abc1234", "short": "abc1234", "subject": "feat: done already"}
                        ],
                    },
                },
            },
            headers=self._headers,
        )
        self.assertEqual(res.status_code, 200)
        data = res.json()
        self.assertEqual(data["task"]["status"], "in_review")
        self.assertEqual(data["handoff"]["commit_hash"], "abc1234")

    def test_transition_persists_patchset_and_exposes_patchset_list(self):
        task = self._create_task(status="in_progress", assigned_agent="developer", dev_agent="developer")
        patchset = {
            "id": "ps-api-1",
            "source_branch": "agent/developer/task-1",
            "base_sha": "a" * 40,
            "head_sha": "b" * 40,
            "commit_count": 2,
            "commit_list": [
                {"hash": "b" * 40, "short": "bbbbbbb", "subject": "feat: ship it"},
                {"hash": "c" * 40, "short": "ccccccc", "subject": "fix: polish"},
            ],
            "changed_files": [
                {"status": "M", "path": "index.html"},
                {"status": "A", "path": ".opc/delivery.json"},
            ],
            "artifact_manifest": {
                "path": ".opc/delivery.json",
                "keys": ["deliverables", "test_or_preview_evidence"],
            },
            "diff_stat": " index.html | 2 +-",
            "status": "submitted",
            "worktree_clean": False,
        }
        res = self.client.post(
            f"/tasks/{task['id']}/transition",
            json={
                "fields": {
                    "status": "in_review",
                    "assignee": None,
                    "current_patchset_id": "ps-api-1",
                    "current_patchset_status": "submitted",
                },
                "handoff": {
                    "stage": "dev_to_review",
                    "from_agent": "developer",
                    "to_agent": "reviewer",
                    "status_from": "in_progress",
                    "status_to": "in_review",
                    "title": "开发交接审查",
                    "summary": "提交 patchset 进入审查",
                    "commit_hash": "b" * 40,
                    "conclusion": "进入审查",
                    "payload": {"patchset": patchset},
                },
            },
            headers=self._headers,
        )
        self.assertEqual(res.status_code, 200)
        data = res.json()
        self.assertEqual(data["task"]["current_patchset_id"], "ps-api-1")
        self.assertEqual(data["task"]["current_patchset_status"], "submitted")
        self.assertEqual(data["handoff"]["payload"]["patchset"]["head_sha"], "b" * 40)

        patchsets = self.client.get(
            f"/tasks/{task['id']}/patchsets",
            headers=self._headers,
        )
        self.assertEqual(patchsets.status_code, 200)
        listed = patchsets.json()
        self.assertEqual(len(listed), 1)
        self.assertEqual(listed[0]["id"], "ps-api-1")
        self.assertEqual(listed[0]["status"], "submitted")
        self.assertFalse(listed[0]["worktree_clean"])
        self.assertEqual(listed[0]["changed_files"][0]["path"], "index.html")
        self.assertEqual(listed[0]["artifact_manifest"]["path"], ".opc/delivery.json")

    def test_runtime_patchset_metrics_returns_queue_and_rate_summary(self):
        task = self._create_task(
            status="approved",
            assigned_agent="manager",
            dev_agent="developer",
        )
        patchset = {
            "id": "ps-metrics-1",
            "source_branch": "agent/developer/task-metrics",
            "base_sha": "a" * 40,
            "head_sha": "b" * 40,
            "commit_count": 1,
            "commit_list": [{"hash": "b" * 40, "short": "bbbbbbb", "subject": "feat: metric"}],
            "diff_stat": " index.html | 2 +-",
            "status": "approved",
            "queue_status": "queued",
            "approved_at": "2026-03-06T00:00:00",
            "queued_at": "2026-03-06T00:00:00",
            "reviewed_main_sha": "c" * 40,
            "worktree_clean": True,
        }
        res = self.client.post(
            f"/tasks/{task['id']}/transition",
            json={
                "fields": {
                    "status": "approved",
                    "assignee": None,
                    "current_patchset_id": "ps-metrics-1",
                    "current_patchset_status": "approved",
                },
                "handoff": {
                    "stage": "review_to_manager",
                    "from_agent": "reviewer",
                    "to_agent": "manager",
                    "status_from": "in_review",
                    "status_to": "approved",
                    "title": "审查通过，交接合并",
                    "summary": "ok",
                    "commit_hash": "b" * 40,
                    "conclusion": "ok",
                    "payload": {"patchset": patchset},
                },
            },
            headers=self._headers,
        )
        self.assertEqual(res.status_code, 200)

        metrics = self.client.get(
            f"/runtime/patchset-metrics?project_id={task['project_id']}",
            headers=self._headers,
        )
        self.assertEqual(metrics.status_code, 200)
        body = metrics.json()
        self.assertEqual(body["flags"]["task_delivery_model"], "patchset")
        self.assertEqual(body["metrics"]["queued_patchsets"], 1)
        self.assertEqual(body["metrics"]["windows"]["last_24h"]["queued_patchsets"], 1)
        self.assertIn("manager_return_rate", body["metrics"])
        self.assertIn("review_cycles_per_task", body["metrics"])

    def test_patchset_upsert_endpoint_updates_queue_status_without_touching_task_refs(self):
        task = self._create_task(status="approved", assigned_agent="manager", dev_agent="developer")
        db.save_task_patchset(
            task["id"],
            {
                "id": "ps-queue-1",
                "task_id": task["id"],
                "base_sha": "a" * 40,
                "head_sha": "b" * 40,
                "status": "approved",
                "queue_status": "queued",
            },
            update_task_refs=True,
        )
        res = self.client.post(
            f"/tasks/{task['id']}/patchsets",
            json={
                "patchset": {
                    "id": "ps-queue-1",
                    "task_id": task["id"],
                    "base_sha": "a" * 40,
                    "head_sha": "b" * 40,
                    "status": "approved",
                    "queue_status": "processing",
                    "queue_reason": "",
                },
                "update_task_refs": False,
            },
            headers=self._agent_headers,
        )
        self.assertEqual(res.status_code, 201)
        body = res.json()
        self.assertEqual(body["queue_status"], "processing")
        task_after = db.get_task(task["id"])
        self.assertEqual(task_after["current_patchset_status"], "approved")

    def test_transition_accepts_agent_token(self):
        task = self._create_task(status="todo")
        res = self.client.post(
            f"/tasks/{task['id']}/transition",
            json={"fields": {"status": "todo", "assignee": None}},
            headers=self._agent_headers,
        )
        self.assertEqual(res.status_code, 200)
        self.assertEqual(res.json()["task"]["status"], "todo")

    def test_transition_allows_description_update(self):
        task = self._create_task(status="triage")
        refined = (
            "## 任务目标\n- 完成 API 鉴权链路校验\n\n"
            "## 范围\n- claim/transition/status/output 接口\n\n"
            "## 验收标准\n- [ ] 自动化测试覆盖关键路径"
        )
        res = self.client.post(
            f"/tasks/{task['id']}/transition",
            json={"fields": {"description": refined}},
            headers=self._agent_headers,
        )
        self.assertEqual(res.status_code, 200)
        self.assertEqual(res.json()["task"]["description"], refined)

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
            headers=self._headers,
        )
        self.assertEqual(res.status_code, 409)

    def test_claim_deletes_task_when_project_path_missing(self):
        missing_project_path = Path(self._tmp.name) / "missing-project-dir"
        missing_project = db.create_project("missing-project", str(missing_project_path))
        task = db.create_task(
            title="missing-path-task",
            description="should be removed",
            project_id=missing_project["id"],
            assigned_agent="developer",
            dev_agent="developer",
            status="todo",
        )
        res = self.client.post(
            "/tasks/claim",
            json={
                "status": "todo",
                "working_status": "in_progress",
                "agent": "developer",
                "agent_key": "developer",
                "respect_assignment": True,
                "project_id": missing_project["id"],
            },
            headers=self._headers,
        )
        self.assertEqual(res.status_code, 200)
        self.assertIsNone(res.json().get("task"))
        self.assertIsNone(db.get_task(task["id"]))

    def test_accept_action_completes_task(self):
        task = self._create_task(status="pending_acceptance")
        res = self.client.post(
            f"/tasks/{task['id']}/actions",
            json={"action": "accept"},
            headers=self._headers,
        )
        self.assertEqual(res.status_code, 200)
        data = res.json()
        self.assertEqual(data["task"]["status"], "completed")

    def test_accept_action_schedules_workspace_cleanup(self):
        task = self._create_task(status="pending_acceptance")
        with mock.patch.object(app_module, "_schedule_task_workspace_cleanup") as schedule:
            res = self.client.post(
                f"/tasks/{task['id']}/actions",
                json={"action": "accept"},
                headers=self._headers,
            )
        self.assertEqual(res.status_code, 200)
        schedule.assert_called_once()
        cleaned_task = schedule.call_args.args[0]
        self.assertEqual(cleaned_task["id"], task["id"])
        self.assertEqual(cleaned_task["status"], "completed")

    def test_reject_action_routes_to_dev(self):
        task = self._create_task(status="pending_acceptance")
        res = self.client.post(
            f"/tasks/{task['id']}/actions",
            json={"action": "reject", "feedback": "验收不通过，请修复边界情况"},
            headers=self._headers,
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
            headers=self._headers,
        )
        self.assertEqual(res.status_code, 200)
        data = res.json()
        self.assertEqual(data["task"]["status"], "in_review")
        self.assertEqual(data["task"]["assigned_agent"], "reviewer")

    def test_retry_blocked_action_resumes_generic_delivery_block(self):
        self._create_custom_agent(key="writer")
        task = self._create_task(
            status="blocked",
            assigned_agent="writer",
        )
        db.add_handoff(
            task["id"],
            stage="writer_delivery_blocked",
            from_agent="writer",
            to_agent="writer",
            status_from="todo",
            status_to="blocked",
            title="Writer 连续未完成交付",
            summary="连续 3 次未产出可交付文件，任务转为 blocked",
            conclusion="Writer 连续 3 次未完成交付，请人工检查后再重试",
            payload={
                "delivery_blocked": True,
                "resume_status": "todo",
                "resume_assigned_agent": "writer",
                "latest_failure_stage": "writer_no_progress",
            },
        )
        res = self.client.post(
            f"/tasks/{task['id']}/actions",
            json={"action": "retry_blocked"},
            headers=self._headers,
        )
        self.assertEqual(res.status_code, 200)
        data = res.json()
        self.assertEqual(data["task"]["status"], "todo")
        self.assertEqual(data["task"]["assigned_agent"], "writer")

    def test_decompose_action_moves_todo_to_decompose(self):
        task = self._create_task(status="todo")
        res = self.client.post(
            f"/tasks/{task['id']}/actions",
            json={"action": "decompose"},
            headers=self._headers,
        )
        self.assertEqual(res.status_code, 200)
        self.assertEqual(res.json()["task"]["status"], "decompose")

    def test_archive_action_only_for_completed(self):
        task = self._create_task(status="completed")
        res = self.client.post(
            f"/tasks/{task['id']}/actions",
            json={"action": "archive"},
            headers=self._headers,
        )
        self.assertEqual(res.status_code, 200)
        self.assertEqual(int(res.json()["task"]["archived"]), 1)

        not_done = self._create_task(status="todo")
        bad = self.client.post(
            f"/tasks/{not_done['id']}/actions",
            json={"action": "archive"},
            headers=self._headers,
        )
        self.assertEqual(bad.status_code, 409)

    def test_force_action_allows_admin_override_status_gate(self):
        task = self._create_task(status="todo")
        res = self.client.post(
            f"/tasks/{task['id']}/actions",
            json={"action": "accept", "force": True},
            headers=self._headers,
        )
        self.assertEqual(res.status_code, 200)
        data = res.json()
        self.assertEqual(data["task"]["status"], "completed")
        self.assertTrue(data.get("forced"))

    def test_add_handoff_without_fence_allows_active_lease(self):
        task = self._create_task(status="todo", assigned_agent="developer", dev_agent="developer")
        claim = self.client.post(
            "/tasks/claim",
            json={
                "status": "todo",
                "working_status": "in_progress",
                "agent": "developer",
                "agent_key": "developer",
                "respect_assignment": True,
                "project_id": self.project["id"],
            },
            headers=self._headers,
        )
        self.assertEqual(claim.status_code, 200)
        claimed = claim.json()["task"]
        self.assertEqual(claimed["status"], "in_progress")

        res = self.client.post(
            f"/tasks/{task['id']}/handoffs",
            json={
                "stage": "manual_repair_note",
                "from_agent": "system",
                "to_agent": "developer",
                "status_from": "in_progress",
                "status_to": "in_progress",
                "title": "人工记录",
                "summary": "无需租约 fence 也允许补录交接说明",
                "conclusion": "交接补录完成",
                "payload": {"note": "recovery"},
            },
            headers=self._headers,
        )
        self.assertEqual(res.status_code, 201)
        self.assertEqual(res.json()["stage"], "manual_repair_note")

    def test_task_files_branch_falls_back_to_assignee(self):
        task = self._create_task(status="in_progress")
        db.update_task(
            task["id"],
            assignee="asmo-dev",
            assigned_agent=None,
            dev_agent=None,
        )
        res = self.client.get(f"/tasks/{task['id']}/files", headers=self._headers)
        self.assertEqual(res.status_code, 200)
        data = res.json()
        self.assertEqual(data["branch"], f"agent/asmo-dev/{task['id']}")

    def test_cancel_schedules_workspace_cleanup_for_cancelled_tasks(self):
        parent = self._create_task(status="todo")
        child = db.create_task(
            title="child-task",
            description="subtask",
            project_id=self.project["id"],
            parent_task_id=parent["id"],
            assigned_agent="developer",
            dev_agent="developer",
            status="todo",
        )
        with mock.patch.object(app_module, "_schedule_task_workspace_cleanup") as schedule:
            res = self.client.post(
                f"/tasks/{parent['id']}/cancel",
                json={"include_subtasks": True},
                headers=self._headers,
            )
        self.assertEqual(res.status_code, 200)
        self.assertGreaterEqual(schedule.call_count, 2)
        task_ids = {call.args[0]["id"] for call in schedule.call_args_list}
        self.assertIn(parent["id"], task_ids)
        self.assertIn(child["id"], task_ids)

    def test_runtime_workspace_cleanup_endpoint_returns_metrics(self):
        res = self.client.get("/runtime/workspace-cleanup", headers=self._headers)
        self.assertEqual(res.status_code, 200)
        body = res.json()
        self.assertIn("config", body)
        self.assertIn("metrics", body)
        self.assertIn("recent_events", body)
        self.assertIn("inflight_task_ids", body)

    def test_runtime_workspace_cleanup_sweep_schedules_terminal_tasks(self):
        done = self._create_task(status="completed")
        cancelled = self._create_task(status="cancelled")
        with mock.patch.object(app_module, "_schedule_task_workspace_cleanup") as schedule:
            res = self.client.post(
                "/runtime/workspace-cleanup/sweep?max_tasks=20",
                headers=self._headers,
            )
        self.assertEqual(res.status_code, 200)
        ids = {call.args[0]["id"] for call in schedule.call_args_list}
        self.assertIn(done["id"], ids)
        self.assertIn(cancelled["id"], ids)

    def test_task_log_refreshes_agent_last_output_timestamp(self):
        task = self._create_task(status="in_progress")
        app_module.AGENT_STATUS["developer"] = {
            "status": "busy",
            "task": task["title"],
            "updated_at": "",
            "last_output_at": "",
        }
        res = self.client.post(
            f"/tasks/{task['id']}/logs",
            json={"agent": "developer", "message": "⏳ 仍在工作中... 已运行 45s"},
            headers=self._headers,
        )
        self.assertEqual(res.status_code, 201)
        self.assertTrue(app_module.AGENT_STATUS["developer"]["last_output_at"])

    def test_task_log_rejects_bad_agent_token(self):
        task = self._create_task(status="in_progress")
        res = self.client.post(
            f"/tasks/{task['id']}/logs",
            json={"agent": "developer", "message": "hello"},
            headers=self._bad_agent_headers,
        )
        self.assertEqual(res.status_code, 401)

    def test_agent_status_rejects_missing_auth(self):
        res = self.client.post(
            "/agents/developer/status",
            json={"status": "idle", "task": ""},
        )
        self.assertEqual(res.status_code, 401)

    def test_agent_status_busy_downgrades_to_idle_when_task_not_owned(self):
        task = self._create_task(status="pending_acceptance", assigned_agent="developer", dev_agent="developer")
        res = self.client.post(
            "/agents/developer/status",
            json={"status": "busy", "task": task["title"], "task_id": task["id"]},
            headers=self._headers,
        )
        self.assertEqual(res.status_code, 200)
        state = app_module.AGENT_STATUS["developer"]
        self.assertEqual(state["status"], "idle")
        self.assertEqual(state["task_id"], "")

    def test_agent_status_rejects_bad_agent_token(self):
        res = self.client.post(
            "/agents/developer/status",
            json={"status": "idle", "task": ""},
            headers=self._bad_agent_headers,
        )
        self.assertEqual(res.status_code, 401)

    def test_agent_status_busy_kept_when_task_is_validly_claimed(self):
        task = self._create_task(status="todo", assigned_agent="developer", dev_agent="developer")
        claim = self.client.post(
            "/tasks/claim",
            json={
                "status": "todo",
                "working_status": "in_progress",
                "agent": "developer",
                "agent_key": "developer",
                "respect_assignment": True,
                "project_id": self.project["id"],
            },
            headers=self._headers,
        )
        self.assertEqual(claim.status_code, 200)
        claimed = claim.json()["task"]
        self.assertIsNotNone(claimed)
        res = self.client.post(
            "/agents/developer/status",
            json={
                "status": "busy",
                "task": task["title"],
                "task_id": task["id"],
                "run_id": claimed["claim_run_id"],
                "lease_token": claimed["lease_token"],
            },
            headers=self._headers,
        )
        self.assertEqual(res.status_code, 200)
        state = app_module.AGENT_STATUS["developer"]
        self.assertEqual(state["status"], "busy")
        self.assertEqual(state["task_id"], task["id"])

    def test_agent_output_accepts_agent_token(self):
        res = self.client.post(
            "/agents/developer/output",
            json={"line": "ok", "event": "line"},
            headers=self._agent_headers,
        )
        self.assertEqual(res.status_code, 200)

    def test_agent_output_rejects_missing_auth(self):
        res = self.client.post(
            "/agents/developer/output",
            json={"line": "ok", "event": "line"},
        )
        self.assertEqual(res.status_code, 401)

    def test_agent_output_rejects_bad_agent_token(self):
        res = self.client.post(
            "/agents/developer/output",
            json={"line": "ok", "event": "line"},
            headers=self._bad_agent_headers,
        )
        self.assertEqual(res.status_code, 401)

    def test_agent_output_persists_project_id_from_task(self):
        task = self._create_task(status="in_progress", assigned_agent="developer", dev_agent="developer")
        res = self.client.post(
            "/agents/developer/output",
            json={"line": "ok", "event": "line", "task_id": task["id"]},
            headers=self._agent_headers,
        )
        self.assertEqual(res.status_code, 200)
        entries = db.get_agent_output_entries("developer", limit=1)
        self.assertEqual(len(entries), 1)
        self.assertEqual(entries[0]["project_id"], self.project["id"])

    def test_agent_outputs_endpoint_can_scope_project(self):
        other_path = Path(self._tmp.name) / "other-project"
        other_path.mkdir(parents=True, exist_ok=True)
        other_project = db.create_project("other-project", str(other_path))
        task_a = self._create_task(status="in_progress", assigned_agent="developer", dev_agent="developer")
        task_b = db.create_task(
            title="other-task",
            description="other project output",
            project_id=other_project["id"],
            status="in_progress",
            assigned_agent="developer",
            dev_agent="developer",
        )
        self.client.post(
            "/agents/developer/output",
            json={"line": "line-a", "event": "line", "task_id": task_a["id"]},
            headers=self._agent_headers,
        )
        self.client.post(
            "/agents/developer/output",
            json={"line": "line-b", "event": "line", "task_id": task_b["id"]},
            headers=self._agent_headers,
        )

        res = self.client.get(
            f"/agents/outputs?project_id={self.project['id']}",
            headers=self._headers,
        )
        self.assertEqual(res.status_code, 200)
        data = res.json()
        self.assertIn("developer", data)
        lines = data["developer"]["lines"]
        self.assertGreaterEqual(len(lines), 1)
        self.assertTrue(all(line["project_id"] == self.project["id"] for line in lines))
        self.assertIn("line-a", [line["line"] for line in lines])
        self.assertNotIn("line-b", [line["line"] for line in lines])

    def test_websocket_project_scope_filters_agent_output_stream(self):
        other_path = Path(self._tmp.name) / "other-project"
        other_path.mkdir(parents=True, exist_ok=True)
        other_project = db.create_project("other-project", str(other_path))
        task_a = self._create_task(status="in_progress", assigned_agent="developer", dev_agent="developer")
        task_b = db.create_task(
            title="other-task",
            description="other project output",
            project_id=other_project["id"],
            status="in_progress",
            assigned_agent="developer",
            dev_agent="developer",
        )
        token = self._headers["Authorization"].split(" ", 1)[1]
        with self.client.websocket_connect(f"/ws?token={token}&project_id={self.project['id']}") as ws:
            init = ws.receive_json()
            self.assertEqual(init["event"], "init")
            self.assertTrue(all(t["project_id"] == self.project["id"] for t in init["tasks"]))

            self.client.post(
                "/agents/developer/output",
                json={"line": "line-b", "event": "line", "task_id": task_b["id"]},
                headers=self._agent_headers,
            )
            self.client.post(
                "/agents/developer/output",
                json={"line": "line-a", "event": "line", "task_id": task_a["id"]},
                headers=self._agent_headers,
            )

            for _ in range(6):
                msg = ws.receive_json()
                if msg.get("event") != "agent_output":
                    continue
                self.assertEqual(msg.get("project_id"), self.project["id"])
                payload = msg.get("output") if isinstance(msg.get("output"), dict) else msg
                self.assertEqual(payload.get("line"), "line-a")
                break
            else:
                self.fail("expected scoped agent_output event for current project")

    def test_worker_alias_status_and_output_use_agent_key(self):
        task = self._create_task(status="todo", assigned_agent="developer", dev_agent="developer")
        claimed = self.client.post(
            "/tasks/claim",
            json={
                "status": "todo",
                "working_status": "in_progress",
                "agent": "developer",
                "agent_key": "developer",
                "respect_assignment": True,
                "project_id": self.project["id"],
            },
            headers=self._headers,
        ).json()["task"]
        self.assertIsNotNone(claimed)

        worker_id = "developer__runtime__w1"
        status = self.client.post(
            "/agents/dev-worker/status",
            json={
                "status": "busy",
                "task": task["title"],
                "agent_key": "developer",
                "worker_id": worker_id,
                "project_id": self.project["id"],
                "task_id": task["id"],
                "run_id": claimed["claim_run_id"],
                "lease_token": claimed["lease_token"],
            },
            headers=self._agent_headers,
        )
        self.assertEqual(status.status_code, 200)
        state = app_module.AGENT_STATUS[worker_id]
        self.assertEqual(state["agent_key"], "developer")
        self.assertEqual(state["status"], "busy")

        out = self.client.post(
            "/agents/dev-worker/output",
            json={
                "line": "hello worker",
                "event": "line",
                "agent_key": "developer",
                "worker_id": worker_id,
                "project_id": self.project["id"],
                "task_id": task["id"],
            },
            headers=self._agent_headers,
        )
        self.assertEqual(out.status_code, 200)
        entries = db.get_agent_output_entries(worker_id, limit=1)
        self.assertEqual(len(entries), 1)
        self.assertEqual(entries[0]["project_id"], self.project["id"])

    def test_runtime_projects_endpoint_supports_agent_token(self):
        self._create_task(status="todo", assigned_agent="developer", dev_agent="developer")
        res = self.client.get("/runtime/projects", headers=self._agent_headers)
        self.assertEqual(res.status_code, 200)
        projects = res.json()
        self.assertTrue(any(str(p.get("id")) == self.project["id"] for p in projects))

    def test_delete_project_removes_project_tasks_and_outputs(self):
        task = self._create_task(status="todo", assigned_agent="developer", dev_agent="developer")
        out = self.client.post(
            "/agents/developer/output",
            json={"line": "to-be-deleted", "event": "line", "task_id": task["id"]},
            headers=self._agent_headers,
        )
        self.assertEqual(out.status_code, 200)

        deleted = self.client.delete(f"/projects/{self.project['id']}", headers=self._headers)
        self.assertEqual(deleted.status_code, 200)
        self.assertEqual(deleted.json().get("ok"), True)

        self.assertIsNone(db.get_project(self.project["id"]))
        self.assertIsNone(db.get_task(task["id"]))
        outputs = db.get_agent_output_entries("developer", limit=10)
        self.assertEqual(len(outputs), 0)

    def test_delete_project_rejects_when_claimed_task_exists(self):
        task = self._create_task(status="in_progress", assigned_agent="developer", dev_agent="developer")
        db.update_task(task["id"], assignee="developer")

        res = self.client.delete(f"/projects/{self.project['id']}", headers=self._headers)
        self.assertEqual(res.status_code, 409)
        self.assertIn("进行中的任务", res.text)
        self.assertIsNotNone(db.get_project(self.project["id"]))

    def test_delete_project_can_delete_project_directory_when_requested(self):
        path = Path(self._tmp.name) / "to-remove-project"
        path.mkdir(parents=True, exist_ok=True)
        nested = path / "README.md"
        nested.write_text("to be removed", encoding="utf-8")
        project = db.create_project("remove-with-files", str(path))
        trash_dir = Path(self._tmp.name) / ".trash"

        def _fake_move(src: Path) -> Path:
            trash_dir.mkdir(parents=True, exist_ok=True)
            target = trash_dir / src.name
            shutil.move(str(src), str(target))
            return target

        with mock.patch.object(app_module, "_move_path_to_trash", side_effect=_fake_move):
            res = self.client.delete(
                f"/projects/{project['id']}?delete_files=1",
                headers=self._headers,
            )
        self.assertEqual(res.status_code, 200)
        data = res.json()
        self.assertEqual(data.get("ok"), True)
        self.assertEqual(data.get("files_deleted"), True)
        self.assertEqual(data.get("files_mode"), "trash")
        self.assertEqual(data.get("files_destination"), str(trash_dir / path.name))
        self.assertIsNone(db.get_project(project["id"]))
        self.assertFalse(path.exists())
        self.assertTrue((trash_dir / path.name).exists())

    def test_delete_project_can_permanently_delete_project_directory(self):
        path = Path(self._tmp.name) / "to-permanent-delete-project"
        path.mkdir(parents=True, exist_ok=True)
        (path / "README.md").write_text("to be removed permanently", encoding="utf-8")
        project = db.create_project("remove-permanent", str(path))

        res = self.client.delete(
            f"/projects/{project['id']}?delete_files=1&delete_permanently=1",
            headers=self._headers,
        )
        self.assertEqual(res.status_code, 200)
        data = res.json()
        self.assertEqual(data.get("ok"), True)
        self.assertEqual(data.get("files_deleted"), True)
        self.assertEqual(data.get("files_mode"), "permanent")
        self.assertIsNone(db.get_project(project["id"]))
        self.assertFalse(path.exists())

    def test_delete_agent_type_cleans_outputs_and_task_references(self):
        key = "asmo-dev"
        self._create_custom_agent(key)
        queued = self._create_task(status="needs_changes", assigned_agent=key, dev_agent=key)
        running = self._create_task(status="coding", assigned_agent=key, dev_agent=key)
        db.update_task(
            running["id"],
            assignee=key,
            claim_run_id="run-1",
            lease_token="lease-1",
            lease_expires_at="2099-01-01T00:00:00",
        )
        out = self.client.post(
            f"/agents/{key}/output",
            json={"line": "hello", "event": "line", "task_id": queued["id"]},
            headers=self._agent_headers,
        )
        self.assertEqual(out.status_code, 200)
        self.assertGreater(len(db.get_agent_output_entries(key, limit=10)), 0)

        deleted = self.client.delete(f"/agent-types/{key}", headers=self._headers)
        self.assertEqual(deleted.status_code, 200)

        queued_after = db.get_task(queued["id"])
        self.assertIsNotNone(queued_after)
        self.assertIsNone(queued_after["assigned_agent"])
        self.assertIsNone(queued_after["dev_agent"])

        running_after = db.get_task(running["id"])
        self.assertIsNotNone(running_after)
        self.assertEqual(running_after["status"], "todo")
        self.assertIsNone(running_after["assignee"])
        self.assertIsNone(running_after["claim_run_id"])
        self.assertIsNone(running_after["lease_token"])

        self.assertEqual(db.get_agent_output_entries(key, limit=10), [])

    def test_deleted_agent_cannot_report_output_or_status(self):
        key = "asmo-dev"
        self._create_custom_agent(key)
        deleted = self.client.delete(f"/agent-types/{key}", headers=self._headers)
        self.assertEqual(deleted.status_code, 200)

        out = self.client.post(
            f"/agents/{key}/output",
            json={"line": "after delete", "event": "line"},
            headers=self._agent_headers,
        )
        self.assertEqual(out.status_code, 404)

        status = self.client.post(
            f"/agents/{key}/status",
            json={"status": "idle", "task": ""},
            headers=self._agent_headers,
        )
        self.assertEqual(status.status_code, 404)

    def test_agent_type_api_exposes_runtime_class_metadata(self):
        self._create_custom_agent("asmo-dev", runtime_profile="developer")

        listed = self.client.get("/agent-types", headers=self._headers)
        self.assertEqual(listed.status_code, 200)
        items = {row["key"]: row for row in listed.json()}

        self.assertEqual(items["developer"]["runtime_class"], "GenericAgent")
        self.assertEqual(items["developer"]["runtime_family"], "generic")
        self.assertEqual(items["developer"]["runtime_profile"], "developer")
        self.assertEqual(items["product_manager"]["runtime_class"], "GenericAgent")
        self.assertEqual(items["product_manager"]["runtime_family"], "generic")
        self.assertEqual(items["asmo-dev"]["runtime_class"], "GenericAgent")
        self.assertEqual(items["asmo-dev"]["runtime_family"], "generic")
        self.assertEqual(items["asmo-dev"]["runtime_profile"], "developer")
        self.assertIn("developer 策略", items["asmo-dev"]["runtime_summary"])

        detail = self.client.get("/agent-types/developer", headers=self._headers)
        self.assertEqual(detail.status_code, 200)
        self.assertEqual(detail.json()["runtime_class"], "GenericAgent")
        self.assertEqual(detail.json()["runtime_profile"], "developer")
        self.assertIn("developer 策略", detail.json()["runtime_summary"])

    def test_alert_accepts_agent_token(self):
        res = self.client.post(
            "/alerts",
            json={
                "agent": "developer",
                "task_id": None,
                "kind": "info",
                "summary": "test",
                "message": "via agent token",
            },
            headers=self._agent_headers,
        )
        self.assertEqual(res.status_code, 201)

    def test_alert_rejects_missing_auth(self):
        res = self.client.post(
            "/alerts",
            json={
                "agent": "developer",
                "task_id": None,
                "kind": "info",
                "summary": "test",
                "message": "missing auth",
            },
        )
        self.assertEqual(res.status_code, 401)

    def test_alert_rejects_bad_agent_token(self):
        res = self.client.post(
            "/alerts",
            json={
                "agent": "developer",
                "task_id": None,
                "kind": "info",
                "summary": "test",
                "message": "bad token",
            },
            headers=self._bad_agent_headers,
        )
        self.assertEqual(res.status_code, 401)

    def test_create_task_with_priority_and_dependencies(self):
        dep = self._create_task(status="todo")
        created = self.client.post(
            "/tasks",
            json={
                "title": "needs dep",
                "description": "create with priority and dependency",
                "project_id": self.project["id"],
                "priority": 0,
                "assigned_agent": "developer",
                "dependencies": [
                    {
                        "depends_on_task_id": dep["id"],
                        "required_state": "completed",
                    }
                ],
            },
            headers=self._headers,
        )
        self.assertEqual(created.status_code, 201)
        task = created.json()
        self.assertEqual(int(task.get("priority", -1)), 0)
        self.assertEqual(int(task.get("blocking_dependency_count") or 0), 1)
        self.assertFalse(bool(task.get("ready")))

        dep_rows = self.client.get(
            f"/tasks/{task['id']}/dependencies",
            headers=self._headers,
        )
        self.assertEqual(dep_rows.status_code, 200)
        data = dep_rows.json()
        self.assertEqual(len(data.get("dependencies") or []), 1)
        self.assertEqual(data["dependencies"][0]["depends_on_task_id"], dep["id"])

    def test_replace_task_dependencies_rejects_cycle(self):
        a = self._create_task(status="todo")
        b = self._create_task(status="todo")
        ok = self.client.put(
            f"/tasks/{a['id']}/dependencies",
            json={
                "dependencies": [
                    {
                        "depends_on_task_id": b["id"],
                        "required_state": "completed",
                    }
                ]
            },
            headers=self._headers,
        )
        self.assertEqual(ok.status_code, 200)

        bad = self.client.put(
            f"/tasks/{b['id']}/dependencies",
            json={
                "dependencies": [
                    {
                        "depends_on_task_id": a["id"],
                        "required_state": "completed",
                    }
                ]
            },
            headers=self._headers,
        )
        self.assertEqual(bad.status_code, 409)


if __name__ == "__main__":
    unittest.main()
