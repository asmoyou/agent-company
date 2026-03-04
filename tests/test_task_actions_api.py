import sys
import tempfile
import unittest
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

    def _create_custom_agent(self, key: str = "asmo-dev") -> None:
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
                "cli": "claude",
            },
            headers=self._headers,
        )
        self.assertEqual(res.status_code, 201)

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


if __name__ == "__main__":
    unittest.main()
