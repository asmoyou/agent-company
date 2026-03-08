import contextlib
import sys
import unittest
from pathlib import Path
from unittest import mock

ROOT = Path(__file__).resolve().parents[1]
AGENTS_DIR = ROOT / "agents"
if str(AGENTS_DIR) not in sys.path:
    sys.path.insert(0, str(AGENTS_DIR))

import base as base_module  # noqa: E402


class BaseAgentAuthReportingTest(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self._agents: list[base_module.BaseAgent] = []

    async def asyncTearDown(self):
        for agent in self._agents:
            with contextlib.suppress(Exception):
                await agent.http.aclose()
            with contextlib.suppress(Exception):
                await agent.http_output.aclose()

    def _new_agent(self, name: str = "developer") -> base_module.BaseAgent:
        agent = base_module.BaseAgent()
        agent.name = name
        self._agents.append(agent)
        return agent

    def _response(self, *, status_code: int = 200, payload: dict | None = None):
        resp = mock.Mock()
        resp.status_code = status_code
        resp.json.return_value = payload or {}
        resp.raise_for_status.return_value = None
        resp.text = ""
        return resp

    async def test_init_sets_agent_token_header_on_clients(self):
        agent = self._new_agent()
        token = str(base_module.AGENT_API_TOKEN or "").strip()
        if token:
            self.assertEqual(agent.http.headers.get("X-Agent-Token"), token)
            self.assertEqual(agent.http_output.headers.get("X-Agent-Token"), token)
        else:
            self.assertNotIn("X-Agent-Token", agent.http.headers)
            self.assertNotIn("X-Agent-Token", agent.http_output.headers)

    async def test_claim_task_posts_identity_and_lease_ttl(self):
        agent = self._new_agent("developer")
        agent.http.post = mock.AsyncMock(
            return_value=self._response(payload={"task": {"id": "task-1"}})
        )

        claimed = await agent.claim_task(
            status="todo",
            working_status="in_progress",
            respect_assignment=True,
            project_id="project-1",
        )
        self.assertEqual(claimed["id"], "task-1")

        call = agent.http.post.await_args
        self.assertEqual(call.args[0], "/tasks/claim")
        payload = call.kwargs["json"]
        self.assertEqual(payload["agent"], "developer")
        self.assertEqual(payload["agent_key"], "developer")
        self.assertEqual(payload["lease_ttl_secs"], base_module.TASK_LEASE_TTL_SECS)
        self.assertEqual(payload["project_id"], "project-1")

    async def test_transition_task_includes_lease_fence_and_default_handoff_agent(self):
        agent = self._new_agent("developer")
        agent._active_task_id = "task-1"
        agent._active_run_id = "run-1"
        agent._active_lease_token = "lease-1"
        agent.http.post = mock.AsyncMock(
            return_value=self._response(payload={"task": {"id": "task-1", "status": "todo"}})
        )

        await agent.transition_task(
            "task-1",
            fields={"status": "todo"},
            handoff={"stage": "note"},
            log_message="ok",
        )

        call = agent.http.post.await_args
        self.assertEqual(call.args[0], "/tasks/task-1/transition")
        payload = call.kwargs["json"]
        self.assertEqual(payload["expected_run_id"], "run-1")
        self.assertEqual(payload["expected_lease_token"], "lease-1")
        self.assertEqual(payload["handoff"]["from_agent"], "developer")
        self.assertEqual(payload["log"]["agent"], "developer")
        self.assertEqual(payload["log"]["message"], "ok")

    async def test_add_log_applies_lease_guard_only_for_active_task(self):
        agent = self._new_agent("developer")
        agent._active_task_id = "task-1"
        agent._active_run_id = "run-1"
        agent._active_lease_token = "lease-1"
        agent.http.post = mock.AsyncMock(return_value=self._response())

        await agent.add_log("task-1", "line-1")
        await agent.add_log("task-2", "line-2")

        first = agent.http.post.await_args_list[0].kwargs["json"]
        self.assertEqual(first["run_id"], "run-1")
        self.assertEqual(first["lease_token"], "lease-1")

        second = agent.http.post.await_args_list[1].kwargs["json"]
        self.assertNotIn("run_id", second)
        self.assertNotIn("lease_token", second)

    async def test_set_agent_status_busy_uses_active_context_and_idle_clears_fields(self):
        agent = self._new_agent("developer")
        agent.project_id_scope = "project-scope"
        agent._active_task_id = "task-1"
        agent._active_project_id = "project-1"
        agent._active_run_id = "run-1"
        agent._active_lease_token = "lease-1"
        agent._active_phase = "cli_running"
        agent._active_cli_pid = 321
        agent.http.post = mock.AsyncMock(return_value=self._response())

        await agent.set_agent_status("busy", "working")
        await agent.set_agent_status("idle", "")

        busy_payload = agent.http.post.await_args_list[0].kwargs["json"]
        self.assertEqual(busy_payload["agent_key"], "developer")
        self.assertEqual(busy_payload["project_id"], "project-1")
        self.assertEqual(busy_payload["task_id"], "task-1")
        self.assertEqual(busy_payload["run_id"], "run-1")
        self.assertEqual(busy_payload["lease_token"], "lease-1")
        self.assertEqual(busy_payload["phase"], "cli_running")
        self.assertEqual(busy_payload["pid"], 321)

        idle_payload = agent.http.post.await_args_list[1].kwargs["json"]
        self.assertEqual(idle_payload["project_id"], "project-scope")
        self.assertEqual(idle_payload["task_id"], "")
        self.assertEqual(idle_payload["run_id"], "")
        self.assertEqual(idle_payload["lease_token"], "")
        self.assertEqual(idle_payload["phase"], "")
        self.assertIsNone(idle_payload["pid"])

    async def test_renew_task_lease_returns_false_on_conflict_response(self):
        agent = self._new_agent("developer")
        conflict = self._response(status_code=409)
        agent.http.post = mock.AsyncMock(return_value=conflict)

        renewed = await agent.renew_task_lease("task-1", "run-1", "lease-1")
        self.assertFalse(renewed)
        conflict.raise_for_status.assert_not_called()

    async def test_transition_task_surfaces_response_detail_on_http_error(self):
        agent = self._new_agent("developer")
        bad = self._response(status_code=422)
        bad.text = "handoff.status_from 与任务当前状态不一致: todo != in_progress"
        agent.http.post = mock.AsyncMock(return_value=bad)

        with self.assertRaises(RuntimeError) as ctx:
            await agent.transition_task("task-1", fields={"status": "in_review"})

        self.assertIn("Transition failed (422)", str(ctx.exception))
        self.assertIn("handoff.status_from", str(ctx.exception))


if __name__ == "__main__":
    unittest.main()
