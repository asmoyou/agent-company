import asyncio
import contextlib
import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
AGENTS_DIR = ROOT / "agents"
if str(AGENTS_DIR) not in sys.path:
    sys.path.insert(0, str(AGENTS_DIR))

import generic as generic_module  # noqa: E402
import manager as manager_module  # noqa: E402
import reviewer as reviewer_module  # noqa: E402
import run_all as run_all_module  # noqa: E402


class AgentRuntimeDispatchTest(unittest.TestCase):
    def test_developer_runtime_dispatch_now_uses_generic_agent(self):
        shutdown = run_all_module.ShutdownToken(asyncio.Event())
        agent = run_all_module.create_agent_instance(
            "developer",
            {
                "key": "developer",
                "name": "开发者",
                "prompt": "do work",
                "poll_statuses": '["todo","needs_changes"]',
                "next_status": "in_review",
                "working_status": "in_progress",
                "cli": "codex",
            },
            shutdown,
        )
        self.assertIs(type(agent), generic_module.GenericAgent)
        self.assertEqual(agent._runtime_profile, "developer")
        self.assertFalse(agent._sync_from_latest_handoff)
        self.assertEqual(agent._commit_hash_mode, "full")
        self.assertEqual(agent._post_commit_retry_max, 6)
        with contextlib.suppress(Exception):
            asyncio.run(agent.http.aclose())
        with contextlib.suppress(Exception):
            asyncio.run(agent.http_output.aclose())

    def test_custom_generic_agent_can_opt_into_developer_profile(self):
        shutdown = run_all_module.ShutdownToken(asyncio.Event())
        agent = run_all_module.create_agent_instance(
            "qa_engineer",
            {
                "key": "qa_engineer",
                "name": "QA Engineer",
                "prompt": "do work",
                "poll_statuses": '["todo","needs_changes"]',
                "next_status": "in_review",
                "working_status": "in_progress",
                "runtime_profile": "developer",
                "cli": "codex",
            },
            shutdown,
        )
        self.assertIs(type(agent), generic_module.GenericAgent)
        self.assertEqual(agent._runtime_profile, "developer")
        self.assertFalse(agent._sync_from_latest_handoff)
        self.assertEqual(agent._commit_hash_mode, "full")
        self.assertEqual(agent._post_commit_retry_max, 6)
        with contextlib.suppress(Exception):
            asyncio.run(agent.http.aclose())
        with contextlib.suppress(Exception):
            asyncio.run(agent.http_output.aclose())

    def test_reviewer_runtime_dispatch_remains_specialized(self):
        shutdown = run_all_module.ShutdownToken(asyncio.Event())
        agent = run_all_module.create_agent_instance(
            "reviewer",
            {
                "key": "reviewer",
                "name": "审查者",
                "prompt": "review",
                "poll_statuses": '["in_review"]',
                "next_status": "approved",
                "working_status": "reviewing",
                "cli": "codex",
            },
            shutdown,
        )
        self.assertIsInstance(agent, reviewer_module.ReviewerAgent)
        with contextlib.suppress(Exception):
            asyncio.run(agent.http.aclose())
        with contextlib.suppress(Exception):
            asyncio.run(agent.http_output.aclose())

    def test_manager_runtime_dispatch_remains_specialized(self):
        shutdown = run_all_module.ShutdownToken(asyncio.Event())
        agent = run_all_module.create_agent_instance(
            "manager",
            {
                "key": "manager",
                "name": "合并管理者",
                "prompt": "merge",
                "poll_statuses": '["approved"]',
                "next_status": "pending_acceptance",
                "working_status": "merging",
                "cli": "codex",
            },
            shutdown,
        )
        self.assertIsInstance(agent, manager_module.ManagerAgent)
        with contextlib.suppress(Exception):
            asyncio.run(agent.http.aclose())
        with contextlib.suppress(Exception):
            asyncio.run(agent.http_output.aclose())


if __name__ == "__main__":
    unittest.main()
