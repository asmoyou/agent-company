import contextlib
import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
AGENTS_DIR = ROOT / "agents"
if str(AGENTS_DIR) not in sys.path:
    sys.path.insert(0, str(AGENTS_DIR))

import base as base_module  # noqa: E402


class ClaudePtyRuntimeTest(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.agent = base_module.BaseAgent()

    async def asyncTearDown(self):
        with contextlib.suppress(Exception):
            await self.agent.http.aclose()
        with contextlib.suppress(Exception):
            await self.agent.http_output.aclose()

    def test_claude_uses_pty(self):
        self.agent.cli_name = "claude"
        self.assertTrue(self.agent._cli_uses_pty())

    def test_codex_stays_on_pipe(self):
        self.agent.cli_name = "codex"
        self.assertFalse(self.agent._cli_uses_pty())

    def test_ansi_regex_strips_osc_sequences(self):
        raw = "ok\x1b]9;4;0;\x07\x1b]0;\x07"
        self.assertEqual(base_module.ANSI_ESCAPE_RE.sub("", raw), "ok")


if __name__ == "__main__":
    unittest.main()
