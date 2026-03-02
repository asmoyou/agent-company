import asyncio
import json
import os
import re
import shutil
from pathlib import Path

import httpx

PROJECT_ROOT = Path(__file__).parent.parent
SERVER_URL = os.getenv("SERVER_URL", "http://localhost:8000")
POLL_INTERVAL = int(os.getenv("POLL_INTERVAL", "5"))
CLI_TIMEOUT = int(os.getenv("CLI_TIMEOUT", "300"))

CLI_TEMPLATES = {
    "claude": ["claude", "--dangerously-skip-permissions", "-p", "{prompt}"],
    "codex":  ["codex", "--full-auto", "{prompt}"],
}


def build_cli_cmd(cli_name: str, prompt: str) -> list[str]:
    template = CLI_TEMPLATES.get(cli_name, [cli_name, "-p", "{prompt}"])
    return [arg.replace("{prompt}", prompt) for arg in template]


class BaseAgent:
    name: str = "base"
    worktree: Path = PROJECT_ROOT
    poll_statuses: list[str] = []
    cli_name: str = "claude"

    def __init__(self):
        self.http = httpx.AsyncClient(base_url=SERVER_URL, timeout=30)

    # ── HTTP helpers ─────────────────────────────────────────────────────────

    async def fetch_tasks(self, status: str) -> list[dict]:
        r = await self.http.get(f"/tasks/status/{status}")
        r.raise_for_status()
        return r.json()

    async def update_task(self, task_id: str, **fields) -> dict:
        r = await self.http.patch(f"/tasks/{task_id}", json=fields)
        r.raise_for_status()
        return r.json()

    async def get_task(self, task_id: str) -> dict:
        r = await self.http.get(f"/tasks/{task_id}")
        r.raise_for_status()
        return r.json()

    async def add_log(self, task_id: str, message: str):
        try:
            await self.http.post(
                f"/tasks/{task_id}/logs",
                json={"agent": self.name, "message": message},
            )
        except Exception:
            pass

    # ── Agent terminal output stream ─────────────────────────────────────────

    def _post_output_bg(self, line: str):
        """Fire-and-forget: stream one line of CLI output to the server."""
        async def _send():
            try:
                await self.http.post(
                    f"/agents/{self.name}/output",
                    json={"line": line},
                    timeout=2.0,
                )
            except Exception:
                pass
        asyncio.create_task(_send())

    async def set_agent_status(self, status: str, task_title: str = ""):
        """Broadcast agent idle/busy status."""
        try:
            await self.http.post(
                f"/agents/{self.name}/status",
                json={"status": status, "task": task_title},
                timeout=2.0,
            )
        except Exception:
            pass

    # ── Git helpers ──────────────────────────────────────────────────────────

    async def git(self, *args: str, cwd: Path | None = None) -> str:
        cwd = cwd or self.worktree
        proc = await asyncio.create_subprocess_exec(
            "git", *args,
            cwd=str(cwd),
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, stderr = await proc.communicate()
        if proc.returncode != 0:
            raise RuntimeError(f"git {' '.join(args)} failed: {stderr.decode().strip()}")
        return stdout.decode().strip()

    # ── CLI runner with real-time streaming ───────────────────────────────────

    async def run_cli(self, prompt: str, cwd: Path | None = None) -> tuple[int, str]:
        """
        Spawn the configured CLI tool (claude/codex/…) and stream its output
        line-by-line to the server's agent terminal endpoint.
        Returns (returncode, full_output).
        """
        cwd = cwd or self.worktree
        cmd = build_cli_cmd(self.cli_name, prompt)
        print(f"[{self.name}] Spawning: {cmd[0]} (cwd={cwd.name})")

        proc = await asyncio.create_subprocess_exec(
            *cmd,
            cwd=str(cwd),
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            env=os.environ.copy(),
        )

        lines: list[str] = []

        async def drain(stream):
            async for raw in stream:
                line = raw.decode(errors="replace").rstrip("\n")
                if line:
                    lines.append(line)
                    self._post_output_bg(line)

        try:
            await asyncio.wait_for(
                asyncio.gather(drain(proc.stdout), drain(proc.stderr)),
                timeout=CLI_TIMEOUT,
            )
        except asyncio.TimeoutError:
            proc.kill()
            lines.append(f"[TIMEOUT after {CLI_TIMEOUT}s]")
            self._post_output_bg(f"⚠ TIMEOUT after {CLI_TIMEOUT}s")
            return -1, "\n".join(lines)

        await proc.wait()
        return proc.returncode, "\n".join(lines)

    # ── JSON decision parser ─────────────────────────────────────────────────

    def parse_json_decision(self, text: str) -> dict | None:
        for m in reversed(re.findall(r"```(?:json)?\s*(\{[\s\S]+?\})\s*```", text)):
            try:
                data = json.loads(m)
                if "decision" in data:
                    return data
            except json.JSONDecodeError:
                pass
        for m in reversed(re.findall(r"\{[^{}]*\}", text)):
            try:
                data = json.loads(m)
                if "decision" in data:
                    return data
            except json.JSONDecodeError:
                pass
        return None

    # ── Main polling loop ────────────────────────────────────────────────────

    async def process_task(self, task: dict):
        raise NotImplementedError

    async def run(self):
        if not shutil.which(self.cli_name):
            print(f"[{self.name}] WARNING: '{self.cli_name}' not found in PATH")

        print(f"[{self.name}] Starting (CLI: {self.cli_name}, polls: {self.poll_statuses})")
        await self.set_agent_status("idle")

        while True:
            for status in self.poll_statuses:
                try:
                    tasks = await self.fetch_tasks(status)
                    for task in tasks:
                        print(f"[{self.name}] Picked up '{task['title'][:40]}' ({status})")
                        await self.set_agent_status("busy", task["title"])
                        self._post_output_bg(f"▶ 开始处理任务: {task['title']}")
                        try:
                            await self.process_task(task)
                        except Exception as e:
                            await self.add_log(task["id"], f"Error: {e}")
                            self._post_output_bg(f"✗ 错误: {e}")
                            print(f"[{self.name}] Error: {e}")
                        await self.set_agent_status("idle")
                        self._post_output_bg("─── 任务完成，等待下一个 ───")
                except Exception as e:
                    print(f"[{self.name}] Poll error ({status}): {e}")
            await asyncio.sleep(POLL_INTERVAL)
