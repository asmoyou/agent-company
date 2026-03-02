import asyncio
import json
import os
import re
import shutil
import time
from pathlib import Path

import httpx

PROJECT_ROOT = Path(__file__).parent.parent
SERVER_URL = os.getenv("SERVER_URL", "http://localhost:8080")
POLL_INTERVAL = int(os.getenv("POLL_INTERVAL", "5"))
CLI_TIMEOUT = int(os.getenv("CLI_TIMEOUT", "300"))
HEARTBEAT_INTERVAL = 45  # seconds between "still working" log entries

CLI_TEMPLATES = {
    "claude": ["claude", "--dangerously-skip-permissions", "-p", "{prompt}"],
    "codex":  ["codex", "--full-auto", "{prompt}"],
}


def build_cli_cmd(cli_name: str, prompt: str) -> list[str]:
    template = CLI_TEMPLATES.get(cli_name, [cli_name, "-p", "{prompt}"])
    return [arg.replace("{prompt}", prompt) for arg in template]


def get_project_dirs(task: dict) -> tuple[Path, Path]:
    """Return (project_root, dev_worktree) for the given task."""
    project_path = task.get("project_path")
    if project_path:
        root = Path(project_path)
    else:
        # Fallback: isolated scratch dir so we never touch OPC-demo itself
        root = PROJECT_ROOT / ".worktrees" / "scratch"
        root.mkdir(parents=True, exist_ok=True)
    return root, root / ".worktrees" / "dev"


class BaseAgent:
    name: str = "base"
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

    # ── Agent terminal stream ─────────────────────────────────────────────────

    def _post_output_bg(self, line: str):
        async def _send():
            try:
                await self.http.post(
                    f"/agents/{self.name}/output",
                    json={"line": line}, timeout=2.0,
                )
            except Exception:
                pass
        asyncio.create_task(_send())

    async def set_agent_status(self, status: str, task_title: str = ""):
        try:
            await self.http.post(
                f"/agents/{self.name}/status",
                json={"status": status, "task": task_title}, timeout=2.0,
            )
        except Exception:
            pass

    # ── Git helpers ──────────────────────────────────────────────────────────

    async def git(self, *args: str, cwd: Path) -> str:
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

    # ── CLI runner with streaming + heartbeat ─────────────────────────────────

    async def run_cli(self, prompt: str, cwd: Path, task_id: str | None = None) -> tuple[int, str]:
        cmd = build_cli_cmd(self.cli_name, prompt)
        print(f"[{self.name}] Spawning {cmd[0]} in {cwd}")
        self._post_output_bg(f"$ {cmd[0]} (cwd: {cwd.name})")

        proc = await asyncio.create_subprocess_exec(
            *cmd,
            cwd=str(cwd),
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            env=os.environ.copy(),
        )

        lines: list[str] = []
        start_time = time.monotonic()

        async def drain(stream):
            async for raw in stream:
                line = raw.decode(errors="replace").rstrip("\n")
                if line:
                    lines.append(line)
                    self._post_output_bg(line)

        async def heartbeat():
            """Post a 'still alive' log every HEARTBEAT_INTERVAL seconds."""
            while True:
                await asyncio.sleep(HEARTBEAT_INTERVAL)
                elapsed = int(time.monotonic() - start_time)
                msg = f"⏳ 仍在工作中... 已运行 {elapsed}s"
                self._post_output_bg(msg)
                if task_id:
                    await self.add_log(task_id, msg)

        hb_task = asyncio.create_task(heartbeat())
        try:
            await asyncio.wait_for(
                asyncio.gather(drain(proc.stdout), drain(proc.stderr)),
                timeout=CLI_TIMEOUT,
            )
        except asyncio.TimeoutError:
            proc.kill()
            lines.append(f"[TIMEOUT after {CLI_TIMEOUT}s]")
            self._post_output_bg(f"⚠ TIMEOUT after {CLI_TIMEOUT}s")
        finally:
            hb_task.cancel()

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

    # ── Main loop ────────────────────────────────────────────────────────────

    async def process_task(self, task: dict):
        raise NotImplementedError

    async def run(self):
        if not shutil.which(self.cli_name):
            print(f"[{self.name}] WARNING: '{self.cli_name}' not found in PATH")

        print(f"[{self.name}] Starting (CLI={self.cli_name}, polls={self.poll_statuses})")
        await self.set_agent_status("idle")

        while True:
            for status in self.poll_statuses:
                try:
                    tasks = await self.fetch_tasks(status)
                    for task in tasks:
                        title = task["title"][:40]
                        print(f"[{self.name}] → '{title}' ({status})")
                        await self.set_agent_status("busy", task["title"])
                        self._post_output_bg(f"▶ 任务: {task['title']}")
                        try:
                            await self.process_task(task)
                        except Exception as e:
                            await self.add_log(task["id"], f"错误: {e}")
                            self._post_output_bg(f"✗ 错误: {e}")
                            print(f"[{self.name}] Error: {e}")
                        await self.set_agent_status("idle")
                        self._post_output_bg("─── 等待下一个任务 ───")
                except Exception as e:
                    print(f"[{self.name}] Poll error ({status}): {e}")
            await asyncio.sleep(POLL_INTERVAL)
