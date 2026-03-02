import asyncio
import json
import os
import re
import shutil
import time
from pathlib import Path

import httpx

PROJECT_ROOT    = Path(__file__).parent.parent
PROMPTS_DIR     = PROJECT_ROOT / "prompts"
SERVER_URL      = os.getenv("SERVER_URL", "http://localhost:8080")
POLL_INTERVAL   = int(os.getenv("POLL_INTERVAL", "5"))
CLI_TIMEOUT     = int(os.getenv("CLI_TIMEOUT", "300"))
HEARTBEAT_SECS  = 45

CLI_TEMPLATES = {
    "claude": ["claude", "--dangerously-skip-permissions", "-p", "{prompt}"],
    # Use non-interactive subcommand to avoid TTY requirement in agent subprocesses.
    "codex":  ["codex", "exec", "{prompt}"],
}


def build_cli_cmd(cli_name: str, prompt: str) -> list[str]:
    template = CLI_TEMPLATES.get(cli_name, [cli_name, "-p", "{prompt}"])
    return [arg.replace("{prompt}", prompt) for arg in template]


def normalize_agent_key(agent_key: str | None, default: str = "developer") -> str:
    raw = (agent_key or default).strip().lower()
    safe = re.sub(r"[^a-z0-9_-]+", "-", raw).strip("-_")
    return safe or default


def get_task_dev_agent(task: dict, fallback: str = "developer") -> str:
    return normalize_agent_key(task.get("dev_agent") or task.get("assigned_agent") or fallback)


def get_agent_branch(agent_key: str) -> str:
    return f"agent/{normalize_agent_key(agent_key)}"


def get_project_dirs(task: dict, agent_key: str | None = None) -> tuple[Path, Path]:
    """Return (project_root, worktree_for_agent)."""
    project_path = task.get("project_path")
    if project_path:
        root = Path(project_path)
    else:
        root = PROJECT_ROOT / ".worktrees" / "scratch"
        root.mkdir(parents=True, exist_ok=True)
    key = normalize_agent_key(agent_key or get_task_dev_agent(task))
    return root, root / ".worktrees" / key


def load_prompt(agent_name: str, project_path: Path | None = None) -> str:
    """
    Load prompt template for an agent.
    Priority: {project_path}/.opc/{agent}.md  >  prompts/{agent}.md
    Returns empty string if neither exists.
    """
    if project_path:
        override = project_path / ".opc" / f"{agent_name}.md"
        if override.exists():
            return override.read_text(encoding="utf-8")
    default = PROMPTS_DIR / f"{agent_name}.md"
    if default.exists():
        return default.read_text(encoding="utf-8")
    return ""


class BaseAgent:
    name: str = "base"
    poll_statuses: list[str] = []
    cli_name: str = "claude"
    working_status: str = ""

    def __init__(self, shutdown_event: asyncio.Event | None = None):
        self.shutdown = shutdown_event or asyncio.Event()
        # trust_env=False: ignore system proxy (SOCKS etc.) for localhost calls
        self.http = httpx.AsyncClient(base_url=SERVER_URL, timeout=30, trust_env=False)

    # ── HTTP helpers ─────────────────────────────────────────────────────────

    async def fetch_tasks(self, status: str) -> list[dict]:
        r = await self.http.get(f"/tasks/status/{status}")
        r.raise_for_status()
        return r.json()

    async def claim_task(
        self, status: str, working_status: str, respect_assignment: bool, project_id: str | None = None
    ) -> dict | None:
        r = await self.http.post(
            "/tasks/claim",
            json={
                "status": status,
                "working_status": working_status,
                "agent": self.name,
                "agent_key": self.name,
                "respect_assignment": respect_assignment,
                "project_id": project_id,
            },
        )
        r.raise_for_status()
        return r.json().get("task")

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

    # ── Agent terminal output ─────────────────────────────────────────────────

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

    # ── Git ───────────────────────────────────────────────────────────────────

    async def git(self, *args: str, cwd: Path) -> str:
        proc = await asyncio.create_subprocess_exec(
            "git", *args, cwd=str(cwd),
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, stderr = await proc.communicate()
        if proc.returncode != 0:
            raise RuntimeError(f"git {' '.join(args)} failed: {stderr.decode().strip()}")
        return stdout.decode().strip()

    # ── CLI runner ────────────────────────────────────────────────────────────

    async def run_cli(self, prompt: str, cwd: Path, task_id: str | None = None) -> tuple[int, str]:
        cmd = build_cli_cmd(self.cli_name, prompt)
        print(f"[{self.name}] Spawning {cmd[0]} (cwd={cwd.name})")
        self._post_output_bg(f"$ {cmd[0]}  cwd={cwd}")

        proc = await asyncio.create_subprocess_exec(
            *cmd, cwd=str(cwd),
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
            while True:
                await asyncio.sleep(HEARTBEAT_SECS)
                elapsed = int(time.monotonic() - start_time)
                msg = f"⏳ 仍在工作中... 已运行 {elapsed}s"
                self._post_output_bg(msg)
                if task_id:
                    await self.add_log(task_id, msg)

        hb = asyncio.create_task(heartbeat())
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
            hb.cancel()

        await proc.wait()
        return proc.returncode, "\n".join(lines)

    async def ensure_agent_workspace(
        self, task: dict, agent_key: str | None = None
    ) -> tuple[Path, Path, str]:
        """
        Ensure git branch+worktree for a given agent key.
        Branch: agent/{key}
        Worktree: .worktrees/{key}
        """
        key = normalize_agent_key(agent_key or get_task_dev_agent(task))
        branch = get_agent_branch(key)
        root, worktree = get_project_dirs(task, agent_key=key)
        root.mkdir(parents=True, exist_ok=True)

        # Bootstrap repo for scratch or non-initialized project paths.
        if not (root / ".git").exists():
            await self.git("init", cwd=root)
            await self.git("config", "user.email", "agent@opc-demo.local", cwd=root)
            await self.git("config", "user.name", "OPC Agent", cwd=root)
            try:
                await self.git("checkout", "-b", "main", cwd=root)
            except Exception:
                pass
            try:
                await self.git("commit", "--allow-empty", "-m", "chore: init project", cwd=root)
            except Exception:
                pass

        has_main = bool((await self.git("branch", "--list", "main", cwd=root)).strip())
        has_agent_branch = bool((await self.git("branch", "--list", branch, cwd=root)).strip())
        if not has_agent_branch:
            base_ref = "main" if has_main else "HEAD"
            await self.git("branch", branch, base_ref, cwd=root)

        if not worktree.exists():
            worktree.parent.mkdir(parents=True, exist_ok=True)
            await self.git("worktree", "add", str(worktree), branch, cwd=root)
        else:
            try:
                await self.git("checkout", branch, cwd=worktree)
            except Exception:
                pass

        await self.git("config", "user.email", "agent@opc-demo.local", cwd=worktree)
        await self.git("config", "user.name", "OPC Agent", cwd=worktree)
        return root, worktree, branch

    # ── JSON decision parser ─────────────────────────────────────────────────

    def parse_json_decision(self, text: str) -> dict | None:
        for m in reversed(re.findall(r"```(?:json)?\s*(\{[\s\S]+?\})\s*```", text)):
            try:
                d = json.loads(m)
                if "decision" in d:
                    return d
            except json.JSONDecodeError:
                pass
        for m in reversed(re.findall(r"\{[^{}]*\}", text)):
            try:
                d = json.loads(m)
                if "decision" in d:
                    return d
            except json.JSONDecodeError:
                pass
        return None

    # ── Main loop ─────────────────────────────────────────────────────────────

    async def process_task(self, task: dict):
        raise NotImplementedError

    def working_status_for(self, status: str) -> str:
        return self.working_status or status

    def respect_assignment_for(self, status: str) -> bool:
        return True

    async def run(self):
        if not shutil.which(self.cli_name):
            print(f"[{self.name}] WARNING: '{self.cli_name}' not found in PATH")
        print(f"[{self.name}] Starting (CLI={self.cli_name}, polls={self.poll_statuses})")
        await self.set_agent_status("idle")

        while not self.shutdown.is_set():
            for status in self.poll_statuses:
                if self.shutdown.is_set():
                    break
                try:
                    while not self.shutdown.is_set():
                        task = await self.claim_task(
                            status=status,
                            working_status=self.working_status_for(status),
                            respect_assignment=self.respect_assignment_for(status),
                        )
                        if not task:
                            break
                        task["_claimed_from_status"] = status
                        if self.shutdown.is_set():
                            break
                        print(f"[{self.name}] → '{task['title'][:40]}' ({status})")
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

            # Sleep for POLL_INTERVAL, but wake up early if shutdown is set
            try:
                await asyncio.wait_for(self.shutdown.wait(), timeout=POLL_INTERVAL)
            except asyncio.TimeoutError:
                pass  # Normal timeout, continue polling

        self._post_output_bg(f"[{self.name}] 已收到关闭信号，停止接受新任务")
        print(f"[{self.name}] Stopped.")
        await self.http.aclose()
