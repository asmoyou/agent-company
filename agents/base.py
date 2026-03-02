import asyncio
import json
import os
import re
import shutil
import tempfile
import time
from pathlib import Path

import httpx

PROJECT_ROOT    = Path(__file__).parent.parent
SERVER_URL      = os.getenv("SERVER_URL", "http://localhost:8080")
POLL_INTERVAL   = int(os.getenv("POLL_INTERVAL", "5"))
CLI_TIMEOUT     = int(os.getenv("CLI_TIMEOUT", "300"))
HEARTBEAT_SECS  = 45

CLI_TEMPLATES = {
    "claude": ["claude", "--dangerously-skip-permissions", "-p", "{prompt}"],
    # Use non-interactive subcommand to avoid TTY requirement in agent subprocesses.
    "codex":  ["codex", "exec", "--dangerously-bypass-approvals-and-sandbox", "{prompt}"],
}
AUTO_REPLY_MAX = int(os.getenv("AUTO_REPLY_MAX", "12"))
# Idle fallback auto-reply is disabled by default to avoid blind ENTER loops.
AUTO_REPLY_IDLE_SECS = int(os.getenv("AUTO_REPLY_IDLE_SECS", "0"))
AUTO_REPLY_TEXT = os.getenv("AUTO_REPLY_TEXT", "\n")
HANDOFF_SYNC_STRATEGY = os.getenv("HANDOFF_SYNC_STRATEGY", "cherry-pick").strip().lower()
INTERACTIVE_PROMPT_RE = re.compile(
    r"(?i)(press\s+enter|hit\s+enter|按回车|回车继续|是否继续|continue\?|proceed\?|确认继续|"
    r"\[y/n\]|\[y/N\]|\(y/n\)|yes/no|select\s+an?\s+option|choose\s+an?\s+option|"
    r"请输入.*继续|input.*continue)"
)


def build_cli_cmd(cli_name: str, prompt: str) -> list[str]:
    template = CLI_TEMPLATES.get(cli_name, [cli_name, "-p", "{prompt}"])
    return [arg.replace("{prompt}", prompt) for arg in template]


def parse_status_list(raw, default: list[str]) -> list[str]:
    """Parse poll_statuses from DB row (JSON string/list) with fallback."""
    if isinstance(raw, list) and raw:
        return [str(x) for x in raw]
    if isinstance(raw, str):
        try:
            decoded = json.loads(raw)
            if isinstance(decoded, list) and decoded:
                return [str(x) for x in decoded]
        except Exception:
            pass
    return list(default)


def normalize_agent_key(agent_key: str | None, default: str = "developer") -> str:
    raw = (agent_key or default).strip().lower()
    safe = re.sub(r"[^a-z0-9_-]+", "-", raw).strip("-_")
    return safe or default


def safe_agent_key(agent_key: str | None) -> str:
    raw = (agent_key or "").strip().lower()
    return re.sub(r"[^a-z0-9_-]+", "-", raw).strip("-_")


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


class BaseAgent:
    name: str = "base"
    poll_statuses: list[str] = []
    cli_name: str = "claude"
    working_status: str = ""

    def __init__(self, shutdown_event: asyncio.Event | None = None):
        self.shutdown = shutdown_event or asyncio.Event()
        # trust_env=False: ignore system proxy (SOCKS etc.) for localhost calls
        self.http = httpx.AsyncClient(base_url=SERVER_URL, timeout=30, trust_env=False)
        self._active_task_id: str | None = None

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

    async def is_task_cancelled(self, task_id: str) -> bool:
        try:
            task = await self.get_task(task_id)
        except Exception:
            return False
        if not task:
            return True
        status = str(task.get("status") or "").strip().lower()
        archived = int(task.get("archived") or 0)
        return status == "cancelled" or archived == 1

    async def stop_if_task_cancelled(self, task_id: str, stage: str = "") -> bool:
        if not await self.is_task_cancelled(task_id):
            return False
        msg = "任务已取消，停止继续执行"
        if stage:
            msg += f"（{stage}）"
        await self.add_log(task_id, msg)
        self._post_output_bg(f"🛑 {msg}")
        return True

    async def add_log(self, task_id: str, message: str):
        try:
            await self.http.post(
                f"/tasks/{task_id}/logs",
                json={"agent": self.name, "message": message},
            )
        except Exception:
            pass

    async def add_alert(
        self,
        summary: str,
        task_id: str | None = None,
        message: str = "",
        kind: str = "error",
        code: str = "",
        stage: str = "",
        metadata: dict | None = None,
    ):
        try:
            await self.http.post(
                "/alerts",
                json={
                    "agent": self.name,
                    "task_id": task_id,
                    "kind": kind,
                    "summary": summary,
                    "message": message,
                    "code": code,
                    "stage": stage,
                    "metadata": metadata or {},
                },
            )
        except Exception:
            pass

    async def get_handoffs(self, task_id: str) -> list[dict]:
        try:
            r = await self.http.get(f"/tasks/{task_id}/handoffs")
            r.raise_for_status()
            data = r.json()
            return data if isinstance(data, list) else []
        except Exception:
            return []

    async def build_handoff_context(self, task_id: str, limit: int = 6) -> str:
        handoffs = await self.get_handoffs(task_id)
        if not handoffs:
            return ""
        tail = handoffs[-limit:]
        lines = ["## 历史交接记录（最近）"]
        for h in tail:
            stage = str(h.get("stage") or "").strip() or "-"
            from_agent = str(h.get("from_agent") or "").strip() or "-"
            to_agent = str(h.get("to_agent") or "").strip() or "-"
            summary = str(h.get("summary") or "").strip()
            commit_hash = str(h.get("commit_hash") or "").strip()
            conclusion = str(h.get("conclusion") or "").strip()
            lines.append(f"- {from_agent} -> {to_agent} [{stage}]")
            if commit_hash:
                lines.append(f"  commit: {commit_hash}")
            if conclusion:
                lines.append(f"  结论: {conclusion[:160]}")
            if summary:
                lines.append(f"  摘要: {summary[:200]}")
        return "\n".join(lines)

    def _extract_handoff_commit(self, handoff: dict) -> str:
        commit_hash = str(handoff.get("commit_hash") or "").strip()
        if commit_hash:
            return commit_hash
        payload = handoff.get("payload")
        if isinstance(payload, dict):
            return str(payload.get("commit_hash") or "").strip()
        return ""

    def _extract_handoff_source_branch(self, handoff: dict) -> str:
        payload = handoff.get("payload")
        if isinstance(payload, dict):
            return str(payload.get("source_branch") or "").strip()
        return ""

    async def get_latest_handoff_for_agent(self, task_id: str) -> dict | None:
        handoffs = await self.get_handoffs(task_id)
        if not handoffs:
            return None
        me = safe_agent_key(self.name)
        for h in reversed(handoffs):
            to_agent = safe_agent_key(h.get("to_agent"))
            if to_agent and to_agent == me:
                return h
        for h in reversed(handoffs):
            if self._extract_handoff_commit(h):
                return h
        return handoffs[-1]

    async def _is_commit_in_head(self, worktree: Path, commit_hash: str) -> bool:
        try:
            await self.git("merge-base", "--is-ancestor", commit_hash, "HEAD", cwd=worktree)
            return True
        except Exception:
            return False

    async def sync_from_latest_handoff(
        self,
        task: dict,
        worktree: Path,
        current_branch: str | None = None,
    ) -> dict:
        strategy = HANDOFF_SYNC_STRATEGY
        if strategy in ("", "none", "off", "disabled"):
            return {"status": "disabled"}

        task_id = task["id"]
        handoff = await self.get_latest_handoff_for_agent(task_id)
        if not handoff:
            return {"status": "no_handoff"}

        commit_hash = self._extract_handoff_commit(handoff) or str(task.get("commit_hash") or "").strip()
        if not commit_hash:
            return {"status": "no_commit"}

        source_branch = self._extract_handoff_source_branch(handoff)
        if current_branch is None:
            try:
                current_branch = await self.git("branch", "--show-current", cwd=worktree)
            except Exception:
                current_branch = ""

        if source_branch and current_branch and source_branch == current_branch:
            return {"status": "same_branch", "commit_hash": commit_hash, "source_branch": source_branch}

        try:
            await self.git("cat-file", "-e", f"{commit_hash}^{{commit}}", cwd=worktree)
        except Exception as e:
            msg = f"[系统错误] 交接引用的 commit 不存在：{commit_hash} ({e})"
            await self.add_log(task_id, msg[:500])
            await self.add_alert(
                summary="交接 commit 不存在",
                task_id=task_id,
                message=msg,
                kind="error",
                code=f"{self.name}_sync_missing_commit",
                stage=f"{self.name}_sync_failed",
                metadata={"commit_hash": commit_hash, "source_branch": source_branch},
            )
            await self.add_handoff(
                task_id,
                stage=f"{self.name}_sync_failed",
                to_agent=self.name,
                status_from=task.get("status"),
                status_to="blocked",
                title="交接同步失败",
                summary=msg[:300],
                commit_hash=commit_hash,
                conclusion="交接 commit 丢失，任务阻塞",
                payload={"reason": "missing_commit_object", "source_branch": source_branch},
            )
            await self.update_task(
                task_id,
                status="blocked",
                assignee=None,
                assigned_agent=self.name,
                review_feedback=msg[:500],
            )
            return {"status": "failed", "reason": "missing_commit_object"}

        if await self._is_commit_in_head(worktree, commit_hash):
            return {"status": "already_contains", "commit_hash": commit_hash, "source_branch": source_branch}

        try:
            if strategy == "merge":
                await self.git("merge", "--no-edit", "--no-ff", commit_hash, cwd=worktree)
            else:
                await self.git("cherry-pick", "-x", commit_hash, cwd=worktree)
        except Exception as e:
            err = str(e)
            try:
                if strategy == "merge":
                    await self.git("merge", "--abort", cwd=worktree)
                else:
                    await self.git("cherry-pick", "--abort", cwd=worktree)
            except Exception:
                pass
            msg = f"[系统错误] 交接 commit 同步失败：{commit_hash}（{strategy}）{err[:260]}"
            await self.add_log(task_id, msg[:500])
            await self.add_alert(
                summary="交接同步失败",
                task_id=task_id,
                message=msg,
                kind="error",
                code=f"{self.name}_sync_failed",
                stage=f"{self.name}_sync_failed",
                metadata={
                    "commit_hash": commit_hash,
                    "source_branch": source_branch,
                    "strategy": strategy,
                },
            )
            await self.add_handoff(
                task_id,
                stage=f"{self.name}_sync_failed",
                to_agent=self.name,
                status_from=task.get("status"),
                status_to="blocked",
                title="交接同步失败",
                summary=msg[:300],
                commit_hash=commit_hash,
                conclusion="交接同步冲突/失败，任务阻塞",
                payload={
                    "reason": "sync_failed",
                    "strategy": strategy,
                    "source_branch": source_branch,
                },
                artifact_path=str(worktree),
            )
            await self.update_task(
                task_id,
                status="blocked",
                assignee=None,
                assigned_agent=self.name,
                review_feedback=msg[:500],
            )
            return {"status": "failed", "reason": "sync_failed"}

        sync_msg = f"已同步交接 commit {commit_hash} 到当前分支（{strategy}）"
        await self.add_log(task_id, sync_msg)
        await self.add_handoff(
            task_id,
            stage=f"{self.name}_sync_in",
            to_agent=self.name,
            status_from=task.get("status"),
            status_to=task.get("status"),
            title="接收交接同步",
            summary=sync_msg,
            commit_hash=commit_hash,
            conclusion="已完成跨 Agent 代码同步",
            payload={
                "strategy": strategy,
                "source_branch": source_branch,
                "source_stage": handoff.get("stage"),
            },
            artifact_path=str(worktree),
        )
        return {"status": "synced", "commit_hash": commit_hash, "source_branch": source_branch}

    async def add_handoff(
        self,
        task_id: str,
        stage: str,
        to_agent: str | None = None,
        status_from: str | None = None,
        status_to: str | None = None,
        title: str = "",
        summary: str = "",
        commit_hash: str | None = None,
        conclusion: str | None = None,
        payload: dict | None = None,
        artifact_path: str | None = None,
    ):
        payload_obj = dict(payload or {})
        if commit_hash and not payload_obj.get("commit_hash"):
            payload_obj["commit_hash"] = commit_hash
        if conclusion and not payload_obj.get("conclusion"):
            payload_obj["conclusion"] = conclusion
        try:
            await self.http.post(
                f"/tasks/{task_id}/handoffs",
                json={
                    "stage": stage,
                    "from_agent": self.name,
                    "to_agent": to_agent,
                    "status_from": status_from,
                    "status_to": status_to,
                    "title": title,
                    "summary": summary,
                    "commit_hash": commit_hash,
                    "conclusion": conclusion,
                    "payload": payload_obj,
                    "artifact_path": artifact_path,
                },
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

    async def git(self, *args: str, cwd: Path, task_id: str | None = None) -> str:
        proc = await asyncio.create_subprocess_exec(
            "git", *args, cwd=str(cwd),
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        watch_task_id = task_id or self._active_task_id

        async def cancel_watcher():
            if not watch_task_id:
                return
            while proc.returncode is None:
                await asyncio.sleep(1)
                if proc.returncode is not None:
                    return
                if await self.is_task_cancelled(watch_task_id):
                    try:
                        proc.kill()
                    except Exception:
                        pass
                    return

        cwatch = asyncio.create_task(cancel_watcher())
        try:
            stdout, stderr = await proc.communicate()
        finally:
            cwatch.cancel()
        if proc.returncode != 0:
            raise RuntimeError(f"git {' '.join(args)} failed: {stderr.decode().strip()}")
        return stdout.decode().strip()

    # ── CLI runner ────────────────────────────────────────────────────────────

    async def run_cli(
        self,
        prompt: str,
        cwd: Path,
        task_id: str | None = None,
        output_schema: dict | None = None,
    ) -> tuple[int, str]:
        schema_path: Path | None = None
        last_message_path: Path | None = None
        if self.cli_name == "codex":
            cmd = ["codex", "exec", "--dangerously-bypass-approvals-and-sandbox"]
            try:
                fd_last, raw_last = tempfile.mkstemp(
                    prefix="opc-codex-last-", suffix=".txt", dir=str(cwd)
                )
                os.close(fd_last)
                last_message_path = Path(raw_last)
                cmd += ["--output-last-message", str(last_message_path)]
            except Exception:
                last_message_path = None

            if output_schema is not None:
                try:
                    fd_schema, raw_schema = tempfile.mkstemp(
                        prefix="opc-codex-schema-", suffix=".json", dir=str(cwd)
                    )
                    with os.fdopen(fd_schema, "w", encoding="utf-8") as f:
                        json.dump(output_schema, f, ensure_ascii=False)
                    schema_path = Path(raw_schema)
                    cmd += ["--output-schema", str(schema_path)]
                except Exception:
                    schema_path = None

            cmd.append(prompt)
        else:
            cmd = build_cli_cmd(self.cli_name, prompt)
        print(f"[{self.name}] Spawning {cmd[0]} (cwd={cwd.name})")
        self._post_output_bg(f"$ {cmd[0]}  cwd={cwd}")
        env = os.environ.copy()
        # Hint CLIs to avoid interactive flows.
        env.setdefault("CI", "1")
        env.setdefault("NONINTERACTIVE", "1")

        proc = await asyncio.create_subprocess_exec(
            *cmd, cwd=str(cwd),
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            env=env,
        )

        lines: list[str] = []
        start_time = time.monotonic()
        last_output_at = time.monotonic()
        auto_reply_count = 0
        # At most one idle-triggered auto-reply for the same output phase.
        idle_reply_output_marker: float | None = None
        auto_reply_lock = asyncio.Lock()

        async def auto_reply(reason: str):
            nonlocal auto_reply_count, idle_reply_output_marker
            if AUTO_REPLY_MAX <= 0:
                return
            if proc.stdin is None or proc.returncode is not None:
                return
            if auto_reply_count >= AUTO_REPLY_MAX:
                return
            async with auto_reply_lock:
                if proc.stdin is None or proc.returncode is not None:
                    return
                if auto_reply_count >= AUTO_REPLY_MAX:
                    return
                try:
                    proc.stdin.write(AUTO_REPLY_TEXT.encode("utf-8", errors="ignore"))
                    await proc.stdin.drain()
                    auto_reply_count += 1
                    idle_reply_output_marker = last_output_at
                    msg = f"↩ 自动应答({auto_reply_count}/{AUTO_REPLY_MAX}) ENTER [{reason[:80]}]"
                    lines.append(msg)
                    self._post_output_bg(msg)
                    if task_id:
                        await self.add_log(task_id, msg)
                except Exception:
                    pass

        async def drain(stream):
            nonlocal last_output_at
            async for raw in stream:
                line = raw.decode(errors="replace").rstrip("\n")
                if line:
                    last_output_at = time.monotonic()
                    lines.append(line)
                    self._post_output_bg(line)
                    if INTERACTIVE_PROMPT_RE.search(line):
                        await auto_reply(f"检测到交互提示: {line}")

        async def heartbeat():
            while True:
                await asyncio.sleep(HEARTBEAT_SECS)
                elapsed = int(time.monotonic() - start_time)
                msg = f"⏳ 仍在工作中... 已运行 {elapsed}s"
                self._post_output_bg(msg)
                if task_id:
                    await self.add_log(task_id, msg)

        async def idle_auto_reply():
            if AUTO_REPLY_IDLE_SECS <= 0:
                return
            while True:
                await asyncio.sleep(AUTO_REPLY_IDLE_SECS)
                if proc.returncode is not None:
                    return
                idle = time.monotonic() - last_output_at
                if idle < AUTO_REPLY_IDLE_SECS:
                    continue
                # Prevent blind periodic ENTER every interval with no new output.
                if idle_reply_output_marker == last_output_at:
                    continue
                await auto_reply(f"idle {int(idle)}s")

        async def cancel_watcher():
            if not task_id:
                return
            while proc.returncode is None:
                await asyncio.sleep(1)
                if proc.returncode is not None:
                    return
                try:
                    current = await self.get_task(task_id)
                except Exception:
                    continue
                if not current:
                    continue
                status = str(current.get("status") or "").strip().lower()
                archived = int(current.get("archived") or 0)
                if status == "cancelled" or archived == 1:
                    msg = "🛑 检测到任务已取消/归档，终止当前 CLI 执行"
                    lines.append(msg)
                    self._post_output_bg(msg)
                    try:
                        proc.kill()
                    except Exception:
                        pass
                    return

        hb = asyncio.create_task(heartbeat())
        iar = asyncio.create_task(idle_auto_reply())
        cwatch = asyncio.create_task(cancel_watcher())
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
            iar.cancel()
            cwatch.cancel()

        await proc.wait()

        if last_message_path and last_message_path.exists():
            try:
                last_msg = last_message_path.read_text(encoding="utf-8", errors="replace").strip()
                if last_msg:
                    lines.append(last_msg)
            except Exception:
                pass

        for p in (last_message_path, schema_path):
            if not p:
                continue
            try:
                p.unlink(missing_ok=True)
            except Exception:
                pass

        return proc.returncode, "\n".join(lines)

    async def _sync_branch_with_main(self, root: Path, worktree: Path, branch: str) -> str:
        """
        Try to fast-sync agent branch by merging `main` into it.
        Returns a short status string; never raises.
        """
        if branch == "main":
            return "main"

        has_main = bool((await self.git("branch", "--list", "main", cwd=root)).strip())
        if not has_main:
            return "no_main"

        try:
            dirty = await self.git("status", "--porcelain", cwd=worktree)
        except Exception as e:
            return f"status_error:{e}"
        if dirty.strip():
            return "dirty"

        try:
            before = await self.git("rev-parse", "HEAD", cwd=worktree)
        except Exception as e:
            return f"head_error:{e}"

        try:
            await self.git("merge", "--no-edit", "main", cwd=worktree)
        except Exception as e:
            err = str(e)
            try:
                await self.git("merge", "--abort", cwd=worktree)
            except Exception:
                pass
            return f"conflict:{err}"

        try:
            after = await self.git("rev-parse", "HEAD", cwd=worktree)
            return "merged" if before != after else "up_to_date"
        except Exception:
            return "synced"

    async def ensure_agent_workspace(
        self, task: dict, agent_key: str | None = None, sync_with_main: bool = True
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

        if sync_with_main:
            sync_result = await self._sync_branch_with_main(root, worktree, branch)
            if sync_result == "merged":
                print(f"[{self.name}] Synced {branch} with main")
            elif sync_result in ("dirty",) or sync_result.startswith("conflict:"):
                print(f"[{self.name}] WARN: main sync skipped for {branch}: {sync_result}")

        return root, worktree, branch

    # ── JSON decision parser ─────────────────────────────────────────────────

    def _normalize_decision_payload(self, payload: dict) -> dict | None:
        decision = str(payload.get("decision") or "").strip().lower()
        if decision not in {"approve", "request_changes"}:
            return None

        placeholder_values = {
            "简要说明通过原因",
            "条列说明需要修改的具体内容",
            "请修复问题",
            "...",
        }

        if decision == "approve":
            comment = str(payload.get("comment") or "").strip()
            if not comment or comment in placeholder_values:
                return None
            return {"decision": "approve", "comment": comment}

        feedback = str(payload.get("feedback") or "").strip()
        if not feedback or feedback in placeholder_values:
            return None
        return {"decision": "request_changes", "feedback": feedback}

    def parse_json_decision(self, text: str) -> dict | None:
        # Only parse near the tail to avoid picking JSON examples echoed from the prompt.
        tail = text[-6000:] if len(text) > 6000 else text

        for m in reversed(re.findall(r"```(?:json)?\s*(\{[\s\S]+?\})\s*```", tail)):
            try:
                d = json.loads(m)
                parsed = self._normalize_decision_payload(d)
                if parsed:
                    return parsed
            except json.JSONDecodeError:
                pass
        for m in reversed(re.findall(r"\{[^{}]*\}", tail)):
            try:
                d = json.loads(m)
                parsed = self._normalize_decision_payload(d)
                if parsed:
                    return parsed
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
                            self._active_task_id = task["id"]
                            await self.process_task(task)
                        except Exception as e:
                            await self.add_log(task["id"], f"错误: {e}")
                            await self.add_alert(
                                summary=f"{self.name} 运行异常",
                                task_id=task["id"],
                                message=str(e),
                                kind="error",
                                code=f"{self.name}_runtime_error",
                                stage="runtime_exception",
                            )
                            self._post_output_bg(f"✗ 错误: {e}")
                            print(f"[{self.name}] Error: {e}")
                        finally:
                            self._active_task_id = None
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
