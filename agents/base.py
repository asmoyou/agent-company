import asyncio
import contextlib
import json
import os
import re
import signal
import shutil
import tempfile
import time
from pathlib import Path

import httpx

PROJECT_ROOT    = Path(__file__).parent.parent
SERVER_URL      = os.getenv("SERVER_URL", "http://localhost:8080")
POLL_INTERVAL   = int(os.getenv("POLL_INTERVAL", "5"))
CLI_TIMEOUT     = int(os.getenv("CLI_TIMEOUT", "300"))
HEARTBEAT_SECS  = int(os.getenv("HEARTBEAT_SECS", "45"))
STATUS_HEARTBEAT_SECS = int(os.getenv("STATUS_HEARTBEAT_SECS", "20"))
AGENT_POST_TIMEOUT_SECS = float(os.getenv("AGENT_POST_TIMEOUT_SECS", "8"))
TASK_LEASE_TTL_SECS = int(os.getenv("TASK_LEASE_TTL_SECS", "180"))
TASK_LEASE_RENEW_INTERVAL_SECS = int(os.getenv("TASK_LEASE_RENEW_INTERVAL_SECS", "20"))
TASK_LEASE_RENEW_WARN_AFTER_ERRORS = int(os.getenv("TASK_LEASE_RENEW_WARN_AFTER_ERRORS", "3"))
TASK_LEASE_RENEW_FAIL_HARD_AFTER_ERRORS = int(
    os.getenv("TASK_LEASE_RENEW_FAIL_HARD_AFTER_ERRORS", "9")
)
TASK_STATUS_POLL_FAILURE_MAX = int(os.getenv("TASK_STATUS_POLL_FAILURE_MAX", "30"))
OUTPUT_POST_MAX_INFLIGHT = int(os.getenv("OUTPUT_POST_MAX_INFLIGHT", "12"))
AGENT_API_TOKEN = str(os.getenv("AGENT_API_TOKEN", "opc-agent-internal")).strip()

CLI_TEMPLATES = {
    "claude": ["claude", "--dangerously-skip-permissions", "-p", "{prompt}"],
    # Use non-interactive subcommand to avoid TTY requirement in agent subprocesses.
    "codex":  ["codex", "exec", "--dangerously-bypass-approvals-and-sandbox", "{prompt}"],
}
# External stdin auto-reply is disabled by default; enable only if explicitly configured.
AUTO_REPLY_MAX = int(os.getenv("AUTO_REPLY_MAX", "0"))
# Idle fallback auto-reply is disabled by default to avoid blind ENTER loops.
AUTO_REPLY_IDLE_SECS = int(os.getenv("AUTO_REPLY_IDLE_SECS", "0"))
AUTO_REPLY_TEXT = os.getenv("AUTO_REPLY_TEXT", "\n")
HANDOFF_SYNC_STRATEGY = os.getenv("HANDOFF_SYNC_STRATEGY", "cherry-pick").strip().lower()
CODEX_ENABLE_OUTPUT_SCHEMA = os.getenv("CODEX_ENABLE_OUTPUT_SCHEMA", "0").strip().lower() in {"1", "true", "yes", "on"}
CONTEXT_MAX_CHARS = int(os.getenv("CONTEXT_MAX_CHARS", "3600"))
CONTEXT_MAX_HANDOFFS = int(os.getenv("CONTEXT_MAX_HANDOFFS", "24"))
CONTEXT_MAX_UNRESOLVED_FEEDBACK = int(os.getenv("CONTEXT_MAX_UNRESOLVED_FEEDBACK", "12"))
INTERACTIVE_PROMPT_RE = re.compile(
    r"(?i)(press\s+enter(?:\s+to\s+continue)?|hit\s+enter(?:\s+to\s+continue)?|"
    r"请按回车(?:继续)?|按下回车键|按回车继续|回车继续)"
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
        auth_headers = {"X-Agent-Token": AGENT_API_TOKEN} if AGENT_API_TOKEN else {}
        # trust_env=False: ignore system proxy (SOCKS etc.) for localhost calls
        self.http = httpx.AsyncClient(
            base_url=SERVER_URL,
            timeout=30,
            trust_env=False,
            headers=auth_headers,
        )
        # Use a separate, bounded channel for terminal stream output so
        # control-plane calls (lease renew/status/task polling) are not starved.
        self.http_output = httpx.AsyncClient(
            base_url=SERVER_URL,
            timeout=AGENT_POST_TIMEOUT_SECS,
            trust_env=False,
            headers=auth_headers,
            limits=httpx.Limits(max_keepalive_connections=8, max_connections=24),
        )
        self._output_post_sem = asyncio.Semaphore(max(1, OUTPUT_POST_MAX_INFLIGHT))
        self.project_id_scope: str | None = str(os.getenv("AGENT_PROJECT_ID", "")).strip() or None
        self.worker_id: str | None = str(os.getenv("AGENT_WORKER_ID", "")).strip() or None
        self._active_task_id: str | None = None
        self._active_run_id: str | None = None
        self._active_lease_token: str | None = None
        self._active_project_id: str | None = None
        self._active_lease_lost: bool = False
        self._active_phase: str = ""
        self._active_cli_pid: int | None = None

    # ── HTTP helpers ─────────────────────────────────────────────────────────

    async def fetch_tasks(self, status: str) -> list[dict]:
        r = await self.http.get(f"/tasks/status/{status}")
        r.raise_for_status()
        return r.json()

    async def claim_task(
        self, status: str, working_status: str, respect_assignment: bool, project_id: str | None = None
    ) -> dict | None:
        scope_project_id = project_id
        if scope_project_id is None:
            scope_project_id = self.project_id_scope
        r = await self.http.post(
            "/tasks/claim",
            json={
                "status": status,
                "working_status": working_status,
                "agent": self.name,
                "agent_key": normalize_agent_key(self.name),
                "respect_assignment": respect_assignment,
                "lease_ttl_secs": TASK_LEASE_TTL_SECS,
                "project_id": scope_project_id,
            },
        )
        r.raise_for_status()
        return r.json().get("task")

    def _lease_guard_fields(self, task_id: str | None = None) -> dict:
        if not self._active_run_id or not self._active_lease_token:
            return {}
        if task_id and self._active_task_id and task_id != self._active_task_id:
            return {}
        return {
            "run_id": self._active_run_id,
            "lease_token": self._active_lease_token,
        }

    def _transition_guard_fields(self, task_id: str | None = None) -> dict:
        if not self._active_run_id or not self._active_lease_token:
            return {}
        if task_id and self._active_task_id and task_id != self._active_task_id:
            return {}
        return {
            "expected_run_id": self._active_run_id,
            "expected_lease_token": self._active_lease_token,
        }

    async def renew_task_lease(
        self,
        task_id: str,
        run_id: str,
        lease_token: str,
        lease_ttl_secs: int = TASK_LEASE_TTL_SECS,
    ) -> bool | None:
        try:
            r = await self.http.post(
                f"/tasks/{task_id}/lease/renew",
                json={
                    "run_id": run_id,
                    "lease_token": lease_token,
                    "lease_ttl_secs": lease_ttl_secs,
                },
                timeout=AGENT_POST_TIMEOUT_SECS,
            )
            if r.status_code in (404, 409):
                return False
            r.raise_for_status()
            return True
        except httpx.HTTPStatusError as e:
            if e.response is not None and e.response.status_code in (404, 409):
                return False
            return None
        except Exception:
            return None

    async def update_task(self, task_id: str, **fields) -> dict:
        r = await self.http.patch(f"/tasks/{task_id}", json=fields)
        r.raise_for_status()
        return r.json()

    async def transition_task(
        self,
        task_id: str,
        *,
        fields: dict | None = None,
        handoff: dict | None = None,
        log_message: str | None = None,
        log_agent: str | None = None,
    ) -> dict | None:
        payload: dict = {}
        if fields:
            payload["fields"] = fields
        if handoff:
            handoff_payload = dict(handoff)
            handoff_payload.setdefault("from_agent", self.name)
            payload["handoff"] = handoff_payload
        if log_message is not None:
            payload["log"] = {
                "agent": log_agent or self.name,
                "message": log_message,
            }
        payload.update(self._transition_guard_fields(task_id))
        r = await self.http.post(f"/tasks/{task_id}/transition", json=payload)
        if r.status_code == 404:
            # Task may be deleted/cancelled while agent is still finishing up.
            self._post_output_bg(f"ℹ 任务不存在，停止同步: {task_id}")
            return None
        r.raise_for_status()
        return r.json()

    async def get_task(self, task_id: str) -> dict:
        r = await self.http.get(f"/tasks/{task_id}")
        r.raise_for_status()
        return r.json()

    async def is_task_cancelled(self, task_id: str) -> bool:
        try:
            task = await self.get_task(task_id)
        except httpx.HTTPStatusError as e:
            # 404 means task has been removed/invalidated; treat as cancelled.
            if e.response is not None and e.response.status_code == 404:
                return True
            return False
        except Exception:
            return False
        if not task:
            return True
        status = str(task.get("status") or "").strip().lower()
        archived = int(task.get("archived") or 0)
        return status == "cancelled" or archived == 1

    async def get_task_if_exists(self, task_id: str) -> dict | None:
        try:
            task = await self.get_task(task_id)
            return task if task else None
        except httpx.HTTPStatusError as e:
            if e.response is not None and e.response.status_code == 404:
                return None
            raise

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
                json={"agent": self.name, "message": message, **self._lease_guard_fields(task_id)},
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

    def _parse_feedback_history(self, raw) -> list[dict]:
        data = raw
        if isinstance(raw, str):
            txt = raw.strip()
            if not txt:
                return []
            try:
                data = json.loads(txt)
            except Exception:
                return []
        if not isinstance(data, list):
            return []
        out = []
        for item in data:
            if not isinstance(item, dict):
                continue
            feedback = str(item.get("feedback") or "").strip()
            if not feedback:
                continue
            resolved_at = str(item.get("resolved_at") or "").strip()
            out.append(
                {
                    "id": str(item.get("id") or "").strip(),
                    "created_at": str(item.get("created_at") or "").strip(),
                    "source": str(item.get("source") or "").strip() or "system",
                    "feedback": feedback[:1200],
                    "resolved": bool(item.get("resolved")) or bool(resolved_at),
                    "resolved_at": resolved_at,
                }
            )
        return out

    def _fit_recent_lines(self, newest_lines: list[str], char_budget: int) -> list[str]:
        if char_budget <= 0:
            return []
        kept: list[str] = []
        used = 0
        for line in newest_lines:
            add = len(line) + 1
            if kept and used + add > char_budget:
                break
            if not kept and add > char_budget:
                kept.append(line[: max(10, char_budget - 1)])
                break
            kept.append(line)
            used += add
        return kept

    def _build_unresolved_feedback_lines(self, task: dict | None) -> list[str]:
        if not task:
            return []
        history = self._parse_feedback_history(task.get("review_feedback_history"))
        unresolved = [h for h in history if not bool(h.get("resolved"))]
        unresolved = unresolved[-max(1, CONTEXT_MAX_UNRESOLVED_FEEDBACK) :]

        # Backward compatibility for historical rows without history payload.
        if not unresolved:
            status = str(task.get("status") or "").strip().lower()
            fallback_feedback = str(task.get("review_feedback") or "").strip()
            if fallback_feedback and status in {"needs_changes", "blocked"}:
                unresolved = [
                    {
                        "id": "FB0000",
                        "source": "legacy",
                        "created_at": str(task.get("updated_at") or "").strip(),
                        "feedback": fallback_feedback[:1200],
                    }
                ]
        if not unresolved:
            return []

        lines = ["## 未解决修改意见（必须全部处理）"]
        for item in unresolved:
            fid = str(item.get("id") or "").strip() or "FB????"
            source = str(item.get("source") or "").strip() or "system"
            created = str(item.get("created_at") or "").strip().replace("T", " ")[:19]
            feedback = str(item.get("feedback") or "").strip()
            meta = f"{fid} | {source}"
            if created:
                meta += f" | {created}"
            lines.append(f"- [{meta}] {feedback[:320]}")
        return lines

    async def build_handoff_context(self, task_id: str, limit: int = 6) -> str:
        budget = max(1200, CONTEXT_MAX_CHARS)
        max_handoffs = max(limit, CONTEXT_MAX_HANDOFFS)
        sections: list[str] = []

        try:
            task = await self.get_task(task_id)
        except Exception:
            task = None

        unresolved_lines = self._build_unresolved_feedback_lines(task)
        if unresolved_lines:
            unresolved_budget = max(400, int(budget * 0.45))
            kept = self._fit_recent_lines(list(reversed(unresolved_lines[1:])), unresolved_budget - 40)
            kept.reverse()
            if kept:
                sections.append("\n".join([unresolved_lines[0], *kept]))

        handoffs = await self.get_handoffs(task_id)
        if handoffs:
            newest = []
            for h in reversed(handoffs[-max_handoffs:]):
                stage = str(h.get("stage") or "").strip() or "-"
                from_agent = str(h.get("from_agent") or "").strip() or "-"
                to_agent = str(h.get("to_agent") or "").strip() or "-"
                summary = str(h.get("summary") or "").strip()
                commit_hash = str(h.get("commit_hash") or "").strip()
                conclusion = str(h.get("conclusion") or "").strip()
                info = conclusion or summary
                line = f"- {from_agent} -> {to_agent} [{stage}]"
                if commit_hash:
                    line += f" commit={commit_hash}"
                if info:
                    line += f" | {info[:180]}"
                newest.append(line)

            used = sum(len(s) + 2 for s in sections)
            handoff_budget = max(300, budget - used - 40)
            kept = self._fit_recent_lines(newest, handoff_budget)
            kept.reverse()
            if kept:
                omitted = max(0, len(newest) - len(kept))
                lines = ["## 历史交接记录（预算内）", *kept]
                if omitted:
                    lines.append(f"- ... 已省略更早 {omitted} 条交接")
                sections.append("\n".join(lines))

        text = "\n\n".join(s for s in sections if s).strip()
        if len(text) > budget:
            return text[: budget - 20].rstrip() + "\n... (context truncated)"
        return text

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

    def _extract_handoff_related_commits(self, handoff: dict) -> list[dict]:
        payload = handoff.get("payload")
        if not isinstance(payload, dict):
            return []
        raw_items = payload.get("related_history_commits")
        if not isinstance(raw_items, list):
            return []
        out: list[dict] = []
        for item in raw_items:
            if not isinstance(item, dict):
                continue
            commit_hash = str(item.get("hash") or item.get("commit_hash") or "").strip()
            if not commit_hash:
                continue
            out.append(
                {
                    "hash": commit_hash,
                    "short": str(item.get("short") or "").strip(),
                    "subject": str(item.get("subject") or "").strip(),
                    "created_at": str(item.get("created_at") or "").strip(),
                    "score": int(item.get("score") or 0),
                }
            )
        return out

    def _extract_handoff_commit_candidates(self, handoff: dict) -> list[str]:
        candidates: list[str] = []
        primary = self._extract_handoff_commit(handoff)
        if primary:
            candidates.append(primary)
        for item in self._extract_handoff_related_commits(handoff):
            h = str(item.get("hash") or "").strip()
            if h:
                candidates.append(h)
        # preserve order, dedupe
        uniq: list[str] = []
        seen: set[str] = set()
        for c in candidates:
            if c in seen:
                continue
            seen.add(c)
            uniq.append(c)
        return uniq

    async def resolve_handoff_commit_candidate(
        self,
        task_id: str,
        repo_root: Path,
    ) -> tuple[str, list[dict]]:
        handoff = await self.get_latest_handoff_for_agent(task_id)
        if not handoff:
            return "", []
        related = self._extract_handoff_related_commits(handoff)
        for commit_hash in self._extract_handoff_commit_candidates(handoff):
            try:
                await self.git("cat-file", "-e", f"{commit_hash}^{{commit}}", cwd=repo_root)
                return commit_hash, related
            except Exception:
                continue
        return "", related

    def _task_commit_keywords(self, task: dict) -> list[str]:
        text = " ".join(
            [
                str(task.get("id") or ""),
                str(task.get("title") or ""),
                str(task.get("description") or ""),
            ]
        ).lower()
        latin = [tok for tok in re.split(r"[^a-z0-9]+", text) if len(tok) >= 3]
        zh = re.findall(r"[\u4e00-\u9fff]{2,}", text)
        words = latin + zh
        # preserve order + dedupe
        out: list[str] = []
        seen: set[str] = set()
        for w in words:
            if w in seen:
                continue
            seen.add(w)
            out.append(w)
        return out[:24]

    async def collect_task_related_commits(
        self,
        task: dict,
        repo_root: Path,
        *,
        max_count: int = 6,
        scan_count: int = 120,
    ) -> list[dict]:
        try:
            raw = await self.git(
                "log",
                f"-n{max(20, int(scan_count))}",
                "--pretty=format:%H%x1f%h%x1f%s%x1f%ci",
                cwd=repo_root,
            )
        except Exception:
            return []
        keywords = self._task_commit_keywords(task)
        task_id = str(task.get("id") or "").strip().lower()
        rows: list[dict] = []
        for ln in str(raw or "").splitlines():
            parts = ln.split("\x1f")
            if len(parts) < 4:
                continue
            full_hash, short_hash, subject, created_at = parts[0], parts[1], parts[2], parts[3]
            hay = subject.lower()
            score = 0
            if task_id and task_id in hay:
                score += 3
            for kw in keywords:
                if kw and kw in hay:
                    score += 1
            rows.append(
                {
                    "hash": full_hash.strip(),
                    "short": short_hash.strip(),
                    "subject": subject.strip(),
                    "created_at": created_at.strip(),
                    "score": score,
                }
            )
        if not rows:
            return []
        ranked = sorted(rows, key=lambda x: (int(x.get("score") or 0), str(x.get("created_at") or "")), reverse=True)
        selected = [r for r in ranked if int(r.get("score") or 0) > 0]
        if not selected:
            selected = rows[:3]
        # dedupe by commit hash and trim
        out: list[dict] = []
        seen: set[str] = set()
        for item in selected:
            h = str(item.get("hash") or "").strip()
            if not h or h in seen:
                continue
            seen.add(h)
            out.append(item)
            if len(out) >= max(1, int(max_count)):
                break
        return out

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
            await self.add_alert(
                summary="交接 commit 不存在",
                task_id=task_id,
                message=msg,
                kind="error",
                code=f"{self.name}_sync_missing_commit",
                stage=f"{self.name}_sync_failed",
                metadata={"commit_hash": commit_hash, "source_branch": source_branch},
            )
            await self.transition_task(
                task_id,
                fields={
                    "status": "blocked",
                    "assignee": None,
                    "assigned_agent": self.name,
                    "review_feedback": msg[:500],
                    "feedback_source": self.name,
                    "feedback_stage": f"{self.name}_sync_failed",
                    "feedback_actor": self.name,
                },
                handoff={
                    "stage": f"{self.name}_sync_failed",
                    "to_agent": self.name,
                    "status_from": task.get("status"),
                    "status_to": "blocked",
                    "title": "交接同步失败",
                    "summary": msg[:300],
                    "commit_hash": commit_hash,
                    "conclusion": "交接 commit 丢失，任务阻塞",
                    "payload": {"reason": "missing_commit_object", "source_branch": source_branch},
                },
                log_message=msg[:500],
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
            await self.transition_task(
                task_id,
                fields={
                    "status": "blocked",
                    "assignee": None,
                    "assigned_agent": self.name,
                    "review_feedback": msg[:500],
                    "feedback_source": self.name,
                    "feedback_stage": f"{self.name}_sync_failed",
                    "feedback_actor": self.name,
                },
                handoff={
                    "stage": f"{self.name}_sync_failed",
                    "to_agent": self.name,
                    "status_from": task.get("status"),
                    "status_to": "blocked",
                    "title": "交接同步失败",
                    "summary": msg[:300],
                    "commit_hash": commit_hash,
                    "conclusion": "交接同步冲突/失败，任务阻塞",
                    "payload": {
                        "reason": "sync_failed",
                        "strategy": strategy,
                        "source_branch": source_branch,
                    },
                    "artifact_path": str(worktree),
                },
                log_message=msg[:500],
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
                    **self._transition_guard_fields(task_id),
                },
            )
        except Exception:
            pass

    # ── Agent terminal output ─────────────────────────────────────────────────

    def _post_output_bg(
        self,
        line: str,
        *,
        kind: str = "meta",
        event: str = "line",
        exit_code: int | None = None,
    ):
        task_id = self._active_task_id
        run_id = self._active_run_id
        project_id = str(self._active_project_id or self.project_id_scope or "").strip() or None
        important = kind in {"meta", "event"} or event in {"started", "finished"}
        if not important and self._output_post_sem.locked():
            # Drop low-priority stream lines when backlog is full.
            return

        async def _send():
            await self._output_post_sem.acquire()
            try:
                await self.http_output.post(
                    f"/agents/{self.name}/output",
                    json={
                        "line": line,
                        "kind": kind,
                        "event": event,
                        "exit_code": exit_code,
                        "agent_key": normalize_agent_key(self.name),
                        "worker_id": self.worker_id,
                        "project_id": project_id,
                        "task_id": task_id,
                        "run_id": run_id,
                    },
                    timeout=AGENT_POST_TIMEOUT_SECS,
                )
            except Exception:
                pass
            finally:
                self._output_post_sem.release()
        asyncio.create_task(_send())

    async def set_agent_status(
        self,
        status: str,
        task_title: str = "",
        *,
        project_id: str | None = None,
        task_id: str | None = None,
        run_id: str | None = None,
        lease_token: str | None = None,
        phase: str | None = None,
        pid: int | None = None,
    ):
        busy = str(status or "").strip().lower() == "busy"
        if busy:
            project_id = project_id if project_id is not None else (self._active_project_id or self.project_id_scope)
            task_id = task_id if task_id is not None else self._active_task_id
            run_id = run_id if run_id is not None else self._active_run_id
            lease_token = lease_token if lease_token is not None else self._active_lease_token
            phase = phase if phase is not None else self._active_phase
            pid = pid if pid is not None else self._active_cli_pid
        else:
            project_id = project_id if project_id is not None else self.project_id_scope
            task_id = ""
            run_id = ""
            lease_token = ""
            phase = ""
            pid = None
        try:
            await self.http.post(
                f"/agents/{self.name}/status",
                json={
                    "status": status,
                    "task": task_title,
                    "agent_key": normalize_agent_key(self.name),
                    "worker_id": self.worker_id,
                    "project_id": project_id,
                    "task_id": task_id,
                    "run_id": run_id,
                    "lease_token": lease_token,
                    "phase": phase,
                    "pid": pid,
                },
                timeout=AGENT_POST_TIMEOUT_SECS,
            )
        except Exception:
            pass

    async def _terminate_proc_tree(self, proc: asyncio.subprocess.Process) -> None:
        """
        Best-effort termination for subprocess and any children.
        We start subprocesses in a new session and kill the whole process group.
        """
        if proc.returncode is not None:
            return
        try:
            if hasattr(os, "killpg"):
                os.killpg(proc.pid, signal.SIGKILL)
                return
        except ProcessLookupError:
            return
        except Exception:
            pass
        with contextlib.suppress(ProcessLookupError, Exception):
            proc.kill()

    # ── Git ───────────────────────────────────────────────────────────────────

    async def git(self, *args: str, cwd: Path, task_id: str | None = None) -> str:
        proc = await asyncio.create_subprocess_exec(
            "git", *args, cwd=str(cwd),
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            start_new_session=True,
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
                    await self._terminate_proc_tree(proc)
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
        expected_status: str | None = None,
        expected_assignee: str | None = None,
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

            if output_schema is not None and CODEX_ENABLE_OUTPUT_SCHEMA:
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
            elif output_schema is not None:
                self._post_output_bg("ℹ codex output_schema 已禁用（使用文件产物做结构化校验）", kind="meta")

            cmd.append(prompt)
        else:
            cmd = build_cli_cmd(self.cli_name, prompt)
        print(f"[{self.name}] Spawning {cmd[0]} (cwd={cwd.name})")
        self._post_output_bg(f"$ {cmd[0]}  cwd={cwd}", kind="meta")
        env = os.environ.copy()
        # Hint CLIs to avoid interactive flows.
        env.setdefault("CI", "1")
        env.setdefault("NONINTERACTIVE", "1")

        self._active_phase = "cli_spawning"
        proc = await asyncio.create_subprocess_exec(
            *cmd, cwd=str(cwd),
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            env=env,
            start_new_session=True,
        )
        self._active_cli_pid = int(proc.pid) if proc.pid is not None else None
        self._active_phase = "cli_running"
        self._post_output_bg(
            f"▶ CLI 已启动: {cmd[0]} (pid={self._active_cli_pid or '-'})",
            kind="event",
            event="started",
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
                    self._post_output_bg(msg, kind="meta")
                    if task_id:
                        await self.add_log(task_id, msg)
                except Exception:
                    pass

        async def drain(stream, stream_kind: str):
            nonlocal last_output_at
            async for raw in stream:
                line = raw.decode(errors="replace").rstrip("\n")
                if line:
                    last_output_at = time.monotonic()
                    lines.append(line)
                    self._post_output_bg(line, kind=stream_kind)
                    if INTERACTIVE_PROMPT_RE.search(line):
                        await auto_reply(f"检测到交互提示: {line}")

        async def heartbeat():
            while True:
                await asyncio.sleep(HEARTBEAT_SECS)
                if proc.returncode is not None:
                    return
                elapsed = int(time.monotonic() - start_time)
                msg = f"⏳ 仍在工作中... 已运行 {elapsed}s"
                self._post_output_bg(msg, kind="meta")
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
            expected_status_lc = str(expected_status or "").strip().lower()
            expected_assignee_lc = str(expected_assignee or "").strip().lower()
            consecutive_api_errors = 0
            while proc.returncode is None:
                await asyncio.sleep(1)
                if proc.returncode is not None:
                    return
                if self._active_task_id == task_id and self._active_lease_lost:
                    msg = "🛑 检测到任务租约失效，终止当前 CLI 执行"
                    lines.append(msg)
                    self._post_output_bg(msg, kind="meta")
                    await self._terminate_proc_tree(proc)
                    return
                try:
                    task = await self.get_task_if_exists(task_id)
                    consecutive_api_errors = 0
                except Exception:
                    # Short transient API errors are tolerated. Prolonged control-plane
                    # failures should stop execution to avoid wedging the task.
                    consecutive_api_errors += 1
                    if consecutive_api_errors >= max(3, TASK_STATUS_POLL_FAILURE_MAX):
                        msg = (
                            f"🛑 连续 {consecutive_api_errors}s 无法获取任务状态，"
                            "终止当前 CLI 执行以避免任务卡住"
                        )
                        lines.append(msg)
                        self._post_output_bg(msg, kind="meta")
                        await self._terminate_proc_tree(proc)
                        return
                    continue
                if not task:
                    msg = "🛑 检测到任务不存在，终止当前 CLI 执行"
                    lines.append(msg)
                    self._post_output_bg(msg, kind="meta")
                    await self._terminate_proc_tree(proc)
                    return
                status = str(task.get("status") or "").strip().lower()
                archived = int(task.get("archived") or 0)
                if status == "cancelled" or archived == 1:
                    msg = "🛑 检测到任务已取消/归档，终止当前 CLI 执行"
                    lines.append(msg)
                    self._post_output_bg(msg, kind="meta")
                    await self._terminate_proc_tree(proc)
                    return
                if expected_status_lc and status != expected_status_lc:
                    msg = f"🛑 检测到任务状态变更为 {status}，终止当前 CLI 执行"
                    lines.append(msg)
                    self._post_output_bg(msg, kind="meta")
                    await self._terminate_proc_tree(proc)
                    return
                if expected_assignee_lc:
                    assignee = str(task.get("assignee") or "").strip().lower()
                    if assignee != expected_assignee_lc:
                        msg = (
                            f"🛑 检测到任务 assignee 变更为 {assignee or '(空)'}，"
                            "终止当前 CLI 执行"
                        )
                        lines.append(msg)
                        self._post_output_bg(msg, kind="meta")
                        await self._terminate_proc_tree(proc)
                        return

        hb = asyncio.create_task(heartbeat())
        iar = asyncio.create_task(idle_auto_reply())
        cwatch = asyncio.create_task(cancel_watcher())
        try:
            await asyncio.wait_for(
                asyncio.gather(
                    drain(proc.stdout, "stdout"),
                    drain(proc.stderr, "stderr"),
                ),
                timeout=CLI_TIMEOUT,
            )
        except asyncio.TimeoutError:
            self._active_phase = "cli_timeout"
            await self._terminate_proc_tree(proc)
            lines.append(f"[TIMEOUT after {CLI_TIMEOUT}s]")
            self._post_output_bg(f"⚠ TIMEOUT after {CLI_TIMEOUT}s", kind="meta")
        finally:
            for t in (hb, iar, cwatch):
                t.cancel()
            for t in (hb, iar, cwatch):
                with contextlib.suppress(asyncio.CancelledError):
                    await t

        await proc.wait()
        self._post_output_bg(
            f"■ CLI 结束: exit={proc.returncode}",
            kind="event",
            event="finished",
            exit_code=proc.returncode,
        )
        self._active_cli_pid = None
        if self._active_phase in {"cli_spawning", "cli_running", "cli_timeout"}:
            self._active_phase = "running"

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
                        self._active_task_id = task["id"]
                        self._active_run_id = str(task.get("claim_run_id") or "").strip() or None
                        self._active_lease_token = str(task.get("lease_token") or "").strip() or None
                        self._active_project_id = str(task.get("project_id") or "").strip() or self.project_id_scope
                        self._active_lease_lost = False
                        self._active_phase = "claimed"
                        self._active_cli_pid = None
                        await self.set_agent_status("busy", task["title"])
                        self._post_output_bg(f"▶ 任务: {task['title']}")
                        async def busy_status_heartbeat():
                            while True:
                                await asyncio.sleep(STATUS_HEARTBEAT_SECS)
                                await self.set_agent_status("busy", task["title"])

                        async def lease_heartbeat():
                            consecutive_errors = 0
                            while True:
                                await asyncio.sleep(TASK_LEASE_RENEW_INTERVAL_SECS)
                                if self._active_task_id != task["id"]:
                                    return
                                if self._active_lease_lost:
                                    return
                                run_id = str(self._active_run_id or "").strip()
                                lease_token = str(self._active_lease_token or "").strip()
                                if not run_id or not lease_token:
                                    return
                                renewed = await self.renew_task_lease(
                                    task_id=task["id"],
                                    run_id=run_id,
                                    lease_token=lease_token,
                                    lease_ttl_secs=TASK_LEASE_TTL_SECS,
                                )
                                if renewed is True:
                                    consecutive_errors = 0
                                    if self._active_phase in {"lease_retry", "claimed"}:
                                        self._active_phase = "running"
                                    continue
                                if renewed is False:
                                    self._active_lease_lost = True
                                    self._active_phase = "lease_lost"
                                    self._post_output_bg(
                                        "🛑 租约续期失败，任务可能已被回收/接管，停止当前执行"
                                    )
                                    return
                                consecutive_errors += 1
                                if consecutive_errors >= TASK_LEASE_RENEW_WARN_AFTER_ERRORS:
                                    self._active_phase = "lease_retry"
                                    self._post_output_bg(
                                        f"⚠ 租约续期连续失败 {consecutive_errors} 次（将继续重试）"
                                    )
                                if consecutive_errors >= TASK_LEASE_RENEW_FAIL_HARD_AFTER_ERRORS:
                                    self._active_lease_lost = True
                                    self._active_phase = "lease_unreachable"
                                    msg = (
                                        f"🛑 租约续期连续失败 {consecutive_errors} 次，"
                                        "主动终止当前执行并重新进入认领循环"
                                    )
                                    self._post_output_bg(msg)
                                    await self.add_log(task["id"], msg)
                                    return

                        busy_hb = asyncio.create_task(busy_status_heartbeat())
                        lease_hb = asyncio.create_task(lease_heartbeat())
                        try:
                            self._active_phase = "running"
                            await self.process_task(task)
                        except Exception as e:
                            self._active_phase = "failed"
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
                            for t in (busy_hb, lease_hb):
                                t.cancel()
                            for t in (busy_hb, lease_hb):
                                with contextlib.suppress(asyncio.CancelledError):
                                    await t
                            self._active_task_id = None
                            self._active_run_id = None
                            self._active_lease_token = None
                            self._active_project_id = None
                            self._active_lease_lost = False
                            self._active_phase = ""
                            self._active_cli_pid = None
                        await self.set_agent_status("idle")
                        self._post_output_bg("─── 等待下一个任务 ───")
                except Exception as e:
                    msg = f"Poll error ({status}): {e}"
                    print(f"[{self.name}] {msg}")
                    self._post_output_bg(f"⚠ {msg[:300]}")

            # Sleep for POLL_INTERVAL, but wake up early if shutdown is set
            try:
                await asyncio.wait_for(self.shutdown.wait(), timeout=POLL_INTERVAL)
            except asyncio.TimeoutError:
                pass  # Normal timeout, continue polling

        self._post_output_bg(f"[{self.name}] 已收到关闭信号，停止接受新任务")
        print(f"[{self.name}] Stopped.")
        await self.http.aclose()
        await self.http_output.aclose()
