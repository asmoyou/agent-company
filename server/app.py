import asyncio
import contextlib
import hmac
import json
import logging
import mimetypes
import os
import re
import shutil
import sqlite3
import subprocess
import sys
import time
from collections import defaultdict, deque
from contextlib import asynccontextmanager
from datetime import datetime
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Literal

from fastapi import (
    Depends,
    FastAPI,
    File as FastAPIFile,
    Header,
    HTTPException,
    Query,
    Request,
    UploadFile,
    WebSocket,
    WebSocketDisconnect,
)
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

import sys
sys.path.insert(0, str(Path(__file__).parent))
import db

PROJECT_ROOT = Path(__file__).parent.parent
REQUEST_LOG_FILE = Path(str(os.getenv("REQUEST_LOG_FILE", "logs/api-access.log")).strip()).expanduser()
if not REQUEST_LOG_FILE.is_absolute():
    REQUEST_LOG_FILE = PROJECT_ROOT / REQUEST_LOG_FILE
REQUEST_LOG_MAX_BYTES = max(
    1024,
    int(str(os.getenv("REQUEST_LOG_MAX_BYTES", str(10 * 1024 * 1024))).strip() or str(10 * 1024 * 1024)),
)
REQUEST_LOG_BACKUP_COUNT = max(
    1,
    int(str(os.getenv("REQUEST_LOG_BACKUP_COUNT", "5")).strip() or "5"),
)
AGENT_STALE_SECS = int(os.getenv("AGENT_STALE_SECS", "150"))
AGENT_WATCHDOG_SECS = int(os.getenv("AGENT_WATCHDOG_SECS", "15"))
TASK_LEASE_TTL_SECS = int(os.getenv("TASK_LEASE_TTL_SECS", "180"))
TASK_LEASE_RENEW_MIN_SECS = int(os.getenv("TASK_LEASE_RENEW_MIN_SECS", "30"))
TASK_LEASE_RENEW_MAX_SECS = int(os.getenv("TASK_LEASE_RENEW_MAX_SECS", "1800"))
TASK_LEASE_RECOVERY_GRACE_SECS = int(os.getenv("TASK_LEASE_RECOVERY_GRACE_SECS", "0"))
AGENT_API_TOKEN = str(os.getenv("AGENT_API_TOKEN", "opc-agent-internal")).strip()
AUTO_CLEANUP_TASK_WORKSPACES = str(
    os.getenv("AUTO_CLEANUP_TASK_WORKSPACES", "1")
).strip().lower() in {"1", "true", "yes", "on"}
TASK_WORKSPACE_FORCE_DELETE_UNMERGED = str(
    os.getenv("TASK_WORKSPACE_FORCE_DELETE_UNMERGED", "0")
).strip().lower() in {"1", "true", "yes", "on"}
TASK_WORKSPACE_SWEEP_SECS = int(os.getenv("TASK_WORKSPACE_SWEEP_SECS", "180"))
TASK_WORKSPACE_SWEEP_BATCH_SIZE = int(os.getenv("TASK_WORKSPACE_SWEEP_BATCH_SIZE", "200"))
TASK_WORKSPACE_CLEANUP_HISTORY_LIMIT = int(
    os.getenv("TASK_WORKSPACE_CLEANUP_HISTORY_LIMIT", "300")
)
STRICT_CLAIM_SCOPE = str(os.getenv("FEATURE_STRICT_CLAIM_SCOPE", "1")).strip().lower() in {
    "1",
    "true",
    "yes",
    "on",
}
PER_PROJECT_MAX_WORKERS = int(os.getenv("PER_PROJECT_MAX_WORKERS", "0"))
PER_AGENT_TYPE_MAX_WORKERS = int(os.getenv("PER_AGENT_TYPE_MAX_WORKERS", "0"))


def _init_request_access_logger() -> logging.Logger:
    logger = logging.getLogger("opc.request.access")
    logger.setLevel(logging.INFO)
    logger.propagate = False
    if logger.handlers:
        return logger
    try:
        REQUEST_LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
        handler = RotatingFileHandler(
            str(REQUEST_LOG_FILE),
            maxBytes=REQUEST_LOG_MAX_BYTES,
            backupCount=REQUEST_LOG_BACKUP_COUNT,
            encoding="utf-8",
        )
        handler.setFormatter(
            logging.Formatter(
                "[%(asctime)s] %(message)s",
                "%Y-%m-%d %H:%M:%S",
            )
        )
        logger.addHandler(handler)
    except Exception:
        logger.addHandler(logging.NullHandler())
    return logger


REQUEST_ACCESS_LOG = _init_request_access_logger()


def normalize_agent_key(agent_key: str | None, default: str = "developer") -> str:
    raw = (agent_key or default).strip().lower()
    safe = re.sub(r"[^a-z0-9_-]+", "-", raw).strip("-_")
    return safe or default


def normalize_worker_key(worker_id: str | None, default: str = "") -> str:
    raw = (worker_id or default).strip().lower()
    safe = re.sub(r"[^a-z0-9_-]+", "-", raw).strip("-_")
    return safe or default


def resolve_agent_runtime_id(
    *,
    agent_name: str,
    worker_id: str | None = None,
    project_id: str | None = None,
) -> tuple[str, str]:
    agent_key = normalize_agent_key(agent_name, default=agent_name)
    runtime = normalize_worker_key(worker_id)
    if runtime:
        return runtime, agent_key
    return agent_key, agent_key


def assignee_matches_agent_type(assignee: str | None, agent_key: str | None) -> bool:
    owner = normalize_agent_key(assignee, default="")
    key = normalize_agent_key(agent_key, default="")
    if not owner or not key:
        return False
    return owner == key or owner.startswith(f"{key}__")


def task_dev_agent(task: dict) -> str:
    # Prefer explicit dev ownership; fall back to current assignee for
    # claimed tasks that have not persisted dev_agent/assigned_agent yet.
    candidate = task.get("dev_agent") or task.get("assigned_agent")
    if not str(candidate or "").strip():
        candidate = task.get("assignee")
    return normalize_agent_key(candidate or "developer")


def task_scope_suffix(task: dict) -> str:
    task_id = str(task.get("id") or "").strip().lower()
    if not task_id:
        return ""
    return re.sub(r"[^a-z0-9_-]+", "-", task_id).strip("-_")


def task_dev_branch(task: dict) -> str:
    base = f"agent/{task_dev_agent(task)}"
    suffix = task_scope_suffix(task)
    return f"{base}/{suffix}" if suffix else base


_TASK_WORKSPACE_CLEANUP_INFLIGHT: set[str] = set()
_TASK_WORKSPACE_CLEANUP_STATE: dict[str, dict] = {}
_TASK_WORKSPACE_CLEANUP_EVENTS: deque = deque(maxlen=max(10, TASK_WORKSPACE_CLEANUP_HISTORY_LIMIT))
_TASK_WORKSPACE_CLEANUP_METRICS: dict[str, int | str] = {
    "scheduled": 0,
    "executed": 0,
    "finalized": 0,
    "failed": 0,
    "last_run_at": "",
    "last_finalized_at": "",
    "last_failed_at": "",
}


def _run_cmd(args: list[str], *, cwd: Path, timeout: int = 30) -> tuple[int, str, str]:
    try:
        proc = subprocess.run(
            args,
            cwd=str(cwd),
            capture_output=True,
            text=True,
            timeout=timeout,
        )
        return proc.returncode, proc.stdout.strip(), proc.stderr.strip()
    except Exception as e:
        return 1, "", str(e)


def _relative_git_path(root: Path, target: Path) -> str | None:
    try:
        rel = target.resolve().relative_to(root.resolve())
    except Exception:
        return None
    rel_posix = rel.as_posix().strip()
    if not rel_posix or rel_posix in {".", ".git"} or rel_posix.startswith(".git/"):
        return None
    return rel_posix


def _auto_commit_project_paths_to_main(
    project_root: Path,
    *,
    changed_paths: list[Path],
    action: str,
) -> dict:
    root = project_root.resolve()
    if not changed_paths:
        return {"status": "skipped", "reason": "empty_paths"}
    if not (root / ".git").exists():
        return {"status": "skipped", "reason": "repo_missing"}

    rel_paths: list[str] = []
    seen: set[str] = set()
    for p in changed_paths:
        rel = _relative_git_path(root, p)
        if rel and rel not in seen:
            seen.add(rel)
            rel_paths.append(rel)
    if not rel_paths:
        return {"status": "skipped", "reason": "no_git_paths"}

    rc_main, out_main, err_main = _run_cmd(["git", "branch", "--list", "main"], cwd=root, timeout=20)
    if rc_main != 0 or not out_main.strip():
        return {
            "status": "skipped",
            "reason": "main_missing",
            "error": (err_main or out_main)[:200],
        }

    rc_cur, out_cur, err_cur = _run_cmd(["git", "branch", "--show-current"], cwd=root, timeout=20)
    if rc_cur != 0:
        return {"status": "error", "reason": "read_branch_failed", "error": err_cur[:200]}
    previous_branch = out_cur.strip()
    switched = False
    restore_warning = ""

    try:
        if previous_branch != "main":
            rc_ck, _, err_ck = _run_cmd(["git", "checkout", "main"], cwd=root, timeout=40)
            if rc_ck != 0:
                return {
                    "status": "error",
                    "reason": "checkout_main_failed",
                    "error": err_ck[:200],
                    "branch": previous_branch,
                }
            switched = True

        rc_add, _, err_add = _run_cmd(
            ["git", "add", "-A", "--", *rel_paths],
            cwd=root,
            timeout=40,
        )
        if rc_add != 0:
            return {"status": "error", "reason": "git_add_failed", "error": err_add[:200]}

        rc_diff, out_diff, err_diff = _run_cmd(
            ["git", "diff", "--cached", "--name-only", "--", *rel_paths],
            cwd=root,
            timeout=30,
        )
        if rc_diff != 0:
            return {"status": "error", "reason": "git_diff_failed", "error": err_diff[:200]}
        staged = [line.strip() for line in out_diff.splitlines() if line.strip()]
        if not staged:
            return {"status": "skipped", "reason": "no_changes"}

        commit_msg = f"chore(files): {action} {len(staged)} path(s)"
        rc_commit, _, err_commit = _run_cmd(
            ["git", "commit", "--only", "-m", commit_msg, "--", *rel_paths],
            cwd=root,
            timeout=60,
        )
        if rc_commit != 0:
            if "nothing to commit" in err_commit.lower():
                return {"status": "skipped", "reason": "no_changes"}
            return {"status": "error", "reason": "git_commit_failed", "error": err_commit[:200]}

        rc_head, out_head, err_head = _run_cmd(["git", "rev-parse", "--short", "HEAD"], cwd=root, timeout=20)
        if rc_head != 0:
            return {
                "status": "committed",
                "branch": "main",
                "paths": staged,
                "commit": "",
                "warning": err_head[:200],
            }
        return {"status": "committed", "branch": "main", "paths": staged, "commit": out_head.strip()}
    finally:
        if switched and previous_branch:
            rc_back, _, err_back = _run_cmd(["git", "checkout", previous_branch], cwd=root, timeout=40)
            if rc_back != 0:
                restore_warning = err_back[:200]
        if restore_warning:
            print(f"[files] WARN: failed to restore branch after auto-commit: {restore_warning}")


def _cleanup_task_workspace_sync(task: dict, reason: str) -> dict:
    task_id = str(task.get("id") or "").strip()
    project_path = str(task.get("project_path") or "").strip()
    status = _norm_status(task.get("status"))
    updated_at = str(task.get("updated_at") or "").strip()

    base = {
        "ok": False,
        "finalized": False,
        "message": "",
        "reason": "",
        "actions": [],
        "warnings": [],
        "status": status,
        "updated_at": updated_at,
    }
    if not task_id or not project_path:
        base["finalized"] = True
        base["reason"] = "missing_task_or_project"
        return base
    if status not in {"completed", "cancelled"}:
        base["finalized"] = True
        base["reason"] = "status_not_terminal"
        return base

    root = Path(project_path)
    if not root.exists() or not (root / ".git").exists():
        base["finalized"] = True
        base["reason"] = "repo_missing"
        return base

    dev_key = task_dev_agent(task)
    scope = task_scope_suffix(task)
    if not scope:
        base["finalized"] = True
        base["reason"] = "missing_task_scope"
        return base
    worktree = root / ".worktrees" / dev_key / scope
    branch = task_dev_branch(task)

    actions: list[str] = []
    warnings: list[str] = []

    if worktree.exists():
        rc, _, err = _run_cmd(
            ["git", "worktree", "remove", "--force", str(worktree)],
            cwd=root,
            timeout=60,
        )
        if rc == 0:
            rel = str(worktree.relative_to(root)) if worktree.is_relative_to(root) else str(worktree)
            actions.append(f"移除工作树 {rel}")
        else:
            warnings.append(f"移除工作树失败: {err[:120] or 'unknown error'}")
    _run_cmd(["git", "worktree", "prune"], cwd=root, timeout=30)

    rc, out, _ = _run_cmd(["git", "branch", "--list", branch], cwd=root, timeout=20)
    branch_exists = rc == 0 and bool(out.strip())
    if branch_exists:
        delete_flag = "-d"
        if status == "cancelled":
            delete_flag = "-D"
        rc_del, _, err_del = _run_cmd(
            ["git", "branch", delete_flag, branch],
            cwd=root,
            timeout=20,
        )
        if rc_del == 0:
            actions.append(f"删除分支 {branch}")
        elif delete_flag == "-d" and TASK_WORKSPACE_FORCE_DELETE_UNMERGED:
            rc_force, _, err_force = _run_cmd(
                ["git", "branch", "-D", branch],
                cwd=root,
                timeout=20,
            )
            if rc_force == 0:
                actions.append(f"强制删除分支 {branch}")
            else:
                warnings.append(f"删除分支失败: {err_force[:120] or 'unknown error'}")
        else:
            warnings.append(f"删除分支失败: {err_del[:120] or 'unknown error'}")

    rc_after, out_after, _ = _run_cmd(["git", "branch", "--list", branch], cwd=root, timeout=20)
    branch_remaining = rc_after == 0 and bool(out_after.strip())
    worktree_remaining = worktree.exists()
    cleanup_done = (not branch_remaining) and (not worktree_remaining)

    if cleanup_done and not actions and not warnings:
        base["ok"] = True
        base["finalized"] = True
        base["reason"] = "nothing_to_cleanup"
        return base

    detail_parts = []
    if actions:
        detail_parts.append("；".join(actions))
    if warnings:
        detail_parts.append("警告: " + "；".join(warnings))
    if worktree_remaining or branch_remaining:
        remain = []
        if worktree_remaining:
            remain.append("worktree仍存在")
        if branch_remaining:
            remain.append("branch仍存在")
        detail_parts.append("剩余: " + "、".join(remain))
    detail = "；".join(detail_parts)
    base["message"] = f"[工作区清理] task={task_id} reason={reason} status={status} -> {detail}"
    base["actions"] = actions
    base["warnings"] = warnings
    if cleanup_done:
        base["ok"] = True
        base["finalized"] = True
        base["reason"] = "done"
        return base

    base["reason"] = "cleanup_incomplete"
    return base


def _record_workspace_cleanup_event(task: dict, trigger: str, result: dict):
    now = datetime.utcnow().isoformat()
    task_id = str(task.get("id") or "").strip()
    project_id = str(task.get("project_id") or "").strip()
    status = _norm_status(task.get("status"))
    event = {
        "at": now,
        "task_id": task_id,
        "project_id": project_id,
        "status": status,
        "trigger": trigger,
        "ok": bool(result.get("ok")),
        "finalized": bool(result.get("finalized")),
        "reason": str(result.get("reason") or ""),
        "actions": list(result.get("actions") or []),
        "warnings": list(result.get("warnings") or []),
        "message": str(result.get("message") or ""),
    }
    _TASK_WORKSPACE_CLEANUP_EVENTS.appendleft(event)
    _TASK_WORKSPACE_CLEANUP_METRICS["last_run_at"] = now
    _TASK_WORKSPACE_CLEANUP_METRICS["executed"] = int(_TASK_WORKSPACE_CLEANUP_METRICS["executed"]) + 1
    if bool(result.get("finalized")):
        _TASK_WORKSPACE_CLEANUP_METRICS["finalized"] = int(_TASK_WORKSPACE_CLEANUP_METRICS["finalized"]) + 1
        _TASK_WORKSPACE_CLEANUP_METRICS["last_finalized_at"] = now
    else:
        _TASK_WORKSPACE_CLEANUP_METRICS["failed"] = int(_TASK_WORKSPACE_CLEANUP_METRICS["failed"]) + 1
        _TASK_WORKSPACE_CLEANUP_METRICS["last_failed_at"] = now
    if task_id:
        _TASK_WORKSPACE_CLEANUP_STATE[task_id] = {
            "status": status,
            "updated_at": str(task.get("updated_at") or ""),
            "ok": bool(result.get("ok")),
            "finalized": bool(result.get("finalized")),
            "reason": str(result.get("reason") or ""),
            "at": now,
        }


def _schedule_task_workspace_cleanup(task: dict, reason: str):
    if not AUTO_CLEANUP_TASK_WORKSPACES:
        return
    task_id = str(task.get("id") or "").strip()
    if not task_id:
        return
    if task_id in _TASK_WORKSPACE_CLEANUP_INFLIGHT:
        return
    _TASK_WORKSPACE_CLEANUP_INFLIGHT.add(task_id)
    _TASK_WORKSPACE_CLEANUP_METRICS["scheduled"] = int(_TASK_WORKSPACE_CLEANUP_METRICS["scheduled"]) + 1

    async def _runner():
        try:
            result = await asyncio.to_thread(_cleanup_task_workspace_sync, dict(task), reason)
            _record_workspace_cleanup_event(task, reason, result)
            message = str(result.get("message") or "").strip()
            if message:
                try:
                    log = db.add_log(task_id, "system", message)
                    await broadcast_log(log)
                except Exception:
                    pass
        finally:
            _TASK_WORKSPACE_CLEANUP_INFLIGHT.discard(task_id)

    asyncio.create_task(_runner())


def _sweep_terminal_task_workspaces(max_tasks: int | None = None, reason: str = "periodic_sweep") -> dict:
    if not AUTO_CLEANUP_TASK_WORKSPACES:
        return {"scheduled": 0, "scanned": 0, "reason": "disabled"}

    limit = max_tasks if max_tasks is not None else TASK_WORKSPACE_SWEEP_BATCH_SIZE
    rows = db.list_terminal_tasks_for_workspace_cleanup(limit=max(1, int(limit)))
    scheduled = 0
    scanned = 0
    for task in rows:
        scanned += 1
        task_id = str(task.get("id") or "").strip()
        status = _norm_status(task.get("status"))
        updated = str(task.get("updated_at") or "")
        if not task_id or status not in {"completed", "cancelled"}:
            continue
        state = _TASK_WORKSPACE_CLEANUP_STATE.get(task_id)
        if state:
            same_snapshot = (
                str(state.get("status") or "") == status
                and str(state.get("updated_at") or "") == updated
            )
            if same_snapshot and bool(state.get("finalized")):
                continue
        if task_id in _TASK_WORKSPACE_CLEANUP_INFLIGHT:
            continue
        _schedule_task_workspace_cleanup(task, reason=reason)
        scheduled += 1
    return {"scheduled": scheduled, "scanned": scanned, "reason": reason}


def _workspace_cleanup_visible_events(user: dict, limit: int = 50) -> list[dict]:
    is_admin = _is_admin(user)
    user_id = str(user.get("id") or "").strip()
    out: list[dict] = []
    max_items = max(1, min(int(limit), 200))
    for event in list(_TASK_WORKSPACE_CLEANUP_EVENTS):
        if len(out) >= max_items:
            break
        pid = str(event.get("project_id") or "").strip()
        if is_admin or not pid or db.user_can_access_project(pid, user_id, False):
            out.append(dict(event))
    return out


def _workspace_cleanup_visible_inflight_task_ids(user: dict) -> list[str]:
    is_admin = _is_admin(user)
    if is_admin:
        return sorted(_TASK_WORKSPACE_CLEANUP_INFLIGHT)
    user_id = str(user.get("id") or "").strip()
    visible: list[str] = []
    for task_id in sorted(_TASK_WORKSPACE_CLEANUP_INFLIGHT):
        task = db.get_task(task_id, user_id=user_id, is_admin=False)
        if task:
            visible.append(task_id)
    return visible


def _default_trash_dir() -> Path:
    if sys.platform == "darwin":
        return Path.home() / ".Trash"
    if sys.platform.startswith("linux"):
        return Path.home() / ".local" / "share" / "Trash" / "files"
    return Path.home() / ".Trash"


def _move_path_to_trash(path: Path) -> Path:
    override = str(os.getenv("PROJECT_TRASH_DIR", "")).strip()
    trash_dir = Path(override).expanduser() if override else _default_trash_dir()
    trash_dir.mkdir(parents=True, exist_ok=True)
    base_name = path.name or "project"
    candidate = trash_dir / base_name
    if candidate.exists():
        stamp = datetime.utcnow().strftime("%Y%m%d-%H%M%S")
        candidate = trash_dir / f"{base_name}.{stamp}"
        idx = 2
        while candidate.exists():
            candidate = trash_dir / f"{base_name}.{stamp}.{idx}"
            idx += 1
    shutil.move(str(path), str(candidate))
    return candidate


def _extract_bearer_token(authorization: str | None) -> str | None:
    raw = str(authorization or "").strip()
    if not raw:
        return None
    if raw.lower().startswith("bearer "):
        token = raw[7:].strip()
        return token or None
    return None


def _is_admin(user: dict) -> bool:
    return str(user.get("role") or "").strip().lower() == db.ROLE_ADMIN


def require_user(authorization: str | None = Header(default=None, alias="Authorization")) -> dict:
    token = _extract_bearer_token(authorization)
    if not token:
        raise HTTPException(401, "未登录")
    user = db.get_session_user(token)
    if not user:
        raise HTTPException(401, "登录已失效，请重新登录")
    return user


def _has_valid_agent_token(x_agent_token: str | None) -> bool:
    provided = str(x_agent_token or "").strip()
    expected = str(AGENT_API_TOKEN or "").strip()
    if not expected or not provided:
        return False
    return hmac.compare_digest(provided, expected)


def require_agent_or_user(
    request: Request,
    authorization: str | None = Header(default=None, alias="Authorization"),
    x_agent_token: str | None = Header(default=None, alias="X-Agent-Token"),
) -> dict:
    if _has_valid_agent_token(x_agent_token):
        return {"id": "agent", "username": "agent", "role": "agent", "auth_type": "agent"}
    token = _extract_bearer_token(authorization)
    if token:
        user = db.get_session_user(token)
        if user:
            user["auth_type"] = "user"
            return user
        raise HTTPException(401, "登录已失效，请重新登录")
    raise HTTPException(401, "未授权（需要用户登录或有效的 X-Agent-Token）")


def require_admin(user: dict = Depends(require_user)) -> dict:
    if not _is_admin(user):
        raise HTTPException(403, "仅管理员可操作")
    return user


def require_project_access(project_id: str, user: dict) -> dict:
    project = db.get_project(project_id, user_id=user["id"], is_admin=_is_admin(user))
    if not project:
        raise HTTPException(404, "Project not found")
    return project


def require_task_access(task_id: str, user: dict) -> dict:
    task = db.get_task(task_id, user_id=user["id"], is_admin=_is_admin(user))
    if not task:
        raise HTTPException(404, "Task not found")
    return task


# ── In-memory agent state (auto-expands for custom agents) ────────────────────
AGENT_OUTPUT: dict = defaultdict(lambda: deque(maxlen=1000))
AGENT_STATUS: dict = defaultdict(
    lambda: {
        "agent_key": "",
        "worker_id": "",
        "status": "idle",
        "task": "",
        "task_id": "",
        "project_id": "",
        "run_id": "",
        "lease_token": "",
        "phase": "",
        "pid": None,
        "updated_at": "",
        "last_output_at": "",
    }
)


def _utcnow_iso() -> str:
    return datetime.utcnow().isoformat()


def _parse_iso(ts: str | None) -> datetime | None:
    raw = str(ts or "").strip()
    if not raw:
        return None
    try:
        return datetime.fromisoformat(raw.rstrip("Z"))
    except Exception:
        return None


def _agent_last_seen(status: dict) -> datetime | None:
    seen = []
    for key in ("updated_at", "last_output_at"):
        dt = _parse_iso(status.get(key))
        if dt is not None:
            seen.append(dt)
    return max(seen) if seen else None


def _ensure_agent_state(agent_name: str) -> dict:
    AGENT_OUTPUT[agent_name]
    state = AGENT_STATUS[agent_name]
    state.setdefault("agent_key", normalize_agent_key(agent_name, default=agent_name))
    state.setdefault("worker_id", "")
    state.setdefault("status", "idle")
    state.setdefault("task", "")
    state.setdefault("task_id", "")
    state.setdefault("project_id", "")
    state.setdefault("run_id", "")
    state.setdefault("lease_token", "")
    state.setdefault("phase", "")
    state.setdefault("pid", None)
    state.setdefault("updated_at", "")
    state.setdefault("last_output_at", "")
    return state


def _agent_outputs_snapshot(user: dict | None = None, project_id: str | None = None) -> dict:
    is_admin = _is_admin(user or {}) if user else True
    user_id = str((user or {}).get("id") or "").strip() if user else ""
    scope_project_id = str(project_id or "").strip()

    def visible_project(pid: str | None) -> bool:
        p = str(pid or "").strip()
        if scope_project_id:
            if not p:
                return False
            if p != scope_project_id:
                return False
        if not p:
            return True
        if is_admin:
            return True
        if not user_id:
            return False
        return db.user_can_access_project(p, user_id, False)

    names = sorted(set(AGENT_OUTPUT.keys()) | set(AGENT_STATUS.keys()))
    snapshot: dict[str, dict] = {}
    for name in names:
        state = dict(_ensure_agent_state(name))
        lines = [
            entry
            for entry in (_normalize_agent_output_entry(x) for x in list(AGENT_OUTPUT[name]))
            if visible_project(entry.get("project_id"))
        ]
        state_visible = visible_project(state.get("project_id"))
        if not state_visible and not lines:
            continue
        if not state_visible and scope_project_id:
            state["status"] = "idle"
            state["task"] = ""
            state["task_id"] = ""
            state["project_id"] = ""
            state["run_id"] = ""
            state["lease_token"] = ""
            state["phase"] = ""
            state["pid"] = None
        snapshot[name] = {"lines": lines, "status": state}
    return snapshot


def _normalize_agent_output_entry(raw) -> dict:
    if isinstance(raw, dict):
        line = str(raw.get("line") or "")
        return {
            "line": line,
            "kind": str(raw.get("kind") or "line").strip().lower() or "line",
            "event": str(raw.get("event") or "line").strip().lower() or "line",
            "agent_key": str(raw.get("agent_key") or "").strip() or None,
            "worker_id": str(raw.get("worker_id") or "").strip() or None,
            "project_id": str(raw.get("project_id") or "").strip() or None,
            "task_id": str(raw.get("task_id") or "").strip() or None,
            "run_id": str(raw.get("run_id") or "").strip() or None,
            "exit_code": int(raw["exit_code"]) if raw.get("exit_code") is not None else None,
            "created_at": str(raw.get("created_at") or _utcnow_iso()),
        }
    return {
        "line": str(raw or ""),
        "kind": "line",
        "event": "line",
        "agent_key": None,
        "worker_id": None,
        "project_id": None,
        "task_id": None,
        "run_id": None,
        "exit_code": None,
        "created_at": _utcnow_iso(),
    }


async def _recover_stale_agent(agent_name: str, stale_secs: int):
    state = _ensure_agent_state(agent_name)
    last_output_at = state.get("last_output_at", "")
    agent_key = normalize_agent_key(state.get("agent_key"), default=agent_name)
    worker_id = str(state.get("worker_id") or "").strip()
    AGENT_STATUS[agent_name] = {
        "agent_key": agent_key,
        "worker_id": worker_id,
        "status": "idle",
        "task": "",
        "task_id": "",
        "project_id": str(state.get("project_id") or "").strip(),
        "run_id": "",
        "lease_token": "",
        "phase": "",
        "pid": None,
        "updated_at": _utcnow_iso(),
        "last_output_at": last_output_at,
    }
    await manager.broadcast(
        {
            "event": "agent_status",
            "agent": agent_name,
            "agent_key": agent_key,
            "worker_id": worker_id,
            "status": "idle",
            "task": "",
            "task_id": "",
            "project_id": str(state.get("project_id") or "").strip(),
            "run_id": "",
            "lease_token": "",
            "phase": "",
            "pid": None,
        }
    )

    recovered = db.recover_stale_tasks_for_agent(agent_key)
    for item in recovered:
        task = item["task"]
        await manager.broadcast({"event": "task_updated", "task": task})
        log = db.add_log(
            task["id"],
            "system",
            (
                f"⚠ 检测到 Agent {agent_name} 超过 {stale_secs}s 无活动，"
                f"任务自动从 {item['from_status']} 回退到 {item['to_status']}。"
            ),
        )
        await broadcast_log(log)


async def _agent_health_watchdog():
    while True:
        await asyncio.sleep(AGENT_WATCHDOG_SECS)
        now = datetime.utcnow()
        protected_task_ids: set[str] = set()
        # If an agent is still heartbeating with a valid lease fence, avoid
        # reclaiming its expired lease immediately (prevents long-task false positives).
        for _, state in list(AGENT_STATUS.items()):
            if str(state.get("status") or "").strip().lower() != "busy":
                continue
            last_seen = _agent_last_seen(state)
            if last_seen is None:
                continue
            idle_secs = int((now - last_seen).total_seconds())
            if idle_secs > AGENT_STALE_SECS:
                continue
            task_id = str(state.get("task_id") or "").strip()
            run_id = str(state.get("run_id") or "").strip()
            lease_token = str(state.get("lease_token") or "").strip()
            if not task_id or not run_id or not lease_token:
                continue
            ok, _ = db.validate_task_lease(
                task_id=task_id,
                expected_run_id=run_id,
                expected_lease_token=lease_token,
                strict_if_active=True,
            )
            if ok:
                protected_task_ids.add(task_id)

        recovered = db.recover_expired_task_leases(
            grace_secs=TASK_LEASE_RECOVERY_GRACE_SECS,
            exclude_task_ids=protected_task_ids,
        )
        if not recovered:
            continue
        touched_agents: set[tuple[str, str]] = set()
        for item in recovered:
            task = item["task"]
            await manager.broadcast({"event": "task_updated", "task": task})
            agent_name = str(item.get("agent_key") or "").strip().lower()
            if agent_name:
                touched_agents.add((agent_name, str(task.get("project_id") or "").strip()))
            expired_secs = int(item.get("expired_secs") or 0)
            log = db.add_log(
                task["id"],
                "system",
                (
                    f"⚠ 检测到任务租约已过期（{expired_secs}s），"
                    f"任务自动从 {item['from_status']} 回退到 {item['to_status']}。"
                ),
            )
            await broadcast_log(log)
        for agent_name, project_id in touched_agents:
            for runtime_name, state in list(AGENT_STATUS.items()):
                state_key = normalize_agent_key(state.get("agent_key"), default=runtime_name)
                state_project = str(state.get("project_id") or "").strip()
                if state_key != agent_name:
                    continue
                if project_id and state_project and state_project != project_id:
                    continue
                AGENT_STATUS[runtime_name] = {
                    "agent_key": state_key,
                    "worker_id": str(state.get("worker_id") or "").strip(),
                    "status": "idle",
                    "task": "",
                    "task_id": "",
                    "project_id": state_project,
                    "run_id": "",
                    "lease_token": "",
                    "phase": "",
                    "pid": None,
                    "updated_at": _utcnow_iso(),
                    "last_output_at": state.get("last_output_at", ""),
                }
                await manager.broadcast(
                    {
                        "event": "agent_status",
                        "agent": runtime_name,
                        "agent_key": state_key,
                        "worker_id": str(state.get("worker_id") or "").strip(),
                        "status": "idle",
                        "task": "",
                        "task_id": "",
                        "project_id": state_project,
                        "run_id": "",
                        "lease_token": "",
                        "phase": "",
                        "pid": None,
                    }
                )


async def _task_workspace_cleanup_watchdog():
    if not AUTO_CLEANUP_TASK_WORKSPACES:
        return
    interval = max(10, int(TASK_WORKSPACE_SWEEP_SECS))
    while True:
        await asyncio.sleep(interval)
        with contextlib.suppress(Exception):
            _sweep_terminal_task_workspaces(
                max_tasks=TASK_WORKSPACE_SWEEP_BATCH_SIZE,
                reason="periodic_sweep",
            )


# Pre-populate built-ins so they appear on init
for _k in ("developer", "reviewer", "manager"):
    _ensure_agent_state(_k)


# ── WebSocket manager ─────────────────────────────────────────────────────────
class ConnectionManager:
    def __init__(self):
        self.active: dict[WebSocket, dict] = {}
        # Serialize broadcasts to preserve per-connection event order and
        # avoid concurrent writes on the same websocket.
        self._broadcast_lock = asyncio.Lock()

    async def connect(self, ws: WebSocket):
        await ws.accept()

    def _can_receive(self, conn: dict, data: dict) -> bool:
        user = conn.get("user") if isinstance(conn, dict) else None
        if not isinstance(user, dict):
            return False
        scope_project_id = (
            str(conn.get("project_id") or "").strip()
            if isinstance(conn, dict)
            else ""
        )
        is_admin = _is_admin(user)
        event = str(data.get("event") or "").strip()
        if not event:
            return True

        def scope_allows(pid: str | None) -> bool:
            if not scope_project_id:
                return True
            p = str(pid or "").strip()
            return bool(p) and p == scope_project_id

        if event.startswith("agent_type_"):
            return True
        if event.startswith("agent_"):
            pid = str(data.get("project_id") or "").strip()
            if not pid:
                output = data.get("output") if isinstance(data.get("output"), dict) else {}
                pid = str(output.get("project_id") or "").strip()
            if not pid:
                return not scope_project_id
            if not scope_allows(pid):
                return False
            if is_admin:
                return True
            return db.user_can_access_project(pid, user.get("id"), False)

        if event == "project_created":
            project = data.get("project") if isinstance(data.get("project"), dict) else {}
            pid = str(project.get("id") or "").strip()
            if not pid:
                return False
            if not scope_allows(pid):
                return False
            if is_admin:
                return True
            return db.user_can_access_project(pid, user.get("id"), False)

        if event == "project_deleted":
            owner_id = str(data.get("project_owner_user_id") or "").strip()
            if owner_id:
                if scope_project_id:
                    pid = str(data.get("project_id") or "").strip()
                    if not scope_allows(pid):
                        return False
                if is_admin:
                    return True
                return owner_id == str(user.get("id") or "")
            pid = str(data.get("project_id") or "").strip()
            if not pid:
                return False
            if not scope_allows(pid):
                return False
            if is_admin:
                return True
            return db.user_can_access_project(pid, user.get("id"), False)

        if event == "files_changed":
            pid = str(data.get("project_id") or "").strip()
            if not pid:
                return False
            if not scope_allows(pid):
                return False
            if is_admin:
                return True
            return db.user_can_access_project(pid, user.get("id"), False)

        if event in {"task_created", "task_updated"}:
            task = data.get("task") if isinstance(data.get("task"), dict) else {}
            pid = str(task.get("project_id") or "").strip()
            if not pid:
                return False
            if not scope_allows(pid):
                return False
            if is_admin:
                return True
            return db.user_can_access_project(pid, user.get("id"), False)

        if event == "log_added":
            log = data.get("log") if isinstance(data.get("log"), dict) else {}
            task_id = str(log.get("task_id") or "").strip()
            if not task_id:
                return False
            task = db.get_task(task_id)
            if not task:
                return False
            pid = str(task.get("project_id") or "").strip()
            if not pid or not scope_allows(pid):
                return False
            if is_admin:
                return True
            return db.user_can_access_project(pid, user.get("id"), False)

        if event == "handoff_added":
            handoff = data.get("handoff") if isinstance(data.get("handoff"), dict) else {}
            task_id = str(handoff.get("task_id") or "").strip()
            if not task_id:
                return False
            task = db.get_task(task_id)
            if not task:
                return False
            pid = str(task.get("project_id") or "").strip()
            if not pid or not scope_allows(pid):
                return False
            if is_admin:
                return True
            return db.user_can_access_project(pid, user.get("id"), False)

        return True

    async def send_init_and_subscribe(
        self,
        ws: WebSocket,
        user: dict,
        payload_builder,
        project_id: str | None = None,
    ):
        # Keep init snapshot and subscription atomic relative to broadcasts.
        async with self._broadcast_lock:
            payload = payload_builder()
            await ws.send_json(payload)
            self.active[ws] = {
                "user": user,
                "project_id": str(project_id or "").strip(),
            }

    def disconnect(self, ws: WebSocket):
        self.active.pop(ws, None)

    async def broadcast(self, data: dict):
        async with self._broadcast_lock:
            sockets = list(self.active.items())
            if not sockets:
                return

            async def _send(ws: WebSocket, conn: dict):
                if not self._can_receive(conn, data):
                    return None
                # Drop slow/broken sockets quickly so broadcasts never block
                # agent heartbeats or task state transitions for long periods.
                await asyncio.wait_for(ws.send_json(data), timeout=1.5)
                return True

            results = await asyncio.gather(
                *(_send(ws, conn) for ws, conn in sockets),
                return_exceptions=True,
            )
            dead = {
                ws for (ws, _), res in zip(sockets, results)
                if isinstance(res, Exception)
            }
            for ws in dead:
                self.active.pop(ws, None)


manager = ConnectionManager()


async def broadcast_log(log: dict):
    await manager.broadcast({"event": "log_added", "log": log})


def broadcast_bg(data: dict):
    asyncio.create_task(manager.broadcast(data))


@asynccontextmanager
async def lifespan(app: FastAPI):
    db.init_db()
    db.reset_stuck_tasks()
    # Pre-populate AGENT_OUTPUT/STATUS for all known agent types
    for at in db.list_agent_types():
        _ensure_agent_state(at["key"])
    known_output_agents = set(db.list_agent_output_agents())
    known_output_agents.update({str(at.get("key") or "").strip().lower() for at in db.list_agent_types()})
    for agent_name in sorted(x for x in known_output_agents if x):
        _ensure_agent_state(agent_name)
        AGENT_OUTPUT[agent_name].clear()
        for entry in db.get_agent_output_entries(agent_name, limit=1000):
            AGENT_OUTPUT[agent_name].append(_normalize_agent_output_entry(entry))
    if AUTO_CLEANUP_TASK_WORKSPACES:
        with contextlib.suppress(Exception):
            _sweep_terminal_task_workspaces(
                max_tasks=TASK_WORKSPACE_SWEEP_BATCH_SIZE,
                reason="startup_sweep",
            )
    watchdog = asyncio.create_task(_agent_health_watchdog())
    cleanup_watchdog = asyncio.create_task(_task_workspace_cleanup_watchdog())
    try:
        yield
    finally:
        watchdog.cancel()
        cleanup_watchdog.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await watchdog
        with contextlib.suppress(asyncio.CancelledError):
            await cleanup_watchdog


app = FastAPI(title="Multi-Agent Task Board", lifespan=lifespan)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])


@app.middleware("http")
async def request_access_log_middleware(request: Request, call_next):
    started = time.perf_counter()
    method = request.method
    path = request.url.path
    query = request.url.query
    target = f"{path}?{query}" if query else path
    client_ip = request.client.host if request.client else "-"
    try:
        response = await call_next(request)
    except Exception:
        elapsed_ms = (time.perf_counter() - started) * 1000
        REQUEST_ACCESS_LOG.info(
            '%s "%s" %s client=%s duration_ms=%.2f',
            method,
            target,
            500,
            client_ip,
            elapsed_ms,
        )
        raise
    elapsed_ms = (time.perf_counter() - started) * 1000
    REQUEST_ACCESS_LOG.info(
        '%s "%s" %s client=%s duration_ms=%.2f',
        method,
        target,
        response.status_code,
        client_ip,
        elapsed_ms,
    )
    return response

frontend_dir = PROJECT_ROOT / "frontend"
if frontend_dir.exists():
    app.mount("/static", StaticFiles(directory=str(frontend_dir)), name="static")


# ── Pydantic models ───────────────────────────────────────────────────────────
class ProjectCreate(BaseModel):
    name: str
    path: str  # absolute filesystem path
    import_existing: bool = False


class AdminSetupRequest(BaseModel):
    password: str


class LoginRequest(BaseModel):
    username: str
    password: str


class UserCreateRequest(BaseModel):
    username: str
    password: str


class UserUpdateRequest(BaseModel):
    username: str | None = None
    password: str | None = None
    role: Literal["admin", "user"] | None = None


class TaskDependencyItem(BaseModel):
    depends_on_task_id: str
    required_state: Literal["completed", "approved"] = "completed"


class TaskDependenciesUpdate(BaseModel):
    dependencies: list[TaskDependencyItem] = Field(default_factory=list)


class TaskCreate(BaseModel):
    title: str
    description: str = ""
    project_id: str | None = None
    parent_task_id: str | None = None
    subtask_order: int | None = None
    assigned_agent: str | None = None
    dev_agent: str | None = None
    priority: int = Field(default=2, ge=0, le=3)
    dependencies: list[TaskDependencyItem] = Field(default_factory=list)
    review_enabled: bool = True
    status: str = "triage"   # default: all new tasks enter triage first

class TaskUpdate(BaseModel):
    description: str | None = None
    status: str | None = None
    assignee: str | None = None
    assigned_agent: str | None = None
    dev_agent: str | None = None
    review_enabled: bool | None = None
    review_feedback: str | None = None
    commit_hash: str | None = None
    priority: int | None = Field(default=None, ge=0, le=3)
    archived: int | None = None
    feedback_source: str | None = None
    feedback_stage: str | None = None
    feedback_actor: str | None = None
    create_handoff: bool | None = None

class LogCreate(BaseModel):
    agent: str
    message: str
    run_id: str | None = None
    lease_token: str | None = None

class HandoffCreate(BaseModel):
    stage: str
    from_agent: str
    to_agent: str | None = None
    status_from: str | None = None
    status_to: str | None = None
    title: str = ""
    summary: str = ""
    commit_hash: str | None = None
    conclusion: str | None = None
    payload: dict = Field(default_factory=dict)
    artifact_path: str | None = None
    expected_run_id: str | None = None
    expected_lease_token: str | None = None

class AgentAlertCreate(BaseModel):
    agent: str
    task_id: str | None = None
    kind: str = "error"
    summary: str
    message: str = ""
    code: str = ""
    stage: str = ""
    metadata: dict = Field(default_factory=dict)

class AgentOutput(BaseModel):
    line: str = ""
    kind: str | None = None
    event: str | None = None
    exit_code: int | None = None
    agent_key: str | None = None
    worker_id: str | None = None
    project_id: str | None = None
    task_id: str | None = None
    run_id: str | None = None

class AgentStatusUpdate(BaseModel):
    status: str
    task: str = ""
    agent_key: str | None = None
    worker_id: str | None = None
    project_id: str | None = None
    task_id: str | None = None
    run_id: str | None = None
    lease_token: str | None = None
    phase: str | None = None
    pid: int | None = None

class AgentTypeCreate(BaseModel):
    key: str
    name: str
    description: str = ""
    prompt: str = ""
    poll_statuses: list[str] = ["todo"]
    next_status: str = "in_review"
    working_status: str = "in_progress"
    cli: str = "claude"

class AgentTypeUpdate(BaseModel):
    name: str | None = None
    description: str | None = None
    prompt: str | None = None
    poll_statuses: list[str] | None = None
    next_status: str | None = None
    working_status: str | None = None
    cli: str | None = None

class GeneratePromptRequest(BaseModel):
    description: str
    cli: str = "claude"

class MkdirRequest(BaseModel):
    path: str

class TaskClaim(BaseModel):
    status: str
    working_status: str
    agent: str
    agent_key: str
    respect_assignment: bool = True
    lease_ttl_secs: int | None = None
    project_id: str | None = None


class TaskLeaseRenewRequest(BaseModel):
    run_id: str
    lease_token: str
    lease_ttl_secs: int | None = None

class CancelTaskRequest(BaseModel):
    reason: str | None = None
    include_subtasks: bool = True


class TaskTransitionRequest(BaseModel):
    fields: TaskUpdate | None = None
    handoff: HandoffCreate | None = None
    log: LogCreate | None = None
    expected_run_id: str | None = None
    expected_lease_token: str | None = None


class TaskActionRequest(BaseModel):
    action: Literal["accept", "reject", "retry_blocked", "decompose", "archive"]
    feedback: str | None = None
    force: bool = False


COMMIT_REQUIRED_STAGES = {
    "dev_to_review",
    "dev_to_approved",
    "review_to_manager",
    "review_to_dev",
    "merge_to_acceptance",
    "merge_to_dev",
    "merge_failed",
}


STATUS_FLOW: dict[str, set[str]] = {
    "triage": {"triage", "triaging", "decompose", "decomposed", "todo", "blocked", "cancelled"},
    "triaging": {"triaging", "triage", "decompose", "decomposed", "todo", "blocked", "cancelled"},
    "decompose": {"decompose", "triaging", "decomposed", "blocked", "triage", "cancelled"},
    "todo": {"todo", "in_progress", "decompose", "blocked", "cancelled"},
    "in_progress": {"in_progress", "in_review", "approved", "todo", "needs_changes", "blocked", "cancelled"},
    "in_review": {"in_review", "reviewing", "needs_changes", "approved", "blocked", "cancelled"},
    "reviewing": {"reviewing", "in_review", "needs_changes", "approved", "blocked", "cancelled"},
    "needs_changes": {"needs_changes", "in_progress", "blocked", "cancelled"},
    "approved": {"approved", "merging", "pending_acceptance", "needs_changes", "blocked", "cancelled"},
    "merging": {"merging", "pending_acceptance", "needs_changes", "approved", "blocked", "cancelled"},
    "pending_acceptance": {"pending_acceptance", "completed", "needs_changes", "cancelled"},
    "blocked": {"blocked", "triage", "decompose", "in_review", "todo", "needs_changes", "cancelled"},
    "decomposed": {"decomposed", "completed", "cancelled"},
    "completed": {"completed"},
    "cancelled": {"cancelled"},
}


def _norm_status(raw: str | None) -> str:
    return str(raw or "").strip().lower()


def _parse_poll_statuses(raw) -> list[str]:
    if isinstance(raw, list):
        return [str(x).strip() for x in raw if str(x).strip()]
    if isinstance(raw, str):
        try:
            data = json.loads(raw)
            if isinstance(data, list):
                return [str(x).strip() for x in data if str(x).strip()]
        except Exception:
            return []
    return []


def _agent_polls_status(agent_key: str | None, status: str) -> bool:
    key = normalize_agent_key(agent_key, default="")
    wanted = _norm_status(status)
    if not key or not wanted:
        return False
    at = db.get_agent_type(key)
    if not at:
        return False
    return wanted in {_norm_status(x) for x in _parse_poll_statuses(at.get("poll_statuses"))}


def _validate_status_transition(before: dict, fields: dict):
    if "status" not in fields:
        return
    prev = _norm_status(before.get("status"))
    nxt = _norm_status(fields.get("status"))
    if not nxt:
        raise HTTPException(422, "status 不能为空")
    if prev == nxt:
        return
    allowed = STATUS_FLOW.get(prev)
    if not allowed or nxt not in allowed:
        raise HTTPException(409, f"非法状态流转: {prev} -> {nxt}")


def _normalize_transition_fields(before: dict, fields: dict) -> dict:
    out = dict(fields or {})
    out.pop("create_handoff", None)
    if _norm_status(out.get("status")) == "needs_changes":
        fallback_dev = normalize_agent_key(
            out.get("dev_agent")
            or before.get("dev_agent")
            or before.get("assigned_agent")
            or "developer"
        )
        if not str(out.get("assigned_agent") or "").strip():
            out["assigned_agent"] = fallback_dev
        if not str(out.get("dev_agent") or "").strip():
            out["dev_agent"] = fallback_dev
        out.setdefault("assignee", None)
    return out


def _build_handoff_row(task_for_handoff: dict, handoff: HandoffCreate | None) -> dict | None:
    if not handoff:
        return None
    payload, commit_hash, conclusion = _prepare_handoff(task_for_handoff, handoff)
    return {
        "stage": str(handoff.stage or "").strip(),
        "from_agent": str(handoff.from_agent or "").strip(),
        "to_agent": handoff.to_agent,
        "status_from": handoff.status_from,
        "status_to": handoff.status_to,
        "title": handoff.title,
        "summary": handoff.summary,
        "commit_hash": commit_hash,
        "conclusion": conclusion,
        "payload": payload,
        "artifact_path": handoff.artifact_path,
    }


async def _apply_transition_and_broadcast(
    task_id: str,
    *,
    before: dict,
    fields: dict,
    handoff_row: dict | None = None,
    log_row: dict | None = None,
    expected_run_id: str | None = None,
    expected_lease_token: str | None = None,
    skip_status_validation: bool = False,
) -> dict:
    normalized_fields = _normalize_transition_fields(before, fields)
    if not skip_status_validation:
        _validate_status_transition(before, normalized_fields)

    try:
        result = db.transition_task(
            task_id,
            fields=normalized_fields,
            handoff=handoff_row,
            log=log_row,
            expected_run_id=expected_run_id,
            expected_lease_token=expected_lease_token,
        )
    except db.LeaseConflictError as e:
        raise HTTPException(409, str(e))
    if not result:
        raise HTTPException(404, "Task not found")

    task = result["task"]
    await manager.broadcast({"event": "task_updated", "task": task})

    handoff = result.get("handoff")
    if handoff:
        await manager.broadcast({"event": "handoff_added", "handoff": _format_handoff(handoff)})

    log = result.get("log")
    if log:
        await broadcast_log(log)

    if "status" in normalized_fields and task.get("parent_task_id"):
        if db.check_parent_completion(task["parent_task_id"]):
            parent = db.get_task(task["parent_task_id"])
            if parent:
                await manager.broadcast({"event": "task_updated", "task": parent})

    before_status = _norm_status(before.get("status"))
    after_status = _norm_status(task.get("status"))
    if after_status in {"completed", "cancelled"} and (
        before_status != after_status or int(task.get("archived") or 0) == 1
    ):
        _schedule_task_workspace_cleanup(
            task,
            reason=f"status_transition:{before_status}->{after_status}",
        )

    return {
        "task": task,
        "handoff": _format_handoff(handoff) if handoff else None,
        "log": log,
    }


def _resolve_blocked_retry(task: dict) -> tuple[str, str | None, str] | None:
    owner = str(task.get("assigned_agent") or "").strip().lower()
    feedback = str(task.get("review_feedback") or "")

    if owner == "reviewer" or "[review_retry=" in feedback or "审查器" in feedback:
        return ("in_review", "reviewer", "重试审查")

    if owner == "leader":
        dev_agent = str(task.get("dev_agent") or "").strip()
        resume_assignee = dev_agent if dev_agent and dev_agent.lower() != "leader" else None
        force_decompose = ".leader-force-decompose.json" in feedback or "leader_force_decompose" in feedback
        if force_decompose:
            return ("decompose", resume_assignee, "重试分解")
        return ("triage", resume_assignee, "重试评估")

    return None


def _prepare_handoff(task: dict, body: HandoffCreate) -> tuple[dict, str | None, str]:
    stage = str(body.stage or "").strip()
    if not stage:
        raise HTTPException(422, "handoff.stage 不能为空")
    from_agent = str(body.from_agent or "").strip()
    if not from_agent:
        raise HTTPException(422, "handoff.from_agent 不能为空")

    payload = dict(body.payload or {})

    related_candidates: list[str] = []
    rel = payload.get("related_history_commits")
    if isinstance(rel, list):
        for item in rel:
            if not isinstance(item, dict):
                continue
            h = str(item.get("hash") or item.get("commit_hash") or "").strip()
            if h:
                related_candidates.append(h)
    if not related_candidates:
        rel2 = payload.get("related_commit_candidates")
        if isinstance(rel2, list):
            for item in rel2:
                if isinstance(item, dict):
                    h = str(item.get("hash") or item.get("commit_hash") or "").strip()
                else:
                    h = str(item or "").strip()
                if h:
                    related_candidates.append(h)

    raw_commit = (
        str(body.commit_hash or "").strip()
        or str(payload.get("commit_hash") or "").strip()
        or (related_candidates[0] if related_candidates else "")
    )
    commit_hash = raw_commit[:120] if raw_commit else None
    if commit_hash:
        payload.setdefault("commit_hash", commit_hash)

    raw_conclusion = (
        str(body.conclusion or "").strip()
        or str(payload.get("conclusion") or "").strip()
        or str(body.summary or "").strip()
    )
    conclusion = raw_conclusion[:500]
    if not conclusion:
        raise HTTPException(422, "handoff.conclusion 不能为空（可复用 summary）")
    payload.setdefault("conclusion", conclusion)

    has_commit = payload.get("has_commit")
    if stage in COMMIT_REQUIRED_STAGES and has_commit is not False and not commit_hash:
        raise HTTPException(422, f"stage={stage} 交接必须包含 commit_hash")

    return payload, commit_hash, conclusion


# ── Root ──────────────────────────────────────────────────────────────────────
@app.get("/")
async def root():
    index = frontend_dir / "index.html"
    return FileResponse(str(index)) if index.exists() else {"status": "ok"}


# ── Auth & Users ──────────────────────────────────────────────────────────────
@app.get("/auth/bootstrap")
async def auth_bootstrap_status():
    return {"admin_password_set": db.admin_password_is_set()}


@app.post("/auth/setup-admin")
async def setup_admin_password(body: AdminSetupRequest):
    password = str(body.password or "")
    if len(password) < 6:
        raise HTTPException(422, "密码至少 6 位")
    if db.admin_password_is_set():
        raise HTTPException(409, "管理员密码已设置")
    user = db.set_admin_initial_password(password)
    if not user:
        raise HTTPException(500, "初始化管理员失败")
    session = db.create_session(user["id"])
    return {"user": user, **session}


@app.post("/auth/login")
async def login(body: LoginRequest):
    username = str(body.username or "").strip().lower()
    password = str(body.password or "")
    if not username or not password:
        raise HTTPException(422, "用户名和密码不能为空")
    if username == "admin" and not db.admin_password_is_set():
        raise HTTPException(409, "管理员尚未设置初始密码")
    auth_result = db.authenticate_user(username, password)
    if not auth_result.get("ok"):
        retry_after = int(auth_result.get("retry_after_secs") or 0)
        if str(auth_result.get("reason") or "") == "locked":
            retry_after = max(1, retry_after)
            raise HTTPException(429, f"登录失败次数过多，请在 {retry_after} 秒后重试")
        if auth_result.get("locked"):
            retry_after = max(1, retry_after)
            raise HTTPException(429, f"登录失败次数过多，账号已锁定 {retry_after} 秒")
        raise HTTPException(401, "用户名或密码错误")
    user = auth_result["user"]
    session = db.create_session(user["id"])
    return {"user": user, **session}


@app.post("/auth/logout")
async def logout(authorization: str | None = Header(default=None, alias="Authorization")):
    token = _extract_bearer_token(authorization)
    if token:
        db.revoke_session(token)
    return {"ok": True}


@app.get("/auth/me")
async def get_me(user: dict = Depends(require_user)):
    return user


@app.get("/users")
async def list_users(_admin: dict = Depends(require_admin)):
    return db.list_users()


@app.post("/users", status_code=201)
async def create_user(body: UserCreateRequest, admin: dict = Depends(require_admin)):
    username = str(body.username or "").strip().lower()
    password = str(body.password or "")
    if not re.match(r"^[a-z][a-z0-9._-]{2,31}$", username):
        raise HTTPException(422, "用户名只能包含小写字母/数字/._-，长度 3-32，且以字母开头")
    if len(password) < 6:
        raise HTTPException(422, "密码至少 6 位")
    if db.get_user_by_username(username):
        raise HTTPException(409, "用户名已存在")
    try:
        return db.create_user(username, password, role=db.ROLE_USER, created_by=admin["id"])
    except ValueError as e:
        raise HTTPException(422, str(e))
    except sqlite3.IntegrityError:
        raise HTTPException(409, "用户名已存在")


@app.patch("/users/{user_id}")
async def update_user(user_id: str, body: UserUpdateRequest, admin: dict = Depends(require_admin)):
    target = db.get_user(user_id)
    if not target:
        raise HTTPException(404, "用户不存在")

    username: str | None = None
    if body.username is not None:
        username = str(body.username or "").strip().lower()
        if not re.match(r"^[a-z][a-z0-9._-]{2,31}$", username):
            raise HTTPException(422, "用户名只能包含小写字母/数字/._-，长度 3-32，且以字母开头")
        if target["username"] == "admin" and username != target["username"]:
            raise HTTPException(422, "admin 账号用户名不可修改")
        hit = db.get_user_by_username(username)
        if hit and hit["id"] != user_id:
            raise HTTPException(409, "用户名已存在")
        if username == target["username"]:
            username = None

    password: str | None = None
    if body.password is not None:
        password = str(body.password or "")
        if len(password) < 6:
            raise HTTPException(422, "密码至少 6 位")

    role: str | None = body.role if body.role is not None else None
    if role is not None:
        if target["username"] == "admin" and role != db.ROLE_ADMIN:
            raise HTTPException(422, "admin 账号角色不可修改")
        if role == target["role"]:
            role = None
        elif role == db.ROLE_USER and target["role"] == db.ROLE_ADMIN:
            if target["id"] == admin["id"]:
                raise HTTPException(422, "不能降低当前登录账号的管理员权限")
            if db.count_users_by_role(db.ROLE_ADMIN) <= 1:
                raise HTTPException(422, "系统至少需要保留一个管理员")

    if username is None and password is None and role is None:
        raise HTTPException(422, "没有可更新字段")

    try:
        updated = db.update_user(
            user_id,
            username=username,
            password=password,
            role=role,
        )
    except ValueError as e:
        raise HTTPException(422, str(e))
    except sqlite3.IntegrityError:
        raise HTTPException(409, "用户名已存在")
    if not updated:
        raise HTTPException(404, "用户不存在")
    return updated


@app.delete("/users/{user_id}")
async def delete_user_api(user_id: str, admin: dict = Depends(require_admin)):
    target = db.get_user(user_id)
    if not target:
        raise HTTPException(404, "用户不存在")
    if target["id"] == admin["id"]:
        raise HTTPException(422, "不能删除当前登录账号")
    if target["username"] == "admin":
        raise HTTPException(422, "admin 账号不可删除")
    if target["role"] == db.ROLE_ADMIN and db.count_users_by_role(db.ROLE_ADMIN) <= 1:
        raise HTTPException(422, "系统至少需要保留一个管理员")
    deleted = db.delete_user(user_id)
    if not deleted:
        raise HTTPException(404, "用户不存在")
    return {"ok": True}


# ── Projects ──────────────────────────────────────────────────────────────────
@app.get("/projects")
async def list_projects(user: dict = Depends(require_user)):
    return db.list_projects(user_id=user["id"], is_admin=_is_admin(user))


@app.get("/runtime/projects")
async def list_runtime_projects(
    include_idle: bool = Query(default=False),
    principal: dict = Depends(require_agent_or_user),
):
    auth_type = str(principal.get("auth_type") or "").strip().lower()
    if auth_type == "agent":
        return db.list_worker_projects(include_idle=include_idle, is_admin=True)
    return db.list_worker_projects(
        include_idle=include_idle,
        user_id=principal["id"],
        is_admin=_is_admin(principal),
    )


@app.get("/runtime/workspace-cleanup")
async def workspace_cleanup_runtime(
    limit: int = Query(default=50, ge=1, le=200),
    user: dict = Depends(require_user),
):
    events = _workspace_cleanup_visible_events(user, limit=limit)
    inflight = _workspace_cleanup_visible_inflight_task_ids(user)
    metrics = dict(_TASK_WORKSPACE_CLEANUP_METRICS)
    metrics["inflight"] = len(_TASK_WORKSPACE_CLEANUP_INFLIGHT)
    metrics["history_size"] = len(_TASK_WORKSPACE_CLEANUP_EVENTS)
    if not _is_admin(user):
        metrics["visible_inflight"] = len(inflight)
        metrics["visible_recent_events"] = len(events)
        metrics["scope"] = "filtered"
    return {
        "config": {
            "auto_cleanup": AUTO_CLEANUP_TASK_WORKSPACES,
            "sweep_secs": TASK_WORKSPACE_SWEEP_SECS,
            "sweep_batch_size": TASK_WORKSPACE_SWEEP_BATCH_SIZE,
            "force_delete_unmerged": TASK_WORKSPACE_FORCE_DELETE_UNMERGED,
        },
        "metrics": metrics,
        "inflight_task_ids": inflight,
        "recent_events": events,
    }


@app.post("/runtime/workspace-cleanup/sweep")
async def workspace_cleanup_sweep(
    max_tasks: int = Query(default=100, ge=1, le=500),
    _admin: dict = Depends(require_admin),
):
    result = _sweep_terminal_task_workspaces(max_tasks=max_tasks, reason="manual_sweep")
    return {
        "ok": True,
        "scheduled": int(result.get("scheduled") or 0),
        "scanned": int(result.get("scanned") or 0),
        "reason": result.get("reason") or "manual_sweep",
    }


@app.post("/projects", status_code=201)
async def create_project(body: ProjectCreate, user: dict = Depends(require_user)):
    path = Path(body.path).expanduser().resolve()
    all_projects = db.list_projects()
    if any(p["path"] == str(path) for p in all_projects):
        raise HTTPException(400, "Path already used by another project")
    if body.import_existing:
        if not _is_admin(user):
            raise HTTPException(403, "导入现有项目仅管理员可操作")
        if not path.exists():
            raise HTTPException(400, "Existing project path does not exist")
        if not path.is_dir():
            raise HTTPException(400, "Existing project path must be a directory")
    project = db.create_project(body.name, str(path), created_by_user_id=user["id"])
    await manager.broadcast({"event": "project_created", "project": project})
    return project

@app.get("/projects/{project_id}")
async def get_project(project_id: str, user: dict = Depends(require_user)):
    return require_project_access(project_id, user)


@app.delete("/projects/{project_id}")
async def delete_project_api(
    project_id: str,
    delete_files: bool = Query(default=False),
    delete_permanently: bool = Query(default=False),
    user: dict = Depends(require_user),
):
    project = require_project_access(project_id, user)
    if db.project_has_claimed_tasks(project_id):
        raise HTTPException(409, "项目存在进行中的任务，无法删除")
    files_deleted = False
    files_mode = "none"
    files_destination = ""
    if delete_files:
        proj_path = Path(str(project.get("path") or "")).expanduser()
        try:
            resolved = proj_path.resolve()
        except Exception:
            raise HTTPException(400, "Invalid project path")
        # Guard against catastrophic paths.
        protected_paths = {Path("/").resolve(), Path.home().resolve()}
        if resolved in protected_paths:
            raise HTTPException(400, "Refuse to delete protected path")
        if resolved.exists():
            if not resolved.is_dir():
                raise HTTPException(400, "Project path is not a directory")
            try:
                if delete_permanently:
                    shutil.rmtree(resolved)
                    files_mode = "permanent"
                else:
                    moved_to = _move_path_to_trash(resolved)
                    files_mode = "trash"
                    files_destination = str(moved_to)
                files_deleted = True
            except Exception as e:
                raise HTTPException(500, f"删除项目目录失败: {e}")
    deleted = db.delete_project(project_id)
    if not deleted:
        raise HTTPException(404, "Project not found")
    await manager.broadcast(
        {
            "event": "project_deleted",
            "project_id": project_id,
            "project_owner_user_id": project.get("created_by_user_id"),
        }
    )
    return {
        "ok": True,
        "project_id": project_id,
        "files_deleted": files_deleted,
        "files_mode": files_mode,
        "files_destination": files_destination,
    }


@app.get("/fs/directories")
async def list_directories(path: str = "~", _admin: dict = Depends(require_admin)):
    raw = (path or "").strip() or "~"
    target = Path(raw).expanduser()
    try:
        resolved = target.resolve()
    except Exception:
        raise HTTPException(400, "Invalid path")

    if not resolved.exists():
        raise HTTPException(404, "Directory not found")
    if not resolved.is_dir():
        raise HTTPException(400, "Path is not a directory")

    dirs = []
    try:
        for entry in sorted(resolved.iterdir(), key=lambda e: e.name.lower()):
            if not entry.is_dir():
                continue
            if entry.name.startswith("."):
                continue
            dirs.append({"name": entry.name, "path": str(entry.resolve())})
    except PermissionError:
        raise HTTPException(403, "Permission denied")

    parent = str(resolved.parent) if resolved.parent != resolved else None
    return {"path": str(resolved), "parent": parent, "directories": dirs}


@app.post("/projects/sync")
async def sync_projects(user: dict = Depends(require_user)):
    """Delete projects whose directories no longer exist on disk.

    Safety: do not delete projects that still have claimed (running) tasks,
    otherwise in-flight agents may hit 404 when syncing task status.
    """
    all_projects = db.list_projects(user_id=user["id"], is_admin=_is_admin(user))
    deleted = []
    kept = []
    skipped_busy = []
    for p in all_projects:
        if Path(p["path"]).exists():
            kept.append(p)
            continue
        if db.project_has_claimed_tasks(p["id"]):
            skipped_busy.append(p)
            kept.append(p)
        else:
            db.delete_project(p["id"])
            deleted.append(p)
            await manager.broadcast(
                {
                    "event": "project_deleted",
                    "project_id": p["id"],
                    "project_owner_user_id": p.get("created_by_user_id"),
                }
            )
    return {"deleted": deleted, "kept": kept, "skipped_busy": skipped_busy}


@app.post("/projects/{project_id}/setup")
async def setup_project(project_id: str, user: dict = Depends(require_user)):
    """Initialize git repo in the project directory (without shared dev worktree)."""
    p = require_project_access(project_id, user)

    proj_path = Path(p["path"])
    if proj_path.exists() and not proj_path.is_dir():
        raise HTTPException(400, "Project path exists but is not a directory")
    proj_path.mkdir(parents=True, exist_ok=True)

    def run(*args, cwd=None):
        result = subprocess.run(
            args, cwd=str(cwd or proj_path),
            capture_output=True, text=True
        )
        return result.returncode, result.stdout.strip(), result.stderr.strip()

    log = []

    # Init git if needed
    if not (proj_path / ".git").exists():
        rc, out, err = run("git", "init")
        log.append(f"git init: {out or err}")
        run("git", "config", "user.email", "agent@opc-demo.local")
        run("git", "config", "user.name", "OPC Agent")
        rc, out, err = run("git", "checkout", "-b", "main")
        log.append(f"checkout main: {out or err}")
        rc, out, err = run("git", "commit", "--allow-empty", "-m", "chore: init project")
        log.append(f"init commit: {out or err}")
    else:
        # Ensure git user config
        run("git", "config", "user.email", "agent@opc-demo.local")
        run("git", "config", "user.name", "OPC Agent")
        log.append("git repo already exists")
    log.append("shared dev worktree disabled; agents create their own worktrees on demand")

    return {"ok": True, "log": log, "path": str(proj_path)}


# ── Tasks ─────────────────────────────────────────────────────────────────────
@app.get("/tasks")
async def list_tasks(project_id: str | None = None, user: dict = Depends(require_user)):
    return db.list_tasks(project_id, user_id=user["id"], is_admin=_is_admin(user))

@app.post("/tasks", status_code=201)
async def create_task(body: TaskCreate, principal: dict = Depends(require_agent_or_user)):
    if STRICT_CLAIM_SCOPE and not str(body.project_id or "").strip():
        raise HTTPException(422, "启用项目隔离后，创建任务必须提供 project_id")
    is_user = str(principal.get("auth_type") or "").strip().lower() == "user"
    if is_user and body.project_id and not db.user_can_access_project(
        body.project_id,
        principal["id"],
        _is_admin(principal),
    ):
        raise HTTPException(404, "Project not found")

    assigned_agent = (
        normalize_agent_key(body.assigned_agent, default="")
        if str(body.assigned_agent or "").strip()
        else None
    )
    dev_agent = (
        normalize_agent_key(body.dev_agent, default="")
        if str(body.dev_agent or "").strip()
        else None
    )
    effective_status = _norm_status(body.status) or "triage"

    # User-created root tasks:
    # - no assigned agent: always enter triage
    # - assigned to a todo-polling agent: skip triage and go directly to todo
    if is_user and not str(body.parent_task_id or "").strip():
        direct_todo = bool(assigned_agent and _agent_polls_status(assigned_agent, "todo"))
        effective_status = "todo" if direct_todo else "triage"
        if direct_todo and not dev_agent:
            dev_agent = assigned_agent

    dependency_payload = [item.model_dump() for item in (body.dependencies or [])]
    created_by = principal.get("id") if is_user else principal.get("username")
    try:
        task = db.create_task(
            title=body.title,
            description=body.description,
            project_id=body.project_id,
            parent_task_id=body.parent_task_id,
            assigned_agent=assigned_agent,
            dev_agent=dev_agent,
            status=effective_status,
            subtask_order=body.subtask_order,
            priority=body.priority,
            dependencies=dependency_payload,
            review_enabled=bool(body.review_enabled),
            created_by=str(created_by or "").strip() or None,
        )
    except db.DependencyCycleError as e:
        raise HTTPException(409, str(e))
    except db.DependencyValidationError as e:
        raise HTTPException(422, str(e))
    await manager.broadcast({"event": "task_created", "task": task})
    return task

# NOTE: /tasks/status/{status} must come before /tasks/{task_id}
@app.get("/tasks/status/{status}")
async def tasks_by_status(status: str, project_id: str | None = None):
    return db.get_tasks_by_status(status, project_id)

@app.post("/tasks/claim")
async def claim_task(body: TaskClaim, principal: dict = Depends(require_agent_or_user)):
    effective_status = str(body.status or "").strip()
    effective_working_status = str(body.working_status or "").strip()
    effective_project_id = str(body.project_id or "").strip() or None
    if STRICT_CLAIM_SCOPE and not effective_project_id:
        raise HTTPException(422, "启用项目隔离后，认领任务必须提供 project_id")
    lease_ttl_secs = int(body.lease_ttl_secs or TASK_LEASE_TTL_SECS)
    lease_ttl_secs = max(TASK_LEASE_RENEW_MIN_SECS, min(TASK_LEASE_RENEW_MAX_SECS, lease_ttl_secs))
    normalized_agent_key = normalize_agent_key(body.agent_key, default=body.agent)
    at = db.get_agent_type(normalized_agent_key)
    if at:
        poll_statuses = _parse_poll_statuses(at.get("poll_statuses"))
        if poll_statuses and effective_status not in poll_statuses:
            raise HTTPException(
                409,
                f"agent={normalized_agent_key} 不能认领 status={effective_status}，允许: {poll_statuses}",
            )
        configured_working = str(at.get("working_status") or "").strip()
        if configured_working:
            effective_working_status = configured_working

    auth_type = str(principal.get("auth_type") or "").strip().lower()
    if auth_type == "user":
        if effective_project_id and not db.user_can_access_project(
            effective_project_id,
            principal["id"],
            _is_admin(principal),
        ):
            raise HTTPException(404, "Project not found")

    # Retry loop: skip tasks whose project directory is missing.
    for _ in range(50):
        task = db.claim_task(
            status=effective_status,
            working_status=effective_working_status,
            agent=body.agent,
            agent_key=normalized_agent_key,
            respect_assignment=body.respect_assignment,
            lease_ttl_secs=lease_ttl_secs,
            project_id=effective_project_id,
            user_id=principal["id"] if auth_type == "user" else None,
            is_admin=_is_admin(principal) if auth_type == "user" else True,
            per_project_max_workers=PER_PROJECT_MAX_WORKERS,
            per_agent_type_max_workers=PER_AGENT_TYPE_MAX_WORKERS,
        )
        if not task:
            return {"task": None}

        project_id = str(task.get("project_id") or "").strip()
        project_path = str(task.get("project_path") or "").strip()
        if not project_id or not project_path or Path(project_path).exists():
            await manager.broadcast({"event": "task_updated", "task": task})
            return {"task": task}

        # Project path no longer exists: task is invalid and should not remain
        # in queue. Hard-delete it (and descendants) and continue claiming.
        deleted_task_ids = db.delete_task_permanently(task["id"], include_subtasks=True) or []
        for tid in deleted_task_ids:
            await manager.broadcast(
                {
                    "event": "task_deleted",
                    "task_id": tid,
                    "reason": "missing_project_path",
                    "message": "项目目录不存在，任务已自动删除并跳过分配。",
                }
            )

    return {"task": None}


@app.post("/tasks/{task_id}/lease/renew")
async def renew_task_lease(
    task_id: str,
    body: TaskLeaseRenewRequest,
    _principal: dict = Depends(require_agent_or_user),
):
    lease_ttl_secs = int(body.lease_ttl_secs or TASK_LEASE_TTL_SECS)
    lease_ttl_secs = max(TASK_LEASE_RENEW_MIN_SECS, min(TASK_LEASE_RENEW_MAX_SECS, lease_ttl_secs))
    try:
        task = db.renew_task_lease(
            task_id=task_id,
            run_id=body.run_id,
            lease_token=body.lease_token,
            lease_ttl_secs=lease_ttl_secs,
        )
    except db.LeaseConflictError as e:
        raise HTTPException(409, str(e))
    if not task:
        raise HTTPException(409, "租约续期失败，任务可能已被回收或重新认领")
    return {
        "ok": True,
        "task_id": task_id,
        "lease_expires_at": task.get("lease_expires_at"),
    }

@app.get("/tasks/{task_id}")
async def get_task(task_id: str):
    task = db.get_task(task_id)
    if not task:
        raise HTTPException(404, "Task not found")
    return task

@app.patch("/tasks/{task_id}")
async def update_task(task_id: str, body: TaskUpdate, user: dict = Depends(require_user)):
    fields = body.model_dump(exclude_unset=True)
    fields.pop("create_handoff", None)

    before = require_task_access(task_id, user)

    if "status" in fields:
        raise HTTPException(
            403,
            "禁止直接 PATCH status。请使用后端动作接口 /tasks/{task_id}/actions 或系统流转接口 /tasks/{task_id}/transition。",
        )

    task = db.update_task(task_id, **fields)
    if not task:
        raise HTTPException(404, "Task not found")
    await manager.broadcast({"event": "task_updated", "task": task})
    return task


@app.post("/tasks/{task_id}/transition")
async def transition_task(
    task_id: str,
    body: TaskTransitionRequest,
    _principal: dict = Depends(require_agent_or_user),
):
    before = db.get_task(task_id)
    if not before:
        raise HTTPException(404, "Task not found")

    fields = body.fields.model_dump(exclude_unset=True) if body.fields else {}
    normalized_fields = _normalize_transition_fields(before, fields)

    log_row = None
    if body.log:
        log_row = {"agent": body.log.agent, "message": body.log.message}

    task_for_handoff = dict(before)
    task_for_handoff.update(normalized_fields)
    handoff_row = _build_handoff_row(task_for_handoff, body.handoff)

    return await _apply_transition_and_broadcast(
        task_id,
        before=before,
        fields=normalized_fields,
        handoff_row=handoff_row,
        log_row=log_row,
        expected_run_id=body.expected_run_id,
        expected_lease_token=body.expected_lease_token,
    )


@app.post("/tasks/{task_id}/actions")
async def task_action(task_id: str, body: TaskActionRequest, user: dict = Depends(require_user)):
    before = require_task_access(task_id, user)

    status = _norm_status(before.get("status"))
    archived = int(before.get("archived") or 0)
    action = body.action
    force_action = bool(body.force)
    if force_action and not _is_admin(user):
        raise HTTPException(403, "force 模式仅管理员可用")

    if action == "archive":
        if status != "completed" and not force_action:
            raise HTTPException(409, f"仅 completed 任务可归档，当前为 {status}")
        if archived == 1:
            return {"task": before, "handoff": None, "log": None, "action": action}
        task = db.update_task(task_id, archived=1)
        if not task:
            raise HTTPException(404, "Task not found")
        await manager.broadcast({"event": "task_updated", "task": task})
        log = db.add_log(task_id, "user", "任务已归档。")
        await broadcast_log(log)
        return {"task": task, "handoff": None, "log": log, "action": action}

    if (status == "cancelled" or archived == 1) and not force_action:
        raise HTTPException(409, "任务已取消/归档，不能执行该动作")

    fields: dict = {}
    handoff_obj: HandoffCreate | None = None
    log_row: dict | None = None

    if action == "accept":
        if status != "pending_acceptance" and not force_action:
            raise HTTPException(409, f"仅 pending_acceptance 可验收通过，当前为 {status}")
        fields = {"status": "completed", "assignee": None}
        handoff_obj = HandoffCreate(
            stage="acceptance_complete",
            from_agent="user",
            to_agent="system",
            status_from=before.get("status"),
            status_to="completed",
            title="验收通过",
            summary="用户验收通过，任务完成",
            conclusion="用户验收通过",
            payload={"decision": "accept"},
        )
        log_row = {"agent": "user", "message": "✅ 用户验收通过，任务完成"}
    elif action == "reject":
        if status != "pending_acceptance" and not force_action:
            raise HTTPException(409, f"仅 pending_acceptance 可退回修改，当前为 {status}")
        feedback = str(body.feedback or "").strip()
        if not feedback:
            raise HTTPException(422, "feedback 不能为空")
        dev_agent = normalize_agent_key(
            before.get("dev_agent") or before.get("assigned_agent") or "developer"
        )
        fields = {
            "status": "needs_changes",
            "assignee": None,
            "assigned_agent": dev_agent,
            "dev_agent": dev_agent,
            "review_feedback": feedback[:1000],
            "feedback_source": "user",
            "feedback_stage": "user_to_dev",
            "feedback_actor": "user",
        }
        handoff_obj = HandoffCreate(
            stage="user_to_dev",
            from_agent="user",
            to_agent=dev_agent,
            status_from=before.get("status"),
            status_to="needs_changes",
            title="人工退回开发",
            summary=feedback[:300],
            commit_hash=str(before.get("commit_hash") or "").strip() or None,
            conclusion="用户验收未通过，退回开发修复",
            payload={
                "decision": "request_changes",
                "feedback_source": "user",
                "feedback_actor": "user",
                "feedback": feedback[:1000],
            },
        )
        log_row = {"agent": "user", "message": f"↩ 人工退回：{feedback[:300]}"}
    elif action == "retry_blocked":
        if status != "blocked" and not force_action:
            raise HTTPException(409, f"仅 blocked 可重试，当前为 {status}")
        target = _resolve_blocked_retry(before)
        if not target:
            raise HTTPException(409, "当前 blocked 任务无法自动判定重试目标，请人工调整")
        to_status, to_agent, label = target
        fields = {"status": to_status, "assignee": None}
        if to_agent is not None:
            fields["assigned_agent"] = to_agent
        handoff_obj = HandoffCreate(
            stage="user_retry_blocked",
            from_agent="user",
            to_agent=to_agent,
            status_from=before.get("status"),
            status_to=to_status,
            title="人工重试任务",
            summary=f"{label}：恢复到 {to_status}",
            conclusion=f"用户触发重试，恢复到 {to_status}",
            payload={
                "action": "retry_blocked",
                "resume_status": to_status,
                "resume_assigned_agent": to_agent,
            },
        )
        log_row = {"agent": "user", "message": f"♻ 人工重试：{label}（{to_status}）"}
    elif action == "decompose":
        if status != "todo" and not force_action:
            raise HTTPException(409, f"仅 todo 可转为分解，当前为 {status}")
        if before.get("parent_task_id"):
            raise HTTPException(409, "子任务不支持手动转分解")
        fields = {"status": "decompose", "assignee": None}
        handoff_obj = HandoffCreate(
            stage="user_force_decompose",
            from_agent="user",
            to_agent="leader",
            status_from=before.get("status"),
            status_to="decompose",
            title="人工要求分解",
            summary="用户要求将任务转为分解模式",
            conclusion="已切换到待分解，等待 Leader 处理",
            payload={"action": "decompose"},
        )
        log_row = {"agent": "user", "message": "↪ 人工要求分解：任务已转为 decompose"}
    else:
        raise HTTPException(422, f"未知 action: {action}")

    normalized_fields = _normalize_transition_fields(before, fields)
    task_for_handoff = dict(before)
    task_for_handoff.update(normalized_fields)
    handoff_row = _build_handoff_row(task_for_handoff, handoff_obj)
    result = await _apply_transition_and_broadcast(
        task_id,
        before=before,
        fields=normalized_fields,
        handoff_row=handoff_row,
        log_row=log_row,
        skip_status_validation=force_action,
    )
    result["action"] = action
    result["forced"] = force_action
    return result


@app.post("/tasks/{task_id}/cancel")
async def cancel_task(task_id: str, body: CancelTaskRequest | None = None, user: dict = Depends(require_user)):
    require_task_access(task_id, user)
    payload = body or CancelTaskRequest()
    cancelled = db.cancel_task(
        task_id,
        include_subtasks=payload.include_subtasks,
    )
    if not cancelled:
        raise HTTPException(404, "Task not found")

    reason = (payload.reason or "").strip()
    root_msg = "任务已取消并归档，不再执行。"
    if reason:
        root_msg += f"\n原因：{reason}"
    child_msg = "因父任务被取消，任务已取消并归档，不再执行。"
    if reason:
        child_msg += f"\n父任务取消原因：{reason}"

    for t in cancelled:
        message = root_msg if t["id"] == task_id else child_msg
        log = db.add_log(t["id"], "system", message)
        await broadcast_log(log)
        await manager.broadcast({"event": "task_updated", "task": t})
        _schedule_task_workspace_cleanup(t, reason="task_cancelled")

    return {
        "task": cancelled[0],
        "affected_count": len(cancelled),
        "affected_task_ids": [t["id"] for t in cancelled],
    }

@app.get("/tasks/{task_id}/subtasks")
async def get_subtasks(task_id: str):
    task = db.get_task(task_id)
    if not task:
        raise HTTPException(404, "Task not found")
    return db.list_subtasks(task_id)


@app.get("/tasks/{task_id}/dependencies")
async def get_task_dependencies(task_id: str, user: dict = Depends(require_user)):
    require_task_access(task_id, user)
    dependencies = db.list_task_dependencies(task_id)
    if dependencies is None:
        raise HTTPException(404, "Task not found")
    dependents = db.list_task_dependents(task_id) or []
    compact_dependents = [
        {
            "id": t.get("id"),
            "project_id": t.get("project_id"),
            "title": t.get("title"),
            "status": t.get("status"),
            "priority": t.get("priority"),
            "ready": t.get("ready"),
            "blocking_dependency_count": t.get("blocking_dependency_count"),
        }
        for t in dependents
    ]
    return {
        "dependencies": dependencies,
        "dependents": compact_dependents,
    }


@app.put("/tasks/{task_id}/dependencies")
async def replace_task_dependencies(
    task_id: str,
    body: TaskDependenciesUpdate,
    user: dict = Depends(require_user),
):
    require_task_access(task_id, user)
    is_admin = _is_admin(user)
    if not is_admin:
        for dep in body.dependencies:
            dep_id = str(dep.depends_on_task_id or "").strip()
            if not dep_id:
                raise HTTPException(422, "depends_on_task_id 不能为空")
            visible = db.get_task(dep_id, user_id=user["id"], is_admin=False)
            if not visible:
                raise HTTPException(404, f"Task not found: {dep_id}")

    payload = [item.model_dump() for item in body.dependencies]
    try:
        dependencies = db.replace_task_dependencies(
            task_id,
            payload,
            created_by=user.get("id"),
        )
    except db.DependencyCycleError as e:
        raise HTTPException(409, str(e))
    except db.DependencyValidationError as e:
        raise HTTPException(422, str(e))
    if dependencies is None:
        raise HTTPException(404, "Task not found")

    task = db.get_task(task_id, user_id=user["id"], is_admin=is_admin)
    if not task:
        raise HTTPException(404, "Task not found")
    await manager.broadcast({"event": "task_updated", "task": task})
    return {"task": task, "dependencies": dependencies}


@app.get("/tasks/{task_id}/logs")
async def get_logs(task_id: str, user: dict = Depends(require_user)):
    require_task_access(task_id, user)
    return db.get_logs(task_id)

@app.post("/tasks/{task_id}/logs", status_code=201)
async def add_log(task_id: str, body: LogCreate, _principal: dict = Depends(require_agent_or_user)):
    agent_name = str(body.agent or "").strip().lower()
    strict_fence = agent_name not in {"system", "user"}
    ok, reason = db.validate_task_lease(
        task_id=task_id,
        expected_run_id=body.run_id,
        expected_lease_token=body.lease_token,
        strict_if_active=strict_fence,
    )
    if not ok:
        if reason == "task_not_found":
            raise HTTPException(404, "Task not found")
        raise HTTPException(409, reason)
    log = db.add_log(task_id, body.agent, body.message)
    if agent_name and agent_name != "system":
        state = _ensure_agent_state(agent_name)
        state["last_output_at"] = _utcnow_iso()
    await broadcast_log(log)
    return log


@app.post("/alerts", status_code=201)
async def add_alert(body: AgentAlertCreate, _principal: dict = Depends(require_agent_or_user)):
    kind = (body.kind or "error").strip().lower()
    if kind not in {"error", "warning", "info"}:
        kind = "error"
    alert = {
        "event": "agent_alert",
        "kind": kind,
        "agent": body.agent,
        "task_id": body.task_id,
        "summary": (body.summary or "").strip()[:240] or "系统告警",
        "message": (body.message or "").strip()[:1200],
        "code": (body.code or "").strip()[:120],
        "stage": (body.stage or "").strip()[:120],
        "metadata": body.metadata or {},
        "created_at": datetime.utcnow().isoformat(),
    }
    await manager.broadcast(alert)
    return alert


def _format_handoff(h: dict) -> dict:
    out = dict(h)
    try:
        payload = h["payload"] if isinstance(h.get("payload"), dict) else json.loads(h.get("payload") or "{}")
    except Exception:
        payload = {}
    out["payload"] = payload
    if not out.get("commit_hash"):
        ch = payload.get("commit_hash")
        out["commit_hash"] = ch if isinstance(ch, str) and ch.strip() else None
    if not out.get("conclusion"):
        cc = payload.get("conclusion")
        if isinstance(cc, str) and cc.strip():
            out["conclusion"] = cc.strip()[:500]
        elif isinstance(payload.get("decision"), str):
            out["conclusion"] = payload["decision"]
        elif out.get("summary"):
            out["conclusion"] = str(out["summary"])[:500]
    return out


@app.get("/tasks/{task_id}/handoffs")
async def get_handoffs(task_id: str):
    task = db.get_task(task_id)
    if not task:
        raise HTTPException(404, "Task not found")
    return [_format_handoff(h) for h in db.get_handoffs(task_id)]


@app.post("/tasks/{task_id}/handoffs", status_code=201)
async def add_handoff(
    task_id: str,
    body: HandoffCreate,
    _principal: dict = Depends(require_agent_or_user),
):
    task = db.get_task(task_id)
    if not task:
        raise HTTPException(404, "Task not found")
    has_fence = bool(str(body.expected_run_id or "").strip() and str(body.expected_lease_token or "").strip())
    ok, reason = db.validate_task_lease(
        task_id=task_id,
        expected_run_id=body.expected_run_id,
        expected_lease_token=body.expected_lease_token,
        strict_if_active=has_fence,
    )
    if not ok:
        if reason == "task_not_found":
            raise HTTPException(404, "Task not found")
        raise HTTPException(409, reason)
    payload, commit_hash, conclusion = _prepare_handoff(task, body)
    stage = str(body.stage or "").strip()
    from_agent = str(body.from_agent or "").strip()
    handoff = db.add_handoff(
        task_id=task_id,
        stage=stage,
        from_agent=from_agent,
        to_agent=body.to_agent,
        status_from=body.status_from,
        status_to=body.status_to,
        title=body.title,
        summary=body.summary,
        commit_hash=commit_hash,
        conclusion=conclusion,
        payload=payload,
        artifact_path=body.artifact_path,
    )
    formatted = _format_handoff(handoff)
    await manager.broadcast({"event": "handoff_added", "handoff": formatted})
    return formatted

@app.get("/tasks/{task_id}/files")
async def get_task_files(task_id: str, user: dict = Depends(require_user)):
    task = require_task_access(task_id, user)

    project_path = task.get("project_path")
    if not project_path:
        return {"location": "(未关联项目)", "branch": "-", "files": [], "has_real_commit": False}

    proj_root   = Path(project_path)
    dev_key = task_dev_agent(task)
    scope = task_scope_suffix(task)
    worktree_dev = (
        proj_root / ".worktrees" / dev_key / scope
        if scope
        else proj_root / ".worktrees" / dev_key
    )
    status       = task.get("status", "")
    commit_hash  = task.get("commit_hash", "")

    if status == "pending_acceptance" or status == "completed":
        inspect_dir = proj_root
        branch      = "main"
        rel_base    = ""
    else:
        inspect_dir = worktree_dev
        branch      = task_dev_branch(task)
        rel_base    = f".worktrees/{dev_key}/{scope}/" if scope else f".worktrees/{dev_key}/"

    files = []
    has_real_commit = False

    if commit_hash:
        probe_dirs = []
        if inspect_dir.exists():
            probe_dirs.append(inspect_dir)
        if proj_root.exists() and proj_root not in probe_dirs:
            probe_dirs.append(proj_root)
        for probe in probe_dirs:
            try:
                log_res = subprocess.run(
                    ["git", "log", "--oneline", f"{commit_hash}^!"],
                    cwd=str(probe), capture_output=True, text=True, timeout=5
                )
                msg = log_res.stdout.strip()
                if not msg:
                    continue
                is_scaffold = any(k in msg for k in ["chore: init", "initial commit", "Initial commit"])
                if is_scaffold:
                    break
                res = subprocess.run(
                    ["git", "show", "--name-only", "--format=", commit_hash],
                    cwd=str(probe), capture_output=True, text=True, timeout=5
                )
                raw = [f.strip() for f in res.stdout.strip().splitlines() if f.strip()]
                files = [(rel_base + f) for f in raw]
                has_real_commit = bool(files)
                if has_real_commit:
                    break
            except Exception:
                continue

    return {
        "location": str(proj_root),
        "branch": branch,
        "files": files,
        "has_real_commit": has_real_commit,
    }


# ── Prompts endpoints (database-backed) ───────────────────────────────────────
ALLOWED_PROMPT_AGENTS = {"developer", "reviewer", "manager", "leader"}


class PromptUpdate(BaseModel):
    content: str


def _get_prompt_agent(agent_name: str) -> dict:
    if agent_name not in ALLOWED_PROMPT_AGENTS:
        raise HTTPException(400, "Invalid agent name")
    at = db.get_agent_type(agent_name)
    if not at:
        raise HTTPException(404, "Agent type not found")
    return at


async def _update_prompt_and_broadcast(agent_name: str, content: str) -> dict:
    updated = db.update_agent_type(agent_name, prompt=content)
    if not updated:
        raise HTTPException(404, "Agent type not found")
    await manager.broadcast({"event": "agent_type_updated", "agent_type": updated})
    return updated


@app.get("/prompts/{agent_name}")
async def get_prompt(agent_name: str, _user: dict = Depends(require_user)):
    at = _get_prompt_agent(agent_name)
    return {
        "agent": agent_name,
        "content": at.get("prompt", ""),
        "source": "database",
    }


@app.put("/prompts/{agent_name}")
async def update_prompt(agent_name: str, body: PromptUpdate, _admin: dict = Depends(require_admin)):
    _get_prompt_agent(agent_name)
    await _update_prompt_and_broadcast(agent_name, body.content)
    return {"ok": True, "source": "database"}


@app.get("/projects/{project_id}/prompts/{agent_name}")
async def get_project_prompt(project_id: str, agent_name: str, user: dict = Depends(require_user)):
    # Project-level prompt overrides are removed; keep endpoint for backward compatibility.
    require_project_access(project_id, user)
    at = _get_prompt_agent(agent_name)
    return {
        "agent": agent_name,
        "content": at.get("prompt", ""),
        "source": "database",
    }


@app.put("/projects/{project_id}/prompts/{agent_name}")
async def update_project_prompt(
    project_id: str,
    agent_name: str,
    body: PromptUpdate,
    user: dict = Depends(require_admin),
):
    # Compatibility route: writes to global DB prompt.
    require_project_access(project_id, user)
    _get_prompt_agent(agent_name)
    await _update_prompt_and_broadcast(agent_name, body.content)
    return {"ok": True, "source": "database"}


@app.delete("/projects/{project_id}/prompts/{agent_name}")
async def delete_project_prompt(project_id: str, agent_name: str, user: dict = Depends(require_admin)):
    # Compatibility route: project-level override no longer exists.
    require_project_access(project_id, user)
    _get_prompt_agent(agent_name)
    return {"ok": True, "source": "database"}


# ── File management helpers ──────────────────────────────────────────────────
def safe_resolve(project_path: str, relative: str) -> Path:
    """Resolve relative path within project root, preventing path traversal."""
    root = Path(project_path).resolve()
    target = (root / relative).resolve()
    if not (target == root or str(target).startswith(str(root) + "/")):
        raise HTTPException(403, "Path traversal not allowed")
    return target


# ── File management endpoints ────────────────────────────────────────────────
@app.get("/projects/{project_id}/files")
async def list_project_files(project_id: str, path: str = "", user: dict = Depends(require_user)):
    p = require_project_access(project_id, user)
    target = safe_resolve(p["path"], path)
    if not target.exists() or not target.is_dir():
        raise HTTPException(404, "Directory not found")

    items = []
    try:
        for entry in sorted(target.iterdir(), key=lambda e: (not e.is_dir(), e.name.lower())):
            if entry.name.startswith("."):
                continue
            stat = entry.stat()
            items.append({
                "name": entry.name,
                "path": str(entry.relative_to(Path(p["path"]).resolve())),
                "is_dir": entry.is_dir(),
                "size": stat.st_size if not entry.is_dir() else 0,
                "modified": stat.st_mtime,
            })
    except PermissionError:
        raise HTTPException(403, "Permission denied")
    return items


@app.get("/projects/{project_id}/files/download")
async def download_project_file(project_id: str, path: str, user: dict = Depends(require_user)):
    p = require_project_access(project_id, user)
    target = safe_resolve(p["path"], path)
    if not target.exists() or not target.is_file():
        raise HTTPException(404, "File not found")
    mime, _ = mimetypes.guess_type(str(target))
    return FileResponse(str(target), media_type=mime or "application/octet-stream", filename=target.name)


@app.post("/projects/{project_id}/files/upload")
async def upload_project_files(
    project_id: str,
    files: list[UploadFile] = FastAPIFile(...),
    path: str = "",
    user: dict = Depends(require_user),
):
    p = require_project_access(project_id, user)
    target_dir = safe_resolve(p["path"], path)
    if not target_dir.exists() or not target_dir.is_dir():
        raise HTTPException(404, "Directory not found")

    uploaded = []
    touched_paths: list[Path] = []
    for f in files:
        # Sanitize filename — strip path components
        safe_name = Path(f.filename).name if f.filename else "untitled"
        dest = target_dir / safe_name
        with open(dest, "wb") as out:
            shutil.copyfileobj(f.file, out)
        uploaded.append(safe_name)
        touched_paths.append(dest)

    git_result = _auto_commit_project_paths_to_main(
        Path(p["path"]),
        changed_paths=touched_paths,
        action="upload",
    )

    await manager.broadcast({"event": "files_changed", "project_id": project_id, "path": path})
    return {"uploaded": uploaded, "git": git_result}


@app.delete("/projects/{project_id}/files")
async def delete_project_file(project_id: str, path: str, user: dict = Depends(require_user)):
    p = require_project_access(project_id, user)
    if not path:
        raise HTTPException(400, "Cannot delete project root")
    target = safe_resolve(p["path"], path)
    if not target.exists():
        raise HTTPException(404, "File not found")
    if target.is_dir():
        shutil.rmtree(target)
    else:
        target.unlink()
    git_result = _auto_commit_project_paths_to_main(
        Path(p["path"]),
        changed_paths=[target],
        action="delete",
    )
    parent_rel = str(Path(path).parent) if str(Path(path).parent) != "." else ""
    await manager.broadcast({"event": "files_changed", "project_id": project_id, "path": parent_rel})
    return {"ok": True, "git": git_result}


@app.post("/projects/{project_id}/files/mkdir")
async def mkdir_project(project_id: str, body: MkdirRequest, user: dict = Depends(require_user)):
    p = require_project_access(project_id, user)
    target = safe_resolve(p["path"], body.path)
    if target.exists():
        raise HTTPException(400, "Path already exists")
    target.mkdir(parents=True, exist_ok=True)
    parent_rel = str(Path(body.path).parent) if str(Path(body.path).parent) != "." else ""
    await manager.broadcast({"event": "files_changed", "project_id": project_id, "path": parent_rel})
    return {"ok": True}


# ── Agent Types ────────────────────────────────────────────────────────────────

@app.get("/agent-types")
async def list_agent_types():
    return db.list_agent_types()

# NOTE: specific routes must come before parametric /{agent_key}
@app.post("/agent-types/generate-prompt")
async def generate_agent_prompt(body: GeneratePromptRequest, _admin: dict = Depends(require_admin)):
    """Call the local CLI to generate a prompt template for a new agent."""
    meta_prompt = (
        "你是一个专业的AI Agent提示词工程师。"
        f"请为以下用途的AI Agent编写一个详细的提示词模板：\n\n{body.description}\n\n"
        "要求：\n"
        "1. 提示词清晰、具体、可操作\n"
        "2. 按需使用以下占位符变量（用花括号包裹）：\n"
        "   {task_title} - 任务标题\n"
        "   {task_description} - 任务的详细需求描述\n"
        "   {rework_section} - 审查反馈（返工时会有内容，初次为空）\n"
        "3. 只输出提示词内容本身，不要任何前缀说明或解释"
    )
    cli = body.cli if body.cli in ("claude", "codex") else "claude"
    cmd = ["claude", "--dangerously-skip-permissions", "-p", meta_prompt] if cli == "claude" \
          else ["codex", "exec", "--dangerously-bypass-approvals-and-sandbox", meta_prompt]
    try:
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, _ = await asyncio.wait_for(proc.communicate(), timeout=90)
        generated = stdout.decode(errors="replace").strip()
        if not generated:
            raise HTTPException(500, "生成结果为空")
        return {"prompt": generated}
    except asyncio.TimeoutError:
        proc.kill()
        raise HTTPException(504, "生成超时（90s）")
    except FileNotFoundError:
        raise HTTPException(500, f"CLI 工具 '{cli}' 未找到，请先安装")

@app.post("/agent-types", status_code=201)
async def create_agent_type(body: AgentTypeCreate, _admin: dict = Depends(require_admin)):
    if not re.match(r'^[a-z][a-z0-9_-]*$', body.key):
        raise HTTPException(400, "key 只能包含小写字母、数字、连字符和下划线，且以字母开头")
    if db.get_agent_type(body.key):
        raise HTTPException(400, f"key '{body.key}' 已存在")
    at = db.create_agent_type(
        body.key, body.name, body.description, body.prompt,
        body.poll_statuses, body.next_status, body.working_status, body.cli,
    )
    AGENT_OUTPUT[body.key]  # pre-create entry
    AGENT_STATUS[body.key]
    await manager.broadcast({"event": "agent_type_created", "agent_type": at})
    return at

@app.get("/agent-types/{agent_key}")
async def get_agent_type(agent_key: str):
    at = db.get_agent_type(agent_key)
    if not at:
        raise HTTPException(404, "Not found")
    return at

@app.put("/agent-types/{agent_key}")
async def update_agent_type(agent_key: str, body: AgentTypeUpdate, _admin: dict = Depends(require_admin)):
    if not db.get_agent_type(agent_key):
        raise HTTPException(404, "Not found")
    fields = {k: v for k, v in body.model_dump().items() if v is not None}
    if not fields:
        return db.get_agent_type(agent_key)
    updated = db.update_agent_type(agent_key, **fields)
    await manager.broadcast({"event": "agent_type_updated", "agent_type": updated})
    return updated

@app.delete("/agent-types/{agent_key}")
async def delete_agent_type(agent_key: str, _admin: dict = Depends(require_admin)):
    key = normalize_agent_key(agent_key, default=agent_key)
    at = db.get_agent_type(key)
    if not at:
        raise HTTPException(404, "Not found")
    if at["is_builtin"]:
        raise HTTPException(403, "内置 Agent 不可删除")
    db.delete_agent_type(key)
    db.delete_agent_outputs_for_agent(key)
    AGENT_OUTPUT.pop(key, None)
    AGENT_STATUS.pop(key, None)
    touched_tasks = db.clear_task_agent_refs_for_deleted_agent(
        key,
        working_status=str(at.get("working_status") or "").strip() or None,
    )
    await manager.broadcast({"event": "agent_type_deleted", "agent_key": key})
    for task in touched_tasks:
        await manager.broadcast({"event": "task_updated", "task": task})
    return {"ok": True}


# ── Agent terminal endpoints ──────────────────────────────────────────────────
@app.get("/agents/outputs")
async def get_agent_outputs(
    project_id: str | None = Query(default=None),
    user: dict = Depends(require_user),
):
    scope_project_id = str(project_id or "").strip() or None
    if scope_project_id and not db.user_can_access_project(
        scope_project_id,
        user["id"],
        _is_admin(user),
    ):
        raise HTTPException(404, "Project not found")
    return _agent_outputs_snapshot(user=user, project_id=scope_project_id)

@app.post("/agents/{agent_name}/output")
async def agent_output(
    agent_name: str,
    body: AgentOutput,
    principal: dict = Depends(require_agent_or_user),
):
    path_agent_key = normalize_agent_key(agent_name, default=agent_name)
    reported_agent_key = normalize_agent_key(body.agent_key, default=path_agent_key)
    if not db.get_agent_type(reported_agent_key):
        raise HTTPException(404, "Agent type not found")
    worker_id = normalize_worker_key(body.worker_id)
    incoming_task_id = str(body.task_id or "").strip() if body.task_id is not None else None
    incoming_project_id = str(body.project_id or "").strip() if body.project_id is not None else None
    runtime_name, _ = resolve_agent_runtime_id(
        agent_name=reported_agent_key,
        worker_id=worker_id,
        project_id=incoming_project_id,
    )
    state = _ensure_agent_state(runtime_name)
    resolved_project_id = incoming_project_id or str(state.get("project_id") or "").strip() or None
    lookup_task_id = incoming_task_id
    if lookup_task_id is None:
        lookup_task_id = str(state.get("task_id") or "").strip() or None
    if lookup_task_id:
        task_row = db.get_task(lookup_task_id)
        if task_row:
            resolved_project_id = str(task_row.get("project_id") or "").strip() or resolved_project_id
    auth_type = str(principal.get("auth_type") or "").strip().lower()
    if auth_type == "user":
        if resolved_project_id and not db.user_can_access_project(
            resolved_project_id,
            principal["id"],
            _is_admin(principal),
        ):
            raise HTTPException(404, "Project not found")
    runtime_name, _ = resolve_agent_runtime_id(
        agent_name=reported_agent_key,
        worker_id=worker_id,
        project_id=resolved_project_id,
    )
    state = _ensure_agent_state(runtime_name)
    state["agent_key"] = reported_agent_key
    state["worker_id"] = worker_id
    entry = _normalize_agent_output_entry(
        {
            "line": body.line,
            "kind": body.kind,
            "event": body.event,
            "agent_key": reported_agent_key,
            "worker_id": worker_id,
            "project_id": resolved_project_id,
            "task_id": incoming_task_id,
            "run_id": body.run_id,
            "exit_code": body.exit_code,
            "created_at": _utcnow_iso(),
        }
    )
    AGENT_OUTPUT[runtime_name].append(entry)
    state["last_output_at"] = _utcnow_iso()
    if body.task_id is not None:
        state["task_id"] = incoming_task_id or ""
    if incoming_project_id is not None or body.task_id is not None:
        state["project_id"] = str(resolved_project_id or "").strip()
    if body.run_id is not None:
        state["run_id"] = str(body.run_id or "").strip()
    db.add_agent_output(
        agent=runtime_name,
        line=entry["line"],
        project_id=entry["project_id"],
        kind=entry["kind"],
        event=entry["event"],
        exit_code=entry["exit_code"],
        task_id=incoming_task_id,
        run_id=body.run_id,
        keep_last=1000,
    )
    broadcast_bg(
        {
            "event": "agent_output",
            "agent": runtime_name,
            "agent_key": reported_agent_key,
            "worker_id": worker_id,
            "project_id": entry["project_id"],
            "line": entry["line"],
            "output": entry,
        }
    )
    return {"ok": True}

@app.post("/agents/{agent_name}/status")
async def agent_status(
    agent_name: str,
    body: AgentStatusUpdate,
    principal: dict = Depends(require_agent_or_user),
):
    path_agent_key = normalize_agent_key(agent_name, default=agent_name)
    reported_agent_key = normalize_agent_key(body.agent_key, default=path_agent_key)
    cfg = db.get_agent_type(reported_agent_key)
    if not cfg:
        raise HTTPException(404, "Agent type not found")
    worker_id = normalize_worker_key(body.worker_id)
    incoming_project_id = (
        str(body.project_id or "").strip()
        if body.project_id is not None
        else ""
    )
    runtime_name, _ = resolve_agent_runtime_id(
        agent_name=reported_agent_key,
        worker_id=worker_id,
        project_id=incoming_project_id,
    )
    prev = _ensure_agent_state(runtime_name)
    busy = str(body.status or "").strip().lower() == "busy"
    status_value = str(body.status or "").strip() or "idle"
    task_value = str(body.task or "")
    project_id = (
        str(body.project_id or "").strip()
        if body.project_id is not None
        else (str(prev.get("project_id") or "").strip() if busy else "")
    )
    task_id = (
        str(body.task_id or "").strip()
        if body.task_id is not None
        else (str(prev.get("task_id") or "").strip() if busy else "")
    )
    run_id = (
        str(body.run_id or "").strip()
        if body.run_id is not None
        else (str(prev.get("run_id") or "").strip() if busy else "")
    )
    lease_token = (
        str(body.lease_token or "").strip()
        if body.lease_token is not None
        else (str(prev.get("lease_token") or "").strip() if busy else "")
    )
    phase = (
        str(body.phase or "").strip()
        if body.phase is not None
        else (str(prev.get("phase") or "").strip() if busy else "")
    )
    pid = body.pid if body.pid is not None else (prev.get("pid") if busy else None)

    if busy:
        task_row = db.get_task(task_id) if task_id else None
        valid_busy = bool(task_row)
        if valid_busy:
            valid_busy = assignee_matches_agent_type(task_row.get("assignee"), reported_agent_key)
        if valid_busy:
            working_status = str((cfg or {}).get("working_status") or "").strip().lower()
            if working_status:
                valid_busy = _norm_status(task_row.get("status")) == working_status
        if valid_busy and run_id:
            claim_run_id = str(task_row.get("claim_run_id") or "").strip()
            if claim_run_id:
                valid_busy = run_id == claim_run_id
        if valid_busy and lease_token:
            row_lease_token = str(task_row.get("lease_token") or "").strip()
            if row_lease_token:
                valid_busy = lease_token == row_lease_token
        if not valid_busy:
            status_value = "idle"
            task_value = ""
            project_id = ""
            task_id = ""
            run_id = ""
            lease_token = ""
            phase = ""
            pid = None
        else:
            project_id = str(task_row.get("project_id") or "").strip() if task_row else project_id
    auth_type = str(principal.get("auth_type") or "").strip().lower()
    if auth_type == "user":
        if project_id and not db.user_can_access_project(project_id, principal["id"], _is_admin(principal)):
            raise HTTPException(404, "Project not found")
        if task_id and not db.get_task(task_id, user_id=principal["id"], is_admin=_is_admin(principal)):
            raise HTTPException(404, "Task not found")

    runtime_name, _ = resolve_agent_runtime_id(
        agent_name=reported_agent_key,
        worker_id=worker_id,
        project_id=project_id,
    )
    AGENT_STATUS[runtime_name] = {
        "agent_key": reported_agent_key,
        "worker_id": worker_id,
        "status": status_value,
        "task": task_value,
        "project_id": project_id,
        "task_id": task_id,
        "run_id": run_id,
        "lease_token": lease_token,
        "phase": phase,
        "pid": pid,
        "updated_at": _utcnow_iso(),
        "last_output_at": prev.get("last_output_at", ""),
    }
    broadcast_bg(
        {
            "event": "agent_status",
            "agent": runtime_name,
            "agent_key": reported_agent_key,
            "worker_id": worker_id,
            "status": status_value,
            "task": task_value,
            "project_id": project_id,
            "task_id": task_id,
            "run_id": run_id,
            "lease_token": lease_token,
            "phase": phase,
            "pid": pid,
        }
    )
    return {"ok": True}


# ── WebSocket ─────────────────────────────────────────────────────────────────
@app.websocket("/ws")
async def websocket_endpoint(
    ws: WebSocket,
    token: str | None = Query(default=None),
    project_id: str | None = Query(default=None),
):
    user = db.get_session_user(str(token or "").strip())
    if not user:
        await ws.accept()
        await ws.close(code=4401)
        return
    scope_project_id = str(project_id or "").strip() or None
    if scope_project_id and not db.user_can_access_project(
        scope_project_id,
        user["id"],
        _is_admin(user),
    ):
        await ws.accept()
        await ws.close(code=4403)
        return
    await manager.connect(ws)
    try:
        await manager.send_init_and_subscribe(
            ws,
            user,
            lambda: {
                "event": "init",
                "tasks": db.list_tasks(
                    project_id=scope_project_id,
                    user_id=user["id"],
                    is_admin=_is_admin(user),
                ),
                "projects": db.list_projects(user_id=user["id"], is_admin=_is_admin(user)),
                "agent_types": db.list_agent_types(),
                "agent_outputs": _agent_outputs_snapshot(
                    user=user,
                    project_id=scope_project_id,
                ),
                "user": user,
            },
            project_id=scope_project_id,
        )
        while True:
            await ws.receive_text()
    except WebSocketDisconnect:
        pass
    finally:
        manager.disconnect(ws)


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "app:app",
        host="0.0.0.0",
        port=8080,
        reload=False,
        access_log=False,
        log_level="warning",
    )
