import asyncio
import contextlib
import json
import mimetypes
import os
import re
import shutil
import subprocess
from collections import defaultdict, deque
from contextlib import asynccontextmanager
from datetime import datetime
from pathlib import Path
from typing import Literal, Set

from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException, UploadFile, File as FastAPIFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

import sys
sys.path.insert(0, str(Path(__file__).parent))
import db

PROJECT_ROOT = Path(__file__).parent.parent
AGENT_STALE_SECS = int(os.getenv("AGENT_STALE_SECS", "150"))
AGENT_WATCHDOG_SECS = int(os.getenv("AGENT_WATCHDOG_SECS", "15"))


def normalize_agent_key(agent_key: str | None, default: str = "developer") -> str:
    raw = (agent_key or default).strip().lower()
    safe = re.sub(r"[^a-z0-9_-]+", "-", raw).strip("-_")
    return safe or default


def task_dev_agent(task: dict) -> str:
    # Prefer explicit dev ownership; fall back to current assignee for
    # claimed tasks that have not persisted dev_agent/assigned_agent yet.
    candidate = task.get("dev_agent") or task.get("assigned_agent")
    if not str(candidate or "").strip():
        candidate = task.get("assignee")
    return normalize_agent_key(candidate or "developer")


def task_dev_branch(task: dict) -> str:
    return f"agent/{task_dev_agent(task)}"


# ── In-memory agent state (auto-expands for custom agents) ────────────────────
AGENT_OUTPUT: dict = defaultdict(lambda: deque(maxlen=1000))
AGENT_STATUS: dict = defaultdict(
    lambda: {"status": "idle", "task": "", "updated_at": "", "last_output_at": ""}
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
    state.setdefault("status", "idle")
    state.setdefault("task", "")
    state.setdefault("updated_at", "")
    state.setdefault("last_output_at", "")
    return state


def _agent_outputs_snapshot() -> dict:
    return {
        name: {"lines": list(AGENT_OUTPUT[name]), "status": dict(_ensure_agent_state(name))}
        for name in AGENT_OUTPUT
    }


async def _recover_stale_agent(agent_name: str, stale_secs: int):
    state = _ensure_agent_state(agent_name)
    last_output_at = state.get("last_output_at", "")
    AGENT_STATUS[agent_name] = {
        "status": "idle",
        "task": "",
        "updated_at": _utcnow_iso(),
        "last_output_at": last_output_at,
    }
    await manager.broadcast(
        {"event": "agent_status", "agent": agent_name, "status": "idle", "task": ""}
    )

    recovered = db.recover_stale_tasks_for_agent(agent_name)
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
        for agent_name, status in list(AGENT_STATUS.items()):
            if str(status.get("status") or "").strip().lower() != "busy":
                continue
            last_seen = _agent_last_seen(status)
            if not last_seen:
                continue
            stale_secs = int((now - last_seen).total_seconds())
            if stale_secs < AGENT_STALE_SECS:
                continue
            await _recover_stale_agent(agent_name, stale_secs)


# Pre-populate built-ins so they appear on init
for _k in ("developer", "reviewer", "manager"):
    _ensure_agent_state(_k)


# ── WebSocket manager ─────────────────────────────────────────────────────────
class ConnectionManager:
    def __init__(self):
        self.active: Set[WebSocket] = set()
        # Serialize broadcasts to preserve per-connection event order and
        # avoid concurrent writes on the same websocket.
        self._broadcast_lock = asyncio.Lock()

    async def connect(self, ws: WebSocket):
        await ws.accept()

    async def send_init_and_subscribe(self, ws: WebSocket, payload_builder):
        # Keep init snapshot and subscription atomic relative to broadcasts.
        async with self._broadcast_lock:
            payload = payload_builder()
            await ws.send_json(payload)
            self.active.add(ws)

    def disconnect(self, ws: WebSocket):
        self.active.discard(ws)

    async def broadcast(self, data: dict):
        async with self._broadcast_lock:
            sockets = list(self.active)
            if not sockets:
                return

            async def _send(ws: WebSocket):
                # Drop slow/broken sockets quickly so broadcasts never block
                # agent heartbeats or task state transitions for long periods.
                await asyncio.wait_for(ws.send_json(data), timeout=1.5)

            results = await asyncio.gather(
                *(_send(ws) for ws in sockets),
                return_exceptions=True,
            )
            dead = {
                ws for ws, res in zip(sockets, results)
                if isinstance(res, Exception)
            }
            self.active -= dead


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
    watchdog = asyncio.create_task(_agent_health_watchdog())
    try:
        yield
    finally:
        watchdog.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await watchdog


app = FastAPI(title="Multi-Agent Task Board", lifespan=lifespan)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

frontend_dir = PROJECT_ROOT / "frontend"
if frontend_dir.exists():
    app.mount("/static", StaticFiles(directory=str(frontend_dir)), name="static")


# ── Pydantic models ───────────────────────────────────────────────────────────
class ProjectCreate(BaseModel):
    name: str
    path: str  # absolute filesystem path
    import_existing: bool = False

class TaskCreate(BaseModel):
    title: str
    description: str = ""
    project_id: str | None = None
    parent_task_id: str | None = None
    assigned_agent: str | None = None
    dev_agent: str | None = None
    status: str = "triage"   # default: all new tasks enter triage first

class TaskUpdate(BaseModel):
    status: str | None = None
    assignee: str | None = None
    assigned_agent: str | None = None
    dev_agent: str | None = None
    review_feedback: str | None = None
    commit_hash: str | None = None
    archived: int | None = None
    feedback_source: str | None = None
    feedback_stage: str | None = None
    feedback_actor: str | None = None
    create_handoff: bool | None = None

class LogCreate(BaseModel):
    agent: str
    message: str

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
    line: str

class AgentStatusUpdate(BaseModel):
    status: str
    task: str = ""

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
    project_id: str | None = None

class CancelTaskRequest(BaseModel):
    reason: str | None = None
    include_subtasks: bool = True


class TaskTransitionRequest(BaseModel):
    fields: TaskUpdate | None = None
    handoff: HandoffCreate | None = None
    log: LogCreate | None = None


class TaskActionRequest(BaseModel):
    action: Literal["accept", "reject", "retry_blocked", "decompose", "archive"]
    feedback: str | None = None


COMMIT_REQUIRED_STAGES = {
    "dev_to_review",
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
    "in_progress": {"in_progress", "in_review", "todo", "needs_changes", "blocked", "cancelled"},
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
) -> dict:
    normalized_fields = _normalize_transition_fields(before, fields)
    _validate_status_transition(before, normalized_fields)

    result = db.transition_task(
        task_id,
        fields=normalized_fields,
        handoff=handoff_row,
        log=log_row,
    )
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

    raw_commit = (
        str(body.commit_hash or "").strip()
        or str(payload.get("commit_hash") or "").strip()
        or str(task.get("commit_hash") or "").strip()
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


# ── Projects ──────────────────────────────────────────────────────────────────
@app.get("/projects")
async def list_projects():
    return db.list_projects()

@app.post("/projects", status_code=201)
async def create_project(body: ProjectCreate):
    path = Path(body.path).expanduser().resolve()
    all_projects = db.list_projects()
    if any(p["path"] == str(path) for p in all_projects):
        raise HTTPException(400, "Path already used by another project")
    if body.import_existing:
        if not path.exists():
            raise HTTPException(400, "Existing project path does not exist")
        if not path.is_dir():
            raise HTTPException(400, "Existing project path must be a directory")
    project = db.create_project(body.name, str(path))
    await manager.broadcast({"event": "project_created", "project": project})
    return project

@app.get("/projects/{project_id}")
async def get_project(project_id: str):
    p = db.get_project(project_id)
    if not p:
        raise HTTPException(404, "Project not found")
    return p


@app.get("/fs/directories")
async def list_directories(path: str = "~"):
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
async def sync_projects():
    """Delete projects whose directories no longer exist on disk.

    Safety: do not delete projects that still have claimed (running) tasks,
    otherwise in-flight agents may hit 404 when syncing task status.
    """
    all_projects = db.list_projects()
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
            await manager.broadcast({"event": "project_deleted", "project_id": p["id"]})
    return {"deleted": deleted, "kept": kept, "skipped_busy": skipped_busy}


@app.post("/projects/{project_id}/setup")
async def setup_project(project_id: str):
    """Initialize git repo in the project directory (without shared dev worktree)."""
    p = db.get_project(project_id)
    if not p:
        raise HTTPException(404, "Project not found")

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
async def list_tasks(project_id: str | None = None):
    return db.list_tasks(project_id)

@app.post("/tasks", status_code=201)
async def create_task(body: TaskCreate):
    task = db.create_task(body.title, body.description, body.project_id,
                          body.parent_task_id, body.assigned_agent, body.dev_agent, body.status)
    await manager.broadcast({"event": "task_created", "task": task})
    return task

# NOTE: /tasks/status/{status} must come before /tasks/{task_id}
@app.get("/tasks/status/{status}")
async def tasks_by_status(status: str, project_id: str | None = None):
    return db.get_tasks_by_status(status, project_id)

@app.post("/tasks/claim")
async def claim_task(body: TaskClaim):
    effective_status = str(body.status or "").strip()
    effective_working_status = str(body.working_status or "").strip()
    at = db.get_agent_type(body.agent_key)
    if at:
        poll_statuses = _parse_poll_statuses(at.get("poll_statuses"))
        if poll_statuses and effective_status not in poll_statuses:
            raise HTTPException(
                409,
                f"agent={body.agent_key} 不能认领 status={effective_status}，允许: {poll_statuses}",
            )
        configured_working = str(at.get("working_status") or "").strip()
        if configured_working:
            effective_working_status = configured_working

    # Retry loop: skip tasks whose project directory is missing.
    for _ in range(50):
        task = db.claim_task(
            status=effective_status,
            working_status=effective_working_status,
            agent=body.agent,
            agent_key=body.agent_key,
            respect_assignment=body.respect_assignment,
            project_id=body.project_id,
        )
        if not task:
            return {"task": None}

        project_id = str(task.get("project_id") or "").strip()
        project_path = str(task.get("project_path") or "").strip()
        if not project_id or not project_path or Path(project_path).exists():
            await manager.broadcast({"event": "task_updated", "task": task})
            return {"task": task}

        # Project path no longer exists: immediately cancel this task so it
        # won't be picked again, then continue claiming next available task.
        cancelled = db.cancel_task(task["id"], include_subtasks=False) or []
        for item in cancelled:
            await manager.broadcast({"event": "task_updated", "task": item})
            log = db.add_log(
                item["id"],
                "system",
                "⚠ 项目目录不存在，任务已自动取消并跳过分配。",
            )
            await broadcast_log(log)

    return {"task": None}

@app.get("/tasks/{task_id}")
async def get_task(task_id: str):
    task = db.get_task(task_id)
    if not task:
        raise HTTPException(404, "Task not found")
    return task

@app.patch("/tasks/{task_id}")
async def update_task(task_id: str, body: TaskUpdate):
    fields = body.model_dump(exclude_unset=True)
    fields.pop("create_handoff", None)

    before = db.get_task(task_id)
    if not before:
        raise HTTPException(404, "Task not found")

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
async def transition_task(task_id: str, body: TaskTransitionRequest):
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
    )


@app.post("/tasks/{task_id}/actions")
async def task_action(task_id: str, body: TaskActionRequest):
    before = db.get_task(task_id)
    if not before:
        raise HTTPException(404, "Task not found")

    status = _norm_status(before.get("status"))
    archived = int(before.get("archived") or 0)
    action = body.action

    if action == "archive":
        if status != "completed":
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

    if status == "cancelled" or archived == 1:
        raise HTTPException(409, "任务已取消/归档，不能执行该动作")

    fields: dict = {}
    handoff_obj: HandoffCreate | None = None
    log_row: dict | None = None

    if action == "accept":
        if status != "pending_acceptance":
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
        if status != "pending_acceptance":
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
        if status != "blocked":
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
        if status != "todo":
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
    )
    result["action"] = action
    return result


@app.post("/tasks/{task_id}/cancel")
async def cancel_task(task_id: str, body: CancelTaskRequest | None = None):
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

    return {
        "task": cancelled[0],
        "affected_count": len(cancelled),
        "affected_task_ids": [t["id"] for t in cancelled],
    }

@app.get("/tasks/{task_id}/subtasks")
async def get_subtasks(task_id: str):
    return db.list_subtasks(task_id)

@app.get("/tasks/{task_id}/logs")
async def get_logs(task_id: str):
    return db.get_logs(task_id)

@app.post("/tasks/{task_id}/logs", status_code=201)
async def add_log(task_id: str, body: LogCreate):
    log = db.add_log(task_id, body.agent, body.message)
    await broadcast_log(log)
    return log


@app.post("/alerts", status_code=201)
async def add_alert(body: AgentAlertCreate):
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
async def add_handoff(task_id: str, body: HandoffCreate):
    task = db.get_task(task_id)
    if not task:
        raise HTTPException(404, "Task not found")
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
async def get_task_files(task_id: str):
    task = db.get_task(task_id)
    if not task:
        raise HTTPException(404, "Task not found")

    project_path = task.get("project_path")
    if not project_path:
        return {"location": "(未关联项目)", "branch": "-", "files": [], "has_real_commit": False}

    proj_root   = Path(project_path)
    dev_key = task_dev_agent(task)
    worktree_dev = proj_root / ".worktrees" / dev_key
    status       = task.get("status", "")
    commit_hash  = task.get("commit_hash", "")

    if status == "pending_acceptance" or status == "completed":
        inspect_dir = proj_root
        branch      = "main"
        rel_base    = ""
    else:
        inspect_dir = worktree_dev
        branch      = task_dev_branch(task)
        rel_base    = f".worktrees/{dev_key}/"

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
async def get_prompt(agent_name: str):
    at = _get_prompt_agent(agent_name)
    return {
        "agent": agent_name,
        "content": at.get("prompt", ""),
        "source": "database",
    }


@app.put("/prompts/{agent_name}")
async def update_prompt(agent_name: str, body: PromptUpdate):
    _get_prompt_agent(agent_name)
    await _update_prompt_and_broadcast(agent_name, body.content)
    return {"ok": True, "source": "database"}


@app.get("/projects/{project_id}/prompts/{agent_name}")
async def get_project_prompt(project_id: str, agent_name: str):
    # Project-level prompt overrides are removed; keep endpoint for backward compatibility.
    p = db.get_project(project_id)
    if not p:
        raise HTTPException(404, "Project not found")
    at = _get_prompt_agent(agent_name)
    return {
        "agent": agent_name,
        "content": at.get("prompt", ""),
        "source": "database",
    }


@app.put("/projects/{project_id}/prompts/{agent_name}")
async def update_project_prompt(project_id: str, agent_name: str, body: PromptUpdate):
    # Compatibility route: writes to global DB prompt.
    p = db.get_project(project_id)
    if not p:
        raise HTTPException(404, "Project not found")
    _get_prompt_agent(agent_name)
    await _update_prompt_and_broadcast(agent_name, body.content)
    return {"ok": True, "source": "database"}


@app.delete("/projects/{project_id}/prompts/{agent_name}")
async def delete_project_prompt(project_id: str, agent_name: str):
    # Compatibility route: project-level override no longer exists.
    p = db.get_project(project_id)
    if not p:
        raise HTTPException(404, "Project not found")
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
async def list_project_files(project_id: str, path: str = ""):
    p = db.get_project(project_id)
    if not p:
        raise HTTPException(404, "Project not found")
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
async def download_project_file(project_id: str, path: str):
    p = db.get_project(project_id)
    if not p:
        raise HTTPException(404, "Project not found")
    target = safe_resolve(p["path"], path)
    if not target.exists() or not target.is_file():
        raise HTTPException(404, "File not found")
    mime, _ = mimetypes.guess_type(str(target))
    return FileResponse(str(target), media_type=mime or "application/octet-stream", filename=target.name)


@app.post("/projects/{project_id}/files/upload")
async def upload_project_files(project_id: str, files: list[UploadFile] = FastAPIFile(...), path: str = ""):
    p = db.get_project(project_id)
    if not p:
        raise HTTPException(404, "Project not found")
    target_dir = safe_resolve(p["path"], path)
    if not target_dir.exists() or not target_dir.is_dir():
        raise HTTPException(404, "Directory not found")

    uploaded = []
    for f in files:
        # Sanitize filename — strip path components
        safe_name = Path(f.filename).name if f.filename else "untitled"
        dest = target_dir / safe_name
        with open(dest, "wb") as out:
            shutil.copyfileobj(f.file, out)
        uploaded.append(safe_name)

    await manager.broadcast({"event": "files_changed", "project_id": project_id, "path": path})
    return {"uploaded": uploaded}


@app.delete("/projects/{project_id}/files")
async def delete_project_file(project_id: str, path: str):
    p = db.get_project(project_id)
    if not p:
        raise HTTPException(404, "Project not found")
    if not path:
        raise HTTPException(400, "Cannot delete project root")
    target = safe_resolve(p["path"], path)
    if not target.exists():
        raise HTTPException(404, "File not found")
    if target.is_dir():
        shutil.rmtree(target)
    else:
        target.unlink()
    parent_rel = str(Path(path).parent) if str(Path(path).parent) != "." else ""
    await manager.broadcast({"event": "files_changed", "project_id": project_id, "path": parent_rel})
    return {"ok": True}


@app.post("/projects/{project_id}/files/mkdir")
async def mkdir_project(project_id: str, body: MkdirRequest):
    p = db.get_project(project_id)
    if not p:
        raise HTTPException(404, "Project not found")
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
async def generate_agent_prompt(body: GeneratePromptRequest):
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
async def create_agent_type(body: AgentTypeCreate):
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
async def update_agent_type(agent_key: str, body: AgentTypeUpdate):
    if not db.get_agent_type(agent_key):
        raise HTTPException(404, "Not found")
    fields = {k: v for k, v in body.model_dump().items() if v is not None}
    if not fields:
        return db.get_agent_type(agent_key)
    updated = db.update_agent_type(agent_key, **fields)
    await manager.broadcast({"event": "agent_type_updated", "agent_type": updated})
    return updated

@app.delete("/agent-types/{agent_key}")
async def delete_agent_type(agent_key: str):
    at = db.get_agent_type(agent_key)
    if not at:
        raise HTTPException(404, "Not found")
    if at["is_builtin"]:
        raise HTTPException(403, "内置 Agent 不可删除")
    db.delete_agent_type(agent_key)
    await manager.broadcast({"event": "agent_type_deleted", "agent_key": agent_key})
    return {"ok": True}


# ── Agent terminal endpoints ──────────────────────────────────────────────────
@app.get("/agents/outputs")
async def get_agent_outputs():
    return _agent_outputs_snapshot()

@app.post("/agents/{agent_name}/output")
async def agent_output(agent_name: str, body: AgentOutput):
    state = _ensure_agent_state(agent_name)
    AGENT_OUTPUT[agent_name].append(body.line)
    state["last_output_at"] = _utcnow_iso()
    broadcast_bg({"event": "agent_output", "agent": agent_name, "line": body.line})
    return {"ok": True}

@app.post("/agents/{agent_name}/status")
async def agent_status(agent_name: str, body: AgentStatusUpdate):
    prev = _ensure_agent_state(agent_name)
    AGENT_STATUS[agent_name] = {
        "status": body.status,
        "task": body.task,
        "updated_at": _utcnow_iso(),
        "last_output_at": prev.get("last_output_at", ""),
    }
    broadcast_bg({"event": "agent_status", "agent": agent_name,
                  "status": body.status, "task": body.task})
    return {"ok": True}


# ── WebSocket ─────────────────────────────────────────────────────────────────
@app.websocket("/ws")
async def websocket_endpoint(ws: WebSocket):
    await manager.connect(ws)
    try:
        await manager.send_init_and_subscribe(
            ws,
            lambda: {
                "event": "init",
                "tasks": db.list_tasks(),
                "projects": db.list_projects(),
                "agent_types": db.list_agent_types(),
                "agent_outputs": _agent_outputs_snapshot(),
            },
        )
        while True:
            await ws.receive_text()
    except WebSocketDisconnect:
        pass
    finally:
        manager.disconnect(ws)


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app:app", host="0.0.0.0", port=8080, reload=False)
