import asyncio
import mimetypes
import re
import shutil
import subprocess
from collections import defaultdict, deque
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Set

from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException, UploadFile, File as FastAPIFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

import sys
sys.path.insert(0, str(Path(__file__).parent))
import db

PROJECT_ROOT = Path(__file__).parent.parent


def normalize_agent_key(agent_key: str | None, default: str = "developer") -> str:
    raw = (agent_key or default).strip().lower()
    safe = re.sub(r"[^a-z0-9_-]+", "-", raw).strip("-_")
    return safe or default


def task_dev_agent(task: dict) -> str:
    return normalize_agent_key(task.get("dev_agent") or task.get("assigned_agent") or "developer")


def task_dev_branch(task: dict) -> str:
    return f"agent/{task_dev_agent(task)}"


# ── In-memory agent state (auto-expands for custom agents) ────────────────────
AGENT_OUTPUT: dict = defaultdict(lambda: deque(maxlen=1000))
AGENT_STATUS: dict = defaultdict(lambda: {"status": "idle", "task": ""})
# Pre-populate built-ins so they appear on init
for _k in ("developer", "reviewer", "manager"):
    AGENT_OUTPUT[_k]
    AGENT_STATUS[_k]


# ── WebSocket manager ─────────────────────────────────────────────────────────
class ConnectionManager:
    def __init__(self):
        self.active: Set[WebSocket] = set()

    async def connect(self, ws: WebSocket):
        await ws.accept()
        self.active.add(ws)

    def disconnect(self, ws: WebSocket):
        self.active.discard(ws)

    async def broadcast(self, data: dict):
        dead = set()
        for ws in self.active:
            try:
                await ws.send_json(data)
            except Exception:
                dead.add(ws)
        self.active -= dead


manager = ConnectionManager()


@asynccontextmanager
async def lifespan(app: FastAPI):
    db.init_db()
    db.reset_stuck_tasks()
    # Pre-populate AGENT_OUTPUT/STATUS for all known agent types
    for at in db.list_agent_types():
        AGENT_OUTPUT[at["key"]]
        AGENT_STATUS[at["key"]]
    yield


app = FastAPI(title="Multi-Agent Task Board", lifespan=lifespan)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

frontend_dir = PROJECT_ROOT / "frontend"
if frontend_dir.exists():
    app.mount("/static", StaticFiles(directory=str(frontend_dir)), name="static")


# ── Pydantic models ───────────────────────────────────────────────────────────
class ProjectCreate(BaseModel):
    name: str
    path: str  # absolute filesystem path

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

class LogCreate(BaseModel):
    agent: str
    message: str

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
    if db.list_projects() and any(p["path"] == str(path) for p in db.list_projects()):
        raise HTTPException(400, "Path already used by another project")
    project = db.create_project(body.name, str(path))
    await manager.broadcast({"event": "project_created", "project": project})
    return project

@app.get("/projects/{project_id}")
async def get_project(project_id: str):
    p = db.get_project(project_id)
    if not p:
        raise HTTPException(404, "Project not found")
    return p

@app.post("/projects/sync")
async def sync_projects():
    """Delete projects whose directories no longer exist on disk."""
    all_projects = db.list_projects()
    deleted = []
    kept = []
    for p in all_projects:
        if Path(p["path"]).exists():
            kept.append(p)
        else:
            db.delete_project(p["id"])
            deleted.append(p)
            await manager.broadcast({"event": "project_deleted", "project_id": p["id"]})
    return {"deleted": deleted, "kept": kept}


@app.post("/projects/{project_id}/setup")
async def setup_project(project_id: str):
    """Initialize git repo in the project directory (without shared dev worktree)."""
    p = db.get_project(project_id)
    if not p:
        raise HTTPException(404, "Project not found")

    proj_path = Path(p["path"])
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
    task = db.claim_task(
        status=body.status,
        working_status=body.working_status,
        agent=body.agent,
        agent_key=body.agent_key,
        respect_assignment=body.respect_assignment,
        project_id=body.project_id,
    )
    if task:
        await manager.broadcast({"event": "task_updated", "task": task})
    return {"task": task}

@app.get("/tasks/{task_id}")
async def get_task(task_id: str):
    task = db.get_task(task_id)
    if not task:
        raise HTTPException(404, "Task not found")
    return task

@app.patch("/tasks/{task_id}")
async def update_task(task_id: str, body: TaskUpdate):
    fields = body.model_dump(exclude_unset=True)
    task = db.update_task(task_id, **fields)
    if not task:
        raise HTTPException(404, "Task not found")
    await manager.broadcast({"event": "task_updated", "task": task})
    # Auto-complete parent when all subtasks are done
    if "status" in fields and task.get("parent_task_id"):
        if db.check_parent_completion(task["parent_task_id"]):
            parent = db.get_task(task["parent_task_id"])
            if parent:
                await manager.broadcast({"event": "task_updated", "task": parent})
    return task


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
        await manager.broadcast({"event": "log_added", "log": log})
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
    await manager.broadcast({"event": "log_added", "log": log})
    return log

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

    if commit_hash and inspect_dir.exists():
        try:
            log_res = subprocess.run(
                ["git", "log", "--oneline", f"{commit_hash}^!"],
                cwd=str(inspect_dir), capture_output=True, text=True, timeout=5
            )
            msg = log_res.stdout.strip()
            is_scaffold = any(k in msg for k in ["chore: init", "initial commit", "Initial commit"])
            if not is_scaffold:
                res = subprocess.run(
                    ["git", "show", "--name-only", "--format=", commit_hash],
                    cwd=str(inspect_dir), capture_output=True, text=True, timeout=5
                )
                raw = [f.strip() for f in res.stdout.strip().splitlines() if f.strip()]
                files = [(rel_base + f) for f in raw]
                has_real_commit = bool(files)
        except Exception:
            pass

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
    return {
        name: {"lines": list(AGENT_OUTPUT[name]), "status": AGENT_STATUS.get(name, {})}
        for name in AGENT_OUTPUT
    }

@app.post("/agents/{agent_name}/output")
async def agent_output(agent_name: str, body: AgentOutput):
    buf = AGENT_OUTPUT.get(agent_name)
    if buf is not None:
        buf.append(body.line)
    await manager.broadcast({"event": "agent_output", "agent": agent_name, "line": body.line})
    return {"ok": True}

@app.post("/agents/{agent_name}/status")
async def agent_status(agent_name: str, body: AgentStatusUpdate):
    AGENT_STATUS[agent_name] = {"status": body.status, "task": body.task}
    await manager.broadcast({"event": "agent_status", "agent": agent_name,
                              "status": body.status, "task": body.task})
    return {"ok": True}


# ── WebSocket ─────────────────────────────────────────────────────────────────
@app.websocket("/ws")
async def websocket_endpoint(ws: WebSocket):
    await manager.connect(ws)
    try:
        tasks   = db.list_tasks()
        projects = db.list_projects()
        await ws.send_json({
            "event": "init",
            "tasks": tasks,
            "projects": projects,
            "agent_types": db.list_agent_types(),
            "agent_outputs": {
                name: {"lines": list(AGENT_OUTPUT[name]), "status": AGENT_STATUS.get(name, {})}
                for name in AGENT_OUTPUT
            },
        })
        while True:
            await ws.receive_text()
    except WebSocketDisconnect:
        manager.disconnect(ws)


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app:app", host="0.0.0.0", port=8080, reload=False)
