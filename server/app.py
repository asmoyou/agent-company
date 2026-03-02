import asyncio
import subprocess
from collections import deque
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Set

from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

import sys
sys.path.insert(0, str(Path(__file__).parent))
import db

PROJECT_ROOT = Path(__file__).parent.parent

# ── In-memory agent state ─────────────────────────────────────────────────────
AGENT_OUTPUT: dict[str, deque] = {
    "developer": deque(maxlen=200),
    "reviewer":  deque(maxlen=200),
    "manager":   deque(maxlen=200),
}
AGENT_STATUS: dict[str, dict] = {
    "developer": {"status": "idle", "task": ""},
    "reviewer":  {"status": "idle", "task": ""},
    "manager":   {"status": "idle", "task": ""},
}


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


PROMPTS_DIR = PROJECT_ROOT / "prompts"
PROMPTS_DIR.mkdir(exist_ok=True)

manager = ConnectionManager()


@asynccontextmanager
async def lifespan(app: FastAPI):
    db.init_db()
    db.reset_stuck_tasks()   # Reset tasks stuck in transient states from last crash
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

class TaskUpdate(BaseModel):
    status: str | None = None
    assignee: str | None = None
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

@app.post("/projects/{project_id}/setup")
async def setup_project(project_id: str):
    """Initialize git repo + dev worktree in the project directory."""
    p = db.get_project(project_id)
    if not p:
        raise HTTPException(404, "Project not found")

    proj_path = Path(p["path"])
    proj_path.mkdir(parents=True, exist_ok=True)
    worktree_dev = proj_path / ".worktrees" / "dev"

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

    # Create dev branch + worktree
    if not worktree_dev.exists():
        rc, out, err = run("git", "branch", "dev")
        log.append(f"branch dev: {out or err or 'ok'}")
        worktree_dev.parent.mkdir(parents=True, exist_ok=True)
        rc, out, err = run("git", "worktree", "add", str(worktree_dev), "dev")
        log.append(f"worktree add: {out or err or 'ok'}")
        # Configure git in dev worktree
        run("git", "config", "user.email", "agent@opc-demo.local", cwd=worktree_dev)
        run("git", "config", "user.name", "OPC Agent", cwd=worktree_dev)
    else:
        log.append("dev worktree already exists")

    return {"ok": True, "log": log, "path": str(proj_path)}


# ── Tasks ─────────────────────────────────────────────────────────────────────
@app.get("/tasks")
async def list_tasks(project_id: str | None = None):
    return db.list_tasks(project_id)

@app.post("/tasks", status_code=201)
async def create_task(body: TaskCreate):
    task = db.create_task(body.title, body.description, body.project_id)
    await manager.broadcast({"event": "task_created", "task": task})
    return task

# NOTE: /tasks/status/{status} must come before /tasks/{task_id}
@app.get("/tasks/status/{status}")
async def tasks_by_status(status: str, project_id: str | None = None):
    return db.get_tasks_by_status(status, project_id)

@app.get("/tasks/{task_id}")
async def get_task(task_id: str):
    task = db.get_task(task_id)
    if not task:
        raise HTTPException(404, "Task not found")
    return task

@app.patch("/tasks/{task_id}")
async def update_task(task_id: str, body: TaskUpdate):
    fields = {k: v for k, v in body.model_dump().items() if v is not None}
    task = db.update_task(task_id, **fields)
    if not task:
        raise HTTPException(404, "Task not found")
    await manager.broadcast({"event": "task_updated", "task": task})
    return task

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
    worktree_dev = proj_root / ".worktrees" / "dev"
    status       = task.get("status", "")
    commit_hash  = task.get("commit_hash", "")

    if status == "pending_acceptance" or status == "completed":
        inspect_dir = proj_root
        branch      = "main"
        rel_base    = ""
    else:
        inspect_dir = worktree_dev
        branch      = "dev"
        rel_base    = ".worktrees/dev/"

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


# ── Prompts endpoints ─────────────────────────────────────────────────────────
ALLOWED_AGENTS = {"developer", "reviewer"}

class PromptUpdate(BaseModel):
    content: str

@app.get("/prompts/{agent_name}")
async def get_prompt(agent_name: str):
    if agent_name not in ALLOWED_AGENTS:
        raise HTTPException(400, "Invalid agent name")
    path = PROMPTS_DIR / f"{agent_name}.md"
    return {
        "agent": agent_name,
        "content": path.read_text(encoding="utf-8") if path.exists() else "",
        "source": "global",
    }

@app.put("/prompts/{agent_name}")
async def update_prompt(agent_name: str, body: PromptUpdate):
    if agent_name not in ALLOWED_AGENTS:
        raise HTTPException(400, "Invalid agent name")
    path = PROMPTS_DIR / f"{agent_name}.md"
    path.write_text(body.content, encoding="utf-8")
    return {"ok": True}

@app.get("/projects/{project_id}/prompts/{agent_name}")
async def get_project_prompt(project_id: str, agent_name: str):
    if agent_name not in ALLOWED_AGENTS:
        raise HTTPException(400, "Invalid agent name")
    p = db.get_project(project_id)
    if not p:
        raise HTTPException(404, "Project not found")
    override = Path(p["path"]) / ".opc" / f"{agent_name}.md"
    if override.exists():
        return {"agent": agent_name, "content": override.read_text(encoding="utf-8"), "source": "project"}
    # Fall back to global
    glob = PROMPTS_DIR / f"{agent_name}.md"
    return {
        "agent": agent_name,
        "content": glob.read_text(encoding="utf-8") if glob.exists() else "",
        "source": "global",
    }

@app.put("/projects/{project_id}/prompts/{agent_name}")
async def update_project_prompt(project_id: str, agent_name: str, body: PromptUpdate):
    if agent_name not in ALLOWED_AGENTS:
        raise HTTPException(400, "Invalid agent name")
    p = db.get_project(project_id)
    if not p:
        raise HTTPException(404, "Project not found")
    opc_dir = Path(p["path"]) / ".opc"
    opc_dir.mkdir(exist_ok=True)
    (opc_dir / f"{agent_name}.md").write_text(body.content, encoding="utf-8")
    return {"ok": True}

@app.delete("/projects/{project_id}/prompts/{agent_name}")
async def delete_project_prompt(project_id: str, agent_name: str):
    """Remove project-level override, falling back to global prompt."""
    if agent_name not in ALLOWED_AGENTS:
        raise HTTPException(400, "Invalid agent name")
    p = db.get_project(project_id)
    if not p:
        raise HTTPException(404, "Project not found")
    override = Path(p["path"]) / ".opc" / f"{agent_name}.md"
    if override.exists():
        override.unlink()
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
