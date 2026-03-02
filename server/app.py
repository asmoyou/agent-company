import asyncio
import os
from contextlib import asynccontextmanager
from collections import deque
from typing import Set

from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))
import db

PROJECT_ROOT = Path(__file__).parent.parent
WORKTREE_DEV = PROJECT_ROOT / ".worktrees" / "dev"

# ── In-memory agent state ────────────────────────────────────────────────────
# Last 200 lines of CLI output per agent
AGENT_OUTPUT: dict[str, deque] = {
    "developer": deque(maxlen=200),
    "reviewer":  deque(maxlen=200),
    "manager":   deque(maxlen=200),
}
# Current status per agent
AGENT_STATUS: dict[str, dict] = {
    "developer": {"status": "idle", "task": ""},
    "reviewer":  {"status": "idle", "task": ""},
    "manager":   {"status": "idle", "task": ""},
}


# ── WebSocket connection manager ─────────────────────────────────────────────

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


# ── App lifecycle ─────────────────────────────────────────────────────────────

@asynccontextmanager
async def lifespan(app: FastAPI):
    db.init_db()
    yield


app = FastAPI(title="Multi-Agent Task Board", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

frontend_dir = PROJECT_ROOT / "frontend"
if frontend_dir.exists():
    app.mount("/static", StaticFiles(directory=str(frontend_dir)), name="static")


# ── Pydantic models ───────────────────────────────────────────────────────────

class TaskCreate(BaseModel):
    title: str
    description: str = ""

class TaskUpdate(BaseModel):
    status: str | None = None
    assignee: str | None = None
    review_feedback: str | None = None
    commit_hash: str | None = None

class LogCreate(BaseModel):
    agent: str
    message: str

class AgentOutput(BaseModel):
    line: str

class AgentStatusUpdate(BaseModel):
    status: str   # "idle" | "busy"
    task: str = ""


# ── Root ──────────────────────────────────────────────────────────────────────

@app.get("/")
async def root():
    index = frontend_dir / "index.html"
    if index.exists():
        return FileResponse(str(index))
    return {"status": "ok"}


# ── Task endpoints ────────────────────────────────────────────────────────────

@app.get("/tasks")
async def list_tasks():
    return db.list_tasks()

@app.post("/tasks", status_code=201)
async def create_task(body: TaskCreate):
    task = db.create_task(body.title, body.description)
    await manager.broadcast({"event": "task_created", "task": task})
    return task

# NOTE: /tasks/status/{status} MUST come before /tasks/{task_id}
@app.get("/tasks/status/{status}")
async def tasks_by_status(status: str):
    return db.get_tasks_by_status(status)

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
    """List files changed in the dev worktree for this task."""
    task = db.get_task(task_id)
    if not task:
        raise HTTPException(404, "Task not found")

    files = []
    try:
        import subprocess
        # Files in last commit of dev worktree
        if WORKTREE_DEV.exists():
            result = subprocess.run(
                ["git", "show", "--name-only", "--format=", "HEAD"],
                cwd=str(WORKTREE_DEV), capture_output=True, text=True, timeout=5
            )
            files = [f for f in result.stdout.strip().splitlines() if f.strip()]
    except Exception:
        pass

    status = task.get("status", "")
    if status == "pending_acceptance":
        location = str(PROJECT_ROOT)
        branch = "main"
    else:
        location = str(WORKTREE_DEV)
        branch = "dev"

    return {"location": location, "branch": branch, "files": files}


# ── Agent terminal endpoints ──────────────────────────────────────────────────

@app.get("/agents/outputs")
async def get_agent_outputs():
    """Return current buffered output + status for all agents (for init)."""
    return {
        name: {
            "lines": list(AGENT_OUTPUT[name]),
            "status": AGENT_STATUS.get(name, {}),
        }
        for name in AGENT_OUTPUT
    }

@app.post("/agents/{agent_name}/output")
async def agent_output(agent_name: str, body: AgentOutput):
    buf = AGENT_OUTPUT.get(agent_name)
    if buf is not None:
        buf.append(body.line)
    await manager.broadcast({
        "event": "agent_output",
        "agent": agent_name,
        "line": body.line,
    })
    return {"ok": True}

@app.post("/agents/{agent_name}/status")
async def agent_status(agent_name: str, body: AgentStatusUpdate):
    AGENT_STATUS[agent_name] = {"status": body.status, "task": body.task}
    await manager.broadcast({
        "event": "agent_status",
        "agent": agent_name,
        "status": body.status,
        "task": body.task,
    })
    return {"ok": True}


# ── WebSocket ─────────────────────────────────────────────────────────────────

@app.websocket("/ws")
async def websocket_endpoint(ws: WebSocket):
    await manager.connect(ws)
    try:
        tasks = db.list_tasks()
        await ws.send_json({
            "event": "init",
            "tasks": tasks,
            "agent_outputs": {
                name: {
                    "lines": list(AGENT_OUTPUT[name]),
                    "status": AGENT_STATUS.get(name, {}),
                }
                for name in AGENT_OUTPUT
            },
        })
        while True:
            await ws.receive_text()
    except WebSocketDisconnect:
        manager.disconnect(ws)


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app:app", host="0.0.0.0", port=8000, reload=False)
