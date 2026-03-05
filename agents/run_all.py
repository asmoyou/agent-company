import asyncio
import contextlib
import json
import os
import re
import signal
import sys
from dataclasses import dataclass
from pathlib import Path

import httpx

sys.path.insert(0, str(Path(__file__).parent))

from developer import DeveloperAgent
from generic import GenericAgent
from leader import LeaderAgent
from manager import ManagerAgent
from reviewer import ReviewerAgent

SERVER_URL = os.getenv("SERVER_URL", "http://localhost:8080")
AGENT_API_TOKEN = str(os.getenv("AGENT_API_TOKEN", "opc-agent-internal")).strip()
BUILTIN_KEYS = {
    "developer",
    "reviewer",
    "manager",
    "leader",
    "product_manager",
    "finance_officer",
    "legal_counsel",
    "business_manager",
    "bid_writer",
    "risk_compliance_officer",
    "admin_specialist",
    "marketing_specialist",
    "art_designer",
    "hr_specialist",
    "operations_specialist",
    "customer_service_specialist",
    "procurement_specialist",
}
RELOAD_INTERVAL_SECS = 3
PROJECT_WORKERS_PER_AGENT = max(1, int(os.getenv("PROJECT_WORKERS_PER_AGENT", "1")))
PER_AGENT_TYPE_MAX_WORKERS = int(os.getenv("PER_AGENT_TYPE_MAX_WORKERS", "0"))
INCLUDE_IDLE_RUNTIME_PROJECTS = str(os.getenv("INCLUDE_IDLE_RUNTIME_PROJECTS", "0")).strip().lower() in {
    "1",
    "true",
    "yes",
    "on",
}
PROJECT_ID_ALLOWLIST = [
    x.strip()
    for x in str(os.getenv("AGENT_PROJECT_IDS", "")).split(",")
    if x.strip()
]


@dataclass
class AgentHandle:
    id: str
    agent_key: str
    project_id: str
    worker_index: int
    signature: str
    stop_event: asyncio.Event
    task: asyncio.Task


class ShutdownToken:
    """Composite shutdown token: set when any underlying event is set."""

    def __init__(self, *events: asyncio.Event):
        self.events = events

    def is_set(self) -> bool:
        return any(e.is_set() for e in self.events)

    async def wait(self):
        if self.is_set():
            return
        waiters = [asyncio.create_task(e.wait()) for e in self.events]
        try:
            done, pending = await asyncio.wait(waiters, return_when=asyncio.FIRST_COMPLETED)
            for t in pending:
                t.cancel()
            for t in done:
                with contextlib.suppress(asyncio.CancelledError):
                    await t
        finally:
            for t in waiters:
                if not t.done():
                    t.cancel()


async def load_agent_types() -> tuple[bool, list[dict]]:
    """Fetch agent type records from the server."""
    try:
        async with httpx.AsyncClient(base_url=SERVER_URL, trust_env=False, timeout=10) as client:
            r = await client.get("/agent-types")
            r.raise_for_status()
            return True, r.json()
    except Exception as e:
        print(f"[run_all] Could not load agent types: {e}")
        return False, []


async def load_runtime_projects() -> tuple[bool, list[dict]]:
    headers = {"X-Agent-Token": AGENT_API_TOKEN} if AGENT_API_TOKEN else {}
    include_idle = "1" if INCLUDE_IDLE_RUNTIME_PROJECTS else "0"
    try:
        async with httpx.AsyncClient(
            base_url=SERVER_URL,
            trust_env=False,
            timeout=10,
            headers=headers,
        ) as client:
            r = await client.get(f"/runtime/projects?include_idle={include_idle}")
            r.raise_for_status()
            return True, r.json()
    except Exception as e:
        print(f"[run_all] Could not load runtime projects: {e}")
        return False, []


def make_signature(agent_type: dict | None, key: str, project_id: str, worker_index: int) -> str:
    at = agent_type or {"key": key}
    payload = {
        "key": at.get("key", key),
        "name": at.get("name"),
        "description": at.get("description"),
        "prompt": at.get("prompt"),
        "poll_statuses": at.get("poll_statuses"),
        "next_status": at.get("next_status"),
        "working_status": at.get("working_status"),
        "cli": at.get("cli"),
        "is_builtin": at.get("is_builtin"),
        "project_id": project_id,
        "worker_index": worker_index,
    }
    return json.dumps(payload, ensure_ascii=False, sort_keys=True)


def create_agent_instance(key: str, config: dict | None, shutdown_token: ShutdownToken):
    if key == "developer":
        return DeveloperAgent(shutdown_token, config)
    if key == "reviewer":
        return ReviewerAgent(shutdown_token, config)
    if key == "manager":
        return ManagerAgent(shutdown_token, config)
    if key == "leader":
        return LeaderAgent(shutdown_token, config)
    cfg = config or {
        "key": key,
        "name": key,
        "description": "",
        "prompt": "",
        "poll_statuses": '["todo"]',
        "next_status": "in_review",
        "working_status": "in_progress",
        "cli": "codex",
        "is_builtin": 1,
    }
    return GenericAgent(cfg, shutdown_token)


def _normalize_runtime_piece(value: str, fallback: str = "worker") -> str:
    safe = re.sub(r"[^a-z0-9_-]+", "-", str(value or "").strip().lower()).strip("-_")
    return safe or fallback


def _worker_id(agent_key: str, project_id: str, worker_index: int) -> str:
    pid_piece = _normalize_runtime_piece(project_id, fallback="project")
    if len(pid_piece) > 20:
        pid_piece = f"{pid_piece[:10]}-{pid_piece[-6:]}"
    return f"{_normalize_runtime_piece(agent_key, fallback='agent')}__{pid_piece}__w{worker_index + 1}"


def start_agent(
    *,
    handle_id: str,
    agent_key: str,
    project_id: str,
    worker_index: int,
    config: dict | None,
    global_shutdown: asyncio.Event,
) -> AgentHandle:
    stop_event = asyncio.Event()
    token = ShutdownToken(global_shutdown, stop_event)
    agent = create_agent_instance(agent_key, config, token)
    agent.project_id_scope = project_id
    agent.worker_id = _worker_id(agent_key, project_id, worker_index)

    print(
        f"[run_all] Starting worker '{handle_id}' "
        f"(agent={agent_key}, project={project_id}, cli={agent.cli_name}, polls={agent.poll_statuses})"
    )
    task = asyncio.create_task(agent.run(), name=f"agent:{handle_id}")

    def _on_done(t: asyncio.Task):
        with contextlib.suppress(asyncio.CancelledError):
            exc = t.exception()
            if exc is not None:
                print(f"[run_all] Worker '{handle_id}' exited with error: {exc}")

    task.add_done_callback(_on_done)
    return AgentHandle(
        id=handle_id,
        agent_key=agent_key,
        project_id=project_id,
        worker_index=worker_index,
        signature=make_signature(config, agent_key, project_id, worker_index),
        stop_event=stop_event,
        task=task,
    )


async def stop_agent(handle: AgentHandle, reason: str):
    print(f"[run_all] Stopping worker '{handle.id}' ({reason})...")
    handle.stop_event.set()
    try:
        await handle.task
    except asyncio.CancelledError:
        raise
    except Exception as e:
        print(f"[run_all] Worker '{handle.id}' stop error: {e}")


def desired_agent_map(agent_types: list[dict]) -> dict[str, dict | None]:
    desired = {t["key"]: t for t in agent_types}
    # Built-ins should always exist even if /agent-types temporarily misses them.
    for key in BUILTIN_KEYS:
        desired.setdefault(key, None)
    return desired


def _apply_project_allowlist(projects: list[dict]) -> list[dict]:
    if not PROJECT_ID_ALLOWLIST:
        return list(projects)
    allow = set(PROJECT_ID_ALLOWLIST)
    filtered = [p for p in projects if str(p.get("id") or "").strip() in allow]
    missing = sorted(allow - {str(p.get("id") or "").strip() for p in filtered})
    for pid in missing:
        filtered.append(
            {
                "id": pid,
                "name": pid,
                "open_task_count": 0,
                "pending_task_count": 0,
                "oldest_open_task_updated_at": None,
            }
        )
    return filtered


def build_desired_workers(
    *,
    agent_map: dict[str, dict | None],
    projects: list[dict],
) -> dict[str, dict]:
    desired: dict[str, dict] = {}
    project_list = _apply_project_allowlist(projects)
    project_list = sorted(
        project_list,
        key=lambda p: (
            1 if not str(p.get("oldest_open_task_updated_at") or "").strip() else 0,
            str(p.get("oldest_open_task_updated_at") or ""),
            str(p.get("created_at") or ""),
            str(p.get("id") or ""),
        ),
    )

    for agent_key in sorted(agent_map.keys()):
        config = agent_map.get(agent_key)
        specs: list[dict] = []
        for p in project_list:
            pid = str(p.get("id") or "").strip()
            if not pid:
                continue
            for worker_index in range(PROJECT_WORKERS_PER_AGENT):
                handle_id = f"{agent_key}@{pid}#{worker_index + 1}"
                specs.append(
                    {
                        "id": handle_id,
                        "agent_key": agent_key,
                        "project_id": pid,
                        "worker_index": worker_index,
                        "config": config,
                    }
                )
        if PER_AGENT_TYPE_MAX_WORKERS > 0:
            specs = specs[:PER_AGENT_TYPE_MAX_WORKERS]
        for spec in specs:
            desired[spec["id"]] = spec
    return desired


async def reconcile_agents(
    handles: dict[str, AgentHandle],
    desired: dict[str, dict],
    global_shutdown: asyncio.Event,
):
    # Remove stale handles for tasks that already ended unexpectedly.
    for key in list(handles.keys()):
        if handles[key].task.done():
            handles.pop(key, None)

    # Stop deleted workers first.
    for key in sorted(set(handles.keys()) - set(desired.keys())):
        handle = handles.pop(key)
        await stop_agent(handle, "deleted")

    # Start new / restart changed workers.
    for key in sorted(desired.keys()):
        spec = desired[key]
        sig = make_signature(
            spec.get("config"),
            spec["agent_key"],
            spec["project_id"],
            int(spec.get("worker_index") or 0),
        )
        current = handles.get(key)
        if current and current.signature == sig and not current.task.done():
            continue
        if current:
            handles.pop(key, None)
            await stop_agent(current, "config changed")
        handles[key] = start_agent(
            handle_id=key,
            agent_key=spec["agent_key"],
            project_id=spec["project_id"],
            worker_index=int(spec.get("worker_index") or 0),
            config=spec.get("config"),
            global_shutdown=global_shutdown,
        )


async def stop_all_agents(handles: dict[str, AgentHandle], reason: str):
    for key in list(handles.keys()):
        handle = handles.pop(key)
        await stop_agent(handle, reason)


async def main():
    shutdown = asyncio.Event()
    loop = asyncio.get_event_loop()

    def handle_signal():
        if not shutdown.is_set():
            print("\n[run_all] 收到关闭信号，等待当前任务完成后退出...")
            shutdown.set()

    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, handle_signal)

    print(
        "[run_all] Agent hot reload enabled "
        f"(interval={RELOAD_INTERVAL_SECS}s, workers/project={PROJECT_WORKERS_PER_AGENT}, "
        f"per-agent-cap={PER_AGENT_TYPE_MAX_WORKERS})."
    )

    handles: dict[str, AgentHandle] = {}
    bootstrapped = False
    last_desired: dict[str, dict] = {}

    while not shutdown.is_set():
        ok_types, agent_types = await load_agent_types()
        ok_projects, projects = await load_runtime_projects()

        if ok_types and ok_projects:
            agent_map = desired_agent_map(agent_types)
            desired = build_desired_workers(agent_map=agent_map, projects=projects)
            last_desired = desired
            await reconcile_agents(handles, desired, shutdown)
            if not bootstrapped:
                print(f"[run_all] Started {len(handles)} worker(s).")
                bootstrapped = True
        elif not bootstrapped:
            # Boot fallback: try explicit project allowlist only.
            if PROJECT_ID_ALLOWLIST:
                agent_map = {k: None for k in BUILTIN_KEYS}
                fallback_projects = [{"id": pid, "name": pid} for pid in PROJECT_ID_ALLOWLIST]
                desired = build_desired_workers(agent_map=agent_map, projects=fallback_projects)
                last_desired = desired
                await reconcile_agents(handles, desired, shutdown)
                print(
                    "[run_all] API unavailable on boot; "
                    f"started built-ins for AGENT_PROJECT_IDS ({len(handles)} worker(s))."
                )
            else:
                print("[run_all] API unavailable on boot and no AGENT_PROJECT_IDS configured.")
            bootstrapped = True
        else:
            # Keep last-known topology so dead workers can still be restarted.
            await reconcile_agents(handles, last_desired, shutdown)
            print("[run_all] API error; kept last-known worker topology and reconciled liveness.")

        try:
            await asyncio.wait_for(shutdown.wait(), timeout=RELOAD_INTERVAL_SECS)
        except asyncio.TimeoutError:
            pass

    await stop_all_agents(handles, "shutdown")
    print("[run_all] All workers stopped cleanly.")


if __name__ == "__main__":
    asyncio.run(main())
