import asyncio
import contextlib
import json
import os
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
BUILTIN_KEYS = {"developer", "reviewer", "manager", "leader"}
RELOAD_INTERVAL_SECS = 3


@dataclass
class AgentHandle:
    key: str
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


def make_signature(agent_type: dict | None, key: str) -> str:
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
    return GenericAgent(config or {"key": key}, shutdown_token)


def start_agent(
    key: str,
    config: dict | None,
    global_shutdown: asyncio.Event,
) -> AgentHandle:
    stop_event = asyncio.Event()
    token = ShutdownToken(global_shutdown, stop_event)
    agent = create_agent_instance(key, config, token)

    print(f"[run_all] Starting agent '{key}' (cli={agent.cli_name}, polls={agent.poll_statuses})")
    task = asyncio.create_task(agent.run(), name=f"agent:{key}")

    def _on_done(t: asyncio.Task):
        with contextlib.suppress(asyncio.CancelledError):
            exc = t.exception()
            if exc is not None:
                print(f"[run_all] Agent '{key}' exited with error: {exc}")

    task.add_done_callback(_on_done)
    return AgentHandle(
        key=key,
        signature=make_signature(config, key),
        stop_event=stop_event,
        task=task,
    )


async def stop_agent(handle: AgentHandle, reason: str):
    print(f"[run_all] Stopping agent '{handle.key}' ({reason})...")
    handle.stop_event.set()
    try:
        await handle.task
    except asyncio.CancelledError:
        raise
    except Exception as e:
        print(f"[run_all] Agent '{handle.key}' stop error: {e}")


def desired_agent_map(agent_types: list[dict]) -> dict[str, dict | None]:
    desired = {t["key"]: t for t in agent_types}
    # Built-ins should always exist even if /agent-types temporarily misses them.
    for key in BUILTIN_KEYS:
        desired.setdefault(key, None)
    return desired


async def reconcile_agents(
    handles: dict[str, AgentHandle],
    desired: dict[str, dict | None],
    global_shutdown: asyncio.Event,
):
    # Remove stale handles for tasks that already ended unexpectedly.
    for key in list(handles.keys()):
        if handles[key].task.done():
            handles.pop(key, None)

    # Stop deleted agents first.
    for key in sorted(set(handles.keys()) - set(desired.keys())):
        handle = handles.pop(key)
        await stop_agent(handle, "deleted")

    # Start new / restart changed.
    for key in sorted(desired.keys()):
        cfg = desired.get(key)
        sig = make_signature(cfg, key)
        current = handles.get(key)
        if current and current.signature == sig and not current.task.done():
            continue
        if current:
            handles.pop(key, None)
            await stop_agent(current, "config changed")
        handles[key] = start_agent(key, cfg, global_shutdown)


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

    print(f"[run_all] Agent hot reload enabled (interval={RELOAD_INTERVAL_SECS}s).")

    handles: dict[str, AgentHandle] = {}
    bootstrapped = False

    while not shutdown.is_set():
        ok, agent_types = await load_agent_types()
        if ok:
            await reconcile_agents(handles, desired_agent_map(agent_types), shutdown)
            if not bootstrapped:
                print(f"[run_all] Started {len(handles)} agent(s).")
                bootstrapped = True
        elif not bootstrapped:
            # If API is temporarily unavailable at boot, still run built-ins with defaults.
            fallback = {k: None for k in BUILTIN_KEYS}
            await reconcile_agents(handles, fallback, shutdown)
            print(f"[run_all] API unavailable on boot; started built-ins with defaults.")
            bootstrapped = True
        else:
            print("[run_all] Skip reload this round due to API error.")

        try:
            await asyncio.wait_for(shutdown.wait(), timeout=RELOAD_INTERVAL_SECS)
        except asyncio.TimeoutError:
            pass

    await stop_all_agents(handles, "shutdown")
    print("[run_all] All agents stopped cleanly.")


if __name__ == "__main__":
    asyncio.run(main())
