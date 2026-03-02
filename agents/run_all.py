import asyncio
import os
import signal
import sys
from pathlib import Path

import httpx

sys.path.insert(0, str(Path(__file__).parent))

from developer import DeveloperAgent
from reviewer  import ReviewerAgent
from manager   import ManagerAgent
from leader    import LeaderAgent
from generic   import GenericAgent

SERVER_URL   = os.getenv("SERVER_URL", "http://localhost:8080")
BUILTIN_KEYS = {"developer", "reviewer", "manager", "leader"}


async def load_agent_types() -> list[dict]:
    """Fetch agent type records from the server."""
    try:
        async with httpx.AsyncClient(base_url=SERVER_URL, trust_env=False, timeout=10) as client:
            r = await client.get("/agent-types")
            r.raise_for_status()
            return r.json()
    except Exception as e:
        print(f"[run_all] Could not load agent types: {e}")
        return []


async def main():
    shutdown = asyncio.Event()
    loop = asyncio.get_event_loop()

    def handle_signal():
        if not shutdown.is_set():
            print("\n[run_all] 收到关闭信号，等待当前任务完成后退出...")
            shutdown.set()

    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, handle_signal)

    agent_types = await load_agent_types()
    cfg = {t["key"]: t for t in agent_types}

    builtin_agents = [
        DeveloperAgent(shutdown, cfg.get("developer")),
        ReviewerAgent(shutdown, cfg.get("reviewer")),
        ManagerAgent(shutdown, cfg.get("manager")),
        LeaderAgent(shutdown, cfg.get("leader")),
    ]

    custom = [t for t in agent_types if t["key"] not in BUILTIN_KEYS]
    custom_agents = [GenericAgent(t, shutdown) for t in custom]
    if custom_agents:
        print(f"[run_all] Loaded {len(custom_agents)} custom agent(s): {[a.name for a in custom_agents]}")

    all_agents = builtin_agents + custom_agents

    print(f"[run_all] Starting {len(all_agents)} agent(s). Press Ctrl+C to stop gracefully.")
    try:
        await asyncio.gather(*[a.run() for a in all_agents])
    except asyncio.CancelledError:
        pass

    print("[run_all] All agents stopped cleanly.")


if __name__ == "__main__":
    asyncio.run(main())
