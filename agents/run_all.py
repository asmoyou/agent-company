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


async def load_custom_agents(shutdown: asyncio.Event) -> list:
    """Fetch non-builtin agent types from the server and instantiate them."""
    try:
        async with httpx.AsyncClient(base_url=SERVER_URL, trust_env=False, timeout=10) as client:
            r = await client.get("/agent-types")
            r.raise_for_status()
            types = r.json()
        custom = [t for t in types if t["key"] not in BUILTIN_KEYS]
        agents = [GenericAgent(t, shutdown) for t in custom]
        if agents:
            print(f"[run_all] Loaded {len(agents)} custom agent(s): {[a.name for a in agents]}")
        return agents
    except Exception as e:
        print(f"[run_all] Could not load custom agents: {e}")
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

    builtin_agents = [
        DeveloperAgent(shutdown),
        ReviewerAgent(shutdown),
        ManagerAgent(shutdown),
        LeaderAgent(shutdown),
    ]

    custom_agents = await load_custom_agents(shutdown)
    all_agents = builtin_agents + custom_agents

    print(f"[run_all] Starting {len(all_agents)} agent(s). Press Ctrl+C to stop gracefully.")
    try:
        await asyncio.gather(*[a.run() for a in all_agents])
    except asyncio.CancelledError:
        pass

    print("[run_all] All agents stopped cleanly.")


if __name__ == "__main__":
    asyncio.run(main())

