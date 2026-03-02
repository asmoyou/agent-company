import asyncio
import os
import signal
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from developer import DeveloperAgent
from reviewer  import ReviewerAgent
from manager   import ManagerAgent


async def main():
    shutdown = asyncio.Event()
    loop = asyncio.get_event_loop()

    def handle_signal():
        if not shutdown.is_set():
            print("\n[run_all] 收到关闭信号，等待当前任务完成后退出...")
            shutdown.set()

    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, handle_signal)

    agents = [
        DeveloperAgent(shutdown),
        ReviewerAgent(shutdown),
        ManagerAgent(shutdown),
    ]

    print("All agents started. Press Ctrl+C to stop gracefully.")
    try:
        await asyncio.gather(*[a.run() for a in agents])
    except asyncio.CancelledError:
        pass

    print("[run_all] All agents stopped cleanly.")


if __name__ == "__main__":
    asyncio.run(main())
