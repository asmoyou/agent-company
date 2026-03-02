import asyncio
import sys
from pathlib import Path

# Ensure agents directory is on path
sys.path.insert(0, str(Path(__file__).parent))

from developer import DeveloperAgent
from reviewer import ReviewerAgent
from manager import ManagerAgent


async def main():
    print("Starting all agents...")
    developer = DeveloperAgent()
    reviewer = ReviewerAgent()
    manager = ManagerAgent()

    await asyncio.gather(
        developer.run(),
        reviewer.run(),
        manager.run(),
    )


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nAgents stopped.")
