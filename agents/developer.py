import asyncio
import os
from pathlib import Path

from base import BaseAgent, PROJECT_ROOT

WORKTREE_DEV = PROJECT_ROOT / ".worktrees" / "dev"


class DeveloperAgent(BaseAgent):
    name = "developer"
    poll_statuses = ["todo", "needs_changes"]
    cli_name = os.getenv("DEVELOPER_CLI", "claude")

    @property
    def worktree(self) -> Path:
        return WORKTREE_DEV

    async def process_task(self, task: dict):
        task_id = task["id"]

        # Claim the task
        await self.update_task(task_id, status="in_progress", assignee=self.name)
        await self.add_log(task_id, f"Developer picked up task (was: {task['status']})")

        # Build prompt
        prompt = f"""You are implementing a software task. Work in the current directory.

Task: {task['title']}

Description:
{task['description']}
"""

        if task["status"] == "needs_changes" and task.get("review_feedback"):
            prompt += f"""
Previous reviewer feedback (address ALL points):
{task['review_feedback']}

Please fix the issues described above.
"""
            await self.add_log(task_id, "Re-implementing based on reviewer feedback")
        else:
            await self.add_log(task_id, "Starting implementation")

        prompt += "\nImplement this completely. Write all necessary files."

        # Run the CLI tool in the dev worktree
        returncode, output = await self.run_cli(prompt, cwd=self.worktree)

        # Log a summary of the output (first 500 chars)
        summary = output[:500].strip()
        if summary:
            await self.add_log(task_id, f"CLI output: {summary}{'...' if len(output) > 500 else ''}")

        if returncode != 0 and returncode != -1:
            # Non-zero but not timeout — CLI may still have done useful work
            await self.add_log(task_id, f"CLI exited with code {returncode}, checking for changes...")

        # Commit whatever was produced
        try:
            await self.git("add", "-A", cwd=self.worktree)
            # Check if there's anything to commit
            diff = await self.git("diff", "--cached", "--stat", cwd=self.worktree)
            if not diff.strip():
                await self.add_log(task_id, "No file changes detected — submitting for review anyway")
                await self.update_task(task_id, status="in_review")
                return

            commit_msg = f"feat: {task['title'][:72]}\n\nTask ID: {task_id}"
            await self.git("commit", "-m", commit_msg, cwd=self.worktree)
            commit_hash = await self.git("rev-parse", "--short", "HEAD", cwd=self.worktree)
            await self.add_log(task_id, f"Committed: {commit_hash}")
            await self.update_task(task_id, status="in_review", commit_hash=commit_hash)

        except Exception as e:
            await self.add_log(task_id, f"Git error: {e}")
            await self.update_task(task_id, status="todo")


if __name__ == "__main__":
    asyncio.run(DeveloperAgent().run())
