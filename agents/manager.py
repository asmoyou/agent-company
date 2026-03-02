import asyncio
import os
from pathlib import Path

from base import BaseAgent, PROJECT_ROOT

WORKTREE_DEV = PROJECT_ROOT / ".worktrees" / "dev"


class ManagerAgent(BaseAgent):
    name = "manager"
    poll_statuses = ["approved"]
    cli_name = os.getenv("MANAGER_CLI", "claude")

    @property
    def worktree(self) -> Path:
        return PROJECT_ROOT

    async def process_task(self, task: dict):
        task_id = task["id"]

        await self.update_task(task_id, status="merging", assignee=self.name)
        await self.add_log(task_id, "Manager 开始合并 dev → main")
        self._post_output_bg("git merge dev → main ...")

        try:
            # Ensure we're on main in the project root worktree
            try:
                current = await self.git("branch", "--show-current")
            except Exception:
                current = ""

            if current != "main":
                try:
                    await self.git("checkout", "main")
                except Exception:
                    await self.git("checkout", "-b", "main")
                self._post_output_bg("切换到 main 分支")

            # Merge the dev BRANCH (not the filesystem path!)
            # Since we used `git worktree add .worktrees/dev dev`, commits in
            # the dev worktree are directly on the shared `dev` branch ref.
            try:
                result = await self.git(
                    "merge", "dev", "--no-ff",
                    "-m", f"merge: {task['title'][:72]}\n\nTask ID: {task_id}",
                )
                self._post_output_bg(f"merge 结果: {result[:100]}")
            except RuntimeError as e:
                if "already up to date" in str(e).lower():
                    self._post_output_bg("Already up to date — 无新提交，跳过")
                    await self.add_log(task_id, "Already up to date")
                    await self.update_task(task_id, status="pending_acceptance")
                    return
                raise

            commit_hash = await self.git("rev-parse", "--short", "HEAD")
            await self.add_log(task_id, f"合并成功: {commit_hash}，文件已在项目根目录")
            self._post_output_bg(f"✓ 合并完成 {commit_hash}")
            await self.update_task(task_id, status="pending_acceptance", commit_hash=commit_hash)

        except Exception as e:
            err = str(e)
            if "already up to date" in err.lower():
                await self.add_log(task_id, "Already up to date，标记为待验收")
                await self.update_task(task_id, status="pending_acceptance")
                return
            await self.add_log(task_id, f"合并失败: {err}")
            self._post_output_bg(f"✗ 合并失败: {err[:200]}")
            try:
                await self.git("merge", "--abort")
            except Exception:
                pass
            await self.update_task(task_id, status="approved")


if __name__ == "__main__":
    asyncio.run(ManagerAgent().run())
