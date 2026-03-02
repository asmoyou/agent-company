import asyncio
import os

from base import BaseAgent, get_project_dirs


class ManagerAgent(BaseAgent):
    name = "manager"
    poll_statuses = ["approved"]
    cli_name = os.getenv("MANAGER_CLI", "claude")

    async def process_task(self, task: dict):
        task_id = task["id"]
        proj_root, worktree_dev = get_project_dirs(task)

        await self.update_task(task_id, status="merging", assignee=self.name)
        await self.add_log(task_id, f"Manager 开始合并 dev→main（项目: {proj_root.name}）")
        self._post_output_bg(f"git merge dev → main in {proj_root.name}")

        try:
            # Ensure on main in project root
            try:
                current = await self.git("branch", "--show-current", cwd=proj_root)
            except Exception:
                current = ""

            if current != "main":
                try:
                    await self.git("checkout", "main", cwd=proj_root)
                except Exception:
                    await self.git(
                        "-c", "user.email=agent@opc-demo.local",
                        "-c", "user.name=OPC Agent",
                        "checkout", "-b", "main", cwd=proj_root,
                    )

            # Merge dev branch (shared ref via git worktree)
            merge_msg = f"merge: {task['title'][:72]}\n\nTask ID: {task_id}"
            try:
                result = await self.git(
                    "-c", "user.email=agent@opc-demo.local",
                    "-c", "user.name=OPC Agent",
                    "merge", "dev", "--no-ff", "-m", merge_msg,
                    cwd=proj_root,
                )
                self._post_output_bg(f"merge: {result[:80]}")
            except RuntimeError as e:
                if "already up to date" in str(e).lower():
                    await self.add_log(task_id, "Already up to date")
                    await self.update_task(task_id, status="pending_acceptance")
                    return
                raise

            commit_hash = await self.git("rev-parse", "--short", "HEAD", cwd=proj_root)
            await self.add_log(task_id, f"✅ 合并成功: {commit_hash}，文件已在 {proj_root}")
            self._post_output_bg(f"✓ 合并完成 {commit_hash}")
            await self.update_task(task_id, status="pending_acceptance", commit_hash=commit_hash)

        except Exception as e:
            err = str(e)
            if "already up to date" in err.lower():
                await self.add_log(task_id, "Already up to date，标记为待验收")
                await self.update_task(task_id, status="pending_acceptance")
                return
            await self.add_log(task_id, f"合并失败: {err[:300]}")
            self._post_output_bg(f"✗ 合并失败: {err[:120]}")
            try:
                await self.git("merge", "--abort", cwd=proj_root)
            except Exception:
                pass
            await self.update_task(task_id, status="approved")


if __name__ == "__main__":
    asyncio.run(ManagerAgent().run())
