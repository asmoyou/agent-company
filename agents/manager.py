import asyncio
import os

from base import BaseAgent, get_task_dev_agent


class ManagerAgent(BaseAgent):
    name = "manager"
    poll_statuses = ["approved"]
    cli_name = os.getenv("MANAGER_CLI", "claude")
    working_status = "merging"

    def respect_assignment_for(self, status: str) -> bool:
        return False

    async def process_task(self, task: dict):
        task_id = task["id"]
        dev_agent = get_task_dev_agent(task)
        commit_hash = (task.get("commit_hash") or "").strip()
        proj_root, _, dev_branch = await self.ensure_agent_workspace(task, agent_key=dev_agent)

        if not commit_hash:
            feedback = "[系统错误] 缺少 commit_hash，无法精确合并。退回开发重提。"
            await self.add_log(task_id, feedback)
            await self.update_task(
                task_id,
                status="needs_changes",
                assignee=None,
                assigned_agent=dev_agent,
                dev_agent=dev_agent,
                review_feedback=feedback,
            )
            return

        await self.add_log(
            task_id,
            f"Manager 开始合并 commit {commit_hash}（源分支: {dev_branch}，项目: {proj_root.name}）",
        )
        self._post_output_bg(f"git merge {commit_hash} ({dev_branch}) → main in {proj_root.name}")

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

            # Ensure target commit belongs to the expected dev branch.
            try:
                await self.git("merge-base", "--is-ancestor", commit_hash, dev_branch, cwd=proj_root)
            except Exception:
                raise RuntimeError(f"commit {commit_hash} 不在分支 {dev_branch} 上")

            # Merge exactly the reviewed commit (not branch HEAD).
            merge_msg = f"merge: {task['title'][:72]}\n\nTask ID: {task_id}"
            try:
                result = await self.git(
                    "-c", "user.email=agent@opc-demo.local",
                    "-c", "user.name=OPC Agent",
                    "merge", commit_hash, "--no-ff", "-m", merge_msg,
                    cwd=proj_root,
                )
                self._post_output_bg(f"merge: {result[:80]}")
            except RuntimeError as e:
                if "already up to date" in str(e).lower():
                    await self.add_log(task_id, "Already up to date")
                    await self.update_task(task_id, status="pending_acceptance", assignee=None)
                    return
                raise

            commit_hash = await self.git("rev-parse", "--short", "HEAD", cwd=proj_root)
            await self.add_log(task_id, f"✅ 合并成功: {commit_hash}，文件已在 {proj_root}")
            self._post_output_bg(f"✓ 合并完成 {commit_hash}")
            await self.update_task(
                task_id, status="pending_acceptance", assignee=None, commit_hash=commit_hash
            )

        except Exception as e:
            err = str(e)
            if "already up to date" in err.lower():
                await self.add_log(task_id, "Already up to date，标记为待验收")
                await self.update_task(task_id, status="pending_acceptance", assignee=None)
                return
            await self.add_log(task_id, f"合并失败: {err[:300]}")
            self._post_output_bg(f"✗ 合并失败: {err[:120]}")
            try:
                await self.git("merge", "--abort", cwd=proj_root)
            except Exception:
                pass
            await self.update_task(task_id, status="approved", assignee=None)


if __name__ == "__main__":
    asyncio.run(ManagerAgent().run())
