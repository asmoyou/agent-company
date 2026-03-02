import asyncio

from base import BaseAgent, get_task_dev_agent, parse_status_list


class ManagerAgent(BaseAgent):
    name = "manager"
    poll_statuses = ["approved"]
    cli_name = "claude"
    working_status = "merging"

    def __init__(self, shutdown_event=None, config: dict | None = None):
        super().__init__(shutdown_event)
        cfg = config or {}
        self.poll_statuses = parse_status_list(cfg.get("poll_statuses"), ["approved"])
        self.cli_name = str(cfg.get("cli") or "claude")
        self.working_status = str(cfg.get("working_status") or "merging")

    def respect_assignment_for(self, status: str) -> bool:
        return False

    async def process_task(self, task: dict):
        task_id = task["id"]
        if await self.stop_if_task_cancelled(task_id, "开始合并前"):
            return
        dev_agent = get_task_dev_agent(task)
        commit_hash = (task.get("commit_hash") or "").strip()
        target_commit = commit_hash
        proj_root, _, dev_branch = await self.ensure_agent_workspace(
            task, agent_key=dev_agent, sync_with_main=False
        )

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
            if await self.stop_if_task_cancelled(task_id, "执行 git merge 前"):
                return
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
            if await self.stop_if_task_cancelled(task_id, "合并后状态更新前"):
                return
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
            low = err.lower()
            is_conflict = ("conflict" in low) or ("automatic merge failed" in low) or ("冲突" in err)
            if is_conflict:
                feedback = (
                    f"[合并冲突] main 与 {dev_branch} 在合并 commit {target_commit} 时发生冲突，"
                    f"请 {dev_agent} 在其分支解决冲突后重新提交。"
                )
                await self.add_log(task_id, f"↩ 发生合并冲突，退回 {dev_agent} 处理")
                await self.update_task(
                    task_id,
                    status="needs_changes",
                    assignee=None,
                    assigned_agent=dev_agent,
                    dev_agent=dev_agent,
                    review_feedback=feedback,
                )
                return
            await self.update_task(task_id, status="approved", assignee=None)


if __name__ == "__main__":
    asyncio.run(ManagerAgent().run())
