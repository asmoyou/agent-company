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
            await self.add_alert(
                summary="合并前置条件缺失：commit_hash",
                task_id=task_id,
                message=feedback,
                kind="error",
                code="manager_missing_commit_hash",
                stage="merge_to_dev",
            )
            await self.add_handoff(
                task_id,
                stage="merge_to_dev",
                to_agent=dev_agent,
                status_from="approved",
                status_to="needs_changes",
                title="合并退回开发",
                summary=feedback,
                conclusion="缺少目标 commit，退回开发重提",
                payload={"reason": "missing_commit_hash", "has_commit": False},
            )
            await self.update_task(
                task_id,
                status="needs_changes",
                assignee=None,
                assigned_agent=dev_agent,
                dev_agent=dev_agent,
                review_feedback=feedback,
                feedback_source=self.name,
                feedback_stage="merge_to_dev",
                feedback_actor=self.name,
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
                    await self.add_handoff(
                        task_id,
                        stage="merge_to_acceptance",
                        to_agent="user",
                        status_from="approved",
                        status_to="pending_acceptance",
                        title="无需合并，进入验收",
                        summary="目标提交已在主分支，直接进入待验收",
                        commit_hash=commit_hash,
                        conclusion="目标提交已存在于 main，直接进入验收",
                        payload={"commit_hash": commit_hash, "already_up_to_date": True},
                    )
                    await self.update_task(task_id, status="pending_acceptance", assignee=None)
                    return
                raise

            commit_hash = await self.git("rev-parse", "--short", "HEAD", cwd=proj_root)
            await self.add_log(task_id, f"✅ 合并成功: {commit_hash}，文件已在 {proj_root}")
            self._post_output_bg(f"✓ 合并完成 {commit_hash}")
            await self.add_handoff(
                task_id,
                stage="merge_to_acceptance",
                to_agent="user",
                status_from="approved",
                status_to="pending_acceptance",
                title="合并完成，交接验收",
                summary=f"已将审查通过提交合并到 main：{commit_hash}",
                commit_hash=commit_hash,
                conclusion="合并完成，进入待验收",
                payload={"commit_hash": commit_hash, "source_branch": dev_branch, "reviewed_commit": target_commit},
                artifact_path=str(proj_root),
            )
            if await self.stop_if_task_cancelled(task_id, "合并后状态更新前"):
                return
            await self.update_task(
                task_id, status="pending_acceptance", assignee=None, commit_hash=commit_hash
            )

        except Exception as e:
            err = str(e)
            if "already up to date" in err.lower():
                await self.add_log(task_id, "Already up to date，标记为待验收")
                await self.add_handoff(
                    task_id,
                    stage="merge_to_acceptance",
                    to_agent="user",
                    status_from="approved",
                    status_to="pending_acceptance",
                    title="无需合并，进入验收",
                    summary="主分支已是最新，直接进入待验收",
                    commit_hash=target_commit,
                    conclusion="无需合并，进入待验收",
                    payload={"already_up_to_date": True},
                )
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
                await self.add_alert(
                    summary="合并冲突，退回开发处理",
                    task_id=task_id,
                    message=feedback,
                    kind="warning",
                    code="manager_merge_conflict",
                    stage="merge_to_dev",
                    metadata={"commit_hash": target_commit, "dev_branch": dev_branch},
                )
                await self.add_handoff(
                    task_id,
                    stage="merge_to_dev",
                    to_agent=dev_agent,
                    status_from="approved",
                    status_to="needs_changes",
                    title="合并冲突，退回开发",
                    summary=feedback,
                    commit_hash=target_commit,
                    conclusion="合并冲突，需开发解决后重提",
                    payload={"reason": "merge_conflict", "commit_hash": target_commit},
                )
                await self.update_task(
                    task_id,
                    status="needs_changes",
                    assignee=None,
                    assigned_agent=dev_agent,
                    dev_agent=dev_agent,
                    review_feedback=feedback,
                    feedback_source=self.name,
                    feedback_stage="merge_to_dev",
                    feedback_actor=self.name,
                )
                return
            await self.add_alert(
                summary="合并失败（系统错误）",
                task_id=task_id,
                message=err[:1000],
                kind="error",
                code="manager_merge_failed",
                stage="merge_failed",
                metadata={"commit_hash": target_commit},
            )
            await self.add_handoff(
                task_id,
                stage="merge_failed",
                to_agent=self.name,
                status_from="approved",
                status_to="approved",
                title="合并失败（系统）",
                summary=f"合并失败但非冲突：{err[:200]}",
                commit_hash=target_commit,
                conclusion="合并系统错误，等待重试",
                payload={"error": err[:500], "commit_hash": target_commit},
            )
            await self.update_task(task_id, status="approved", assignee=None)


if __name__ == "__main__":
    asyncio.run(ManagerAgent().run())
