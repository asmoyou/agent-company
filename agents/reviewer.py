import asyncio

from base import BaseAgent, get_task_dev_agent, load_prompt, parse_status_list


class ReviewerAgent(BaseAgent):
    name = "reviewer"
    poll_statuses = ["in_review"]
    cli_name = "claude"
    working_status = "reviewing"

    def __init__(self, shutdown_event=None, config: dict | None = None):
        super().__init__(shutdown_event)
        cfg = config or {}
        self.poll_statuses = parse_status_list(cfg.get("poll_statuses"), ["in_review"])
        self.cli_name = str(cfg.get("cli") or "claude")
        self.working_status = str(cfg.get("working_status") or "reviewing")

    def respect_assignment_for(self, status: str) -> bool:
        return False

    async def get_diff_for_commit(self, worktree_dev, commit_hash: str) -> str:
        return await self.git("show", commit_hash, cwd=worktree_dev)

    async def process_task(self, task: dict):
        task_id = task["id"]
        dev_agent = get_task_dev_agent(task)
        commit_hash = (task.get("commit_hash") or "").strip()
        proj_root, worktree_dev, branch = await self.ensure_agent_workspace(task, agent_key=dev_agent)

        if not commit_hash:
            feedback = "[系统错误] 缺少 commit_hash，无法进行精确审查。请 developer 重新提交。"
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
            f"Reviewer 开始审查（dev_agent={dev_agent}, 分支={branch}, commit={commit_hash}）",
        )

        try:
            diff = await self.get_diff_for_commit(worktree_dev, commit_hash)
        except Exception as e:
            feedback = f"[系统错误] 无法读取目标 commit={commit_hash}：{e}"
            await self.add_log(task_id, feedback[:500])
            await self.update_task(
                task_id,
                status="needs_changes",
                assignee=None,
                assigned_agent=dev_agent,
                dev_agent=dev_agent,
                review_feedback=feedback[:500],
            )
            return

        await self.add_log(task_id, f"获取到 commit diff ({len(diff)} 字符): {commit_hash}")

        # ── Build prompt from template ────────────────────────────────────────
        template = load_prompt("reviewer", project_path=proj_root)
        if template:
            prompt = template.format(
                task_title=task["title"],
                task_description=task["description"] or "(无额外描述)",
                commit_hash=commit_hash,
                dev_agent=dev_agent,
                diff=diff[:8000],
            )
        else:
            prompt = (
                f"审查任务「{task['title']}」的代码变更（commit={commit_hash}）：\n\n```\n{diff[:8000]}\n```\n\n"
                "输出 JSON 决定：{\"decision\":\"approve\",\"comment\":\"...\"} "
                "或 {\"decision\":\"request_changes\",\"feedback\":\"...\"}"
            )

        returncode, output = await self.run_cli(prompt, cwd=worktree_dev, task_id=task_id)
        if returncode != 0:
            feedback = f"[系统错误] 审查器执行失败（exit={returncode}），请修复环境后重试。"
            await self.add_log(task_id, feedback)
            if output.strip():
                await self.add_log(task_id, f"错误输出:\n{output[:800]}")
            await self.update_task(
                task_id,
                status="needs_changes",
                assignee=None,
                assigned_agent=dev_agent,
                dev_agent=dev_agent,
                review_feedback=feedback,
            )
            return
        await self.add_log(task_id, f"审查输出:\n{output[:400]}")

        decision = self.parse_json_decision(output)
        if decision is None:
            low = output.lower()
            if any(w in low for w in ["approve", "lgtm", "looks good", "合格", "通过", "同意"]):
                decision = {"decision": "approve", "comment": output[:300].strip()}
            elif any(w in low for w in ["fix", "issue", "problem", "需要修改", "问题", "错误"]):
                decision = {"decision": "request_changes", "feedback": output[:500].strip()}
            else:
                decision = {"decision": "request_changes", "feedback": "审查输出无法解析为有效 JSON，请补充明确审查意见并重试。"}

        if decision["decision"] == "approve":
            comment = decision.get("comment", "LGTM")
            await self.add_log(task_id, f"✅ 审查通过: {comment[:200]}")
            await self.update_task(task_id, status="approved", assignee=None, review_feedback=comment)
        else:
            feedback = decision.get("feedback", "请修复问题")
            await self.add_log(task_id, f"↩ 需修改: {feedback[:200]}")
            await self.update_task(
                task_id,
                status="needs_changes",
                assignee=None,
                assigned_agent=dev_agent,
                dev_agent=dev_agent,
                review_feedback=feedback,
            )


if __name__ == "__main__":
    asyncio.run(ReviewerAgent().run())
