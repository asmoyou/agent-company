import asyncio
import os

from base import BaseAgent, get_project_dirs, load_prompt


class ReviewerAgent(BaseAgent):
    name = "reviewer"
    poll_statuses = ["in_review"]
    cli_name = os.getenv("REVIEWER_CLI", "claude")

    async def get_diff(self, worktree_dev) -> str:
        try:
            return await self.git("show", "HEAD", cwd=worktree_dev)
        except Exception as e:
            try:
                return await self.git("diff", "HEAD", cwd=worktree_dev)
            except Exception:
                return f"(无法获取 diff: {e})"

    async def process_task(self, task: dict):
        task_id = task["id"]
        proj_root, worktree_dev = get_project_dirs(task)

        await self.update_task(task_id, status="reviewing", assignee=self.name)
        await self.add_log(task_id, "Reviewer 开始审查")

        diff = await self.get_diff(worktree_dev)
        await self.add_log(task_id, f"获取到 diff ({len(diff)} 字符)")

        # ── Build prompt from template ────────────────────────────────────────
        template = load_prompt("reviewer", project_path=proj_root)
        if template:
            prompt = template.format(
                task_title=task["title"],
                task_description=task["description"] or "(无额外描述)",
                diff=diff[:8000],
            )
        else:
            prompt = (
                f"审查任务「{task['title']}」的代码变更：\n\n```\n{diff[:8000]}\n```\n\n"
                "输出 JSON 决定：{\"decision\":\"approve\",\"comment\":\"...\"} "
                "或 {\"decision\":\"request_changes\",\"feedback\":\"...\"}"
            )

        returncode, output = await self.run_cli(prompt, cwd=worktree_dev, task_id=task_id)
        await self.add_log(task_id, f"审查输出:\n{output[:400]}")

        decision = self.parse_json_decision(output)
        if decision is None:
            low = output.lower()
            if any(w in low for w in ["approve", "lgtm", "looks good", "合格", "通过", "同意"]):
                decision = {"decision": "approve", "comment": output[:300].strip()}
            elif any(w in low for w in ["fix", "issue", "problem", "需要修改", "问题", "错误"]):
                decision = {"decision": "request_changes", "feedback": output[:500].strip()}
            else:
                decision = {"decision": "approve", "comment": "(auto-approved)"}

        if decision["decision"] == "approve":
            comment = decision.get("comment", "LGTM")
            await self.add_log(task_id, f"✅ 审查通过: {comment[:200]}")
            await self.update_task(task_id, status="approved", review_feedback=comment)
        else:
            feedback = decision.get("feedback", "请修复问题")
            await self.add_log(task_id, f"↩ 需修改: {feedback[:200]}")
            await self.update_task(task_id, status="needs_changes", review_feedback=feedback)


if __name__ == "__main__":
    asyncio.run(ReviewerAgent().run())
