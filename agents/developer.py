import asyncio

from base import BaseAgent, parse_status_list

DEVELOPER_PROMPT_DEFAULT = (
    "你是一名专业软件工程师，负责实现以下任务。\n\n"
    "## 任务信息\n\n"
    "**标题**：{task_title}\n\n"
    "**需求描述**：\n"
    "{task_description}\n\n"
    "{rework_section}\n\n"
    "## 工作要求\n\n"
    "1. **所有成果必须写入文件**，不要只在终端打印输出\n"
    "   - 代码任务 → 创建对应语言的源文件（.py / .ts / .go 等）\n"
    "   - 文档/方案任务 → 创建 `.md` 文件，把完整内容写入\n"
    "   - 至少创建一个文件，否则任务无法通过审查\n\n"
    "2. **质量标准**\n"
    "   - 代码需有适当注释，边界情况需处理\n"
    "   - 文档需完整、结构清晰\n\n"
    "3. 直接开始实现，不需要解释计划"
)


class DeveloperAgent(BaseAgent):
    name = "developer"
    poll_statuses = ["todo", "needs_changes"]
    cli_name = "claude"
    working_status = "in_progress"

    def __init__(self, shutdown_event=None, config: dict | None = None):
        super().__init__(shutdown_event)
        cfg = config or {}
        self.poll_statuses = parse_status_list(cfg.get("poll_statuses"), ["todo", "needs_changes"])
        self.cli_name = str(cfg.get("cli") or "claude")
        self.prompt_template = str(cfg.get("prompt") or DEVELOPER_PROMPT_DEFAULT)
        self.working_status = str(cfg.get("working_status") or "in_progress")

    async def process_task(self, task: dict):
        task_id = task["id"]
        if await self.stop_if_task_cancelled(task_id, "开始处理前"):
            return
        _, worktree_dev, branch = await self.ensure_agent_workspace(task, agent_key=self.name)

        await self.add_log(task_id, f"Developer 接手，分支: {branch}，工作目录: {worktree_dev}")

        prev_status = task.get("_claimed_from_status", task.get("status"))
        is_rework = prev_status == "needs_changes" and task.get("review_feedback")
        if is_rework:
            await self.add_log(task_id, "根据审查意见返工")

        # ── Build prompt from template ────────────────────────────────────────
        rework_section = ""
        if is_rework:
            rework_section = f"## 审查反馈（必须全部修复）\n\n{task['review_feedback']}"

        template = (self.prompt_template or "").strip()
        if template:
            try:
                prompt = template.format(
                    task_title=task["title"],
                    task_description=task["description"] or "(无额外描述)",
                    rework_section=rework_section,
                )
            except Exception:
                prompt = template
        else:
            # Built-in fallback (should rarely be used)
            prompt = (
                f"实现任务：{task['title']}\n\n{task['description']}\n\n"
                f"{rework_section}\n\n"
                "要求：把所有内容写入文件，不要只输出文字。"
            )

        # ── Run CLI ───────────────────────────────────────────────────────────
        returncode, output = await self.run_cli(prompt, cwd=worktree_dev, task_id=task_id)
        if returncode != 0:
            await self.add_log(task_id, f"❌ CLI 执行失败（exit={returncode}），任务退回 {prev_status}")
            if output.strip():
                await self.add_log(task_id, f"错误输出:\n{output[:800]}")
            await self.update_task(task_id, status=prev_status, assignee=None)
            return
        if output.strip():
            await self.add_log(task_id, f"CLI 输出摘要:\n{output[:400]}")
        if await self.stop_if_task_cancelled(task_id, "CLI 执行后"):
            return

        # ── Stage & check diff ────────────────────────────────────────────────
        await self.git("add", "-A", cwd=worktree_dev)
        diff = await self.git("diff", "--cached", "--stat", cwd=worktree_dev)

        # Fallback: save stdout as .md if nothing was written to disk
        if not diff.strip() and output.strip() and len(output) > 50:
            safe = "".join(
                c if c.isalnum() or c in "-_ " else "_"
                for c in task["title"][:40]
            ).strip().replace(" ", "_")
            fallback = worktree_dev / f"{safe or 'deliverable'}.md"
            fallback.write_text(f"# {task['title']}\n\n{output}\n", encoding="utf-8")
            await self.add_log(task_id, f"CLI 未创建文件，输出已保存为 {fallback.name}")
            await self.git("add", "-A", cwd=worktree_dev)
            diff = await self.git("diff", "--cached", "--stat", cwd=worktree_dev)

        if not diff.strip():
            await self.add_log(task_id, "无文件变更，提交审查")
            if await self.stop_if_task_cancelled(task_id, "推进审查前"):
                return
            await self.update_task(
                task_id,
                status="in_review",
                assignee=None,
                assigned_agent=self.name,
                dev_agent=self.name,
            )
            return

        try:
            await self.git(
                "-c", "user.email=agent@opc-demo.local",
                "-c", "user.name=OPC Agent",
                "commit", "-m", f"feat: {task['title'][:72]}\n\nTask ID: {task_id}",
                cwd=worktree_dev,
            )
            commit_hash = await self.git("rev-parse", "--short", "HEAD", cwd=worktree_dev)
            await self.add_log(task_id, f"已提交: {commit_hash}\n{diff.strip()}")
            if await self.stop_if_task_cancelled(task_id, "提交后状态更新前"):
                return
            await self.update_task(
                task_id,
                status="in_review",
                assignee=None,
                assigned_agent=self.name,
                dev_agent=self.name,
                commit_hash=commit_hash,
            )
        except Exception as e:
            await self.add_log(task_id, f"提交失败: {e}")
            await self.update_task(task_id, status="todo", assignee=None)


if __name__ == "__main__":
    asyncio.run(DeveloperAgent().run())
