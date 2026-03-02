import asyncio
import os

from base import BaseAgent, get_project_dirs


class DeveloperAgent(BaseAgent):
    name = "developer"
    poll_statuses = ["todo", "needs_changes"]
    cli_name = os.getenv("DEVELOPER_CLI", "claude")

    async def process_task(self, task: dict):
        task_id = task["id"]
        _, worktree_dev = get_project_dirs(task)

        await self.update_task(task_id, status="in_progress", assignee=self.name)
        await self.add_log(task_id, f"Developer 接手（来自: {task['status']}），工作目录: {worktree_dev}")

        is_rework = task["status"] == "needs_changes" and task.get("review_feedback")

        prompt_lines = [
            "你是一名专业工程师，需要完成以下任务。",
            f"任务标题：{task['title']}",
            f"任务描述：{task['description']}",
            "",
        ]
        if is_rework:
            prompt_lines += [
                "【审查反馈，必须全部修复】",
                task["review_feedback"],
                "",
            ]
            await self.add_log(task_id, "根据审查意见返工")

        prompt_lines += [
            "【重要要求】",
            "所有内容必须写入文件保存到磁盘，不要只在终端输出文字。",
            "- 代码任务 → 创建对应语言的源文件（.py / .js / .ts 等）",
            "- 文档/方案任务 → 创建 .md 文件，把完整内容写入",
            "- 至少创建一个文件，否则任务无法验收",
            "现在开始，把所有内容写入文件。",
        ]

        returncode, output = await self.run_cli(
            "\n".join(prompt_lines), cwd=worktree_dev, task_id=task_id
        )

        if output.strip():
            await self.add_log(task_id, f"CLI 输出摘要:\n{output[:400]}")

        # Stage all changes
        await self.git("add", "-A", cwd=worktree_dev)
        diff = await self.git("diff", "--cached", "--stat", cwd=worktree_dev)

        # Fallback: if CLI wrote nothing, save stdout as .md
        if not diff.strip():
            if output.strip() and len(output) > 50:
                safe = "".join(
                    c if c.isalnum() or c in "-_ " else "_"
                    for c in task["title"][:40]
                ).strip().replace(" ", "_")
                fallback = worktree_dev / f"{safe or 'deliverable'}.md"
                fallback.write_text(f"# {task['title']}\n\n{output}\n", encoding="utf-8")
                await self.add_log(task_id, f"CLI 未创建文件，已将输出保存为 {fallback.name}")
                await self.git("add", "-A", cwd=worktree_dev)
                diff = await self.git("diff", "--cached", "--stat", cwd=worktree_dev)

        if not diff.strip():
            await self.add_log(task_id, "未检测到文件变更，仍提交审查")
            await self.update_task(task_id, status="in_review")
            return

        try:
            await self.git(
                "-c", "user.email=agent@opc-demo.local",
                "-c", "user.name=OPC Agent",
                "commit", "-m", f"feat: {task['title'][:72]}\n\nTask ID: {task_id}",
                cwd=worktree_dev,
            )
            commit_hash = await self.git("rev-parse", "--short", "HEAD", cwd=worktree_dev)
            await self.add_log(task_id, f"已提交: {commit_hash}\n变更:\n{diff.strip()}")
            await self.update_task(task_id, status="in_review", commit_hash=commit_hash)
        except Exception as e:
            await self.add_log(task_id, f"提交失败: {e}")
            await self.update_task(task_id, status="todo")


if __name__ == "__main__":
    asyncio.run(DeveloperAgent().run())
