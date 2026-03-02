import asyncio
import json

from base import BaseAgent


class GenericAgent(BaseAgent):
    """
    A configurable agent driven by an agent_type record from the database.
    Behavior: polls configured statuses → runs CLI prompt → commits any file
    changes → advances task to next_status.
    """

    def __init__(self, config: dict, shutdown_event=None):
        super().__init__(shutdown_event)
        self.name           = config["key"]
        self._display_name  = config["name"]
        self.poll_statuses  = json.loads(config.get("poll_statuses") or '["todo"]')
        self.next_status    = config.get("next_status") or "in_review"
        self._working_status = config.get("working_status") or "in_progress"
        self._prompt_tpl    = config.get("prompt") or ""
        self.cli_name       = config.get("cli") or "claude"

    async def process_task(self, task: dict):
        task_id = task["id"]
        proj_root, worktree_dev, branch = await self.ensure_agent_workspace(task, agent_key=self.name)

        await self.add_log(
            task_id, f"{self._display_name} 接手，分支: {branch}，工作目录: {worktree_dev}"
        )

        prev_status = task.get("_claimed_from_status", task.get("status"))
        is_rework = task.get("review_feedback") and prev_status == "needs_changes"
        rework_section = (
            f"## 审查反馈（必须全部修复）\n\n{task['review_feedback']}"
            if is_rework else ""
        )

        if self._prompt_tpl:
            try:
                prompt = self._prompt_tpl.format(
                    task_title=task["title"],
                    task_description=task["description"] or "(无额外描述)",
                    rework_section=rework_section,
                )
            except KeyError:
                prompt = self._prompt_tpl
        else:
            prompt = (
                f"完成任务：{task['title']}\n\n"
                f"{task['description'] or ''}\n\n"
                f"{rework_section}\n\n"
                "把所有输出写入文件，不要只输出文字。"
            )

        returncode, output = await self.run_cli(prompt, cwd=worktree_dev, task_id=task_id)
        if output.strip():
            await self.add_log(task_id, f"输出摘要:\n{output[:400]}")

        # Stage & check diff
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
            await self.add_log(task_id, f"无文件创建，输出已保存为 {fallback.name}")
            await self.git("add", "-A", cwd=worktree_dev)
            diff = await self.git("diff", "--cached", "--stat", cwd=worktree_dev)

        if diff.strip():
            try:
                await self.git(
                    "-c", "user.email=agent@opc-demo.local",
                    "-c", "user.name=OPC Agent",
                    "commit", "-m", f"{self.name}: {task['title'][:72]}\n\nTask ID: {task_id}",
                    cwd=worktree_dev,
                )
                commit_hash = await self.git("rev-parse", "--short", "HEAD", cwd=worktree_dev)
                await self.add_log(task_id, f"已提交: {commit_hash}\n{diff.strip()}")
                update_fields = {
                    "status": self.next_status,
                    "assignee": None,
                    "commit_hash": commit_hash,
                }
                if self.next_status == "in_review":
                    update_fields["assigned_agent"] = self.name
                    update_fields["dev_agent"] = self.name
                await self.update_task(task_id, **update_fields)
                return
            except Exception as e:
                await self.add_log(task_id, f"提交失败: {e}")

        await self.add_log(task_id, f"无文件变更，推进至 {self.next_status}")
        update_fields = {"status": self.next_status, "assignee": None}
        if self.next_status == "in_review":
            update_fields["assigned_agent"] = self.name
            update_fields["dev_agent"] = self.name
        await self.update_task(task_id, **update_fields)

    def working_status_for(self, status: str) -> str:
        return self._working_status or status


if __name__ == "__main__":
    import sys
    print("GenericAgent requires config dict; use run_all.py to launch.")
    sys.exit(1)
