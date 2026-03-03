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
        if await self.stop_if_task_cancelled(task_id, "开始处理前"):
            return
        proj_root, worktree_dev, branch = await self.ensure_agent_workspace(task, agent_key=self.name)

        await self.add_log(
            task_id, f"{self._display_name} 接手，分支: {branch}，工作目录: {worktree_dev}"
        )
        sync_result = await self.sync_from_latest_handoff(task, worktree_dev, current_branch=branch)
        if sync_result.get("status") == "failed":
            return

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
        handoff_context = await self.build_handoff_context(task_id)
        if handoff_context:
            prompt += f"\n\n{handoff_context}\n"

        returncode, output = await self.run_cli(prompt, cwd=worktree_dev, task_id=task_id)
        if returncode != 0:
            if await self.stop_if_task_cancelled(task_id, "CLI 失败后"):
                return
            prev_status = task.get("_claimed_from_status", task.get("status"))
            await self.add_log(task_id, f"❌ CLI 执行失败（exit={returncode}），任务退回 {prev_status}")
            if output.strip():
                await self.add_log(task_id, f"错误输出:\n{output[:800]}")
            await self.add_alert(
                summary=f"{self._display_name} 执行失败（exit={returncode}）",
                task_id=task_id,
                message=output[-1200:].strip(),
                kind="error",
                code=f"{self.name}_cli_failed",
                stage=f"{self.name}_failed",
                metadata={"exit_code": returncode},
            )
            await self.transition_task(
                task_id,
                fields={"status": prev_status, "assignee": None},
                handoff={
                    "stage": f"{self.name}_failed",
                    "to_agent": self.name,
                    "status_from": prev_status,
                    "status_to": prev_status,
                    "title": f"{self._display_name} 执行失败",
                    "summary": f"CLI 执行失败（exit={returncode}）",
                    "conclusion": f"{self._display_name} 执行失败，回退到 {prev_status}",
                    "payload": {"exit_code": returncode},
                },
                log_message=f"❌ CLI 执行失败（exit={returncode}），任务退回 {prev_status}",
            )
            return
        if output.strip():
            await self.add_log(task_id, f"输出摘要:\n{output[:400]}")
        if await self.stop_if_task_cancelled(task_id, "CLI 执行后"):
            return

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
                if await self.stop_if_task_cancelled(task_id, "提交后状态更新前"):
                    return
                if self.next_status == "in_review":
                    update_fields["assigned_agent"] = self.name
                    update_fields["dev_agent"] = self.name
                await self.transition_task(
                    task_id,
                    fields=update_fields,
                    handoff={
                        "stage": f"{self.name}_handoff",
                        "to_agent": (update_fields.get("assigned_agent") or self.next_status),
                        "status_from": prev_status,
                        "status_to": self.next_status,
                        "title": f"{self._display_name} 交接",
                        "summary": f"已提交 commit {commit_hash}，推进到 {self.next_status}",
                        "commit_hash": commit_hash,
                        "conclusion": f"{self._display_name} 完成，推进到 {self.next_status}",
                        "payload": {"commit_hash": commit_hash, "diff_stat": diff.strip(), "source_branch": branch},
                        "artifact_path": str(worktree_dev),
                    },
                )
                return
            except Exception as e:
                await self.add_log(task_id, f"提交失败: {e}")
                await self.add_alert(
                    summary=f"{self._display_name} 提交失败",
                    task_id=task_id,
                    message=str(e),
                    kind="error",
                    code=f"{self.name}_commit_failed",
                    stage=f"{self.name}_failed",
                )
                await self.transition_task(
                    task_id,
                    fields={"status": prev_status, "assignee": None},
                    handoff={
                        "stage": f"{self.name}_failed",
                        "to_agent": self.name,
                        "status_from": prev_status,
                        "status_to": prev_status,
                        "title": f"{self._display_name} 提交失败",
                        "summary": str(e)[:300],
                        "conclusion": f"{self._display_name} 提交失败，任务回退",
                        "payload": {"error": str(e)},
                    },
                    log_message=f"提交失败: {e}",
                )
                return

        await self.add_log(task_id, f"无文件变更，推进至 {self.next_status}")
        if await self.stop_if_task_cancelled(task_id, "无变更推进前"):
            return
        update_fields = {"status": self.next_status, "assignee": None}
        if self.next_status == "in_review":
            update_fields["assigned_agent"] = self.name
            update_fields["dev_agent"] = self.name
        await self.transition_task(
            task_id,
            fields=update_fields,
            handoff={
                "stage": f"{self.name}_handoff",
                "to_agent": (update_fields.get("assigned_agent") or self.next_status),
                "status_from": prev_status,
                "status_to": self.next_status,
                "title": f"{self._display_name} 交接",
                "summary": f"无文件变更，推进到 {self.next_status}",
                "conclusion": f"无文件变更，推进到 {self.next_status}",
                "payload": {"has_commit": False, "source_branch": branch},
                "artifact_path": str(worktree_dev),
            },
            log_message=f"无文件变更，推进至 {self.next_status}",
        )

    def working_status_for(self, status: str) -> str:
        return self._working_status or status


if __name__ == "__main__":
    import sys
    print("GenericAgent requires config dict; use run_all.py to launch.")
    sys.exit(1)
