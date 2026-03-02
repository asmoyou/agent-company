import asyncio
import json
import os
import re
from pathlib import Path

from base import BaseAgent, get_task_dev_agent, parse_status_list

REVIEWER_PROMPT_DEFAULT = (
    "你是资深代码/文档审查工程师，负责审查以下变更。\n\n"
    "## 任务信息\n\n"
    "**标题**：{task_title}\n\n"
    "**需求描述**：\n"
    "{task_description}\n\n"
    "## 变更内容\n\n"
    "```\n"
    "{diff}\n"
    "```\n\n"
    "## 审查要点\n\n"
    "- 是否完整实现了需求描述中的所有要求\n"
    "- 代码/内容是否正确，有无明显错误或遗漏\n"
    "- 代码质量、可读性、边界情况处理\n"
    "- 文件结构是否合理\n\n"
    "## 输出格式\n\n"
    "审查完毕后，在回复最后一行只输出一个 JSON 对象（不要代码块、不要额外文字）：\n"
    '- decision 只能是 "approve" 或 "request_changes"\n'
    '- decision="approve" 时必须提供 comment 字段\n'
    '- decision="request_changes" 时必须提供 feedback 字段'
)

REVIEW_DECISION_SCHEMA = {
    "type": "object",
    "additionalProperties": False,
    "properties": {
        "decision": {"type": "string", "enum": ["approve", "request_changes"]},
        "comment": {"type": "string"},
        "feedback": {"type": "string"},
    },
    "required": ["decision"],
}

REVIEWER_SYSTEM_RETRY_MAX = int(os.getenv("REVIEWER_SYSTEM_RETRY_MAX", "3"))
REVIEWER_SYSTEM_RETRY_BACKOFF_SECS = int(os.getenv("REVIEWER_SYSTEM_RETRY_BACKOFF_SECS", "20"))


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
        self.prompt_template = str(cfg.get("prompt") or REVIEWER_PROMPT_DEFAULT)
        self.working_status = str(cfg.get("working_status") or "reviewing")

    def respect_assignment_for(self, status: str) -> bool:
        return False

    def _current_system_retry(self, feedback: str | None) -> int:
        m = re.search(r"\[review_retry=(\d+)/(\d+)\]", str(feedback or ""))
        return int(m.group(1)) if m else 0

    async def _handle_system_error(
        self,
        task: dict,
        dev_agent: str,
        reason: str,
        output: str = "",
    ) -> None:
        def summarize_output(text: str, head: int = 1200, tail: int = 1200) -> str:
            s = (text or "").strip()
            if not s:
                return ""
            if len(s) <= head + tail + 64:
                return s
            return f"{s[:head]}\n\n... <省略 {len(s) - head - tail} 字符> ...\n\n{s[-tail:]}"

        task_id = task["id"]
        if await self.stop_if_task_cancelled(task_id, "系统错误处理前"):
            return

        current = self._current_system_retry(task.get("review_feedback"))
        next_retry = current + 1

        if next_retry <= REVIEWER_SYSTEM_RETRY_MAX:
            feedback = (
                f"[系统错误][review_retry={next_retry}/{REVIEWER_SYSTEM_RETRY_MAX}] "
                f"{reason}"
            )
            await self.add_log(
                task_id,
                f"{feedback}；{REVIEWER_SYSTEM_RETRY_BACKOFF_SECS}s 后自动重试审查",
            )
            out_summary = summarize_output(output)
            if out_summary:
                await self.add_log(task_id, f"错误输出摘要:\n{out_summary}")
            await self.update_task(
                task_id,
                status="in_review",
                assignee=None,
                assigned_agent=self.name,
                dev_agent=dev_agent,
                review_feedback=feedback,
            )
            await self.add_handoff(
                task_id,
                stage="review_system_retry",
                to_agent=self.name,
                status_from="in_review",
                status_to="in_review",
                title="审查系统错误重试",
                summary=feedback,
                conclusion="审查器临时错误，自动重试中",
                payload={"retry": next_retry, "max": REVIEWER_SYSTEM_RETRY_MAX, "reason": reason},
            )
            # Back off before next poll cycle to avoid rapid retry loops.
            await asyncio.sleep(REVIEWER_SYSTEM_RETRY_BACKOFF_SECS)
            return

        feedback = (
            f"[系统错误] 审查器连续失败 {REVIEWER_SYSTEM_RETRY_MAX} 次：{reason}。"
            "任务已进入 blocked。请修复环境后点击“重试审查”或将状态改回 in_review。"
        )
        await self.add_log(task_id, feedback)
        out_summary = summarize_output(output)
        if out_summary:
            await self.add_log(task_id, f"错误输出摘要:\n{out_summary}")
        await self.update_task(
            task_id,
            status="blocked",
            assignee=None,
            assigned_agent=self.name,
            dev_agent=dev_agent,
            review_feedback=feedback,
        )
        await self.add_alert(
            summary="审查器执行失败，任务已阻塞",
            task_id=task_id,
            message=feedback,
            kind="error",
            code="reviewer_system_failed",
            stage="review_system_failed",
            metadata={"reason": reason},
        )
        await self.add_handoff(
            task_id,
            stage="review_system_failed",
            to_agent=self.name,
            status_from="in_review",
            status_to="blocked",
            title="审查系统错误终止",
            summary=feedback,
            conclusion="审查流程阻塞，等待环境修复后重试",
            payload={"reason": reason},
        )

    async def get_diff_for_commit(self, worktree_dev, commit_hash: str, repo_root=None) -> str:
        try:
            return await self.git("show", commit_hash, cwd=worktree_dev)
        except Exception as first_error:
            if repo_root is None:
                raise first_error
            return await self.git("show", commit_hash, cwd=repo_root)

    def _load_decision_file(self, path: Path) -> dict | None:
        if not path.exists():
            return None
        try:
            raw = path.read_text(encoding="utf-8", errors="replace").strip()
            if not raw:
                return None
            data = json.loads(raw)
            if not isinstance(data, dict):
                return None
            return self._normalize_decision_payload(data)
        except Exception:
            return None

    async def process_task(self, task: dict):
        task_id = task["id"]
        if await self.stop_if_task_cancelled(task_id, "开始审查前"):
            return
        dev_agent = get_task_dev_agent(task)
        commit_hash = (task.get("commit_hash") or "").strip()
        proj_root, worktree_dev, branch = await self.ensure_agent_workspace(
            task, agent_key=dev_agent, sync_with_main=False
        )

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
            await self.add_alert(
                summary="审查前置条件缺失：commit_hash",
                task_id=task_id,
                message=feedback,
                kind="error",
                code="reviewer_missing_commit_hash",
                stage="review_to_dev",
            )
            await self.add_handoff(
                task_id,
                stage="review_to_dev",
                to_agent=dev_agent,
                status_from="in_review",
                status_to="needs_changes",
                title="审查退回开发",
                summary=feedback,
                conclusion="缺少可审查 commit，退回开发重提",
                payload={"reason": "missing_commit_hash", "has_commit": False},
            )
            return

        decision_dir = worktree_dev / ".opc" / "decisions"
        decision_dir.mkdir(parents=True, exist_ok=True)
        decision_file = decision_dir / f"{task_id}.review.json"
        try:
            decision_file.unlink(missing_ok=True)
        except Exception:
            pass

        await self.add_log(
            task_id,
            f"Reviewer 开始审查（dev_agent={dev_agent}, 分支={branch}, commit={commit_hash}）",
        )

        try:
            diff = await self.get_diff_for_commit(worktree_dev, commit_hash, repo_root=proj_root)
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
            await self.add_alert(
                summary="审查无法读取目标 commit",
                task_id=task_id,
                message=feedback[:1000],
                kind="error",
                code="reviewer_cannot_read_commit",
                stage="review_to_dev",
                metadata={"commit_hash": commit_hash},
            )
            await self.add_handoff(
                task_id,
                stage="review_to_dev",
                to_agent=dev_agent,
                status_from="in_review",
                status_to="needs_changes",
                title="审查退回开发",
                summary=feedback[:300],
                commit_hash=commit_hash,
                conclusion="目标 commit 无法读取，退回开发处理",
                payload={"reason": "cannot_read_commit", "commit_hash": commit_hash},
            )
            return

        await self.add_log(task_id, f"获取到 commit diff ({len(diff)} 字符): {commit_hash}")

        # ── Build prompt from template ────────────────────────────────────────
        template = (self.prompt_template or "").strip()
        if template:
            try:
                prompt = template.format(
                    task_title=task["title"],
                    task_description=task["description"] or "(无额外描述)",
                    commit_hash=commit_hash,
                    dev_agent=dev_agent,
                    diff=diff[:8000],
                )
            except Exception:
                prompt = template
        else:
            prompt = (
                f"审查任务「{task['title']}」的代码变更（commit={commit_hash}）：\n\n```\n{diff[:8000]}\n```\n\n"
                "输出 JSON 决定：{\"decision\":\"approve\",\"comment\":\"...\"} "
                "或 {\"decision\":\"request_changes\",\"feedback\":\"...\"}"
            )
        handoff_context = await self.build_handoff_context(task_id)
        if handoff_context:
            prompt += f"\n\n{handoff_context}\n"
        prompt += (
            "\n\n## 结构化交付（必须）\n"
            f"请将最终审查决定写入 JSON 文件：{decision_file}\n"
            "文件只能是一个 JSON 对象，格式如下二选一：\n"
            '{"decision":"approve","comment":"..."}\n'
            '{"decision":"request_changes","feedback":"..."}\n'
            "同时在回复最后一行输出同一个 JSON 对象。"
        )

        returncode, output = await self.run_cli(
            prompt,
            cwd=worktree_dev,
            task_id=task_id,
            output_schema=REVIEW_DECISION_SCHEMA,
        )
        if returncode != 0:
            if await self.stop_if_task_cancelled(task_id, "审查 CLI 失败后"):
                return
            await self._handle_system_error(
                task,
                dev_agent=dev_agent,
                reason=f"审查器执行失败（exit={returncode}）",
                output=output,
            )
            return
        await self.add_log(task_id, f"审查输出:\n{output[:400]}")
        if await self.stop_if_task_cancelled(task_id, "审查输出后"):
            return

        decision = self._load_decision_file(decision_file)
        if decision is not None:
            await self.add_log(task_id, f"已读取结构化审查结果文件: {decision_file}")
        else:
            await self.add_log(task_id, f"未获取到有效结构化结果文件，回退到终端输出解析: {decision_file.name}")
            decision = self.parse_json_decision(output)
        if decision is None:
            tail = output[-2000:].strip()
            if re.search(r"\b(lgtm|looks good|approved)\b|审查通过|同意合并|通过审查", tail, re.IGNORECASE):
                decision = {"decision": "approve", "comment": (tail[:300] or "LGTM")}
            else:
                await self._handle_system_error(
                    task,
                    dev_agent=dev_agent,
                    reason="审查输出无法解析为有效 JSON，请检查 reviewer 提示词与 CLI 输出格式",
                    output=output,
                )
                return

        if decision["decision"] == "approve":
            comment = decision.get("comment", "LGTM")
            await self.add_log(task_id, f"✅ 审查通过: {comment[:200]}")
            await self.add_handoff(
                task_id,
                stage="review_to_manager",
                to_agent="manager",
                status_from="in_review",
                status_to="approved",
                title="审查通过，交接合并",
                summary=comment[:300],
                commit_hash=commit_hash,
                conclusion=comment[:300] or "审查通过",
                payload={"decision": "approve", "commit_hash": commit_hash, "source_branch": branch},
                artifact_path=str(decision_file),
            )
            await self.update_task(task_id, status="approved", assignee=None, review_feedback=comment)
        else:
            feedback = decision.get("feedback", "请修复问题")
            await self.add_log(task_id, f"↩ 需修改: {feedback[:200]}")
            await self.add_handoff(
                task_id,
                stage="review_to_dev",
                to_agent=dev_agent,
                status_from="in_review",
                status_to="needs_changes",
                title="审查退回开发",
                summary=feedback[:300],
                commit_hash=commit_hash,
                conclusion=feedback[:300] or "审查未通过",
                payload={"decision": "request_changes", "commit_hash": commit_hash, "source_branch": branch},
                artifact_path=str(decision_file),
            )
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
