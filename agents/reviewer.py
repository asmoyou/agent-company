import asyncio
import json
import os
import re
from datetime import UTC, datetime
from pathlib import Path

from base import BaseAgent, TASK_DELIVERY_MODEL, get_task_dev_agent, parse_status_list
from prompt_registry import REVIEWER_PROMPT_DEFAULT
from task_intelligence import (
    build_feedback_from_issues,
    evidence_bundle_has_blockers,
    next_retry_strategy,
    normalize_issue_list,
    select_reasoning_effort,
    summarize_evidence_blockers,
)

REVIEW_DECISION_SCHEMA = {
    "type": "object",
    "additionalProperties": False,
    "properties": {
        "decision": {"type": "string", "enum": ["approve", "request_changes"]},
        "comment": {"type": "string"},
        "feedback": {"type": "string"},
        "issues": {
            "type": "array",
            "items": {
                "type": "object",
                "additionalProperties": True,
                "properties": {
                    "issue_id": {"type": "string"},
                    "acceptance_item": {"type": "string"},
                    "severity": {"type": "string"},
                    "category": {"type": "string"},
                    "summary": {"type": "string"},
                    "reproducer": {"type": "string"},
                    "evidence_gap": {"type": "string"},
                    "scope": {"type": "string"},
                    "fix_hint": {"type": "string"},
                    "status": {"type": "string"},
                },
                "required": ["summary", "status"],
            },
        },
    },
    "required": ["decision"],
}

REVIEWER_SYSTEM_RETRY_MAX = int(os.getenv("REVIEWER_SYSTEM_RETRY_MAX", "3"))
REVIEWER_SYSTEM_RETRY_BACKOFF_SECS = int(os.getenv("REVIEWER_SYSTEM_RETRY_BACKOFF_SECS", "20"))
REVIEW_DIFF_PREVIEW_CHARS = int(os.getenv("REVIEW_DIFF_PREVIEW_CHARS", "12000"))


class ReviewerAgent(BaseAgent):
    name = "reviewer"
    poll_statuses = ["in_review"]
    cli_name = "codex"
    working_status = "reviewing"

    def __init__(self, shutdown_event=None, config: dict | None = None):
        super().__init__(shutdown_event)
        cfg = config or {}
        self.poll_statuses = parse_status_list(cfg.get("poll_statuses"), ["in_review"])
        self.cli_name = str(cfg.get("cli") or "codex")
        self.prompt_template = str(cfg.get("prompt") or REVIEWER_PROMPT_DEFAULT)
        self.working_status = str(cfg.get("working_status") or "reviewing")

    def respect_assignment_for(self, status: str) -> bool:
        return False

    def _current_system_retry(self, feedback: str | None) -> int:
        m = re.search(r"\[review_retry=(\d+)/(\d+)\]", str(feedback or ""))
        return int(m.group(1)) if m else 0

    def _preferred_reasoning_effort(self, task: dict) -> str | None:
        return select_reasoning_effort(
            task,
            agent=self.name,
            operation="review",
            cli_name=self.cli_name,
        )

    async def _is_ancestor(self, repo_root: Path, ancestor: str, ref: str) -> bool:
        try:
            await self.git("merge-base", "--is-ancestor", ancestor, ref, cwd=repo_root)
            return True
        except Exception:
            return False

    async def _is_patch_equivalent_on_ref(self, repo_root: Path, commit_hash: str, ref: str) -> bool:
        commit = str(commit_hash or "").strip()
        if not commit:
            return False
        try:
            out = (await self.git("cherry", ref, commit, cwd=repo_root)).strip()
        except Exception:
            return False
        if not out:
            return False
        for raw in out.splitlines():
            line = raw.strip()
            if not line:
                continue
            parts = line.split(maxsplit=1)
            if len(parts) != 2:
                continue
            marker, candidate = parts[0], parts[1]
            if marker not in {"+", "-"}:
                continue
            if candidate.startswith(commit) or commit.startswith(candidate):
                return marker == "-"
        return False

    def _build_non_mergeable_commit_feedback(
        self,
        *,
        commit_hash: str,
        parent_commit: str,
        dev_branch: str,
    ) -> str:
        return (
            f"[提交基线不一致] 目标 commit {commit_hash[:12]} 的父提交 "
            f"{parent_commit[:12]} 不在 main 上（或非等价提交），该提交不是可独立合并变更。\n"
            "修改建议：\n"
            f"1. 在 `{dev_branch}` 基于最新 `main` 重建本次改动并生成新 commit；\n"
            "2. 保证新 commit 仅包含本任务文件，不携带历史任务残留改动；\n"
            "3. 交接时附上 `git show --name-status <new_commit>` 结果摘要。"
        )

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
        current_status = str(task.get("status") or "").strip() or "reviewing"
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
            await self.transition_task(
                task_id,
                fields={
                    "status": "in_review",
                    "assignee": None,
                    "assigned_agent": self.name,
                    "dev_agent": dev_agent,
                    "review_feedback": feedback,
                    "feedback_source": self.name,
                    "feedback_stage": "review_system_retry",
                    "feedback_actor": self.name,
                },
                handoff={
                    "stage": "review_system_retry",
                    "to_agent": self.name,
                    "status_from": current_status,
                    "status_to": "in_review",
                    "title": "审查系统错误重试",
                    "summary": feedback,
                    "conclusion": "审查器临时错误，自动重试中",
                    "payload": {"retry": next_retry, "max": REVIEWER_SYSTEM_RETRY_MAX, "reason": reason},
                },
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
        await self.transition_task(
            task_id,
            fields={
                "status": "blocked",
                "assignee": None,
                "assigned_agent": self.name,
                "dev_agent": dev_agent,
                "review_feedback": feedback,
                "feedback_source": self.name,
                "feedback_stage": "review_system_failed",
                "feedback_actor": self.name,
            },
            handoff={
                "stage": "review_system_failed",
                "to_agent": self.name,
                "status_from": current_status,
                "status_to": "blocked",
                "title": "审查系统错误终止",
                "summary": feedback,
                "conclusion": "审查流程阻塞，等待环境修复后重试",
                "payload": {"reason": reason},
            },
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

    async def get_diff_for_commit(self, worktree_dev, commit_hash: str, repo_root=None) -> str:
        try:
            return await self.git("show", commit_hash, cwd=worktree_dev)
        except Exception as first_error:
            if repo_root is None:
                raise first_error
            return await self.git("show", commit_hash, cwd=repo_root)

    async def get_diff_for_patchset(
        self,
        worktree_dev,
        *,
        head_sha: str,
        base_sha: str = "",
        repo_root=None,
    ) -> str:
        try:
            if base_sha:
                return await self.git("diff", f"{base_sha}..{head_sha}", cwd=worktree_dev)
            return await self.git("show", head_sha, cwd=worktree_dev)
        except Exception as first_error:
            if repo_root is None:
                raise first_error
            if base_sha:
                return await self.git("diff", f"{base_sha}..{head_sha}", cwd=repo_root)
            return await self.git("show", head_sha, cwd=repo_root)

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
        current_status = str(task.get("status") or "").strip() or "reviewing"
        if await self.stop_if_task_cancelled(task_id, "开始审查前"):
            return
        dev_agent = get_task_dev_agent(task)
        patchset = await self.resolve_task_patchset(task)
        commit_hash = (task.get("commit_hash") or "").strip()
        task_commit_hash = commit_hash
        related_history_commits: list[dict] = []
        proj_root, worktree_dev, branch = await self.ensure_agent_workspace(
            task, agent_key=dev_agent, sync_with_main=False
        )
        patchset_head = str((patchset or {}).get("head_sha") or "").strip()
        patchset_base = str((patchset or {}).get("base_sha") or "").strip()
        use_patchset = TASK_DELIVERY_MODEL == "patchset" and bool(patchset_head)
        if use_patchset:
            commit_hash = patchset_head
        if not commit_hash:
            commit_hash, related_history_commits = await self.resolve_handoff_commit_candidate(
                task_id,
                worktree_dev,
            )

        if not commit_hash:
            feedback = "[系统错误] 缺少 commit_hash，无法进行精确审查。请 developer 重新提交。"
            issues = normalize_issue_list(
                [{
                    "issue_id": "missing-commit-hash",
                    "acceptance_item": "可审查交付",
                    "severity": "high",
                    "category": "evidence",
                    "summary": "缺少可审查 commit_hash，reviewer 无法定位交付物",
                    "reproducer": "任务 handoff 未提供 commit_hash，且历史交接无法解析有效候选 commit",
                    "evidence_gap": "缺少精确交付 commit 证据",
                    "scope": "handoff metadata",
                    "fix_hint": "developer 重新提交并确保 handoff 携带 commit_hash / patchset",
                    "status": "new",
                }],
                default_status="open",
            )
            await self.transition_task(
                task_id,
                fields={
                    "status": "needs_changes",
                    "assignee": None,
                    "assigned_agent": dev_agent,
                    "dev_agent": dev_agent,
                    "review_feedback": build_feedback_from_issues(issues, feedback),
                    "feedback_source": self.name,
                    "feedback_stage": "review_to_dev",
                    "feedback_actor": self.name,
                },
                handoff={
                    "stage": "review_to_dev",
                    "to_agent": dev_agent,
                    "status_from": current_status,
                    "status_to": "needs_changes",
                    "title": "审查退回开发",
                    "summary": feedback,
                    "conclusion": "缺少可审查 commit，退回开发重提",
                    "payload": {
                        "reason": "missing_commit_hash",
                        "has_commit": False,
                        "related_commit_candidates": related_history_commits,
                        "issues": issues,
                        "attempt": {
                            "stage": "review_to_dev",
                            "outcome": "request_changes",
                            "execution_phase": "critic",
                            "retry_strategy": "repro_first",
                            "failure_fingerprint": "",
                            "same_fingerprint_streak": 0,
                            "summary": feedback,
                            "metadata": {"issue_count": len(issues)},
                        },
                    },
                },
                log_message=feedback,
            )
            await self.add_alert(
                summary="审查前置条件缺失：commit_hash",
                task_id=task_id,
                message=feedback,
                kind="error",
                code="reviewer_missing_commit_hash",
                stage="review_to_dev",
            )
            return
        if use_patchset:
            try:
                dirty = (await self.git("status", "--porcelain", cwd=worktree_dev)).strip()
            except Exception:
                dirty = ""
            if dirty:
                feedback = (
                    "[交付不完整] 当前任务分支仍有未提交改动，reviewer 不会基于脏工作区继续审查。\n"
                    "请先清理工作区并重新提交，再重新流转审查。"
                )
                issues = normalize_issue_list(
                    [{
                        "issue_id": "dirty-worktree",
                        "acceptance_item": "可审查交付",
                        "severity": "high",
                        "category": "scope",
                        "summary": "patchset 对应工作区仍有未提交改动，交付未冻结",
                        "reproducer": "reviewer 在工作区执行 git status --porcelain 仍返回脏文件",
                        "evidence_gap": "缺少干净工作区证据",
                        "scope": "worktree",
                        "fix_hint": "先清理未提交改动并重新提交，再重新流转审查",
                        "status": "new",
                    }],
                    default_status="open",
                )
                await self.transition_task(
                    task_id,
                    fields={
                        "status": "needs_changes",
                        "assignee": None,
                        "assigned_agent": dev_agent,
                        "dev_agent": dev_agent,
                        "review_feedback": build_feedback_from_issues(issues, feedback)[:1000],
                        "feedback_source": self.name,
                        "feedback_stage": "review_to_dev",
                        "feedback_actor": self.name,
                        "current_patchset_id": str(patchset.get("id") or "").strip() or None,
                        "current_patchset_status": "rejected",
                    },
                    handoff={
                        "stage": "review_to_dev",
                        "to_agent": dev_agent,
                        "status_from": current_status,
                        "status_to": "needs_changes",
                        "title": "审查退回开发",
                        "summary": feedback[:300],
                        "commit_hash": commit_hash,
                        "conclusion": "工作区存在未提交改动，退回开发清理后重提",
                        "payload": {
                            "decision": "request_changes",
                            "reason": "dirty_worktree",
                            "issues": issues,
                            "commit_hash": commit_hash,
                            "source_branch": branch,
                            "attempt": {
                                "stage": "review_to_dev",
                                "outcome": "request_changes",
                                "execution_phase": "critic",
                                "retry_strategy": "surface_freeze",
                                "failure_fingerprint": "",
                                "same_fingerprint_streak": 0,
                                "summary": feedback[:300],
                                "metadata": {"issue_count": len(issues)},
                            },
                            "patchset": {
                                **patchset,
                                "status": "rejected",
                                "summary": "工作区存在未提交改动",
                            },
                        },
                    },
                    log_message=f"↩ 需修改: {feedback[:200]}",
                )
                return
        if not task_commit_hash and related_history_commits:
            top = related_history_commits[0]
            await self.add_log(
                task_id,
                (
                    "任务未显式提供 commit_hash，已使用历史提交证据进行审查："
                    f"{top.get('short') or str(commit_hash)[:12]}（候选 {len(related_history_commits)} 条）"
                ),
            )

        if not use_patchset:
            parent_commit = ""
            try:
                parent_commit = (await self.git("rev-parse", f"{commit_hash}^", cwd=proj_root)).strip()
            except Exception:
                parent_commit = ""
            if parent_commit:
                parent_on_main = await self._is_ancestor(proj_root, parent_commit, "main")
                if not parent_on_main:
                    parent_on_main = await self._is_patch_equivalent_on_ref(
                        proj_root, parent_commit, "main"
                    )
                if not parent_on_main:
                    feedback = self._build_non_mergeable_commit_feedback(
                        commit_hash=commit_hash,
                        parent_commit=parent_commit,
                        dev_branch=branch,
                    )
                    issues = normalize_issue_list(
                        [{
                            "issue_id": "non-mergeable-baseline",
                            "acceptance_item": "可独立合并交付",
                            "severity": "high",
                            "category": "packaging",
                            "summary": "目标 commit 的父提交不在 main 上，当前交付不可独立合并",
                            "reproducer": f"parent_commit={parent_commit[:12]} 不在 main 或其等价补丁集上",
                            "evidence_gap": "缺少独立可合并的提交基线",
                            "scope": branch,
                            "fix_hint": "基于最新 main 重建本次改动并生成新的独立 commit",
                            "status": "new",
                        }],
                        default_status="open",
                    )
                    await self.transition_task(
                        task_id,
                        fields={
                            "status": "needs_changes",
                            "assignee": None,
                            "assigned_agent": dev_agent,
                            "dev_agent": dev_agent,
                            "review_feedback": build_feedback_from_issues(issues, feedback)[:1000],
                            "feedback_source": self.name,
                            "feedback_stage": "review_to_dev",
                            "feedback_actor": self.name,
                        },
                        handoff={
                            "stage": "review_to_dev",
                            "to_agent": dev_agent,
                            "status_from": current_status,
                            "status_to": "needs_changes",
                            "title": "审查退回开发",
                            "summary": feedback[:300],
                            "commit_hash": commit_hash,
                            "conclusion": "提交基线不一致，退回开发重建可独立合并的 commit",
                            "payload": {
                                "decision": "request_changes",
                                "issues": issues,
                                "commit_hash": commit_hash,
                                "source_branch": branch,
                                "related_history_commits": related_history_commits,
                                "reason": "non_mergeable_commit_baseline",
                                "attempt": {
                                    "stage": "review_to_dev",
                                    "outcome": "request_changes",
                                    "execution_phase": "critic",
                                    "retry_strategy": "package_audit",
                                    "failure_fingerprint": "",
                                    "same_fingerprint_streak": 0,
                                    "summary": feedback[:300],
                                    "metadata": {"issue_count": len(issues)},
                                },
                            },
                        },
                        log_message=f"↩ 需修改: {feedback[:200]}",
                    )
                    return

        decision_dir = worktree_dev / ".opc" / "decisions"
        decision_dir.mkdir(parents=True, exist_ok=True)
        decision_file = decision_dir / f"{task_id}.review.json"
        try:
            decision_file.unlink(missing_ok=True)
        except Exception:
            pass

        patchset_note = ""
        if use_patchset and patchset and str(patchset.get("id") or "").strip():
            patchset_note = (
                f", patchset={patchset.get('id')}, "
                f"base={str(patchset.get('base_sha') or '')[:12] or '-'}"
            )
        await self.add_log(
            task_id,
            f"Reviewer 开始审查（dev_agent={dev_agent}, 分支={branch}, commit={commit_hash}{patchset_note}）",
        )

        try:
            if use_patchset:
                diff = await self.get_diff_for_patchset(
                    worktree_dev,
                    head_sha=patchset_head,
                    base_sha=patchset_base,
                    repo_root=proj_root,
                )
            else:
                diff = await self.get_diff_for_commit(worktree_dev, commit_hash, repo_root=proj_root)
        except Exception as e:
            feedback = f"[系统错误] 无法读取目标 commit={commit_hash}：{e}"
            issues = normalize_issue_list(
                [{
                    "issue_id": "cannot-read-commit",
                    "acceptance_item": "可审查交付",
                    "severity": "high",
                    "category": "evidence",
                    "summary": "reviewer 无法读取目标 commit 或 patchset diff",
                    "reproducer": str(e),
                    "evidence_gap": "缺少可读取的 diff 证据",
                    "scope": commit_hash,
                    "fix_hint": "修复提交引用或重新生成可访问的 patchset/commit 后再送审",
                    "status": "new",
                }],
                default_status="open",
            )
            await self.transition_task(
                task_id,
                fields={
                    "status": "needs_changes",
                    "assignee": None,
                    "assigned_agent": dev_agent,
                    "dev_agent": dev_agent,
                    "review_feedback": build_feedback_from_issues(issues, feedback)[:500],
                    "feedback_source": self.name,
                    "feedback_stage": "review_to_dev",
                    "feedback_actor": self.name,
                },
                    handoff={
                        "stage": "review_to_dev",
                        "to_agent": dev_agent,
                        "status_from": current_status,
                        "status_to": "needs_changes",
                    "title": "审查退回开发",
                    "summary": feedback[:300],
                    "commit_hash": commit_hash,
                    "conclusion": "目标 commit 无法读取，退回开发处理",
                    "payload": {
                        "reason": "cannot_read_commit",
                        "commit_hash": commit_hash,
                        "issues": issues,
                        "attempt": {
                            "stage": "review_to_dev",
                            "outcome": "request_changes",
                            "execution_phase": "critic",
                            "retry_strategy": "repro_first",
                            "failure_fingerprint": "",
                            "same_fingerprint_streak": 0,
                            "summary": feedback[:300],
                            "metadata": {"issue_count": len(issues)},
                        },
                    },
                },
                log_message=feedback[:500],
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
            return

        if use_patchset:
            await self.add_log(
                task_id,
                f"获取到 patchset diff ({len(diff)} 字符): {str(patchset.get('id') or '')} head={commit_hash}",
            )
        else:
            await self.add_log(task_id, f"获取到 commit diff ({len(diff)} 字符): {commit_hash}")

        diff_file = decision_dir / f"{task_id}.review.patch"
        try:
            diff_file.write_text(diff, encoding="utf-8")
            await self.add_log(task_id, f"已写入完整 diff 文件: {diff_file}")
        except Exception as e:
            await self.add_log(task_id, f"写入完整 diff 文件失败，回退内联摘要: {e}")
            diff_file = None

        preview_chars = max(1000, int(REVIEW_DIFF_PREVIEW_CHARS))
        diff_preview = diff[:preview_chars]
        diff_for_prompt = diff_preview
        if diff_file is not None:
            diff_for_prompt = (
                f"[完整 diff 文件] {diff_file}\n"
                f"[diff 总字符数] {len(diff)}\n"
                "请基于该文件进行完整审查，不要仅依赖以下预览。\n\n"
                f"{diff_preview}"
            )

        # ── Build prompt from template ────────────────────────────────────────
        template = (self.prompt_template or "").strip()
        if template:
            try:
                prompt = template.format(
                    task_title=task["title"],
                    task_description=task["description"] or "(无额外描述)",
                    commit_hash=commit_hash,
                    dev_agent=dev_agent,
                    diff=diff_for_prompt,
                    diff_file=str(diff_file) if diff_file is not None else "",
                    diff_preview=diff_preview,
                    diff_total_chars=len(diff),
                )
            except Exception:
                prompt = template
        else:
            prompt = (
                f"审查任务「{task['title']}」的代码变更（commit={commit_hash}）：\n\n```\n{diff_for_prompt}\n```\n\n"
                "输出 JSON 决定：{\"decision\":\"approve\",\"comment\":\"...\"} "
                "或 {\"decision\":\"request_changes\",\"feedback\":\"...\"}"
            )
        if use_patchset:
            prompt += (
                "\n\n## Patchset 上下文\n"
                f"- patchset_id: {str(patchset.get('id') or '').strip()}\n"
                f"- base_sha: {patchset_base or '(empty)'}\n"
                f"- head_sha: {patchset_head}\n"
                f"- commit_count: {int(patchset.get('commit_count') or 0)}\n"
                f"- worktree_clean: {'yes' if bool(patchset.get('worktree_clean')) else 'no'}\n"
            )
        if diff_file is not None:
            prompt += (
                "\n\n## 审查完整性要求\n"
                f"- 完整 diff 文件位于：{diff_file}\n"
                "- 必须基于完整 diff 给出结论，不可只依据预览片段。"
            )
        review_contract = self.build_review_contract_block(task)
        if review_contract:
            prompt += f"\n\n{review_contract}\n"
        issue_ledger = self.build_issue_ledger_block(task)
        if issue_ledger:
            prompt += f"\n\n{issue_ledger}\n"
        latest_evidence = task.get("latest_evidence")
        if isinstance(latest_evidence, dict):
            prompt += (
                "\n\n## 最近一次送审证据包\n"
                f"- summary: {str(latest_evidence.get('summary') or '').strip()}\n"
                f"- created_at: {str(latest_evidence.get('created_at') or '').strip()}\n"
                f"- evidence_bundle: {json.dumps(latest_evidence.get('bundle') or {}, ensure_ascii=False)[:6000]}\n"
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
            expected_status=str(task.get("status") or "").strip().lower(),
            expected_assignee=self.name,
            reasoning_effort=self._preferred_reasoning_effort(task),
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

        reviewed_main_sha = ""
        if use_patchset:
            try:
                reviewed_main_sha = (await self.git("rev-parse", "main", cwd=proj_root)).strip()
            except Exception:
                reviewed_main_sha = ""
        patchset_for_handoff = patchset
        if use_patchset and patchset:
            try:
                patchset_for_handoff = await self.enrich_patchset_snapshot(
                    proj_root,
                    patchset,
                    source_branch=branch,
                )
            except Exception:
                patchset_for_handoff = patchset
        review_timestamp = datetime.now(UTC).isoformat()
        issues = normalize_issue_list(decision.get("issues"), default_status="open")
        latest_bundle = {}
        latest_evidence = task.get("latest_evidence")
        if isinstance(latest_evidence, dict) and isinstance(latest_evidence.get("bundle"), dict):
            latest_bundle = latest_evidence.get("bundle") or {}

        if decision["decision"] == "approve":
            machine_blockers = summarize_evidence_blockers(latest_bundle, limit=4)
            unresolved_issue_count = sum(
                1 for item in issues if str(item.get("status") or "").strip().lower() != "resolved"
            )
            if evidence_bundle_has_blockers(latest_bundle) or unresolved_issue_count:
                details = machine_blockers or ["模型给出了 approve，但机器校验仍存在未闭环问题。"]
                feedback = "[机器校验阻止通过] " + "；".join(details[:4])
                if unresolved_issue_count and not machine_blockers:
                    feedback += f"；仍有 {unresolved_issue_count} 个 unresolved issues"
                issues = normalize_issue_list(
                    issues
                    or [
                        {
                            "issue_id": "machine-review-blocker",
                            "acceptance_item": "机器校验",
                            "severity": "high",
                            "category": "evidence",
                            "summary": feedback,
                            "reproducer": "检查 latest_evidence.bundle 与 reviewer 决策是否一致",
                            "evidence_gap": "latest_evidence.bundle 仍存在 blocker",
                            "scope": "review gate",
                            "fix_hint": "先消除机器校验 blocker，再重新送审",
                            "status": "open",
                        }
                    ],
                    default_status="open",
                )
                decision = {"decision": "request_changes", "feedback": feedback, "issues": issues}

        if decision["decision"] == "approve":
            comment = decision.get("comment", "LGTM")
            await self.transition_task(
                task_id,
                fields={
                    "status": "approved",
                    "assignee": None,
                    "review_feedback": comment,
                    "feedback_source": self.name,
                    "feedback_stage": "review_to_manager",
                    "feedback_actor": self.name,
                    "current_patchset_id": str((patchset or {}).get("id") or "").strip() or None if use_patchset else None,
                    "current_patchset_status": "approved" if use_patchset else None,
                },
                handoff={
                    "stage": "review_to_manager",
                    "to_agent": "manager",
                    "status_from": current_status,
                    "status_to": "approved",
                    "title": "审查通过，交接合并",
                    "summary": comment[:300],
                    "commit_hash": commit_hash,
                    "conclusion": comment[:300] or "审查通过",
                    "payload": {
                        "decision": "approve",
                        "commit_hash": commit_hash,
                        "source_branch": branch,
                        "related_history_commits": related_history_commits,
                        "resolve_open_issues": True,
                        "resolve_issue_sources": ["reviewer", "verifier"],
                        "issue_resolution_reason": "review_approved",
                        "attempt": {
                            "stage": "review_to_manager",
                            "outcome": "approved",
                            "execution_phase": "critic",
                            "retry_strategy": str(task.get("retry_strategy") or "default_implement"),
                            "failure_fingerprint": "",
                            "same_fingerprint_streak": 0,
                            "summary": comment[:300],
                            "metadata": {"issue_count": len(issues)},
                        },
                        "patchset": (
                            {
                                **(patchset_for_handoff or {}),
                                "status": "approved",
                                "summary": comment[:300],
                                "queue_status": "queued",
                                "queue_reason": "",
                                "approved_at": review_timestamp,
                                "queued_at": review_timestamp,
                                "reviewed_main_sha": reviewed_main_sha,
                            }
                            if use_patchset
                            else {}
                        ),
                    },
                    "artifact_path": str(decision_file),
                },
                log_message=f"✅ 审查通过: {comment[:200]}",
            )
        else:
            feedback = decision.get("feedback", "请修复问题")
            issue_feedback = build_feedback_from_issues(issues, feedback)
            issue_categories = {str(item.get("category") or "").strip().lower() for item in issues}
            retry_strategy = next_retry_strategy(
                current_strategy=str(task.get("retry_strategy") or "default_implement"),
                failure_stage="review_to_dev",
                same_fingerprint_streak=1,
                open_issue_count=len(issues),
                has_surface_violation="scope" in issue_categories,
                has_evidence_gap=bool(issue_categories & {"coverage", "evidence", "docs"}),
            )
            await self.transition_task(
                task_id,
                fields={
                    "status": "needs_changes",
                    "assignee": None,
                    "assigned_agent": dev_agent,
                    "dev_agent": dev_agent,
                    "review_feedback": issue_feedback,
                    "feedback_source": self.name,
                    "feedback_stage": "review_to_dev",
                    "feedback_actor": self.name,
                    "current_patchset_id": str((patchset or {}).get("id") or "").strip() or None if use_patchset else None,
                    "current_patchset_status": "rejected" if use_patchset else None,
                },
                handoff={
                    "stage": "review_to_dev",
                    "to_agent": dev_agent,
                    "status_from": current_status,
                    "status_to": "needs_changes",
                    "title": "审查退回开发",
                    "summary": issue_feedback[:300],
                    "commit_hash": commit_hash,
                    "conclusion": issue_feedback[:300] or "审查未通过",
                    "payload": {
                        "decision": "request_changes",
                        "issues": issues,
                        "resolve_missing_issues": True,
                        "commit_hash": commit_hash,
                        "source_branch": branch,
                        "related_history_commits": related_history_commits,
                        "attempt": {
                            "stage": "review_to_dev",
                            "outcome": "request_changes",
                            "execution_phase": "critic",
                            "retry_strategy": retry_strategy,
                            "failure_fingerprint": "",
                            "same_fingerprint_streak": 0,
                            "summary": issue_feedback[:300],
                            "metadata": {"issue_count": len(issues)},
                        },
                        "patchset": (
                            {
                                **(patchset_for_handoff or {}),
                                "status": "rejected",
                                "summary": issue_feedback[:300],
                                "queue_status": "",
                                "queue_reason": "review_rejected",
                                "reviewed_main_sha": reviewed_main_sha,
                            }
                            if use_patchset
                            else {}
                        ),
                    },
                    "artifact_path": str(decision_file),
                },
                log_message=f"↩ 需修改: {issue_feedback[:200]}",
            )


if __name__ == "__main__":
    asyncio.run(ReviewerAgent().run())
