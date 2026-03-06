import asyncio
import json
import re
from datetime import UTC, datetime
from pathlib import Path

from base import BaseAgent, MANAGER_MERGE_MODE, TASK_DELIVERY_MODEL, get_task_dev_agent, parse_status_list

MANAGER_PROMPT_DEFAULT = (
    "你是发布合并管理者。请优先将已审查 patchset(base..head) 以 deterministic squash merge 方式合并到 main；"
    "只有缺少 patchset 时才回退到 commit 路径。\n\n"
    "任务标题：{task_title}\n"
    "目标 commit：{commit_hash}\n"
    "来源分支：{dev_branch}\n"
    "仓库路径：{project_path}\n\n"
    "请执行：\n"
    "1. 切换到 main（不存在则创建）。\n"
    "2. 验证目标 commit 在来源分支上（git merge-base --is-ancestor）。\n"
    "3. 仅合并目标 commit（不要合并整个分支 HEAD）。\n"
    "4. 提交信息使用：{merge_message}\n\n"
    "若冲突，停止并保留冲突现场，不要强行解决。\n"
    "完成后把结果写入 JSON 文件：{decision_file}\n"
    "JSON 格式：\n"
    '{"decision":"merged|already_up_to_date|conflict|failed","message":"..."}\n'
    "并在回复最后一行输出同一个 JSON 对象。"
)


def _utcnow_iso() -> str:
    return datetime.now(UTC).isoformat()


class ManagerAgent(BaseAgent):
    name = "manager"
    poll_statuses = ["approved"]
    cli_name = "codex"
    working_status = "merging"

    def __init__(self, shutdown_event=None, config: dict | None = None):
        super().__init__(shutdown_event)
        cfg = config or {}
        self.poll_statuses = parse_status_list(cfg.get("poll_statuses"), ["approved"])
        self.cli_name = str(cfg.get("cli") or "codex")
        self.prompt_template = str(cfg.get("prompt") or MANAGER_PROMPT_DEFAULT)
        self.working_status = str(cfg.get("working_status") or "merging")

    def respect_assignment_for(self, status: str) -> bool:
        return False

    async def _is_ancestor(self, repo_root: Path, ancestor: str, ref: str) -> bool:
        try:
            await self.git("merge-base", "--is-ancestor", ancestor, ref, cwd=repo_root)
            return True
        except Exception:
            return False

    async def _ensure_on_main(self, repo_root: Path):
        try:
            current = (await self.git("branch", "--show-current", cwd=repo_root)).strip()
        except Exception:
            current = ""
        if current == "main":
            return
        try:
            await self.git("checkout", "main", cwd=repo_root)
        except Exception:
            await self.git("checkout", "-b", "main", cwd=repo_root)

    async def _cleanup_merge_state(self, repo_root: Path):
        for cmd in (("merge", "--abort"), ("cherry-pick", "--abort"), ("rebase", "--abort")):
            try:
                await self.git(*cmd, cwd=repo_root)
            except Exception:
                continue

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

    def _output_has_conflict_signal(self, text: str) -> bool:
        raw = str(text or "")
        if not raw:
            return False
        low = raw.lower()

        if re.search(r"(?m)^conflict\b", low):
            return True
        for marker in (
            "conflict (",
            "automatic merge failed",
            "merge conflict",
            "resolve all conflicts manually",
            "error: could not apply",
            "cherry-pick failed",
        ):
            if marker in low:
                return True

        if re.search(
            r"(合并冲突|发生冲突|存在冲突|出现冲突|冲突文件|请解决冲突)",
            raw,
        ):
            if re.search(
                r"(无冲突|没有冲突|未发生冲突|未出现冲突|冲突情况[:：]\s*无冲突)",
                raw,
            ):
                return False
            return True
        return False

    def _parse_unmerged_from_status(self, text: str) -> list[dict]:
        out: list[dict] = []
        for raw in str(text or "").splitlines():
            line = raw.rstrip()
            if len(line) < 3:
                continue
            code = line[:2]
            if code not in {"DD", "AU", "UD", "UA", "DU", "AA", "UU"}:
                continue
            path = line[3:].strip()
            if " -> " in path:
                path = path.split(" -> ", 1)[1].strip()
            if not path:
                continue
            out.append({"code": code, "path": path})
        return out

    async def _attempt_auto_merge_strategies(self, repo_root: Path, target_commit: str) -> dict:
        """
        Try safe, deterministic cherry-pick strategies before giving up.
        Returns:
          {
            resolved: bool,
            strategy: str,
            head_after: str,
            attempts: [{strategy,status,error,conflicts}],
          }
        """
        attempts: list[dict] = []
        strategies: list[tuple[str, list[str]]] = [
            ("theirs", ["-X", "theirs"]),
            ("ours", ["-X", "ours"]),
        ]

        for strategy_name, strategy_flags in strategies:
            await self._cleanup_merge_state(repo_root)
            await self._ensure_on_main(repo_root)
            try:
                await self.git(
                    "cherry-pick",
                    "-x",
                    *strategy_flags,
                    target_commit,
                    cwd=repo_root,
                )
                head_after = (await self.git("rev-parse", "--short", "HEAD", cwd=repo_root)).strip()
                attempts.append({"strategy": strategy_name, "status": "merged", "conflicts": []})
                return {
                    "resolved": True,
                    "strategy": strategy_name,
                    "head_after": head_after,
                    "attempts": attempts,
                }
            except Exception as e:
                status_text = ""
                conflicts: list[dict] = []
                try:
                    status_text = await self.git("status", "--porcelain", cwd=repo_root)
                    conflicts = self._parse_unmerged_from_status(status_text)
                except Exception:
                    pass
                attempts.append(
                    {
                        "strategy": strategy_name,
                        "status": "failed",
                        "error": str(e)[:240],
                        "conflicts": conflicts,
                    }
                )
                await self._cleanup_merge_state(repo_root)

        await self._ensure_on_main(repo_root)
        return {"resolved": False, "strategy": "", "head_after": "", "attempts": attempts}

    def _build_conflict_rework_feedback(
        self,
        *,
        target_commit: str,
        dev_agent: str,
        dev_branch: str,
        attempts: list[dict],
        conflicts: list[dict],
    ) -> str:
        short_commit = (target_commit or "")[:12]
        lines: list[str] = [
            (
                f"[合并冲突] Manager 已尝试自动处理冲突但失败：commit {short_commit} "
                "仍无法无冲突合并到 main。"
            ),
            "修改建议（请按顺序执行，避免重复返工）：",
            (
                f"1. 在 `{dev_branch}` 基于最新 `main` 重建本次改动，"
                "确保形成可独立 cherry-pick 的新提交（不要复用旧 commit）。"
            ),
            (
                "2. 仅保留与当前任务直接相关文件，移除历史任务残留改动；"
                "提交前自检 `git show --name-status <new_commit>`。"
            ),
            (
                "3. 在交接说明中逐条写明每个冲突文件如何处理（保留/删除/迁移到哪个文件），"
                "并附上新的 commit_hash。"
            ),
        ]

        if attempts:
            lines.append("Manager 自动处理尝试记录：")
            for item in attempts[:4]:
                strategy = str(item.get("strategy") or "").strip() or "unknown"
                status = str(item.get("status") or "").strip() or "unknown"
                err = str(item.get("error") or "").strip()
                if err:
                    lines.append(f"- 策略 `{strategy}`: {status}（{err[:120]}）")
                else:
                    lines.append(f"- 策略 `{strategy}`: {status}")

        if conflicts:
            lines.append("冲突文件与处理指引：")
            for item in conflicts[:8]:
                code = str(item.get("code") or "").strip().upper()
                path = str(item.get("path") or "").strip()
                if not path:
                    continue
                tip = "先对齐 main 后手工确认该文件最终内容。"
                if code == "DU":
                    tip = "main 已删除、提交仍修改；请确认该文件是否应废弃或迁移。"
                elif code == "UD":
                    tip = "提交删除、main 仍修改；请确认是否应保留删除。"
                elif code == "AA":
                    tip = "双方都新增同名文件；请合并为单一版本。"
                elif code == "UU":
                    tip = "双方均修改同一文件；请手工合并关键差异。"
                lines.append(f"- `{code} {path}`：{tip}")

        lines.append(f"请 {dev_agent} 按上述建议提交新 commit 后重新流转审查。")
        return "\n".join(lines)

    async def _merge_patchset_squash(
        self,
        repo_root: Path,
        *,
        head_sha: str,
        merge_message: str,
    ) -> dict:
        await self._cleanup_merge_state(repo_root)
        await self._ensure_on_main(repo_root)
        try:
            await self.git("config", "user.email", "agent@opc-demo.local", cwd=repo_root)
            await self.git("config", "user.name", "OPC Agent", cwd=repo_root)
        except Exception:
            pass
        head_before = (await self.git("rev-parse", "--short", "HEAD", cwd=repo_root)).strip()
        try:
            await self.git("merge", "--squash", "--no-commit", head_sha, cwd=repo_root)
        except Exception as e:
            status_text = ""
            conflicts: list[dict] = []
            try:
                status_text = await self.git("status", "--porcelain", cwd=repo_root)
                conflicts = self._parse_unmerged_from_status(status_text)
            except Exception:
                pass
            try:
                await self.git("merge", "--abort", cwd=repo_root)
            except Exception:
                pass
            return {
                "status": "conflict",
                "head_before": head_before,
                "head_after": head_before,
                "error": str(e)[:240],
                "conflicts": conflicts,
            }

        try:
            staged = (await self.git("diff", "--cached", "--stat", cwd=repo_root)).strip()
        except Exception:
            staged = ""
        if not staged:
            return {
                "status": "already_up_to_date",
                "head_before": head_before,
                "head_after": head_before,
                "conflicts": [],
            }

        await self.git("commit", "-m", merge_message, cwd=repo_root)
        head_after = (await self.git("rev-parse", "--short", "HEAD", cwd=repo_root)).strip()
        return {
            "status": "merged",
            "head_before": head_before,
            "head_after": head_after,
            "conflicts": [],
        }

    async def _process_patchset(
        self,
        task: dict,
        *,
        task_id: str,
        dev_agent: str,
        proj_root: Path,
        dev_branch: str,
        patchset: dict,
    ) -> bool:
        try:
            patchset = await self.enrich_patchset_snapshot(
                proj_root,
                patchset,
                source_branch=dev_branch,
            )
        except Exception:
            patchset = dict(patchset or {})
        head_sha = str(patchset.get("head_sha") or "").strip()
        patchset_id = str(patchset.get("id") or "").strip()
        if not head_sha:
            return False
        queue_started_at = _utcnow_iso()
        main_head_before = ""
        try:
            await self._ensure_on_main(proj_root)
            main_head_before = (await self.git("rev-parse", "HEAD", cwd=proj_root)).strip()
        except Exception:
            main_head_before = ""
        reviewed_main_sha = str(patchset.get("reviewed_main_sha") or "").strip()

        try:
            await self.git("cat-file", "-e", f"{head_sha}^{{commit}}", cwd=proj_root)
        except Exception as e:
            feedback = f"[合并前置检查失败] patchset head 不存在：{e}"
            await self._return_to_dev_for_merge_issue(
                task_id=task_id,
                dev_agent=dev_agent,
                target_commit=head_sha,
                related_history_commits=[],
                feedback=feedback,
                dev_branch=dev_branch,
                patchset=patchset,
            )
            return True

        await self.add_log(
            task_id,
            (
                f"Manager 开始合并 patchset {patchset_id or '-'} "
                f"(branch={dev_branch}, base={str(patchset.get('base_sha') or '')[:12] or '-'}, "
                f"head={head_sha[:12]})"
            ),
        )
        try:
            await self.update_patchset(
                task_id,
                patchset={
                    **patchset,
                    "status": "approved",
                    "queue_status": "processing",
                    "queue_reason": "",
                    "queue_started_at": queue_started_at,
                    "queue_main_sha": main_head_before,
                    "reviewed_main_sha": reviewed_main_sha,
                    "summary": f"patchset {patchset_id or '-'} 进入 merge queue 处理",
                },
                update_task_refs=False,
            )
        except Exception:
            pass
        merge_message = f"merge: {task['title'][:72]} | Task ID: {task_id}"
        result = await self._merge_patchset_squash(
            proj_root,
            head_sha=head_sha,
            merge_message=merge_message,
        )
        status = str(result.get("status") or "").strip().lower()
        if status in {"merged", "already_up_to_date"}:
            head_after = str(result.get("head_after") or "").strip() or head_sha[:7]
            queue_finished_at = _utcnow_iso()
            summary = (
                f"patchset {patchset_id or '-'} 已 squash 合并到 main：{head_after}"
                if status == "merged"
                else f"patchset {patchset_id or '-'} 已在 main，无需重复合并"
            )
            await self.transition_task(
                task_id,
                fields={
                    "status": "pending_acceptance",
                    "assignee": None,
                    "commit_hash": head_after,
                    "current_patchset_id": patchset_id or None,
                    "current_patchset_status": "merged",
                    "merged_patchset_id": patchset_id or None,
                },
                handoff={
                    "stage": "merge_to_acceptance",
                    "to_agent": "user",
                    "status_from": "approved",
                    "status_to": "pending_acceptance",
                    "title": "合并完成，交接验收",
                    "summary": summary,
                    "commit_hash": head_after,
                    "conclusion": "patchset 合并完成，进入待验收",
                    "payload": {
                        "commit_hash": head_after,
                        "source_branch": dev_branch,
                        "patchset": {
                            **patchset,
                            "status": "merged",
                            "merge_strategy": "squash",
                            "summary": summary,
                            "queue_status": "merged",
                            "queue_reason": "",
                            "queue_started_at": queue_started_at,
                            "queue_finished_at": queue_finished_at,
                            "merged_at": queue_finished_at,
                            "queue_main_sha": main_head_before,
                            "reviewed_main_sha": reviewed_main_sha,
                        },
                    },
                    "artifact_path": str(proj_root),
                },
                log_message=f"✅ 合并成功: {head_after}",
            )
            return True

        queue_reason = "merge_conflict"
        if reviewed_main_sha and main_head_before and reviewed_main_sha != main_head_before:
            queue_reason = "merge_conflict_after_main_advanced"
        refresh_hint = self._build_patchset_refresh_hint(
            patchset=patchset,
            reviewed_main_sha=reviewed_main_sha,
            latest_main_sha=main_head_before,
            queue_reason=queue_reason,
            conflicts=list(result.get("conflicts") or []),
        )
        feedback = self._build_patchset_conflict_feedback(
            patchset=patchset,
            dev_branch=dev_branch,
            conflicts=list(result.get("conflicts") or []),
            refresh_hint=refresh_hint,
        )
        queue_finished_at = _utcnow_iso()
        await self.transition_task(
            task_id,
            fields={
                "status": "needs_changes",
                "assignee": None,
                "assigned_agent": dev_agent,
                "dev_agent": dev_agent,
                "review_feedback": feedback[:1000],
                "feedback_source": self.name,
                "feedback_stage": "merge_to_dev",
                "feedback_actor": self.name,
                "current_patchset_id": patchset_id or None,
                "current_patchset_status": "stale",
            },
            handoff={
                "stage": "merge_to_dev",
                "to_agent": dev_agent,
                "status_from": "approved",
                "status_to": "needs_changes",
                "title": "合并退回开发",
                "summary": feedback[:300],
                "commit_hash": head_sha,
                "conclusion": "patchset 无法合并到最新 main，退回开发刷新后重提",
                "payload": {
                    "reason": "patchset_merge_conflict",
                    "commit_hash": head_sha,
                    "refresh_hint": refresh_hint,
                    "patchset": {
                        **patchset,
                        "status": "stale",
                        "merge_strategy": "squash",
                        "summary": feedback[:300],
                        "queue_status": "stale",
                        "queue_reason": queue_reason,
                        "queue_started_at": queue_started_at,
                        "queue_finished_at": queue_finished_at,
                        "queue_main_sha": main_head_before,
                        "reviewed_main_sha": reviewed_main_sha,
                    },
                    "conflicts": list(result.get("conflicts") or []),
                },
            },
            log_message=feedback[:300],
        )
        return True

    def _load_decision_file(self, path: Path) -> dict | None:
        if not path.exists():
            return None
        try:
            raw = path.read_text(encoding="utf-8", errors="replace").strip()
            if not raw:
                return None
            data = json.loads(raw)
        except Exception:
            return None
        if not isinstance(data, dict):
            return None
        decision = str(data.get("decision") or "").strip().lower()
        if decision not in {"merged", "already_up_to_date", "conflict", "failed"}:
            return None
        msg = str(data.get("message") or "").strip()
        return {"decision": decision, "message": msg}

    def _parse_decision_from_output(self, text: str) -> dict | None:
        tail = text[-5000:] if len(text) > 5000 else text
        for m in reversed(re.findall(r"\{[^{}]*\}", tail)):
            try:
                data = json.loads(m)
            except Exception:
                continue
            if not isinstance(data, dict):
                continue
            decision = str(data.get("decision") or "").strip().lower()
            if decision not in {"merged", "already_up_to_date", "conflict", "failed"}:
                continue
            return {"decision": decision, "message": str(data.get("message") or "").strip()}
        return None

    async def _return_to_dev_for_merge_issue(
        self,
        *,
        task_id: str,
        dev_agent: str,
        target_commit: str,
        related_history_commits: list[dict],
        feedback: str,
        dev_branch: str = "",
        patchset: dict | None = None,
    ):
        await self.add_log(task_id, feedback[:500])
        await self.add_alert(
            summary="合并退回开发",
            task_id=task_id,
            message=feedback[:1000],
            kind="warning",
            code="manager_merge_to_dev",
            stage="merge_to_dev",
            metadata={"commit_hash": target_commit, "dev_branch": dev_branch},
        )
        await self.transition_task(
            task_id,
            fields={
                "status": "needs_changes",
                "assignee": None,
                "assigned_agent": dev_agent,
                "dev_agent": dev_agent,
                "review_feedback": feedback[:1000],
                "feedback_source": self.name,
                "feedback_stage": "merge_to_dev",
                "feedback_actor": self.name,
                "current_patchset_id": (
                    str((patchset or {}).get("id") or "").strip() or None
                ),
                "current_patchset_status": (
                    "stale" if patchset else None
                ),
            },
            handoff={
                "stage": "merge_to_dev",
                "to_agent": dev_agent,
                "status_from": "approved",
                "status_to": "needs_changes",
                "title": "合并退回开发",
                "summary": feedback[:300],
                "commit_hash": target_commit,
                "conclusion": "合并前置检查失败或发生冲突，退回开发处理",
                "payload": {
                    "reason": "merge_to_dev",
                    "commit_hash": target_commit,
                    "related_history_commits": related_history_commits,
                    "patchset": (
                        {
                            **patchset,
                            "status": "stale",
                            "summary": feedback[:300],
                        }
                        if patchset
                        else {}
                    ),
                },
            },
            log_message=feedback[:300],
        )

    async def _squash_merge_patchset(
        self,
        repo_root: Path,
        *,
        head_sha: str,
        merge_message: str,
    ) -> dict:
        await self._cleanup_merge_state(repo_root)
        await self._ensure_on_main(repo_root)
        head_before = (await self.git("rev-parse", "--short", "HEAD", cwd=repo_root)).strip()
        try:
            await self.git("merge", "--squash", "--no-commit", head_sha, cwd=repo_root)
        except Exception as e:
            status_text = ""
            conflicts: list[dict] = []
            try:
                status_text = await self.git("status", "--porcelain", cwd=repo_root)
                conflicts = self._parse_unmerged_from_status(status_text)
            except Exception:
                pass
            try:
                await self.git("merge", "--abort", cwd=repo_root)
            except Exception:
                pass
            return {
                "ok": False,
                "status": "conflict",
                "head_before": head_before,
                "head_after": head_before,
                "error": str(e)[:240],
                "conflicts": conflicts,
            }

        staged = ""
        try:
            staged = await self.git("diff", "--cached", "--name-only", cwd=repo_root)
        except Exception:
            staged = ""
        if not staged.strip():
            try:
                await self.git("merge", "--abort", cwd=repo_root)
            except Exception:
                pass
            return {
                "ok": True,
                "status": "already_up_to_date",
                "head_before": head_before,
                "head_after": head_before,
                "error": "",
                "conflicts": [],
            }

        await self.git("commit", "-m", merge_message, cwd=repo_root)
        head_after = (await self.git("rev-parse", "--short", "HEAD", cwd=repo_root)).strip()
        return {
            "ok": True,
            "status": "merged",
            "head_before": head_before,
            "head_after": head_after,
            "error": "",
            "conflicts": [],
        }

    def _build_patchset_conflict_feedback(
        self,
        *,
        patchset: dict,
        dev_branch: str,
        conflicts: list[dict],
        refresh_hint: dict | None = None,
    ) -> str:
        patchset_id = str(patchset.get("id") or "").strip()[:16]
        head_sha = str(patchset.get("head_sha") or "").strip()[:12]
        changed_files = patchset.get("changed_files") if isinstance(patchset.get("changed_files"), list) else []
        lines = [
            f"[Patchset 合并冲突] patchset {patchset_id or '-'}（head={head_sha or '-'}) 无法无冲突落到最新 main。",
            "修改建议：",
            f"1. 在 `{dev_branch}` 基于最新 `main` 刷新当前任务分支，并保留最终净变更。",
            "2. 清理工作区后重新生成 patchset，再重新送审。",
            "3. 交接时附上新的 base/head 与 changed files 摘要。",
        ]
        if refresh_hint:
            lines.append(
                "4. refresh 基线："
                f"reviewed_main={str(refresh_hint.get('reviewed_main_sha') or '-')[:12]} -> "
                f"latest_main={str(refresh_hint.get('latest_main_sha') or '-')[:12]}。"
            )
        if changed_files:
            summary = " · ".join(
                f"{str(item.get('status') or 'M').strip().upper()} {str(item.get('path') or '').strip()}"
                for item in changed_files[:6]
                if str(item.get("path") or "").strip()
            )
            if summary:
                lines.append(f"变更摘要：{summary}")
        for item in conflicts[:8]:
            code = str(item.get('code') or '').strip().upper()
            path = str(item.get('path') or '').strip()
            if code and path:
                lines.append(f"- `{code} {path}`")
        return "\n".join(lines)

    def _build_patchset_refresh_hint(
        self,
        *,
        patchset: dict,
        reviewed_main_sha: str,
        latest_main_sha: str,
        queue_reason: str,
        conflicts: list[dict],
    ) -> dict:
        changed_files = patchset.get("changed_files") if isinstance(patchset.get("changed_files"), list) else []
        return {
            "action": "refresh_patchset",
            "reason": queue_reason,
            "base_sha": str(patchset.get("base_sha") or "").strip(),
            "head_sha": str(patchset.get("head_sha") or "").strip(),
            "reviewed_main_sha": reviewed_main_sha,
            "latest_main_sha": latest_main_sha,
            "main_advanced": bool(reviewed_main_sha and latest_main_sha and reviewed_main_sha != latest_main_sha),
            "changed_files": changed_files[:32],
            "conflicts": conflicts[:16],
            "steps": [
                "同步最新 main 到当前任务分支并解决冲突。",
                "确认只保留当前任务范围内的净变更。",
                "清理工作区后重新生成 patchset，再重新送审。",
            ],
        }

    async def process_task(self, task: dict):
        task_id = task["id"]
        if await self.stop_if_task_cancelled(task_id, "开始合并前"):
            return

        dev_agent = get_task_dev_agent(task)
        patchset = await self.resolve_task_patchset(task)
        commit_hash = (task.get("commit_hash") or "").strip()
        task_commit_hash = commit_hash
        related_history_commits: list[dict] = []
        proj_root, _, dev_branch = await self.ensure_agent_workspace(
            task, agent_key=dev_agent, sync_with_main=False
        )
        if (
            TASK_DELIVERY_MODEL == "patchset"
            and MANAGER_MERGE_MODE != "single_commit"
            and patchset
            and str(patchset.get("head_sha") or "").strip()
        ):
            handled = await self._process_patchset(
                task,
                task_id=task_id,
                dev_agent=dev_agent,
                proj_root=proj_root,
                dev_branch=dev_branch,
                patchset=patchset,
            )
            if handled:
                return

        if not commit_hash:
            commit_hash, related_history_commits = await self.resolve_handoff_commit_candidate(
                task_id,
                proj_root,
            )
        target_commit = commit_hash

        if not task_commit_hash and commit_hash:
            head = related_history_commits[0] if related_history_commits else {}
            await self.add_log(
                task_id,
                (
                    "任务未显式提供 commit_hash，已改用历史提交证据继续合并："
                    f"{head.get('short') or commit_hash[:12]}"
                ),
            )

        if not target_commit:
            feedback = (
                "[系统错误] 缺少可合并 commit_hash（未找到可用历史提交证据），"
                "无法精确合并。退回开发重提。"
            )
            await self._return_to_dev_for_merge_issue(
                task_id=task_id,
                dev_agent=dev_agent,
                target_commit="",
                related_history_commits=related_history_commits,
                feedback=feedback,
                dev_branch=dev_branch,
            )
            return

        await self.add_log(
            task_id,
            f"Manager 开始合并 commit {target_commit}（源分支: {dev_branch}，项目: {proj_root.name}）",
        )

        try:
            await self.git("cat-file", "-e", f"{target_commit}^{{commit}}", cwd=proj_root)
        except Exception as e:
            feedback = f"[合并前置检查失败] {e}。请 {dev_agent} 重新提交可合并 commit。"
            await self._return_to_dev_for_merge_issue(
                task_id=task_id,
                dev_agent=dev_agent,
                target_commit=target_commit,
                related_history_commits=related_history_commits,
                feedback=feedback,
                dev_branch=dev_branch,
            )
            return

        source_contains_target = await self._is_ancestor(proj_root, target_commit, dev_branch)
        if not source_contains_target:
            await self.add_log(
                task_id,
                (
                    f"⚠ 目标提交 {target_commit[:12]} 当前不在分支 {dev_branch} HEAD 上；"
                    "继续按 commit hash 精确合并。"
                ),
            )

        await self._ensure_on_main(proj_root)
        parent_commit = ""
        try:
            parent_commit = (await self.git("rev-parse", f"{target_commit}^", cwd=proj_root)).strip()
        except Exception:
            parent_commit = ""
        if parent_commit:
            parent_on_main = await self._is_ancestor(proj_root, parent_commit, "main")
            if not parent_on_main:
                parent_on_main = await self._is_patch_equivalent_on_ref(
                    proj_root, parent_commit, "main"
                )
            if not parent_on_main:
                feedback = (
                    f"[提交基线不一致] 目标 commit {target_commit[:12]} 的父提交 "
                    f"{parent_commit[:12]} 不在 main 上（或非等价提交），该提交不是可独立合并变更。\n"
                    "修改建议：\n"
                    f"1. 在 `{dev_branch}` 基于最新 `main` 重建本次改动并生成新 commit；\n"
                    "2. 保证新 commit 仅包含本任务文件，不携带历史任务残留改动；\n"
                    "3. 交接时附上 `git show --name-status <new_commit>` 结果摘要。"
                )
                await self._return_to_dev_for_merge_issue(
                    task_id=task_id,
                    dev_agent=dev_agent,
                    target_commit=target_commit,
                    related_history_commits=related_history_commits,
                    feedback=feedback,
                    dev_branch=dev_branch,
                )
                return

        head_before = (await self.git("rev-parse", "--short", "HEAD", cwd=proj_root)).strip()
        before_contains_target = await self._is_ancestor(proj_root, target_commit, "main")
        before_contains_equivalent = (
            False
            if before_contains_target
            else await self._is_patch_equivalent_on_ref(proj_root, target_commit, "main")
        )

        decision_dir = proj_root / ".opc" / "decisions"
        decision_dir.mkdir(parents=True, exist_ok=True)
        decision_file = decision_dir / f"{task_id}.manager-merge.json"
        try:
            decision_file.unlink(missing_ok=True)
        except Exception:
            pass

        merge_message = f"merge: {task['title'][:72]} | Task ID: {task_id}"
        template = (self.prompt_template or "").strip()
        if template:
            try:
                prompt = template.format(
                    task_title=task["title"],
                    task_description=task.get("description") or "(无额外描述)",
                    commit_hash=target_commit,
                    dev_branch=dev_branch,
                    project_path=str(proj_root),
                    merge_message=merge_message,
                    decision_file=str(decision_file),
                )
            except Exception:
                prompt = template
        else:
            prompt = MANAGER_PROMPT_DEFAULT.format(
                task_title=task["title"],
                commit_hash=target_commit,
                dev_branch=dev_branch,
                project_path=str(proj_root),
                merge_message=merge_message,
                decision_file=str(decision_file),
            )
        handoff_context = await self.build_handoff_context(task_id)
        if handoff_context:
            prompt += f"\n\n{handoff_context}\n"

        returncode, output = await self.run_cli(
            prompt,
            cwd=proj_root,
            task_id=task_id,
            expected_status=str(task.get("status") or "").strip().lower(),
            expected_assignee=self.name,
        )
        if await self.stop_if_task_cancelled(task_id, "合并 CLI 执行后"):
            return
        if output.strip():
            await self.add_log(task_id, f"合并输出摘要:\n{output[:500]}")

        decision = self._load_decision_file(decision_file)
        if decision is None:
            decision = self._parse_decision_from_output(output)

        # Best-effort cleanup if CLI left repository in partial merge state.
        await self._cleanup_merge_state(proj_root)
        await self._ensure_on_main(proj_root)

        head_after = (await self.git("rev-parse", "--short", "HEAD", cwd=proj_root)).strip()
        after_contains_target = await self._is_ancestor(proj_root, target_commit, "main")
        after_contains_equivalent = (
            True
            if after_contains_target
            else await self._is_patch_equivalent_on_ref(proj_root, target_commit, "main")
        )
        merge_effective = after_contains_target or after_contains_equivalent
        auto_merge_info: dict | None = None
        is_conflict = (
            (decision or {}).get("decision") == "conflict"
            or self._output_has_conflict_signal(output)
        )

        if is_conflict and not merge_effective:
            await self.add_log(
                task_id,
                "检测到冲突，Manager 开始自动处理（策略：cherry-pick -X theirs / -X ours）。",
            )
            auto_merge_info = await self._attempt_auto_merge_strategies(proj_root, target_commit)
            if auto_merge_info.get("resolved"):
                head_after = str(auto_merge_info.get("head_after") or "").strip() or (
                    await self.git("rev-parse", "--short", "HEAD", cwd=proj_root)
                ).strip()
                after_contains_target = await self._is_ancestor(proj_root, target_commit, "main")
                after_contains_equivalent = (
                    True
                    if after_contains_target
                    else await self._is_patch_equivalent_on_ref(proj_root, target_commit, "main")
                )
                merge_effective = after_contains_target or after_contains_equivalent
                if merge_effective:
                    await self.add_log(
                        task_id,
                        (
                            "冲突自动处理成功："
                            f"strategy={auto_merge_info.get('strategy')}, head={head_after}"
                        ),
                    )
            else:
                await self.add_log(task_id, "冲突自动处理失败，将携带修改建议退回开发。")

        if merge_effective:
            already_up_to_date = (
                before_contains_target
                or before_contains_equivalent
                or (decision or {}).get("decision") == "already_up_to_date"
                or head_before == head_after
            )
            if already_up_to_date:
                await self.transition_task(
                    task_id,
                    fields={"status": "pending_acceptance", "assignee": None, "commit_hash": head_after},
                    handoff={
                        "stage": "merge_to_acceptance",
                        "to_agent": "user",
                        "status_from": "approved",
                        "status_to": "pending_acceptance",
                        "title": "无需合并，进入验收",
                        "summary": "目标提交已在 main，直接进入待验收",
                        "commit_hash": head_after or target_commit,
                        "conclusion": "目标提交已存在于 main，进入验收",
                        "payload": {
                            "commit_hash": head_after or target_commit,
                            "already_up_to_date": True,
                            "reviewed_commit": target_commit,
                            "related_history_commits": related_history_commits,
                            "cli_exit_code": returncode,
                        },
                        "artifact_path": str(proj_root),
                    },
                    log_message="目标提交已在 main，进入待验收",
                )
                return

            await self.transition_task(
                task_id,
                fields={"status": "pending_acceptance", "assignee": None, "commit_hash": head_after},
                handoff={
                    "stage": "merge_to_acceptance",
                    "to_agent": "user",
                    "status_from": "approved",
                    "status_to": "pending_acceptance",
                    "title": "合并完成，交接验收",
                    "summary": (
                        f"冲突自动处理成功（{auto_merge_info.get('strategy')}），已合并到 main：{head_after}"
                        if auto_merge_info and auto_merge_info.get("resolved")
                        else f"CLI 已将审查通过提交合并到 main：{head_after}"
                    ),
                    "commit_hash": head_after,
                    "conclusion": "合并完成，进入待验收",
                    "payload": {
                        "commit_hash": head_after,
                        "source_branch": dev_branch,
                        "reviewed_commit": target_commit,
                        "related_history_commits": related_history_commits,
                        "cli_exit_code": returncode,
                        "auto_merge_strategy": (
                            str(auto_merge_info.get("strategy") or "").strip()
                            if auto_merge_info and auto_merge_info.get("resolved")
                            else ""
                        ),
                    },
                    "artifact_path": str(proj_root),
                },
                log_message=f"✅ 合并成功: {head_after}",
            )
            return

        if is_conflict:
            attempts = []
            if auto_merge_info and isinstance(auto_merge_info.get("attempts"), list):
                attempts = auto_merge_info.get("attempts") or []
            merged_conflicts: list[dict] = []
            seen = set()
            for item in attempts:
                for c in (item.get("conflicts") or []):
                    code = str(c.get("code") or "").strip().upper()
                    path = str(c.get("path") or "").strip()
                    key = (code, path)
                    if not code or not path or key in seen:
                        continue
                    seen.add(key)
                    merged_conflicts.append({"code": code, "path": path})

            feedback = self._build_conflict_rework_feedback(
                target_commit=target_commit,
                dev_agent=dev_agent,
                dev_branch=dev_branch,
                attempts=attempts,
                conflicts=merged_conflicts,
            )
            await self._return_to_dev_for_merge_issue(
                task_id=task_id,
                dev_agent=dev_agent,
                target_commit=target_commit,
                related_history_commits=related_history_commits,
                feedback=feedback,
                dev_branch=dev_branch,
            )
            return

        err_hint = str((decision or {}).get("message") or "").strip()
        if not err_hint:
            err_hint = (output[-500:] or f"exit={returncode}").strip()
        await self.add_alert(
            summary="合并失败（系统错误）",
            task_id=task_id,
            message=err_hint[:1000],
            kind="error",
            code="manager_merge_failed",
            stage="merge_failed",
            metadata={"commit_hash": target_commit, "exit_code": returncode},
        )
        await self.transition_task(
            task_id,
            fields={"status": "approved", "assignee": None},
            handoff={
                "stage": "merge_failed",
                "to_agent": self.name,
                "status_from": "approved",
                "status_to": "approved",
                "title": "合并失败（系统）",
                "summary": f"CLI 合并未生效：{err_hint[:200]}",
                "commit_hash": target_commit,
                "conclusion": "合并系统错误，等待重试",
                "payload": {"error": err_hint[:500], "commit_hash": target_commit, "exit_code": returncode},
            },
        )


if __name__ == "__main__":
    asyncio.run(ManagerAgent().run())
