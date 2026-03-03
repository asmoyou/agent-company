import asyncio
import json
import re
from pathlib import Path

from base import BaseAgent, get_project_dirs, parse_status_list

TRIAGE_PROMPT_DEFAULT = (
    "你是一个专业的项目评估与分解专家。请评估以下任务是否需要分解：\n\n"
    "## 任务标题\n{task_title}\n\n"
    "## 任务描述\n{task_description}\n\n"
    "## 可用 Agent 类型\n{agent_list}\n\n"
    "## 评估标准\n"
    "- **简单任务**：可以由单个 agent 独立完成，工作量在 1-2 小时内\n"
    "- **复杂任务**：涉及多个独立功能模块，或需要不同专业技能协作\n\n"
    "## 输出格式（严格 JSON，不要任何其他文字）\n\n"
    "如果是简单任务：\n"
    '{"action": "simple", "reason": "一句话说明为何不需要分解"}\n\n'
    "如果是复杂任务：\n"
    '{"action": "decompose", "subtasks": [\n'
    '  {"title": "子任务标题", "objective":"子任务目标", "parent_refs":["R1"], "deliverables":["交付物1"], "acceptance_criteria":["验收1","验收2"], "agent": "developer"}\n'
    "]}"
)

FORCE_DECOMPOSE_PROMPT = (
    "你是一个专业的项目分解专家。请将以下任务分解为 2-5 个可执行的子任务：\n\n"
    "## 任务标题\n{task_title}\n\n"
    "## 任务描述\n{task_description}\n\n"
    "## 可用 Agent 类型\n{agent_list}\n\n"
    "只输出 JSON 数组，不要任何其他文字，字段必须完整且具体：\n"
    "[\n"
    '  {"title": "子任务标题", "objective":"子任务目标", "parent_refs":["R1"], "deliverables":["交付物1"], "acceptance_criteria":["验收1","验收2"], "agent": "developer"}\n'
    "]"
)

LEADER_PROMPT_QUALITY_BLOCK = (
    "## 子任务质量门槛（必须满足）\n"
    "1. 子任务必须是可独立验收的功能，不得空泛。\n"
    "2. 每个子任务必须提供：\n"
    "   - title: 明确功能点（不要“完善功能/优化体验/相关开发”）\n"
    "   - objective: 具体目标（包含对象、行为、结果）\n"
    "   - parent_refs: 必须引用父任务需求编号（如 [\"R1\",\"R3\"]）\n"
    "   - todo_steps: 具体执行步骤清单（按顺序）\n"
    "   - deliverables: 具体交付物清单（文件/接口/页面/脚本/测试）\n"
    "   - acceptance_criteria: 可验证的验收标准，至少 2 条\n"
    "3. parent_refs 只能引用给定的需求编号，禁止自造编号。\n"
    "4. 禁止输出空泛词：如“完善功能”“相关逻辑”“进行优化”“处理需求”等。\n"
)

LEADER_SUBTASK_SCHEMA = {
    "type": "object",
    "additionalProperties": False,
    "properties": {
        "title": {"type": "string"},
        "objective": {"type": "string"},
        "description": {"type": "string"},
        "parent_refs": {
            "type": "array",
            "items": {"type": "string"},
            "minItems": 1,
        },
        "implementation_scope": {
            "type": "array",
            "items": {"type": "string"},
        },
        "todo_steps": {
            "type": "array",
            "items": {"type": "string"},
            "minItems": 2,
        },
        "deliverables": {
            "type": "array",
            "items": {"type": "string"},
            "minItems": 1,
        },
        "acceptance_criteria": {
            "type": "array",
            "items": {"type": "string"},
            "minItems": 2,
        },
        "agent": {"type": "string"},
    },
    "required": ["title", "objective", "parent_refs", "todo_steps", "deliverables", "acceptance_criteria"],
}

LEADER_TRIAGE_SCHEMA = {
    "type": "object",
    "additionalProperties": False,
    "properties": {
        "action": {"type": "string", "enum": ["simple", "decompose"]},
        "reason": {"type": "string"},
        "subtasks": {
            "type": "array",
            "items": LEADER_SUBTASK_SCHEMA,
        },
    },
    "required": ["action"],
}

LEADER_FORCE_DECOMPOSE_SCHEMA = {
    "type": "array",
    "minItems": 2,
    "items": LEADER_SUBTASK_SCHEMA,
}

GENERIC_SUBTASK_PATTERNS = [
    r"完善功能",
    r"优化(体验|性能|功能)?",
    r"相关(逻辑|功能|开发|内容)",
    r"处理(需求|逻辑)",
    r"模块开发",
    r"功能开发",
    r"页面开发",
    r"接口开发",
    r"实现功能",
    r"支持功能",
]

PARENT_REQUIREMENT_SPLIT_RE = re.compile(r"(?:[\n\r]+|[。！？!?；;]+)")
LEADING_BULLET_RE = re.compile(r"^\s*(?:[-*•]+|\d+[.)、:]?)\s*")


class LeaderAgent(BaseAgent):
    name = "leader"
    poll_statuses = ["triage", "decompose"]
    cli_name = "claude"
    working_status = "triaging"

    def __init__(self, shutdown_event=None, config: dict | None = None):
        super().__init__(shutdown_event)
        cfg = config or {}
        self.poll_statuses = parse_status_list(cfg.get("poll_statuses"), ["triage", "decompose"])
        self.cli_name = str(cfg.get("cli") or "claude")
        self.prompt_template = str(cfg.get("prompt") or TRIAGE_PROMPT_DEFAULT)
        self.working_status = str(cfg.get("working_status") or "triaging")

    def respect_assignment_for(self, status: str) -> bool:
        return False

    def _render_prompt(self, template: str, **kwargs) -> str:
        out = str(template or "")
        for k, v in kwargs.items():
            out = out.replace("{" + k + "}", str(v))
        return out

    async def _get_agent_list(self) -> str:
        try:
            r = await self.http.get("/agent-types")
            r.raise_for_status()
            types = r.json()
            lines = [
                f"- {t['key']}: {t['name']} — {t.get('description', '')}"
                for t in types if t["key"] not in ("leader", "manager")
            ]
            return "\n".join(lines) if lines else "- developer: 开发者"
        except Exception:
            return "- developer: 开发者"

    def _load_json_file(self, path: Path):
        if not path.exists():
            return None
        try:
            raw = path.read_text(encoding="utf-8", errors="replace").strip()
            if not raw:
                return None
            return json.loads(raw)
        except Exception:
            return None

    def _build_parent_requirements(self, task: dict | None) -> list[dict]:
        if not task:
            return []
        title = str(task.get("title") or "").strip()
        description = str(task.get("description") or "").strip()
        candidates: list[str] = []
        if title:
            candidates.append(title)
        for seg in PARENT_REQUIREMENT_SPLIT_RE.split(description):
            s = LEADING_BULLET_RE.sub("", str(seg or "")).strip()
            if len(s) < 4:
                continue
            candidates.append(s)

        if not candidates:
            candidates.append("完成父任务描述中的核心需求")

        out: list[dict] = []
        seen: set[str] = set()
        for text in candidates:
            clean = re.sub(r"\s+", " ", text).strip()
            key = clean.lower()
            if not clean or key in seen:
                continue
            seen.add(key)
            out.append({"id": f"R{len(out)+1}", "text": clean[:180]})
            if len(out) >= 12:
                break
        return out

    def _format_parent_requirements(self, reqs: list[dict]) -> str:
        if not reqs:
            return "- R1: 完成父任务描述中的核心需求"
        return "\n".join([f"- {r['id']}: {r['text']}" for r in reqs])

    def _todo_assigned_agent(self, task: dict) -> str | None:
        """Resolve assignee for todo stage; leader should not keep ownership there."""
        assigned = str(task.get("assigned_agent") or "").strip()
        dev_agent = str(task.get("dev_agent") or "").strip()
        if assigned and assigned != self.name:
            return assigned
        if dev_agent and dev_agent != self.name:
            return dev_agent
        return None

    def _is_generic_text(self, text: str) -> bool:
        t = (text or "").strip()
        if len(t) < 6:
            return True
        return any(re.search(p, t, re.IGNORECASE) for p in GENERIC_SUBTASK_PATTERNS)

    def _as_text_list(self, raw) -> list[str]:
        if not isinstance(raw, list):
            return []
        out = []
        for x in raw:
            s = str(x or "").strip()
            if not s:
                continue
            out.append(s[:200])
        return out

    def _is_complex_task(self, task: dict) -> bool:
        text = f"{task.get('title') or ''}\n{task.get('description') or ''}"
        score = 0
        if len((task.get("description") or "").strip()) >= 120:
            score += 1
        if re.search(r"(并且|同时|以及|另外|此外|包含|并发|多模块|多步骤)", text):
            score += 1
        if len(re.findall(r"[、,，；;]", text)) >= 2:
            score += 1
        facets = [
            "前端", "后端", "接口", "数据库", "鉴权", "登录", "支付", "部署",
            "测试", "文档", "监控", "缓存", "队列", "消息", "权限", "审核",
        ]
        hit = sum(1 for k in facets if k in text)
        if hit >= 2:
            score += 2
        elif hit == 1:
            score += 1
        return score >= 3

    def _build_subtask_description(
        self,
        objective: str,
        parent_refs: list[str],
        scope: list[str],
        todo_steps: list[str],
        deliverables: list[str],
        acceptance: list[str],
    ) -> str:
        lines = ["## 子任务目标", objective.strip()]
        if parent_refs:
            lines.append("")
            lines.append("## 关联父需求编号")
            lines.extend([f"- {x}" for x in parent_refs])
        if scope:
            lines.append("")
            lines.append("## 实施范围")
            lines.extend([f"- {x}" for x in scope])
        lines.append("")
        lines.append("## TODO 步骤")
        lines.extend([f"- [ ] {x}" for x in todo_steps])
        lines.append("")
        lines.append("## 交付物")
        lines.extend([f"- {x}" for x in deliverables])
        lines.append("")
        lines.append("## 验收标准")
        lines.extend([f"- [ ] {x}" for x in acceptance])
        return "\n".join(lines)[:3000]

    def _normalize_subtasks(self, raw_subtasks, parent_requirements: list[dict] | None = None) -> tuple[list[dict], list[str]]:
        if not isinstance(raw_subtasks, list):
            return [], ["subtasks 不是数组"]
        valid_parent_refs = {str(r.get("id") or "").strip() for r in (parent_requirements or []) if str(r.get("id") or "").strip()}
        out = []
        issues: list[str] = []
        for i, st in enumerate(raw_subtasks, 1):
            if not isinstance(st, dict):
                issues.append(f"#{i} 不是对象")
                continue
            title = str(st.get("title") or "").strip()
            if not title:
                issues.append(f"#{i} 缺少 title")
                continue
            objective = (
                str(st.get("objective") or "").strip()
                or str(st.get("description") or "").strip()
            )
            todo_steps = self._as_text_list(st.get("todo_steps"))
            deliverables = self._as_text_list(st.get("deliverables"))
            acceptance = self._as_text_list(st.get("acceptance_criteria"))
            scope = self._as_text_list(st.get("implementation_scope"))
            parent_refs = self._as_text_list(st.get("parent_refs"))

            if not objective or len(objective) < 20:
                issues.append(f"#{i} objective 过短")
                continue
            if len(todo_steps) < 2:
                issues.append(f"#{i} todo_steps 少于2条")
                continue
            if len(deliverables) < 1:
                issues.append(f"#{i} deliverables 为空")
                continue
            if len(acceptance) < 2:
                issues.append(f"#{i} acceptance_criteria 少于2条")
                continue
            if len(parent_refs) < 1:
                issues.append(f"#{i} parent_refs 为空")
                continue
            if valid_parent_refs:
                invalid_refs = [x for x in parent_refs if x not in valid_parent_refs]
                if invalid_refs:
                    issues.append(f"#{i} parent_refs 非法: {', '.join(invalid_refs[:3])}")
                    continue
            if self._is_generic_text(title) or self._is_generic_text(objective):
                issues.append(f"#{i} 内容过于空泛")
                continue

            agent = str(st.get("agent") or "developer").strip() or "developer"
            desc = self._build_subtask_description(
                objective=objective,
                parent_refs=parent_refs,
                scope=scope,
                todo_steps=todo_steps,
                deliverables=deliverables,
                acceptance=acceptance,
            )
            out.append(
                {
                    "title": title[:200],
                    "description": desc,
                    "agent": agent,
                    "objective": objective[:300],
                    "parent_refs": parent_refs[:8],
                    "todo_steps": todo_steps[:12],
                    "deliverables": deliverables[:8],
                    "acceptance_criteria": acceptance[:10],
                    "implementation_scope": scope[:8],
                }
            )
        return out, issues

    def _normalize_triage_decision(self, raw_decision, parent_requirements: list[dict] | None = None) -> tuple[dict | None, list[str]]:
        if not isinstance(raw_decision, dict):
            return None, ["triage 结果不是对象"]
        action = str(raw_decision.get("action") or "").strip().lower()
        if action == "simple":
            reason = str(raw_decision.get("reason") or "").strip() or "判定为简单任务"
            return {"action": "simple", "reason": reason[:500]}, []
        if action == "decompose":
            subtasks, issues = self._normalize_subtasks(
                raw_decision.get("subtasks"),
                parent_requirements=parent_requirements,
            )
            if not subtasks:
                return None, issues or ["subtasks 为空或不满足质量门槛"]
            if len(subtasks) < 2:
                return None, issues + ["decompose 至少需要 2 个子任务"]
            reason = str(raw_decision.get("reason") or "").strip()
            out = {"action": "decompose", "subtasks": subtasks}
            if reason:
                out["reason"] = reason[:500]
            return out, issues
        return None, [f"未知 action: {action or '(empty)'}"]

    async def _handle_structured_output_error(
        self,
        task: dict,
        prev_status: str,
        stage_code: str,
        reason: str,
        output: str = "",
    ):
        task_id = task["id"]
        msg = f"[系统错误] Leader 结构化结果无效：{reason}"
        await self.add_log(task_id, msg)
        if output.strip():
            await self.add_log(task_id, f"输出摘要:\n{output[:1000]}")
        await self.add_alert(
            summary="Leader 结构化结果无效，任务阻塞",
            task_id=task_id,
            message=msg,
            kind="error",
            code=stage_code,
            stage="leader_failed",
        )
        await self.transition_task(
            task_id,
            fields={
                "status": "blocked",
                "assignee": None,
                "assigned_agent": self.name,
                "review_feedback": msg,
                "feedback_source": self.name,
                "feedback_stage": "leader_failed",
                "feedback_actor": self.name,
            },
            handoff={
                "stage": "leader_failed",
                "to_agent": self.name,
                "status_from": prev_status,
                "status_to": "blocked",
                "title": "Leader 结构化结果无效",
                "summary": msg,
                "conclusion": "结构化结果无效，任务阻塞等待修复",
                "payload": {"reason": reason, "stage_code": stage_code},
            },
        )

    async def _create_subtasks(self, task: dict, subtasks: list[dict]) -> int:
        created = 0
        for i, st in enumerate(subtasks, 1):
            title = str(st.get("title") or f"子任务 {i}")[:200]
            desc  = str(st.get("description") or "")
            agent = str(st.get("agent") or "developer")
            try:
                r = await self.http.post("/tasks", json={
                    "title": title,
                    "description": desc,
                    "project_id": task.get("project_id"),
                    "parent_task_id": task["id"],
                    "assigned_agent": agent,
                    "status": "todo",   # subtasks skip triage
                })
                r.raise_for_status()
                sub = r.json()
                created += 1
                await self.add_log(task["id"], f"  ✓ [{i}] {title} (→ {agent})")
                self._post_output_bg(f"  子任务 {i}: {title}")
                await self.add_handoff(
                    task["id"],
                    stage="leader_assign_subtask",
                    to_agent=agent,
                    status_from=task.get("status"),
                    status_to="todo",
                    title=f"分配子任务 {i}",
                    summary=f"已创建子任务「{title}」并分配给 {agent}",
                    conclusion=f"子任务已分配给 {agent}",
                    payload={
                        "subtask_id": sub.get("id"),
                        "subtask_title": title,
                        "subtask_agent": agent,
                        "objective": st.get("objective"),
                        "parent_refs": st.get("parent_refs"),
                        "todo_steps": st.get("todo_steps"),
                        "deliverables": st.get("deliverables"),
                        "acceptance_criteria": st.get("acceptance_criteria"),
                    },
                )
            except Exception as e:
                await self.add_log(task["id"], f"  ✗ [{i}] 创建失败: {e}")
        return created

    async def process_task(self, task: dict):
        original_status = task.get("_claimed_from_status", task.get("status"))  # "triage" or "decompose"
        task_id = task["id"]
        if await self.stop_if_task_cancelled(task_id, "开始评估前"):
            return

        if original_status == "decompose":
            # User explicitly requested decomposition — skip evaluation
            await self.add_log(task_id, "Leader 执行强制分解（用户指定）")
            await self._force_decompose(task)
        else:
            # Auto triage — evaluate complexity first
            await self.add_log(task_id, "Leader 评估任务复杂度...")
            await self._auto_triage(task)

    async def _auto_triage(self, task: dict):
        task_id = task["id"]

        # Idempotency: if subtasks already exist (crash recovery), just mark decomposed
        r = await self.http.get(f"/tasks/{task_id}/subtasks")
        if r.status_code == 200 and r.json():
            await self.add_log(task_id, "已有子任务，标记为 decomposed（崩溃恢复）")
            await self.transition_task(
                task_id,
                fields={"status": "decomposed", "assignee": None},
                handoff={
                    "stage": "leader_recover_decomposed",
                    "to_agent": "multi-agent",
                    "status_from": task.get("_claimed_from_status", task.get("status")),
                    "status_to": "decomposed",
                    "title": "分解状态恢复",
                    "summary": "检测到已存在子任务，恢复父任务为 decomposed",
                    "conclusion": "子任务已存在，父任务标记为 decomposed",
                    "payload": {"recovered": True},
                },
                log_message="检测到已有子任务，恢复父任务状态为 decomposed",
            )
            return

        agent_list  = await self._get_agent_list()
        prompt_tpl  = self.prompt_template or TRIAGE_PROMPT_DEFAULT
        prompt = self._render_prompt(
            prompt_tpl,
            task_title=task["title"],
            task_description=task["description"] or "(无额外描述)",
            agent_list=agent_list,
        )
        unresolved = [
            x for x in ("{task_title}", "{task_description}", "{agent_list}") if x in prompt
        ]
        if unresolved:
            await self.add_log(task_id, f"⚠ Leader 模板仍有未替换占位符: {', '.join(unresolved)}，回退默认模板")
            prompt = self._render_prompt(
                TRIAGE_PROMPT_DEFAULT,
                task_title=task["title"],
                task_description=task["description"] or "(无额外描述)",
                agent_list=agent_list,
            )
        parent_requirements = self._build_parent_requirements(task)
        prompt += "\n\n## 父任务需求清单（必须引用编号）\n"
        prompt += self._format_parent_requirements(parent_requirements)
        handoff_context = await self.build_handoff_context(task_id)
        if handoff_context:
            prompt += f"\n\n{handoff_context}\n"
        prompt += f"\n\n{LEADER_PROMPT_QUALITY_BLOCK}\n"

        proj_root, _ = get_project_dirs(task)
        run_dir = proj_root if proj_root.exists() else Path.cwd()
        decision_dir = run_dir / ".opc" / "decisions"
        decision_dir.mkdir(parents=True, exist_ok=True)
        decision_file = decision_dir / f"{task_id}.leader-triage.json"
        try:
            decision_file.unlink(missing_ok=True)
        except Exception:
            pass

        prompt += (
            "\n\n## 结构化交付（必须）\n"
            f"请把最终评估写入 JSON 文件：{decision_file}\n"
            "仅允许以下二选一格式：\n"
            '{"action":"simple","reason":"..."}\n'
            '{"action":"decompose","subtasks":[{"title":"...","objective":"...","implementation_scope":["..."],'
            '"parent_refs":["R1"],"todo_steps":["步骤1","步骤2"],"deliverables":["..."],"acceptance_criteria":["...","..."],"agent":"developer"}]}\n'
            "同时在回复最后一行输出同一个 JSON 对象。"
        )

        returncode, output = await self.run_cli(
            prompt,
            cwd=run_dir,
            task_id=task_id,
            output_schema=LEADER_TRIAGE_SCHEMA,
        )
        if returncode != 0:
            if await self.stop_if_task_cancelled(task_id, "评估 CLI 失败后"):
                return
            prev_status = task.get("_claimed_from_status", task.get("status", "triage"))
            fail_msg = f"❌ Leader 评估失败（exit={returncode}），退回 {prev_status}"
            await self.add_log(task_id, fail_msg)
            if output.strip():
                await self.add_log(task_id, f"错误输出:\n{output[:800]}")
            await self.add_alert(
                summary=f"Leader 评估失败（exit={returncode}）",
                task_id=task_id,
                message=output[-1200:].strip(),
                kind="error",
                code="leader_triage_failed",
                stage="leader_failed",
                metadata={"exit_code": returncode},
            )
            await self.transition_task(
                task_id,
                fields={"status": prev_status, "assignee": None},
                handoff={
                    "stage": "leader_failed",
                    "to_agent": self.name,
                    "status_from": prev_status,
                    "status_to": prev_status,
                    "title": "任务评估失败",
                    "summary": f"Leader 评估失败（exit={returncode}），保持状态 {prev_status}",
                    "conclusion": f"评估失败，保持状态 {prev_status}",
                    "payload": {"exit_code": returncode},
                },
            )
            return
        await self.add_log(task_id, f"评估输出:\n{output[:600]}")
        if await self.stop_if_task_cancelled(task_id, "评估输出后"):
            return

        raw_decision = self._load_json_file(decision_file)
        if raw_decision is None:
            await self._handle_structured_output_error(
                task,
                prev_status=task.get("_claimed_from_status", task.get("status", "triage")),
                stage_code="leader_triage_invalid_output",
                reason=f"未读取到有效结构化结果文件：{decision_file}",
                output=output,
            )
            return
        decision, quality_issues = self._normalize_triage_decision(
            raw_decision,
            parent_requirements=parent_requirements,
        )
        if decision is None:
            issue_text = "; ".join(quality_issues[:6]) if quality_issues else "未知原因"
            await self._handle_structured_output_error(
                task,
                prev_status=task.get("_claimed_from_status", task.get("status", "triage")),
                stage_code="leader_triage_invalid_schema",
                reason=f"结构化结果字段不完整或质量不足：{decision_file}；{issue_text}",
                output=output,
            )
            return
        await self.add_log(task_id, f"已读取结构化评估结果文件: {decision_file}")
        complex_by_rule = self._is_complex_task(task)
        await self.add_log(task_id, f"复杂度规则判定: {'complex' if complex_by_rule else 'simple'}")

        if decision and decision.get("action") == "decompose":
            subtasks = decision.get("subtasks") or []
            if not subtasks:
                await self._handle_structured_output_error(
                    task,
                    prev_status=task.get("_claimed_from_status", task.get("status", "triage")),
                    stage_code="leader_triage_invalid_schema",
                    reason=f"action=decompose 但 subtasks 为空：{decision_file}",
                    output=output,
                )
                return

            if not complex_by_rule:
                await self.add_log(
                    task_id,
                    "复杂度规则判定: simple，但结构化结果为 decompose；按分解结果执行。"
                )
            await self.add_log(task_id, f"判断为复杂任务，分解为 {len(subtasks)} 个子任务")
            n = await self._create_subtasks(task, subtasks)
            await self.transition_task(
                task_id,
                fields={"status": "decomposed", "assignee": None},
                handoff={
                    "stage": "leader_to_decomposed",
                    "to_agent": "multi-agent",
                    "status_from": task.get("_claimed_from_status", task.get("status")),
                    "status_to": "decomposed",
                    "title": "任务分解完成",
                    "summary": f"已分解为 {n} 个子任务并分配",
                    "conclusion": f"复杂任务，已拆分为 {n} 个子任务",
                    "payload": {"subtask_count": n, "decision": "decompose"},
                    "artifact_path": str(decision_file),
                },
                log_message=f"✅ 分解完成，共创建 {n} 个子任务",
            )
            self._post_output_bg(f"✓ 已分解为 {n} 个子任务")
            return

        # Simple task — push to todo
        reason = decision.get("reason", "判定为简单任务")
        todo_agent = self._todo_assigned_agent(task) or "developer"
        await self.transition_task(
            task_id,
            fields={
                "status": "todo",
                "assignee": None,
                "assigned_agent": self._todo_assigned_agent(task),
            },
            handoff={
                "stage": "leader_to_todo",
                "to_agent": todo_agent,
                "status_from": task.get("_claimed_from_status", task.get("status")),
                "status_to": "todo",
                "title": "任务转入开发",
                "summary": reason[:300],
                "conclusion": reason[:300] or "判定为简单任务，转入开发",
                "payload": {"action": "simple"},
                "artifact_path": str(decision_file),
            },
            log_message=f"判断为简单任务：{reason}",
        )
        self._post_output_bg(f"✓ 简单任务，推进至 todo")

    async def _force_decompose(self, task: dict):
        task_id = task["id"]
        agent_list = await self._get_agent_list()
        prompt = self._render_prompt(
            FORCE_DECOMPOSE_PROMPT,
            task_title=task["title"],
            task_description=task["description"] or "(无额外描述)",
            agent_list=agent_list,
        )
        unresolved = [
            x for x in ("{task_title}", "{task_description}", "{agent_list}") if x in prompt
        ]
        if unresolved:
            await self.add_log(task_id, f"⚠ Leader 强制分解模板仍有未替换占位符: {', '.join(unresolved)}，回退默认模板")
            prompt = self._render_prompt(
                FORCE_DECOMPOSE_PROMPT,
                task_title=task["title"],
                task_description=task["description"] or "(无额外描述)",
                agent_list=agent_list,
            )
        parent_requirements = self._build_parent_requirements(task)
        prompt += "\n\n## 父任务需求清单（必须引用编号）\n"
        prompt += self._format_parent_requirements(parent_requirements)
        handoff_context = await self.build_handoff_context(task_id)
        if handoff_context:
            prompt += f"\n\n{handoff_context}\n"

        proj_root, _ = get_project_dirs(task)
        run_dir = proj_root if proj_root.exists() else Path.cwd()
        decision_dir = run_dir / ".opc" / "decisions"
        decision_dir.mkdir(parents=True, exist_ok=True)
        decision_file = decision_dir / f"{task_id}.leader-force-decompose.json"
        try:
            decision_file.unlink(missing_ok=True)
        except Exception:
            pass

        prompt += (
            "\n\n## 结构化交付（必须）\n"
            f"请把子任务数组写入 JSON 文件：{decision_file}\n"
            "文件内容必须是 JSON 数组，每个元素必须包含：\n"
            '- title\n- objective\n- parent_refs(至少1条，且必须引用需求编号)\n- implementation_scope(可选)\n- todo_steps(至少2条)\n- deliverables(至少1条)\n- acceptance_criteria(至少2条)\n- agent\n'
            "同时在回复最后一行输出同一个 JSON 数组。"
        )

        returncode, output = await self.run_cli(
            prompt,
            cwd=run_dir,
            task_id=task_id,
            output_schema=LEADER_FORCE_DECOMPOSE_SCHEMA,
        )
        if returncode != 0:
            if await self.stop_if_task_cancelled(task_id, "分解 CLI 失败后"):
                return
            prev_status = task.get("_claimed_from_status", task.get("status", "decompose"))
            fail_msg = f"❌ Leader 分解失败（exit={returncode}），退回 {prev_status}"
            await self.add_log(task_id, fail_msg)
            if output.strip():
                await self.add_log(task_id, f"错误输出:\n{output[:800]}")
            await self.add_alert(
                summary=f"Leader 分解失败（exit={returncode}）",
                task_id=task_id,
                message=output[-1200:].strip(),
                kind="error",
                code="leader_decompose_failed",
                stage="leader_failed",
                metadata={"exit_code": returncode},
            )
            await self.transition_task(
                task_id,
                fields={"status": prev_status, "assignee": None},
                handoff={
                    "stage": "leader_failed",
                    "to_agent": self.name,
                    "status_from": prev_status,
                    "status_to": prev_status,
                    "title": "任务分解失败",
                    "summary": f"Leader 分解失败（exit={returncode}），保持状态 {prev_status}",
                    "conclusion": f"分解失败，保持状态 {prev_status}",
                    "payload": {"exit_code": returncode},
                },
            )
            return
        await self.add_log(task_id, f"分解输出:\n{output[:600]}")
        if await self.stop_if_task_cancelled(task_id, "分解输出后"):
            return

        raw_subtasks = self._load_json_file(decision_file)
        subtasks, quality_issues = self._normalize_subtasks(
            raw_subtasks,
            parent_requirements=parent_requirements,
        )
        if raw_subtasks is None:
            await self._handle_structured_output_error(
                task,
                prev_status=task.get("_claimed_from_status", task.get("status", "decompose")),
                stage_code="leader_force_decompose_invalid_output",
                reason=f"未读取到有效结构化子任务文件：{decision_file}",
                output=output,
            )
            return
        await self.add_log(task_id, f"已读取结构化分解结果文件: {decision_file}")
        if not subtasks:
            issue_text = "; ".join(quality_issues[:6]) if quality_issues else "未知原因"
            await self._handle_structured_output_error(
                task,
                prev_status=task.get("_claimed_from_status", task.get("status", "decompose")),
                stage_code="leader_force_decompose_invalid_schema",
                reason=f"子任务数组为空或质量不足：{decision_file}；{issue_text}",
                output=output,
            )
            return

        n = await self._create_subtasks(task, subtasks)
        await self.transition_task(
            task_id,
            fields={"status": "decomposed", "assignee": None},
            handoff={
                "stage": "leader_to_decomposed",
                "to_agent": "multi-agent",
                "status_from": task.get("_claimed_from_status", task.get("status")),
                "status_to": "decomposed",
                "title": "强制分解完成",
                "summary": f"已强制分解为 {n} 个子任务",
                "conclusion": f"强制分解完成，共 {n} 个子任务",
                "payload": {"subtask_count": n, "forced": True},
                "artifact_path": str(decision_file),
            },
            log_message=f"✅ 强制分解完成，共创建 {n} 个子任务",
        )
        self._post_output_bg(f"✓ 强制分解完成: {n} 个子任务")


if __name__ == "__main__":
    asyncio.run(LeaderAgent().run())
